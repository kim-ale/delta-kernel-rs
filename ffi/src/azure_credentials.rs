//! On-demand Azure bearer acquisition through caller-owned, consuming completion tickets.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::{self, CredentialProvider};
use delta_kernel::KernelError;
use delta_kernel_ffi_macros::handle_descriptor;
use tokio::sync::Notify;

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{KernelStringSlice, NullableCvoid};

const MAX_WAITERS: usize = 1024;
const CANCEL_TIMEOUT: u32 = 1;
const CANCEL_NO_INTEREST: u32 = 2;

/// Queues acquisition and unconditionally owns `ticket`, even if queuing fails.
///
/// `request_id` identifies cancellation independently of the ticket address. The remaining
/// acquisition budget and minimum useful token lifetime are milliseconds. Queue work and return
/// promptly; do not wait for acquisition. Complete, fail, or free the ticket exactly once.
pub type StartAzureCredentialRequestFn = extern "C" fn(
    context: NullableCvoid,
    request_id: u64,
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    remaining_ms: u32,
    minimum_lifetime_ms: u32,
);

/// Requests cooperative cancellation after Start returns: reason 1 is timeout, 2 no waiters.
/// Cancellation does not consume the foreign-owned ticket and does not await foreign work.
pub type CancelAzureCredentialRequestFn =
    extern "C" fn(context: NullableCvoid, request_id: u64, reason: u32);

/// Releases the retained context once its final provider, request, ticket, or upcall is gone.
pub type ReleaseAzureCredentialContextFn = extern "C" fn(context: NullableCvoid);

/// Versioned callback configuration, copied during successful creation; no acquisition starts then.
///
/// Callbacks must be nonblocking, non-unwinding, concurrent, and safe on any thread. Context
/// ownership transfers only on creation success. Cancellation is at most once per request and
/// follows Start's return, including retirement during Start. Release runs outside native locks.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CAzureCredentialProviderConfig {
    /// Must be 1.
    pub abi_version: u32,
    /// Must equal sizeof(CAzureCredentialProviderConfig).
    pub struct_size: u32,
    /// Absolute acquisition budget, 1 through 120000 milliseconds.
    pub acquisition_timeout_ms: u32,
    /// Minimum remaining lifetime, 1 through 3600000 milliseconds.
    pub minimum_lifetime_ms: u32,
    /// Maximum bearer bytes, 1 through 65536.
    pub max_token_bytes: u32,
    /// Maximum foreign tickets, including retired but unreturned tickets, 1 through 1024.
    pub max_outstanding_requests: u32,
    /// Opaque caller context, retained only on successful creation; null is allowed.
    pub context: NullableCvoid,
    /// Required queueing callback.
    pub start: Option<
        extern "C" fn(
            context: NullableCvoid,
            request_id: u64,
            ticket: Handle<ExclusiveAzureCredentialRequest>,
            remaining_ms: u32,
            minimum_lifetime_ms: u32,
        ),
    >,
    /// Optional cooperative cancellation callback.
    pub cancel: Option<extern "C" fn(context: NullableCvoid, request_id: u64, reason: u32)>,
    /// Required final context release callback.
    pub release: Option<extern "C" fn(context: NullableCvoid)>,
}

/// Shared, bounded provider for the kernel's selected object_store Azure credential trait.
///
/// A usable cache hit does not acquire. A cache miss joins one flight or starts one. There is no
/// automatic retry, stale fallback, JWT parsing, or requirement that renewed token bytes differ.
/// Failure ends this attempt; a subsequent caller may try again. Native waiters are capped at 1024.
pub struct AzureCredentialProvider {
    context: Arc<Context>,
    state: Arc<Mutex<State>>,
    tickets: Arc<AtomicUsize>,
    waiters: Arc<AtomicUsize>,
}

/// Opaque shared provider descriptor. Free each owned reference exactly once.
#[handle_descriptor(target = AzureCredentialProvider, mutable = false, sized = true)]
pub struct SharedAzureCredentialProvider;

/// Opaque exclusive request descriptor. Complete, fail, or free exactly once on any thread.
#[handle_descriptor(target = AzureCredentialRequest, mutable = true, sized = true)]
pub struct ExclusiveAzureCredentialRequest;

/// Foreign-owned completion ticket; dropping it fails a still-live acquisition.
pub struct AzureCredentialRequest {
    request: Arc<Request>,
    state: Weak<Mutex<State>>,
    _permit: Permit,
}

impl fmt::Debug for AzureCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AzureCredentialProvider { credentials: <redacted> }")
    }
}

#[async_trait]
impl CredentialProvider for AzureCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        if let Some(cached) = lock(&self.state)
            .cache
            .as_ref()
            .filter(|cache| cache.valid())
        {
            return Ok(cached.credential.clone());
        }
        let expired = lock(&self.state)
            .active
            .as_ref()
            .filter(|request| Instant::now() >= request.deadline)
            .cloned();
        if let Some(request) = expired {
            let _ = finish(
                &Arc::downgrade(&self.state),
                &request,
                Err(FixedFailure::Timeout),
                Some(CANCEL_TIMEOUT),
            );
        }
        let permit = Permit::acquire(&self.waiters, MAX_WAITERS)
            .ok_or_else(|| FixedFailure::WaiterCapacity.store_error())?;
        let (interest, ticket) = {
            let mut state = lock(&self.state);
            if let Some(cached) = state.cache.as_ref().filter(|cache| cache.valid()) {
                return Ok(cached.credential.clone());
            }
            let (request, ticket) = if let Some(request) = &state.active {
                lock(&request.inner).interests += 1;
                (request.clone(), None)
            } else {
                let ticket_permit = Permit::acquire(
                    &self.tickets,
                    self.context.max_outstanding_requests as usize,
                )
                .ok_or_else(|| FixedFailure::TicketCapacity.store_error())?;
                let id = state
                    .next_id
                    .checked_add(1)
                    .ok_or_else(|| FixedFailure::Sequence.store_error())?;
                let deadline = Instant::now()
                    .checked_add(Duration::from_millis(
                        self.context.acquisition_timeout_ms.into(),
                    ))
                    .ok_or_else(|| FixedFailure::Timeout.store_error())?;
                let request = Arc::new(Request {
                    id,
                    deadline,
                    context: self.context.clone(),
                    inner: Mutex::new(RequestState {
                        result: None,
                        interests: 1,
                        start_returned: false,
                        cancel_reason: None,
                        cancel_sent: false,
                    }),
                    notify: Notify::new(),
                });
                state.next_id = id;
                state.active = Some(request.clone());
                let ticket = AzureCredentialRequest {
                    request: request.clone(),
                    state: Arc::downgrade(&self.state),
                    _permit: ticket_permit,
                };
                (request, Some(ticket))
            };
            (
                Interest {
                    state: self.state.clone(),
                    request,
                    _permit: permit,
                },
                ticket,
            )
        };
        let request = &interest.request;
        if let Some(ticket) = ticket {
            let remaining_ms = request
                .deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(u128::from(u32::MAX)) as u32;
            (request.context.start)(
                request.context.context,
                request.id,
                Box::new(ticket).into(),
                remaining_ms,
                request.context.minimum_lifetime_ms,
            );
            lock(&request.inner).start_returned = true;
            request.deliver_cancel();
        }
        loop {
            let mut notified = std::pin::pin!(request.notify.notified());
            notified.as_mut().enable();
            let result = lock(&request.inner).result.clone();
            if let Some(result) = result {
                return match result {
                    Ok(cache) if cache.valid() => Ok(cache.credential),
                    Ok(_) => Err(FixedFailure::Expiry.store_error()),
                    Err(failure) => Err(failure.store_error()),
                };
            }
            if tokio::time::timeout_at(request.deadline.into(), notified)
                .await
                .is_err()
            {
                let _ = finish(
                    &Arc::downgrade(&interest.state),
                    request,
                    Err(FixedFailure::Timeout),
                    Some(CANCEL_TIMEOUT),
                );
            }
        }
    }
}

impl Drop for AzureCredentialProvider {
    fn drop(&mut self) {
        let request = lock(&self.state).active.clone();
        if let Some(request) = request {
            let _ = finish(
                &Arc::downgrade(&self.state),
                &request,
                Err(FixedFailure::Cancelled),
                Some(CANCEL_NO_INTEREST),
            );
        }
    }
}

impl Drop for AzureCredentialRequest {
    fn drop(&mut self) {
        let _ = finish(
            &self.state,
            &self.request,
            Err(FixedFailure::Abandoned),
            None,
        );
    }
}

/// Creates an owned provider reference, or returns an allocated, sanitized configuration error.
/// Validation failure invokes no callback and transfers no context ownership.
///
/// # Safety
///
/// `config` is null or addresses an aligned initialized version/size prefix; when those fields
/// are accepted it must address the entire initialized descriptor for this call. All callbacks
/// and context obey the descriptor's concurrency, nonblocking, and no-unwind contracts until
/// Release. `allocate_error` must be valid and copy the borrowed error message before returning.
#[no_mangle]
pub unsafe extern "C" fn create_azure_credential_provider(
    config: *const CAzureCredentialProviderConfig,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedAzureCredentialProvider>> {
    let result = unsafe { validate_config(config) }.map(|context| {
        Arc::new(AzureCredentialProvider {
            context: Arc::new(context),
            state: Arc::new(Mutex::new(State {
                cache: None,
                active: None,
                next_id: 0,
            })),
            tickets: Arc::new(AtomicUsize::new(0)),
            waiters: Arc::new(AtomicUsize::new(0)),
        })
        .into()
    });
    unsafe {
        result
            .map_err(FixedFailure::kernel_error)
            .into_extern_result(&allocate_error)
    }
}

/// Unconditionally consumes a provider reference without waiting for foreign acquisition work.
/// Other provider references, native waiters, tickets, and active upcalls retain their context.
///
/// # Safety
///
/// `provider` must be valid and owned by the caller; it must not be used again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_azure_credential_provider(
    provider: Handle<SharedAzureCredentialProvider>,
) {
    unsafe { provider.drop_handle() };
}

/// Consumes `ticket` before validation. Returns true for accepted completion, false if retired.
///
/// `token` is copied synchronously, bounded by the provider limit, and must follow RFC 6750
/// bearer syntax. `expires_unix_ms` is a positive UTC expiry with the configured minimum lifetime.
/// Invalid live input fails the generation and returns an allocated sanitized error. Retired
/// input is ignored. Payload copying never holds the generation gate or disables its timeout.
///
/// # Safety
///
/// `ticket` must be valid, exclusively owned, and never reused after this call, including errors.
/// If read, a non-null token pointer must address its stated number of initialized bytes until
/// this call returns. `allocate_error` must be valid and copy the borrowed message.
#[no_mangle]
pub unsafe extern "C" fn complete_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    token: KernelStringSlice,
    expires_unix_ms: i64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    let ticket = unsafe { ticket.into_inner() };
    if !ticket.live() {
        return ExternResult::Ok(false);
    }
    let payload = unsafe { validate_token(&token, expires_unix_ms, &ticket.request.context) };
    let result =
        finish(&ticket.state, &ticket.request, payload, None).map_err(FixedFailure::kernel_error);
    unsafe { result.into_extern_result(&allocate_error) }
}

/// Consumes `ticket` and fails acquisition: kind 1 transient, 2 permanent, 3 cancelled.
///
/// Returns true if accepted, false if retired. Unknown kinds fail a live generation and return
/// a sanitized error. There is no automatic retry; transient failure has a distinct diagnostic.
///
/// # Safety
///
/// `ticket` must be valid, exclusively owned, and not reused, regardless of the result.
/// `allocate_error` must be valid and copy the borrowed error message.
#[no_mangle]
pub unsafe extern "C" fn fail_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    failure_kind: u32,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    let ticket = unsafe { ticket.into_inner() };
    let failure = match failure_kind {
        1 => FixedFailure::Transient,
        2 => FixedFailure::Permanent,
        3 => FixedFailure::Cancelled,
        _ => FixedFailure::FailureKind,
    };
    let result = finish(&ticket.state, &ticket.request, Err(failure), None)
        .map_err(FixedFailure::kernel_error);
    unsafe { result.into_extern_result(&allocate_error) }
}

/// Consumes an unused ticket and fails a still-live acquisition without an error allocation.
///
/// # Safety
///
/// `ticket` must be valid, exclusively owned, and never used again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
) {
    unsafe { ticket.drop_handle() };
}

struct Context {
    context: NullableCvoid,
    start: StartAzureCredentialRequestFn,
    cancel: Option<CancelAzureCredentialRequestFn>,
    release: ReleaseAzureCredentialContextFn,
    acquisition_timeout_ms: u32,
    minimum_lifetime_ms: u32,
    max_token_bytes: u32,
    max_outstanding_requests: u32,
}

/// # Safety
/// Creation requires callbacks and their opaque context to support concurrent, any-thread use.
unsafe impl Send for Context {}
/// # Safety
/// The same creation contract permits shared callback/context access without native exclusivity.
unsafe impl Sync for Context {}

impl Drop for Context {
    fn drop(&mut self) {
        (self.release)(self.context);
    }
}

struct State {
    cache: Option<Cached>,
    active: Option<Arc<Request>>,
    next_id: u64,
}

#[derive(Clone)]
struct Cached {
    credential: Arc<AzureCredential>,
    valid_until: Instant,
    expires_at: SystemTime,
    minimum_lifetime: Duration,
}

impl Cached {
    fn valid(&self) -> bool {
        Instant::now() <= self.valid_until
            && self
                .expires_at
                .duration_since(SystemTime::now())
                .is_ok_and(|remaining| remaining >= self.minimum_lifetime)
    }
}

struct Request {
    id: u64,
    deadline: Instant,
    context: Arc<Context>,
    inner: Mutex<RequestState>,
    notify: Notify,
}

struct RequestState {
    result: Option<Result<Cached, FixedFailure>>,
    interests: usize,
    start_returned: bool,
    cancel_reason: Option<u32>,
    cancel_sent: bool,
}

impl Request {
    fn deliver_cancel(&self) {
        let reason = {
            let mut inner = lock(&self.inner);
            if !inner.start_returned || inner.cancel_sent {
                return;
            }
            let Some(reason) = inner.cancel_reason else {
                return;
            };
            inner.cancel_sent = true;
            reason
        };
        if let Some(cancel) = self.context.cancel {
            cancel(self.context.context, self.id, reason);
        }
    }
}

impl AzureCredentialRequest {
    fn live(&self) -> bool {
        if Instant::now() >= self.request.deadline {
            let _ = finish(
                &self.state,
                &self.request,
                Err(FixedFailure::Timeout),
                Some(CANCEL_TIMEOUT),
            );
            return false;
        }
        lock(&self.request.inner).result.is_none()
    }
}

struct Interest {
    state: Arc<Mutex<State>>,
    request: Arc<Request>,
    _permit: Permit,
}

impl Drop for Interest {
    fn drop(&mut self) {
        let detached = {
            let mut state = lock(&self.state);
            let mut inner = lock(&self.request.inner);
            inner.interests -= 1;
            if inner.interests != 0 || inner.result.is_some() {
                return;
            }
            inner.result = Some(Err(FixedFailure::Cancelled));
            inner.cancel_reason = Some(CANCEL_NO_INTEREST);
            detach(&mut state, &self.request)
        };
        drop(detached);
        self.request.notify.notify_waiters();
        self.request.deliver_cancel();
    }
}

struct Permit(Arc<AtomicUsize>);

impl Permit {
    fn acquire(counter: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit).then(|| count + 1)
            })
            .ok()
            .map(|_| Self(counter.clone()))
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FixedFailure {
    Config,
    Token,
    Expiry,
    Timeout,
    Cancelled,
    Transient,
    Permanent,
    FailureKind,
    Abandoned,
    TicketCapacity,
    WaiterCapacity,
    Sequence,
}

impl fmt::Display for FixedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Config => "invalid Azure credential provider configuration",
            Self::Token => "invalid Azure bearer token payload",
            Self::Expiry => "invalid Azure bearer token expiry or remaining lifetime",
            Self::Timeout => "Azure credential acquisition timed out",
            Self::Cancelled => "Azure credential acquisition cancelled",
            Self::Transient => "transient Azure credential acquisition failure; caller may retry",
            Self::Permanent => "permanent Azure credential acquisition failure",
            Self::FailureKind => "invalid Azure credential acquisition failure kind",
            Self::Abandoned => "Azure credential acquisition ticket abandoned",
            Self::TicketCapacity => "Azure credential outstanding ticket capacity exhausted",
            Self::WaiterCapacity => "Azure credential native waiter capacity exhausted",
            Self::Sequence => "Azure credential request sequence exhausted",
        })
    }
}

impl std::error::Error for FixedFailure {}

impl FixedFailure {
    fn kernel_error(self) -> KernelError {
        KernelError::generic(self.to_string())
    }

    fn store_error(self) -> object_store::Error {
        object_store::Error::Generic {
            store: "AzureCredentialProvider",
            source: Box::new(self),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn detach(state: &mut State, request: &Arc<Request>) -> Option<Arc<Request>> {
    if state
        .active
        .as_ref()
        .is_some_and(|active| Arc::ptr_eq(active, request))
    {
        state.active.take()
    } else {
        None
    }
}

fn finish(
    state: &Weak<Mutex<State>>,
    request: &Arc<Request>,
    mut result: Result<Cached, FixedFailure>,
    mut cancel_reason: Option<u32>,
) -> Result<bool, FixedFailure> {
    let Some(state) = state.upgrade() else {
        return Ok(false);
    };
    let (outcome, detached) = {
        let mut state = lock(&state);
        let mut inner = lock(&request.inner);
        if inner.result.is_some() {
            return Ok(false);
        }
        let accepted = Instant::now() < request.deadline && inner.interests != 0;
        if !accepted {
            result = Err(FixedFailure::Timeout);
            cancel_reason = Some(CANCEL_TIMEOUT);
        } else if result.as_ref().is_ok_and(|cache| !cache.valid()) {
            result = Err(FixedFailure::Expiry);
        }
        if let Ok(cache) = &result {
            state.cache = Some(cache.clone());
        }
        let outcome = match result {
            Err(
                failure @ (FixedFailure::Token | FixedFailure::Expiry | FixedFailure::FailureKind),
            ) if accepted => Err(failure),
            _ => Ok(accepted),
        };
        inner.result = Some(result);
        inner.cancel_reason = cancel_reason;
        (outcome, detach(&mut state, request))
    };
    drop(detached);
    request.notify.notify_waiters();
    request.deliver_cancel();
    outcome
}

unsafe fn validate_config(
    config: *const CAzureCredentialProviderConfig,
) -> Result<Context, FixedFailure> {
    #[repr(C)]
    struct Prefix {
        abi_version: u32,
        struct_size: u32,
    }
    if config.is_null() {
        return Err(FixedFailure::Config);
    }
    let prefix = unsafe { &*config.cast::<Prefix>() };
    if prefix.abi_version != 1
        || prefix.struct_size as usize != std::mem::size_of::<CAzureCredentialProviderConfig>()
    {
        return Err(FixedFailure::Config);
    }
    let config = unsafe { &*config };
    if !(1..=120_000).contains(&config.acquisition_timeout_ms)
        || !(1..=3_600_000).contains(&config.minimum_lifetime_ms)
        || !(1..=65_536).contains(&config.max_token_bytes)
        || !(1..=1024).contains(&config.max_outstanding_requests)
    {
        return Err(FixedFailure::Config);
    }
    let start = config.start.ok_or(FixedFailure::Config)?;
    let release = config.release.ok_or(FixedFailure::Config)?;
    Ok(Context {
        context: config.context,
        start,
        cancel: config.cancel,
        release,
        acquisition_timeout_ms: config.acquisition_timeout_ms,
        minimum_lifetime_ms: config.minimum_lifetime_ms,
        max_token_bytes: config.max_token_bytes,
        max_outstanding_requests: config.max_outstanding_requests,
    })
}

unsafe fn validate_token(
    token: &KernelStringSlice,
    expires_unix_ms: i64,
    context: &Context,
) -> Result<Cached, FixedFailure> {
    if token.len == 0 || token.len > context.max_token_bytes as usize || token.ptr.is_null() {
        return Err(FixedFailure::Token);
    }
    let bytes = unsafe { std::slice::from_raw_parts(token.ptr.cast::<u8>(), token.len) };
    let token = std::str::from_utf8(bytes).map_err(|_| FixedFailure::Token)?;
    let padding = bytes
        .iter()
        .position(|byte| *byte == b'=')
        .unwrap_or(bytes.len());
    if padding == 0
        || !bytes[..padding]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/".contains(byte))
        || !bytes[padding..].iter().all(|byte| *byte == b'=')
    {
        return Err(FixedFailure::Token);
    }
    let expiry = u64::try_from(expires_unix_ms)
        .ok()
        .filter(|expiry| *expiry > 0)
        .ok_or(FixedFailure::Expiry)?;
    let expires_at = UNIX_EPOCH
        .checked_add(Duration::from_millis(expiry))
        .ok_or(FixedFailure::Expiry)?;
    let monotonic = Instant::now();
    let remaining = expires_at
        .duration_since(SystemTime::now())
        .map_err(|_| FixedFailure::Expiry)?;
    let minimum_lifetime = Duration::from_millis(context.minimum_lifetime_ms.into());
    monotonic
        .checked_add(remaining)
        .ok_or(FixedFailure::Expiry)?;
    let valid_until = monotonic
        .checked_add(
            remaining
                .checked_sub(minimum_lifetime)
                .ok_or(FixedFailure::Expiry)?,
        )
        .ok_or(FixedFailure::Expiry)?;
    Ok(Cached {
        credential: Arc::new(AzureCredential::BearerToken(token.to_owned())),
        valid_until,
        expires_at,
        minimum_lifetime,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ptr::NonNull;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::task::{Context as TaskContext, Poll, Waker};

    use delta_kernel_default_engine::executor::tokio::{
        TokioBackgroundExecutor, TokioMultiThreadExecutor,
    };
    use delta_kernel_default_engine::executor::TaskExecutor;

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic, recover_error};
    use crate::kernel_string_slice;

    #[derive(Default)]
    struct Host {
        releases: AtomicUsize,
        starts: AtomicUsize,
        tickets: Mutex<Vec<(u64, Handle<ExclusiveAzureCredentialRequest>)>>,
        cancellations: Mutex<Vec<(u64, u32)>>,
        synchronous: AtomicBool,
        retire_during_start: AtomicBool,
        nested_free: Mutex<Option<Handle<SharedAzureCredentialProvider>>>,
        state: Mutex<Weak<Mutex<State>>>,
        started: Notify,
    }

    fn host(context: NullableCvoid) -> &'static Host {
        unsafe { &*context.unwrap().as_ptr().cast::<Host>() }
    }

    extern "C" fn start(
        context: NullableCvoid,
        id: u64,
        ticket: Handle<ExclusiveAzureCredentialRequest>,
        remaining_ms: u32,
        minimum_lifetime_ms: u32,
    ) {
        let host = host(context);
        assert!(remaining_ms <= 120_000);
        assert_eq!(minimum_lifetime_ms, 1);
        assert!(lock(&host.state).upgrade().unwrap().try_lock().is_ok());
        host.starts.fetch_add(1, Ordering::SeqCst);
        if host.synchronous.load(Ordering::SeqCst) {
            assert!(complete(ticket, "synchronous", expiry(10_000)));
        } else {
            if host.retire_during_start.load(Ordering::SeqCst) {
                let ticket_ref = unsafe { ticket.as_ref() };
                finish(
                    &ticket_ref.state,
                    &ticket_ref.request,
                    Err(FixedFailure::Cancelled),
                    Some(CANCEL_NO_INTEREST),
                )
                .unwrap();
                assert!(lock(&host.cancellations).is_empty());
            }
            lock(&host.tickets).push((id, ticket));
        }
        host.started.notify_waiters();
    }

    extern "C" fn cancel(context: NullableCvoid, id: u64, reason: u32) {
        let host = host(context);
        if let Some(state) = lock(&host.state).upgrade() {
            assert!(state.try_lock().is_ok());
        }
        lock(&host.cancellations).push((id, reason));
    }

    extern "C" fn release(context: NullableCvoid) {
        let host = unsafe { Arc::from_raw(context.unwrap().as_ptr().cast::<Host>()) };
        if let Some(state) = lock(&host.state).upgrade() {
            assert!(state.try_lock().is_ok());
        }
        host.releases.fetch_add(1, Ordering::SeqCst);
        let nested = lock(&host.nested_free).take();
        if let Some(nested) = nested {
            unsafe { free_azure_credential_provider(nested) };
        }
    }

    fn create(
        host: &Arc<Host>,
        mut config: CAzureCredentialProviderConfig,
    ) -> Handle<SharedAzureCredentialProvider> {
        config.context = NonNull::new(Arc::into_raw(host.clone()).cast_mut().cast());
        config.start = Some(start);
        config.cancel = Some(cancel);
        config.release = Some(release);
        let handle =
            ok_or_panic(unsafe { create_azure_credential_provider(&config, allocate_err) });
        *lock(&host.state) = Arc::downgrade(&unsafe { handle.as_ref() }.state);
        handle
    }

    fn provider(
        host: &Arc<Host>,
        config: CAzureCredentialProviderConfig,
    ) -> Arc<AzureCredentialProvider> {
        unsafe { create(host, config).into_inner() }
    }

    fn expiry(remaining_ms: u64) -> i64 {
        (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + u128::from(remaining_ms)) as i64
    }

    fn complete(ticket: Handle<ExclusiveAzureCredentialRequest>, token: &str, expiry: i64) -> bool {
        ok_or_panic(unsafe {
            complete_azure_credential_request(
                ticket,
                kernel_string_slice!(token),
                expiry,
                allocate_err,
            )
        })
    }

    fn token(credential: &AzureCredential) -> &str {
        let AzureCredential::BearerToken(token) = credential else {
            panic!("expected bearer")
        };
        token
    }

    fn live_ticket(
        provider: &Arc<AzureCredentialProvider>,
    ) -> (Interest, Handle<ExclusiveAzureCredentialRequest>) {
        let mut state = lock(&provider.state);
        state.next_id += 1;
        let request = Arc::new(Request {
            id: state.next_id,
            deadline: Instant::now() + Duration::from_secs(60),
            context: provider.context.clone(),
            inner: Mutex::new(RequestState {
                result: None,
                interests: 1,
                start_returned: true,
                cancel_reason: None,
                cancel_sent: false,
            }),
            notify: Notify::new(),
        });
        state.active = Some(request.clone());
        let ticket = Box::new(AzureCredentialRequest {
            request: request.clone(),
            state: Arc::downgrade(&provider.state),
            _permit: Permit::acquire(&provider.tickets, 1024).unwrap(),
        })
        .into();
        let interest = Interest {
            request,
            state: provider.state.clone(),
            _permit: Permit::acquire(&provider.waiters, MAX_WAITERS).unwrap(),
        };
        (interest, ticket)
    }

    async fn take_ticket(host: &Host) -> (u64, Handle<ExclusiveAzureCredentialRequest>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut notified = std::pin::pin!(host.started.notified());
                notified.as_mut().enable();
                let ticket = lock(&host.tickets).pop();
                if let Some(ticket) = ticket {
                    return ticket;
                }
                notified.await;
            }
        })
        .await
        .unwrap()
    }

    fn get(
        provider: &Arc<AzureCredentialProvider>,
    ) -> tokio::task::JoinHandle<object_store::Result<Arc<AzureCredential>>> {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get_credential().await })
    }

    async fn joined(provider: &AzureCredentialProvider, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while provider.waiters.load(Ordering::SeqCst) != count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    extern "C" fn unused_start(
        _context: NullableCvoid,
        _id: u64,
        ticket: Handle<ExclusiveAzureCredentialRequest>,
        _remaining_ms: u32,
        _minimum_lifetime_ms: u32,
    ) {
        unsafe { free_azure_credential_request(ticket) };
    }

    extern "C" fn unused_release(_context: NullableCvoid) {}

    fn config() -> CAzureCredentialProviderConfig {
        CAzureCredentialProviderConfig {
            abi_version: 1,
            struct_size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
            acquisition_timeout_ms: 1000,
            minimum_lifetime_ms: 1,
            max_token_bytes: 65_536,
            max_outstanding_requests: 1024,
            context: None,
            start: Some(unused_start),
            cancel: None,
            release: Some(unused_release),
        }
    }

    #[test]
    fn create_and_free_without_acquisition() {
        let handle =
            ok_or_panic(unsafe { create_azure_credential_provider(&config(), allocate_err) });
        let provider = unsafe { handle.clone_as_arc() };
        let _: Arc<dyn CredentialProvider<Credential = AzureCredential>> = provider.clone();
        assert!(!format!("{provider:?}").contains("BearerToken"));
        unsafe { free_azure_credential_provider(handle) };
    }

    #[test]
    fn invalid_configuration_returns_sanitized_owned_error() {
        let result = unsafe { create_azure_credential_provider(std::ptr::null(), allocate_err) };
        let ExternResult::Err(error) = result else {
            panic!("expected error")
        };
        assert_eq!(
            unsafe { recover_error(error) }.message,
            FixedFailure::Config.kernel_error().to_string()
        );
    }

    #[rstest::rstest]
    #[case(0, 0)]
    #[case(0, 2)]
    #[case(1, 0)]
    #[case(1, u32::MAX)]
    #[case(2, 0)]
    #[case(2, 120_001)]
    #[case(3, 0)]
    #[case(3, 3_600_001)]
    #[case(4, 0)]
    #[case(4, 65_537)]
    #[case(5, 0)]
    #[case(5, 1025)]
    #[case(6, 0)]
    #[case(7, 0)]
    fn invalid_configuration_never_takes_context(#[case] field: u32, #[case] value: u32) {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.context = Some(NonNull::from(host.as_ref()).cast());
        config.release = Some(release);
        match field {
            0 => config.abi_version = value,
            1 => config.struct_size = value,
            2 => config.acquisition_timeout_ms = value,
            3 => config.minimum_lifetime_ms = value,
            4 => config.max_token_bytes = value,
            5 => config.max_outstanding_requests = value,
            6 => config.start = None,
            7 => config.release = None,
            _ => unreachable!(),
        }
        let ExternResult::Err(error) =
            (unsafe { create_azure_credential_provider(&config, allocate_err) })
        else {
            panic!("invalid config accepted")
        };
        assert_eq!(
            unsafe { recover_error(error) }.message,
            FixedFailure::Config.kernel_error().to_string()
        );
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        assert_eq!(host.starts.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&host), 1);
    }

    #[test]
    fn release_is_once_outside_locks_and_can_free_another_provider() {
        let outer = Arc::new(Host::default());
        let inner = Arc::new(Host::default());
        let handle = create(&outer, config());
        *lock(&outer.nested_free) = Some(create(&inner, config()));
        let duplicate = unsafe { handle.clone_handle() };
        unsafe { free_azure_credential_provider(handle) };
        assert_eq!(outer.releases.load(Ordering::SeqCst), 0);
        unsafe { free_azure_credential_provider(duplicate) };
        assert_eq!(outer.releases.load(Ordering::SeqCst), 1);
        assert_eq!(inner.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest::rstest]
    #[case(b"", false)]
    #[case(b"=", false)]
    #[case(b"===", false)]
    #[case(b"secret\r\nheader", false)]
    #[case(b"bad space", false)]
    #[case(b"bad:colon", false)]
    #[case(b"bad=middle", false)]
    #[case(b"\xff", false)]
    #[case(b"\xc3\xa9", false)]
    #[case(b"aZ09-._~+/===", true)]
    fn consuming_completion_validates_and_redacts(#[case] bytes: &[u8], #[case] valid: bool) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (interest, ticket) = live_ticket(&provider);
        let result = unsafe {
            complete_azure_credential_request(
                ticket,
                KernelStringSlice {
                    ptr: bytes.as_ptr().cast(),
                    len: bytes.len(),
                },
                expiry(10_000),
                allocate_err,
            )
        };
        if valid {
            assert!(ok_or_panic(result));
            assert!(lock(&provider.state).cache.is_some());
        } else {
            let ExternResult::Err(error) = result else {
                panic!("invalid token accepted")
            };
            let error = unsafe { recover_error(error) };
            assert_eq!(
                error.message,
                FixedFailure::Token.kernel_error().to_string()
            );
            assert_eq!(
                format!(
                    "{:?}",
                    lock(&interest.request.inner)
                        .result
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .err()
                        .unwrap()
                ),
                "Token"
            );
            assert!(lock(&provider.state).cache.is_none());
        }
        assert!(lock(&provider.state).active.is_none());
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
        assert!(lock(&host.cancellations).is_empty());
        drop(interest);
        assert_eq!(provider.waiters.load(Ordering::SeqCst), 0);
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn payload_bounds_are_checked_before_dereferencing() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        for (ptr, len) in [
            (std::ptr::null(), 1),
            (NonNull::dangling().as_ptr(), 65_537),
        ] {
            let (interest, ticket) = live_ticket(&provider);
            let result = unsafe {
                complete_azure_credential_request(
                    ticket,
                    KernelStringSlice { ptr, len },
                    expiry(10_000),
                    allocate_err,
                )
            };
            let ExternResult::Err(error) = result else {
                panic!("invalid bounds accepted")
            };
            assert_eq!(
                unsafe { recover_error(error) }.message,
                FixedFailure::Token.kernel_error().to_string()
            );
            drop(interest);
        }
    }

    #[rstest::rstest]
    #[case(4, 4, true)]
    #[case(4, 5, false)]
    #[case(65_536, 65_536, true)]
    fn exact_configured_and_global_token_limits(
        #[case] limit: u32,
        #[case] length: usize,
        #[case] accepted: bool,
    ) {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.max_token_bytes = limit;
        let provider = provider(&host, config);
        let (interest, ticket) = live_ticket(&provider);
        let bearer = "a".repeat(length);
        let result = unsafe {
            complete_azure_credential_request(
                ticket,
                kernel_string_slice!(bearer),
                expiry(120_000),
                allocate_err,
            )
        };
        if accepted {
            assert!(ok_or_panic(result));
        } else {
            let ExternResult::Err(error) = result else {
                panic!("oversized token accepted")
            };
            assert_eq!(
                unsafe { recover_error(error) }.message,
                FixedFailure::Token.kernel_error().to_string()
            );
        }
        drop(interest);
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }

    #[rstest::rstest]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    fn invalid_expiry_fails_the_generation(#[case] expires: i64) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (interest, ticket) = live_ticket(&provider);
        let bearer = "opaque";
        let result = unsafe {
            complete_azure_credential_request(
                ticket,
                kernel_string_slice!(bearer),
                expires,
                allocate_err,
            )
        };
        let ExternResult::Err(error) = result else {
            panic!("invalid expiry accepted")
        };
        assert_eq!(
            unsafe { recover_error(error) }.message,
            FixedFailure::Expiry.kernel_error().to_string()
        );
        assert!(lock(&provider.state).active.is_none());
        drop(interest);
    }

    #[test]
    fn minimum_lifetime_and_checked_publication_expiry_are_enforced() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (interest, ticket) = live_ticket(&provider);
        let bearer = "opaque";
        let mut cache = unsafe {
            validate_token(
                &kernel_string_slice!(bearer),
                expiry(10_000),
                &provider.context,
            )
        }
        .ok()
        .unwrap();
        cache.valid_until = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            finish(
                &Arc::downgrade(&provider.state),
                &interest.request,
                Ok(cache),
                None
            ),
            Err(FixedFailure::Expiry)
        );
        assert!(lock(&provider.state).cache.is_none());
        assert!(!complete(ticket, "ignored", expiry(10_000)));
        drop(interest);
        assert!(unsafe {
            validate_token(&kernel_string_slice!(bearer), expiry(0), &provider.context)
        }
        .is_err());
        if UNIX_EPOCH
            .checked_add(Duration::from_millis(i64::MAX as u64))
            .is_none()
        {
            assert!(unsafe {
                validate_token(&kernel_string_slice!(bearer), i64::MAX, &provider.context)
            }
            .is_err());
        }
    }

    #[rstest::rstest]
    #[case(1, FixedFailure::Transient)]
    #[case(2, FixedFailure::Permanent)]
    #[case(3, FixedFailure::Cancelled)]
    #[case(0, FixedFailure::FailureKind)]
    #[case(u32::MAX, FixedFailure::FailureKind)]
    fn consuming_failure_is_fixed_and_terminal(#[case] kind: u32, #[case] expected: FixedFailure) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (interest, ticket) = live_ticket(&provider);
        let result = unsafe { fail_azure_credential_request(ticket, kind, allocate_err) };
        if expected == FixedFailure::FailureKind {
            let ExternResult::Err(error) = result else {
                panic!("invalid kind accepted")
            };
            assert_eq!(
                unsafe { recover_error(error) }.message,
                expected.kernel_error().to_string()
            );
        } else {
            assert!(ok_or_panic(result));
        }
        assert_eq!(
            lock(&interest.request.inner)
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .err(),
            Some(&expected)
        );
        assert!(lock(&provider.state).active.is_none());
        assert!(lock(&host.cancellations).is_empty());
    }

    #[test]
    fn request_free_fails_and_late_ticket_keeps_context_without_cycles() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let provider_weak = Arc::downgrade(&provider);
        let state_weak = Arc::downgrade(&provider.state);
        let (interest, ticket) = live_ticket(&provider);
        unsafe { free_azure_credential_request(ticket) };
        assert_eq!(
            lock(&interest.request.inner)
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .err(),
            Some(&FixedFailure::Abandoned)
        );
        drop(interest);
        let (interest, ticket) = live_ticket(&provider);
        let request_weak = Arc::downgrade(&interest.request);
        unsafe { free_azure_credential_provider(provider.into()) };
        drop(interest);
        assert!(provider_weak.upgrade().is_none());
        assert!(state_weak.upgrade().is_none());
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        assert!(!ok_or_panic(unsafe {
            complete_azure_credential_request(
                ticket,
                KernelStringSlice {
                    ptr: std::ptr::null(),
                    len: usize::MAX,
                },
                -1,
                allocate_err,
            )
        }));
        assert!(request_weak.upgrade().is_none());
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; consuming ownership is tested separately"
    )]
    async fn synchronous_start_completion_is_cacheable_without_cancellation() {
        let host = Arc::new(Host::default());
        host.synchronous.store(true, Ordering::SeqCst);
        let provider = provider(&host, config());
        let first = provider.get_credential().await.unwrap();
        let cached = provider.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&first, &cached));
        assert_eq!(token(&cached), "synchronous");
        assert_eq!(host.starts.load(Ordering::SeqCst), 1);
        assert!(lock(&host.cancellations).is_empty());
        assert_eq!(provider.waiters.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; terminal ownership is tested separately"
    )]
    async fn concurrent_waiters_share_flight_and_cancel_independently() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let first = get(&provider);
        let (_, ticket) = take_ticket(&host).await;
        let second = get(&provider);
        joined(&provider, 2).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(lock(&host.cancellations).is_empty());
        assert!(complete(ticket, "shared", expiry(10_000)));
        let second = second.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &second,
            &provider.get_credential().await.unwrap()
        ));
        assert_eq!(host.starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; late-ticket ownership is tested separately"
    )]
    async fn last_waiter_cancel_detaches_and_late_completion_cannot_seed_cache() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let first = get(&provider);
        let (old_id, old_ticket) = take_ticket(&host).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(lock(&provider.state).active.is_none());
        let second = get(&provider);
        let (new_id, new_ticket) = take_ticket(&host).await;
        assert!(new_id > old_id);
        assert!(!complete(old_ticket, "late", expiry(10_000)));
        assert!(lock(&provider.state).cache.is_none());
        assert!(complete(new_ticket, "fresh", expiry(10_000)));
        assert_eq!(token(&second.await.unwrap().unwrap()), "fresh");
        assert_eq!(
            *lock(&host.cancellations),
            vec![(old_id, CANCEL_NO_INTEREST)]
        );
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timeout and foreign thread; ticket memory is tested separately"
    )]
    async fn timeout_progresses_while_foreign_completion_owns_ticket() {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.acquisition_timeout_ms = 100;
        let provider = provider(&host, config);
        let waiter = get(&provider);
        let (id, ticket) = take_ticket(&host).await;
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let claimed = unsafe { ticket.into_inner() };
            ready_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            complete(claimed.into(), "late", expiry(10_000))
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(*lock(&host.cancellations), vec![(id, CANCEL_TIMEOUT)]);
        assert!(lock(&provider.state).active.is_none());
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 1);
        resume_tx.send(()).unwrap();
        assert!(!worker.join().unwrap());
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; permit accounting is tested through exports"
    )]
    async fn retired_foreign_tickets_exhaust_capacity_until_consumed() {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.max_outstanding_requests = 2;
        let provider = provider(&host, config);
        let mut retained = Vec::new();
        for _ in 0..2 {
            let waiter = get(&provider);
            retained.push(take_ticket(&host).await.1);
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
        }
        assert!(provider
            .get_credential()
            .await
            .unwrap_err()
            .to_string()
            .contains("ticket capacity"));
        unsafe { free_azure_credential_request(retained.pop().unwrap()) };
        let waiter = get(&provider);
        let (_, ticket) = take_ticket(&host).await;
        assert!(complete(ticket, "capacity", expiry(10_000)));
        waiter.await.unwrap().unwrap();
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        unsafe { free_azure_credential_request(retained.pop().unwrap()) };
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; consuming completion is tested separately"
    )]
    async fn cache_renews_on_demand_and_accepts_identical_short_lived_tokens() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        for _ in 0..2 {
            let waiter = get(&provider);
            let (_, ticket) = take_ticket(&host).await;
            assert!(complete(ticket, "same", expiry(500)));
            assert_eq!(token(&waiter.await.unwrap().unwrap()), "same");
            assert_eq!(token(&provider.get_credential().await.unwrap()), "same");
            lock(&provider.state).cache.as_mut().unwrap().valid_until =
                Instant::now() - Duration::from_millis(1);
        }
        assert_eq!(host.starts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; no-lock callback release is tested separately"
    )]
    async fn cancellation_is_deferred_until_start_returns() {
        let host = Arc::new(Host::default());
        host.retire_during_start.store(true, Ordering::SeqCst);
        let provider = provider(&host, config());
        assert!(provider
            .get_credential()
            .await
            .unwrap_err()
            .to_string()
            .contains("cancelled"));
        let (id, ticket) = take_ticket(&host).await;
        assert_eq!(*lock(&host.cancellations), vec![(id, CANCEL_NO_INTEREST)]);
        assert!(!ok_or_panic(unsafe {
            fail_azure_credential_request(ticket, 0, allocate_err)
        }));
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; permit release is covered by ownership tests"
    )]
    async fn native_waiter_bound_and_every_interest_drop_are_independent() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let mut futures: Vec<_> = (0..MAX_WAITERS)
            .map(|_| Box::pin(provider.get_credential()))
            .collect();
        let mut task = TaskContext::from_waker(Waker::noop());
        for future in &mut futures {
            assert!(future.as_mut().poll(&mut task).is_pending());
        }
        let (_, ticket) = take_ticket(&host).await;
        let error = provider.get_credential().await.unwrap_err();
        assert!(error.to_string().contains("waiter capacity"));
        assert_eq!(host.starts.load(Ordering::SeqCst), 1);
        drop(futures.remove(0));
        assert_eq!(provider.waiters.load(Ordering::SeqCst), MAX_WAITERS - 1);
        assert!(lock(&host.cancellations).is_empty());
        let mut replacement = Box::pin(provider.get_credential());
        assert!(replacement.as_mut().poll(&mut task).is_pending());
        drop(futures);
        assert!(lock(&host.cancellations).is_empty());
        drop(replacement);
        assert_eq!(provider.waiters.load(Ordering::SeqCst), 0);
        assert_eq!(lock(&host.cancellations).len(), 1);
        assert!(!complete(ticket, "late", expiry(10_000)));
        let mut unpolled = Box::pin(provider.get_credential());
        assert!(matches!(unpolled.as_mut().poll(&mut task), Poll::Pending));
        drop(unpolled);
        let (_, ticket) = take_ticket(&host).await;
        unsafe { free_azure_credential_request(ticket) };
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio native timer/runtime; fixed failure kinds are tested separately"
    )]
    async fn transient_failure_has_no_retry_or_stale_fallback_until_next_caller() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let first = get(&provider);
        let (_, ticket) = take_ticket(&host).await;
        assert!(complete(ticket, "old", expiry(10_000)));
        first.await.unwrap().unwrap();
        lock(&provider.state).cache.as_mut().unwrap().valid_until =
            Instant::now() - Duration::from_millis(1);
        let failed = get(&provider);
        let (_, ticket) = take_ticket(&host).await;
        assert!(ok_or_panic(unsafe {
            fail_azure_credential_request(ticket, 1, allocate_err)
        }));
        let error = tokio::time::timeout(Duration::from_secs(5), failed)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("transient"));
        assert_eq!(host.starts.load(Ordering::SeqCst), 2);
        assert!(lock(&host.cancellations).is_empty());
        let retry = get(&provider);
        let (_, ticket) = take_ticket(&host).await;
        assert!(complete(ticket, "fresh", expiry(10_000)));
        assert_eq!(token(&retry.await.unwrap().unwrap()), "fresh");
        assert_eq!(host.starts.load(Ordering::SeqCst), 3);
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[cfg_attr(
        miri,
        ignore = "Stock owned executors use native threads/timers; late-ticket ownership is tested separately"
    )]
    fn stock_owned_executor_deadline_and_completion_without_runtime(#[case] multithreaded: bool) {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.acquisition_timeout_ms = 100;
        let provider = provider(&host, config);
        let caller = provider.clone();
        let task = async move { caller.get_credential().await };
        let error = if multithreaded {
            TokioMultiThreadExecutor::new_owned_runtime(Some(2), Some(2))
                .unwrap()
                .block_on(task)
        } else {
            TokioBackgroundExecutor::new().block_on(task)
        }
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        let (id, ticket) = lock(&host.tickets).pop().unwrap();
        assert_eq!(*lock(&host.cancellations), vec![(id, CANCEL_TIMEOUT)]);
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        assert!(!complete(ticket, "late", expiry(10_000)));
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }
}
