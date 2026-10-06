//! Per-request Azure bearer acquisition through caller-owned, consuming completion tickets.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::{self, CredentialProvider};
use delta_kernel::KernelError;
use delta_kernel_ffi_macros::handle_descriptor;
use tokio::sync::oneshot;

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{KernelStringSlice, NullableCvoid};

const CANCEL_TIMEOUT: u32 = 1;
const CANCEL_AWAITER_DROPPED: u32 = 2;

/// Queues acquisition and unconditionally owns `ticket`, even if queuing fails.
/// `request_id` identifies cancellation independently of the ticket address. Acquisition budget
/// and minimum useful lifetime are milliseconds. Queue work and return promptly without waiting
/// for acquisition; complete, fail, or free the ticket exactly once.
pub type StartAzureCredentialRequestFn = extern "C" fn(
    context: NullableCvoid,
    request_id: u64,
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    remaining_ms: u32,
    minimum_lifetime_ms: u32,
);

/// Requests cancellation after Start returns: reason 1 is timeout, 2 is awaiter dropped.
/// Cancellation neither consumes the foreign-owned ticket nor waits for foreign work.
pub type CancelAzureCredentialRequestFn =
    extern "C" fn(context: NullableCvoid, request_id: u64, reason: u32);

/// Releases the retained context once its final provider, request, ticket, or upcall is gone.
pub type ReleaseAzureCredentialContextFn = extern "C" fn(context: NullableCvoid);

/// Versioned configuration copied on successful creation, which starts no acquisition.
/// Callbacks must be nonblocking, non-unwinding, concurrent, and safe on any thread. Context
/// ownership transfers only on success. Cancel is at most once per pending request, after Start
/// returns, including timeout observed during Start. Release runs outside native locks.
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

/// Shared bridge to the kernel's selected object_store Azure credential trait.
/// Every acquisition starts an independent request. The caller's identity SDK owns caching,
/// deduplication, and refresh policy; this bridge has no cache, fallback, JWT parsing, or retries.
pub struct AzureCredentialProvider {
    context: Arc<Context>,
    tickets: Arc<AtomicUsize>,
    next_id: AtomicU64,
}

/// Opaque shared provider descriptor. Free each owned reference exactly once.
#[handle_descriptor(target = AzureCredentialProvider, mutable = false, sized = true)]
pub struct SharedAzureCredentialProvider;

/// Opaque exclusive request descriptor. Complete, fail, or free exactly once on any thread.
#[handle_descriptor(target = AzureCredentialRequest, mutable = true, sized = true)]
pub struct ExclusiveAzureCredentialRequest;

/// Foreign-owned completion ticket; dropping it fails a still-pending acquisition.
pub struct AzureCredentialRequest {
    request: Arc<Request>,
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
        let (ticket, receiver) = self.new_request().map_err(FixedFailure::store_error)?;
        let guard = RequestGuard(ticket.request.clone());
        let request = &guard.0;
        let remaining_ms = request
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u32;
        (request.context.start)(
            request.context.context,
            request.id,
            Box::new(ticket).into(),
            remaining_ms,
            request.context.minimum_lifetime_ms,
        );
        lock(&request.inner).start_returned = true;
        request.deliver_cancel();
        let result = match tokio::time::timeout_at(request.deadline.into(), receiver).await {
            Ok(result) if Instant::now() < request.deadline => {
                result.unwrap_or(Err(FixedFailure::Abandoned))
            }
            _ => {
                request.retire(CANCEL_TIMEOUT);
                Err(FixedFailure::Timeout)
            }
        };
        result
            .and_then(|payload| {
                if payload.valid() {
                    Ok(payload.credential)
                } else {
                    Err(FixedFailure::Expiry)
                }
            })
            .map_err(FixedFailure::store_error)
    }
}

impl Drop for AzureCredentialRequest {
    fn drop(&mut self) {
        let _ = self.request.finish(Err(FixedFailure::Abandoned));
    }
}

/// Creates an owned provider reference or an allocated sanitized configuration error.
/// Validation failure invokes no callback and transfers no context ownership.
///
/// # Safety
/// `config` is null or addresses an aligned initialized version/size prefix. If accepted, it
/// must address the full initialized descriptor. Callbacks/context must obey the descriptor's
/// concurrency, nonblocking, and no-unwind contracts until Release. `allocate_error` must be
/// valid and copy the borrowed error message before returning.
#[no_mangle]
pub unsafe extern "C" fn create_azure_credential_provider(
    config: *const CAzureCredentialProviderConfig,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedAzureCredentialProvider>> {
    // SAFETY: The caller provides the prefix/full descriptor required by validation.
    let result = unsafe { validate_config(config) }.map(|context| {
        Arc::new(AzureCredentialProvider {
            context: Arc::new(context),
            tickets: Arc::new(AtomicUsize::new(0)),
            next_id: AtomicU64::new(0),
        })
        .into()
    });
    // SAFETY: The caller's allocator copies this call's sanitized error message.
    unsafe {
        result
            .map_err(FixedFailure::kernel_error)
            .into_extern_result(&allocate_error)
    }
}

/// Unconditionally consumes a provider reference without waiting for foreign work.
/// Other provider references, requests, tickets, and active upcalls retain their context.
///
/// # Safety
/// `provider` must be valid, owned by the caller, and never used again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_azure_credential_provider(
    provider: Handle<SharedAzureCredentialProvider>,
) {
    // SAFETY: The caller transfers one owned provider reference.
    unsafe { provider.drop_handle() };
}

/// Consumes `ticket` before validation; returns true if accepted, false if retired.
/// Copies `token` synchronously within the provider limit, enforcing RFC 6750 bearer syntax.
/// `expires_unix_ms` is positive UTC expiry with the configured minimum useful lifetime.
/// Invalid live input fails this request and returns an allocated sanitized error. Retired input
/// is ignored. Copying holds no request lock and cannot disable the native acquisition deadline.
///
/// # Safety
/// `ticket` must be valid, exclusively owned, and never reused, including after errors.
/// If read, a non-null token pointer must address its stated initialized bytes until return.
/// `allocate_error` must be valid and copy the borrowed error message.
#[no_mangle]
pub unsafe extern "C" fn complete_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    token: KernelStringSlice,
    expires_unix_ms: i64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    // SAFETY: Ownership transfers unconditionally before any input validation.
    let ticket = unsafe { ticket.into_inner() };
    if !ticket.request.live() {
        return ExternResult::Ok(false);
    }
    // SAFETY: Bounds are checked before reading the caller's borrowed token bytes.
    let payload = unsafe { validate_token(&token, expires_unix_ms, &ticket.request.context) };
    let result = ticket
        .request
        .finish(payload)
        .map_err(FixedFailure::kernel_error);
    // SAFETY: The allocator copies this call's sanitized error message.
    unsafe { result.into_extern_result(&allocate_error) }
}

/// Consumes `ticket` and fails acquisition: kind 1 transient, 2 permanent, 3 cancelled.
/// Returns true if accepted, false if retired. Unknown live kinds return a sanitized error.
/// Transient failures are diagnostic only; the bridge never retries automatically.
///
/// # Safety
/// `ticket` must be valid, exclusively owned, and never reused, regardless of the result.
/// `allocate_error` must be valid and copy the borrowed error message.
#[no_mangle]
pub unsafe extern "C" fn fail_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
    failure_kind: u32,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    // SAFETY: Ownership transfers unconditionally before failure-kind validation.
    let ticket = unsafe { ticket.into_inner() };
    let failure = match failure_kind {
        1 => FixedFailure::Transient,
        2 => FixedFailure::Permanent,
        3 => FixedFailure::Cancelled,
        _ => FixedFailure::FailureKind,
    };
    let result = ticket
        .request
        .finish(Err(failure))
        .map_err(FixedFailure::kernel_error);
    // SAFETY: The allocator copies this call's sanitized error message.
    unsafe { result.into_extern_result(&allocate_error) }
}

/// Consumes an unused ticket, failing a pending acquisition without allocating an error.
///
/// # Safety
/// `ticket` must be valid, exclusively owned, and never used again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_azure_credential_request(
    ticket: Handle<ExclusiveAzureCredentialRequest>,
) {
    // SAFETY: The caller transfers exclusive ticket ownership.
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
// SAFETY: Creation requires concurrent, any-thread callbacks and opaque-context access.
unsafe impl Send for Context {}
// SAFETY: The same caller contract permits shared callback/context access on any thread.
unsafe impl Sync for Context {}
impl Drop for Context {
    fn drop(&mut self) {
        (self.release)(self.context);
    }
}

type Completion = Result<Payload, FixedFailure>;
struct Payload {
    credential: Arc<AzureCredential>,
    useful_until: Instant,
    expires_at: SystemTime,
    minimum_lifetime: Duration,
}
impl Payload {
    fn valid(&self) -> bool {
        Instant::now() <= self.useful_until
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
}
struct RequestState {
    sender: Option<oneshot::Sender<Completion>>,
    start_returned: bool,
    cancel_reason: Option<u32>,
}
impl Request {
    fn live(&self) -> bool {
        if Instant::now() >= self.deadline {
            self.retire(CANCEL_TIMEOUT);
            return false;
        }
        lock(&self.inner).sender.is_some()
    }
    fn claim(&self) -> Option<oneshot::Sender<Completion>> {
        let mut inner = lock(&self.inner);
        if Instant::now() >= self.deadline {
            drop(inner);
            self.retire(CANCEL_TIMEOUT);
            return None;
        }
        inner.sender.take()
    }
    fn finish(&self, result: Completion) -> Result<bool, FixedFailure> {
        let result = result.and_then(|payload| {
            if payload.valid() {
                Ok(payload)
            } else {
                Err(FixedFailure::Expiry)
            }
        });
        let outcome = match &result {
            Err(
                failure @ (FixedFailure::Token | FixedFailure::Expiry | FixedFailure::FailureKind),
            ) => Err(*failure),
            _ => Ok(true),
        };
        let Some(sender) = self.claim() else {
            return Ok(false);
        };
        if Instant::now() >= self.deadline {
            let _ = sender.send(Err(FixedFailure::Timeout));
            return Ok(false);
        }
        if sender.send(result).is_ok() {
            outcome
        } else {
            Ok(false)
        }
    }
    fn retire(&self, reason: u32) {
        let sender = {
            let mut inner = lock(&self.inner);
            let sender = inner.sender.take();
            if sender.is_some() {
                inner.cancel_reason = Some(reason);
            }
            sender
        };
        drop(sender);
        self.deliver_cancel();
    }
    fn deliver_cancel(&self) {
        let reason = {
            let mut inner = lock(&self.inner);
            if !inner.start_returned {
                return;
            }
            inner.cancel_reason.take()
        };
        if let (Some(reason), Some(cancel)) = (reason, self.context.cancel) {
            cancel(self.context.context, self.id, reason);
        }
    }
}
struct RequestGuard(Arc<Request>);
impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.0.retire(CANCEL_AWAITER_DROPPED);
    }
}
impl AzureCredentialProvider {
    fn new_request(
        &self,
    ) -> Result<(AzureCredentialRequest, oneshot::Receiver<Completion>), FixedFailure> {
        let permit = Permit::acquire(
            &self.tickets,
            self.context.max_outstanding_requests as usize,
        )
        .ok_or(FixedFailure::TicketCapacity)?;
        let id = self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| FixedFailure::Sequence)?
            + 1;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(
                self.context.acquisition_timeout_ms.into(),
            ))
            .ok_or(FixedFailure::Timeout)?;
        let (sender, receiver) = oneshot::channel();
        let request = Arc::new(Request {
            id,
            deadline,
            context: self.context.clone(),
            inner: Mutex::new(RequestState {
                sender: Some(sender),
                start_returned: false,
                cancel_reason: None,
            }),
        });
        Ok((
            AzureCredentialRequest {
                request,
                _permit: permit,
            },
            receiver,
        ))
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
    // SAFETY: The caller supplies at least the aligned, initialized prefix.
    let prefix = unsafe { &*config.cast::<Prefix>() };
    if prefix.abi_version != 1
        || prefix.struct_size as usize != std::mem::size_of::<CAzureCredentialProviderConfig>()
    {
        return Err(FixedFailure::Config);
    }
    // SAFETY: An accepted prefix requires the full initialized descriptor from the caller.
    let config = unsafe { &*config };
    if !(1..=120_000).contains(&config.acquisition_timeout_ms)
        || !(1..=3_600_000).contains(&config.minimum_lifetime_ms)
        || !(1..=65_536).contains(&config.max_token_bytes)
        || !(1..=1024).contains(&config.max_outstanding_requests)
    {
        return Err(FixedFailure::Config);
    }
    Ok(Context {
        context: config.context,
        start: config.start.ok_or(FixedFailure::Config)?,
        cancel: config.cancel,
        release: config.release.ok_or(FixedFailure::Config)?,
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
) -> Completion {
    if token.len == 0 || token.len > context.max_token_bytes as usize || token.ptr.is_null() {
        return Err(FixedFailure::Token);
    }
    // SAFETY: The caller supplies initialized bytes; the checked length is at most 65536.
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
    let useful_until = monotonic
        .checked_add(
            remaining
                .checked_sub(minimum_lifetime)
                .ok_or(FixedFailure::Expiry)?,
        )
        .ok_or(FixedFailure::Expiry)?;
    Ok(Payload {
        credential: Arc::new(AzureCredential::BearerToken(token.to_owned())),
        useful_until,
        expires_at,
        minimum_lifetime,
    })
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic, recover_error};
    use crate::kernel_string_slice;

    #[derive(Default)]
    struct Host {
        releases: AtomicUsize,
        starts: AtomicUsize,
        tickets: Mutex<Vec<(u64, Handle<ExclusiveAzureCredentialRequest>)>>,
        cancellations: Mutex<Vec<(u64, u32)>>,
        synchronous: AtomicBool,
        expired_start: AtomicBool,
    }
    fn host(context: NullableCvoid) -> &'static Host {
        // SAFETY: Tests retain the Host until the final context Release.
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
        assert!(remaining_ms <= 120_000 && (1..=3_600_000).contains(&minimum_lifetime_ms));
        assert!(unsafe { ticket.as_ref() }.request.inner.try_lock().is_ok());
        host.starts.fetch_add(1, Ordering::SeqCst);
        if host.expired_start.load(Ordering::SeqCst) {
            assert!(
                !std::thread::spawn(move || complete(ticket, ignored_slice(), -1))
                    .join()
                    .unwrap()
            );
            assert!(lock(&host.cancellations).is_empty());
        } else if host.synchronous.load(Ordering::SeqCst) {
            assert!(complete(ticket, slice("same"), expiry(120_000)));
        } else {
            lock(&host.tickets).push((id, ticket));
        }
    }
    extern "C" fn cancel(context: NullableCvoid, id: u64, reason: u32) {
        lock(&host(context).cancellations).push((id, reason));
    }
    extern "C" fn release(context: NullableCvoid) {
        // SAFETY: Successful creation transfers exactly one Arc reference to Release.
        let host = unsafe { Arc::from_raw(context.unwrap().as_ptr().cast::<Host>()) };
        host.releases.fetch_add(1, Ordering::SeqCst);
    }
    fn config() -> CAzureCredentialProviderConfig {
        CAzureCredentialProviderConfig {
            abi_version: 1,
            struct_size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
            acquisition_timeout_ms: 1000,
            minimum_lifetime_ms: 1,
            max_token_bytes: 65_536,
            max_outstanding_requests: 1024,
            context: None,
            start: Some(start),
            cancel: Some(cancel),
            release: Some(release),
        }
    }
    fn create(
        host: &Arc<Host>,
        mut config: CAzureCredentialProviderConfig,
    ) -> Handle<SharedAzureCredentialProvider> {
        config.context = NonNull::new(Arc::into_raw(host.clone()).cast_mut().cast());
        ok_or_panic(unsafe { create_azure_credential_provider(&config, allocate_err) })
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
    fn ignored_slice() -> KernelStringSlice {
        KernelStringSlice {
            ptr: std::ptr::null(),
            len: usize::MAX,
        }
    }
    fn slice(token: &str) -> KernelStringSlice {
        kernel_string_slice!(token)
    }
    fn complete(
        ticket: Handle<ExclusiveAzureCredentialRequest>,
        token: KernelStringSlice,
        expiry: i64,
    ) -> bool {
        ok_or_panic(unsafe {
            complete_azure_credential_request(ticket, token, expiry, allocate_err)
        })
    }
    fn assert_error<T>(result: ExternResult<T>, failure: FixedFailure) {
        let ExternResult::Err(error) = result else {
            panic!("expected sanitized error");
        };
        let error = unsafe { recover_error(error) };
        assert!(matches!(error.etype, FFIKernelError::GenericError));
        assert_eq!(error.message, failure.kernel_error().to_string());
    }
    fn live_ticket(
        provider: &AzureCredentialProvider,
    ) -> (
        RequestGuard,
        Handle<ExclusiveAzureCredentialRequest>,
        oneshot::Receiver<Completion>,
    ) {
        let (mut ticket, receiver) = provider.new_request().unwrap();
        Arc::get_mut(&mut ticket.request).unwrap().deadline =
            Instant::now() + Duration::from_secs(120);
        lock(&ticket.request.inner).start_returned = true;
        (
            RequestGuard(ticket.request.clone()),
            Box::new(ticket).into(),
            receiver,
        )
    }
    fn token(credential: &AzureCredential) -> &str {
        let AzureCredential::BearerToken(token) = credential else {
            panic!("expected bearer");
        };
        token
    }
    fn get(
        provider: &Arc<AzureCredentialProvider>,
    ) -> tokio::task::JoinHandle<object_store::Result<Arc<AzureCredential>>> {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get_credential().await })
    }
    async fn take_ticket(host: &Host) -> (u64, Handle<ExclusiveAzureCredentialRequest>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ticket) = lock(&host.tickets).pop() {
                    return ticket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    fn configuration_limits_shared_handles_and_redacted_debug(#[case] maximum: bool) {
        let host = Arc::new(Host::default());
        let mut config = config();
        if maximum {
            config.acquisition_timeout_ms = 120_000;
            config.minimum_lifetime_ms = 3_600_000;
        } else {
            config.acquisition_timeout_ms = 1;
            config.max_token_bytes = 1;
            config.max_outstanding_requests = 1;
        }
        let handle = create(&host, config);
        let shared = unsafe { handle.clone_handle() };
        let provider = unsafe { handle.clone_as_arc() };
        let _: Arc<dyn CredentialProvider<Credential = AzureCredential>> = provider.clone();
        assert_eq!(
            format!("{provider:?}"),
            "AzureCredentialProvider { credentials: <redacted> }"
        );
        unsafe {
            free_azure_credential_provider(handle);
            free_azure_credential_provider(shared);
        }
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
        assert_eq!(host.starts.load(Ordering::SeqCst), 0);
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
        assert_error(
            unsafe { create_azure_credential_provider(&config, allocate_err) },
            FixedFailure::Config,
        );
        assert_eq!(host.starts.load(Ordering::SeqCst), 0);
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&host), 1);
    }
    #[test]
    fn configuration_rejects_null_and_reads_only_invalid_abi_prefix() {
        #[repr(C)]
        struct Prefix {
            version: u32,
            size: u32,
        }
        assert_error(
            unsafe { create_azure_credential_provider(std::ptr::null(), allocate_err) },
            FixedFailure::Config,
        );
        for prefix in [
            Prefix {
                version: 2,
                size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
            },
            Prefix {
                version: 1,
                size: 8,
            },
        ] {
            assert_error(
                unsafe {
                    create_azure_credential_provider(
                        std::ptr::from_ref(&prefix).cast(),
                        allocate_err,
                    )
                },
                FixedFailure::Config,
            );
        }
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
    fn consuming_completion_validates_utf8_bearer_and_releases_once(
        #[case] bytes: &[u8],
        #[case] valid: bool,
    ) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (guard, ticket, mut receiver) = live_ticket(&provider);
        let result = unsafe {
            complete_azure_credential_request(
                ticket,
                KernelStringSlice {
                    ptr: bytes.as_ptr().cast(),
                    len: bytes.len(),
                },
                expiry(120_000),
                allocate_err,
            )
        };
        if valid {
            assert!(ok_or_panic(result));
            assert_eq!(
                token(&receiver.try_recv().unwrap().unwrap().credential).as_bytes(),
                bytes
            );
        } else {
            assert_error(result, FixedFailure::Token);
            assert_eq!(
                receiver.try_recv().unwrap().err(),
                Some(FixedFailure::Token)
            );
        }
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
        drop(guard);
        assert!(lock(&host.cancellations).is_empty());
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }
    #[rstest::rstest]
    #[case(4, 4, true)]
    #[case(4, 5, false)]
    #[case(65_536, 65_536, true)]
    fn exact_token_limits_and_invalid_pointer_bounds(
        #[case] limit: u32,
        #[case] length: usize,
        #[case] valid: bool,
    ) {
        let host = Arc::new(Host::default());
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                max_token_bytes: limit,
                ..config()
            },
        );
        let (guard, ticket, _receiver) = live_ticket(&provider);
        let bearer = "a".repeat(length);
        let result = unsafe {
            complete_azure_credential_request(
                ticket,
                kernel_string_slice!(bearer),
                expiry(120_000),
                allocate_err,
            )
        };
        if valid {
            assert!(ok_or_panic(result));
        } else {
            assert_error(result, FixedFailure::Token);
        }
        drop(guard);
        for (ptr, len) in [
            (std::ptr::null(), 1),
            (NonNull::dangling().as_ptr(), 65_537),
        ] {
            let (_guard, ticket, _receiver) = live_ticket(&provider);
            assert_error(
                unsafe {
                    complete_azure_credential_request(
                        ticket,
                        KernelStringSlice { ptr, len },
                        expiry(120_000),
                        allocate_err,
                    )
                },
                FixedFailure::Token,
            );
        }
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }
    #[rstest::rstest]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    fn consuming_completion_rejects_invalid_expiry(#[case] expires: i64) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (_guard, ticket, mut receiver) = live_ticket(&provider);
        assert_error(
            unsafe {
                complete_azure_credential_request(ticket, slice("opaque"), expires, allocate_err)
            },
            FixedFailure::Expiry,
        );
        assert_eq!(
            receiver.try_recv().unwrap().err(),
            Some(FixedFailure::Expiry)
        );
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn minimum_lifetime_and_delivery_recheck_are_enforced() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (guard, ticket, mut receiver) = live_ticket(&provider);
        let mut payload =
            unsafe { validate_token(&slice("opaque"), expiry(120_000), &provider.context) }
                .ok()
                .unwrap();
        payload.useful_until = Instant::now() - Duration::from_millis(1);
        assert_eq!(guard.0.finish(Ok(payload)), Err(FixedFailure::Expiry));
        assert_eq!(
            receiver.try_recv().unwrap().err(),
            Some(FixedFailure::Expiry)
        );
        assert!(!complete(ticket, ignored_slice(), -1));
        assert!(unsafe { validate_token(&slice("opaque"), expiry(0), &provider.context) }.is_err());
        if UNIX_EPOCH
            .checked_add(Duration::from_millis(i64::MAX as u64))
            .is_none()
        {
            assert!(
                unsafe { validate_token(&slice("opaque"), i64::MAX, &provider.context) }.is_err()
            );
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
        let (_guard, ticket, mut receiver) = live_ticket(&provider);
        let result = unsafe { fail_azure_credential_request(ticket, kind, allocate_err) };
        if expected == FixedFailure::FailureKind {
            assert_error(result, expected);
        } else {
            assert!(ok_or_panic(result));
        }
        assert_eq!(receiver.try_recv().unwrap().err(), Some(expected));
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
        assert!(lock(&host.cancellations).is_empty());
    }
    #[rstest::rstest]
    #[case(0, false)]
    #[case(1, false)]
    #[case(2, false)]
    #[case(0, true)]
    #[case(1, true)]
    #[case(2, true)]
    fn consuming_ticket_on_any_thread_after_provider_free(
        #[case] action: u32,
        #[case] retired: bool,
    ) {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let weak = Arc::downgrade(&provider);
        let counter = provider.tickets.clone();
        let (guard, ticket, mut receiver) = live_ticket(&provider);
        if retired {
            guard.0.retire(CANCEL_AWAITER_DROPPED);
        }
        unsafe { free_azure_credential_provider(provider.into()) };
        assert!(weak.upgrade().is_none());
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        std::thread::spawn(move || match action {
            0 => assert_eq!(
                complete(
                    ticket,
                    if retired {
                        ignored_slice()
                    } else {
                        slice("opaque")
                    },
                    expiry(120_000)
                ),
                !retired
            ),
            1 => assert_eq!(
                ok_or_panic(unsafe {
                    fail_azure_credential_request(ticket, if retired { 0 } else { 2 }, allocate_err)
                }),
                !retired
            ),
            _ => unsafe { free_azure_credential_request(ticket) },
        })
        .join()
        .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        if !retired {
            let result = receiver.try_recv().unwrap();
            match action {
                0 => assert_eq!(token(&result.unwrap().credential), "opaque"),
                1 => assert_eq!(result.err(), Some(FixedFailure::Permanent)),
                _ => assert_eq!(result.err(), Some(FixedFailure::Abandoned)),
            }
        }
        drop(guard);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&host), 1);
    }
    #[test]
    fn retired_tickets_keep_capacity_and_sequence_never_wraps() {
        let host = Arc::new(Host::default());
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                max_outstanding_requests: 1,
                ..config()
            },
        );
        let (guard, ticket, _receiver) = live_ticket(&provider);
        drop(guard);
        assert_eq!(
            provider.new_request().err(),
            Some(FixedFailure::TicketCapacity)
        );
        unsafe { free_azure_credential_request(ticket) };
        provider.next_id.store(u64::MAX - 1, Ordering::Relaxed);
        let (guard, ticket, _receiver) = live_ticket(&provider);
        assert_eq!(guard.0.id, u64::MAX);
        unsafe { free_azure_credential_request(ticket) };
        drop(guard);
        assert_eq!(provider.new_request().err(), Some(FixedFailure::Sequence));
        assert_eq!(provider.next_id.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn timeout_observed_during_start_defers_cancel_until_return() {
        let host = Arc::new(Host::default());
        host.expired_start.store(true, Ordering::SeqCst);
        let provider = provider(&host, config());
        let (mut ticket, mut receiver) = provider.new_request().unwrap();
        Arc::get_mut(&mut ticket.request).unwrap().deadline = Instant::now();
        let guard = RequestGuard(ticket.request.clone());
        start(
            provider.context.context,
            guard.0.id,
            Box::new(ticket).into(),
            0,
            1,
        );
        assert!(lock(&host.cancellations).is_empty());
        lock(&guard.0.inner).start_returned = true;
        guard.0.deliver_cancel();
        guard.0.retire(CANCEL_TIMEOUT);
        assert_eq!(
            *lock(&host.cancellations),
            vec![(guard.0.id, CANCEL_TIMEOUT)]
        );
        assert!(receiver.try_recv().is_err());
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        drop(guard);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn closed_receiver_rejects_completion_without_retaining_context() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let (guard, ticket, receiver) = live_ticket(&provider);
        drop(receiver);
        assert!(!complete(ticket, slice("undelivered"), expiry(120_000)));
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
        drop(guard);
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio timer; FFI covered by consuming_completion_validates_utf8_bearer_and_releases_once"
    )]
    async fn successive_sdk_cached_tokens_still_start_twice() {
        let host = Arc::new(Host::default());
        host.synchronous.store(true, Ordering::SeqCst);
        let provider = provider(&host, config());
        for _ in 0..2 {
            assert_eq!(token(&provider.get_credential().await.unwrap()), "same");
        }
        assert_eq!(host.starts.load(Ordering::SeqCst), 2);
        assert!(lock(&host.cancellations).is_empty());
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio timer; FFI covered by consuming_ticket_on_any_thread_after_provider_free"
    )]
    async fn concurrent_calls_start_distinct_tickets_and_drop_cancels_only_first() {
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        let first = get(&provider);
        let (first_id, first_ticket) = take_ticket(&host).await;
        let second = get(&provider);
        let (second_id, second_ticket) = take_ticket(&host).await;
        assert_ne!(first_id, second_id);
        assert_eq!(host.starts.load(Ordering::SeqCst), 2);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(
            *lock(&host.cancellations),
            vec![(first_id, CANCEL_AWAITER_DROPPED)]
        );
        assert!(!complete(first_ticket, ignored_slice(), -1));
        assert!(complete(
            second_ticket,
            slice("independent"),
            expiry(120_000)
        ));
        assert_eq!(token(&second.await.unwrap().unwrap()), "independent");
        assert_eq!(provider.tickets.load(Ordering::SeqCst), 0);
    }
    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    #[cfg_attr(
        miri,
        ignore = "Tokio timer; FFI covered by consuming_ticket_on_any_thread_after_provider_free"
    )]
    async fn timeout_returns_while_foreign_ticket_or_claimed_sender_is_held(
        #[case] claim_sender: bool,
    ) {
        let host = Arc::new(Host::default());
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                acquisition_timeout_ms: 100,
                ..config()
            },
        );
        let counter = provider.tickets.clone();
        let waiter = get(&provider);
        let (id, ticket) = take_ticket(&host).await;
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let ticket = unsafe { ticket.into_inner() };
            assert!(ticket.request.live());
            let sender = claim_sender.then(|| ticket.request.claim().unwrap());
            ready_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if let Some(sender) = sender {
                assert!(sender.send(Err(FixedFailure::Abandoned)).is_err());
            }
            complete(ticket.into(), ignored_slice(), -1)
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(
            *lock(&host.cancellations),
            if claim_sender {
                vec![]
            } else {
                vec![(id, CANCEL_TIMEOUT)]
            }
        );
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        resume_tx.send(()).unwrap();
        assert!(!worker.join().unwrap());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }
}
