//! Synchronous Azure bearer acquisition with caller-owned caching, timeout, and cancellation.

use std::fmt;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::{self, CredentialProvider};
use delta_kernel::KernelError;
use delta_kernel_ffi_macros::handle_descriptor;

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{ExclusiveRustString, NullableCvoid};

/// Output initialized by the kernel to `has_token = 0` and zero expiry before acquisition.
#[repr(C)]
pub struct CAzureBearerToken {
    /// Set to 1 after initializing `token`. Any nonzero value transfers an initialized token;
    /// only 0 and 1 are accepted. With 0, the kernel never reads the uninitialized token field.
    pub has_token: u32,
    /// Set with `allocate_kernel_string` using the supplied allocator. The kernel consumes
    /// this owned handle on every callback status; do not reuse or free it after returning.
    pub token: Handle<ExclusiveRustString>,
    /// Positive UTC expiry in milliseconds since the Unix epoch, not a relative lifetime.
    pub expires_unix_ms: i64,
}

/// Acquires a token synchronously: 0 success, 1 transient failure, 2 permanent failure,
/// 3 cancelled. Unknown statuses produce a fixed error. The callback must release any
/// `allocate_kernel_string` allocation error itself and must not return exception text.
pub type CAzureCredentialCallback = extern "C" fn(
    context: NullableCvoid,
    out: *mut CAzureBearerToken,
    allocate_error: AllocateErrorFn,
) -> u32;

/// Configuration copied on successful creation, which starts no acquisition.
/// Callbacks/context must support concurrent calls on any thread and must not unwind or
/// reenter acquisition on the same provider. Acquisition may block the executor thread:
/// the caller owns caching, refresh, timeout, and cancellation; the kernel cannot preempt it.
/// Context ownership transfers only on success, until the final provider reference is dropped.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CAzureCredentialProviderConfig {
    /// Must be 2.
    pub abi_version: u32,
    /// Must equal sizeof(CAzureCredentialProviderConfig).
    pub struct_size: u32,
    /// Minimum remaining lifetime at callback return, 1 through 3600000 milliseconds.
    pub minimum_lifetime_ms: u32,
    /// Maximum bearer bytes, 1 through 65536.
    pub max_token_bytes: u32,
    /// Opaque caller context; null is allowed.
    pub context: NullableCvoid,
    /// Required synchronous acquisition callback; `out` is borrowed only for this call.
    pub acquire: Option<
        extern "C" fn(
            context: NullableCvoid,
            out: *mut CAzureBearerToken,
            allocate_error: AllocateErrorFn,
        ) -> u32,
    >,
    /// Required final context release callback, invoked without native locks.
    pub release: Option<extern "C" fn(context: NullableCvoid)>,
}

/// Bridge to the selected object_store Azure credential trait. Each lookup invokes the
/// synchronous callback directly, without native caching, retries, or a separate runtime.
pub struct AzureCredentialProvider {
    config: ValidatedContext,
    allocate_error: AllocateErrorFn,
}

/// Opaque shared provider descriptor. Free each owned reference exactly once.
#[handle_descriptor(target = AzureCredentialProvider, mutable = false, sized = true)]
pub struct SharedAzureCredentialProvider;

impl fmt::Debug for AzureCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AzureCredentialProvider { credentials: <redacted> }")
    }
}

#[async_trait]
impl CredentialProvider for AzureCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        self.collect()
            .map_err(|error| object_store::Error::Generic {
                store: "AzureCredentialProvider",
                source: Box::new(error),
            })
    }
}

impl AzureCredentialProvider {
    fn collect(&self) -> delta_kernel::Result<Arc<AzureCredential>> {
        let mut output = MaybeUninit::<CAzureBearerToken>::uninit();
        let out = output.as_mut_ptr();
        // SAFETY: Only the initialized flag/expiry and callback-initialized handle are read.
        let (status, has_token, token, expires_unix_ms) = unsafe {
            (*out).has_token = 0;
            (*out).expires_unix_ms = 0;
            let status = (self.config.acquire)(self.config.context, out, self.allocate_error);
            let has_token = (*out).has_token;
            let token = if has_token != 0 {
                Some(std::ptr::read(std::ptr::addr_of!((*out).token)).into_inner())
            } else {
                None
            };
            (status, has_token, token, (*out).expires_unix_ms)
        };
        let failure = match status {
            0 => None,
            1 => Some("transient Azure credential acquisition failure; caller may retry"),
            2 => Some("permanent Azure credential acquisition failure"),
            3 => Some("Azure credential acquisition cancelled"),
            _ => Some("invalid Azure credential acquisition status"),
        };
        if let Some(failure) = failure {
            return Err(KernelError::generic(failure));
        }
        if has_token > 1 {
            return Err(KernelError::generic(
                "invalid Azure bearer token presence flag",
            ));
        }
        let token = token.ok_or_else(|| KernelError::generic("missing Azure bearer token"))?;
        validate_token(&token, expires_unix_ms, &self.config)?;
        Ok(Arc::new(AzureCredential::BearerToken(*token)))
    }
}

/// Creates an owned provider reference or an allocated sanitized configuration error.
/// Validation failure invokes no callback and transfers no context ownership.
///
/// # Safety
/// `config` is null or addresses an aligned initialized version/size prefix. An accepted prefix
/// must address the full descriptor. Callbacks/context must obey the descriptor contract until
/// release. `allocate_error` must remain valid through final release and copy borrowed messages.
#[no_mangle]
pub unsafe extern "C" fn create_azure_credential_provider(
    config: *const CAzureCredentialProviderConfig,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedAzureCredentialProvider>> {
    // SAFETY: The caller provides the prefix/full descriptor required by validation.
    let result = unsafe { validate_config(config) }.map(|config| {
        Arc::new(AzureCredentialProvider {
            config,
            allocate_error,
        })
        .into()
    });
    // SAFETY: The caller's allocator copies this call's sanitized error message.
    unsafe { result.into_extern_result(&allocate_error) }
}

/// Unconditionally consumes a provider reference. Other engine/provider references and borrowed
/// acquisition futures keep the context alive; final release runs on the last reference's thread.
///
/// # Safety
/// `provider` must be valid, owned by the caller, and never used again after this call.
/// Do not free a reference while an acquisition borrows it without another retained reference.
#[no_mangle]
pub unsafe extern "C" fn free_azure_credential_provider(
    provider: Handle<SharedAzureCredentialProvider>,
) {
    // SAFETY: The caller transfers one owned provider reference.
    unsafe { provider.drop_handle() };
}

struct ValidatedContext {
    context: NullableCvoid,
    acquire: CAzureCredentialCallback,
    release: extern "C" fn(context: NullableCvoid),
    minimum_lifetime_ms: u32,
    max_token_bytes: u32,
}
// SAFETY: Creation requires concurrent, any-thread callbacks and opaque-context access.
unsafe impl Send for ValidatedContext {}
// SAFETY: The same caller contract permits shared callback/context access on any thread.
unsafe impl Sync for ValidatedContext {}
impl Drop for ValidatedContext {
    fn drop(&mut self) {
        (self.release)(self.context);
    }
}

unsafe fn validate_config(
    config: *const CAzureCredentialProviderConfig,
) -> delta_kernel::Result<ValidatedContext> {
    #[repr(C)]
    struct Prefix {
        abi_version: u32,
        struct_size: u32,
    }
    let invalid = || KernelError::generic("invalid Azure credential provider configuration");
    if config.is_null() {
        return Err(invalid());
    }
    // SAFETY: The caller supplies at least the aligned, initialized prefix.
    let prefix = unsafe { &*config.cast::<Prefix>() };
    if prefix.abi_version != 2
        || prefix.struct_size as usize != std::mem::size_of::<CAzureCredentialProviderConfig>()
    {
        return Err(KernelError::generic("invalid Azure credential provider configuration: expected ABI version 2 and exact size"));
    }
    // SAFETY: An accepted prefix requires the full initialized descriptor from the caller.
    let config = unsafe { &*config };
    if !(1..=3_600_000).contains(&config.minimum_lifetime_ms)
        || !(1..=65_536).contains(&config.max_token_bytes)
    {
        return Err(invalid());
    }
    let acquire = config.acquire.ok_or_else(invalid)?;
    let release = config.release.ok_or_else(invalid)?;
    Ok(ValidatedContext {
        context: config.context,
        acquire,
        release,
        minimum_lifetime_ms: config.minimum_lifetime_ms,
        max_token_bytes: config.max_token_bytes,
    })
}

fn validate_token(
    token: &str,
    expires_unix_ms: i64,
    config: &ValidatedContext,
) -> delta_kernel::Result<()> {
    let bytes = token.as_bytes();
    let padding = bytes
        .iter()
        .position(|byte| *byte == b'=')
        .unwrap_or(bytes.len());
    if bytes.is_empty()
        || bytes.len() > config.max_token_bytes as usize
        || padding == 0
        || !bytes[..padding]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/".contains(byte))
        || !bytes[padding..].iter().all(|byte| *byte == b'=')
    {
        return Err(KernelError::generic("invalid Azure bearer token payload"));
    }
    let invalid_expiry =
        || KernelError::generic("invalid Azure bearer token expiry or remaining lifetime");
    let expiry = u64::try_from(expires_unix_ms)
        .ok()
        .filter(|expiry| *expiry > 0)
        .ok_or_else(invalid_expiry)?;
    let expires_at = UNIX_EPOCH
        .checked_add(Duration::from_millis(expiry))
        .ok_or_else(invalid_expiry)?;
    let remaining = expires_at
        .duration_since(SystemTime::now())
        .map_err(|_| invalid_expiry())?;
    if remaining < Duration::from_millis(config.minimum_lifetime_ms.into()) {
        return Err(invalid_expiry());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Mutex};
    use std::task::{Context as TaskContext, Poll, Waker};

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic, recover_error};
    use crate::{allocate_kernel_string, kernel_string_slice, KernelStringSlice};

    struct Host {
        releases: AtomicUsize,
        acquisitions: AtomicUsize,
        allocate_error: AllocateErrorFn,
        token: Option<String>,
        expiry: i64,
        status: u32,
        presence: u32,
        gate: Option<(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>)>,
    }
    impl Default for Host {
        fn default() -> Self {
            Self {
                releases: AtomicUsize::new(0),
                acquisitions: AtomicUsize::new(0),
                allocate_error: allocate_err,
                token: Some("opaque".into()),
                expiry: expiry(120_000),
                status: 0,
                presence: 1,
                gate: None,
            }
        }
    }
    extern "C" fn acquire(
        context: NullableCvoid,
        out: *mut CAzureBearerToken,
        allocator: AllocateErrorFn,
    ) -> u32 {
        // SAFETY: The provider retains Host; the output flag and expiry are initialized.
        let host = unsafe { &*context.unwrap().as_ptr().cast::<Host>() };
        assert_eq!(unsafe { (*out).has_token }, 0);
        assert_eq!(unsafe { (*out).expires_unix_ms }, 0);
        assert!(std::ptr::fn_addr_eq(allocator, host.allocate_error));
        host.acquisitions.fetch_add(1, Ordering::SeqCst);
        if let Some((entered, resume)) = &host.gate {
            entered.send(()).unwrap();
            resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        // SAFETY: A nonzero presence flag transfers this initialized exclusive handle.
        unsafe {
            if let Some(token) = &host.token {
                (*out).token = ok_or_panic(allocate_kernel_string(
                    kernel_string_slice!(token),
                    allocator,
                ));
                (*out).has_token = host.presence;
            }
            (*out).expires_unix_ms = host.expiry;
        }
        host.status
    }
    extern "C" fn release(context: NullableCvoid) {
        // SAFETY: Successful creation transfers exactly one Arc reference to Release.
        let host = unsafe { Arc::from_raw(context.unwrap().as_ptr().cast::<Host>()) };
        host.releases.fetch_add(1, Ordering::SeqCst);
    }
    fn config() -> CAzureCredentialProviderConfig {
        CAzureCredentialProviderConfig {
            abi_version: 2,
            struct_size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
            minimum_lifetime_ms: 1,
            max_token_bytes: 65_536,
            context: None,
            acquire: Some(acquire),
            release: Some(release),
        }
    }
    fn create(
        host: &Arc<Host>,
        mut config: CAzureCredentialProviderConfig,
    ) -> Handle<SharedAzureCredentialProvider> {
        config.context = NonNull::new(Arc::into_raw(host.clone()).cast_mut().cast());
        ok_or_panic(unsafe { create_azure_credential_provider(&config, host.allocate_error) })
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
    fn assert_error<T>(result: ExternResult<T>) {
        let ExternResult::Err(error) = result else {
            panic!("expected sanitized error");
        };
        let error = unsafe { recover_error(error) };
        assert!(error
            .message
            .contains("invalid Azure credential provider configuration"));
    }
    fn token(credential: &AzureCredential) -> &str {
        let AzureCredential::BearerToken(token) = credential else {
            panic!("expected bearer");
        };
        token
    }
    fn get(provider: &AzureCredentialProvider) -> object_store::Result<Arc<AzureCredential>> {
        match provider
            .get_credential()
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("synchronous acquisition must finish in its first poll"),
        }
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    fn configuration_limits_shared_handles_and_redacted_debug(#[case] maximum: bool) {
        let host = Arc::new(Host::default());
        let mut config = config();
        if maximum {
            config.minimum_lifetime_ms = 3_600_000;
        } else {
            config.max_token_bytes = 1;
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
        assert_eq!(host.acquisitions.load(Ordering::SeqCst), 0);
    }
    #[rstest::rstest]
    #[case(0, 0)]
    #[case(0, 1)]
    #[case(1, 0)]
    #[case(1, u32::MAX)]
    #[case(2, 0)]
    #[case(2, 3_600_001)]
    #[case(3, 0)]
    #[case(3, 65_537)]
    #[case(4, 0)]
    #[case(5, 0)]
    fn invalid_configuration_never_takes_context(#[case] field: u32, #[case] value: u32) {
        let host = Arc::new(Host::default());
        let mut config = config();
        config.context = Some(NonNull::from(host.as_ref()).cast());
        match field {
            0 => config.abi_version = value,
            1 => config.struct_size = value,
            2 => config.minimum_lifetime_ms = value,
            3 => config.max_token_bytes = value,
            4 => config.acquire = None,
            5 => config.release = None,
            _ => unreachable!(),
        }
        assert_error(unsafe { create_azure_credential_provider(&config, allocate_err) });
        assert_eq!(host.acquisitions.load(Ordering::SeqCst), 0);
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
        assert_error(unsafe { create_azure_credential_provider(std::ptr::null(), allocate_err) });
        for prefix in [
            Prefix {
                version: 1,
                size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
            },
            Prefix {
                version: 2,
                size: 8,
            },
        ] {
            assert_error(unsafe {
                create_azure_credential_provider(std::ptr::from_ref(&prefix).cast(), allocate_err)
            });
        }
    }
    #[rstest::rstest]
    #[case("", false)]
    #[case("=", false)]
    #[case("===", false)]
    #[case("secret\r\nheader", false)]
    #[case("bad space", false)]
    #[case("bad:colon", false)]
    #[case("bad=middle", false)]
    #[case("\u{e9}", false)]
    #[case("aZ09-._~+/===", true)]
    fn owned_tokens_validate_bearer_syntax_and_release_once(
        #[case] bearer: &str,
        #[case] valid: bool,
    ) {
        let host = Arc::new(Host {
            token: Some(bearer.into()),
            ..Host::default()
        });
        let provider = provider(&host, config());
        let result = provider.collect();
        if valid {
            assert_eq!(token(&result.unwrap()), bearer);
        } else {
            assert_eq!(
                result.unwrap_err().to_string(),
                KernelError::generic("invalid Azure bearer token payload").to_string()
            );
        }
        drop(provider);
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
    }
    #[rstest::rstest]
    #[case(4, 4, true)]
    #[case(4, 5, false)]
    #[case(65_536, 65_536, true)]
    fn exact_owned_token_limits(#[case] limit: u32, #[case] length: usize, #[case] valid: bool) {
        let host = Arc::new(Host {
            token: Some("a".repeat(length)),
            ..Host::default()
        });
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                max_token_bytes: limit,
                ..config()
            },
        );
        assert_eq!(provider.collect().is_ok(), valid);
    }
    #[rstest::rstest]
    #[case(-1, false)]
    #[case(0, false)]
    #[case(1, false)]
    #[case(expiry(500), false)]
    #[case(expiry(120_000), true)]
    #[case(i64::MAX, UNIX_EPOCH.checked_add(Duration::from_millis(i64::MAX as u64)).is_some())]
    fn utc_expiry_and_minimum_lifetime_are_checked(#[case] expires: i64, #[case] valid: bool) {
        let host = Arc::new(Host {
            expiry: expires,
            ..Host::default()
        });
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                minimum_lifetime_ms: 1_000,
                ..config()
            },
        );
        assert_eq!(provider.collect().is_ok(), valid);
    }
    #[rstest::rstest]
    #[case(0, 1, "")]
    #[case(1, 1, "transient")]
    #[case(2, 1, "permanent")]
    #[case(3, 1, "cancelled")]
    #[case(u32::MAX, 1, "invalid Azure credential acquisition status")]
    #[case(0, 2, "invalid Azure bearer token presence flag")]
    #[case(1, 2, "transient")]
    fn returned_token_is_consumed_on_every_status(
        #[case] status: u32,
        #[case] presence: u32,
        #[case] expected: &str,
    ) {
        let host = Arc::new(Host {
            status,
            presence,
            ..Host::default()
        });
        let provider = provider(&host, config());
        let result = get(&provider);
        if status == 0 && presence == 1 {
            assert_eq!(token(&result.unwrap()), "opaque");
        } else {
            let message = result.unwrap_err().to_string();
            assert!(message.contains(expected));
            assert!(!message.contains("opaque"));
        }
    }
    #[test]
    fn success_without_token_is_missing_and_successive_lookups_call_again() {
        let missing = Arc::new(Host {
            token: None,
            ..Host::default()
        });
        assert!(get(&provider(&missing, config()))
            .unwrap_err()
            .to_string()
            .contains("missing"));
        let host = Arc::new(Host::default());
        let provider = provider(&host, config());
        for _ in 0..2 {
            assert_eq!(token(&get(&provider).unwrap()), "opaque");
        }
        assert_eq!(host.acquisitions.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn callback_releases_invalid_utf8_allocation_error_and_returns_fixed_failure() {
        extern "C" fn invalid_utf8(
            _: NullableCvoid,
            _: *mut CAzureBearerToken,
            allocator: AllocateErrorFn,
        ) -> u32 {
            let bytes = [0xff];
            let result = unsafe {
                allocate_kernel_string(
                    KernelStringSlice {
                        ptr: bytes.as_ptr().cast(),
                        len: bytes.len(),
                    },
                    allocator,
                )
            };
            let ExternResult::Err(error) = result else {
                panic!("expected UTF-8 allocation error")
            };
            unsafe { recover_error(error) };
            2
        }
        let host = Arc::new(Host::default());
        let provider = provider(
            &host,
            CAzureCredentialProviderConfig {
                acquire: Some(invalid_utf8),
                ..config()
            },
        );
        assert!(get(&provider)
            .unwrap_err()
            .to_string()
            .contains("permanent"));
    }
    #[rstest::rstest]
    #[case(1)]
    #[case(2)]
    fn blocked_concurrent_callbacks_retain_context_after_caller_handle_free(#[case] calls: usize) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let host = Arc::new(Host {
            gate: Some((entered_tx, Mutex::new(resume_rx))),
            ..Host::default()
        });
        let handle = create(&host, config());
        let workers: Vec<_> = (0..calls)
            .map(|_| {
                let provider = unsafe { handle.clone_as_arc() };
                std::thread::spawn(move || get(&provider).unwrap())
            })
            .collect();
        for _ in 0..calls {
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        unsafe { free_azure_credential_provider(handle) };
        assert_eq!(host.releases.load(Ordering::SeqCst), 0);
        assert_eq!(host.acquisitions.load(Ordering::SeqCst), calls);
        for _ in 0..calls {
            resume_tx.send(()).unwrap();
        }
        for worker in workers {
            assert_eq!(token(&worker.join().unwrap()), "opaque");
        }
        assert_eq!(host.releases.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&host), 1);
    }
}
