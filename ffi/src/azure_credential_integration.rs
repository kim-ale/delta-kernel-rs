use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::CredentialProvider;
use delta_kernel::StorageHandler;
use rstest::rstest;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use url::Url;
use wiremock::matchers::{body_bytes, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::azure_credentials::{
    create_azure_credential_provider, free_azure_credential_provider, CAzureBearerToken,
    CAzureCredentialProviderConfig, SharedAzureCredentialProvider,
};
use crate::error::{AllocateErrorFn, FFIKernelError};
use crate::ffi_test_utils::{
    allocate_err, assert_extern_result_error_contains, error_only_engine_handle, ok_or_panic,
};
use crate::handle::Handle;
use crate::rest_engine::CRestEndpointConfig;
use crate::{
    allocate_kernel_string, builder_build, builder_with_azure_credential_provider,
    builder_with_multithreaded_executor, builder_with_option, builder_with_rest_object_store,
    free_engine, free_engine_builder, get_engine_builder, kernel_string_slice,
    ExclusiveEngineBuilder, NullableCvoid, SharedExternEngine,
};

mod table_operations;

const TABLE_URL: &str = "abfss://container@account.dfs.core.windows.net/table/";

#[derive(Default)]
struct CallbackCounts {
    starts: AtomicUsize,
    refreshes: AtomicUsize,
    releases: AtomicUsize,
    first_expiry_ms: AtomicI64,
    first_lifetime_ms: AtomicI64,
}

#[derive(Clone, Copy, Debug)]
enum CallbackOutcome {
    Missing,
    Fail,
    Complete,
    Renew,
}

struct CallbackContext {
    counts: Arc<CallbackCounts>,
    outcome: CallbackOutcome,
    token: Mutex<Option<(&'static str, i64)>>,
}

extern "C" fn acquire_token(
    context: NullableCvoid,
    out: *mut CAzureBearerToken,
    allocate_error: AllocateErrorFn,
) -> u32 {
    let context = unsafe { &*context.unwrap().as_ptr().cast::<CallbackContext>() };
    let counts = context.counts.clone();
    let outcome = context.outcome;
    counts.starts.fetch_add(1, Ordering::SeqCst);
    if matches!(outcome, CallbackOutcome::Missing) {
        return 0;
    }
    let mut cached = context.token.lock().unwrap();
    let (token, expiry) = if matches!(outcome, CallbackOutcome::Fail) {
        ("token-A", 0)
    } else if let Some((token, expiry)) = (*cached).filter(|(_, expiry)| *expiry - unix_ms() >= 1) {
        (token, expiry)
    } else {
        let generation = counts.refreshes.fetch_add(1, Ordering::SeqCst) + 1;
        let (token, lifetime_ms) = match (outcome, generation) {
            (CallbackOutcome::Renew, 1) => {
                let configured = counts.first_lifetime_ms.load(Ordering::SeqCst);
                ("token-A", if configured > 0 { configured } else { 1_000 })
            }
            (CallbackOutcome::Renew, _) => ("token-B", 60_000),
            _ => ("token-A", 60_000),
        };
        let expiry = unix_ms() + lifetime_ms;
        if generation == 1 {
            counts.first_expiry_ms.store(expiry, Ordering::SeqCst);
        }
        *cached = Some((token, expiry));
        (token, expiry)
    };
    drop(cached);
    // SAFETY: The callback initializes the token before setting its presence flag.
    unsafe {
        (*out).token = ok_or_panic(allocate_kernel_string(
            kernel_string_slice!(token),
            allocate_error,
        ));
        (*out).has_token = 1;
        (*out).expires_unix_ms = expiry;
    }
    if matches!(outcome, CallbackOutcome::Fail) {
        1
    } else {
        0
    }
}

extern "C" fn release_context(context: NullableCvoid) {
    let context = unsafe { Box::from_raw(context.unwrap().as_ptr().cast::<CallbackContext>()) };
    context.counts.releases.fetch_add(1, Ordering::SeqCst);
}

fn create_provider(counts: &Arc<CallbackCounts>) -> Handle<SharedAzureCredentialProvider> {
    create_provider_with_outcome(counts, CallbackOutcome::Missing)
}

fn create_provider_with_outcome(
    counts: &Arc<CallbackCounts>,
    outcome: CallbackOutcome,
) -> Handle<SharedAzureCredentialProvider> {
    let context = Box::new(CallbackContext {
        counts: counts.clone(),
        outcome,
        token: Mutex::new(None),
    });
    let config = CAzureCredentialProviderConfig {
        abi_version: 2,
        struct_size: std::mem::size_of::<CAzureCredentialProviderConfig>() as u32,
        minimum_lifetime_ms: 1,
        max_token_bytes: 256,
        context: NonNull::new(Box::into_raw(context).cast()),
        acquire: Some(acquire_token),
        release: Some(release_context),
    };
    ok_or_panic(unsafe { create_azure_credential_provider(&config, allocate_err) })
}

fn engine_builder(url: &str) -> Handle<ExclusiveEngineBuilder> {
    ok_or_panic(unsafe { get_engine_builder(kernel_string_slice!(url), allocate_err) })
}

fn with_provider(
    builder: Handle<ExclusiveEngineBuilder>,
    provider: &Handle<SharedAzureCredentialProvider>,
) -> Handle<ExclusiveEngineBuilder> {
    ok_or_panic(unsafe { builder_with_azure_credential_provider(builder, provider) })
}

fn with_option(
    builder: Handle<ExclusiveEngineBuilder>,
    key: &str,
    value: &str,
) -> Handle<ExclusiveEngineBuilder> {
    ok_or_panic(unsafe {
        builder_with_option(
            builder,
            kernel_string_slice!(key),
            kernel_string_slice!(value),
        )
    })
}

fn assert_counts(counts: &CallbackCounts, starts: usize, releases: usize) {
    assert_eq!(counts.starts.load(Ordering::SeqCst), starts);
    assert_eq!(counts.releases.load(Ordering::SeqCst), releases);
}

fn assert_refreshes(counts: &CallbackCounts, refreshes: usize, releases: usize) {
    assert_eq!(counts.refreshes.load(Ordering::SeqCst), refreshes);
    assert_eq!(counts.releases.load(Ordering::SeqCst), releases);
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn rest_config() -> CRestEndpointConfig {
    let empty = "";
    let page_token = "page_token";
    let start_from = "start_from";
    let recursive = "recursive";
    let overwrite = "overwrite";
    let contents = "contents";
    let next_page_token = "next_page_token";
    let path = "path";
    let size = "size";
    let is_directory = "is_directory";
    let last_modified = "last_modified";
    CRestEndpointConfig {
        files_prefix: kernel_string_slice!(empty),
        directories_prefix: kernel_string_slice!(empty),
        page_token_param: kernel_string_slice!(page_token),
        start_from_param: kernel_string_slice!(start_from),
        recursive_param: kernel_string_slice!(recursive),
        overwrite_param: kernel_string_slice!(overwrite),
        contents_field: kernel_string_slice!(contents),
        next_page_token_field: kernel_string_slice!(next_page_token),
        entry_path_field: kernel_string_slice!(path),
        entry_size_field: kernel_string_slice!(size),
        entry_is_directory_field: kernel_string_slice!(is_directory),
        entry_last_modified_field: kernel_string_slice!(last_modified),
        entry_strip_prefix: kernel_string_slice!(empty),
    }
}

#[rstest]
fn caller_provider_free_leaves_builder_reference_until_abandonment(
    #[values(false, true)] multithreaded: bool,
) {
    let counts = Arc::new(CallbackCounts::default());
    let provider = create_provider(&counts);
    let mut builder = with_provider(engine_builder(TABLE_URL), &provider);
    if multithreaded {
        builder = unsafe { builder_with_multithreaded_executor(builder, 2, 2) };
    }
    unsafe { free_azure_credential_provider(provider) };
    assert_counts(&counts, 0, 0);
    unsafe { free_engine_builder(builder) };
    assert_counts(&counts, 0, 1);
}

#[rstest]
fn owned_callbacks_consume_token_on_success_and_failure(
    #[values(
        CallbackOutcome::Complete,
        CallbackOutcome::Fail,
        CallbackOutcome::Missing
    )]
    outcome: CallbackOutcome,
) {
    let counts = Arc::new(CallbackCounts::default());
    let provider = create_provider_with_outcome(&counts, outcome);
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let result = runtime.block_on(unsafe { provider.as_ref() }.get_credential());
    match outcome {
        CallbackOutcome::Complete => assert!(matches!(
            result.unwrap().as_ref(),
            AzureCredential::BearerToken(token) if token == "token-A"
        )),
        CallbackOutcome::Fail => {
            assert!(result.unwrap_err().to_string().contains("transient"));
        }
        CallbackOutcome::Missing => {
            assert!(result.unwrap_err().to_string().contains("missing"));
        }
        CallbackOutcome::Renew => unreachable!(),
    }
    assert_counts(&counts, 1, 0);
    unsafe { free_azure_credential_provider(provider) };
    assert_counts(&counts, 1, 1);
}

#[test]
fn engine_handle_borrow_and_free_keep_ownership() {
    let engine = error_only_engine_handle();
    assert!(std::ptr::eq(unsafe { engine.as_ref() }, unsafe {
        engine.as_ref()
    }));
    unsafe { free_engine(engine) };
}

fn http_runtime() -> Runtime {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn build_azure_engine(
    endpoint: &str,
    counts: &Arc<CallbackCounts>,
    outcome: CallbackOutcome,
    multithreaded: bool,
) -> Handle<SharedExternEngine> {
    let provider = create_provider_with_outcome(counts, outcome);
    let builder = with_provider(engine_builder(TABLE_URL), &provider);
    unsafe { free_azure_credential_provider(provider) };
    assert_counts(counts, 0, 0);
    let builder = with_option(builder, "azure_endpoint", endpoint);
    let builder = with_option(builder, "allow_http", "true");
    let mut builder = with_option(builder, "azure_timeout", "2s");
    if multithreaded {
        builder = unsafe { builder_with_multithreaded_executor(builder, 2, 2) };
    }
    let engine = ok_or_panic(unsafe { builder_build(builder) });
    assert_counts(counts, 0, 0);
    engine
}

fn exercise_head_read_and_put(storage: &dyn StorageHandler, file: &Url) {
    let metadata = storage.head(file).unwrap();
    assert_eq!(metadata.location, *file);
    assert_eq!(metadata.size, 4);
    let data: Vec<_> = storage
        .read_files(vec![(file.clone(), None)])
        .unwrap()
        .collect::<delta_kernel::Result<_>>()
        .unwrap();
    assert_eq!(data, vec![Bytes::from_static(b"data")]);
    storage
        .put(file, Bytes::from_static(b"written"), false)
        .unwrap();
    assert_eq!(storage.head(file).unwrap().size, 4);
}

#[rstest]
#[cfg_attr(
    miri,
    ignore = "HTTP/Tokio runtimes; unsafe covered by owned_callbacks_consume_token_on_success_and_failure, caller_provider_free_leaves_builder_reference_until_abandonment, rejected_build_consumes_builder_and_releases_provider_without_acquisition, engine_handle_borrow_and_free_keep_ownership, tests::engine_builder"
)]
fn same_ffi_engine_renews_authorization_for_real_head_read_and_put(
    #[values(false, true)] multithreaded: bool,
) {
    let runtime = http_runtime();
    let server = runtime.block_on(MockServer::start());
    runtime.block_on(async {
        for token in ["token-A", "token-B"] {
            let authorization = format!("Bearer {token}");
            for verb in ["HEAD", "GET"] {
                Mock::given(method(verb))
                    .and(path("/prefix/container/table/blob"))
                    .and(header("authorization", authorization.as_str()))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_bytes(b"data".to_vec())
                            .insert_header("etag", "\"test-etag\"")
                            .insert_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                    )
                    .expect(if verb == "HEAD" { 2 } else { 1 })
                    .mount(&server)
                    .await;
            }
            Mock::given(method("PUT"))
                .and(path("/prefix/container/table/blob"))
                .and(header("authorization", authorization.as_str()))
                .and(header("if-none-match", "*"))
                .and(body_bytes(b"written"))
                .respond_with(
                    ResponseTemplate::new(201)
                        .insert_header("etag", "\"written-etag\"")
                        .insert_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                )
                .expect(1)
                .mount(&server)
                .await;
        }
    });
    let counts = Arc::new(CallbackCounts::default());
    let engine = build_azure_engine(
        &format!("{}/prefix", server.uri()),
        &counts,
        CallbackOutcome::Renew,
        multithreaded,
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let storage = kernel_engine.storage_handler();
    let file = Url::parse(TABLE_URL).unwrap().join("blob").unwrap();
    exercise_head_read_and_put(storage.as_ref(), &file);
    assert_counts(&counts, 4, 0);
    assert_refreshes(&counts, 1, 0);
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    let wait_ms = (counts.first_expiry_ms.load(Ordering::SeqCst) - unix_ms()).max(0) as u64 + 25;
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(wait_ms)).await });
    assert_counts(&counts, 4, 0);
    assert_refreshes(&counts, 1, 0);
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        4
    );
    exercise_head_read_and_put(storage.as_ref(), &file);
    assert_counts(&counts, 8, 0);
    assert_refreshes(&counts, 2, 0);
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    runtime.block_on(server.verify());
    let requests = runtime.block_on(server.received_requests()).unwrap();
    let authorization: Vec<_> = requests
        .iter()
        .map(|request| {
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect();
    assert_eq!(
        authorization,
        ["Bearer token-A"; 4]
            .into_iter()
            .chain(["Bearer token-B"; 4])
            .collect::<Vec<_>>()
    );
    drop(storage);
    drop(kernel_engine);
    unsafe { free_engine(engine) };
    assert_counts(&counts, 8, 1);
    assert_refreshes(&counts, 2, 1);
}

#[rstest]
#[cfg_attr(
    miri,
    ignore = "HTTP/Tokio runtimes; unsafe covered by owned_callbacks_consume_token_on_success_and_failure, caller_provider_free_leaves_builder_reference_until_abandonment, rejected_build_consumes_builder_and_releases_provider_without_acquisition, engine_handle_borrow_and_free_keep_ownership, tests::engine_builder"
)]
fn failed_acquisition_sends_no_http_and_only_retries_on_new_operation(
    #[values(false, true)] multithreaded: bool,
    #[values(CallbackOutcome::Fail, CallbackOutcome::Missing)] outcome: CallbackOutcome,
) {
    let runtime = http_runtime();
    let server = runtime.block_on(MockServer::start());
    let counts = Arc::new(CallbackCounts::default());
    let engine = build_azure_engine(&server.uri(), &counts, outcome, multithreaded);
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let storage = kernel_engine.storage_handler();
    let file = Url::parse(TABLE_URL).unwrap().join("blob").unwrap();
    let expected_error = match outcome {
        CallbackOutcome::Fail => "transient",
        CallbackOutcome::Missing => "missing",
        _ => unreachable!(),
    };
    for attempt in 1..=2 {
        assert!(storage
            .head(&file)
            .unwrap_err()
            .to_string()
            .contains(expected_error));
        assert_counts(&counts, attempt, 0);
        assert!(runtime
            .block_on(server.received_requests())
            .unwrap()
            .is_empty());
    }
    drop(storage);
    drop(kernel_engine);
    unsafe { free_engine(engine) };
    assert_counts(&counts, 2, 1);
}

#[test]
fn provider_replacement_releases_old_reference_and_retains_new_reference() {
    let old_counts = Arc::new(CallbackCounts::default());
    let new_counts = Arc::new(CallbackCounts::default());
    let old_provider = create_provider(&old_counts);
    let new_provider = create_provider(&new_counts);
    let builder = with_provider(engine_builder(TABLE_URL), &old_provider);
    unsafe { free_azure_credential_provider(old_provider) };
    assert_counts(&old_counts, 0, 0);
    let builder = with_provider(builder, &new_provider);
    assert_counts(&old_counts, 0, 1);
    unsafe { free_azure_credential_provider(new_provider) };
    assert_counts(&new_counts, 0, 0);
    unsafe { free_engine_builder(builder) };
    assert_counts(&old_counts, 0, 1);
    assert_counts(&new_counts, 0, 1);
}

#[rstest]
#[case::non_azure("memory:///", None, "require an Azure storage URL")]
#[case::competing_auth(TABLE_URL, Some(("bearer_token", "private-test-token")), "conflicts with")]
#[case::unsigned(TABLE_URL, Some(("skip_signature", "true")), "to be false")]
#[case::emulator(TABLE_URL, Some(("use_emulator", "true")), "to be false")]
fn rejected_build_consumes_builder_and_releases_provider_without_acquisition(
    #[case] url: &str,
    #[case] option: Option<(&str, &str)>,
    #[case] expected_error: &str,
) {
    let counts = Arc::new(CallbackCounts::default());
    let provider = create_provider(&counts);
    let mut builder = with_provider(engine_builder(url), &provider);
    if let Some((key, value)) = option {
        builder = with_option(builder, key, value);
    }
    unsafe { free_azure_credential_provider(provider) };
    assert_counts(&counts, 0, 0);
    assert_extern_result_error_contains(
        unsafe { builder_build(builder) },
        FFIKernelError::GenericError,
        expected_error,
    );
    assert_counts(&counts, 0, 1);
}

#[rstest]
fn rest_conflict_in_either_setter_order_consumes_builder_but_borrows_provider(
    #[values(false, true)] provider_first: bool,
) {
    let counts = Arc::new(CallbackCounts::default());
    let provider = create_provider(&counts);
    let builder = engine_builder("http://localhost/");
    let config = rest_config();
    if provider_first {
        let builder = with_provider(builder, &provider);
        unsafe { free_azure_credential_provider(provider) };
        assert_counts(&counts, 0, 0);
        assert_extern_result_error_contains(
            unsafe { builder_with_rest_object_store(builder, &config, None, None) },
            FFIKernelError::GenericError,
            "REST storage conflicts with an Azure credential provider",
        );
    } else {
        let builder =
            ok_or_panic(unsafe { builder_with_rest_object_store(builder, &config, None, None) });
        assert_extern_result_error_contains(
            unsafe { builder_with_azure_credential_provider(builder, &provider) },
            FFIKernelError::GenericError,
            "Azure credential provider conflicts with REST storage",
        );
        assert_counts(&counts, 0, 0);
        unsafe { free_azure_credential_provider(provider) };
    }
    assert_counts(&counts, 0, 1);
}
