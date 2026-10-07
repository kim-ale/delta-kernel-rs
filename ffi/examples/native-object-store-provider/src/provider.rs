use std::ffi::c_void;
use std::mem::{forget, size_of};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use delta_kernel_native_store_abi::{
    KernelNativeByteSliceV1, KernelNativeObjectMetaV1, KernelNativeObjectStoreDescriptorV1,
    KernelNativeStringSliceV1, KERNEL_NATIVE_GET_FULL, KERNEL_NATIVE_GET_HEAD,
    KERNEL_NATIVE_PUT_CREATE, KERNEL_NATIVE_PUT_OVERWRITE, KERNEL_NATIVE_STATUS_ALREADY_EXISTS,
    KERNEL_NATIVE_STATUS_GENERIC, KERNEL_NATIVE_STATUS_NOT_FOUND,
    KERNEL_NATIVE_STATUS_NOT_SUPPORTED, KERNEL_NATIVE_STATUS_OK, KERNEL_NATIVE_STORE_ABI_V1,
};
use futures::TryStreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
};
use tokio::runtime::{Builder, Runtime};

use crate::credentials::CustomCredentialProvider;

const MAX_PAGE_ITEMS: u32 = 1024;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const INITIAL_PATH: &str = "table/_delta_log/00000000000000000000.json";
const APPEND_PATH: &str = "table/_delta_log/00000000000000000001.json";
const INITIAL_COMMIT: &str = concat!(
    "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
    "{\"metaData\":{\"id\":\"00000000-0000-0000-0000-000000000001\",",
    "\"format\":{\"provider\":\"parquet\",\"options\":{}},",
    "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[",
    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",",
    "\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",",
    "\"partitionColumns\":[],\"configuration\":{},\"createdTime\":0}}\n",
);
const APPEND_COMMIT: &str = "{\"commitInfo\":{\"timestamp\":1}}\n";

static RUNTIME: OnceLock<Result<Runtime, i32>> = OnceLock::new();
static RELEASES: AtomicU64 = AtomicU64::new(0);
static GETS: AtomicU64 = AtomicU64::new(0);
static LISTS: AtomicU64 = AtomicU64::new(0);
static PUTS: AtomicU64 = AtomicU64::new(0);
static DELETES: AtomicU64 = AtomicU64::new(0);

pub(crate) struct ProviderContext {
    store: Arc<dyn ObjectStore>,
    memory: bool,
}

pub(crate) fn create_memory() -> Result<ProviderContext, i32> {
    let context = ProviderContext {
        store: Arc::new(InMemory::new()),
        memory: true,
    };
    runtime()?
        .block_on(context.store.put_opts(
            &Path::from(INITIAL_PATH),
            Bytes::from_static(INITIAL_COMMIT.as_bytes()).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ))
        .map_err(error_status)?;
    Ok(context)
}

pub(crate) fn create_azure(endpoint: String) -> Result<ProviderContext, i32> {
    let _entered = runtime()?.enter();
    let store = MicrosoftAzureBuilder::new()
        .with_account("account")
        .with_container_name("container")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_credentials(Arc::new(CustomCredentialProvider))
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .map_err(error_status)?;
    Ok(ProviderContext {
        store: Arc::new(store),
        memory: false,
    })
}

pub(crate) fn descriptor(context: ProviderContext) -> KernelNativeObjectStoreDescriptorV1 {
    KernelNativeObjectStoreDescriptorV1 {
        abi_version: KERNEL_NATIVE_STORE_ABI_V1,
        struct_size: size_of::<KernelNativeObjectStoreDescriptorV1>() as u32,
        context: Box::into_raw(Box::new(context)).cast(),
        get: Some(get),
        list: Some(list),
        put: Some(put),
        delete_object: Some(delete_object),
        release: Some(release),
    }
}

pub(crate) unsafe fn append_commit(context: *mut c_void) -> Result<(), i32> {
    // SAFETY: The caller holds a live provider context for the entire call.
    let context = unsafe { context_ref(context) }?;
    if !context.memory {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    runtime()?
        .block_on(context.store.put_opts(
            &Path::from(APPEND_PATH),
            Bytes::from_static(APPEND_COMMIT.as_bytes()).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ))
        .map(|_| ())
        .map_err(error_status)
}

pub(crate) fn release_count() -> u64 {
    RELEASES.load(Ordering::SeqCst)
}

pub(crate) fn callback_count() -> u64 {
    GETS.load(Ordering::SeqCst)
        .saturating_add(LISTS.load(Ordering::SeqCst))
        .saturating_add(PUTS.load(Ordering::SeqCst))
        .saturating_add(DELETES.load(Ordering::SeqCst))
}

pub(crate) fn guarded(operation: impl FnOnce() -> Result<(), i32>) -> i32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => KERNEL_NATIVE_STATUS_OK,
        Ok(Err(status)) => status,
        Err(payload) => {
            forget(payload);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

pub(crate) unsafe fn copy_string(value: KernelNativeStringSliceV1) -> Result<String, i32> {
    // SAFETY: The caller guarantees readable bytes through this synchronous copy.
    let bytes = unsafe {
        copy_bytes(KernelNativeByteSliceV1 {
            ptr: value.ptr.cast(),
            len: value.len,
        })
    }?;
    String::from_utf8(bytes).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)
}

unsafe extern "C" fn get(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    flags: u32,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(
        *mut c_void,
        *const KernelNativeObjectMetaV1,
        KernelNativeByteSliceV1,
    ) -> i32,
) -> i32 {
    guarded(|| {
        GETS.fetch_add(1, Ordering::SeqCst);
        let head = match flags {
            KERNEL_NATIVE_GET_FULL => false,
            KERNEL_NATIVE_GET_HEAD => true,
            _ => return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED),
        };
        // SAFETY: Kernel retains the provider context and input through callback return.
        let (context, path) = unsafe { (context_ref(context)?, copy_string(path)?) };
        let path = Path::parse(path).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let (object_metadata, body) = runtime()?.block_on(async {
            let result = context
                .store
                .get_opts(
                    &path,
                    GetOptions {
                        head,
                        ..Default::default()
                    },
                )
                .await
                .map_err(error_status)?;
            let metadata = result.meta.clone();
            let body = if head {
                Bytes::new()
            } else {
                bounded_body(result, MAX_BODY_BYTES).await?
            };
            Ok::<_, i32>((metadata, body))
        })?;
        let metadata = borrowed_metadata(&object_metadata);
        // SAFETY: All output storage lives through this one synchronous sink call; nothing is
        // retained.
        let status = unsafe {
            sink(
                sink_context,
                &metadata,
                KernelNativeByteSliceV1 {
                    ptr: body.as_ptr(),
                    len: body.len(),
                },
            )
        };
        sink_status(status)
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "The frozen v1 C ABI fixes this signature."
)]
unsafe extern "C" fn list(
    context: *mut c_void,
    prefix: KernelNativeStringSliceV1,
    start_after: KernelNativeStringSliceV1,
    max_items: u32,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV1) -> i32,
    has_more: *mut u32,
) -> i32 {
    guarded(|| {
        LISTS.fetch_add(1, Ordering::SeqCst);
        if has_more.is_null() || !(1..=MAX_PAGE_ITEMS).contains(&max_items) {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: Kernel retains the provider context and input slices through callback return.
        let (context, prefix, start_after) = unsafe {
            (
                context_ref(context)?,
                copy_string(prefix)?,
                copy_string(start_after)?,
            )
        };
        let native_prefix = if prefix.is_empty() {
            None
        } else {
            Some(Path::parse(&prefix).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?)
        };
        if !start_after.is_empty() {
            Path::parse(&start_after).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        }
        let mut objects = runtime()?
            .block_on(
                context
                    .store
                    .list(native_prefix.as_ref())
                    .try_collect::<Vec<_>>(),
            )
            .map_err(error_status)?;
        objects.sort_unstable_by(|left, right| left.location.cmp(&right.location));
        let mut page: Vec<_> = objects
            .into_iter()
            .filter(|metadata| {
                metadata.location.as_ref().starts_with(&prefix)
                    && metadata.location.as_ref() > start_after.as_str()
            })
            .take(max_items as usize + 1)
            .collect();
        let more = u32::from(page.len() > max_items as usize);
        page.truncate(max_items as usize);
        for metadata in &page {
            let metadata = borrowed_metadata(metadata);
            // SAFETY: Each stack descriptor and its owned path remain live through sink return.
            sink_status(unsafe { sink(sink_context, &metadata) })?;
        }
        // SAFETY: The caller provides exclusively writable, aligned output for this call.
        unsafe { has_more.write(more) };
        Ok(())
    })
}

unsafe extern "C" fn put(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    body: KernelNativeByteSliceV1,
    mode: u32,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        let mode = match mode {
            KERNEL_NATIVE_PUT_OVERWRITE => PutMode::Overwrite,
            KERNEL_NATIVE_PUT_CREATE => PutMode::Create,
            _ => return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED),
        };
        // SAFETY: Inputs are readable through return and are copied before asynchronous native I/O.
        let (context, path, body) =
            unsafe { (context_ref(context)?, copy_string(path)?, copy_bytes(body)?) };
        let path = Path::parse(path).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        runtime()?
            .block_on(context.store.put_opts(
                &path,
                Bytes::from(body).into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            ))
            .map(|_| ())
            .map_err(error_status)
    })
}

unsafe extern "C" fn delete_object(context: *mut c_void, path: KernelNativeStringSliceV1) -> i32 {
    guarded(|| {
        DELETES.fetch_add(1, Ordering::SeqCst);
        // SAFETY: Kernel holds a live context and readable path through callback return.
        let (context, path) = unsafe { (context_ref(context)?, copy_string(path)?) };
        let path = Path::parse(path).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        runtime()?
            .block_on(context.store.delete(&path))
            .map_err(error_status)
    })
}

unsafe extern "C" fn release(context: *mut c_void) {
    guarded(|| {
        if !context.is_null() {
            // SAFETY: Only the final owner calls release, once, with this provider's allocation.
            let context = unsafe { Box::from_raw(context.cast::<ProviderContext>()) };
            RELEASES.fetch_add(1, Ordering::SeqCst);
            drop(context);
        }
        Ok(())
    });
}

fn runtime() -> Result<&'static Runtime, i32> {
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)
        })
        .as_ref()
        .map_err(|status| *status)
}

unsafe fn context_ref<'context>(context: *mut c_void) -> Result<&'context ProviderContext, i32> {
    // SAFETY: Non-null pointers must reference this provider's live immutable allocation.
    unsafe { context.cast::<ProviderContext>().as_ref() }.ok_or(KERNEL_NATIVE_STATUS_GENERIC)
}

unsafe fn copy_bytes(value: KernelNativeByteSliceV1) -> Result<Vec<u8>, i32> {
    if value.len == 0 {
        return Ok(Vec::new());
    }
    if value.ptr.is_null() || value.len > isize::MAX as usize {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    // SAFETY: The ABI caller guarantees one readable allocation of at least len bytes.
    Ok(unsafe { slice::from_raw_parts(value.ptr, value.len) }.to_vec())
}

fn borrowed_metadata(metadata: &ObjectMeta) -> KernelNativeObjectMetaV1 {
    let location = metadata.location.as_ref();
    KernelNativeObjectMetaV1 {
        location: KernelNativeStringSliceV1 {
            ptr: location.as_ptr().cast(),
            len: location.len(),
        },
        size: metadata.size,
        last_modified_unix_ms: metadata.last_modified.timestamp_millis(),
    }
}

fn sink_status(status: i32) -> Result<(), i32> {
    if status == KERNEL_NATIVE_STATUS_OK {
        Ok(())
    } else {
        Err(KERNEL_NATIVE_STATUS_GENERIC)
    }
}

async fn bounded_body(result: object_store::GetResult, limit: usize) -> Result<Bytes, i32> {
    if result.meta.size > limit as u64 {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    let mut stream = result.into_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.try_next().await.map_err(error_status)? {
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|size| size > limit)
        {
            return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

fn error_status(error: object_store::Error) -> i32 {
    match error {
        object_store::Error::NotFound { .. } => KERNEL_NATIVE_STATUS_NOT_FOUND,
        object_store::Error::AlreadyExists { .. } => KERNEL_NATIVE_STATUS_ALREADY_EXISTS,
        object_store::Error::NotSupported { .. } => KERNEL_NATIVE_STATUS_NOT_SUPPORTED,
        _ => KERNEL_NATIVE_STATUS_GENERIC,
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::ptr;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        prototype_append_commit, prototype_callback_count, prototype_create_azure,
        prototype_create_memory, prototype_descriptor_size, prototype_release_count,
    };

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Debug, PartialEq, Eq)]
    struct OwnedMetadata {
        location: String,
        size: u64,
        timestamp: i64,
    }

    #[derive(Default)]
    struct Capture {
        metadata: Vec<OwnedMetadata>,
        bodies: Vec<Vec<u8>>,
    }

    struct Fixture(KernelNativeObjectStoreDescriptorV1);

    impl Fixture {
        fn memory() -> Self {
            let mut output = MaybeUninit::uninit();
            // SAFETY: The factory has exclusive aligned output and runs on an ordinary test thread.
            assert_eq!(unsafe { prototype_create_memory(output.as_mut_ptr()) }, 0);
            // SAFETY: A successful factory initializes every descriptor field.
            Self(unsafe { output.assume_init() })
        }

        fn get(&self, path: &str, flags: u32) -> (i32, Capture) {
            let mut capture = Capture::default();
            // SAFETY: The fixture owns context; inputs and capture live through synchronous return.
            let status = unsafe {
                self.0.get.unwrap()(
                    self.0.context,
                    string(path),
                    flags,
                    ptr::from_mut(&mut capture).cast(),
                    capture_get,
                )
            };
            (status, capture)
        }

        fn list(&self, prefix: &str, after: &str, maximum: u32) -> (i32, Capture, u32) {
            let mut capture = Capture::default();
            let mut more = 99;
            // SAFETY: All input, output and sink storage lives until this callback returns.
            let status = unsafe {
                self.0.list.unwrap()(
                    self.0.context,
                    string(prefix),
                    string(after),
                    maximum,
                    ptr::from_mut(&mut capture).cast(),
                    capture_list,
                    &mut more,
                )
            };
            (status, capture, more)
        }

        fn put(&self, path: &str, body: &[u8], mode: u32) -> i32 {
            // SAFETY: The fixture retains context and the buffers throughout the callback.
            unsafe { self.0.put.unwrap()(self.0.context, string(path), bytes(body), mode) }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // SAFETY: The fixture is the only owner, and no callback remains in flight.
            unsafe { self.0.release.unwrap()(self.0.context) };
        }
    }

    fn string(value: &str) -> KernelNativeStringSliceV1 {
        KernelNativeStringSliceV1 {
            ptr: value.as_ptr().cast(),
            len: value.len(),
        }
    }

    fn bytes(value: &[u8]) -> KernelNativeByteSliceV1 {
        KernelNativeByteSliceV1 {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    unsafe fn own_metadata(
        metadata: *const KernelNativeObjectMetaV1,
    ) -> Result<OwnedMetadata, i32> {
        // SAFETY: The provider supplies readable metadata and UTF-8 until the sink returns.
        let metadata = unsafe { metadata.as_ref() }.ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
        Ok(OwnedMetadata {
            // SAFETY: The metadata path remains readable for the synchronous copy.
            location: unsafe { copy_string(metadata.location) }?,
            size: metadata.size,
            timestamp: metadata.last_modified_unix_ms,
        })
    }

    unsafe extern "C" fn capture_get(
        context: *mut c_void,
        metadata: *const KernelNativeObjectMetaV1,
        body: KernelNativeByteSliceV1,
    ) -> i32 {
        guarded(|| {
            // SAFETY: Fixture::get exclusively lends its capture and provider output for this call.
            let capture = unsafe { context.cast::<Capture>().as_mut() }
                .ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
            // SAFETY: The provider owns all returned bytes and metadata until this sink returns.
            let (metadata, body) = unsafe { (own_metadata(metadata)?, copy_bytes(body)?) };
            capture.metadata.push(metadata);
            capture.bodies.push(body);
            Ok(())
        })
    }

    unsafe extern "C" fn capture_list(
        context: *mut c_void,
        metadata: *const KernelNativeObjectMetaV1,
    ) -> i32 {
        guarded(|| {
            // SAFETY: Fixture::list exclusively lends its capture and provider output for this
            // call.
            let capture = unsafe { context.cast::<Capture>().as_mut() }
                .ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
            // SAFETY: The provider metadata is readable through synchronous sink return.
            capture.metadata.push(unsafe { own_metadata(metadata) }?);
            Ok(())
        })
    }

    unsafe extern "C" fn reject_get(
        _context: *mut c_void,
        _metadata: *const KernelNativeObjectMetaV1,
        _body: KernelNativeByteSliceV1,
    ) -> i32 {
        KERNEL_NATIVE_STATUS_NOT_FOUND
    }

    unsafe extern "C" fn reject_list(
        context: *mut c_void,
        _metadata: *const KernelNativeObjectMetaV1,
    ) -> i32 {
        guarded(|| {
            // SAFETY: The test passes an exclusively writable counter, live until callback return.
            let calls =
                unsafe { context.cast::<u32>().as_mut() }.ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
            *calls += 1;
            Err(-99)
        })
    }

    #[test]
    fn oversized_get_metadata_is_rejected_before_polling_body() {
        let context = create_memory().unwrap();
        runtime().unwrap().block_on(async {
            let mut result = context
                .store
                .get_opts(&Path::from(INITIAL_PATH), Default::default())
                .await
                .unwrap();
            result.meta.size = MAX_BODY_BYTES as u64 + 1;
            result.payload =
                object_store::GetResultPayload::Stream(Box::pin(futures::stream::poll_fn(|_| {
                    panic!("oversized metadata must be rejected before body polling")
                })));
            assert_eq!(
                bounded_body(result, MAX_BODY_BYTES).await.unwrap_err(),
                KERNEL_NATIVE_STATUS_NOT_SUPPORTED
            );
        });
    }

    #[test]
    fn get_stream_cannot_exceed_body_limit_with_small_declared_metadata() {
        let context = create_memory().unwrap();
        runtime().unwrap().block_on(async {
            let mut result = context
                .store
                .get_opts(&Path::from(INITIAL_PATH), Default::default())
                .await
                .unwrap();
            result.meta.size = 8;
            result.payload =
                object_store::GetResultPayload::Stream(Box::pin(futures::stream::iter([
                    Ok(Bytes::from_static(b"12345678")),
                    Ok(Bytes::from_static(b"9")),
                ])));
            assert_eq!(
                bounded_body(result, 8).await.unwrap_err(),
                KERNEL_NATIVE_STATUS_NOT_SUPPORTED
            );
        });
    }

    #[test]
    fn memory_factory_populates_descriptor_and_get_head_copy_valid_table_metadata() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        assert_eq!(fixture.0.abi_version, KERNEL_NATIVE_STORE_ABI_V1);
        assert_eq!(fixture.0.struct_size, prototype_descriptor_size());
        assert_eq!(
            fixture.0.struct_size as usize,
            size_of::<KernelNativeObjectStoreDescriptorV1>()
        );
        assert!(!fixture.0.context.is_null());
        assert!(fixture.0.get.is_some() && fixture.0.list.is_some() && fixture.0.put.is_some());
        assert!(fixture.0.delete_object.is_some() && fixture.0.release.is_some());
        let baseline = prototype_callback_count();
        let (status, full) = fixture.get(INITIAL_PATH, KERNEL_NATIVE_GET_FULL);
        assert_eq!(status, KERNEL_NATIVE_STATUS_OK);
        assert_eq!(full.metadata.len(), 1);
        assert_eq!(full.metadata[0].location, INITIAL_PATH);
        assert_eq!(full.metadata[0].size, INITIAL_COMMIT.len() as u64);
        assert_eq!(full.bodies, vec![INITIAL_COMMIT.as_bytes().to_vec()]);
        let commit = std::str::from_utf8(&full.bodies[0]).unwrap();
        assert!(commit.ends_with('\n'));
        let actions: Vec<serde_json::Value> = commit
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(actions[0]["protocol"]["minReaderVersion"], 1);
        assert_eq!(actions[1]["metaData"]["format"]["provider"], "parquet");
        let schema: serde_json::Value =
            serde_json::from_str(actions[1]["metaData"]["schemaString"].as_str().unwrap()).unwrap();
        assert_eq!(schema["type"], "struct");
        assert_eq!(schema["fields"][0]["name"], "id");
        assert_eq!(schema["fields"][0]["type"], "long");
        let (status, head) = fixture.get(INITIAL_PATH, KERNEL_NATIVE_GET_HEAD);
        assert_eq!(status, KERNEL_NATIVE_STATUS_OK);
        assert_eq!(head.metadata, full.metadata);
        assert_eq!(head.bodies, vec![Vec::<u8>::new()]);
        assert_eq!(prototype_callback_count() - baseline, 2);
    }

    #[test]
    fn list_pages_are_sorted_bounded_exclusive_and_empty_after_last_entry() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        // SAFETY: The fixture retains its provider-owned context for this borrowed mutation.
        assert_eq!(unsafe { prototype_append_commit(fixture.0.context) }, 0);
        // SAFETY: The same live context remains owned; native create must detect the existing
        // commit.
        assert_eq!(unsafe { prototype_append_commit(fixture.0.context) }, 2);
        assert_eq!(
            fixture.put("outside/item", b"not in prefix", KERNEL_NATIVE_PUT_CREATE),
            0
        );
        let (status, first, more) = fixture.list("table/_delta_log", "", 1);
        assert_eq!((status, more), (0, 1));
        assert_eq!(first.metadata.len(), 1);
        assert_eq!(first.metadata[0].location, INITIAL_PATH);
        let (status, second, more) = fixture.list("table/_delta_log", INITIAL_PATH, 1);
        assert_eq!((status, more), (0, 0));
        assert_eq!(second.metadata.len(), 1);
        assert_eq!(second.metadata[0].location, APPEND_PATH);
        let (status, empty, more) = fixture.list("table/_delta_log", APPEND_PATH, 1);
        assert_eq!((status, more), (0, 0));
        assert!(empty.metadata.is_empty());
        let (status, all, more) = fixture.list("", "", 1024);
        assert_eq!((status, more), (0, 0));
        assert_eq!(all.metadata.len(), 3);
        assert!(all
            .metadata
            .windows(2)
            .all(|pair| pair[0].location < pair[1].location));
        let (status, appended) = fixture.get(APPEND_PATH, KERNEL_NATIVE_GET_FULL);
        assert_eq!(status, 0);
        assert_eq!(appended.bodies[0], APPEND_COMMIT.as_bytes());
    }

    #[test]
    fn native_put_create_overwrite_and_delete_preserve_binary_payload_and_statuses() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        let payload = [0, 255, 42];
        assert_eq!(
            fixture.put("data/item", &payload, KERNEL_NATIVE_PUT_CREATE),
            0
        );
        assert_eq!(
            fixture.put("data/item", b"duplicate", KERNEL_NATIVE_PUT_CREATE),
            2
        );
        let (status, stored) = fixture.get("data/item", KERNEL_NATIVE_GET_FULL);
        assert_eq!(status, 0);
        assert_eq!(stored.bodies[0], payload);
        assert_eq!(
            fixture.put("data/item", b"replaced", KERNEL_NATIVE_PUT_OVERWRITE),
            0
        );
        let (status, stored) = fixture.get("data/item", KERNEL_NATIVE_GET_FULL);
        assert_eq!(status, 0);
        assert_eq!(stored.bodies[0], b"replaced");
        // SAFETY: The fixture owns context and the path remains readable through deletion.
        let status =
            unsafe { fixture.0.delete_object.unwrap()(fixture.0.context, string("data/item")) };
        assert_eq!(status, 0);
        let (status, missing) = fixture.get("data/item", KERNEL_NATIVE_GET_FULL);
        assert_eq!(status, KERNEL_NATIVE_STATUS_NOT_FOUND);
        assert!(missing.metadata.is_empty() && missing.bodies.is_empty());
    }

    #[test]
    fn callbacks_reject_invalid_modes_utf8_null_buffers_and_page_bounds() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        assert_eq!(
            fixture.get(INITIAL_PATH, 99).0,
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        assert_eq!(
            fixture.put("data/item", b"", 99),
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        for limit in [0, 1025] {
            let (status, capture, more) = fixture.list("", "", limit);
            assert_eq!(status, KERNEL_NATIVE_STATUS_GENERIC);
            assert!(capture.metadata.is_empty());
            assert_eq!(more, 99);
        }
        let invalid_utf8 = [255];
        let invalid_path = KernelNativeStringSliceV1 {
            ptr: invalid_utf8.as_ptr().cast(),
            len: 1,
        };
        // SAFETY: Invalid UTF-8 is nevertheless a live readable byte allocation; no sink is needed.
        let status = unsafe {
            fixture.0.get.unwrap()(
                fixture.0.context,
                invalid_path,
                0,
                ptr::null_mut(),
                reject_get,
            )
        };
        assert_eq!(status, KERNEL_NATIVE_STATUS_GENERIC);
        // SAFETY: A null non-empty body is deliberately rejected before any dereference.
        let status = unsafe {
            fixture.0.put.unwrap()(
                fixture.0.context,
                string("data/item"),
                KernelNativeByteSliceV1 {
                    ptr: ptr::null(),
                    len: 1,
                },
                0,
            )
        };
        assert_eq!(status, KERNEL_NATIVE_STATUS_GENERIC);
        // SAFETY: Null context is rejected without dereference or sink invocation.
        let status = unsafe {
            get(
                ptr::null_mut(),
                string(INITIAL_PATH),
                0,
                ptr::null_mut(),
                reject_get,
            )
        };
        assert_eq!(status, 3);
        // SAFETY: Null has_more is rejected before any sink invocation.
        let status = unsafe {
            fixture.0.list.unwrap()(
                fixture.0.context,
                string(""),
                string(""),
                1,
                ptr::null_mut(),
                reject_list,
                ptr::null_mut(),
            )
        };
        assert_eq!(status, 3);
    }

    #[test]
    fn sink_errors_become_generic_and_stop_listing_without_publishing_has_more() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        assert_eq!(fixture.put("table/another", b"body", 1), 0);
        // SAFETY: Context is live; the rejecting sink borrows no state and does not unwind.
        let status = unsafe {
            fixture.0.get.unwrap()(
                fixture.0.context,
                string(INITIAL_PATH),
                0,
                ptr::null_mut(),
                reject_get,
            )
        };
        assert_eq!(status, 3);
        let mut calls = 0_u32;
        let mut more = 99;
        // SAFETY: Counter and output are exclusively lent until synchronous callback return.
        let status = unsafe {
            fixture.0.list.unwrap()(
                fixture.0.context,
                string("table"),
                string(""),
                2,
                ptr::from_mut(&mut calls).cast(),
                reject_list,
                &mut more,
            )
        };
        assert_eq!(status, 3);
        assert_eq!(calls, 1);
        assert_eq!(more, 99);
    }

    #[test]
    fn factories_leave_existing_output_untouched_on_failure() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        let mut output = fixture.0;
        let original_context = output.context;
        // SAFETY: The output is writable; the empty endpoint is valid UTF-8 but not a service URL.
        assert_eq!(
            unsafe { prototype_create_azure(string(""), &mut output) },
            3
        );
        assert_eq!(output.context, original_context);
        assert_eq!(output.abi_version, fixture.0.abi_version);
        assert_eq!(output.struct_size, fixture.0.struct_size);
        // SAFETY: A null factory output is accepted as an invalid argument and never dereferenced.
        assert_eq!(unsafe { prototype_create_memory(ptr::null_mut()) }, 3);
        assert_eq!(fixture.get(INITIAL_PATH, 0).0, 0);
    }

    #[test]
    fn azure_context_rejects_memory_append_without_network_or_credential_retrieval() {
        let _serial = TEST_LOCK.lock().unwrap();
        let mut output = MaybeUninit::uninit();
        // SAFETY: The endpoint lives through the call and output is exclusively writable.
        let status =
            unsafe { prototype_create_azure(string("http://127.0.0.1:1"), output.as_mut_ptr()) };
        assert_eq!(status, 0);
        // SAFETY: Successful factory output is fully initialized and owned by this fixture.
        let fixture = Fixture(unsafe { output.assume_init() });
        // SAFETY: The fixture holds context throughout this memory-only helper's rejection.
        assert_eq!(
            unsafe { prototype_append_commit(fixture.0.context) },
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
    }

    #[test]
    fn context_release_inside_another_runtime_does_not_drop_provider_runtime() {
        let _serial = TEST_LOCK.lock().unwrap();
        let baseline = prototype_release_count();
        let fixture = Fixture::memory();
        let other_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        other_runtime.block_on(async move { drop(fixture) });
        assert_eq!(prototype_release_count() - baseline, 1);
        let next = Fixture::memory();
        assert_eq!(next.get(INITIAL_PATH, 0).0, 0);
        drop(next);
        assert_eq!(prototype_release_count() - baseline, 2);
    }

    #[test]
    fn empty_null_slices_are_valid_and_nonempty_null_slices_fail_without_dereference() {
        // SAFETY: The ABI permits null for zero length; nonempty null is checked before access.
        let empty_string = unsafe {
            copy_string(KernelNativeStringSliceV1 {
                ptr: ptr::null(),
                len: 0,
            })
        };
        assert_eq!(empty_string, Ok(String::new()));
        // SAFETY: Empty null payload is valid and no storage is read.
        let empty_body = unsafe {
            copy_bytes(KernelNativeByteSliceV1 {
                ptr: ptr::null(),
                len: 0,
            })
        };
        assert_eq!(empty_body, Ok(Vec::new()));
        // SAFETY: This invalid null input is rejected before any dereference.
        let invalid_body = unsafe {
            copy_bytes(KernelNativeByteSliceV1 {
                ptr: ptr::null(),
                len: 1,
            })
        };
        assert_eq!(invalid_body, Err(3));
    }

    #[test]
    fn guarded_rust_panics_become_generic_status() {
        assert_eq!(
            guarded(|| panic!("provider boundary test")),
            KERNEL_NATIVE_STATUS_GENERIC
        );
    }
}
