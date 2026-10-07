use std::ffi::c_void;
use std::mem::{forget, size_of};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use delta_kernel_native_store_abi::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode, PutOptions, RetryConfig,
};
use tokio::runtime::{Builder, Runtime};

use crate::credentials::CustomCredentialProvider;

const MAX_LIST_ITEMS: usize = KERNEL_NATIVE_LIST_BATCH_SIZE;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 64 * 1024;
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

mod marshalling;
mod multipart;
mod operations;
use marshalling::{borrowed_metadata, charge, check_metadata, optional_string, string_view};

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

struct ProviderCursor {
    stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
    _store: Arc<dyn ObjectStore>,
    exhausted: bool,
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

pub(crate) fn descriptor(context: ProviderContext) -> KernelNativeObjectStoreDescriptorV4 {
    KernelNativeObjectStoreDescriptorV4 {
        abi_version: KERNEL_NATIVE_STORE_ABI_V4,
        struct_size: size_of::<KernelNativeObjectStoreDescriptorV4>() as u32,
        context: Box::into_raw(Box::new(context)).cast(),
        get: Some(get),
        get_ranges: Some(operations::get_ranges),
        list_open: Some(list_open),
        list_next: Some(list_next),
        list_close: Some(list_close),
        list_delimiter: Some(operations::list_delimiter),
        put: Some(put),
        delete_batch: Some(operations::delete_batch),
        copy: Some(operations::copy),
        rename: Some(operations::rename),
        multipart_open: Some(multipart::open),
        multipart_part_open: Some(multipart::part_open),
        multipart_part_wait: Some(multipart::part_wait),
        multipart_part_close: Some(multipart::part_close),
        multipart_complete: Some(multipart::complete),
        multipart_abort: Some(multipart::abort),
        multipart_close: Some(multipart::close),
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
    if value.len > MAX_PATH_BYTES {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
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
    options: KernelNativeGetOptionsV4,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(
        *mut c_void,
        *const KernelNativeObjectMetaV4,
        KernelNativeByteSliceV1,
        u64,
        u64,
        *const KernelNativeKeyValueV4,
        usize,
    ) -> i32,
) -> i32 {
    guarded(|| {
        GETS.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: Kernel retains the provider context and input through callback return.
        let (context, path) = unsafe { (context_ref(context)?, copy_path(path)?) };
        let path = Path::parse(path).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let mut request_bytes = path.as_ref().len();
        let options = unsafe { marshalling::get_options(options, &mut request_bytes) }?;
        let head = options.head;
        let (object_metadata, range, body, attributes) = runtime()?.block_on(async {
            let result = context
                .store
                .get_opts(&path, options)
                .await
                .map_err(error_status)?;
            let mut budget = 0;
            check_metadata(&result.meta, &mut budget)?;
            let metadata = result.meta.clone();
            let attributes = marshalling::attribute_pairs(&result.attributes, &mut budget)?;
            let range = result.range.clone();
            if range.start > range.end || range.end > metadata.size {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
            let body = if head {
                Bytes::new()
            } else {
                bounded_body(result, MAX_BODY_BYTES - budget).await?
            };
            if !head && body.len() as u64 != range.end - range.start {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
            Ok::<_, i32>((metadata, range, body, attributes))
        })?;
        let metadata = borrowed_metadata(&object_metadata);
        let attributes = marshalling::pair_views(&attributes);
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
                range.start,
                range.end,
                attributes.as_ptr(),
                attributes.len(),
            )
        };
        sink_status(status)
    })
}

unsafe extern "C" fn list_open(
    context: *mut c_void,
    prefix: KernelNativeStringSliceV1,
    start_after: KernelNativeStringSliceV1,
    output: *mut *mut c_void,
) -> i32 {
    guarded(|| {
        if output.is_null() || !output.is_aligned() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: Kernel retains the provider context and input slices through callback return.
        let (context, prefix, offset) = unsafe {
            (
                context_ref(context)?,
                copy_path(prefix)?,
                copy_path(start_after)?,
            )
        };
        let native_prefix = if prefix.is_empty() {
            None
        } else {
            Some(Path::parse(&prefix).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?)
        };
        let native_offset = if offset.is_empty() {
            None
        } else {
            Some(Path::parse(offset).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?)
        };
        let _entered = runtime()?.enter();
        let store = Arc::clone(&context.store);
        let stream = match native_offset.as_ref() {
            Some(offset) => store.list_with_offset(native_prefix.as_ref(), offset),
            None => store.list(native_prefix.as_ref()),
        };
        let cursor = Box::new(ProviderCursor {
            _store: store,
            stream,
            exhausted: false,
        });
        // SAFETY: The caller exclusively lends aligned output; ownership transfers only here.
        unsafe { output.write(Box::into_raw(cursor).cast()) };
        Ok(())
    })
}

unsafe extern "C" fn list_next(
    cursor: *mut c_void,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV4) -> i32,
    has_more: *mut u32,
) -> i32 {
    guarded(|| {
        LISTS.fetch_add(1, Ordering::SeqCst);
        if cursor.is_null()
            || !cursor.cast::<ProviderCursor>().is_aligned()
            || sink_context.is_null()
            || has_more.is_null()
            || !has_more.is_aligned()
        {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: The caller exclusively borrows this live cursor until advance returns.
        let cursor = unsafe { &mut *cursor.cast::<ProviderCursor>() };
        runtime()?.block_on(async {
            let mut budget = 0;
            if !cursor.exhausted {
                for _ in 0..MAX_LIST_ITEMS {
                    let Some(metadata) = cursor.stream.try_next().await.map_err(error_status)?
                    else {
                        cursor.exhausted = true;
                        break;
                    };
                    check_metadata(&metadata, &mut budget)?;
                    let metadata = borrowed_metadata(&metadata);
                    // SAFETY: Metadata and its path remain live through the serial sink call.
                    sink_status(unsafe { sink(sink_context, &metadata) })?;
                }
            }
            // SAFETY: The caller exclusively lends this aligned flag; errors leave it untouched.
            unsafe { has_more.write(u32::from(!cursor.exhausted)) };
            Ok(())
        })
    })
}

unsafe extern "C" fn list_close(cursor: *mut c_void) {
    guarded(|| {
        if !cursor.is_null() {
            if !cursor.cast::<ProviderCursor>().is_aligned() {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
            // SAFETY: The caller consumes this provider allocation once, without an active next.
            drop(unsafe { Box::from_raw(cursor.cast::<ProviderCursor>()) });
        }
        Ok(())
    });
}

unsafe extern "C" fn put(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    body: KernelNativeByteSliceV1,
    options: KernelNativeWriteOptionsV4,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: The caller retains context and readable input through callback return.
        let (context, path) = unsafe { (context_ref(context)?, copy_path(path)?) };
        let path = Path::parse(path).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let mut budget = path.as_ref().len();
        let options = unsafe { marshalling::write_options(options, false, &mut budget) }?;
        charge(&mut budget, body.len)?;
        // SAFETY: The size bound is checked before reading or copying the borrowed payload.
        let body = unsafe { copy_bytes(body) }?;
        let result = runtime()?
            .block_on(context.store.put_opts(
                &path,
                Bytes::from(body).into(),
                PutOptions {
                    mode: options.mode,
                    tags: options.tags,
                    attributes: options.attributes,
                    ..Default::default()
                },
            ))
            .map_err(error_status)?;
        unsafe { marshalling::put_result(&result, sink_context, sink) }
    })
}

unsafe extern "C" fn release(context: *mut c_void) {
    guarded(|| {
        if !context.is_null() {
            if !context.cast::<ProviderContext>().is_aligned() {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
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
    if !context.cast::<ProviderContext>().is_aligned() {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    // SAFETY: Non-null pointers must reference this provider's live immutable allocation.
    unsafe { context.cast::<ProviderContext>().as_ref() }.ok_or(KERNEL_NATIVE_STATUS_GENERIC)
}

unsafe fn copy_path(value: KernelNativeStringSliceV1) -> Result<String, i32> {
    if value.len > MAX_PATH_BYTES {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    // SAFETY: The callback caller guarantees readable bytes through this synchronous copy.
    unsafe { copy_string(value) }
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

fn sink_status(status: i32) -> Result<(), i32> {
    if status == KERNEL_NATIVE_STATUS_OK {
        Ok(())
    } else {
        Err(KERNEL_NATIVE_STATUS_GENERIC)
    }
}

async fn bounded_body(result: object_store::GetResult, limit: usize) -> Result<Bytes, i32> {
    if result
        .range
        .end
        .checked_sub(result.range.start)
        .is_none_or(|length| length > limit as u64)
    {
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

fn native_get_options(options: KernelNativeGetOptionsV3) -> Result<GetOptions, i32> {
    if options.head > 1 {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    let range = match options.range_kind {
        0 if options.start == 0 && options.end == 0 => None,
        1 => Some(GetRange::Bounded(options.start..options.end)),
        2 if options.end == 0 => Some(GetRange::Offset(options.start)),
        3 if options.start == 0 => Some(GetRange::Suffix(options.end)),
        _ => return Err(KERNEL_NATIVE_STATUS_GENERIC),
    };
    if let Some(range) = &range {
        range.is_valid().map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
    }
    Ok(GetOptions {
        head: options.head == 1,
        range,
        ..Default::default()
    })
}

fn error_status(error: object_store::Error) -> i32 {
    operations::error_status_ref(&error)
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::{fmt, ptr};

    use async_trait::async_trait;
    use futures::{Stream, StreamExt};
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectStoreExt, PutMultipartOptions,
        PutPayload, PutResult,
    };

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
        e_tag: Option<String>,
        version: Option<String>,
    }

    #[derive(Default)]
    struct Capture {
        metadata: Vec<OwnedMetadata>,
        bodies: Vec<Vec<u8>>,
        ranges: Vec<Range<u64>>,
    }

    struct Fixture(KernelNativeObjectStoreDescriptorV4);

    struct Cursor(*mut c_void);

    // SAFETY: This exclusive cursor owner never overlaps next/close; both support any thread.
    unsafe impl Send for Cursor {}

    impl Cursor {
        fn next(&mut self) -> (i32, Capture, u32) {
            let mut capture = Capture::default();
            let mut more = u32::MAX;
            // SAFETY: This owner exclusively lends cursor, capture and flag through return.
            let status = unsafe {
                list_next(
                    self.0,
                    ptr::from_mut(&mut capture).cast(),
                    capture_list,
                    &mut more,
                )
            };
            (status, capture, more)
        }
    }

    impl Drop for Cursor {
        fn drop(&mut self) {
            // SAFETY: This owner consumes the cursor once, after all advances return.
            unsafe { list_close(self.0) };
        }
    }

    impl Fixture {
        fn memory() -> Self {
            let mut output = MaybeUninit::uninit();
            // SAFETY: The factory has exclusive aligned output and runs on an ordinary test thread.
            assert_eq!(unsafe { prototype_create_memory(output.as_mut_ptr()) }, 0);
            // SAFETY: A successful factory initializes every descriptor field.
            Self(unsafe { output.assume_init() })
        }

        fn get(&self, path: &str) -> (i32, Capture) {
            self.get_opts(path, KernelNativeGetOptionsV3::default())
        }

        fn from_store(store: Arc<dyn ObjectStore>) -> Self {
            Self(descriptor(ProviderContext {
                store,
                memory: true,
            }))
        }

        fn get_opts(&self, path: &str, options: KernelNativeGetOptionsV3) -> (i32, Capture) {
            let mut capture = Capture::default();
            // SAFETY: The fixture owns context; inputs and capture live through synchronous return.
            let status = unsafe {
                self.0.get.unwrap()(
                    self.0.context,
                    string(path),
                    v4_get(options),
                    ptr::from_mut(&mut capture).cast(),
                    capture_get,
                )
            };
            (status, capture)
        }

        fn list(&self, prefix: &str) -> (i32, Capture) {
            let mut capture = Capture::default();
            let mut cursor = match self.open(prefix, "") {
                Ok(cursor) => cursor,
                Err(status) => return (status, capture),
            };
            loop {
                let (status, page, more) = cursor.next();
                capture.metadata.extend(page.metadata);
                if status != 0 || more == 0 {
                    return (status, capture);
                }
            }
        }

        fn open(&self, prefix: &str, offset: &str) -> Result<Cursor, i32> {
            let mut cursor = ptr::null_mut();
            // SAFETY: This fixture retains context and exclusively lends cursor output.
            let status = unsafe {
                self.0.list_open.unwrap()(
                    self.0.context,
                    string(prefix),
                    string(offset),
                    &mut cursor,
                )
            };
            if status == 0 {
                Ok(Cursor(cursor))
            } else {
                Err(status)
            }
        }

        fn put(&self, path: &str, body: &[u8], mode: u32) -> i32 {
            // SAFETY: The fixture retains context and all input bytes through synchronous return.
            unsafe { put(self.0.context, string(path), bytes(body), mode) }
        }

        fn delete(&self, path: &str) -> i32 {
            // SAFETY: The fixture retains context and the readable path through return.
            unsafe { delete_object(self.0.context, string(path)) }
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

    fn v4_get(base: KernelNativeGetOptionsV3) -> KernelNativeGetOptionsV4 {
        KernelNativeGetOptionsV4 {
            base,
            if_match: optional_string(None),
            if_none_match: optional_string(None),
            version: optional_string(None),
            time_flags: 0,
            modified_nanos: 0,
            modified_seconds: 0,
            unmodified_seconds: 0,
            unmodified_nanos: 0,
        }
    }

    fn write_options(mode: u32) -> KernelNativeWriteOptionsV4 {
        KernelNativeWriteOptionsV4 {
            mode,
            e_tag: optional_string(None),
            version: optional_string(None),
            tags: ptr::null(),
            tags_len: 0,
            attributes: ptr::null(),
            attributes_len: 0,
        }
    }

    unsafe fn get(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
        options: KernelNativeGetOptionsV3,
        sink_context: *mut c_void,
        sink: unsafe extern "C" fn(
            *mut c_void,
            *const KernelNativeObjectMetaV4,
            KernelNativeByteSliceV1,
            u64,
            u64,
            *const KernelNativeKeyValueV4,
            usize,
        ) -> i32,
    ) -> i32 {
        unsafe { super::get(context, path, v4_get(options), sink_context, sink) }
    }

    unsafe extern "C" fn ignore_put(
        _context: *mut c_void,
        _result: *const KernelNativePutResultV4,
    ) -> i32 {
        0
    }

    unsafe fn put(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
        body: KernelNativeByteSliceV1,
        mode: u32,
    ) -> i32 {
        unsafe {
            super::put(
                context,
                path,
                body,
                write_options(mode),
                ptr::from_mut(&mut 0_u8).cast(),
                ignore_put,
            )
        }
    }

    unsafe extern "C" fn capture_delete_status(
        context: *mut c_void,
        _path: KernelNativeStringSliceV1,
        status: i32,
    ) -> i32 {
        unsafe { *context.cast::<i32>() = status };
        0
    }

    unsafe fn delete_object(context: *mut c_void, path: KernelNativeStringSliceV1) -> i32 {
        let mut item_status = 0;
        let status = unsafe {
            operations::delete_batch(
                context,
                &path,
                1,
                ptr::from_mut(&mut item_status).cast(),
                capture_delete_status,
            )
        };
        if status == 0 {
            item_status
        } else {
            status
        }
    }

    unsafe fn own_metadata(
        metadata: *const KernelNativeObjectMetaV4,
    ) -> Result<OwnedMetadata, i32> {
        // SAFETY: The provider supplies readable metadata and UTF-8 until the sink returns.
        let metadata = unsafe { metadata.as_ref() }.ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
        let base = &metadata.base;
        Ok(OwnedMetadata {
            // SAFETY: The metadata path remains readable for the synchronous copy.
            location: unsafe { copy_string(base.location) }?,
            size: base.size,
            timestamp: base.last_modified_unix_ms,
            e_tag: unsafe { marshalling::copy_optional(metadata.e_tag, &mut 0) }?,
            version: unsafe { marshalling::copy_optional(metadata.version, &mut 0) }?,
        })
    }

    unsafe extern "C" fn capture_get(
        context: *mut c_void,
        metadata: *const KernelNativeObjectMetaV4,
        body: KernelNativeByteSliceV1,
        start: u64,
        end: u64,
        _attributes: *const KernelNativeKeyValueV4,
        _attributes_len: usize,
    ) -> i32 {
        guarded(|| {
            // SAFETY: Fixture::get exclusively lends its capture and provider output for this call.
            let capture = unsafe { context.cast::<Capture>().as_mut() }
                .ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
            // SAFETY: The provider owns all returned bytes and metadata until this sink returns.
            let (metadata, body) = unsafe { (own_metadata(metadata)?, copy_bytes(body)?) };
            capture.metadata.push(metadata);
            capture.bodies.push(body);
            capture.ranges.push(start..end);
            Ok(())
        })
    }

    unsafe extern "C" fn capture_list(
        context: *mut c_void,
        metadata: *const KernelNativeObjectMetaV4,
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
        _metadata: *const KernelNativeObjectMetaV4,
        _body: KernelNativeByteSliceV1,
        _start: u64,
        _end: u64,
        _attributes: *const KernelNativeKeyValueV4,
        _attributes_len: usize,
    ) -> i32 {
        KERNEL_NATIVE_STATUS_NOT_FOUND
    }

    unsafe extern "C" fn reject_list(
        context: *mut c_void,
        _metadata: *const KernelNativeObjectMetaV4,
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
    fn oversized_get_range_is_rejected_before_polling_body() {
        let context = create_memory().unwrap();
        runtime().unwrap().block_on(async {
            let mut result = context
                .store
                .get_opts(&Path::from(INITIAL_PATH), Default::default())
                .await
                .unwrap();
            result.meta.size = MAX_BODY_BYTES as u64 + 1;
            result.range = 0..result.meta.size;
            result.payload =
                object_store::GetResultPayload::Stream(Box::pin(futures::stream::poll_fn(|_| {
                    panic!("oversized returned range must be rejected before body polling")
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
            result.range = 0..8;
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
    fn memory_factory_populates_v4_descriptor_and_full_get_list_copy_seeded_table() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        assert_eq!(fixture.0.abi_version, KERNEL_NATIVE_STORE_ABI_V4);
        assert_eq!(fixture.0.struct_size, prototype_descriptor_size());
        assert_eq!(
            fixture.0.struct_size as usize,
            size_of::<KernelNativeObjectStoreDescriptorV4>()
        );
        assert_eq!(size_of::<KernelNativeGetOptionsV3>(), 24);
        if size_of::<usize>() == 8 {
            assert_eq!(fixture.0.struct_size, 160);
        }
        assert!(!fixture.0.context.is_null());
        assert!(fixture.0.get.is_some() && fixture.0.release.is_some());
        assert!(
            fixture.0.list_open.is_some()
                && fixture.0.list_next.is_some()
                && fixture.0.list_close.is_some()
        );
        assert!(fixture.0.put.is_some() && fixture.0.delete_batch.is_some());
        let baseline = prototype_callback_count();
        let (status, full) = fixture.get(INITIAL_PATH);
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
        let (status, listing) = fixture.list("table/_delta_log");
        assert_eq!(status, KERNEL_NATIVE_STATUS_OK);
        drop(fixture);
        assert_eq!(listing.metadata, full.metadata);
        assert_eq!(prototype_callback_count() - baseline, 2);
    }

    #[test]
    fn native_append_is_visible_once_and_listing_preserves_memory_store_order() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        // SAFETY: The fixture retains its provider-owned context for this borrowed mutation.
        assert_eq!(unsafe { prototype_append_commit(fixture.0.context) }, 0);
        // SAFETY: The same live context remains owned; native create must detect the existing
        // commit.
        assert_eq!(unsafe { prototype_append_commit(fixture.0.context) }, 2);
        let (status, all) = fixture.list("table/_delta_log");
        assert_eq!(status, 0);
        assert_eq!(
            all.metadata
                .iter()
                .map(|metadata| metadata.location.as_str())
                .collect::<Vec<_>>(),
            [INITIAL_PATH, APPEND_PATH]
        );
        let (status, empty) = fixture.list("missing");
        assert_eq!(status, 0);
        assert!(empty.metadata.is_empty());
        let (status, appended) = fixture.get(APPEND_PATH);
        assert_eq!(status, 0);
        assert_eq!(appended.bodies[0], APPEND_COMMIT.as_bytes());
    }

    #[test]
    fn listing_retains_one_native_stream_for_300_entries_without_collecting_or_restarting() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        seed_listing(&store, 300);
        let fixture = Fixture::from_store(store.clone());
        let mut cursor = fixture.open("items", "").unwrap();
        assert_eq!(
            store.list_requests.lock().unwrap().as_slice(),
            &[(Some(Path::from("items")), None)]
        );
        assert_eq!(store.polls.load(Ordering::SeqCst), 0);
        let mut all = Vec::new();
        for (length, more) in [(128, 1), (128, 1), (44, 0)] {
            let (status, page, has_more) = cursor.next();
            assert_eq!((status, page.metadata.len(), has_more), (0, length, more));
            all.extend(page.metadata.into_iter().map(|meta| meta.location));
            assert_eq!(
                store.polls.load(Ordering::SeqCst) as usize,
                all.len() + usize::from(more == 0)
            );
            assert_eq!(store.list_requests.lock().unwrap().len(), 1);
        }
        assert_eq!(
            all,
            (0..300)
                .map(|index| format!("items/item-{index:03}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(Arc::strong_count(&store), 3);
        drop(cursor);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&store), 2);
    }

    #[test]
    fn callbacks_validate_slices_contexts_and_sink_errors() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        let context = fixture.0.context;
        let sink_context = ptr::null_mut();
        let mut invalid_capture = Capture::default();
        let capture_context = ptr::from_mut(&mut invalid_capture).cast();
        let initial = string(INITIAL_PATH);
        let options = KernelNativeGetOptionsV3::default();
        let invalid_utf8 = [255];
        for path in [
            KernelNativeStringSliceV1 {
                ptr: invalid_utf8.as_ptr().cast(),
                len: 1,
            },
            KernelNativeStringSliceV1 {
                ptr: ptr::null(),
                len: 1,
            },
        ] {
            // SAFETY: Each slice is readable or rejected before dereference; the fixture is live.
            let statuses = unsafe {
                let mut output = ptr::null_mut();
                (
                    get(context, path, options, capture_context, capture_get),
                    list_open(context, path, string(""), &mut output),
                    list_open(context, string(""), path, &mut output),
                    put(context, path, bytes(b""), KERNEL_NATIVE_PUT_CREATE),
                    delete_object(context, path),
                )
            };
            assert_eq!(statuses, (3, 3, 3, 3, 3));
        }
        assert!(invalid_capture.metadata.is_empty() && invalid_capture.bodies.is_empty());
        let empty = KernelNativeStringSliceV1 {
            ptr: ptr::null(),
            len: 0,
        };
        let mut raw_cursor = ptr::null_mut();
        // SAFETY: Empty null prefix and offset are valid; output is exclusively lent.
        assert_eq!(
            unsafe { list_open(context, empty, empty, &mut raw_cursor) },
            0
        );
        let mut cursor = Cursor(raw_cursor);
        let (status, capture, more) = cursor.next();
        assert_eq!((status, more), (0, 0));
        assert_eq!(capture.metadata.len(), 1);
        // SAFETY: Null contexts and null nonempty buffers are rejected without dereference.
        unsafe {
            assert_eq!(
                get(ptr::null_mut(), initial, options, sink_context, capture_get),
                3
            );
            assert_eq!(list_open(ptr::null_mut(), empty, empty, &mut raw_cursor), 3);
            assert_eq!(list_open(context, empty, empty, ptr::null_mut()), 3);
            assert_eq!(
                list_next(ptr::null_mut(), sink_context, capture_list, &mut 0),
                3
            );
            assert_eq!(list_next(cursor.0, sink_context, capture_list, &mut 0), 3);
            assert_eq!(
                list_next(cursor.0, context, capture_list, ptr::null_mut()),
                3
            );
            assert_eq!(put(ptr::null_mut(), initial, bytes(b""), 0), 3);
            assert_eq!(delete_object(ptr::null_mut(), initial), 3);
            assert_eq!(prototype_append_commit(ptr::null_mut()), 3);
            assert_eq!(
                copy_bytes(KernelNativeByteSliceV1 {
                    ptr: ptr::null(),
                    len: 0
                }),
                Ok(Vec::new())
            );
            assert_eq!(
                copy_bytes(KernelNativeByteSliceV1 {
                    ptr: ptr::null(),
                    len: 1
                }),
                Err(3)
            );
        }
        // SAFETY: Sinks may reject output; all provider inputs and context remain live.
        unsafe {
            assert_eq!(get(context, initial, options, sink_context, capture_get), 3);
            assert_eq!(get(context, initial, options, context, reject_get), 3);
        }
        let mut calls = 0_u32;
        let calls_context = ptr::from_mut(&mut calls).cast();
        // SAFETY: The fixture holds context throughout the native append; counter is lent to sink.
        unsafe {
            assert_eq!(prototype_append_commit(context), 0);
            let failing = fixture.open("", "").unwrap();
            assert_eq!(list_next(failing.0, calls_context, reject_list, &mut 0), 3);
        }
        assert_eq!(calls, 1);
        let oversized = "x".repeat(MAX_PATH_BYTES + 1);
        assert_eq!(
            fixture.get(&oversized).0,
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        assert_eq!(
            fixture.list(&oversized).0,
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        assert_eq!(
            fixture.open("", &oversized).err(),
            Some(KERNEL_NATIVE_STATUS_NOT_SUPPORTED)
        );
        assert_eq!(
            fixture.put(&oversized, b"", 0),
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        assert_eq!(
            fixture.delete(&oversized),
            KERNEL_NATIVE_STATUS_NOT_SUPPORTED
        );
        // SAFETY: Null nonempty bodies are rejected; oversized bodies are rejected before copy.
        unsafe {
            assert_eq!(
                put(
                    context,
                    initial,
                    KernelNativeByteSliceV1 {
                        ptr: ptr::null(),
                        len: 1
                    },
                    0
                ),
                3
            );
            assert_eq!(
                put(
                    context,
                    initial,
                    KernelNativeByteSliceV1 {
                        ptr: ptr::null(),
                        len: MAX_BODY_BYTES + 1
                    },
                    0
                ),
                4
            );
        }
        assert_eq!(fixture.put(INITIAL_PATH, b"", 99), 4);
        let (status, missing) = fixture.get("missing");
        assert_eq!(status, KERNEL_NATIVE_STATUS_NOT_FOUND);
        assert!(missing.metadata.is_empty() && missing.bodies.is_empty());
    }

    #[test]
    fn factories_reject_misaligned_output_before_allocation() {
        let output = std::ptr::NonNull::<u8>::dangling().as_ptr().cast();
        unsafe {
            assert_eq!(
                prototype_create_memory(output),
                KERNEL_NATIVE_STATUS_GENERIC
            );
            assert_eq!(
                prototype_create_azure(string("http://127.0.0.1:1"), output),
                KERNEL_NATIVE_STATUS_GENERIC
            );
        }
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
        let oversized = "x".repeat(MAX_PATH_BYTES + 1);
        // SAFETY: The readable oversized endpoint must fail before output transfer.
        assert_eq!(
            unsafe { prototype_create_azure(string(&oversized), &mut output) },
            4
        );
        let invalid_utf8 = [255];
        // SAFETY: Both malformed slices are rejected before endpoint construction.
        unsafe {
            assert_eq!(
                prototype_create_azure(
                    KernelNativeStringSliceV1 {
                        ptr: invalid_utf8.as_ptr().cast(),
                        len: 1
                    },
                    &mut output
                ),
                3
            );
            assert_eq!(
                prototype_create_azure(
                    KernelNativeStringSliceV1 {
                        ptr: ptr::null(),
                        len: 1
                    },
                    &mut output
                ),
                3
            );
        }
        assert_eq!(output.context, original_context);
        // SAFETY: A null factory output is accepted as an invalid argument and never dereferenced.
        assert_eq!(unsafe { prototype_create_memory(ptr::null_mut()) }, 3);
        assert_eq!(
            unsafe { prototype_create_azure(string("http://127.0.0.1:1"), ptr::null_mut()) },
            3
        );
        assert_eq!(fixture.get(INITIAL_PATH).0, 0);
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
        assert_eq!(next.get(INITIAL_PATH).0, 0);
        drop(next);
        assert_eq!(prototype_release_count() - baseline, 2);
    }

    #[test]
    fn guarded_rust_panics_become_generic_status() {
        assert_eq!(
            guarded(|| panic!("provider boundary test")),
            KERNEL_NATIVE_STATUS_GENERIC
        );
    }

    #[derive(Debug, Default)]
    struct StoreSpy {
        inner: InMemory,
        gets: Mutex<Vec<GetOptions>>,
        put_modes: Mutex<Vec<PutMode>>,
        put_options: Mutex<Vec<PutOptions>>,
        range_requests: Mutex<Vec<(Path, Vec<Range<u64>>)>>,
        range_results: Option<Vec<Bytes>>,
        deletes: AtomicU64,
        delete_inputs: Arc<Mutex<Vec<Path>>>,
        delete_case: &'static str,
        copies: Mutex<Vec<(Path, Path, CopyOptions)>>,
        renames: Mutex<Vec<(Path, Path, object_store::RenameOptions)>>,
        delimiter_requests: Mutex<Vec<Option<Path>>>,
        delimiter_override: Option<ListResult>,
        multipart_options: Mutex<Vec<(Path, PutMultipartOptions)>>,
        upload_probe: Option<Arc<v4::UploadProbe>>,
        multipart_unimplemented: bool,
        response_version: bool,
        response_attributes: Option<object_store::Attributes>,
        list_requests: Mutex<Vec<(Option<Path>, Option<Path>)>>,
        polls: Arc<AtomicU64>,
        stream_drops: Arc<AtomicU64>,
        body_polls: Arc<AtomicU64>,
        listing_override: Option<Vec<ObjectMeta>>,
        fail_after: Option<usize>,
        fail_open: bool,
        large_metadata: bool,
        forbid_body: bool,
    }

    impl fmt::Display for StoreSpy {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("StoreSpy")
        }
    }

    struct CountedListing {
        inner: BoxStream<'static, object_store::Result<ObjectMeta>>,
        polls: Arc<AtomicU64>,
        drops: Arc<AtomicU64>,
        fail_after: Option<usize>,
        emitted: usize,
    }

    impl Stream for CountedListing {
        type Item = object_store::Result<ObjectMeta>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.fail_after == Some(self.emitted) {
                self.emitted += 1;
                return Poll::Ready(Some(Err(object_store::Error::Generic {
                    store: "StoreSpy",
                    source: "listing failure".into(),
                })));
            }
            let result = self.inner.as_mut().poll_next(context);
            if matches!(result, Poll::Ready(Some(_))) {
                self.emitted += 1;
            }
            result
        }
    }

    impl Drop for CountedListing {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl StoreSpy {
        fn listing(
            &self,
            prefix: Option<&Path>,
            offset: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            assert!(!self.fail_open, "test native open failure");
            self.list_requests
                .lock()
                .unwrap()
                .push((prefix.cloned(), offset.cloned()));
            let inner = match &self.listing_override {
                Some(objects) => futures::stream::iter(objects.clone().into_iter().map(Ok)).boxed(),
                None => match offset {
                    Some(offset) => self.inner.list_with_offset(prefix, offset),
                    None => self.inner.list(prefix),
                },
            };
            CountedListing {
                inner,
                polls: self.polls.clone(),
                drops: self.stream_drops.clone(),
                fail_after: self.fail_after,
                emitted: 0,
            }
            .boxed()
        }
    }

    #[async_trait]
    impl ObjectStore for StoreSpy {
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.lock().unwrap().push(options.clone());
            let full_head = options.head && options.range.is_none();
            let mut result = self.inner.get_opts(path, options).await?;
            if self.response_version {
                result.meta.version = Some(String::new());
            }
            if let Some(attributes) = &self.response_attributes {
                result.attributes = attributes.clone();
            }
            if self.large_metadata {
                result.meta.size = MAX_BODY_BYTES as u64 + 1;
                if full_head {
                    result.range = 0..result.meta.size;
                }
            }
            if self.forbid_body {
                let polls = self.body_polls.clone();
                result.payload = object_store::GetResultPayload::Stream(
                    futures::stream::poll_fn(move |_| {
                        polls.fetch_add(1, Ordering::SeqCst);
                        panic!("HEAD body must never be polled")
                    })
                    .boxed(),
                );
            }
            Ok(result)
        }

        async fn put_opts(
            &self,
            path: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.put_modes.lock().unwrap().push(options.mode.clone());
            self.put_options.lock().unwrap().push(options.clone());
            let mut result = self.inner.put_opts(path, payload, options).await?;
            if self.response_version {
                result.version = Some(String::new());
            }
            Ok(result)
        }

        async fn get_ranges(
            &self,
            path: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.range_requests
                .lock()
                .unwrap()
                .push((path.clone(), ranges.to_vec()));
            if let Some(results) = &self.range_results {
                return Ok(results.clone());
            }
            self.inner.get_ranges(path, ranges).await
        }

        fn delete_stream(
            &self,
            paths: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            let inputs = self.delete_inputs.clone();
            let paths = paths
                .map(move |result| {
                    if let Ok(path) = &result {
                        inputs.lock().unwrap().push(path.clone());
                    }
                    result
                })
                .boxed();
            if !self.delete_case.is_empty() {
                let case = self.delete_case;
                return futures::stream::once(async move {
                    let paths = paths.try_collect::<Vec<_>>().await.unwrap();
                    let error = object_store::Error::Generic {
                        store: "StoreSpy",
                        source: "aggregate delete failure".into(),
                    };
                    let results = if case == "aggregate" {
                        vec![Ok(paths[0].clone()), Err(error)]
                    } else {
                        vec![
                            Err(object_store::Error::NotFound {
                                path: paths[0].to_string(),
                                source: "item missing".into(),
                            }),
                            Ok(paths[1].clone()),
                            Err(error),
                            Ok(paths[2].clone()),
                        ]
                    };
                    futures::stream::iter(results)
                })
                .flatten()
                .boxed();
            }
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.listing(prefix, None)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.listing(prefix, Some(offset))
        }

        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.multipart_options
                .lock()
                .unwrap()
                .push((path.clone(), options.clone()));
            if self.multipart_unimplemented {
                return Err(object_store::Error::NotImplemented {
                    operation: "put_multipart_opts".into(),
                    implementer: "StoreSpy".into(),
                });
            }
            let inner = self.inner.put_multipart_opts(path, options).await?;
            match &self.upload_probe {
                Some(probe) => Ok(Box::new(v4::SpyUpload::new(inner, probe.clone()))),
                None => Ok(inner),
            }
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.delimiter_requests
                .lock()
                .unwrap()
                .push(prefix.cloned());
            if let Some(result) = &self.delimiter_override {
                return Ok(ListResult {
                    objects: result.objects.clone(),
                    common_prefixes: result.common_prefixes.clone(),
                    extensions: Default::default(),
                });
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.copies
                .lock()
                .unwrap()
                .push((from.clone(), to.clone(), options.clone()));
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::RenameOptions,
        ) -> object_store::Result<()> {
            self.renames
                .lock()
                .unwrap()
                .push((from.clone(), to.clone(), options.clone()));
            self.inner.rename_opts(from, to, options).await
        }
    }

    fn seed_listing(store: &StoreSpy, count: usize) {
        runtime().unwrap().block_on(async {
            for index in (0..count).rev() {
                store
                    .inner
                    .put(
                        &Path::from(format!("items/item-{index:03}")),
                        Bytes::new().into(),
                    )
                    .await
                    .unwrap();
            }
        });
    }

    #[test]
    fn get_forwards_native_options_and_memory_store_returns_bounded_offset_suffix_and_head() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        runtime()
            .unwrap()
            .block_on(store.inner.put(
                &Path::from("data"),
                Bytes::from_static(b"0123456789").into(),
            ))
            .unwrap();
        let fixture = Fixture::from_store(store.clone());
        let cases = [
            (0, 0, 0, 0, None, b"0123456789".as_slice()),
            (
                0,
                1,
                2,
                6,
                Some(GetRange::Bounded(2..6)),
                b"2345".as_slice(),
            ),
            (0, 2, 4, 0, Some(GetRange::Offset(4)), b"456789".as_slice()),
            (0, 3, 0, 3, Some(GetRange::Suffix(3)), b"789".as_slice()),
            (1, 0, 0, 0, None, b"".as_slice()),
            (1, 1, 2, 6, Some(GetRange::Bounded(2..6)), b"".as_slice()),
        ];
        for (head, kind, start, end, range, expected) in cases {
            let native = runtime()
                .unwrap()
                .block_on(store.inner.get_opts(
                    &Path::from("data"),
                    GetOptions {
                        head: head == 1,
                        range: range.clone(),
                        ..Default::default()
                    },
                ))
                .unwrap();
            let (status, capture) = fixture.get_opts(
                "data",
                KernelNativeGetOptionsV3 {
                    head,
                    range_kind: kind,
                    start,
                    end,
                },
            );
            assert_eq!(status, 0);
            assert_eq!(capture.metadata[0].size, 10);
            assert_eq!(capture.bodies, [expected]);
            assert_eq!(capture.ranges, [native.range]);
            let calls = store.gets.lock().unwrap();
            let observed = calls.last().unwrap();
            assert_eq!(observed.head, head == 1);
            assert_eq!(observed.range, range);
        }
        assert_eq!(store.gets.lock().unwrap().len(), 6);
        for options in [
            KernelNativeGetOptionsV3 {
                head: 2,
                ..Default::default()
            },
            KernelNativeGetOptionsV3 {
                range_kind: 4,
                ..Default::default()
            },
            KernelNativeGetOptionsV3 {
                range_kind: 1,
                start: 4,
                end: 3,
                ..Default::default()
            },
        ] {
            assert_eq!(fixture.get_opts("data", options).0, 3);
        }
        assert_eq!(store.gets.lock().unwrap().len(), 6);
    }

    #[test]
    fn large_object_metadata_allows_small_native_range_and_head_never_polls_body() {
        let _serial = TEST_LOCK.lock().unwrap();
        for head in [false, true] {
            let store = Arc::new(StoreSpy {
                large_metadata: true,
                forbid_body: head,
                ..Default::default()
            });
            runtime()
                .unwrap()
                .block_on(store.inner.put(
                    &Path::from("data"),
                    Bytes::from_static(b"0123456789").into(),
                ))
                .unwrap();
            let fixture = Fixture::from_store(store.clone());
            let options = if head {
                KernelNativeGetOptionsV3 {
                    head: 1,
                    ..Default::default()
                }
            } else {
                KernelNativeGetOptionsV3 {
                    range_kind: 1,
                    start: 2,
                    end: 4,
                    ..Default::default()
                }
            };
            let (status, capture) = fixture.get_opts("data", options);
            assert_eq!(status, 0);
            assert_eq!(capture.metadata[0].size, MAX_BODY_BYTES as u64 + 1);
            assert_eq!(
                capture.bodies[0],
                if head {
                    b"".as_slice()
                } else {
                    b"23".as_slice()
                }
            );
            assert_eq!(
                capture.ranges,
                [if head {
                    0..MAX_BODY_BYTES as u64 + 1
                } else {
                    2..4
                }]
            );
            assert_eq!(store.body_polls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn put_modes_reach_native_store_create_conflicts_overwrite_and_delete_are_readable_and_counted()
    {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        let baseline = prototype_callback_count();
        assert_eq!(fixture.put("data", b"first", KERNEL_NATIVE_PUT_CREATE), 0);
        assert_eq!(
            fixture.put("data", b"conflict", KERNEL_NATIVE_PUT_CREATE),
            2
        );
        assert_eq!(fixture.get("data").1.bodies, [b"first"]);
        assert_eq!(
            fixture.put("data", b"replacement", KERNEL_NATIVE_PUT_OVERWRITE),
            0
        );
        assert_eq!(fixture.get("data").1.bodies, [b"replacement"]);
        assert_eq!(
            *store.put_modes.lock().unwrap(),
            [PutMode::Create, PutMode::Create, PutMode::Overwrite]
        );
        assert_eq!(fixture.delete("data"), 0);
        assert_eq!(store.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.get("data").0, 1);
        let mut cursor = fixture.open("", "").unwrap();
        assert_eq!(prototype_callback_count() - baseline, 7);
        assert_eq!(cursor.next().0, 0);
        assert_eq!(prototype_callback_count() - baseline, 8);
    }

    #[test]
    fn listing_forwards_prefix_and_offset_and_full_final_batch_requires_empty_eof_advance() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        seed_listing(&store, 129);
        let fixture = Fixture::from_store(store.clone());
        let mut cursor = fixture.open("items", "items/item-000").unwrap();
        assert_eq!(
            store.list_requests.lock().unwrap().as_slice(),
            &[(
                Some(Path::from("items")),
                Some(Path::from("items/item-000"))
            )]
        );
        let (status, page, more) = cursor.next();
        assert_eq!((status, page.metadata.len(), more), (0, 128, 1));
        assert_eq!(page.metadata[0].location, "items/item-001");
        assert_eq!(page.metadata[127].location, "items/item-128");
        let (status, page, more) = cursor.next();
        assert_eq!((status, page.metadata.len(), more), (0, 0, 0));
        assert_eq!(store.polls.load(Ordering::SeqCst), 129);
        assert_eq!(cursor.next().2, 0);
        assert_eq!(store.polls.load(Ordering::SeqCst), 129);
        drop(cursor);
        let mut empty = fixture.open("missing", "").unwrap();
        assert_eq!(empty.next().2, 0);
    }

    #[test]
    fn cursor_open_failure_leaves_output_untouched_and_partial_native_error_is_cleaned_on_close() {
        let _serial = TEST_LOCK.lock().unwrap();
        let failing = Arc::new(StoreSpy {
            fail_open: true,
            ..Default::default()
        });
        let fixture = Fixture::from_store(failing.clone());
        let mut output = fixture.0.context;
        // SAFETY: Native open panics before cursor allocation; output is exclusively lent.
        assert_eq!(
            unsafe { list_open(fixture.0.context, string(""), string(""), &mut output) },
            3
        );
        assert_eq!(output, fixture.0.context);
        assert_eq!(Arc::strong_count(&failing), 2);
        assert_eq!(failing.stream_drops.load(Ordering::SeqCst), 0);
        let store = Arc::new(StoreSpy {
            fail_after: Some(2),
            ..Default::default()
        });
        seed_listing(&store, 4);
        let fixture = Fixture::from_store(store.clone());
        let mut cursor = fixture.open("items", "").unwrap();
        let (status, partial, more) = cursor.next();
        assert_eq!((status, partial.metadata.len(), more), (3, 2, u32::MAX));
        assert_eq!(store.polls.load(Ordering::SeqCst), 3);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 0);
        drop(cursor);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&store), 2);
        let cursor = fixture.open("items", "").unwrap();
        let mut calls = 0_u32;
        let mut more = u32::MAX;
        // SAFETY: This owner exclusively lends cursor, counter and flag until sink failure.
        assert_eq!(
            unsafe {
                list_next(
                    cursor.0,
                    ptr::from_mut(&mut calls).cast(),
                    reject_list,
                    &mut more,
                )
            },
            3
        );
        assert_eq!((calls, more), (1, u32::MAX));
        assert_eq!(store.polls.load(Ordering::SeqCst), 4);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 1);
        drop(cursor);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&store), 2);
    }

    #[test]
    fn cursor_close_on_another_thread_inside_runtime_drops_stream_and_store_without_polling() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        seed_listing(&store, 300);
        let fixture = Fixture::from_store(store.clone());
        let cursor = fixture.open("items", "").unwrap();
        assert_eq!(Arc::strong_count(&store), 3);
        std::thread::spawn(move || {
            let other_runtime = Builder::new_current_thread().enable_all().build().unwrap();
            other_runtime.block_on(async move { drop(cursor) });
        })
        .join()
        .unwrap();
        assert_eq!(store.polls.load(Ordering::SeqCst), 0);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&store), 2);
        assert_eq!(fixture.list("items").1.metadata.len(), 300);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn listing_preserves_unsorted_native_order_and_rejects_oversized_output_paths_before_sink() {
        let _serial = TEST_LOCK.lock().unwrap();
        let context = create_memory().unwrap();
        let metadata = runtime()
            .unwrap()
            .block_on(context.store.head(&Path::from(INITIAL_PATH)))
            .unwrap();
        let objects = ["items/c", "items/a", "items/b"].map(|path| ObjectMeta {
            location: Path::from(path),
            ..metadata.clone()
        });
        let store = Arc::new(StoreSpy {
            listing_override: Some(objects.to_vec()),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let (status, capture) = fixture.list("items");
        assert_eq!(status, 0);
        assert_eq!(
            capture
                .metadata
                .into_iter()
                .map(|meta| meta.location)
                .collect::<Vec<_>>(),
            ["items/c", "items/a", "items/b"]
        );
        let store = Arc::new(StoreSpy {
            listing_override: Some(vec![ObjectMeta {
                location: Path::from("x".repeat(MAX_PATH_BYTES + 1)),
                ..metadata
            }]),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let mut cursor = fixture.open("", "").unwrap();
        let (status, capture, more) = cursor.next();
        assert_eq!((status, capture.metadata.len(), more), (4, 0, u32::MAX));
        drop(cursor);
        assert_eq!(store.stream_drops.load(Ordering::SeqCst), 1);
    }

    include!("provider/v4_tests.rs");
}
