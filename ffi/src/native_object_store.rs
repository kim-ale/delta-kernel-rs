//! Experimental native v1 [`ObjectStore`] adapter, using only the provider's C descriptor.
//!
//! The provider owns storage, authentication, runtime, and allocations. Kernel copies borrowed
//! sink output and retains the adopted context until the last handle, builder, engine, stream,
//! or blocking operation releases it. Synchronous I/O callbacks run on Tokio blocking workers;
//! release can run on any thread. Dropping an async operation does not cancel a native call.
//!
//! Supported operations are full GET/HEAD, ranges sliced from a buffered full GET, ordered
//! recursive LIST pages, atomic Create/Overwrite PUT, and individual DELETE. Bodies are limited
//! to 64 MiB, paths to 64 KiB, and LIST pages to 128 objects. No ETag or version is reported.
//! Conditional/versioned reads, write tags/attributes/extensions, multipart, copy, and delimiter
//! listing return explicit unsupported errors. Both native modules must remain loaded for the
//! lifetime of all references and callbacks; this prototype does not implement module unloading.

use std::ffi::c_void;
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{
    self, Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult, TagSet,
};
use delta_kernel::{KernelError, KernelResult};
use delta_kernel_ffi_macros::handle_descriptor;
pub use delta_kernel_native_store_abi::*;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;

const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 64 * 1024;
const LIST_PAGE_SIZE: u32 = 128;

/// Kernel-owned adapter for an adopted v1 native provider, opaque across the C ABI.
#[derive(Clone)]
pub struct NativeObjectStore {
    context: Arc<NativeContext>,
}

/// Shared native-store handle. Release with [`free_native_object_store`].
#[handle_descriptor(target=NativeObjectStore, mutable=false, sized=true)]
pub struct SharedNativeObjectStore;

struct NativeContext {
    descriptor: KernelNativeObjectStoreDescriptorV1,
}

struct GetSinkState {
    path: String,
    head: bool,
    called: bool,
    output: Option<(ObjectMeta, Bytes)>,
    error: Option<ObjectStoreError>,
}

struct ListSinkState {
    prefix: Path,
    start_after: String,
    objects: Vec<ObjectMeta>,
    error: Option<ObjectStoreError>,
}

struct ListPage {
    objects: Vec<ObjectMeta>,
    has_more: bool,
}

// SAFETY: adoption requires concurrent, non-unwinding callbacks and a context that remains valid
// on any thread until final release. Kernel never dereferences the provider's context.
unsafe impl Send for NativeContext {}
// SAFETY: the same adoption contract permits overlapping I/O calls; release runs only after
// every reference, including each blocking worker's reference, has been dropped.
unsafe impl Sync for NativeContext {}

impl Drop for NativeContext {
    fn drop(&mut self) {
        if let Some(release) = self.descriptor.release {
            // SAFETY: the descriptor was accepted exactly once; this is its final owned reference.
            unsafe { release(self.descriptor.context) };
        }
    }
}

impl fmt::Debug for NativeObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeObjectStore(v1)")
    }
}

impl fmt::Display for NativeObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeObjectStore(v1)")
    }
}

#[async_trait]
impl ObjectStore for NativeObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let mode = native_put_mode(&options)?;
        let body = copy_put_payload(payload)?;
        let path = location.to_string();
        let store = self.clone();
        run_blocking(move || store.put_sync(&path, &body, mode)).await?;
        Ok(object_store::delta_kernel_compat::empty_put_result())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(not_supported("multipart upload"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        validate_get_options(&options)?;
        let path = location.to_string();
        let head = options.head;
        let store = self.clone();
        let (meta, body) = run_blocking(move || store.get_sync(&path, head)).await?;
        make_get_result(meta, body, options)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let path = location.to_string();
                    run_blocking(move || store.delete_sync(&path)).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.list_from(prefix, String::new())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.list_from(prefix, offset.to_string())
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        Err(not_supported("delimiter listing"))
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        Err(not_supported("copy"))
    }
}

impl NativeObjectStore {
    /// # Safety
    ///
    /// The descriptor and provider must satisfy [`get_native_object_store`]'s contract.
    unsafe fn adopt(descriptor: *const KernelNativeObjectStoreDescriptorV1) -> KernelResult<Self> {
        let invalid = || KernelError::generic("invalid native object store descriptor");
        if descriptor.is_null() {
            return Err(invalid());
        }
        // SAFETY: only the two readable header fields are inspected before layout validation.
        let abi_version = unsafe { descriptor.cast::<u32>().read_unaligned() };
        let struct_size = unsafe { descriptor.cast::<u32>().add(1).read_unaligned() };
        if abi_version != KERNEL_NATIVE_STORE_ABI_V1
            || struct_size as usize != size_of::<KernelNativeObjectStoreDescriptorV1>()
            || !descriptor.is_aligned()
        {
            return Err(invalid());
        }
        // SAFETY: a matching header promises readable, initialized storage for this exact layout.
        let descriptor = unsafe { *descriptor };
        if descriptor.context.is_null()
            || descriptor.get.is_none()
            || descriptor.list.is_none()
            || descriptor.put.is_none()
            || descriptor.delete_object.is_none()
            || descriptor.release.is_none()
        {
            return Err(invalid());
        }
        Ok(Self {
            context: Arc::new(NativeContext { descriptor }),
        })
    }

    fn get_sync(&self, path: &str, head: bool) -> ObjectStoreResult<(ObjectMeta, Bytes)> {
        validate_input_path(path)?;
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .get
            .ok_or_else(|| generic_error("native GET callback is missing"))?;
        let mut state = GetSinkState {
            path: path.to_string(),
            head,
            called: false,
            output: None,
            error: None,
        };
        let flags = if head {
            KERNEL_NATIVE_GET_HEAD
        } else {
            KERNEL_NATIVE_GET_FULL
        };
        // SAFETY: input and sink state live through this synchronous call. The provider invokes
        // sinks synchronously and cannot retain them; the context reference keeps it alive.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(path),
                flags,
                (&mut state as *mut GetSinkState).cast(),
                get_sink,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, path)?;
        state
            .output
            .ok_or_else(|| generic_error("native GET did not invoke its sink exactly once"))
    }

    fn list_page_sync(&self, prefix: &str, start_after: &str) -> ObjectStoreResult<ListPage> {
        validate_input_path(prefix)?;
        validate_input_path(start_after)?;
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .list
            .ok_or_else(|| generic_error("native LIST callback is missing"))?;
        let mut state = ListSinkState {
            prefix: Path::parse(prefix).map_err(|_| generic_error("invalid native LIST prefix"))?,
            start_after: start_after.to_string(),
            objects: Vec::with_capacity(LIST_PAGE_SIZE as usize),
            error: None,
        };
        let mut has_more = 0;
        // SAFETY: input strings, output flag, and sink state live until callback return. Each
        // sink copies its borrowed metadata without taking ownership of provider allocations.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(prefix),
                string_slice(start_after),
                LIST_PAGE_SIZE,
                (&mut state as *mut ListSinkState).cast(),
                list_sink,
                &mut has_more,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, prefix)?;
        if has_more > 1 || (has_more == 1 && state.objects.is_empty()) {
            return Err(generic_error("invalid native LIST continuation"));
        }
        Ok(ListPage {
            objects: state.objects,
            has_more: has_more == 1,
        })
    }

    fn put_sync(&self, path: &str, body: &Bytes, mode: u32) -> ObjectStoreResult<()> {
        validate_input_path(path)?;
        if body.len() > MAX_BODY_BYTES {
            return Err(not_supported("PUT bodies larger than 64 MiB"));
        }
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .put
            .ok_or_else(|| generic_error("native PUT callback is missing"))?;
        // SAFETY: both owned inputs remain alive throughout the callback; no slices leave this
        // blocking worker, and the retained context cannot be released during the call.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(path),
                KernelNativeByteSliceV1 {
                    ptr: body.as_ptr(),
                    len: body.len(),
                },
                mode,
            )
        };
        check_status(status, path)
    }

    fn delete_sync(&self, path: &str) -> ObjectStoreResult<()> {
        validate_input_path(path)?;
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .delete_object
            .ok_or_else(|| generic_error("native DELETE callback is missing"))?;
        // SAFETY: path is borrowed only for the synchronous call and context is retained.
        let status = unsafe { callback(descriptor.context, string_slice(path)) };
        check_status(status, path)
    }

    fn list_from(
        &self,
        prefix: Option<&Path>,
        start_after: String,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix = prefix.map(ToString::to_string).unwrap_or_default();
        stream::try_unfold(
            (self.clone(), prefix, start_after, false),
            |(store, prefix, start_after, done)| async move {
                if done {
                    return Ok::<_, ObjectStoreError>(None);
                }
                let worker_store = store.clone();
                let worker_prefix = prefix.clone();
                let worker_offset = start_after.clone();
                let page = run_blocking(move || {
                    worker_store.list_page_sync(&worker_prefix, &worker_offset)
                })
                .await?;
                let next_offset = page
                    .objects
                    .last()
                    .map(|meta| meta.location.to_string())
                    .unwrap_or(start_after);
                let objects = stream::iter(page.objects.into_iter().map(Ok::<_, ObjectStoreError>));
                Ok(Some((
                    objects,
                    (store, prefix, next_offset, !page.has_more),
                )))
            },
        )
        .try_flatten()
        .boxed()
    }
}

/// Adopt an independent native v1 object-store descriptor and return an owned shared handle.
///
/// `descriptor` is borrowed only during this call and copied on success. Only success transfers
/// ownership of its provider context; Kernel then calls `release` exactly once after its final
/// reference is dropped. Rejection never calls any descriptor callback or adopts the context.
/// Adoption itself performs no I/O. `allocate_error` is used only to report an error during this
/// call and is not retained. The caller owns the returned handle and must free it.
///
/// # Errors
///
/// Returns a generic Kernel error for null descriptors, unknown versions, non-exact sizes,
/// misaligned v1 storage, null context, or any missing callback.
///
/// # Safety
///
/// A non-null descriptor must expose two readable initialized `u32` header fields. When those
/// fields claim the exact v1 layout, it must also expose the full initialized descriptor. Invalid
/// non-null pointers are caller violations, not recoverable errors. After successful adoption,
/// the context must not be freed or adopted again by the caller. All callbacks must be thread
/// safe, support concurrent calls, never unwind, and follow the ABI sink/input lifetime contract.
/// Sinks must finish before callback return and must not be retained or invoked concurrently
/// within one operation. Release must be valid on any thread. Provider code and Kernel code must
/// stay loaded until all handles, engines, streams, and native calls have completed.
#[no_mangle]
pub unsafe extern "C" fn get_native_object_store(
    descriptor: *const KernelNativeObjectStoreDescriptorV1,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedNativeObjectStore>> {
    unsafe { NativeObjectStore::adopt(descriptor) }
        .map(|store| Arc::new(store).into())
        .into_extern_result(&allocate_error)
}

/// Consume a shared native-store handle, releasing the context only if no references remain.
///
/// Builders, engines, streams, and in-flight blocking calls retain independent references.
///
/// # Safety
///
/// `store` must be valid, must not be used concurrently during this call, and must not be used or
/// freed again afterwards. The provider's release function must remain callable on this thread.
#[no_mangle]
pub unsafe extern "C" fn free_native_object_store(store: Handle<SharedNativeObjectStore>) {
    unsafe { store.drop_handle() };
}

async fn run_blocking<T, Operation>(operation: Operation) -> ObjectStoreResult<T>
where
    T: Send + 'static,
    Operation: FnOnce() -> ObjectStoreResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| generic_error("native object store blocking task failed"))?
}

fn string_slice(value: &str) -> KernelNativeStringSliceV1 {
    KernelNativeStringSliceV1 {
        ptr: value.as_ptr().cast(),
        len: value.len(),
    }
}

fn validate_input_path(path: &str) -> ObjectStoreResult<()> {
    if path.len() > MAX_PATH_BYTES {
        return Err(not_supported("paths larger than 64 KiB"));
    }
    Ok(())
}

fn generic_error(message: &'static str) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "NativeObjectStore",
        source: message.into(),
    }
}

fn not_supported(operation: &str) -> ObjectStoreError {
    ObjectStoreError::NotSupported {
        source: format!("native object store v1 does not support {operation}").into(),
    }
}

fn check_status(status: i32, path: &str) -> ObjectStoreResult<()> {
    match status {
        KERNEL_NATIVE_STATUS_OK => Ok(()),
        KERNEL_NATIVE_STATUS_NOT_FOUND => Err(ObjectStoreError::NotFound {
            path: path.to_string(),
            source: "native object not found".into(),
        }),
        KERNEL_NATIVE_STATUS_ALREADY_EXISTS => Err(ObjectStoreError::AlreadyExists {
            path: path.to_string(),
            source: "native object already exists".into(),
        }),
        KERNEL_NATIVE_STATUS_NOT_SUPPORTED => {
            Err(not_supported("the requested provider operation"))
        }
        _ => Err(generic_error("native object store callback failed")),
    }
}

fn validate_get_options(options: &GetOptions) -> ObjectStoreResult<()> {
    if options.version.is_some()
        || options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || !options.extensions.is_empty()
    {
        return Err(not_supported(
            "versioned/conditional reads or request extensions",
        ));
    }
    if let Some(range) = &options.range {
        range
            .is_valid()
            .map_err(|_| generic_error("invalid native GET range"))?;
    }
    Ok(())
}

fn native_put_mode(options: &PutOptions) -> ObjectStoreResult<u32> {
    if options.tags != TagSet::default()
        || !options.attributes.is_empty()
        || !options.extensions.is_empty()
    {
        return Err(not_supported(
            "write tags, attributes, or request extensions",
        ));
    }
    match options.mode {
        PutMode::Overwrite => Ok(KERNEL_NATIVE_PUT_OVERWRITE),
        PutMode::Create => Ok(KERNEL_NATIVE_PUT_CREATE),
        PutMode::Update(_) => Err(not_supported("conditional writes")),
    }
}

fn copy_put_payload(payload: PutPayload) -> ObjectStoreResult<Bytes> {
    let size = payload.iter().try_fold(0usize, |size, part| {
        size.checked_add(part.len())
            .filter(|size| *size <= MAX_BODY_BYTES)
            .ok_or_else(|| not_supported("PUT bodies larger than 64 MiB"))
    })?;
    let mut body = Vec::with_capacity(size);
    for part in payload {
        body.extend_from_slice(&part);
    }
    Ok(Bytes::from(body))
}

fn make_get_result(
    meta: ObjectMeta,
    body: Bytes,
    options: GetOptions,
) -> ObjectStoreResult<GetResult> {
    let range = if options.head {
        0..meta.size
    } else {
        match options.range {
            Some(range) => range
                .as_range(meta.size)
                .map_err(|_| generic_error("invalid native GET range"))?,
            None => 0..meta.size,
        }
    };
    let payload = if options.head {
        GetResultPayload::Stream(stream::empty().boxed())
    } else {
        let start = usize::try_from(range.start)
            .map_err(|_| generic_error("native GET range exceeds addressable memory"))?;
        let end = usize::try_from(range.end)
            .map_err(|_| generic_error("native GET range exceeds addressable memory"))?;
        if body.get(start..end).is_none() {
            return Err(generic_error("native GET body does not cover its range"));
        }
        let body = body.slice(start..end);
        GetResultPayload::Stream(stream::once(std::future::ready(Ok(body))).boxed())
    };
    Ok(object_store::delta_kernel_compat::get_result(
        payload,
        meta,
        range,
        Attributes::new(),
    ))
}

unsafe fn copy_meta(meta: *const KernelNativeObjectMetaV1) -> ObjectStoreResult<ObjectMeta> {
    if meta.is_null() || !meta.is_aligned() {
        return Err(generic_error("invalid native object metadata pointer"));
    }
    // SAFETY: the provider promises initialized metadata valid throughout the sink call.
    let meta = unsafe { &*meta };
    if meta.location.len == 0 || meta.location.len > MAX_PATH_BYTES || meta.location.ptr.is_null() {
        return Err(generic_error("invalid native object location slice"));
    }
    // SAFETY: the provider guarantees these bytes are readable through the sink return.
    let location =
        unsafe { std::slice::from_raw_parts(meta.location.ptr.cast::<u8>(), meta.location.len) };
    let location = std::str::from_utf8(location)
        .map_err(|_| generic_error("native object location is not UTF-8"))?;
    let path = Path::parse(location).map_err(|_| generic_error("invalid native object path"))?;
    if path.as_ref() != location {
        return Err(generic_error("native object path is not store-relative"));
    }
    let last_modified = DateTime::<Utc>::from_timestamp_millis(meta.last_modified_unix_ms)
        .ok_or_else(|| generic_error("invalid native object modification timestamp"))?;
    Ok(ObjectMeta {
        location: path,
        size: meta.size,
        last_modified,
        e_tag: None,
        version: None,
    })
}

unsafe extern "C" fn get_sink(
    context: *mut c_void,
    meta: *const KernelNativeObjectMetaV1,
    body: KernelNativeByteSliceV1,
) -> i32 {
    // SAFETY: only get_sync supplies this live state, and the provider cannot retain the sink.
    let state = unsafe { &mut *context.cast::<GetSinkState>() };
    if state.called {
        state.error = Some(generic_error("native GET invoked its sink more than once"));
        return KERNEL_NATIVE_STATUS_GENERIC;
    }
    state.called = true;
    let output = (|| {
        let meta = unsafe { copy_meta(meta) }?;
        if meta.location.as_ref() != state.path {
            return Err(generic_error(
                "native GET returned a different object location",
            ));
        }
        if (state.head && body.len != 0)
            || (!state.head && meta.size != body.len as u64)
            || body.len > MAX_BODY_BYTES
            || (body.len != 0 && body.ptr.is_null())
        {
            return Err(generic_error("invalid native GET body length or pointer"));
        }
        let bytes = if body.len == 0 {
            Bytes::new()
        } else {
            // SAFETY: the provider guarantees readable bytes; the length is bounded above.
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(body.ptr, body.len) })
        };
        Ok((meta, bytes))
    })();
    match output {
        Ok(output) => {
            state.output = Some(output);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

unsafe extern "C" fn list_sink(context: *mut c_void, meta: *const KernelNativeObjectMetaV1) -> i32 {
    // SAFETY: only list_page_sync supplies this live state; sink calls cannot overlap or escape.
    let state = unsafe { &mut *context.cast::<ListSinkState>() };
    if state.error.is_some() {
        return KERNEL_NATIVE_STATUS_GENERIC;
    }
    let output = (|| {
        if state.objects.len() >= LIST_PAGE_SIZE as usize {
            return Err(generic_error("native LIST exceeded its page limit"));
        }
        let meta = unsafe { copy_meta(meta) }?;
        if !meta.location.prefix_matches(&state.prefix)
            || meta.location.as_ref() <= state.start_after.as_str()
            || state
                .objects
                .last()
                .is_some_and(|previous| previous.location >= meta.location)
        {
            return Err(generic_error(
                "native LIST violated prefix or strict ordering",
            ));
        }
        Ok(meta)
    })();
    match output {
        Ok(meta) => {
            state.objects.push(meta);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use delta_kernel::object_store::{Attribute, GetRange, UpdateVersion};
    use rstest::rstest;

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_contains, ok_or_panic, recover_error,
    };
    use crate::{kernel_string_slice, KernelStringSlice};

    #[derive(Default)]
    struct Probe {
        releases: AtomicUsize,
        io_calls: AtomicUsize,
        put_mode: AtomicU32,
        put_body: Mutex<Vec<u8>>,
        deleted: Mutex<String>,
        released: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    struct CallGate {
        entered: std::sync::mpsc::Sender<()>,
        resume: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum GetCase {
        Valid,
        Empty,
        MissingSink,
        DuplicateSink,
        NullMeta,
        EmptyLocation,
        InvalidUtf8,
        WrongLocation,
        AbsoluteLocation,
        OversizedLocation,
        InvalidTimestamp,
        SizeMismatch,
        HeadBody,
        NullBody,
        OversizedBody,
    }

    struct Provider {
        probe: Arc<Probe>,
        get_case: GetCase,
        list_names: Vec<String>,
        has_more: u32,
        status: i32,
        gate: Option<Arc<CallGate>>,
    }

    impl Default for Provider {
        fn default() -> Self {
            Self {
                probe: Arc::new(Probe::default()),
                get_case: GetCase::Valid,
                list_names: vec!["table/a".into(), "table/b".into()],
                has_more: 0,
                status: KERNEL_NATIVE_STATUS_OK,
                gate: None,
            }
        }
    }

    fn descriptor_for(provider: Provider) -> (KernelNativeObjectStoreDescriptorV1, Arc<Probe>) {
        let probe = provider.probe.clone();
        let descriptor = KernelNativeObjectStoreDescriptorV1 {
            abi_version: KERNEL_NATIVE_STORE_ABI_V1,
            struct_size: size_of::<KernelNativeObjectStoreDescriptorV1>() as u32,
            context: Box::into_raw(Box::new(provider)).cast(),
            get: Some(provider_get),
            list: Some(provider_list),
            put: Some(provider_put),
            delete_object: Some(provider_delete),
            release: Some(provider_release),
        };
        (descriptor, probe)
    }

    fn handle_for(provider: Provider) -> (Handle<SharedNativeObjectStore>, Arc<Probe>) {
        let (descriptor, probe) = descriptor_for(provider);
        let handle = ok_or_panic(unsafe { get_native_object_store(&descriptor, allocate_err) });
        (handle, probe)
    }

    unsafe fn input_string(value: KernelNativeStringSliceV1) -> String {
        if value.len == 0 {
            return String::new();
        }
        let bytes = unsafe { std::slice::from_raw_parts(value.ptr.cast(), value.len) };
        std::str::from_utf8(bytes).unwrap().to_string()
    }

    unsafe extern "C" fn provider_release(context: *mut c_void) {
        let provider = unsafe { Box::from_raw(context.cast::<Provider>()) };
        provider.probe.releases.fetch_add(1, Ordering::SeqCst);
        if let Some(released) = provider.probe.released.lock().unwrap().take() {
            let _ = released.send(());
        };
    }

    unsafe extern "C" fn provider_get(
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
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &provider.gate {
            gate.entered.send(()).unwrap();
            gate.resume.lock().unwrap().recv().unwrap();
        }
        if provider.get_case == GetCase::MissingSink {
            return provider.status;
        }
        let mut name = unsafe { input_string(path) }.into_bytes();
        match provider.get_case {
            GetCase::InvalidUtf8 => name = vec![0xff],
            GetCase::WrongLocation => name = b"other/object".to_vec(),
            GetCase::AbsoluteLocation => name = b"/table/a".to_vec(),
            _ => {}
        }
        let mut body = b"data".to_vec();
        if provider.get_case == GetCase::Empty
            || (flags == KERNEL_NATIVE_GET_HEAD && provider.get_case != GetCase::HeadBody)
        {
            body.clear();
        }
        let mut meta = KernelNativeObjectMetaV1 {
            location: KernelNativeStringSliceV1 {
                ptr: name.as_ptr().cast(),
                len: name.len(),
            },
            size: if provider.get_case == GetCase::Empty {
                0
            } else {
                4
            },
            last_modified_unix_ms: -1,
        };
        let mut bytes = KernelNativeByteSliceV1 {
            ptr: if body.is_empty() {
                std::ptr::null()
            } else {
                body.as_ptr()
            },
            len: body.len(),
        };
        match provider.get_case {
            GetCase::EmptyLocation => {
                meta.location.ptr = std::ptr::null();
                meta.location.len = 0;
            }
            GetCase::OversizedLocation => meta.location.len = MAX_PATH_BYTES + 1,
            GetCase::InvalidTimestamp => meta.last_modified_unix_ms = i64::MAX,
            GetCase::SizeMismatch => meta.size = 5,
            GetCase::NullBody => bytes.ptr = std::ptr::null(),
            GetCase::OversizedBody => {
                bytes.len = MAX_BODY_BYTES + 1;
                meta.size = bytes.len as u64;
            }
            _ => {}
        }
        let meta_ptr = if provider.get_case == GetCase::NullMeta {
            std::ptr::null()
        } else {
            &meta as *const KernelNativeObjectMetaV1
        };
        let status = unsafe { sink(sink_context, meta_ptr, bytes) };
        if status != KERNEL_NATIVE_STATUS_OK {
            return status;
        }
        if provider.get_case == GetCase::DuplicateSink {
            return unsafe { sink(sink_context, meta_ptr, bytes) };
        }
        provider.status
    }

    unsafe extern "C" fn provider_list(
        context: *mut c_void,
        prefix: KernelNativeStringSliceV1,
        start_after: KernelNativeStringSliceV1,
        max_items: u32,
        sink_context: *mut c_void,
        sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV1) -> i32,
        has_more: *mut u32,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(max_items, LIST_PAGE_SIZE);
        let _prefix = unsafe { input_string(prefix) };
        let _start_after = unsafe { input_string(start_after) };
        unsafe { *has_more = provider.has_more };
        for name in provider.list_names.clone() {
            let meta = KernelNativeObjectMetaV1 {
                location: string_slice(&name),
                size: 4,
                last_modified_unix_ms: 0,
            };
            let status = unsafe { sink(sink_context, &meta) };
            if status != KERNEL_NATIVE_STATUS_OK {
                return status;
            }
        }
        provider.status
    }

    unsafe extern "C" fn provider_put(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
        body: KernelNativeByteSliceV1,
        mode: u32,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(unsafe { input_string(path) }, "table/a");
        let bytes = if body.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(body.ptr, body.len) }
        };
        *provider.probe.put_body.lock().unwrap() = bytes.to_vec();
        provider.probe.put_mode.store(mode, Ordering::SeqCst);
        provider.status
    }

    unsafe extern "C" fn provider_delete(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        *provider.probe.deleted.lock().unwrap() = unsafe { input_string(path) };
        provider.status
    }

    fn rest_config() -> crate::rest_engine::CRestEndpointConfig {
        let field = "field";
        let empty = "";
        crate::rest_engine::CRestEndpointConfig {
            files_prefix: kernel_string_slice!(empty),
            directories_prefix: kernel_string_slice!(empty),
            page_token_param: kernel_string_slice!(field),
            start_from_param: kernel_string_slice!(field),
            recursive_param: kernel_string_slice!(field),
            overwrite_param: kernel_string_slice!(field),
            contents_field: kernel_string_slice!(field),
            next_page_token_field: kernel_string_slice!(field),
            entry_path_field: kernel_string_slice!(field),
            entry_size_field: kernel_string_slice!(field),
            entry_is_directory_field: kernel_string_slice!(field),
            entry_last_modified_field: kernel_string_slice!(field),
            entry_strip_prefix: kernel_string_slice!(empty),
        }
    }

    #[rstest]
    #[case(None)]
    #[case(Some((0, 0)))]
    #[case(Some((2, size_of::<KernelNativeObjectStoreDescriptorV1>() as u32)))]
    #[case(Some((1, 0)))]
    #[case(Some((1, size_of::<KernelNativeObjectStoreDescriptorV1>() as u32 - 1)))]
    #[case(Some((1, size_of::<KernelNativeObjectStoreDescriptorV1>() as u32 + 1)))]
    fn descriptor_header_is_rejected_before_reading_full_layout(
        #[case] header: Option<(u32, u32)>,
    ) {
        let fields = header
            .map(|(version, size)| [version, size])
            .unwrap_or_default();
        let descriptor = if header.is_some() {
            fields.as_ptr().cast()
        } else {
            std::ptr::null()
        };
        assert_extern_result_error_contains(
            unsafe { get_native_object_store(descriptor, allocate_err) },
            FFIKernelError::GenericError,
            "invalid native object store descriptor",
        );
    }

    #[rstest]
    #[case("context")]
    #[case("get")]
    #[case("list")]
    #[case("put")]
    #[case("delete")]
    #[case("release")]
    fn missing_descriptor_fields_do_not_adopt_or_release(#[case] field: &str) {
        let (mut descriptor, probe) = descriptor_for(Provider::default());
        let context = descriptor.context;
        match field {
            "context" => descriptor.context = std::ptr::null_mut(),
            "get" => descriptor.get = None,
            "list" => descriptor.list = None,
            "put" => descriptor.put = None,
            "delete" => descriptor.delete_object = None,
            "release" => descriptor.release = None,
            _ => unreachable!(),
        }
        assert_extern_result_error_contains(
            unsafe { get_native_object_store(&descriptor, allocate_err) },
            FFIKernelError::GenericError,
            "invalid native object store descriptor",
        );
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        unsafe { provider_release(context) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn handle_and_operation_references_delay_release_exactly_once() {
        let (handle, probe) = handle_for(Provider::default());
        let retained = unsafe { handle.clone_as_arc() };
        let clone = unsafe { handle.clone_handle() };
        let operation_store = retained.as_ref().clone();
        let operation = move || operation_store.get_sync("table/a", false);
        unsafe { free_native_object_store(handle) };
        unsafe { free_native_object_store(clone) };
        drop(retained);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        let (meta, body) = operation().unwrap();
        assert_eq!(body.as_ref(), b"data");
        assert_eq!(meta.location.as_ref(), "table/a");
        drop(operation);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lazy_listing_keeps_context_without_invoking_io() {
        let (handle, probe) = handle_for(Provider::default());
        let prefix = Path::from("table");
        let listing = unsafe { handle.as_ref() }.list(Some(&prefix));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        drop(listing);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_read_retains_context_until_native_callback_finishes() {
        let (entered, started) = std::sync::mpsc::channel();
        let (resume, waiting) = std::sync::mpsc::channel();
        let (released, finished) = std::sync::mpsc::channel();
        let (handle, probe) = handle_for(Provider {
            gate: Some(Arc::new(CallGate {
                entered,
                resume: Mutex::new(waiting),
            })),
            ..Default::default()
        });
        *probe.released.lock().unwrap() = Some(released);
        let store = unsafe { handle.clone_as_arc() };
        let read = tokio::spawn(async move {
            store
                .get_opts(&Path::from("table/a"), Default::default())
                .await
        });
        unsafe { free_native_object_store(handle) };
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        resume.send(()).unwrap();
        tokio::task::spawn_blocking(move || finished.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("abandon")]
    #[case("rest_to_native")]
    #[case("replace_native")]
    #[case("replace_rest")]
    #[case("invalid_option")]
    #[case("invalid_rest")]
    fn builder_cleanup_preserves_borrowing_and_releases_context(#[case] action: &str) {
        let (handle, probe) = handle_for(Provider::default());
        let path = "memory:///table/";
        let builder = ok_or_panic(unsafe {
            crate::get_engine_builder(kernel_string_slice!(path), allocate_err)
        });
        let builder = if action == "rest_to_native" {
            let config = rest_config();
            ok_or_panic(unsafe {
                crate::builder_with_rest_object_store(builder, &config, None, None)
            })
        } else {
            builder
        };
        let builder = ok_or_panic(unsafe {
            crate::builder_with_object_store(builder, handle.shallow_copy())
        });
        let message = "retained allocator";
        let allocator = unsafe { builder.as_ref() }.allocate_fn;
        let error = allocator(FFIKernelError::GenericError, kernel_string_slice!(message));
        assert_eq!(unsafe { recover_error(error) }.message, message);
        assert!(matches!(
            unsafe { &builder.as_ref().object_store_backend },
            crate::ObjectStoreBackend::Native(_)
        ));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        match action {
            "abandon" | "rest_to_native" => unsafe { crate::free_engine_builder(builder) },
            "replace_native" => {
                let (replacement, replacement_probe) = handle_for(Provider::default());
                let builder = ok_or_panic(unsafe {
                    crate::builder_with_object_store(builder, replacement.shallow_copy())
                });
                assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
                unsafe { free_native_object_store(replacement) };
                assert_eq!(replacement_probe.releases.load(Ordering::SeqCst), 0);
                unsafe { crate::free_engine_builder(builder) };
                assert_eq!(replacement_probe.releases.load(Ordering::SeqCst), 1);
            }
            "replace_rest" => {
                let config = rest_config();
                let builder = ok_or_panic(unsafe {
                    crate::builder_with_rest_object_store(builder, &config, None, None)
                });
                assert!(matches!(
                    unsafe { &builder.as_ref().object_store_backend },
                    crate::ObjectStoreBackend::Rest(_)
                ));
                unsafe { crate::free_engine_builder(builder) };
            }
            "invalid_option" => {
                let bytes = [0xff];
                let key = KernelStringSlice {
                    ptr: bytes.as_ptr().cast(),
                    len: bytes.len(),
                };
                let value = "value";
                assert_extern_result_error_contains(
                    unsafe {
                        crate::builder_with_option(builder, key, kernel_string_slice!(value))
                    },
                    FFIKernelError::Utf8Error,
                    "invalid utf-8",
                );
            }
            "invalid_rest" => assert_extern_result_error_contains(
                unsafe {
                    crate::builder_with_rest_object_store(builder, std::ptr::null(), None, None)
                },
                FFIKernelError::GenericError,
                "null CRestEndpointConfig",
            ),
            _ => unreachable!(),
        }
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    #[case(false, false, b"data".as_slice())]
    #[case(true, false, b"".as_slice())]
    #[case(false, true, b"".as_slice())]
    fn get_copies_output_before_provider_buffers_drop(
        #[case] head: bool,
        #[case] empty: bool,
        #[case] expected: &[u8],
    ) {
        let (handle, probe) = handle_for(Provider {
            get_case: if empty {
                GetCase::Empty
            } else {
                GetCase::Valid
            },
            ..Default::default()
        });
        let path = "table/\u{96ea}";
        let (meta, body) = unsafe { handle.as_ref() }.get_sync(path, head).unwrap();
        unsafe { free_native_object_store(handle) };
        assert_eq!(meta.location.as_ref(), path);
        assert_eq!(meta.size, if empty { 0 } else { 4 });
        assert_eq!(meta.last_modified.timestamp_millis(), -1);
        assert_eq!(meta.e_tag, None);
        assert_eq!(meta.version, None);
        assert_eq!(body.as_ref(), expected);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case(GetCase::MissingSink, false)]
    #[case(GetCase::DuplicateSink, false)]
    #[case(GetCase::NullMeta, false)]
    #[case(GetCase::EmptyLocation, false)]
    #[case(GetCase::InvalidUtf8, false)]
    #[case(GetCase::WrongLocation, false)]
    #[case(GetCase::AbsoluteLocation, false)]
    #[case(GetCase::OversizedLocation, false)]
    #[case(GetCase::InvalidTimestamp, false)]
    #[case(GetCase::SizeMismatch, false)]
    #[case(GetCase::HeadBody, true)]
    #[case(GetCase::NullBody, false)]
    #[case(GetCase::OversizedBody, false)]
    fn malformed_get_output_is_rejected(#[case] get_case: GetCase, #[case] head: bool) {
        let (handle, probe) = handle_for(Provider {
            get_case,
            ..Default::default()
        });
        let result = unsafe { handle.as_ref() }.get_sync("table/a", head);
        assert!(matches!(result, Err(ObjectStoreError::Generic { .. })));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("table", "", vec!["table/a".into(), "table/b".into()], 0)]
    #[case("table", "table/a", vec!["table/b".into(), "table/c".into()], 1)]
    #[case("", "", vec!["a".into(), "table/b".into()], 0)]
    #[case("table", "", vec![], 0)]
    #[case(
        "table",
        "",
        (0..LIST_PAGE_SIZE).map(|index| format!("table/{index:04}")).collect(),
        1
    )]
    fn list_copies_ordered_pages_with_prefix_and_exclusive_offset(
        #[case] prefix: &str,
        #[case] offset: &str,
        #[case] names: Vec<String>,
        #[case] has_more: u32,
    ) {
        let (handle, probe) = handle_for(Provider {
            list_names: names.clone(),
            has_more,
            ..Default::default()
        });
        let page = unsafe { handle.as_ref() }
            .list_page_sync(prefix, offset)
            .unwrap();
        unsafe { free_native_object_store(handle) };
        assert_eq!(page.has_more, has_more == 1);
        let paths: Vec<_> = page
            .objects
            .iter()
            .map(|meta| meta.location.to_string())
            .collect();
        assert_eq!(paths, names);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("prefix")]
    #[case("segment_prefix")]
    #[case("duplicate")]
    #[case("descending")]
    #[case("offset")]
    #[case("page_limit")]
    #[case("has_more")]
    #[case("no_progress")]
    #[case("partial_error")]
    fn invalid_list_pages_discard_partial_output(#[case] violation: &str) {
        let mut provider = Provider::default();
        let mut offset = "";
        match violation {
            "prefix" => provider.list_names.push("wrong/c".into()),
            "segment_prefix" => provider.list_names.push("table_other/c".into()),
            "duplicate" => provider.list_names.push("table/b".into()),
            "descending" => provider.list_names.reverse(),
            "offset" => offset = "table/a",
            "page_limit" => {
                provider.list_names = (0..=LIST_PAGE_SIZE)
                    .map(|index| format!("table/{index:04}"))
                    .collect();
            }
            "has_more" => provider.has_more = 2,
            "no_progress" => {
                provider.list_names.clear();
                provider.has_more = 1;
            }
            "partial_error" => provider.status = KERNEL_NATIVE_STATUS_GENERIC,
            _ => unreachable!(),
        }
        let (handle, probe) = handle_for(provider);
        let result = unsafe { handle.as_ref() }.list_page_sync("table", offset);
        assert!(matches!(result, Err(ObjectStoreError::Generic { .. })));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case(KERNEL_NATIVE_STATUS_OK, "ok")]
    #[case(KERNEL_NATIVE_STATUS_NOT_FOUND, "not_found")]
    #[case(KERNEL_NATIVE_STATUS_ALREADY_EXISTS, "exists")]
    #[case(KERNEL_NATIVE_STATUS_NOT_SUPPORTED, "unsupported")]
    #[case(KERNEL_NATIVE_STATUS_GENERIC, "generic")]
    #[case(-1, "generic")]
    #[case(i32::MAX, "generic")]
    fn callback_statuses_map_to_fixed_sanitized_errors(
        #[case] status: i32,
        #[case] expected: &str,
    ) {
        let result = check_status(status, "table/a");
        match expected {
            "ok" => assert!(result.is_ok()),
            "not_found" => assert!(matches!(result, Err(ObjectStoreError::NotFound { .. }))),
            "exists" => assert!(matches!(
                result,
                Err(ObjectStoreError::AlreadyExists { .. })
            )),
            "unsupported" => assert!(matches!(result, Err(ObjectStoreError::NotSupported { .. }))),
            "generic" => match result.unwrap_err() {
                ObjectStoreError::Generic { source, .. } => {
                    assert_eq!(source.to_string(), "native object store callback failed");
                }
                error => panic!("unexpected error {error}"),
            },
            _ => unreachable!(),
        }
    }

    #[rstest]
    #[case(None, Some(b"data".as_slice()))]
    #[case(Some(GetRange::Bounded(1..3)), Some(b"at".as_slice()))]
    #[case(Some(GetRange::Bounded(2..9)), Some(b"ta".as_slice()))]
    #[case(Some(GetRange::Offset(2)), Some(b"ta".as_slice()))]
    #[case(Some(GetRange::Suffix(2)), Some(b"ta".as_slice()))]
    #[case(Some(GetRange::Suffix(0)), Some(b"".as_slice()))]
    #[case(Some(GetRange::Bounded(4..5)), None)]
    #[case(Some(GetRange::Bounded(std::ops::Range { start: 2, end: 1 })), None)]
    fn get_ranges_use_full_object_metadata_and_owned_bytes(
        #[case] range: Option<GetRange>,
        #[case] expected: Option<&[u8]>,
    ) {
        let meta = ObjectMeta {
            location: Path::from("table/a"),
            last_modified: DateTime::<Utc>::UNIX_EPOCH,
            size: 4,
            e_tag: None,
            version: None,
        };
        let result = make_get_result(
            meta,
            Bytes::from_static(b"data"),
            GetOptions {
                range,
                ..Default::default()
            },
        );
        if let Some(expected) = expected {
            let result = result.unwrap();
            assert_eq!(result.meta.size, 4);
            assert_eq!(
                futures::executor::block_on(result.bytes())
                    .unwrap()
                    .as_ref(),
                expected,
            );
        } else {
            assert!(result.is_err());
        }
    }

    #[rstest]
    #[case("version")]
    #[case("if_match")]
    #[case("if_none_match")]
    #[case("if_modified_since")]
    #[case("if_unmodified_since")]
    #[case("extensions")]
    fn unsupported_get_options_are_explicitly_rejected(#[case] option: &str) {
        let mut options = GetOptions::default();
        match option {
            "version" => options.version = Some("v1".into()),
            "if_match" => options.if_match = Some("tag".into()),
            "if_none_match" => options.if_none_match = Some("tag".into()),
            "if_modified_since" => options.if_modified_since = Some(DateTime::<Utc>::UNIX_EPOCH),
            "if_unmodified_since" => {
                options.if_unmodified_since = Some(DateTime::<Utc>::UNIX_EPOCH);
            }
            "extensions" => {
                options.extensions.insert(1u32);
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            validate_get_options(&options),
            Err(ObjectStoreError::NotSupported { .. })
        ));
    }

    #[rstest]
    #[case("overwrite", Some(KERNEL_NATIVE_PUT_OVERWRITE))]
    #[case("create", Some(KERNEL_NATIVE_PUT_CREATE))]
    #[case("update", None)]
    #[case("tags", None)]
    #[case("attributes", None)]
    #[case("extensions", None)]
    fn put_modes_and_unsupported_options_are_explicit(
        #[case] option: &str,
        #[case] expected: Option<u32>,
    ) {
        let mut options = PutOptions::default();
        match option {
            "overwrite" => {}
            "create" => options.mode = PutMode::Create,
            "update" => {
                options.mode = PutMode::Update(UpdateVersion {
                    e_tag: Some("tag".into()),
                    version: None,
                });
            }
            "tags" => options.tags.push("name", "value"),
            "attributes" => {
                options
                    .attributes
                    .insert(Attribute::ContentType, "text/plain".into());
            }
            "extensions" => {
                options.extensions.insert(1u32);
            }
            _ => unreachable!(),
        }
        let result = native_put_mode(&options);
        if let Some(expected) = expected {
            assert_eq!(result.unwrap(), expected);
        } else {
            assert!(matches!(result, Err(ObjectStoreError::NotSupported { .. })));
        }
    }

    #[rstest]
    #[case(KERNEL_NATIVE_PUT_OVERWRITE, b"data".as_slice())]
    #[case(KERNEL_NATIVE_PUT_CREATE, b"".as_slice())]
    fn put_and_delete_borrow_inputs_and_release_once(#[case] mode: u32, #[case] input: &[u8]) {
        let (handle, probe) = handle_for(Provider::default());
        let store = unsafe { handle.clone_as_arc() };
        unsafe { free_native_object_store(handle) };
        let body = copy_put_payload(Bytes::copy_from_slice(input).into()).unwrap();
        store.put_sync("table/a", &body, mode).unwrap();
        store.delete_sync("table/a").unwrap();
        drop(body);
        assert_eq!(probe.put_mode.load(Ordering::SeqCst), mode);
        assert_eq!(*probe.put_body.lock().unwrap(), input);
        assert_eq!(*probe.deleted.lock().unwrap(), "table/a");
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 2);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        drop(store);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }
}
