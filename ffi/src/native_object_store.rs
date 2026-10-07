//! [`ObjectStore`] forwarding through an independent provider's v3 C descriptor.
//!
//! The provider owns storage, authentication, runtime, and allocations. Kernel copies borrowed
//! sink output and retains the adopted context until the last handle, builder, engine, stream,
//! or blocking operation releases it. Synchronous I/O callbacks run on Tokio blocking workers;
//! release can run on any thread. Dropping an async operation does not cancel a native call.
//!
//! GET, HEAD, ranges, atomic Create/Overwrite PUT, DELETE and native cursor listing are forwarded.
//! Byte payloads are limited to 64 MiB and paths to 64 KiB. Listing batches have at most 128
//! entries, without a total listing limit or ordering policy. Both modules remain loaded through
//! final release.

use std::ffi::c_void;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{
    self, Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetRange, GetResult,
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
const MAX_LIST_ITEMS: usize = KERNEL_NATIVE_LIST_BATCH_SIZE;

/// Kernel-owned forwarding adapter, opaque across the C ABI.
#[derive(Clone)]
pub struct NativeObjectStore {
    context: Arc<NativeContext>,
}

/// Shared native-store handle. Release with [`free_native_object_store`].
#[handle_descriptor(target=NativeObjectStore, mutable=false, sized=true)]
pub struct SharedNativeObjectStore;

struct NativeContext {
    descriptor: KernelNativeObjectStoreDescriptorV3,
}

struct GetSinkState {
    path: String,
    head: bool,
    range: Option<GetRange>,
    called: bool,
    output: Option<(ObjectMeta, Bytes, Range<u64>)>,
    error: Option<ObjectStoreError>,
}

struct ListSinkState {
    prefix: Path,
    offset: Option<Path>,
    objects: Vec<ObjectMeta>,
    error: Option<ObjectStoreError>,
}

struct NativeListing {
    store: NativeObjectStore,
    cursor: NonNull<c_void>,
    prefix: Path,
    offset: Option<Path>,
}

// SAFETY: the provider permits moving its opaque cursor between threads. Exclusive ownership
// moves into each blocking advance, so close cannot overlap next, including after cancellation.
unsafe impl Send for NativeListing {}

impl Drop for NativeListing {
    fn drop(&mut self) {
        if let Some(close) = self.store.context.descriptor.list_close {
            // SAFETY: this successfully opened cursor is consumed once, before the store drops.
            unsafe { close(self.cursor.as_ptr()) };
        }
    }
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
        formatter.write_str("NativeObjectStore")
    }
}

impl fmt::Display for NativeObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeObjectStore")
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
        let mode = validate_put_options(&options)?;
        let length = payload.iter().try_fold(0usize, |length, chunk| {
            length
                .checked_add(chunk.len())
                .filter(|length| *length <= MAX_BODY_BYTES)
                .ok_or_else(|| not_supported("PUT payloads larger than 64 MiB"))
        })?;
        let path = location.to_string();
        validate_input_path(&path)?;
        let store = self.clone();
        run_blocking(move || {
            let mut bytes = Vec::with_capacity(length);
            for chunk in payload {
                bytes.extend_from_slice(&chunk);
            }
            store.put_sync(&path, &bytes, mode)
        })
        .await?;
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
        let path = location.to_string();
        let store = self.clone();
        let (meta, body, range) = run_blocking(move || store.get_sync(&path, &options)).await?;
        let payload = GetResultPayload::Stream(stream::once(std::future::ready(Ok(body))).boxed());
        Ok(object_store::delta_kernel_compat::get_result(
            payload,
            meta,
            range,
            Attributes::new(),
        ))
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
                    run_blocking(move || {
                        store.delete_sync(location.as_ref())?;
                        Ok(location)
                    })
                    .await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.list_stream(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.list_stream(prefix, Some(offset))
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
    unsafe fn adopt(descriptor: *const KernelNativeObjectStoreDescriptorV3) -> KernelResult<Self> {
        let invalid = || KernelError::generic("invalid native object store descriptor");
        if descriptor.is_null() {
            return Err(invalid());
        }
        // SAFETY: only the two readable header fields are inspected before layout validation.
        let abi_version = unsafe { descriptor.cast::<u32>().read_unaligned() };
        let struct_size = unsafe { descriptor.cast::<u32>().add(1).read_unaligned() };
        if abi_version != KERNEL_NATIVE_STORE_ABI_V3
            || struct_size as usize != size_of::<KernelNativeObjectStoreDescriptorV3>()
            || !descriptor.is_aligned()
        {
            return Err(invalid());
        }
        // SAFETY: a matching header promises readable, initialized storage for this exact layout.
        let descriptor = unsafe { *descriptor };
        if descriptor.context.is_null()
            || descriptor.get.is_none()
            || descriptor.list_open.is_none()
            || descriptor.list_next.is_none()
            || descriptor.list_close.is_none()
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

    fn get_sync(
        &self,
        path: &str,
        options: &GetOptions,
    ) -> ObjectStoreResult<(ObjectMeta, Bytes, Range<u64>)> {
        validate_input_path(path)?;
        let request = validate_get_options(options)?;
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .get
            .ok_or_else(|| generic_error("native GET callback is missing"))?;
        let mut state = GetSinkState {
            path: path.to_string(),
            head: options.head,
            range: options.range.clone(),
            called: false,
            output: None,
            error: None,
        };
        // SAFETY: input and sink state live through this synchronous call. The provider invokes
        // sinks synchronously and cannot retain them; the context reference keeps it alive.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(path),
                request,
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

    fn put_sync(&self, path: &str, bytes: &[u8], mode: u32) -> ObjectStoreResult<()> {
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .put
            .ok_or_else(|| generic_error("native PUT callback is missing"))?;
        // SAFETY: the store reference, path and copied payload live through callback return.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(path),
                KernelNativeByteSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
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
        // SAFETY: the retained context and borrowed path live through callback return.
        check_status(
            unsafe { callback(descriptor.context, string_slice(path)) },
            path,
        )
    }

    fn list_stream(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.map(ToString::to_string).unwrap_or_default();
        let offset = offset.map(ToString::to_string);
        stream::try_unfold(
            (Some(store), None::<NativeListing>),
            move |(store, listing)| {
                let prefix = prefix.clone();
                let offset = offset.clone();
                async move {
                    if store.is_none() && listing.is_none() {
                        return Ok::<_, ObjectStoreError>(None);
                    }
                    let (listing, page) = run_blocking(move || {
                        let listing = match listing {
                            Some(listing) => listing,
                            None => NativeListing::open(
                                store
                                    .ok_or_else(|| generic_error("missing native listing store"))?,
                                &prefix,
                                offset.as_deref(),
                            )?,
                        };
                        listing.next_sync()
                    })
                    .await?;
                    Ok(Some((
                        stream::iter(page.into_iter().map(Ok)),
                        (None, listing),
                    )))
                }
            },
        )
        .try_flatten()
        .boxed()
    }
}

impl NativeListing {
    fn open(
        store: NativeObjectStore,
        prefix: &str,
        offset: Option<&str>,
    ) -> ObjectStoreResult<Self> {
        validate_input_path(prefix)?;
        validate_input_path(offset.unwrap_or_default())?;
        let prefix_path =
            Path::parse(prefix).map_err(|_| generic_error("invalid native LIST prefix"))?;
        let offset_path = offset
            .map(Path::parse)
            .transpose()
            .map_err(|_| generic_error("invalid native LIST offset"))?;
        let descriptor = &store.context.descriptor;
        let callback = descriptor
            .list_open
            .ok_or_else(|| generic_error("native LIST open callback is missing"))?;
        let mut cursor = std::ptr::null_mut();
        // SAFETY: inputs and cursor output remain live through open; only success initializes it.
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(prefix),
                string_slice(offset.unwrap_or_default()),
                &mut cursor,
            )
        };
        check_status(status, prefix)?;
        let cursor = NonNull::new(cursor)
            .ok_or_else(|| generic_error("native LIST opened a null cursor"))?;
        Ok(Self {
            store,
            cursor,
            prefix: prefix_path,
            offset: offset_path,
        })
    }

    fn next_sync(self) -> ObjectStoreResult<(Option<Self>, Vec<ObjectMeta>)> {
        let callback = self
            .store
            .context
            .descriptor
            .list_next
            .ok_or_else(|| generic_error("native LIST next callback is missing"))?;
        let mut state = ListSinkState {
            prefix: self.prefix.clone(),
            offset: self.offset.clone(),
            objects: Vec::new(),
            error: None,
        };
        let mut has_more = u32::MAX;
        // SAFETY: this worker owns the cursor exclusively; borrowed sink output is copied before
        // return. On error or cancellation, ownership drops only after the callback finishes.
        let status = unsafe {
            callback(
                self.cursor.as_ptr(),
                (&mut state as *mut ListSinkState).cast(),
                list_sink,
                &mut has_more,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, self.prefix.as_ref())?;
        if has_more > 1 || (has_more == 1 && state.objects.is_empty()) {
            return Err(generic_error(
                "native LIST returned an invalid progress flag",
            ));
        }
        let listing = if has_more == 1 { Some(self) } else { None };
        Ok((listing, state.objects))
    }
}

/// Adopt a forwarding v3 descriptor and return an owned shared handle.
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
/// misaligned v3 storage, null context, or any missing callback. Versions one and two are rejected.
///
/// # Safety
///
/// A non-null descriptor must expose two readable initialized `u32` header fields. When those
/// fields claim the exact v3 layout, it must also expose the full initialized descriptor. Invalid
/// non-null pointers are caller violations, not recoverable errors. After successful adoption,
/// the context must not be freed or adopted again by the caller. All callbacks must be thread
/// safe, support concurrent calls, never unwind, and follow the ABI sink/input lifetime contract.
/// Sinks must finish before callback return and must not be retained or invoked concurrently
/// within one operation. Release must be valid on any thread. Provider code and Kernel code must
/// stay loaded until all handles, engines, streams, and native calls have completed.
#[no_mangle]
pub unsafe extern "C" fn get_native_object_store(
    descriptor: *const KernelNativeObjectStoreDescriptorV3,
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
        source: format!("native object store forwarding does not support {operation}").into(),
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

fn validate_get_options(options: &GetOptions) -> ObjectStoreResult<KernelNativeGetOptionsV3> {
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
    let mut request = KernelNativeGetOptionsV3 {
        head: u32::from(options.head),
        ..Default::default()
    };
    if let Some(range) = &options.range {
        range
            .is_valid()
            .map_err(|_| generic_error("invalid requested GET range"))?;
        match range {
            GetRange::Bounded(range) => {
                request.range_kind = 1;
                request.start = range.start;
                request.end = range.end;
            }
            GetRange::Offset(start) => {
                request.range_kind = 2;
                request.start = *start;
            }
            GetRange::Suffix(length) => {
                request.range_kind = 3;
                request.end = *length;
            }
        }
    }
    Ok(request)
}

fn validate_put_options(options: &PutOptions) -> ObjectStoreResult<u32> {
    if options.tags != TagSet::default()
        || !options.attributes.is_empty()
        || !options.extensions.is_empty()
    {
        return Err(not_supported("PUT tags, attributes or request extensions"));
    }
    match options.mode {
        PutMode::Overwrite => Ok(KERNEL_NATIVE_PUT_OVERWRITE),
        PutMode::Create => Ok(KERNEL_NATIVE_PUT_CREATE),
        PutMode::Update(_) => Err(not_supported("conditional Update PUT")),
    }
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
    range_start: u64,
    range_end: u64,
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
        let range = range_start..range_end;
        if range_start > range_end || range_end > meta.size {
            return Err(generic_error("invalid native GET range"));
        }
        if !state.head {
            let expected = match &state.range {
                Some(requested) => requested
                    .as_range(meta.size)
                    .map_err(|_| generic_error("native GET accepted an invalid range"))?,
                None => 0..meta.size,
            };
            if range != expected {
                return Err(generic_error("native GET returned a different range"));
            }
        }
        if body.len as u64
            != if state.head {
                0
            } else {
                range_end - range_start
            }
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
        Ok((meta, bytes, range))
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
    // SAFETY: only next_sync supplies this live state; sink calls cannot overlap or escape.
    let state = unsafe { &mut *context.cast::<ListSinkState>() };
    if state.error.is_some() {
        return KERNEL_NATIVE_STATUS_GENERIC;
    }
    let output = (|| {
        if state.objects.len() >= MAX_LIST_ITEMS {
            return Err(generic_error("native LIST exceeded its batch limit"));
        }
        let meta = unsafe { copy_meta(meta) }?;
        if !meta.location.prefix_matches(&state.prefix)
            || state
                .offset
                .as_ref()
                .is_some_and(|offset| meta.location <= *offset)
        {
            return Err(generic_error(
                "native LIST violated prefix or exclusive offset",
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use rstest::rstest;

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_contains, ok_or_panic, recover_error,
    };
    use crate::{kernel_string_slice, KernelStringSlice};

    type GetRequest = (String, u32, u32, u64, u64);

    #[derive(Default)]
    struct Probe {
        releases: AtomicUsize,
        io_calls: AtomicUsize,
        opens: AtomicUsize,
        advances: AtomicUsize,
        closes: AtomicUsize,
        get_requests: Mutex<Vec<GetRequest>>,
        put_requests: Mutex<Vec<(String, Vec<u8>, u32)>>,
        delete_requests: Mutex<Vec<String>>,
        list_requests: Mutex<Vec<(String, String)>>,
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
        UnalignedMeta,
        NullLocation,
        EmptyLocation,
        InvalidUtf8,
        WrongLocation,
        AbsoluteLocation,
        OversizedLocation,
        InvalidTimestamp,
        SizeMismatch,
        NullBody,
        OversizedBody,
        HeadBody,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ListCase {
        Valid,
        Overflow,
        BadFlag,
        MissingFlag,
        NoProgress,
        NullMeta,
        OffsetViolation,
    }

    struct Provider {
        probe: Arc<Probe>,
        get_case: GetCase,
        get_size: u64,
        actual_range: Option<Range<u64>>,
        expected_get: Option<(u32, u32, u64, u64)>,
        list_names: Vec<String>,
        list_case: ListCase,
        open_status: i32,
        null_cursor: bool,
        list_gate: Option<Arc<CallGate>>,
        gate_after: usize,
        status: i32,
        gate: Option<Arc<CallGate>>,
    }

    impl Default for Provider {
        fn default() -> Self {
            Self {
                probe: Arc::new(Probe::default()),
                get_case: GetCase::Valid,
                get_size: 4,
                actual_range: None,
                expected_get: None,
                list_names: vec!["table/a".into(), "table/b".into()],
                list_case: ListCase::Valid,
                open_status: KERNEL_NATIVE_STATUS_OK,
                null_cursor: false,
                list_gate: None,
                gate_after: 0,
                status: KERNEL_NATIVE_STATUS_OK,
                gate: None,
            }
        }
    }

    fn descriptor_for(provider: Provider) -> (KernelNativeObjectStoreDescriptorV3, Arc<Probe>) {
        let probe = provider.probe.clone();
        let descriptor = KernelNativeObjectStoreDescriptorV3 {
            abi_version: KERNEL_NATIVE_STORE_ABI_V3,
            struct_size: size_of::<KernelNativeObjectStoreDescriptorV3>() as u32,
            context: Box::into_raw(Box::new(provider)).cast(),
            get: Some(provider_get),
            list_open: Some(provider_list_open),
            list_next: Some(provider_list_next),
            list_close: Some(provider_list_close),
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
        options: KernelNativeGetOptionsV3,
        sink_context: *mut c_void,
        sink: unsafe extern "C" fn(
            *mut c_void,
            *const KernelNativeObjectMetaV1,
            KernelNativeByteSliceV1,
            u64,
            u64,
        ) -> i32,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        let path = unsafe { input_string(path) };
        let request = (options.head, options.range_kind, options.start, options.end);
        if let Some(expected) = provider.expected_get {
            assert_eq!(request, expected);
        }
        provider.probe.get_requests.lock().unwrap().push((
            path.clone(),
            request.0,
            request.1,
            request.2,
            request.3,
        ));
        if let Some(gate) = &provider.gate {
            gate.entered.send(()).unwrap();
            gate.resume.lock().unwrap().recv().unwrap();
        }
        if provider.get_case == GetCase::MissingSink {
            return provider.status;
        }
        let mut name = path.into_bytes();
        match provider.get_case {
            GetCase::InvalidUtf8 => name = vec![0xff],
            GetCase::WrongLocation => name = b"other/object".to_vec(),
            GetCase::AbsoluteLocation => name = b"/table/a".to_vec(),
            _ => {}
        }
        let size = if provider.get_case == GetCase::Empty {
            0
        } else {
            provider.get_size
        };
        let requested = match options.range_kind {
            0 => None,
            1 => Some(GetRange::Bounded(options.start..options.end)),
            2 => Some(GetRange::Offset(options.start)),
            3 => Some(GetRange::Suffix(options.end)),
            _ => unreachable!(),
        };
        let range = provider.actual_range.clone().unwrap_or_else(|| {
            requested
                .map(|range| range.as_range(size).unwrap())
                .unwrap_or(0..size)
        });
        let body = if options.head == 1 && provider.get_case != GetCase::HeadBody {
            Vec::new()
        } else if size <= 4 && range.start <= range.end && range.end <= size {
            b"data"[range.start as usize..range.end as usize].to_vec()
        } else if range.end.saturating_sub(range.start) <= MAX_BODY_BYTES as u64 {
            vec![b'x'; range.end.saturating_sub(range.start) as usize]
        } else {
            b"data".to_vec()
        };
        let mut meta = KernelNativeObjectMetaV1 {
            location: KernelNativeStringSliceV1 {
                ptr: name.as_ptr().cast(),
                len: name.len(),
            },
            size,
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
            GetCase::NullLocation => meta.location.ptr = std::ptr::null(),
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
        } else if provider.get_case == GetCase::UnalignedMeta {
            NonNull::<u8>::dangling().as_ptr().cast()
        } else {
            &meta as *const KernelNativeObjectMetaV1
        };
        let status = unsafe { sink(sink_context, meta_ptr, bytes, range.start, range.end) };
        if status != KERNEL_NATIVE_STATUS_OK {
            return status;
        }
        if provider.get_case == GetCase::DuplicateSink {
            return unsafe { sink(sink_context, meta_ptr, bytes, range.start, range.end) };
        }
        provider.status
    }

    struct ProviderCursor {
        probe: Arc<Probe>,
        names: Vec<String>,
        position: usize,
        case: ListCase,
        gate: Option<Arc<CallGate>>,
        gate_after: usize,
        status: i32,
    }

    unsafe extern "C" fn provider_list_open(
        context: *mut c_void,
        prefix: KernelNativeStringSliceV1,
        offset: KernelNativeStringSliceV1,
        cursor: *mut *mut c_void,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.opens.fetch_add(1, Ordering::SeqCst);
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        let prefix = unsafe { input_string(prefix) };
        let offset = unsafe { input_string(offset) };
        provider
            .probe
            .list_requests
            .lock()
            .unwrap()
            .push((prefix, offset.clone()));
        if provider.open_status != KERNEL_NATIVE_STATUS_OK {
            return provider.open_status;
        }
        if provider.null_cursor {
            unsafe { *cursor = std::ptr::null_mut() };
            return KERNEL_NATIVE_STATUS_OK;
        }
        unsafe {
            *cursor = Box::into_raw(Box::new(ProviderCursor {
                probe: provider.probe.clone(),
                names: provider
                    .list_names
                    .iter()
                    .filter(|name| {
                        provider.list_case == ListCase::OffsetViolation
                            || name.as_str() > offset.as_str()
                    })
                    .cloned()
                    .collect(),
                position: 0,
                case: provider.list_case,
                gate: provider.list_gate.clone(),
                gate_after: provider.gate_after,
                status: provider.status,
            }))
            .cast()
        };
        KERNEL_NATIVE_STATUS_OK
    }

    unsafe extern "C" fn provider_list_next(
        cursor: *mut c_void,
        sink_context: *mut c_void,
        sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV1) -> i32,
        has_more: *mut u32,
    ) -> i32 {
        let cursor = unsafe { &mut *cursor.cast::<ProviderCursor>() };
        cursor.probe.advances.fetch_add(1, Ordering::SeqCst);
        if cursor.position >= cursor.gate_after {
            if let Some(gate) = cursor.gate.take() {
                gate.entered.send(()).unwrap();
                gate.resume.lock().unwrap().recv().unwrap();
            }
        }
        let batch_size = if cursor.case == ListCase::Overflow {
            MAX_LIST_ITEMS + 1
        } else {
            MAX_LIST_ITEMS
        };
        let end = if cursor.case == ListCase::NoProgress {
            cursor.position
        } else {
            (cursor.position + batch_size).min(cursor.names.len())
        };
        for name in &cursor.names[cursor.position..end] {
            let meta = KernelNativeObjectMetaV1 {
                location: string_slice(name),
                size: 4,
                last_modified_unix_ms: 0,
            };
            let meta_ptr = if cursor.case == ListCase::NullMeta {
                std::ptr::null()
            } else {
                &meta
            };
            let status = unsafe { sink(sink_context, meta_ptr) };
            if status != KERNEL_NATIVE_STATUS_OK {
                return status;
            }
        }
        let count = end - cursor.position;
        cursor.position = end;
        match cursor.case {
            ListCase::MissingFlag => {}
            ListCase::BadFlag => unsafe { *has_more = 2 },
            ListCase::NoProgress => unsafe { *has_more = 1 },
            _ => unsafe { *has_more = u32::from(count == MAX_LIST_ITEMS) },
        }
        cursor.status
    }

    unsafe extern "C" fn provider_list_close(cursor: *mut c_void) {
        let cursor = unsafe { Box::from_raw(cursor.cast::<ProviderCursor>()) };
        assert_eq!(cursor.probe.releases.load(Ordering::SeqCst), 0);
        cursor.probe.closes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn provider_put(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
        body: KernelNativeByteSliceV1,
        mode: u32,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        let path = unsafe { input_string(path) };
        let body = if body.len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(body.ptr, body.len) }.to_vec()
        };
        provider
            .probe
            .put_requests
            .lock()
            .unwrap()
            .push((path, body, mode));
        provider.status
    }

    unsafe extern "C" fn provider_delete(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
    ) -> i32 {
        let provider = unsafe { &*context.cast::<Provider>() };
        provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
        provider
            .probe
            .delete_requests
            .lock()
            .unwrap()
            .push(unsafe { input_string(path) });
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
    #[case(Some((1, 56)))]
    #[case(Some((2, 0)))]
    #[case(Some((2, 48)))]
    #[case(Some((3, size_of::<KernelNativeObjectStoreDescriptorV3>() as u32 - 1)))]
    #[case(Some((3, size_of::<KernelNativeObjectStoreDescriptorV3>() as u32 + 1)))]
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
    #[case("list_open")]
    #[case("list_next")]
    #[case("list_close")]
    #[case("put")]
    #[case("delete")]
    #[case("release")]
    fn missing_descriptor_fields_do_not_adopt_or_release(#[case] field: &str) {
        let (mut descriptor, probe) = descriptor_for(Provider::default());
        let context = descriptor.context;
        match field {
            "context" => descriptor.context = std::ptr::null_mut(),
            "get" => descriptor.get = None,
            "list_open" => descriptor.list_open = None,
            "list_next" => descriptor.list_next = None,
            "list_close" => descriptor.list_close = None,
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
        let operation = move || operation_store.get_sync("table/a", &Default::default());
        unsafe { free_native_object_store(handle) };
        unsafe { free_native_object_store(clone) };
        drop(retained);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        let (meta, body, _) = operation().unwrap();
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
    #[case(false, b"data".as_slice())]
    #[case(true, b"".as_slice())]
    fn get_copies_output_before_provider_buffers_drop(
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
        let (meta, body, _) = unsafe { handle.as_ref() }
            .get_sync(path, &Default::default())
            .unwrap();
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
    #[case(GetCase::MissingSink)]
    #[case(GetCase::DuplicateSink)]
    #[case(GetCase::NullMeta)]
    #[case(GetCase::UnalignedMeta)]
    #[case(GetCase::NullLocation)]
    #[case(GetCase::EmptyLocation)]
    #[case(GetCase::InvalidUtf8)]
    #[case(GetCase::WrongLocation)]
    #[case(GetCase::AbsoluteLocation)]
    #[case(GetCase::OversizedLocation)]
    #[case(GetCase::InvalidTimestamp)]
    #[case(GetCase::SizeMismatch)]
    #[case(GetCase::NullBody)]
    #[case(GetCase::OversizedBody)]
    fn malformed_get_output_is_rejected(#[case] get_case: GetCase) {
        let (handle, probe) = handle_for(Provider {
            get_case,
            ..Default::default()
        });
        let result = unsafe { handle.as_ref() }.get_sync("table/a", &Default::default());
        assert!(matches!(result, Err(ObjectStoreError::Generic { .. })));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("table", vec!["table/a".into(), "table/b".into()])]
    #[case("", vec!["a".into(), "table/b".into()])]
    #[case("table", vec![])]
    #[case(
        "table",
        (0..MAX_LIST_ITEMS).map(|index| format!("table/{index:04}")).collect()
    )]
    #[tokio::test]
    async fn list_copies_native_listing(#[case] prefix: &str, #[case] names: Vec<String>) {
        let (handle, probe) = handle_for(Provider {
            list_names: names.clone(),
            ..Default::default()
        });
        let objects = unsafe { handle.as_ref() }
            .list(Some(&Path::from(prefix)))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        unsafe { free_native_object_store(handle) };
        let paths: Vec<_> = objects
            .iter()
            .map(|meta| meta.location.to_string())
            .collect();
        assert_eq!(paths, names);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("prefix")]
    #[case("segment_prefix")]
    #[case("limit")]
    #[case("partial_error")]
    #[case("bad_flag")]
    #[case("missing_flag")]
    #[case("no_progress")]
    #[case("null_meta")]
    #[case("offset")]
    #[tokio::test]
    async fn invalid_list_pages_discard_partial_output(#[case] violation: &str) {
        let mut provider = Provider::default();
        match violation {
            "prefix" => provider.list_names.push("wrong/c".into()),
            "segment_prefix" => provider.list_names.push("table_other/c".into()),
            "limit" => {
                provider.list_case = ListCase::Overflow;
                provider.list_names = (0..=MAX_LIST_ITEMS + 1)
                    .map(|index| format!("table/{index:04}"))
                    .collect();
            }
            "partial_error" => provider.status = KERNEL_NATIVE_STATUS_GENERIC,
            "bad_flag" => provider.list_case = ListCase::BadFlag,
            "missing_flag" => provider.list_case = ListCase::MissingFlag,
            "no_progress" => provider.list_case = ListCase::NoProgress,
            "null_meta" => provider.list_case = ListCase::NullMeta,
            "offset" => provider.list_case = ListCase::OffsetViolation,
            _ => unreachable!(),
        }
        let (handle, probe) = handle_for(provider);
        let offset = Path::from(if violation == "offset" {
            "table/a"
        } else {
            "table"
        });
        let result = unsafe { handle.as_ref() }
            .list_with_offset(Some(&Path::from("table")), &offset)
            .try_next()
            .await;
        assert!(matches!(result, Err(ObjectStoreError::Generic { .. })));
        assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case(KERNEL_NATIVE_STATUS_OK, "ok")]
    #[case(KERNEL_NATIVE_STATUS_NOT_FOUND, "not_found")]
    #[case(KERNEL_NATIVE_STATUS_ALREADY_EXISTS, "already_exists")]
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
            "already_exists" => assert!(matches!(
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
    #[case("version")]
    #[case("if_match")]
    #[case("if_none_match")]
    #[case("if_modified_since")]
    #[case("if_unmodified_since")]
    #[case("extensions")]
    #[tokio::test]
    async fn unsupported_get_options_are_explicitly_rejected(#[case] option: &str) {
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
        let (handle, probe) = handle_for(Provider::default());
        assert!(matches!(
            unsafe { handle.as_ref() }
                .get_opts(&Path::from("table/a"), options)
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        unsafe { free_native_object_store(handle) };
    }

    #[tokio::test]
    async fn writes_deletes_and_offset_listing_forward_to_provider() {
        let (handle, probe) = handle_for(Provider::default());
        let store = unsafe { handle.clone_as_arc() };
        unsafe { free_native_object_store(handle) };
        assert!(store
            .put_opts(
                &Path::from("table/a"),
                Bytes::new().into(),
                Default::default()
            )
            .await
            .is_ok());
        assert!(store
            .delete_stream(stream::empty().boxed())
            .try_collect::<Vec<_>>()
            .await
            .is_ok());
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 1);
        let objects = store
            .list_with_offset(Some(&Path::from("table")), &Path::from("table/a"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location.as_ref(), "table/b");
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 2);
        drop(store);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn v3_layout_and_misaligned_descriptor_header_are_checked() {
        assert_eq!(size_of::<KernelNativeGetOptionsV3>(), 24);
        if size_of::<usize>() == 8 {
            assert_eq!(size_of::<KernelNativeObjectStoreDescriptorV3>(), 72);
        }
        let mut storage = [0u64; 2];
        let descriptor = unsafe { storage.as_mut_ptr().cast::<u8>().add(1) }
            .cast::<KernelNativeObjectStoreDescriptorV3>();
        unsafe {
            descriptor
                .cast::<u32>()
                .write_unaligned(KERNEL_NATIVE_STORE_ABI_V3);
            descriptor
                .cast::<u32>()
                .add(1)
                .write_unaligned(size_of::<KernelNativeObjectStoreDescriptorV3>() as u32);
        }
        assert_extern_result_error_contains(
            unsafe { get_native_object_store(descriptor, allocate_err) },
            FFIKernelError::GenericError,
            "invalid native object store descriptor",
        );
    }

    #[rstest]
    #[case(None, false, (0, 0, 0, 0), 0..4, b"data".as_slice())]
    #[case(None, true, (1, 0, 0, 0), 0..4, b"".as_slice())]
    #[case(Some(GetRange::Bounded(1..3)), false, (0, 1, 1, 3), 1..3, b"at".as_slice())]
    #[case(Some(GetRange::Bounded(2..20)), false, (0, 1, 2, 20), 2..4, b"ta".as_slice())]
    #[case(Some(GetRange::Offset(1)), false, (0, 2, 1, 0), 1..4, b"ata".as_slice())]
    #[case(Some(GetRange::Suffix(2)), false, (0, 3, 0, 2), 2..4, b"ta".as_slice())]
    #[case(Some(GetRange::Suffix(20)), false, (0, 3, 0, 20), 0..4, b"data".as_slice())]
    #[case(Some(GetRange::Suffix(0)), false, (0, 3, 0, 0), 4..4, b"".as_slice())]
    #[case(Some(GetRange::Bounded(1..3)), true, (1, 1, 1, 3), 0..4, b"".as_slice())]
    #[tokio::test]
    async fn get_forwards_options_and_preserves_actual_range(
        #[case] range: Option<GetRange>,
        #[case] head: bool,
        #[case] expected: (u32, u32, u64, u64),
        #[case] actual: Range<u64>,
        #[case] body: &[u8],
    ) {
        let (handle, probe) = handle_for(Provider {
            expected_get: Some(expected),
            actual_range: head.then_some(actual.clone()),
            ..Default::default()
        });
        let result = unsafe { handle.as_ref() }
            .get_opts(
                &Path::from("table/a"),
                GetOptions {
                    range,
                    head,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.meta.size, 4);
        assert_eq!(result.range, actual);
        assert_eq!(result.bytes().await.unwrap().as_ref(), body);
        assert_eq!(probe.get_requests.lock().unwrap().len(), 1);
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(true, None, 0..0)]
    #[case(true, Some(GetRange::Bounded(1..3)), 0..(MAX_BODY_BYTES as u64 + 4096))]
    #[case(false, Some(GetRange::Bounded(1..3)), 1..3)]
    #[case(false, Some(GetRange::Offset(MAX_BODY_BYTES as u64 + 4094)), (MAX_BODY_BYTES as u64 + 4094)..(MAX_BODY_BYTES as u64 + 4096))]
    #[case(false, Some(GetRange::Suffix(2)), (MAX_BODY_BYTES as u64 + 4094)..(MAX_BODY_BYTES as u64 + 4096))]
    #[tokio::test]
    async fn head_and_small_ranges_allow_full_metadata_larger_than_payload_bound(
        #[case] head: bool,
        #[case] range: Option<GetRange>,
        #[case] actual: Range<u64>,
    ) {
        let (handle, _) = handle_for(Provider {
            get_size: MAX_BODY_BYTES as u64 + 4096,
            actual_range: Some(actual.clone()),
            ..Default::default()
        });
        let result = unsafe { handle.as_ref() }
            .get_opts(
                &Path::from("table/a"),
                GetOptions {
                    head,
                    range,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.meta.size, MAX_BODY_BYTES as u64 + 4096);
        assert_eq!(result.range, actual);
        assert_eq!(
            result.bytes().await.unwrap().len(),
            if head { 0 } else { 2 }
        );
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(false, None, Range { start: 3, end: 2 })]
    #[case(false, None, 0..5)]
    #[case(false, None, 1..4)]
    #[case(false, Some(GetRange::Bounded(1..3)), 0..2)]
    #[case(false, Some(GetRange::Offset(1)), 0..4)]
    #[case(false, Some(GetRange::Suffix(2)), 0..2)]
    #[case(false, Some(GetRange::Offset(4)), 4..4)]
    #[case(true, None, 0..5)]
    #[tokio::test]
    async fn malformed_actual_get_ranges_are_rejected(
        #[case] head: bool,
        #[case] range: Option<GetRange>,
        #[case] actual: Range<u64>,
    ) {
        let (handle, _) = handle_for(Provider {
            actual_range: Some(actual),
            ..Default::default()
        });
        assert!(matches!(
            unsafe { handle.as_ref() }
                .get_opts(
                    &Path::from("table/a"),
                    GetOptions {
                        head,
                        range,
                        ..Default::default()
                    }
                )
                .await,
            Err(ObjectStoreError::Generic { .. })
        ));
        unsafe { free_native_object_store(handle) };
    }

    #[tokio::test]
    async fn head_with_body_and_invalid_requested_ranges_are_rejected() {
        let (handle, probe) = handle_for(Provider {
            get_case: GetCase::HeadBody,
            ..Default::default()
        });
        assert!(matches!(
            unsafe { handle.as_ref() }
                .get_opts(
                    &Path::from("table/a"),
                    GetOptions {
                        head: true,
                        ..Default::default()
                    }
                )
                .await,
            Err(ObjectStoreError::Generic { .. })
        ));
        for range in [3..3, Range { start: 4, end: 3 }] {
            assert!(matches!(
                unsafe { handle.as_ref() }
                    .get_opts(
                        &Path::from("table/a"),
                        GetOptions {
                            range: Some(GetRange::Bounded(range)),
                            ..Default::default()
                        }
                    )
                    .await,
                Err(ObjectStoreError::Generic { .. })
            ));
        }
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 1);
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(PutMode::Overwrite, KERNEL_NATIVE_PUT_OVERWRITE)]
    #[case(PutMode::Create, KERNEL_NATIVE_PUT_CREATE)]
    #[tokio::test]
    async fn put_forwards_chunked_and_empty_payloads_and_atomic_mode(
        #[case] mode: PutMode,
        #[case] native_mode: u32,
    ) {
        let (handle, probe) = handle_for(Provider::default());
        let store = unsafe { handle.as_ref() };
        let payload: PutPayload = [
            Bytes::from_static(b"first"),
            Bytes::new(),
            Bytes::from_static(b"second"),
        ]
        .into_iter()
        .collect();
        let result = store
            .put_opts(&Path::from("table/a"), payload, mode.clone().into())
            .await
            .unwrap();
        assert_eq!(result.e_tag, None);
        assert_eq!(result.version, None);
        store
            .put_opts(&Path::from("table/empty"), PutPayload::new(), mode.into())
            .await
            .unwrap();
        assert_eq!(
            *probe.put_requests.lock().unwrap(),
            vec![
                ("table/a".into(), b"firstsecond".to_vec(), native_mode),
                ("table/empty".into(), vec![], native_mode)
            ]
        );
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(KERNEL_NATIVE_STATUS_ALREADY_EXISTS)]
    #[case(KERNEL_NATIVE_STATUS_NOT_FOUND)]
    #[case(KERNEL_NATIVE_STATUS_GENERIC)]
    #[case(KERNEL_NATIVE_STATUS_NOT_SUPPORTED)]
    #[tokio::test]
    async fn get_put_delete_propagate_native_error_statuses(#[case] status: i32) {
        let (handle, probe) = handle_for(Provider {
            status,
            ..Default::default()
        });
        let store = unsafe { handle.as_ref() };
        let get = store
            .get_opts(&Path::from("table/a"), Default::default())
            .await
            .map(|_| ());
        let put = store
            .put_opts(
                &Path::from("table/a"),
                b"new".as_slice().into(),
                PutMode::Create.into(),
            )
            .await
            .map(|_| ());
        let delete = store
            .delete_stream(stream::iter([Ok(Path::from("table/a"))]).boxed())
            .try_collect::<Vec<_>>()
            .await
            .map(|_| ());
        for result in [get, put, delete] {
            let error = result.unwrap_err();
            match status {
                KERNEL_NATIVE_STATUS_ALREADY_EXISTS => assert!(
                    matches!(error, ObjectStoreError::AlreadyExists { path, .. } if path == "table/a")
                ),
                KERNEL_NATIVE_STATUS_NOT_FOUND => assert!(
                    matches!(error, ObjectStoreError::NotFound { path, .. } if path == "table/a")
                ),
                KERNEL_NATIVE_STATUS_NOT_SUPPORTED => {
                    assert!(matches!(error, ObjectStoreError::NotSupported { .. }))
                }
                _ => assert!(matches!(error, ObjectStoreError::Generic { .. })),
            }
        }
        assert_eq!(
            *probe.put_requests.lock().unwrap(),
            vec![("table/a".into(), b"new".to_vec(), KERNEL_NATIVE_PUT_CREATE)]
        );
        assert_eq!(*probe.delete_requests.lock().unwrap(), vec!["table/a"]);
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 3);
        unsafe { free_native_object_store(handle) };
    }

    #[tokio::test]
    async fn delete_forwards_each_path_and_does_not_call_provider_for_input_error() {
        let (handle, probe) = handle_for(Provider::default());
        let mut deletes = unsafe { handle.as_ref() }.delete_stream(
            stream::iter([
                Ok(Path::from("table/b")),
                Err(generic_error("input error")),
                Ok(Path::from("table/a")),
            ])
            .boxed(),
        );
        unsafe { free_native_object_store(handle) };
        assert_eq!(
            deletes.try_next().await.unwrap().unwrap(),
            Path::from("table/b")
        );
        assert!(deletes.try_next().await.is_err());
        assert_eq!(
            deletes.try_next().await.unwrap().unwrap(),
            Path::from("table/a")
        );
        assert!(deletes.try_next().await.unwrap().is_none());
        assert_eq!(
            *probe.delete_requests.lock().unwrap(),
            vec!["table/b", "table/a"]
        );
        drop(deletes);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case("update")]
    #[case("tags")]
    #[case("attributes")]
    #[case("extensions")]
    #[tokio::test]
    async fn unsupported_put_options_are_rejected_without_io(#[case] option: &str) {
        let mut options = PutOptions::default();
        match option {
            "update" => {
                options.mode = PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some("tag".into()),
                    version: None,
                })
            }
            "tags" => options.tags.push("key", "value"),
            "attributes" => {
                options.attributes.insert(
                    object_store::Attribute::ContentType,
                    "application/json".into(),
                );
            }
            "extensions" => {
                options.extensions.insert(1u32);
            }
            _ => unreachable!(),
        }
        let (handle, probe) = handle_for(Provider::default());
        assert!(matches!(
            unsafe { handle.as_ref() }
                .put_opts(&Path::from("table/a"), b"data".as_slice().into(), options)
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        unsafe { free_native_object_store(handle) };
    }

    #[tokio::test]
    async fn bounded_inputs_and_remaining_operations_are_explicitly_unsupported() {
        let (handle, probe) = handle_for(Provider::default());
        let store = unsafe { handle.as_ref() };
        let oversized = Path::from("a".repeat(MAX_PATH_BYTES + 1));
        assert!(matches!(
            store.get_opts(&oversized, Default::default()).await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store
                .put_opts(&oversized, PutPayload::new(), Default::default())
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store
                .delete_stream(stream::iter([Ok(oversized.clone())]).boxed())
                .try_next()
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store.list(Some(&oversized)).try_next().await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store.list_with_offset(None, &oversized).try_next().await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        let chunk = Bytes::from_static(&[0; 1024 * 1024]);
        let payload: PutPayload =
            std::iter::repeat_n(chunk, MAX_BODY_BYTES / (1024 * 1024) + 1).collect();
        assert!(matches!(
            store
                .put_opts(&Path::from("table/a"), payload, Default::default())
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store
                .put_multipart_opts(&Path::from("table/a"), Default::default())
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store.list_with_delimiter(None).await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(matches!(
            store
                .copy_opts(
                    &Path::from("table/a"),
                    &Path::from("table/b"),
                    Default::default()
                )
                .await,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(0, 1)]
    #[case(128, 2)]
    #[case(256, 3)]
    #[case(300, 3)]
    #[tokio::test]
    async fn listing_preserves_native_order_across_batches_on_one_cursor(
        #[case] count: usize,
        #[case] advances: usize,
    ) {
        let names: Vec<_> = (0..count)
            .rev()
            .map(|index| format!("table/{index:04}"))
            .collect();
        let (handle, probe) = handle_for(Provider {
            list_names: names.clone(),
            ..Default::default()
        });
        let mut listing = unsafe { handle.as_ref() }.list(Some(&Path::from("table")));
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.opens.load(Ordering::SeqCst), 0);
        let mut paths = Vec::new();
        while let Some(meta) = listing.try_next().await.unwrap() {
            paths.push(meta.location.to_string());
        }
        assert_eq!(paths, names);
        assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
        assert_eq!(probe.advances.load(Ordering::SeqCst), advances);
        assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
        drop(listing);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_listing_after_first_batch_closes_retained_cursor_once() {
        let (handle, probe) = handle_for(Provider {
            list_names: (0..300).map(|index| format!("table/{index:04}")).collect(),
            ..Default::default()
        });
        let mut listing = unsafe { handle.as_ref() }.list(None);
        unsafe { free_native_object_store(handle) };
        for _ in 0..MAX_LIST_ITEMS {
            assert!(listing.try_next().await.unwrap().is_some());
        }
        assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
        assert_eq!(probe.advances.load(Ordering::SeqCst), 1);
        assert_eq!(probe.closes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        drop(listing);
        assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn offset_is_forwarded_and_native_order_and_duplicates_are_not_rewritten() {
        let names = vec![
            "table/z".into(),
            "table/c".into(),
            "table/c".into(),
            "table/b".into(),
        ];
        let (handle, probe) = handle_for(Provider {
            list_names: names.clone(),
            ..Default::default()
        });
        let objects = unsafe { handle.as_ref() }
            .list_with_offset(Some(&Path::from("table")), &Path::from("table/a"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            objects
                .iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            names
        );
        assert_eq!(
            *probe.list_requests.lock().unwrap(),
            vec![("table".into(), "table/a".into())]
        );
        assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
        unsafe { free_native_object_store(handle) };
    }

    #[rstest]
    #[case(false, KERNEL_NATIVE_STATUS_GENERIC)]
    #[case(false, KERNEL_NATIVE_STATUS_NOT_SUPPORTED)]
    #[case(true, KERNEL_NATIVE_STATUS_OK)]
    #[tokio::test]
    async fn failed_or_null_cursor_open_does_not_advance_or_close(
        #[case] null_cursor: bool,
        #[case] open_status: i32,
    ) {
        let (handle, probe) = handle_for(Provider {
            null_cursor,
            open_status,
            ..Default::default()
        });
        assert!(unsafe { handle.as_ref() }
            .list(None)
            .try_next()
            .await
            .is_err());
        assert_eq!(probe.advances.load(Ordering::SeqCst), 0);
        assert_eq!(probe.closes.load(Ordering::SeqCst), 0);
        unsafe { free_native_object_store(handle) };
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[case(0)]
    #[case(MAX_LIST_ITEMS)]
    #[tokio::test]
    async fn cancelled_listing_closes_once_only_after_blocking_next_returns(
        #[case] gate_after: usize,
    ) {
        let (entered, started) = std::sync::mpsc::channel();
        let (resume, waiting) = std::sync::mpsc::channel();
        let (released, finished) = std::sync::mpsc::channel();
        let (handle, probe) = handle_for(Provider {
            list_names: (0..300).map(|index| format!("table/{index:04}")).collect(),
            list_gate: Some(Arc::new(CallGate {
                entered,
                resume: Mutex::new(waiting),
            })),
            gate_after,
            ..Default::default()
        });
        *probe.released.lock().unwrap() = Some(released);
        let mut listing = unsafe { handle.as_ref() }.list(None);
        unsafe { free_native_object_store(handle) };
        for _ in 0..gate_after {
            assert!(listing.try_next().await.unwrap().is_some());
        }
        let advance = tokio::spawn(async move { listing.try_next().await });
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        advance.abort();
        assert!(advance.await.unwrap_err().is_cancelled());
        assert_eq!(probe.closes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
        resume.send(()).unwrap();
        tokio::task::spawn_blocking(move || finished.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
        assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
        assert_eq!(
            probe.advances.load(Ordering::SeqCst),
            gate_after / MAX_LIST_ITEMS + 1
        );
    }
}
