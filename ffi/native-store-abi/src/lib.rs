//! Native ObjectStore forwarding contract. No Rust object or allocator crosses this ABI.
//!
//! Descriptor creation transfers context only when Kernel accepts it. Callbacks must support
//! concurrent operations, never unwind, and finish serial, non-overlapping sink calls before
//! return. Input slices live
//! through callback return; output slices and metadata live through sink return. Sinks deep-copy
//! outputs and must not be retained. The provider alone frees context through `release`, once the
//! final Kernel reference is gone. Both native modules must stay loaded until all calls complete.

use std::ffi::{c_char, c_void};

/// Supported experimental descriptor version.
pub const KERNEL_NATIVE_STORE_ABI_V3: u32 = 3;
/// Callback completed successfully.
pub const KERNEL_NATIVE_STATUS_OK: i32 = 0;
/// Object does not exist.
pub const KERNEL_NATIVE_STATUS_NOT_FOUND: i32 = 1;
/// Atomic create found an existing object.
pub const KERNEL_NATIVE_STATUS_ALREADY_EXISTS: i32 = 2;
/// Provider operation failed; no secret error text crosses this ABI.
pub const KERNEL_NATIVE_STATUS_GENERIC: i32 = 3;
/// Provider does not support this operation.
pub const KERNEL_NATIVE_STATUS_NOT_SUPPORTED: i32 = 4;
/// Replace the complete object atomically.
pub const KERNEL_NATIVE_PUT_OVERWRITE: u32 = 0;
/// Create the complete object atomically only when absent.
pub const KERNEL_NATIVE_PUT_CREATE: u32 = 1;
/// Maximum records returned by one native cursor advance.
pub const KERNEL_NATIVE_LIST_BATCH_SIZE: usize = 128;
/// Expanded native-operation descriptor version.
pub const KERNEL_NATIVE_STORE_ABI_V4: u32 = 4;
/// Native conditional operation failed.
pub const KERNEL_NATIVE_STATUS_PRECONDITION: i32 = 5;
/// Native conditional GET reports no modification.
pub const KERNEL_NATIVE_STATUS_NOT_MODIFIED: i32 = 6;
/// Native backend does not implement this operation.
pub const KERNEL_NATIVE_STATUS_NOT_IMPLEMENTED: i32 = 7;
/// Conditional Update PUT using ETag/version.
pub const KERNEL_NATIVE_PUT_UPDATE: u32 = 2;
/// Maximum borrowed collection length, except 128-record list/delete batches.
pub const KERNEL_NATIVE_MAX_COLLECTION: usize = 4096;

/// Native GET options: full, bounded, offset or suffix range, and metadata-only HEAD.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KernelNativeGetOptionsV3 {
    /// Zero for GET, one for HEAD. Other values are invalid.
    pub head: u32,
    /// Zero: full, one: bounded [start,end), two: offset(start), three: suffix(end).
    pub range_kind: u32,
    /// Start for bounded and offset ranges; zero otherwise.
    pub start: u64,
    /// Exclusive end for bounded ranges, length for suffix ranges; zero otherwise.
    pub end: u64,
}

/// Borrowed UTF-8 bytes. Null is permitted only with zero length.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeStringSliceV1 {
    /// First UTF-8 byte.
    pub ptr: *const c_char,
    /// Byte count, not character count.
    pub len: usize,
}

/// Borrowed payload bytes. Null is permitted only with zero length.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeByteSliceV1 {
    /// First byte.
    pub ptr: *const u8,
    /// Byte count.
    pub len: usize,
}

/// Borrowed object metadata; all paths are store-relative.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeObjectMetaV1 {
    /// Object path.
    pub location: KernelNativeStringSliceV1,
    /// Full object size.
    pub size: u64,
    /// Modification timestamp in milliseconds since Unix epoch.
    pub last_modified_unix_ms: i64,
}

/// Independent native provider descriptor. All callbacks and context must be non-null.
///
/// GET forwards options to the provider store and emits full metadata, the returned range and
/// only that range's bytes. HEAD emits no body. Bodies are at most 64 MiB; paths at most 64 KiB.
/// LIST opens a provider-owned native stream, forwarding prefix and exclusive offset. Advance
/// emits at most 128 entries and sets has_more to zero or one. Order is the native store's order.
/// A full final batch may set has_more to one, followed by an empty final batch with zero.
/// Cursor output is initialized only on successful open; caller closes every successful cursor,
/// including failed advances and cancellation, before releasing its retained store context.
/// A nonzero sink result aborts the operation. Descriptor storage is borrowed during adoption
/// and copied by Kernel; after successful adoption only Kernel owns context release.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeObjectStoreDescriptorV3 {
    /// Must equal KERNEL_NATIVE_STORE_ABI_V3.
    pub abi_version: u32,
    /// Must equal the exact size of this v3 struct.
    pub struct_size: u32,
    /// Provider-owned opaque context. Kernel never dereferences it.
    pub context: *mut c_void,
    /// GET/HEAD/range forwarding; output bytes and actual range are borrowed through the sink.
    pub get: Option<
        unsafe extern "C" fn(
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
        ) -> i32,
    >,
    /// Open a native listing stream; no Rust stream or allocation crosses the boundary.
    pub list_open: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            prefix: KernelNativeStringSliceV1,
            start_after: KernelNativeStringSliceV1,
            cursor: *mut *mut c_void,
        ) -> i32,
    >,
    /// Advance an exclusively borrowed cursor. Sink calls are serial and cannot escape return.
    pub list_next: Option<
        unsafe extern "C" fn(
            cursor: *mut c_void,
            sink_context: *mut c_void,
            sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV1) -> i32,
            has_more: *mut u32,
        ) -> i32,
    >,
    /// Consume a native cursor on any thread; never concurrently with an advance.
    pub list_close: Option<unsafe extern "C" fn(*mut c_void)>,
    /// Full-object atomic Create/Overwrite PUT, forwarding bytes and mode to the native store.
    pub put: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeByteSliceV1,
            u32,
        ) -> i32,
    >,
    /// Individual deletion delegated to the native store.
    pub delete_object: Option<unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1) -> i32>,
    /// Final context release, on any thread, once per adopted context.
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}

/// Borrowed key/value pair, used for native tags and attributes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeKeyValueV4 {
    /// UTF-8 key. Attribute keys use fixed names described by the contract.
    pub key: KernelNativeStringSliceV1,
    /// UTF-8 value, including empty values.
    pub value: KernelNativeStringSliceV1,
}

/// Full-object metadata with optional ETag/version. Null empty strings mean None;
/// non-null empty strings mean Some(""). All storage lives through sink return.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeObjectMetaV4 {
    /// Existing path/size/modification-time representation.
    pub base: KernelNativeObjectMetaV1,
    /// Optional ETag.
    pub e_tag: KernelNativeStringSliceV1,
    /// Optional version.
    pub version: KernelNativeStringSliceV1,
}

/// PUT or multipart-completion result, borrowed through its single sink call.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativePutResultV4 {
    /// Optional ETag.
    pub e_tag: KernelNativeStringSliceV1,
    /// Optional version.
    pub version: KernelNativeStringSliceV1,
}

/// Bounded byte range [start,end).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeRangeV4 {
    /// Inclusive start.
    pub start: u64,
    /// Exclusive end.
    pub end: u64,
}

/// GET conditions and native range options. Optional strings follow metadata rules.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeGetOptionsV4 {
    /// Existing HEAD/range representation.
    pub base: KernelNativeGetOptionsV3,
    /// Optional If-Match.
    pub if_match: KernelNativeStringSliceV1,
    /// Optional If-None-Match.
    pub if_none_match: KernelNativeStringSliceV1,
    /// Optional object version.
    pub version: KernelNativeStringSliceV1,
    /// Bit 0: modified-since present; bit 1: unmodified-since present. Other bits invalid.
    pub time_flags: u32,
    /// Modified-since nanoseconds, less than 1,000,000,000.
    pub modified_nanos: u32,
    /// Modified-since Unix seconds.
    pub modified_seconds: i64,
    /// Unmodified-since Unix seconds.
    pub unmodified_seconds: i64,
    /// Unmodified-since nanoseconds, less than 1,000,000,000.
    pub unmodified_nanos: u32,
}

/// Write options. Multipart requires mode=Overwrite and no update strings.
/// Tags/attributes are borrowed arrays, copied by the provider before native work.
/// Attribute keys are content-disposition,content-encoding,content-language,
/// content-type,cache-control,storage-class, or `metadata:<user key>`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeWriteOptionsV4 {
    /// Overwrite=0, Create=1, Update=2.
    pub mode: u32,
    /// Conditional Update ETag.
    pub e_tag: KernelNativeStringSliceV1,
    /// Conditional Update version.
    pub version: KernelNativeStringSliceV1,
    /// Tag array; null permitted only when count is zero.
    pub tags: *const KernelNativeKeyValueV4,
    /// Tag count.
    pub tags_len: usize,
    /// Attribute array; null permitted only when count is zero.
    pub attributes: *const KernelNativeKeyValueV4,
    /// Attribute count.
    pub attributes_len: usize,
}

/// Native-operation forwarding v4. Every slot is mandatory; unsupported backends
/// return their native error status instead of adapter-side emulation.
/// Input arrays and slices are borrowed through callback return. Output arrays,
/// metadata and strings are borrowed only through serial sink calls and deep-copied.
/// Opened cursors/uploads/parts are provider allocations and closed exactly once
/// by the matching close slot, before their retained context owner is released.
/// Multipart part_open calls native put_part at invocation time and returns an
/// owned future handle; part_wait polls it once. Close drops that future. Upload
/// complete/abort borrow the upload; upload_close drops it, without promising abort.
/// Part handles retain the upload lifetime. No advance/wait/close may race on one handle.
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(
    clippy::type_complexity,
    reason = "Inline nullable C callback types are required for correct cbindgen declarations."
)]
pub struct KernelNativeObjectStoreDescriptorV4 {
    /// Must equal four; older descriptors are rejected.
    pub abi_version: u32,
    /// Exact descriptor size.
    pub struct_size: u32,
    /// Provider context.
    pub context: *mut c_void,
    /// GET/HEAD/range/conditional/version request; sink receives attributes and actual range.
    pub get: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeGetOptionsV4,
            *mut c_void,
            unsafe extern "C" fn(
                *mut c_void,
                *const KernelNativeObjectMetaV4,
                KernelNativeByteSliceV1,
                u64,
                u64,
                *const KernelNativeKeyValueV4,
                usize,
            ) -> i32,
        ) -> i32,
    >,
    /// One native get_ranges call for the complete requested range array. Results match input
    /// order.
    pub get_ranges: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            *const KernelNativeRangeV4,
            usize,
            *mut c_void,
            unsafe extern "C" fn(*mut c_void, usize, KernelNativeByteSliceV1) -> i32,
        ) -> i32,
    >,
    /// Open native listing stream.
    pub list_open: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeStringSliceV1,
            *mut *mut c_void,
        ) -> i32,
    >,
    /// Advance at most 128 records, preserving native order.
    pub list_next: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV4) -> i32,
            *mut u32,
        ) -> i32,
    >,
    /// Consume cursor.
    pub list_close: Option<unsafe extern "C" fn(*mut c_void)>,
    /// Native delimiter listing: one sink for objects and common prefixes.
    pub list_delimiter: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            *mut c_void,
            unsafe extern "C" fn(
                *mut c_void,
                *const KernelNativeObjectMetaV4,
                usize,
                *const KernelNativeStringSliceV1,
                usize,
            ) -> i32,
        ) -> i32,
    >,
    /// Native put_opts and borrowed result.
    pub put: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeByteSliceV1,
            KernelNativeWriteOptionsV4,
            *mut c_void,
            unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
        ) -> i32,
    >,
    /// Native delete_stream call for up to 128 paths. Sink preserves native result order,
    /// including per-item errors; aggregate native errors may yield fewer results than input
    /// paths.
    pub delete_batch: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const KernelNativeStringSliceV1,
            usize,
            *mut c_void,
            unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1, i32) -> i32,
        ) -> i32,
    >,
    /// Native copy_opts; mode 0=Overwrite,1=Create.
    pub copy: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeStringSliceV1,
            u32,
        ) -> i32,
    >,
    /// Native rename_opts; mode 0=Overwrite,1=Create.
    pub rename: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeStringSliceV1,
            u32,
        ) -> i32,
    >,
    /// Native put_multipart_opts; output initialized only on success.
    pub multipart_open: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeWriteOptionsV4,
            *mut *mut c_void,
        ) -> i32,
    >,
    /// Native put_part at invocation time; output initialized only on success.
    pub multipart_part_open:
        Option<unsafe extern "C" fn(*mut c_void, KernelNativeByteSliceV1, *mut *mut c_void) -> i32>,
    /// Poll native part future once, exclusively borrowed.
    pub multipart_part_wait: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    /// Consume native part future handle.
    pub multipart_part_close: Option<unsafe extern "C" fn(*mut c_void)>,
    /// Native upload.complete, exclusively borrowed, returning PUT result.
    pub multipart_complete: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
        ) -> i32,
    >,
    /// Native upload.abort, exclusively borrowed.
    pub multipart_abort: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    /// Consume native upload without adapter-side abort policy.
    pub multipart_close: Option<unsafe extern "C" fn(*mut c_void)>,
    /// Final context release after all borrowed operations and owned children close.
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}
