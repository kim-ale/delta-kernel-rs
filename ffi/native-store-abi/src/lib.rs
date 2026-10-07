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
