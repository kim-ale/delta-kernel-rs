//! Experimental v1 native store C contract. No Rust object, future or allocator crosses this ABI.
//!
//! Descriptor creation transfers context only when Kernel accepts it. Callbacks must support
//! concurrent calls, never unwind, and finish all sink calls before returning. Input slices live
//! through callback return; output slices and metadata live through sink return. Sinks deep-copy
//! outputs and must not be retained. The provider alone frees context through `release`, once the
//! final Kernel reference is gone. Both native modules must stay loaded until all calls complete.

use std::ffi::{c_char, c_void};

/// Supported experimental descriptor version.
pub const KERNEL_NATIVE_STORE_ABI_V1: u32 = 1;
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
/// Retrieve full bytes and metadata.
pub const KERNEL_NATIVE_GET_FULL: u32 = 0;
/// Retrieve metadata with an empty body.
pub const KERNEL_NATIVE_GET_HEAD: u32 = 1;
/// Atomically replace the full object.
pub const KERNEL_NATIVE_PUT_OVERWRITE: u32 = 0;
/// Atomically publish the full object only when absent.
pub const KERNEL_NATIVE_PUT_CREATE: u32 = 1;

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
/// GET invokes its sink exactly once on success. LIST returns at most `max_items`, ascending,
/// matching `prefix` and strictly greater than `start_after`; `has_more` is 0 or 1. If more
/// entries remain the page must make progress. PUT obeys full-object atomic create/overwrite.
/// A nonzero sink result aborts the operation. Descriptor storage is borrowed during adoption
/// and copied by Kernel; after successful adoption only Kernel owns context release.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelNativeObjectStoreDescriptorV1 {
    /// Must equal KERNEL_NATIVE_STORE_ABI_V1.
    pub abi_version: u32,
    /// Must equal the exact size of this v1 struct.
    pub struct_size: u32,
    /// Provider-owned opaque context. Kernel never dereferences it.
    pub context: *mut c_void,
    /// Full GET or HEAD; outputs are borrowed only through the sink call.
    pub get: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            path: KernelNativeStringSliceV1,
            flags: u32,
            sink_context: *mut c_void,
            sink: unsafe extern "C" fn(
                *mut c_void,
                *const KernelNativeObjectMetaV1,
                KernelNativeByteSliceV1,
            ) -> i32,
        ) -> i32,
    >,
    /// Ordered bounded listing with an exclusive path offset, without retained callbacks.
    pub list: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            prefix: KernelNativeStringSliceV1,
            start_after: KernelNativeStringSliceV1,
            max_items: u32,
            sink_context: *mut c_void,
            sink: unsafe extern "C" fn(*mut c_void, *const KernelNativeObjectMetaV1) -> i32,
            has_more: *mut u32,
        ) -> i32,
    >,
    /// Atomic full-object write. Inputs are borrowed through return.
    pub put: Option<
        unsafe extern "C" fn(
            *mut c_void,
            KernelNativeStringSliceV1,
            KernelNativeByteSliceV1,
            u32,
        ) -> i32,
    >,
    /// Individual object deletion.
    pub delete_object: Option<unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1) -> i32>,
    /// Final context release, on any thread, once per adopted context.
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}
