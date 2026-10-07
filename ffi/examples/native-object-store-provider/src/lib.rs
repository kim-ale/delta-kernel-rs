//! Independent native ObjectStore prototype. Only the v3 C descriptor crosses DLL boundaries.
//! The provider module and its runtime must stay loaded for the lifetime of the process.

use std::ffi::c_void;
use std::mem::size_of;

use delta_kernel_native_store_abi::{
    KernelNativeObjectStoreDescriptorV3, KernelNativeStringSliceV1, KERNEL_NATIVE_STATUS_GENERIC,
};

mod credentials;
mod provider;

/// Creates an InMemory store seeded with an empty Delta table at `table/`, version zero.
/// Returns a native status code. Writes `out` only on success; the caller owns the context until
/// Kernel accepts the descriptor, then only Kernel may invoke its release callback.
///
/// # Safety
/// `out` must be null or aligned, writable descriptor storage exclusively borrowed for this call.
/// Call from an ordinary thread, not an asynchronous runtime worker.
#[no_mangle]
pub unsafe extern "C" fn prototype_create_memory(
    out: *mut KernelNativeObjectStoreDescriptorV3,
) -> i32 {
    provider::guarded(|| {
        if out.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let context = provider::create_memory()?;
        // SAFETY: The caller provides exclusive writable storage; no fallible work follows
        // transfer.
        unsafe { out.write(provider::descriptor(context)) };
        Ok(())
    })
}

/// Creates native Azure storage with synthetic rotating bearer credentials, account `account`,
/// container `container`, and the supplied full endpoint URL. Intended for a loopback fixture.
/// Returns a native status code, without error text. Writes `out` only on success.
/// Ownership transfers only when Kernel accepts the descriptor, as with `prototype_create_memory`.
///
/// # Safety
/// `endpoint` must point to readable UTF-8 for its byte length, or be null with zero length.
/// `out` must be null or aligned, writable descriptor storage exclusively borrowed for this call.
/// Call from an ordinary thread, not an asynchronous runtime worker.
#[no_mangle]
pub unsafe extern "C" fn prototype_create_azure(
    endpoint: KernelNativeStringSliceV1,
    out: *mut KernelNativeObjectStoreDescriptorV3,
) -> i32 {
    provider::guarded(|| {
        if out.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        // SAFETY: The endpoint is readable through this call and is copied before client creation.
        let endpoint = unsafe { provider::copy_string(endpoint) }?;
        let context = provider::create_azure(endpoint)?;
        // SAFETY: The caller provides exclusive writable storage; no fallible work follows
        // transfer.
        unsafe { out.write(provider::descriptor(context)) };
        Ok(())
    })
}

/// Atomically appends version one to the seeded memory table. Returns AlreadyExists status 2 on
/// repetition and NotSupported for Azure. Does not consume the context or change its ownership.
///
/// # Safety
/// `context` must be null or an unreleased context from this provider. After Kernel adoption, this
/// is only a borrowed alias: retain a Kernel owner throughout the call and never invoke release.
/// Call on an ordinary thread or a blocking worker, not an asynchronous runtime worker.
#[no_mangle]
pub unsafe extern "C" fn prototype_append_commit(context: *mut c_void) -> i32 {
    provider::guarded(|| {
        // SAFETY: The caller retains a live owner for the whole synchronous operation.
        unsafe { provider::append_commit(context) }
    })
}

/// Returns the process-global count of final context releases; factories do not reset it.
#[no_mangle]
pub extern "C" fn prototype_release_count() -> u64 {
    provider::release_count()
}

/// Returns process-global native Azure credential retrievals, not memory storage operations.
#[no_mangle]
pub extern "C" fn prototype_credential_requests() -> u64 {
    credentials::request_count()
}

/// Returns the largest synthetic credential generation issued, or zero before any retrieval.
#[no_mangle]
pub extern "C" fn prototype_credential_generation() -> u64 {
    credentials::generation()
}

/// Returns the aggregate process-global GET, LIST advance, PUT and DELETE invocation count.
/// Includes failed attempts; excludes LIST open/close, factories, sinks, direct append and release.
#[no_mangle]
pub extern "C" fn prototype_callback_count() -> u64 {
    provider::callback_count()
}

/// Returns this library's exact descriptor size for the current platform, without packing.
#[no_mangle]
pub extern "C" fn prototype_descriptor_size() -> u32 {
    size_of::<KernelNativeObjectStoreDescriptorV3>() as u32
}
