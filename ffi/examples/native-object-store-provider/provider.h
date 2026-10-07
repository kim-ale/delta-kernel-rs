#ifndef NATIVE_OBJECT_STORE_PROVIDER_H
#define NATIVE_OBJECT_STORE_PROVIDER_H

#include <stdint.h>
#include "delta_kernel_ffi.h"

#if defined(_WIN32)
#define NATIVE_OBJECT_STORE_PROVIDER_API __declspec(dllimport)
#else
#define NATIVE_OBJECT_STORE_PROVIDER_API
#endif

#ifdef __cplusplus
using ffi::KernelNativeObjectStoreDescriptorV4;
using ffi::KernelNativeStringSliceV1;
extern "C" {
#endif

/* Factories borrow out, write only on success, and return Kernel native status codes.
 * The v4 descriptor has context and 18 mandatory callbacks. Measured x64 size
 * is 160 bytes; query prototype_descriptor_size rather than assuming packing.
 * Initialize from an ordinary thread. The caller owns a successful context until
 * Kernel accepts the descriptor. Release rejected/unadopted descriptors using their
 * release slot. After adoption only Kernel may release the context.
 * Keep both native DLLs loaded for process lifetime; never unload this provider.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_create_memory(
    KernelNativeObjectStoreDescriptorV4 *out);

/* endpoint is a full UTF-8 Azure service URL, borrowed only through return.
 * Account is "account", container is "container". HTTP is permitted for fixtures.
 * Native credentials are synthetic bearer tokens, not live Azure authentication.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_create_azure(
    KernelNativeStringSliceV1 endpoint,
    KernelNativeObjectStoreDescriptorV4 *out);

/* Borrows a live memory context, including after adoption. Retain a Kernel owner
 * throughout the call; never race final release. Creates version one once, returning
 * shared AlreadyExists status 2 on repetition and NotSupported for Azure. This is not an
 * injected write callback. Invoke from an ordinary
 * thread or blocking worker, never an asynchronous runtime worker.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_append_commit(void *context);

/* Process-global counters are never reset. Use before/after baselines in fixtures.
 * Callback count sums GET/ranges, LIST advance/delimiter, PUT/copy/rename,
 * multipart open/part-open/complete, and DELETE-batch attempts, including failures.
 * Waits, aborts, closes, LIST open, append, factory, sink, and release are excluded.
 * Credential generation is the largest issued
 * generation; memory storage does not acquire or simulate credentials.
 */
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_release_count(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_credential_requests(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_credential_generation(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_callback_count(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint32_t prototype_descriptor_size(void);

/* All callbacks use provider-owned object_store methods directly. Strings are UTF-8
 * and at most 64 KiB. Borrowed arrays are aligned, at most 4096 elements (128 for
 * deletion/each cursor advance); input/output bytes are at most 64 MiB in aggregate.
 * GET/ranges/PUT/delimiter/DELETE/complete sinks require a non-null context, run
 * serially, finish before callback return, and must never unwind or be retained.
 * Nonzero sink status returns Generic 3. Discard all output from a failed callback;
 * this does not roll back native storage side effects. DELETE item errors are sink
 * statuses, not outer callback failures; aggregate native errors can yield fewer
 * items than input. Null-empty optional ETag/version is None; nonnull-empty is Some("").
 * Status: 0=OK,1=NotFound,2=AlreadyExists,3=Generic,4=NotSupported,5=Precondition,
 * 6=NotModified,7=NotImplemented. No native error text crosses the boundary.
 * Multipart open/part-open write handles only on success. Part-open invokes native
 * put_part immediately; wait polls its future once, without the upload mutex.
 * Close consumes exactly once. Parts retain their parent upload/store lifetime.
 * Finish parts before complete/abort as required by the native trait. Upload close
 * drops without auto-abort; use explicit abort for native backend cleanup.
 * Request extensions have no ABI representation and are rejected by Kernel's adapter.
 */

#ifdef __cplusplus
}
#endif

#endif