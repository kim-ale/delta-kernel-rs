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
using ffi::KernelNativeObjectStoreDescriptorV1;
using ffi::KernelNativeStringSliceV1;
extern "C" {
#endif

/* Factories borrow out, write only on success, and return Kernel native status codes.
 * Initialize from an ordinary thread. The caller owns a successful context until
 * Kernel accepts the descriptor. Release rejected/unadopted descriptors using their
 * release slot. After adoption only Kernel may release the context.
 * Keep both native DLLs loaded for process lifetime; never unload this provider.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_create_memory(
    KernelNativeObjectStoreDescriptorV1 *out);

/* endpoint is a full UTF-8 Azure service URL, borrowed only through return.
 * Account is "account", container is "container". HTTP is permitted for fixtures.
 * Native credentials are synthetic bearer tokens, not live Azure authentication.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_create_azure(
    KernelNativeStringSliceV1 endpoint,
    KernelNativeObjectStoreDescriptorV1 *out);

/* Borrows a live memory context, including after adoption. Retain a Kernel owner
 * throughout the call; never race final release. Creates version one once, returning
 * AlreadyExists on repetition and NotSupported for Azure. Invoke from an ordinary
 * thread or blocking worker, never an asynchronous runtime worker.
 */
NATIVE_OBJECT_STORE_PROVIDER_API int32_t prototype_append_commit(void *context);

/* Process-global counters are never reset. Use before/after baselines in fixtures.
 * Callback count sums GET/LIST/PUT/DELETE attempts, including failures, not append,
 * factory, sink, or release calls. Credential generation is the largest issued
 * generation; memory storage does not acquire or simulate credentials.
 */
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_release_count(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_credential_requests(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_credential_generation(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint64_t prototype_callback_count(void);
NATIVE_OBJECT_STORE_PROVIDER_API uint32_t prototype_descriptor_size(void);

#ifdef __cplusplus
}
#endif

#endif