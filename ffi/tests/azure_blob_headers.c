#define DEFINE_DEFAULT_ENGINE_BASE
#include "delta_kernel_ffi.h"

#ifdef __cplusplus
using namespace ffi;
using ProbeResult = ExternResultHandleExclusiveEngineBuilder;
using ProbeBuilder = HandleExclusiveEngineBuilder;
#else
typedef struct ExternResultHandleExclusiveEngineBuilder ProbeResult;
typedef HandleExclusiveEngineBuilder ProbeBuilder;
#endif

static void fill_headers(NullableCvoid context, struct CAuthHeaders *out,
                         AllocateErrorFn allocate_error) {
    (void)context;
    (void)allocate_error;
    out->count = 0;
    out->ttl_ms = 0;
}

ProbeResult probe_azure_blob_headers(ProbeBuilder builder, bool dynamic) {
    return builder_with_azure_blob_headers(builder, dynamic ? fill_headers : NULL, NULL);
}
