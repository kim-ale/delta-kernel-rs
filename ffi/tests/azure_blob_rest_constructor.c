#define DEFINE_DEFAULT_ENGINE_BASE

#ifdef __cplusplus
#include "delta_kernel_ffi.hpp"
using namespace ffi;
using BlobBuilderHandle = Handle<ExclusiveEngineBuilder>;
using BlobBuilderResult = ExternResult<BlobBuilderHandle>;
#else
#include "delta_kernel_ffi.h"
typedef HandleExclusiveEngineBuilder BlobBuilderHandle;
typedef ExternResultHandleExclusiveEngineBuilder BlobBuilderResult;
#endif

static void ready_headers(void *context, CAuthHeaders *out, AllocateErrorFn allocate_error) {
    (void)context;
    (void)allocate_error;
    out->count = 0;
    out->ttl_ms = 0;
}

BlobBuilderResult blob_callback_constructor(BlobBuilderHandle builder) {
    return builder_with_azure_blob_rest_store(builder, ready_headers, NULL);
}

BlobBuilderResult blob_sas_constructor(BlobBuilderHandle builder) {
    return builder_with_azure_blob_rest_store(builder, NULL, NULL);
}