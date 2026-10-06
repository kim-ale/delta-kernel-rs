#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "delta_kernel_ffi.h"
#ifdef __cplusplus
using namespace ffi;
#endif

typedef struct ExampleError {
  EngineError base;
  char* message;
} ExampleError;

typedef struct Context {
  pthread_mutex_t mutex;
  unsigned acquisitions;
  unsigned releases;
} Context;

static pthread_mutex_t error_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned errors_allocated;
static unsigned errors_freed;

static KernelStringSlice slice(const char* text) {
  KernelStringSlice result = { text, strlen(text) };
  return result;
}

static EngineError* allocate_error(FFIKernelError kind, KernelStringSlice message) {
  ExampleError* error = (ExampleError*)malloc(sizeof(*error));
  if (!error || message.len == SIZE_MAX) abort();
  error->base.etype = kind;
  error->message = (char*)malloc(message.len + 1);
  if (!error->message) abort();
  if (message.len) memcpy(error->message, message.ptr, message.len);
  error->message[message.len] = '\0';
  pthread_mutex_lock(&error_mutex);
  errors_allocated++;
  pthread_mutex_unlock(&error_mutex);
  return &error->base;
}

static void free_error(EngineError* native_error) {
  ExampleError* error = (ExampleError*)native_error;
  free(error->message);
  free(error);
  pthread_mutex_lock(&error_mutex);
  errors_freed++;
  pthread_mutex_unlock(&error_mutex);
}

static uint32_t acquire(NullableCvoid data, CAzureBearerToken* out,
                        AllocateErrorFn allocator) {
  Context* context = (Context*)data;
  pthread_mutex_lock(&context->mutex);
  context->acquisitions++;
  pthread_mutex_unlock(&context->mutex);
  const char* token = getenv("AzureStorageBearerToken");
  size_t length = token ? strnlen(token, 65537) : 0;
  struct timespec now;
  if (!length || length > 65536 || clock_gettime(CLOCK_REALTIME, &now)) return 2;
  KernelStringSlice bearer = { token, length };
  ExternResultHandleExclusiveRustString result = allocate_kernel_string(bearer, allocator);
  if (result.tag != OkHandleExclusiveRustString) {
    free_error(result.err);
    return 2;
  }
  out->token = result.ok;
  out->expires_unix_ms = (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000 + 3600000;
  out->has_token = 1;
  return 0;
}

static void release(NullableCvoid data) {
  Context* context = (Context*)data;
  pthread_mutex_lock(&context->mutex);
  context->releases++;
  pthread_mutex_unlock(&context->mutex);
}

static bool initialize(Context* context) {
  memset(context, 0, sizeof(*context));
  return pthread_mutex_init(&context->mutex, NULL) == 0;
}

static bool finish(Context* context, bool accepted) {
  pthread_mutex_lock(&context->mutex);
  bool valid = context->releases == (accepted ? 1U : 0U);
  pthread_mutex_unlock(&context->mutex);
  pthread_mutex_destroy(&context->mutex);
  return valid;
}

static CAzureCredentialProviderConfig configuration(Context* context) {
  CAzureCredentialProviderConfig config;
  memset(&config, 0, sizeof(config));
  config.abi_version = 2;
  config.struct_size = (uint32_t)sizeof(config);
  config.minimum_lifetime_ms = 60000;
  config.max_token_bytes = 65536;
  config.context = context;
  config.acquire = acquire;
  config.release = release;
  return config;
}

static unsigned release_count(Context* context) {
  pthread_mutex_lock(&context->mutex);
  unsigned count = context->releases;
  pthread_mutex_unlock(&context->mutex);
  return count;
}

static bool update_builder(HandleExclusiveEngineBuilder* builder,
                           ExternResultHandleExclusiveEngineBuilder result) {
  *builder = NULL;
  if (result.tag != OkHandleExclusiveEngineBuilder) {
    free_error(result.err);
    return false;
  }
  *builder = result.ok;
  return true;
}

static bool invalid_configurations(void) {
  Context context;
  if (!initialize(&context)) return false;
  bool valid = true;
  for (unsigned test = 0; test < 10; test++) {
    CAzureCredentialProviderConfig config = configuration(&context);
    switch (test) {
      case 1: config.abi_version = 0; break;
      case 2: config.struct_size--; break;
      case 3: config.abi_version = 1; break;
      case 4: config.minimum_lifetime_ms = 0; break;
      case 5: config.max_token_bytes = 0; break;
      case 6: config.acquire = NULL; break;
      case 7: config.release = NULL; break;
      case 8: config.minimum_lifetime_ms = 3600001; break;
      case 9: config.max_token_bytes = 65537; break;
      default: break;
    }
    ExternResultHandleSharedAzureCredentialProvider result =
        create_azure_credential_provider(test ? &config : NULL, allocate_error);
    if (result.tag == ErrHandleSharedAzureCredentialProvider) free_error(result.err);
    else {
      free_azure_credential_provider(result.ok);
      valid = false;
    }
  }
  return finish(&context, false) && valid && !context.acquisitions;
}

static const char* test_url = "abfss://container@account.dfs.core.windows.net/table/";

static bool lifecycle(unsigned scenario) {
  Context contexts[2];
  if (!initialize(&contexts[0])) return false;
  if (!initialize(&contexts[1])) {
    finish(&contexts[0], false);
    return false;
  }
  HandleSharedAzureCredentialProvider providers[2] = { NULL, NULL };
  bool created[2] = { false, false };
  bool valid = true;
  HandleExclusiveEngineBuilder builder = NULL;
  for (unsigned index = 0; index < 2; index++) {
    CAzureCredentialProviderConfig config = configuration(&contexts[index]);
    ExternResultHandleSharedAzureCredentialProvider result =
        create_azure_credential_provider(&config, allocate_error);
    if (result.tag != OkHandleSharedAzureCredentialProvider) {
      free_error(result.err);
      valid = false;
      break;
    }
    providers[index] = result.ok;
    created[index] = true;
  }
  if (valid) {
    ExternResultHandleExclusiveEngineBuilder result =
        get_engine_builder(slice(scenario == 3 ? "file:///tmp/" : test_url), allocate_error);
    valid = update_builder(&builder, result);
  }
  if (valid)
    valid = update_builder(&builder, builder_with_azure_credential_provider(builder, &providers[0]));
  if (valid && scenario == 4)
    valid = update_builder(&builder, builder_with_azure_credential_provider(builder, &providers[1]));
  for (unsigned index = 0; index < 2; index++)
    if (providers[index]) free_azure_credential_provider(providers[index]);
  if (valid) {
    valid = release_count(&contexts[scenario == 4 ? 1 : 0]) == 0;
    valid = release_count(&contexts[scenario == 4 ? 0 : 1]) == 1 && valid;
  }
  if (valid && scenario == 2) {
    const char invalid_utf8[] = { (char)0xff };
    KernelStringSlice bad = { invalid_utf8, sizeof(invalid_utf8) };
    ExternResultHandleExclusiveEngineBuilder result = builder_with_option(builder, slice("key"), bad);
    builder = NULL;
    valid = result.tag == ErrHandleExclusiveEngineBuilder;
    if (valid) free_error(result.err);
    else builder = result.ok;
  } else if (valid && (scenario == 1 || scenario == 3)) {
    ExternResultHandleSharedExternEngine result = builder_build(builder);
    builder = NULL;
    valid = (result.tag == OkHandleSharedExternEngine) == (scenario == 1);
    if (result.tag == OkHandleSharedExternEngine) {
      ExternResultHandleExclusiveSnapshotBuilder snapshot = get_snapshot_builder(slice(test_url), result.ok);
      free_engine(result.ok);
      if (snapshot.tag == OkHandleExclusiveSnapshotBuilder) {
        valid = release_count(&contexts[0]) == 0 && valid;
        free_snapshot_builder(snapshot.ok);
      } else {
        free_error(snapshot.err);
        valid = false;
      }
    } else free_error(result.err);
  }
  if (builder) free_engine_builder(builder);
  for (unsigned index = 0; index < 2; index++) {
    valid = finish(&contexts[index], created[index]) && valid;
    valid = !contexts[index].acquisitions && valid;
  }
  return valid;
}

static bool self_test(void) {
  bool valid = invalid_configurations();
  for (unsigned scenario = 0; scenario < 5; scenario++) valid = lifecycle(scenario) && valid;
  return valid && errors_allocated == errors_freed && errors_allocated;
}

static bool read_table(const char* url, const char* endpoint) {
  Context context;
  if (!initialize(&context)) return false;
  CAzureCredentialProviderConfig config = configuration(&context);
  ExternResultHandleSharedAzureCredentialProvider provider =
      create_azure_credential_provider(&config, allocate_error);
  if (provider.tag != OkHandleSharedAzureCredentialProvider) {
    free_error(provider.err);
    finish(&context, false);
    return false;
  }
  HandleExclusiveEngineBuilder builder = NULL;
  bool valid = update_builder(&builder, get_engine_builder(slice(url), allocate_error));
  if (valid)
    valid = update_builder(&builder, builder_with_azure_credential_provider(builder, &provider.ok));
  free_azure_credential_provider(provider.ok);
  if (valid && endpoint) {
    valid = update_builder(&builder, builder_with_option(builder, slice("azure_storage_endpoint"), slice(endpoint)));
    if (valid && !strncmp(endpoint, "http://127.0.0.1:", 17))
      valid = update_builder(&builder, builder_with_option(builder, slice("azure_allow_http"), slice("true")));
  }
  if (valid) {
    ExternResultHandleSharedExternEngine engine = builder_build(builder);
    builder = NULL;
    valid = engine.tag == OkHandleSharedExternEngine;
    if (valid) {
      ExternResultHandleExclusiveSnapshotBuilder snapshot_builder = get_snapshot_builder(slice(url), engine.ok);
      valid = snapshot_builder.tag == OkHandleExclusiveSnapshotBuilder;
      if (valid) {
        ExternResultHandleSharedSnapshot snapshot = snapshot_builder_build(snapshot_builder.ok);
        valid = snapshot.tag == OkHandleSharedSnapshot;
        if (valid) free_snapshot(snapshot.ok);
        else free_error(snapshot.err);
      } else free_error(snapshot_builder.err);
      free_engine(engine.ok);
    } else free_error(engine.err);
  }
  if (builder) free_engine_builder(builder);
  valid = finish(&context, true) && valid;
    printf("Credential acquisitions: %u; releases: %u\n",
      context.acquisitions, context.releases);
  return valid;
}

int main(int argc, char* argv[]) {
  if (argc == 1 || (argc == 2 && !strcmp(argv[1], "--self-test"))) {
    bool passed = self_test();
    puts(passed ? "Ownership self-test passed (no acquisition)." : "Ownership self-test failed.");
    return passed ? 0 : 1;
  }
  if (argc == 2 || argc == 3) {
    bool passed = read_table(argv[1], argc == 3 ? argv[2] : NULL);
    fputs(passed ? "Snapshot opened.\n" : "Kernel operation failed (details suppressed).\n", stderr);
    return passed ? 0 : 1;
  }
  fputs("Usage: azure_credentials [--self-test | TABLE_URL [ENDPOINT]]\n", stderr);
  return 1;
}