---
title: Azure Credential C ABI Consumer
description: Generated-header C consumer with synchronous Azure bearer callbacks and ownership tests.
---

## Build

Requires a matching, default-engine-enabled `delta_kernel_ffi` shared library,
the generated C header, C11 and C++17 compilers, CMake, and POSIX pthreads.
Linux and WSL are supported. Native Windows is unsupported by this example;
CMake rejects it because the example uses POSIX mutexes for concurrent counters.

From the kernel repository root, after the kernel library has been built:

```bash
cmake -S ffi/examples/azure-credentials -B /tmp/azure-credentials-build
cmake --build /tmp/azure-credentials-build
ctest --test-dir /tmp/azure-credentials-build --output-on-failure
```

The default library directory is `target/debug`. For a separate Linux build,
add `-DKERNEL_TARGET_DIR="$PWD/target-linux"` to the configure command. Headers
still come from `target/ffi-headers`; override `KERNEL_HEADER_DIR` independently
when needed. Set `KERNEL_PROFILE=release` for a release library. The executable's
build RPATH points to the selected library directory.

The build compiles the consumer as C and syntax-checks the same source as C++
against the real generated `delta_kernel_ffi.h`, with
`DEFINE_DEFAULT_ENGINE_BASE` and warnings treated as errors. The C++ check uses
the header's `ffi` namespace. No callback typedefs or substitute headers are
defined by the example. Its error representation follows `common/kernel_utils`:
an `EngineError` prefix plus a copied, caller-owned message. Unlike that helper,
it never prints the copied message.

## Ownership Self-Test

```bash
/tmp/azure-credentials-build/azure_credentials --self-test
```

Running without arguments also selects this test. It performs no storage I/O,
reads no bearer environment variable, and requires no Azure credentials.

* Rejects null, incompatible, out-of-bounds, and missing-required-callback configs
* Verifies rejected creation invokes no callback and transfers no context ownership
* Checks provider retention through builder abandonment and provider replacement
* Builds an Azure engine without acquisition and abandons a fresh snapshot builder
* Checks unconditional builder consumption on invalid UTF-8 and non-Azure build errors
* Checks zero acquisition calls, exactly one Release per accepted context, and balanced error allocation/free

Both ABI-v2 callbacks are compiled in C and C++. The ownership self-test does not
acquire a token. Actual acquisition and request signing are covered by Rust FFI
tests or a controlled loopback run. CTest caps this network-free test at 30 seconds;
that harness limit is not a kernel acquisition timeout.

## Snapshot Mode

Set `AzureStorageBearerToken` privately in the launching process environment.
Do not pass it as an argument or put it in shell history. Then run:

```bash
/tmp/azure-credentials-build/azure_credentials \
  'abfss://container@account.dfs.core.windows.net/table/'
```

An optional second argument sets `azure_storage_endpoint`. An explicit
`http://127.0.0.1:<port>` endpoint also enables `azure_allow_http` for a controlled
loopback fixture. Other endpoint overrides should use HTTPS. Never send a real
bearer token to a test server; use a synthetic bearer for loopback tests.

> [!WARNING]
> This is a static-credential string demo, not an OAuth implementation. The callback
> rereads the environment on each acquisition and supplies a wall-clock expiry
> one hour ahead solely for demonstration. It neither parses JWTs nor knows the
> token's actual expiry, and it cannot guarantee freshness or renew credentials.
> Replace that step with an identity SDK returning the actual token and expiry
> before using this pattern with real authentication.

Acquire runs synchronously for each native credential lookup. It allocates an
owned kernel string, sets the actual output expiry and then sets `has_token = 1`.
The kernel consumes that token on success and failure. Callback statuses are 0
success, 1 transient failure, 2 permanent failure and 3 cancelled. The callback
frees errors returned by `allocate_kernel_string` before returning failure.

This adapter does not cache or deduplicate acquisition. A production caller must
own refresh policy, its network timeout and cancellation. The callback can block
the executor thread; the kernel cannot time it out or forcibly cancel it. Do not
reenter the same provider or wait on work needing that executor.

Release marks the context after all retained builder, engine and provider references
and synchronous calls are gone. Main frees those references before destroying the
context's mutex. No worker, ticket queue, condition variable or callback-completion
export is needed.

Errors are copied before the allocation callback returns and freed by the
consumer. Output uses fixed diagnostics and callback counts only, never token
bytes, URLs, environment values, or raw kernel error messages. This example does
not enable kernel tracing. It makes no changes to the .NET integration.