---
title: Azure Credential C ABI Consumer
description: Generated-header C consumer with asynchronous Azure bearer callbacks and ownership tests.
---

## Build

Requires a matching, default-engine-enabled `delta_kernel_ffi` shared library,
the generated C header, C11 and C++17 compilers, CMake, and POSIX pthreads.
Linux and WSL are supported. Native Windows is unsupported by this example;
CMake rejects it rather than substituting a different threading implementation.

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
* Checks zero Start/Cancel calls, exactly one Release per accepted context, and balanced error allocation/free

All three callback assignments are compiled in C and C++. The ownership test
does not invoke Start, Cancel, or ticket completion at runtime. Actual acquisition,
completion, cancellation races, and native HTTP integration need the Rust FFI
interop tests or a separately controlled Azure/loopback run. A refused endpoint
is not a bounded acquisition test: stock storage retries can extend its runtime.
CTest caps the network-free ownership test at 30 seconds.

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
> This is a static-credential string demo, not an OAuth implementation. The worker
> rereads the environment on each acquisition and supplies a wall-clock expiry
> one hour ahead solely for demonstration. It neither parses JWTs nor knows the
> token's actual expiry, and it cannot guarantee freshness or renew credentials.
> Replace that step with an identity SDK returning the actual token and expiry
> before using this pattern with real authentication.

Start queues one exclusively owned ticket and returns without doing credential
work. Each native credential lookup is independent; this example supplies no token
cache or acquisition deduplication. A full queue frees the rejected ticket once.
The worker owns each accepted
ticket until it completes or fails it once. A false completion result means the
native request was retired or delivery failed, not that the consumed ticket can be retried.
Cancel records only the numeric request ID, never accesses a raw ticket, and does
not compete with the worker for ticket ownership. The callbacks use short mutex
critical sections; no callback performs credential I/O or joins a worker.

Release only marks the context and signals main. Main drops its builder, engine,
snapshot, and caller provider references, waits for native Release, then stops
and joins the worker before destroying the context. This remains safe when
Release occurs inside completion while the worker still uses its context.

Errors are copied before the allocation callback returns and freed by the
consumer. Output uses fixed diagnostics and callback counts only, never token
bytes, URLs, environment values, or raw kernel error messages. This example does
not enable kernel tracing. It makes no changes to the .NET integration.