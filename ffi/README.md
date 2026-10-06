---
title: Delta Kernel Rust FFI
description: C and C++ interfaces, build instructions, and caller ownership contracts.
---

This crate provides a C foreign function interface (ffi) for delta-kernel-rs.

## Azure Credential Providers

Default-engine builds support on-demand Azure bearer acquisition through
`create_azure_credential_provider` and `builder_with_azure_credential_provider`.
The provider is attached before normal Azure store construction. Later requests
use renewed credentials through the same engine and store; credential renewal
does not rebuild snapshots. Without a provider, existing construction is unchanged.

Each credential lookup directly invokes a synchronous callback inside the existing
async Rust provider interface. The caller owns caching, refresh, acquisition timeout
and cancellation. Acquisition may block the native executor thread, and the kernel
cannot preempt a hung callback. Do not reenter acquisition on the same provider or
block on work that requires that executor to make progress. Idle engines do not
invoke acquisition.

`CAzureCredentialProviderConfig` ABI version 2 requires the exact generated size,
Acquire and Release callbacks, minimum remaining lifetime of 1-3600000 milliseconds,
and a maximum token length of 1-65536 bytes. Version 1's async descriptor is rejected.

Acquire receives a borrowed `CAzureBearerToken` output and the error allocator.
Return 0 for success, 1 for transient failure, 2 for permanent failure, or 3 for
cancelled acquisition. Allocate the token with `allocate_kernel_string`, initialize
the owned token handle, set its actual UTC Unix-millisecond expiry, then set
`has_token = 1`. The kernel consumes the returned handle on every status. With
`has_token = 0` no token field is read. Allocation errors remain caller-owned and
must be freed by the callback. Raw provider error messages are not propagated.

Callbacks must support concurrent any-thread use and never unwind across C.
Release runs once after the final provider/engine reference and active acquisition
are gone. There are no completion tickets, request IDs, native acquisition timers,
late completion, cancellation callbacks or separate acquisition runtime.

The builder setter unconditionally consumes its builder and borrows the provider
handle, retaining its own reference. Replace the builder with the returned handle;
never reuse the input, including on failure. Freeing the caller's provider reference
does not invalidate a builder, engine or admitted request that retains it. Error
allocators copy their borrowed message and return caller-owned error storage.

Provider mode constructs the built-in Azure backend inside FFI, bypassing custom
URL handlers. Without a provider, stock URL-handler selection is unchanged.
Recognized configuration options are forwarded to the native Azure builder, which
gives the custom provider precedence over coexisting static or built-in credentials
during normal credential resolution. `use_emulator=true` selects native emulator
credentials instead and discards the unused custom provider during construction.
`skip_signature=true` omits authentication and does not invoke acquisition.
Attaching a provider enables neither option. Provider mode still rejects non-Azure
backends and REST attachment.
Acquisition errors do not fall back to ambient credentials or replay mutations.
There is no automatic retry or proactive idle acquisition. A subsequent operation
can retry a failed acquisition; failure kinds distinguish transient, permanent and
cancelled results without copying foreign exception text.

The [C consumer](examples/azure-credentials/README.md) demonstrates owned handles,
direct synchronous acquisition and cleanup using the generated header. Its environment-token
step is a static demonstration, not an OAuth refresh implementation. Applications
must supply an identity SDK returning a real token and its real expiry.

### Local Azurite Smoke Test

The ignored `azurite_emulator_roundtrip_bypasses_custom_provider` test exercises
write, overwrite, HEAD, read and delete through one FFI-built engine on both stock
executors. It attaches a failing custom provider and checks that native emulator
authentication bypasses acquisition and releases the unused provider exactly once.

Start an isolated blob service on a free loopback port. Authentication stays enabled;
`--skipApiVersionCheck` only allows the client's storage API version.

```sh
azurite-blob --blobHost 127.0.0.1 --blobPort 11006 --inMemoryPersistence --disableTelemetry --skipApiVersionCheck
```

In a separate PowerShell launch shell, create the private test container using the
public Azurite development-account key, then run each Arrow track from the repository root:

```powershell
$connection = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:11006/devstoreaccount1;'
az storage container create --name delta-kernel-ffi-smoke --connection-string $connection --only-show-errors
$previousEndpoint = $env:AZURITE_BLOB_STORAGE_URL
try {
    $env:AZURITE_BLOB_STORAGE_URL = 'http://127.0.0.1:11006'
    foreach ($arrow in 'arrow-59', 'arrow-60') {
        cargo test --locked -p delta_kernel_ffi --lib --no-default-features --features "default-engine-rustls,$arrow" azurite_emulator_roundtrip -- --ignored
        if ($LASTEXITCODE -ne 0) { throw "Azurite smoke failed for $arrow" }
    }
} finally {
    $env:AZURITE_BLOB_STORAGE_URL = $previousEndpoint
    az storage container delete --name delta-kernel-ffi-smoke --connection-string $connection --only-show-errors
}
```

Stop the owned service afterward. With a different port, change both the connection
string and endpoint override. The test requires an explicit HTTP loopback endpoint,
does not change process-wide Rust environment settings, and deletes its unique blobs.
It is ignored in normal test runs and does not establish real Azure/TLS acceptance.

## Building

### Building Kernel and Headers
You can build static and shared-libraries, as well as the include headers by running:

```sh
cargo build [--release]
```

For additional features like tracing support, use:

```sh
cargo build [--release] --features tracing
```

This will place libraries in the root `target` dir (`../target/[debug,release]` from the directory containing this README), and headers in `../target/ffi-headers`. In that directory there will be a `delta_kernel_ffi.h` file, which is the C header, and a `delta_kernel_ffi.hpp` which is the C++ header.

## Examples

This crate provides two main examples demonstrating different aspects of the FFI:

### 1. Read Table Example (`examples/read-table`)

This example shows how to read data from a Delta table using the FFI. It demonstrates:
- Opening and reading Delta tables
- Schema inspection
- Data retrieval with optional Arrow integration

To build and run this example (after building the ffi as above):

```sh
cd examples/read-table
mkdir build
cd build
cmake ..
make
./read_table ../../../../kernel/tests/data/table-with-dv-small
```

Note there are two configurations that can currently be configured in cmake:
```bash
# turn on VERBOSE mode (default is off) - print more diagnostics
$ cmake -DVERBOSE=yes ..
# turn off PRINT_DATA (default is on) - see below
$ cmake -DPRINT_DATA=no ..
```

By default this has a dependency on
[`arrow-glib`](https://github.com/apache/arrow/blob/main/c_glib/README.md). You can read install
instructions for your platform [here](https://arrow.apache.org/install/).

If you don't want to install `arrow-glib` you can run the above `cmake` command as:

```sh
cmake -DPRINT_DATA=no ..
```

and the example will only print out the schema of the table, not the data.

### 2. Visit Expression Example (`examples/visit-expression`)

This example demonstrates how to work with Delta expressions through the FFI:
- Expression parsing and traversal
- Expression visitor pattern implementation
- Testing expression functionality

To build and run this example:

```sh
cd examples/visit-expression
mkdir build
cd build
cmake ..
make
./visit_expression
```

## Testing

The examples include comprehensive testing capabilities:

### Running Tests

After building an example, you can run the associated tests:

```sh
# For read-table example
cd examples/read-table/build
make test

# For visit-expression example
cd examples/visit-expression/build
make test
```

### Test Scripts

The examples use test scripts located in the `tests/` directory:
- `tests/read-table-testing/run_test.sh` - Tests table reading functionality
- `tests/test-expression-visitor/run_test.sh` - Tests expression visitor functionality

These scripts validate the output against expected results and provide detailed diagnostics.

## C/C++ Extension (VSCode)

By default the VSCode C/C++ Extension does not use any defines flags. You can open `settings.json` and set the following line:
```
    "C_Cpp.default.defines": [
        "DEFINE_DEFAULT_ENGINE_BASE",
        "DEFINE_SYNC_ENGINE"
    ]
```