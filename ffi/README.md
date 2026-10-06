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

Every credential lookup starts an independent caller request. The FFI bridge does
not cache tokens or combine concurrent requests. The caller's identity SDK owns
caching, acquisition deduplication and refresh policy. A cached caller result may
complete synchronously; a cache miss queues asynchronous work. No lookup occurs
solely because an engine is idle.

`CAzureCredentialProviderConfig` version 1 requires its exact generated structure
size, Start and Release callbacks, and explicit limits. Acquisition timeout is
1-120000 milliseconds, minimum remaining token lifetime 1-3600000 milliseconds,
token size 1-65536 bytes, and outstanding tickets 1-1024. Timed-out foreign tickets
retain capacity until completed, failed or freed.

Start receives a numeric request ID and one exclusive request ticket. Queue foreign
acquisition and return promptly. Complete, fail or free that ticket exactly once;
those calls consume it even on error. Completion copies borrowed token bytes during
the call and requires the actual absolute UTC expiry in Unix milliseconds. Never
derive expiry from token text. A false result means retired, not reusable.

Cancel is optional and cooperative. It receives the request ID after Start returns
and never owns the ticket. Release runs once after all provider, builder, store,
request and active callback ownership ends. All callbacks must be nonblocking,
nonthrowing, any-thread-safe and safe for concurrent calls. Native timeout cannot
reclaim a ticket that foreign code still owns or forcibly terminate application work.

The builder setter unconditionally consumes its builder and borrows the provider
handle, retaining its own reference. Replace the builder with the returned handle;
never reuse the input, including on failure. Freeing the caller's provider reference
does not invalidate a builder, engine or admitted request that retains it. Error
allocators copy their borrowed message and return caller-owned error storage.

Provider mode constructs the built-in Azure backend inside FFI, bypassing custom
URL handlers. Without a provider, stock URL-handler selection is unchanged.
Provider mode rejects non-Azure backends, REST attachment, competing authentication
options and enabled emulator/unsigned-request modes.
Acquisition errors do not fall back to ambient credentials or replay mutations.
There is no automatic retry or proactive idle acquisition. A subsequent operation
can retry a failed acquisition; failure kinds distinguish transient, permanent and
cancelled results without copying foreign exception text.

The [C consumer](examples/azure-credentials/README.md) demonstrates owned handles,
queued completion and cleanup using the generated header. Its environment-token
step is a static demonstration, not an OAuth refresh implementation. Applications
must supply an identity SDK returning a real token and its real expiry.

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