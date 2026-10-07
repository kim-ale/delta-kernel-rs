<!-- markdownlint-disable-file -->
# Independent native ObjectStore provider

This standalone Rust cdylib uses its own `object_store = "=0.14.2"` and Tokio.
Its only Kernel-related dependency is the frozen, dependency-free
`delta_kernel_native_store_abi` crate. The empty `[workspace]` keeps it outside
the main workspace. No Kernel Rust trait, future, runtime, or allocator crosses
the DLL boundary. The DLL name is `native_object_store_provider.dll` on Windows.

## Public contract

[provider.h](provider.h) includes the generated `delta_kernel_ffi.h`; it does
not redeclare ABI structs. C++ imports the generated types from namespace `ffi`.
Package both headers from the same ABI build. Factory exports return the frozen
native status integers: OK 0, NotFound 1, AlreadyExists 2, Generic 3, NotSupported 4.
No native error text, endpoint diagnostics, or secrets are returned.

* `int32_t prototype_create_memory(KernelNativeObjectStoreDescriptorV1 *out)`
* `int32_t prototype_create_azure(KernelNativeStringSliceV1 endpoint, KernelNativeObjectStoreDescriptorV1 *out)`
* `int32_t prototype_append_commit(void *context)`
* `uint64_t prototype_release_count(void)`
* `uint64_t prototype_credential_requests(void)`
* `uint64_t prototype_credential_generation(void)`
* `uint64_t prototype_callback_count(void)`
* `uint32_t prototype_descriptor_size(void)`

Successful factories populate ABI version 1, the exact native struct size, a
non-null context, and all mandatory `get`, `list`, `put`, `delete_object` and
`release` slots. Factory output is untouched on failure. Ordinary x64 layout
is expected to be 56 bytes; query `prototype_descriptor_size()` and compare
the generated header rather than hard-coding a packing assumption.

## Ownership and threading

The caller owns context until Kernel accepts the descriptor. If adoption fails
or is abandoned, invoke the descriptor's `release(context)` once. After successful
adoption, only Kernel owns final release; do not release through a retained copy.
`prototype_append_commit` accepts a borrowed context alias while a Kernel owner
remains live. Do not race it, or any callback, against final release.

Factories initialize a process-global `OnceLock<Result<Runtime, i32>>` on an
ordinary thread. The runtime owns worker threads and lives for the whole process.
Context release drops only the provider allocation and its `Arc<dyn ObjectStore>`,
never a runtime, and may occur inside a Kernel runtime worker. The context is
immutable; storage implementations and global counters synchronize themselves.

Kernel must invoke synchronous storage callbacks on blocking workers. They use
the provider runtime's `block_on`; do not call them on an asynchronous runtime
worker. Inputs are copied before native operations. Output metadata and bytes
are borrowed only during a synchronous sink call on the callback's thread.
No foreign input, sink, or sink context is retained. Sinks must not unwind.
Unwinding Rust panics in guarded provider operations return Generic; invalid
foreign pointers, process aborts and allocation failure are not recoverable.

> [!WARNING]
> Pin both DLLs for process lifetime. In particular, .NET consumers must not call
> `NativeLibrary.Free` on the provider module. Native workers and callback pointers
> remain module-owned even after all store contexts have been released.

## Native fixtures

The memory factory seeds `table/_delta_log/00000000000000000000.json` with
newline-terminated protocol and metadata actions: an unpartitioned Parquet table
with a nullable `id: long` column. There are no Add actions or data files.
Appending creates version one with a timestamp-only CommitInfo action, using
native atomic `PutMode::Create`; repetition returns AlreadyExists.
Memory storage does not acquire credentials or simulate authentication refresh.

The Azure factory builds ordinary `MicrosoftAzure` with account `account`,
container `container`, the caller's full service endpoint, HTTP allowed, and
zero retries. Use a loopback Azure-protocol fixture, not a live cloud resource.
Its native `CustomCredentialProvider` returns `BearerToken` values of the form
`native-token-{generation}`. Zero-based retrieval index `calls` determines
`generation = calls / 2 + 1`, yielding generations 1, 1, 2, 2, 3 and so on.
This is automatic retrieval-driven synthetic rotation, not OAuth acquisition,
expiry-based refresh, or a claim of real Azure authentication.

Credential retrievals and the maximum issued generation are process-global,
monotonic counters. They are not per-store and factories do not reset them.
Record baselines. A later fixture must inspect actual HTTP Authorization headers
to prove native Azure dispatch changes while retaining the same store and engine;
counter changes alone do not establish that evidence.

GET rejects metadata over 64 MiB before reading its body and bounds accumulated
stream bytes to 64 MiB. It then returns buffered bytes; HEAD uses native
`GetOptions.head` and an empty body, without restricting the object's metadata size.
LIST buffers and sorts the entire native prefix listing for every page,
then emits ascending entries strictly after the offset, at most 1..1024 items,
with `has_more` equal to 0 or 1. This repeated full-list buffering is prototype-only.
PUT supports native atomic Create/Overwrite with default tags and attributes.
DELETE delegates to `ObjectStoreExt::delete`; Azure may issue a batch POST.
Unknown GET/PUT flags return NotSupported; any nonzero sink status becomes Generic.
Streaming, multipart, large-table performance, cancellation, unloading and a
complete production ObjectStore contract are outside this prototype.

## Validation handoff

Unit tests cover native callback boundaries, owned sink copies, pages, atomic
writes, invalid inputs, errors, final release, bounded body collection and direct
credential rotation. Thirteen provider tests pass locally. The parent Kernel
adapter passes 80 focused cases under Miri on each Arrow track, including actual
in-flight read cancellation. All 540 FFI unit cases pass on each track. Generated C11/C++17 callers
compile and confirm descriptor/string/metadata layouts.

The paired package-only net8 consumer loaded both independent DLLs, loaded native
memory version zero, appended version one and observed it using the same engine.
Context cleanup, failed adoption/builds, repeated disposal and isolated sessions
passed. Its Azure loopback fixture observed six native credential retrievals and
three distinct Authorization generations across repeated snapshots of one engine.
No managed storage or authentication callbacks supplied those values.

These are local fixture results, not live cloud credentials, expiry/OAuth refresh,
full scans/checkpoints, general plugin unloading or production Table integration.
The paired prototype is in the delta-dotnet `kim_ale/native-object-store-prototype`
branch, under `examples/native-object-store-prototype`.