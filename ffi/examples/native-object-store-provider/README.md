---
title: Independent native ObjectStore provider
description: Experimental v4 C ABI forwarding to an independently owned native ObjectStore
---
<!-- markdownlint-disable-file -->

## Overview

This forwarding experiment is a standalone Rust cdylib with its own
`object_store = "=0.14.2"` and Tokio runtime. Its only Kernel dependency is the plain,
dependency-free `delta_kernel_native_store_abi` crate; no Kernel Rust trait or
runtime is used. The empty `[workspace]` keeps it outside the main workspace.
The Windows DLL is `native_object_store_provider.dll`.

## Contract

[provider.h](provider.h) includes the generated `delta_kernel_ffi.h`; it does
not redeclare ABI types. C++ imports the generated types from namespace `ffi`.
Package both headers from the same ABI build. Factories return
`KernelNativeObjectStoreDescriptorV4`, ABI version 4, with a context and 18
mandatory callbacks: GET, batched ranges, cursor open/next/close, delimiter
listing, PUT, batched deletion, copy, rename, multipart open, part open/wait/close,
complete, abort, upload close, and release. Measured x64 layout is **160 bytes**;
query `prototype_descriptor_size()` rather than assuming packing. Shared string,
byte-slice, and base metadata types retain their V1 names; metadata and PUT results
use V4 wrappers for optional ETag/version. Failed factories leave output untouched.

Common status codes are OK 0, NotFound 1, AlreadyExists 2, Generic 3,
NotSupported 4, Precondition 5, NotModified 6, and NotImplemented 7.
Native typed errors determine these statuses; no error text or secrets cross
the ABI. Any nonzero sink result becomes Generic. Discard all borrowed output
from a failed callback, including partial batch output. This does not roll back
native storage side effects. Per-item DELETE errors are distinct from outer
callback failures.

GET maps `KernelNativeGetOptionsV4` to native `GetOptions` and calls
`ObjectStore::get_opts`. Its 24-byte V3 base has `head` 0 for GET or 1 for HEAD;
`range_kind` is 0 for full, 1 for bounded `[start,end)`, 2 for offset `start`,
or 3 for suffix length `end`. The sink receives full native metadata, the native
returned range, and only that range's bytes. The provider does not slice a full
object to implement ranges. Optional If-Match, If-None-Match, and version are
forwarded unchanged; timestamp conditions preserve validated Unix seconds and
nanoseconds through `chrono::DateTime`. Native backends decide conditional/version
semantics. HEAD returns metadata with an empty body and never polls the payload.
Before polling a GET payload, its returned range length is limited by the 64 MiB
aggregate output budget; actual streamed bytes are independently bounded.
Full metadata size can exceed 64 MiB for a small range or HEAD. All input paths,
prefixes, offsets and factory endpoints, and output paths, are bounded to 64 KiB.
PUT checks the body and aggregate request bounds before copying borrowed bytes.
All optional strings distinguish null-empty (`None`) from nonnull-empty (`Some("")`).
GET returns native ETag/version, actual range, and typed attributes; metadata
modification times use Unix milliseconds, while condition timestamps retain
nanosecond precision.

Batched ranges copy an aligned array of at most 4096 bounded ranges, call native
`get_ranges` once with unchanged order and duplicates, and emit one indexed body
per input. The provider does not coalesce ranges or loop over native GET calls.
Native result count and each returned length are checked before any sink call;
total returned bytes are at most 64 MiB. The backend may allocate its results
before these output checks run.

LIST open forwards prefix and exclusive `start_after` to native `list_with_offset`,
or `list` when the offset is empty. It writes the cursor only on success and does
not poll the stream. A provider-owned cursor retains its own store `Arc` and one
owned `BoxStream<'static, Result<ObjectMeta>>`. Each exclusively borrowed advance
uses `runtime.block_on` to poll sequentially and emit at most 128 records.
There is no full-list collection, sorting, page restart, or total listing limit.
Ordering is the native ObjectStore's ordering. `has_more` becomes zero only on
native EOF; a full final batch can return one followed by an empty advance with
zero. Failure can follow partial sink output; the caller must discard the failed
advance and close the cursor. Close also handles early termination and drops the
stream and provider allocation without dropping the global runtime.

PUT maps Overwrite, Create, and conditional Update with both optional ETag/version
to native `PutMode` and delegates to `put_opts`. Its single sink returns the native
`PutResult`, including optional ETag/version. Tags use `TagSet::push`; attributes
use `Attributes::insert`, with content-disposition, content-encoding,
content-language, content-type, cache-control, storage-class, and `metadata:<key>`
names. Each tag/attribute array is bounded to 4096 pairs, each UTF-8 string to
64 KiB, and each request/output to 64 MiB in aggregate. Duplicate attribute keys
are rejected, not silently overwritten.

DELETE copies up to 128 paths, constructs one owned input stream, and calls native
`delete_stream` once. It emits native successes and typed per-item errors in
native order, continuing after errors. Errors with a native path retain that path;
pathless aggregate errors use a null-empty path. Aggregate errors may yield fewer
results than input paths or appear between per-path results. Each batch bounds
path results to its input count and pathless aggregate errors to 128 independently.
The provider does not require equal input/output counts. Empty subranges are
forwarded to native batch-range policy; backends may accept or reject them.

COPY and RENAME call native `copy_opts` and `rename_opts`, respectively, with
Overwrite/Create target modes. The provider does not implement rename as copy/delete.
Delimiter listing calls native `list_with_delimiter` once and returns its objects
and common prefixes through one sink, without rebuilding or sorting a listing.
Each result array is bounded to 4096 entries; overflow fails instead of truncating.

Multipart open delegates tags/attributes to native `put_multipart_opts`; only
Overwrite with no Update strings is accepted. An upload owns an
`Arc<Mutex<UploadState>>` containing the native `Box<dyn MultipartUpload>` and a
retained store. Part open copies its at-most-64-MiB payload and immediately invokes
native `put_part` under the provider runtime's enter guard. It does not poll the
future or use `block_on` to construct it. Each part retains the parent and owns
one optional native future. Wait takes that future and uses `runtime.block_on`
once without holding the upload mutex; different part futures may poll concurrently.
Complete/abort serialize native upload calls through the upload mutex. The caller
must finish parts first, following the native trait contract. Complete returns
native ETag/version through one PUT-result sink. Part close drops its future;
upload close drops the upload without inventing auto-abort policy. Explicit abort
delegates backend cleanup, including backends that cannot clean up on drop.

Factory, append, and statistics export names are unchanged. Callback counts sum
GET/ranges, LIST advance/delimiter, PUT/copy/rename, multipart open/part-open/complete,
and DELETE-batch attempts, including failures. Waits, aborts, closes, LIST open,
factories, sinks, append, and release are excluded. Release and credential
counters are process-global and never reset.

## Ownership and threading

The caller owns context until Kernel accepts the descriptor. Release an
unadopted descriptor exactly once; after adoption, only Kernel releases it.
Keep an owner alive during every callback or borrowed append operation and never
race final release. Inputs are validated and copied during the callback; outputs
are borrowed only through synchronous sink return. No foreign pointer or sink is
retained. Sinks must not unwind. Guarded Rust panics become Generic; invalid
foreign pointers, aborts, and allocation failure are not recoverable.

Each successful LIST open transfers one cursor to the caller. Keep the context
owner alive until that cursor is closed, including after failed advances.
Advances exclusively borrow the cursor; never overlap advance and close.
Close consumes it exactly once on any thread, including inside another runtime.
Contexts are immutable and their native stores support concurrent operations.
Sink functions and sink contexts must be non-null. Inputs and output storage must
be correctly aligned where their types require alignment. The provider validates
null/length/alignment combinations; the caller still guarantees valid initialized
foreign allocations and live handles. Parts, cursors, and uploads are closed
exactly once, and wait/next/close must never race on the same handle.

Initialize factories on ordinary threads and invoke storage callbacks on blocking
workers, not asynchronous runtime workers. The provider's process-global
`OnceLock<Result<Runtime, i32>>` lives for the whole process. Release drops only
the context and its `Arc<dyn ObjectStore>`, never the runtime, and may run inside
another runtime. Native stores and counters synchronize concurrent access.

> [!WARNING]
> Keep both DLLs loaded for process lifetime. Do not call `NativeLibrary.Free` on
> the provider: its workers and callback pointers remain module-owned after release.

## Native fixtures

Memory storage seeds version zero at `table/`: an empty, unpartitioned Parquet
table with nullable `id: long`. `prototype_append_commit` creates version one
once using native atomic `PutMode::Create`, returning the shared AlreadyExists
status 2 on duplication and NotSupported for Azure. This helper is not an injected write
API. Memory storage does not acquire credentials.

Azure storage uses ordinary `MicrosoftAzure`, account `account`, container
`container`, the supplied full endpoint, HTTP allowed, and zero retries. Use a
real Azure-client HTTP loopback fixture, not a live cloud resource. Native
`CustomCredentialProvider` supplies synthetic `native-token-{generation}` bearer
tokens with `generation = calls / 2 + 1` for zero-based retrieval index `calls`.
Compare actual Authorization headers on repeated requests from one store, with
counter baselines. Counters alone do not prove rotation on the wire.

Request extensions have no ABI representation and are rejected by the Kernel
adapter, not replaced with provider policy. Response extensions do not cross the
boundary. This is bounded native-operation parity, not the full unbounded
ObjectStore API. GET output is buffered; there is no streaming GET sink or
native-call cancellation. Live credentials, OAuth/expiry refresh, unloading, and
production Table integration remain outside this fixture. Public Kernel checkpoint
acceptance is run by the parent against both DLLs; provider tests use no Kernel
Rust implementation or extra DLL-loader dependency.

## Local validation

The standalone provider requires `chrono = "0.4"` directly for precise condition
timestamps; it remains independent of the root manifest. Run tests, strict clippy,
and the DLL build serially against its own manifest and lockfile:

```powershell
cargo test --manifest-path ffi/examples/native-object-store-provider/Cargo.toml --target-dir target-provider --offline --locked -j2 --quiet
cargo clippy --manifest-path ffi/examples/native-object-store-provider/Cargo.toml --target-dir target-provider --offline --locked -j2 --all-targets -- -D warnings
cargo build --manifest-path ffi/examples/native-object-store-provider/Cargo.toml --target-dir target-provider --offline --locked -j2 --quiet
```

Tests retain factory/bounds/cursor/seed/append/credential coverage and use true
native-method spies for batches, copy/rename, delimiter, and multipart options.
Memory tests verify native effects and metadata, while multipart probes verify
invocation-before-polling order, reverse and concurrent waits, explicit abort,
NotImplemented propagation, part failures, and retained parent lifetimes.