---
title: Independent native ObjectStore provider
description: Experimental v3 C ABI forwarding to an independently owned native ObjectStore
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
`KernelNativeObjectStoreDescriptorV3`, ABI version 3, with `context`, `get`,
`list_open`, `list_next`, `list_close`, `put`, `delete_object`, and `release` slots.
All callbacks and the provider context are non-null. Ordinary x64 layout is
**72 bytes, not 80**;
query `prototype_descriptor_size()` rather than assuming packing. Shared string,
byte-slice, and metadata types retain their V1 names. Failed factories leave
output untouched.

Common status codes are OK 0, NotFound 1, AlreadyExists 2, Generic 3, and
NotSupported 4. No error
text or secrets cross the ABI. Any nonzero sink result becomes Generic.

GET maps the 24-byte `KernelNativeGetOptionsV3` directly to native `GetOptions`
and calls `ObjectStore::get_opts`. `head` is 0 for GET or 1 for HEAD;
`range_kind` is 0 for full, 1 for bounded `[start,end)`, 2 for offset `start`,
or 3 for suffix length `end`. The sink receives full native metadata, the native
returned range, and only that range's bytes. The provider does not slice a full
object to implement ranges. HEAD returns metadata with an empty body and never
polls the payload. Before polling a GET payload, its returned range length is
limited to 64 MiB; actual streamed bytes are independently bounded to 64 MiB.
Full metadata size can exceed 64 MiB for a small range or HEAD. All input paths,
prefixes, offsets and factory endpoints, and output paths, are bounded to 64 KiB.
PUT checks the body bound before copying borrowed bytes.

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

PUT maps Create and Overwrite to native `PutMode` and delegates to `put_opts`.
Atomic create conflicts return AlreadyExists 2. DELETE delegates to native
`ObjectStoreExt::delete`, including the backend's own delete-stream behavior.

Factory, append, and statistics export names are unchanged. Callback counts sum
GET, LIST advance, PUT and DELETE attempts, including failures. LIST open/close,
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
Sink functions must be non-null; GET and LIST advance reject null sink contexts.

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

This is not full ObjectStore parity. Conditional/versioned GET, request
extensions, PUT tags/attributes/conditional Update, multipart upload, delimiter
listing, and copy are not represented by this descriptor. GET output is buffered
within the returned-byte bound; there is no streaming GET sink or native-call
cancellation. Live credentials, OAuth/expiry refresh, unloading, and production
Table integration remain outside this independent fixture. Managed write tests
can exercise existing public Kernel transactions through an HTTP fixture; no
additional write-probe export is provided.