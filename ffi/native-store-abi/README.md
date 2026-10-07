<!-- markdownlint-disable-file -->
# Experimental Native ObjectStore ABI

The dependency-free crate defines a versioned C descriptor for an independently
constructed native store. Both the Kernel adapter and a separate provider compile
these plain data/function declarations. No Kernel Rust trait object, runtime,
future, allocation or dependency identity is shared between libraries.

Kernel's generated `delta_kernel_ffi.h` is the C/C++ artifact. Use the header from
the matching Kernel build instead of manually redeclaring callback layouts.
The version-4 descriptor is 160 bytes on x64: context and 18 native operation
slots. UTF-8/byte slices are 16 bytes; enriched metadata is 64 bytes and includes
optional ETag/version. Older descriptor versions are rejected, not reinterpreted.
Some base types retain V1/V3 names; that does not enable legacy descriptor adoption.

## Ownership

Factories initially give the caller ownership of context. Rejected adoption
leaves that ownership with the caller; successful adoption copies the descriptor
and transfers release responsibility to Kernel. A shared store handle, builder,
engine, stream or in-flight blocking call can retain context. Only the provider's
release function destroys it, once after the final reference. Kernel never frees
a provider allocation directly.

Inputs are borrowed through callback return. Output metadata and bytes are borrowed
through sink return and copied by Kernel. A provider cannot retain inputs, sink
functions or sink contexts. One callback's sinks execute serially; operations may
overlap across threads. Callbacks cannot unwind. Modules remain loaded for process
lifetime in this prototype; hot unloading is not supported.

Each successful listing open transfers one opaque provider cursor. The adapter
retains the context and exclusively moves the cursor into each blocking advance.
Close runs once on exhaustion, failure or abandonment, before context release.
Cancellation does not stop a native call: cursor cleanup waits for the worker to
finish, then closes it. The provider alone destroys its cursor allocation.

Multipart open owns a native upload. Part open invokes native `put_part` at
invocation time and returns an owned future handle; wait polls it once and close
drops it. Parts retain the upload/context, including when a blocking wait outlives
task cancellation. Complete and abort exclusively borrow the native upload.
Upload close drops it without promising implicit abort; callers use explicit abort
when needed, following native backend cleanup behavior. Foreign allocations are
never freed by Kernel directly.

## Capabilities

GET forwards HEAD, full/bounded/offset/suffix ranges, version, ETag and timestamp
conditions to native `get_opts`. Timestamp conditions preserve nanoseconds.
The provider returns its actual metadata, range and bytes. There is no full-object
range emulation. HEAD never polls the body. Returned payloads and PUT bodies are
bounded at 64 MiB; full-object metadata may exceed this bound for HEAD/small ranges.
Paths and individual UTF-8 fields are bounded at 64 KiB. Native `get_ranges`
receives one batch with unchanged range order and duplicates. Empty subranges
are delegated to native policy, not rejected or emulated locally.

PUT forwards Create/Overwrite/Update, tags and typed attributes to native `put_opts`.
GET returns attributes, ETags and versions; PUT and multipart completion return
native ETags/versions. Optional strings distinguish None from a present empty
string. Collections have at most 4096 entries and aggregate request/result data
is bounded to 64 MiB. Native precondition/not-modified/not-implemented errors have
distinct status values; no secret native error text crosses the ABI.

LIST forwards prefix/offset into one retained native stream. Next polls at most
128 records, without a total listing cap, full collection, sorting or restart.
Ordering remains native-store behavior; Kernel's existing engine owns any required
sorting. An exactly full final batch may need one empty advance to report EOF.
Failed batches are discarded and the cursor is closed.

DELETE forwards bounded batches to native `delete_stream`, preserving native
per-path and aggregate errors. Each batch accepts at most 128 path results plus
128 pathless aggregate errors. A failed outer callback discards that batch's
captured results, but does not roll back native storage effects.

Copy/rename forward target modes to native methods; delimiter listing returns
native objects and common prefixes. Multipart forwards open, part construction,
part wait, completion, abort and close. Native backends remain free to reject
unsupported operations. There are no adapter-side batch coalescing, copy/rename
fallbacks or storage/authentication policies.

Arbitrary request `Extensions` cannot cross the independent-library boundary and
are explicitly rejected. Response extensions are not exposed. GET results remain
buffered; native multi-range and delimiter methods may allocate before the provider
can inspect their returned size. This is bounded typed-operation forwarding, not
an unbounded or production-stable ObjectStore ABI.

Authentication acquisition/caching/refresh is private provider behavior. This
descriptor does not transmit credentials or require managed I/O/auth callbacks.
The example provider proves synthetic native credential rotation through real
Azure-protocol loopback requests, not live OAuth, token expiry or service acceptance.

Windows cross-DLL tests exercise native batches, copy/rename, delimiter listing,
multipart and rich metadata through the adapter. The regular package consumer
proves a public Kernel no-Add commit and a small memory checkpoint/reload. That
checkpoint uses ordinary PUT, not multipart; separate cross-DLL tests cover multipart.
Streaming GET, native cancellation, module unloading, cross-platform packages,
live authentication and production Table integration remain outside this experiment.