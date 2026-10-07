<!-- markdownlint-disable-file -->
# Experimental Native ObjectStore ABI

The dependency-free crate defines a versioned C descriptor for an independently
constructed native store. Both the Kernel adapter and a separate provider compile
these plain data/function declarations. No Kernel Rust trait object, runtime,
future, allocation or dependency identity is shared between libraries.

Kernel's generated `delta_kernel_ffi.h` is the C/C++ artifact. Use the header from
the matching Kernel build instead of manually redeclaring callback layouts.
The version-3 descriptor is 72 bytes on x64: context, GET, LIST open/next/close,
PUT, DELETE and release. GET options are 24 bytes; UTF-8/byte slices remain
16 bytes and metadata 32 bytes. Versions 1 and 2 are rejected, not reinterpreted.

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

## Capabilities

GET forwards HEAD and full/bounded/offset/suffix ranges to native `get_opts`.
The provider returns its actual metadata, range and bytes. There is no full-object
range emulation. HEAD never polls the body. Returned payloads and PUT bodies are
bounded at 64 MiB; full-object metadata may exceed this bound for HEAD/small ranges.
Paths are bounded at 64 KiB. PUT forwards atomic Create/Overwrite and DELETE delegates
to the native store. Create conflicts map to AlreadyExists, not generic success.

LIST forwards prefix/offset into one retained native stream. Next polls at most
128 records, without a total listing cap, full collection, sorting or restart.
Ordering remains native-store behavior; Kernel's existing engine owns any required
sorting. An exactly full final batch may need one empty advance to report EOF.
Failed batches are discarded and the cursor is closed.

Not full ObjectStore parity: conditional/version reads, Update PUT, tags/attributes,
extensions, multipart, copy and delimiter listing are not represented. Multi-range
reads use the dependency's default coalescing implementation and DELETE streams
are decomposed into single native calls. Forwarding native batch methods would
preserve provider optimizations but needs additional ABI representations.

Authentication acquisition/caching/refresh is private provider behavior. This
descriptor does not transmit credentials or require managed I/O/auth callbacks.
The example provider proves synthetic native credential rotation through real
Azure-protocol loopback requests, not live OAuth, token expiry or service acceptance.

The package consumer proves snapshots and an actual public Kernel no-Add transaction
persisted through native Azure atomic PUT, not provider-only fixture append. This
does not establish data-file writes or full checkpoint support; those can use
multipart, currently unsupported. Streaming GET, module unloading, cross-platform
packages and production Table integration remain outside this experiment.