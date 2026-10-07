<!-- markdownlint-disable-file -->
# Experimental Native ObjectStore ABI

The dependency-free crate defines a versioned C descriptor for an independently
constructed native store. Both the Kernel adapter and a separate provider compile
these plain data/function declarations. No Kernel Rust trait object, runtime,
future, allocation or dependency identity is shared between libraries.

Kernel's generated `delta_kernel_ffi.h` is the C/C++ artifact. Use the header from
the matching Kernel build instead of manually redeclaring callback layouts.
On the validated x64 targets the descriptor is 56 bytes, UTF-8/byte slices are
16 bytes and metadata is 32 bytes. Version 1 requires exact size and all callbacks.

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

## Capabilities

The prototype supports full GET/HEAD, buffered local range slicing, ordered
exclusive-offset listing pages, atomic Create/Overwrite PUT and individual DELETE.
It rejects conditional/versioned reads, write tags/attributes/extensions, copy,
delimiter listing and multipart. Kernel limits paths to 64 KiB, bodies to 64 MiB
and pages to 128 records; providers must also enforce their allocation bounds.

Authentication acquisition/caching/refresh is private provider behavior. This
descriptor does not transmit credentials or require managed I/O/auth callbacks.
The example provider proves synthetic native credential rotation through real
Azure-protocol loopback requests, not live OAuth, token expiry or service acceptance.

The ABI is experimental, not a production-stable extension contract. Streaming,
capability negotiation, cancellation/timeouts, unbounded listing performance,
cross-platform packages and production managed Table integration need follow-up
design before promoting it.