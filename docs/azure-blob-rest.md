---
title: Literal Azure Blob REST Store
description: Container-scoped Blob HTTP with ready dynamic headers for Kernel reads and writes.
ms.date: 2026-10-07
---

## Scope

`AzureBlobRestStore` implements literal Azure Blob HTTP through `reqwest::Client`.
It does not delegate to `MicrosoftAzure`, reinterpret the JSON REST configuration,
or acquire credentials. The existing JSON `RestObjectStore` remains a separate
backend with its existing contract.

The transport URL identifies exactly one container, for example
`https://account.blob.core.windows.net/container/`. An optional SAS query is
preserved on every object, listing, block, and publication request. Userinfo,
fragments, nested container paths, duplicate query keys, and non-SAS query keys
are rejected without echoing their values.

Snapshot URLs remain logical `az://container/table/`. Object paths are relative
to the container, so `table/_delta_log/...` is appended exactly once. For a
Blob-compatible OneLake endpoint, the container segment can be the workspace ID;
the object path contains the lakehouse/table hierarchy. Direct DFS APIs are not
implemented by this dialect.

## Rust Setup

```rust,no_run
use std::sync::Arc;
use delta_kernel_default_engine::rest_store::{
    AzureBlobRestStore, AuthHeaderProvider,
};
use reqwest::Client;
use url::Url;

fn blob_store(
    container_url: Url,
    ready_headers: Arc<dyn AuthHeaderProvider>,
) -> delta_kernel::object_store::Result<Arc<AzureBlobRestStore>> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| delta_kernel::object_store::Error::Generic {
            store: "client",
            source: error.into(),
        })?;
    Ok(Arc::new(AzureBlobRestStore::new(
        container_url, client, ready_headers,
    )?))
}
```

The client is reused. A caller-supplied client must not inject default auth or
protocol headers and should disable redirects. The Rust factory
`build_azure_blob_rest_store` reuses the existing REST TLS configuration with
redirects disabled. Configure `TokioMultiThreadExecutor` when constructing a
`DefaultEngineBuilder` for checkpoint writes.

## Authentication and Options

The existing `AuthHeaderProvider` supplies ready `Authorization: Bearer ...`
and custom Fabric/context headers. Alternatively, the container URL must contain
a nonempty SAS signature. Missing auth, non-Bearer auth, and provider failures
fail before dispatch. There is no Shared Key signer, native Azure credential
discovery, OAuth acquisition, 401 refresh policy, or global credential cache.

`RefreshingHeaderProvider` retains its existing TTL behavior. Headers are
requested again for every HTTP dispatch and retry. Context headers may include
`x-ms-fabric-*`; transport headers, conditional headers, and Blob protocol
headers cannot override operation semantics. The store sets `x-ms-version`
to `2021-12-02` and generates `x-ms-date` per dispatch. Store formatting and
protocol error messages omit transport URLs, SAS values, and provider errors.

The Rust factory and FFI builder accept these existing REST option keys:

* `header.<Name>` for static ready headers when no callback/provider is supplied
* `tls.cert_path`, `tls.key_path`, and `tls.ca_path`, set together for mTLS
* `tls.dns_override` and `tls.timeout_secs`
* `retry.max_retries`, default `0`
* `put.verify_on_ambiguous=false`, or omitted

Unknown options and native Azure credential keys are rejected. Verification of
ambiguous Create outcomes is not implemented, so `put.verify_on_ambiguous=true`
returns an explicit unsupported error rather than being ignored.

## FFI Setup

Call `get_engine_builder` with the HTTP(S) container URL, add static/TLS/retry
options, and select `builder_with_azure_blob_rest_store`. It accepts the existing
nullable `CAuthHeaderCallback` and opaque context; no `CRestEndpointConfig` is
passed. Configure `builder_with_multithreaded_executor` for checkpoint writes,
then call `builder_build`. Snapshot builders receive the logical `az://` URL.

The Blob setter consumes the input builder unconditionally. Retain only the
returned handle, then build or free it. Callback context must remain valid for
the engine lifetime and support concurrent calls. Callback string ownership and
TTL follow the existing REST contract. No HTTP behavior or multipart handles
are added to the FFI layer.

## Protocol Behavior

* GET/HEAD send native HTTP preconditions and optional `versionid`. Range reads
  require HTTP 206, an exact valid Content-Range, and matching payload length.
  Suffix ranges resolve size with HEAD, then issue a bounded GET guarded by ETag
  or version. Empty suffixes and unguarded resolution fail; the store never
  downloads and slices the full object.
* HEAD requires Content-Length. A present invalid Last-Modified fails rather
  than becoming epoch. ETags and `x-ms-version-id` are preserved.
* Listings issue container-root `restype=container&comp=list` requests and parse
  typed XML. Every NextMarker is followed, including short/empty pages. Cycles
  and globally decreasing object paths fail. Directory entries are filtered.
  Offsets are exclusive and recursive. HNS ordering that differs from lexical
  path ordering is rejected, not silently reordered or accepted.
* Full PUT uses BlockBlob. Create sends `If-None-Match: *`; BlobAlreadyExists and
  ConditionNotMet map to AlreadyExists only for Create. Other 412 responses map
  to Precondition. Create is not retried, even when retries are configured.
* DELETE sends individual Blob requests, each with current headers. GET, HEAD,
  list, DELETE, overwrite PUT, block staging, and block-list publication can
  retry transient server/connect/timeout failures when configured.
* Multipart assigns unique equal-length block IDs at `put_part` invocation,
  stages each owned part, then publishes ordered Latest entries through typed
  XML. Pending, failed, aborted, closed, or dropped uploads cannot publish.
  Abort/drop never delete an existing object. Uncommitted blocks remain until
  the service expires them; no cleanup promise is made.

## Restrictions and Validation

Full PUT and each multipart part are limited to 64 MiB; multipart total size is
not buffered or capped at 64 MiB. At most 50,000 parts may be registered. Full
GET and XML listing pages are buffered in memory. Copy, delimiter listing,
Update, encoded XML names, write tags/attributes/extensions, and GET extensions
return explicit unsupported errors. Read attributes are currently empty.

Local stateful HTTP tests exercise atomic concurrent Create, ready changing
Bearer/context headers, strict ranges, metadata errors, pagination, retries,
block ordering/failure/abort, real Kernel commits, and checkpoint-only reload.
The large checkpoint contains deterministic high-entropy metadata and measures
25,179,185 serialized bytes, with three block uploads and one block-list commit.
The small checkpoint uses full PUT. JSON REST regression tests remain separate.

No live Azure or OneLake request has been made. Exact endpoint/API-version
acceptance and the private Fabric actor-header contract remain unverified.
Approach A delegates to the native Azure store and has different signing and
transport constraints; this separate Approach B owns the literal protocol and
its maintenance cost. Neither local mock result is cloud-service acceptance.