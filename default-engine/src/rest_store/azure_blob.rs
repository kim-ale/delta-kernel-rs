//! Literal Azure Blob HTTP protocol, independent of the JSON file API and native Azure store.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{
    self, Attributes, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};
use futures::stream::BoxStream;
use futures::StreamExt;
use reqwest::header::HeaderMap;
use reqwest::{Client, Method, Response, StatusCode};
use url::Url;

use super::{AuthHeaderProvider, RestClientOptions, StaticHeaderProvider};

mod multipart;
#[cfg(test)]
mod tests;
mod xml;

/// A container-scoped, literal Blob REST store using ready Bearer headers or a SAS URL.
///
/// Object paths are container-relative. Use `az://container/table/` for Kernel snapshots,
/// not the transport URL. This store performs no OAuth acquisition or Shared Key signing.
#[derive(Clone)]
pub struct AzureBlobRestStore {
    container_url: Url,
    client: Client,
    auth: Arc<dyn AuthHeaderProvider>,
    max_retries: u32,
    has_sas: bool,
}

impl std::fmt::Debug for AzureBlobRestStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AzureBlobRestStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for AzureBlobRestStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AzureBlobRestStore")
    }
}

impl AzureBlobRestStore {
    /// Construct a container-scoped store. HTTP is accepted for local testing; use HTTPS
    /// in production. Only SAS query keys are accepted, and all are retained on each request.
    /// The client must not supply default protocol/auth headers; disable redirects to avoid
    /// forwarding custom context headers. Returns an error for an invalid container URL.
    pub fn new(
        container_url: Url,
        client: Client,
        auth: Arc<dyn AuthHeaderProvider>,
    ) -> Result<Self> {
        let (container_url, has_sas) = validate_container_url(container_url)?;
        Ok(Self {
            container_url,
            client,
            auth,
            max_retries: 0,
            has_sas,
        })
    }

    /// Retry transient idempotent requests, obtaining fresh provider headers per attempt.
    /// Conditional Create is never retried, because its outcome may be ambiguous.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    fn object_url(&self, location: &Path) -> Result<Url> {
        let mut url = self.container_url.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| error("invalid container URL"))?;
        for part in location.parts() {
            segments.push(part.as_ref());
        }
        drop(segments);
        Ok(url)
    }

    fn headers(&self) -> Result<HeaderMap> {
        let mut headers = self
            .auth
            .headers()
            .map_err(|_| error("header provider failed"))?;
        for name in headers.keys() {
            if matches!(
                name.as_str(),
                "host"
                    | "content-length"
                    | "connection"
                    | "transfer-encoding"
                    | "te"
                    | "trailer"
                    | "upgrade"
                    | "keep-alive"
                    | "proxy-authorization"
                    | "proxy-authenticate"
                    | "range"
                    | "if-match"
                    | "if-none-match"
                    | "if-modified-since"
                    | "if-unmodified-since"
                    | "content-type"
                    | "content-md5"
                    | "date"
                    | "if-range"
                    | "content-encoding"
                    | "content-language"
                    | "content-disposition"
                    | "cache-control"
            ) || matches!(
                name.as_str(),
                "x-ms-version"
                    | "x-ms-date"
                    | "x-ms-range"
                    | "x-ms-range-get-content-md5"
                    | "x-ms-range-get-content-crc64"
                    | "x-ms-tags"
                    | "x-ms-lease-id"
                    | "x-ms-delete-snapshots"
                    | "x-ms-access-tier"
                    | "x-ms-rehydrate-priority"
            ) || [
                "x-ms-meta-",
                "x-ms-blob-",
                "x-ms-copy-",
                "x-ms-source-",
                "x-ms-if-",
                "x-ms-encryption",
                "x-ms-expiry-",
            ]
            .iter()
            .any(|prefix| name.as_str().starts_with(prefix))
            {
                return Err(error("header provider supplied a reserved protocol header"));
            }
        }
        if let Some(value) = headers.get("authorization") {
            let bearer = value
                .to_str()
                .ok()
                .and_then(|value| value.strip_prefix("Bearer "));
            if bearer.is_none_or(|token| token.is_empty() || token.chars().any(char::is_whitespace))
                || headers.get_all("authorization").iter().count() != 1
            {
                return Err(unsupported(
                    "only ready Bearer Authorization is supported; no Shared Key signing",
                ));
            }
        } else if !self.has_sas {
            return Err(error(
                "Blob requests require Bearer Authorization or a SAS signature",
            ));
        }
        headers.insert(
            "x-ms-version",
            reqwest::header::HeaderValue::from_static("2021-12-02"),
        );
        insert_header(
            &mut headers,
            "x-ms-date",
            &chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        )?;
        Ok(headers)
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        protocol: HeaderMap,
        body: Option<bytes::Bytes>,
        retry: bool,
    ) -> Result<Response> {
        let mut attempts = 0;
        loop {
            let mut headers = self.headers()?;
            headers.extend(protocol.clone());
            let mut request = self
                .client
                .request(method.clone(), url.clone())
                .headers(headers);
            if let Some(body) = &body {
                request = request.body(body.clone());
            }
            match request.send().await {
                Ok(response)
                    if retry
                        && attempts < self.max_retries
                        && response.status().is_server_error() => {}
                Ok(response) => return Ok(response),
                Err(failure)
                    if retry
                        && attempts < self.max_retries
                        && (failure.is_timeout() || failure.is_connect()) => {}
                Err(_) => {
                    return Err(error(
                        "Blob HTTP transport failed (URL and credentials redacted)",
                    ))
                }
            }
            attempts += 1;
            tokio::time::sleep(std::time::Duration::from_millis(
                (50u64 << attempts.min(6)).min(2000),
            ))
            .await;
        }
    }

    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix_path = prefix.cloned().unwrap_or_default();
        let prefix = if prefix_path.as_ref().is_empty() {
            String::new()
        } else {
            format!("{prefix_path}/")
        };
        let offset = offset.cloned();
        Box::pin(async_stream::try_stream! {
            let mut marker = String::new();
            let mut seen = HashSet::new();
            let mut last = None::<Path>;
            loop {
                let mut url = store.container_url.clone();
                url.query_pairs_mut().append_pair("restype", "container").append_pair("comp", "list")
                    .append_pair("prefix", &prefix).append_pair("maxresults", "5000");
                if !marker.is_empty() { url.query_pairs_mut().append_pair("marker", &marker); }
                let response = ensure(store.send(Method::GET, url, HeaderMap::new(), None, true).await?, "list", false)?;
                let body = response.bytes().await.map_err(|_| error("Blob listing body failed"))?;
                let page = xml::parse_list(&body)?;
                for meta in page.objects {
                    if !meta.location.prefix_matches(&prefix_path) { Err(error("Blob listing returned an unrelated prefix"))?; }
                    if last.as_ref().is_some_and(|last| meta.location < *last) { Err(error("Blob listing is not globally ordered"))?; }
                    last = Some(meta.location.clone());
                    if offset.as_ref().is_none_or(|offset| meta.location > *offset) { yield meta; }
                }
                if page.marker.is_empty() { break; }
                if !seen.insert(page.marker.clone()) { Err(error("Blob listing repeated a continuation marker"))?; }
                marker = page.marker;
            }
        })
    }
}

fn error(message: &str) -> Error {
    Error::Generic {
        store: "AzureBlobRestStore",
        source: message.to_string().into(),
    }
}

fn unsupported(message: &str) -> Error {
    Error::NotSupported {
        source: message.to_string().into(),
    }
}

fn ensure(response: Response, path: &str, create: bool) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let source = format!("Blob HTTP status {}", status.as_u16()).into();
    Err(match status {
        StatusCode::NOT_FOUND => Error::NotFound {
            path: path.to_string(),
            source,
        },
        StatusCode::NOT_MODIFIED => Error::NotModified {
            path: path.to_string(),
            source,
        },
        StatusCode::PRECONDITION_FAILED if !create => Error::Precondition {
            path: path.to_string(),
            source,
        },
        StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
            if create
                && response
                    .headers()
                    .get("x-ms-error-code")
                    .and_then(|header| header.to_str().ok())
                    .is_some_and(|code| {
                        matches!(code, "BlobAlreadyExists" | "ConditionNotMet")
                    }) =>
        {
            Error::AlreadyExists {
                path: path.to_string(),
                source,
            }
        }
        _ => error(&format!("Blob HTTP status {}", status.as_u16())),
    })
}

#[async_trait]
impl ObjectStore for AzureBlobRestStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !options.extensions.is_empty() {
            return Err(unsupported("GET extensions"));
        }
        if let Some(range) = &options.range {
            range.is_valid().map_err(|_| error("invalid Blob range"))?;
        }
        let mut url = self.object_url(location)?;
        if let Some(version) = &options.version {
            url.query_pairs_mut().append_pair("versionid", version);
        }
        let mut protocol = HeaderMap::new();
        for (name, value) in [
            ("if-match", options.if_match.as_ref()),
            ("if-none-match", options.if_none_match.as_ref()),
        ] {
            if let Some(value) = value {
                insert_header(&mut protocol, name, value)?;
            }
        }
        for (name, date) in [
            ("if-modified-since", options.if_modified_since),
            ("if-unmodified-since", options.if_unmodified_since),
        ] {
            if let Some(date) = date {
                insert_header(
                    &mut protocol,
                    name,
                    &date.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
                )?;
            }
        }
        let requested = if !options.head {
            match &options.range {
                Some(GetRange::Suffix(_)) => {
                    let response = ensure(
                        self.send(Method::HEAD, url.clone(), protocol.clone(), None, true)
                            .await?,
                        location.as_ref(),
                        false,
                    )?;
                    if response.status() != StatusCode::OK {
                        return Err(error("suffix resolution requires HTTP 200 HEAD"));
                    }
                    let size = header_u64(response.headers(), "content-length")?;
                    let range = options
                        .range
                        .as_ref()
                        .ok_or_else(|| error("missing suffix range"))?
                        .as_range(size)
                        .map_err(|_| error("invalid Blob suffix range"))?;
                    if range.is_empty() {
                        return Err(error("empty Blob suffix range"));
                    }
                    if let Some(etag) = text_header(response.headers(), "etag")? {
                        insert_header(&mut protocol, "if-match", &etag)?;
                    } else if options.version.is_none() {
                        return Err(error(
                            "suffix resolution requires an ETag or object version",
                        ));
                    }
                    Some(GetRange::Bounded(range))
                }
                range => range.clone(),
            }
        } else {
            None
        };
        if let Some(range) = &requested {
            let value = match range {
                GetRange::Bounded(range) => format!("bytes={}-{}", range.start, range.end - 1),
                GetRange::Offset(start) => format!("bytes={start}-"),
                GetRange::Suffix(_) => return Err(error("unresolved Blob suffix range")),
            };
            insert_header(&mut protocol, "range", &value)?;
        }
        let method = if options.head {
            Method::HEAD
        } else {
            Method::GET
        };
        let response = ensure(
            self.send(method, url, protocol, None, true).await?,
            location.as_ref(),
            false,
        )?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = if options.head {
            bytes::Bytes::new()
        } else {
            response
                .bytes()
                .await
                .map_err(|_| error("Blob response body failed"))?
        };
        let (range, size) = if let Some(requested) = requested {
            if status != StatusCode::PARTIAL_CONTENT {
                return Err(error("ranged Blob GET requires HTTP 206"));
            }
            let (actual, total) =
                parse_content_range(&required_header(&headers, "content-range")?)?;
            let expected = requested
                .as_range(total)
                .map_err(|_| error("invalid Blob response range"))?;
            if actual != expected || actual.end - actual.start != body.len() as u64 {
                return Err(error(
                    "Blob Content-Range or payload length does not match the request",
                ));
            }
            (actual, total)
        } else {
            if status != StatusCode::OK || headers.contains_key("content-range") {
                return Err(error(
                    "full Blob GET/HEAD requires HTTP 200 without Content-Range",
                ));
            }
            let size = header_u64(&headers, "content-length")?;
            if !options.head && size != body.len() as u64 {
                return Err(error("Blob Content-Length does not match body"));
            }
            (0..size, size)
        };
        let last_modified = match text_header(&headers, "last-modified")? {
            Some(value) => chrono::DateTime::parse_from_rfc2822(&value)
                .map_err(|_| error("invalid Blob Last-Modified header"))?
                .with_timezone(&chrono::Utc),
            None => chrono::DateTime::UNIX_EPOCH,
        };
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size,
            e_tag: text_header(&headers, "etag")?,
            version: text_header(&headers, "x-ms-version-id")?,
        };
        options.check_preconditions(&meta)?;
        let payload = if options.head {
            GetResultPayload::Stream(Box::pin(futures::stream::empty()))
        } else {
            GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(body) })))
        };
        Ok(object_store::delta_kernel_compat::get_result(
            payload,
            meta,
            range,
            Attributes::new(),
        ))
    }
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        validate_write_options(&opts.tags, &opts.attributes, opts.extensions.is_empty())?;
        let create = match opts.mode {
            PutMode::Create => true,
            PutMode::Overwrite => false,
            PutMode::Update(_) => return Err(unsupported("PutMode::Update")),
        };
        if payload.content_length() > MAX_PART_SIZE {
            return Err(unsupported("full PUT larger than 64MiB; use multipart"));
        }
        let mut protocol = HeaderMap::new();
        insert_header(&mut protocol, "x-ms-blob-type", "BlockBlob")?;
        if create {
            insert_header(&mut protocol, "if-none-match", "*")?;
        }
        let response = ensure(
            self.send(
                Method::PUT,
                self.object_url(location)?,
                protocol,
                Some(payload.into()),
                !create,
            )
            .await?,
            location.as_ref(),
            create,
        )?;
        put_result(response.headers())
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        validate_write_options(&opts.tags, &opts.attributes, opts.extensions.is_empty())?;
        Ok(Box::new(multipart::BlobUpload::new(
            self.clone(),
            location.clone(),
        )?))
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_paginated(prefix, None)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_paginated(prefix, Some(offset))
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        Box::pin(locations.then(move |location| {
            let store = store.clone();
            async move {
                let location = location?;
                ensure(
                    store
                        .send(
                            Method::DELETE,
                            store.object_url(&location)?,
                            HeaderMap::new(),
                            None,
                            true,
                        )
                        .await?,
                    location.as_ref(),
                    false,
                )?;
                Ok(location)
            }
        }))
    }
    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(unsupported("delimiter listing"))
    }
    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
        Err(unsupported("copy"))
    }
}

const MAX_PART_SIZE: usize = 64 * 1024 * 1024;

/// Build a literal Blob store from a container URL and REST builder options.
///
/// Accepts `header.*`, the existing `tls.*` keys, and `retry.max_retries`. A supplied
/// provider replaces static headers. `put.verify_on_ambiguous=true`, unknown options,
/// invalid values, and invalid container URLs return errors. No native Azure options
/// or credential discovery are supported. TLS uses [`super::build_rest_client`].
pub fn build_azure_blob_rest_store(
    container_url: &Url,
    options: &HashMap<String, String>,
    auth: Option<Arc<dyn AuthHeaderProvider>>,
) -> Result<Arc<AzureBlobRestStore>> {
    validate_container_url(container_url.clone())?;
    for key in options.keys() {
        if !key.starts_with("header.")
            && !matches!(
                key.as_str(),
                "tls.cert_path"
                    | "tls.key_path"
                    | "tls.ca_path"
                    | "tls.dns_override"
                    | "tls.timeout_secs"
                    | "retry.max_retries"
                    | "put.verify_on_ambiguous"
            )
        {
            return Err(unsupported("unknown Blob REST builder option"));
        }
    }
    match options.get("put.verify_on_ambiguous").map(String::as_str) {
        None | Some("false") => {}
        Some("true") => return Err(unsupported("ambiguous Create verification")),
        _ => {
            return Err(error(
                "invalid put.verify_on_ambiguous; expected true or false",
            ))
        }
    }
    let retries = options
        .get("retry.max_retries")
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| error("invalid retry.max_retries"))
        })
        .transpose()?
        .unwrap_or(0);
    let timeout_secs = options
        .get("tls.timeout_secs")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| error("invalid tls.timeout_secs"))
        })
        .transpose()?;
    let auth = match auth {
        Some(auth) => auth,
        None => Arc::new(
            StaticHeaderProvider::from_pairs(options.iter().filter_map(|(key, value)| {
                key.strip_prefix("header.")
                    .map(|name| (name.to_string(), value.clone()))
            }))
            .map_err(|_| error("invalid static Blob header"))?,
        ),
    };
    let tls = RestClientOptions {
        cert_path: options.get("tls.cert_path").cloned(),
        key_path: options.get("tls.key_path").cloned(),
        ca_path: options.get("tls.ca_path").cloned(),
        dns_overrides: options
            .get("tls.dns_override")
            .map(|value| value.split(',').map(str::to_string).collect())
            .unwrap_or_default(),
        timeout_secs,
    };
    let client = super::client::build_rest_client_with_redirect_policy(
        &tls,
        reqwest::redirect::Policy::none(),
    )
    .map_err(|_| error("Blob REST client configuration failed"))?;
    Ok(Arc::new(
        AzureBlobRestStore::new(container_url.clone(), client, auth)?.with_max_retries(retries),
    ))
}

fn validate_write_options(
    tags: &object_store::TagSet,
    attributes: &Attributes,
    extensions_empty: bool,
) -> Result<()> {
    if !tags.encoded().is_empty() || !attributes.is_empty() || !extensions_empty {
        return Err(unsupported("write tags, attributes, and extensions"));
    }
    Ok(())
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<()> {
    headers.insert(
        name,
        reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| error("invalid Blob operation header"))?,
    );
    Ok(())
}

fn text_header(headers: &HeaderMap, name: &str) -> Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| error("invalid Blob response header"))
        })
        .transpose()
}

fn required_header(headers: &HeaderMap, name: &str) -> Result<String> {
    text_header(headers, name)?.ok_or_else(|| error("missing required Blob response header"))
}

fn header_u64(headers: &HeaderMap, name: &str) -> Result<u64> {
    required_header(headers, name)?
        .parse()
        .map_err(|_| error("invalid numeric Blob response header"))
}

fn parse_content_range(value: &str) -> Result<(std::ops::Range<u64>, u64)> {
    let invalid = || error("malformed Blob Content-Range");
    let (range, total) = value
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .ok_or_else(invalid)?;
    let (start, end) = range.split_once('-').ok_or_else(invalid)?;
    let start: u64 = start.parse().map_err(|_| invalid())?;
    let end: u64 = end.parse().map_err(|_| invalid())?;
    let total: u64 = total.parse().map_err(|_| invalid())?;
    if start > end || end >= total {
        return Err(invalid());
    }
    Ok((start..end.checked_add(1).ok_or_else(invalid)?, total))
}

fn put_result(headers: &HeaderMap) -> Result<PutResult> {
    let mut result = object_store::delta_kernel_compat::empty_put_result();
    result.e_tag = text_header(headers, "etag")?;
    result.version = text_header(headers, "x-ms-version-id")?;
    Ok(result)
}

fn validate_container_url(mut url: Url) -> Result<(Url, bool)> {
    let path = url.path().strip_prefix('/').unwrap_or_default();
    let path = path.strip_suffix('/').unwrap_or(path);
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || path.is_empty()
        || path.contains('/')
        || Path::from_url_path(path).map_or(true, |path| path.parts().count() != 1)
    {
        return Err(error(
            "expected an HTTP(S) container URL without userinfo or fragment",
        ));
    }
    let mut keys = HashSet::new();
    let mut has_sas = false;
    for (key, value) in url.query_pairs() {
        if !matches!(
            key.as_ref(),
            "sv" | "ss"
                | "srt"
                | "sp"
                | "st"
                | "se"
                | "spr"
                | "sip"
                | "si"
                | "sr"
                | "sig"
                | "skoid"
                | "sktid"
                | "skt"
                | "ske"
                | "sks"
                | "skv"
                | "saoid"
                | "suoid"
                | "scid"
                | "ses"
                | "rscc"
                | "rscd"
                | "rsce"
                | "rscl"
                | "rsct"
        ) || !keys.insert(key.to_string())
        {
            return Err(error(
                "container URL contains unsupported or duplicate query parameters",
            ));
        }
        has_sas |= key == "sig" && !value.is_empty();
    }
    url.path_segments_mut()
        .map_err(|_| error("invalid container URL"))?
        .pop_if_empty();
    Ok((url, has_sas))
}
