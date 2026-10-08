//! Native Azure Blob protocol with optional ready, synchronous request headers.
//!
//! This is not the JSON REST file API. The returned native store owns XML listing, ranges,
//! conditional writes, block uploads, retries, and native error mapping.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::{collections::HashMap, sync::Arc};
//! use url::Url;
//! use delta_kernel_default_engine::{DefaultEngineBuilder, rest_store::azure_blob::azure_blob_store_from_url_opts};
//!
//! let url = Url::parse("az://container/table/")?;
//! let options = HashMap::from([
//!     ("azure_storage_account_name".into(), "account".into()),
//!     ("azure_storage_token".into(), "ready-native-token".into()),
//!     ("header.x-custom-context".into(), "context".into()),
//! ]);
//! let store = azure_blob_store_from_url_opts(&url, &options, None)?;
//! let engine = DefaultEngineBuilder::new(Arc::new(store)).build();
//! # let _ = engine;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use delta_kernel::object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use delta_kernel::object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
    ReqwestConnector,
};
use delta_kernel::object_store::{ClientOptions, Result as ObjectStoreResult, RetryConfig};
use delta_kernel::KernelError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONNECTION};
use url::Url;

use super::{AuthHeaderProvider, StaticHeaderProvider};

/// Who authorizes native Blob requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlobHeaderMode {
    /// Preserve native credentials; custom headers are strictly additive.
    #[default]
    NativeCredentials,
    /// Disable native signing and require ready caller bearer headers or native SAS query auth.
    CallerHeaders,
}

impl BlobHeaderMode {
    /// Read `blob.auth_mode`: absent, `native`, or `default` preserves native credentials;
    /// `headers` selects caller headers. Returns an error for every other value.
    pub fn from_options(options: &HashMap<String, String>) -> ObjectStoreResult<Self> {
        match options.get("blob.auth_mode").map(String::as_str) {
            None | Some("native" | "default") => Ok(Self::NativeCredentials),
            Some("headers") => Ok(Self::CallerHeaders),
            Some(_) => Err(config_error(
                "blob.auth_mode must be native, default, or headers",
            )),
        }
    }
}

/// Build a native Azure Blob store for a logical `az://container/table/` table location.
///
/// `options` accepts keys recognized by native [`AzureConfigKey`] (including client options),
/// `blob.auth_mode`, `header.<Name>`, and `retry.max_retries` (native retry configuration).
/// Unknown keys, all `tls.*`, and `put.verify_on_ambiguous` are errors before any I/O. REST
/// mTLS/DNS/timeout and readback policy are not native Azure options. Native configuration and
/// error mapping remain authoritative: on the Arrow 59/object_store 0.13 track a Create PUT
/// HTTP 412 is `Precondition`, not `AlreadyExists`.
///
/// With `provider = None`, static `header.*` options supply a provider if any exist. A supplied
/// provider takes precedence over static header values, matching the REST callback contract.
/// All static names/values must nevertheless be valid and unique. With no headers and native
/// mode, the native builder is returned unchanged. The table prefix is not a store prefix;
/// pass the same logical URL to Snapshot, and store-relative keys start with `table/`.
///
/// Caller mode requires a provider and disables native credential lookup. SAS is accepted only
/// if `sig` is already present on the native request URI (for example a native endpoint query);
/// native SAS credential configuration alone is suppressed by `SkipSignature`. Native mode
/// supports native SAS credentials normally. No new SAS generation/signing policy is added.
pub fn azure_blob_store_from_url_opts(
    url: &Url,
    options: &HashMap<String, String>,
    provider: Option<Arc<dyn AuthHeaderProvider>>,
) -> ObjectStoreResult<MicrosoftAzure> {
    if url.scheme() != "az"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.path().ends_with('/')
    {
        return Err(config_error(
            "Azure Blob headers require logical az://container/table/",
        ));
    }
    let mode = BlobHeaderMode::from_options(options)?;
    let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.to_string());
    let mut static_headers = HeaderMap::new();
    for (key, value) in options {
        if key == "blob.auth_mode" {
            continue;
        }
        if let Some(name) = key.strip_prefix("header.") {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| config_error("Invalid static Azure header name"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| config_error("Invalid static Azure header value"))?;
            if static_headers.insert(name, value).is_some() {
                return Err(config_error("Duplicate static Azure header"));
            }
        } else if key == "retry.max_retries" {
            builder = builder.with_retry(RetryConfig {
                max_retries: value
                    .parse()
                    .map_err(|_| config_error("Invalid retry.max_retries"))?,
                ..Default::default()
            });
        } else if key.starts_with("tls.") || key == "put.verify_on_ambiguous" {
            return Err(config_error(
                "REST TLS and ambiguous-write options are not native Azure options",
            ));
        } else {
            builder = builder.with_config(key.parse::<AzureConfigKey>()?, value);
        }
    }
    let provider = provider.or_else(|| {
        (!static_headers.is_empty()).then(|| {
            Arc::new(StaticHeaderProvider::new(static_headers)) as Arc<dyn AuthHeaderProvider>
        })
    });
    build_azure_blob_store_with_headers(builder, provider, mode)
}

/// Build native Azure storage with per-attempt headers, without replacing its protocol.
///
/// With no provider in native mode, calls `builder.build()` unchanged. Otherwise the stock
/// Reqwest storage connector replaces any custom connector on the builder. Native mode retains
/// the original credential provider and identity transport, even for a same-origin identity
/// endpoint. Environment configuration must remain stable during construction.
///
/// The provider must return ready values and be concurrency-safe. No new refresh/cache policy
/// is added. Headers are attached after native signing, on each native retry attempt. Reserved,
/// colliding, duplicate, and Shared Key signed headers (including every `x-ms` prefix) fail
/// before dispatch. Caller mode accepts only a nonempty bearer Authorization, or request SAS;
/// it cannot sign Shared Key and never falls back to anonymous requests.
///
/// Caller mode rejects native batch deletion before HTTP dispatch: outer headers cannot
/// authorize embedded requests. Native mode preserves batch authorization, but additions apply
/// only to the outer POST, not embedded subrequests. No body rewriting occurs. Native redirect
/// behavior remains unchanged; redirects can forward headers without another provider call.
pub fn build_azure_blob_store_with_headers(
    builder: MicrosoftAzureBuilder,
    provider: Option<Arc<dyn AuthHeaderProvider>>,
    mode: BlobHeaderMode,
) -> ObjectStoreResult<MicrosoftAzure> {
    let Some(provider) = provider else {
        return match mode {
            BlobHeaderMode::NativeCredentials => builder.build(),
            BlobHeaderMode::CallerHeaders => Err(config_error("caller headers require a provider")),
        };
    };
    let builder = match mode {
        BlobHeaderMode::NativeCredentials => {
            let credentials = Arc::clone(builder.clone().build()?.credentials());
            builder.with_credentials(credentials)
        }
        BlobHeaderMode::CallerHeaders => builder.with_skip_signature(true),
    };
    builder
        .with_http_connector(HeaderConnector { provider, mode })
        .build()
}

#[derive(Debug)]
struct HeaderConnector {
    provider: Arc<dyn AuthHeaderProvider>,
    mode: BlobHeaderMode,
}

impl HttpConnector for HeaderConnector {
    fn connect(&self, options: &ClientOptions) -> ObjectStoreResult<HttpClient> {
        Ok(HttpClient::new(HeaderService {
            inner: ReqwestConnector::default().connect(options)?,
            provider: self.provider.clone(),
            defaults: options.get_default_headers().cloned().unwrap_or_default(),
            mode: self.mode,
        }))
    }
}

#[derive(Debug)]
struct HeaderService {
    inner: HttpClient,
    provider: Arc<dyn AuthHeaderProvider>,
    defaults: HeaderMap,
    mode: BlobHeaderMode,
}

#[async_trait]
impl HttpService for HeaderService {
    async fn call(&self, mut request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let url = Url::parse(&request.uri().to_string())
            .map_err(|_| header_error("Invalid native Azure request URI"))?;
        if self.mode == BlobHeaderMode::CallerHeaders
            && request.method().as_str() == "POST"
            && url
                .query_pairs()
                .any(|(name, value)| name == "comp" && value == "batch")
        {
            return Err(header_error(
                "Caller headers cannot authorize native Azure batch subrequests",
            ));
        }
        let headers = self
            .provider
            .headers()
            .map_err(|_| header_error("Azure custom header provider failed"))?;
        validate_headers(&request, &self.defaults, &headers, self.mode)?;
        if self.mode == BlobHeaderMode::CallerHeaders
            && !headers.contains_key(AUTHORIZATION)
            && !url
                .query_pairs()
                .any(|(name, value)| name == "sig" && !value.trim().is_empty())
        {
            return Err(header_error(
                "Caller headers require bearer authorization or native SAS",
            ));
        }
        for (name, value) in &headers {
            request.headers_mut().append(name.clone(), value.clone());
        }
        self.inner.execute(request).await
    }
}

fn validate_headers(
    request: &HttpRequest,
    defaults: &HeaderMap,
    headers: &HeaderMap,
    mode: BlobHeaderMode,
) -> Result<(), HttpError> {
    let shared_key = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_ascii_whitespace().next())
        .is_some_and(|scheme| {
            scheme.eq_ignore_ascii_case("SharedKey") || scheme.eq_ignore_ascii_case("SharedKeyLite")
        });
    for name in headers.keys() {
        let reserved = matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "upgrade"
        );
        let signed = name.as_str().starts_with("x-ms")
            || matches!(
                name.as_str(),
                "content-encoding"
                    | "content-language"
                    | "content-md5"
                    | "content-type"
                    | "date"
                    | "if-modified-since"
                    | "if-match"
                    | "if-none-match"
                    | "if-unmodified-since"
                    | "range"
            );
        if reserved
            || (shared_key && signed)
            || headers.get_all(name).iter().count() != 1
            || request.headers().contains_key(name)
            || defaults.contains_key(name)
            || connection_names(request.headers(), name)
            || connection_names(defaults, name)
        {
            return Err(header_error("Invalid Azure custom header addition"));
        }
        if name == AUTHORIZATION {
            let bearer = headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split_once(' '))
                .is_some_and(|(scheme, token)| {
                    scheme.eq_ignore_ascii_case("Bearer")
                        && !token.is_empty()
                        && !token.bytes().any(|byte| byte.is_ascii_whitespace())
                });
            if mode != BlobHeaderMode::CallerHeaders || !bearer {
                return Err(header_error(
                    "Caller Authorization must be a ready nonempty bearer header",
                ));
            }
        }
    }
    if mode == BlobHeaderMode::CallerHeaders
        && (request.headers().contains_key(AUTHORIZATION) || defaults.contains_key(AUTHORIZATION))
    {
        return Err(header_error(
            "Caller headers cannot override native or default Authorization",
        ));
    }
    Ok(())
}

fn connection_names(headers: &HeaderMap, name: &HeaderName) -> bool {
    headers.get_all(CONNECTION).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(name.as_str()))
        })
    })
}

fn header_error(message: &'static str) -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, KernelError::generic(message))
}

fn config_error(message: &'static str) -> delta_kernel::object_store::Error {
    delta_kernel::object_store::Error::Generic {
        store: "AzureBlobHeaders",
        source: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{
        Error as ObjectStoreError, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode,
        PutOptions,
    };
    use futures::TryStreamExt;
    use rstest::rstest;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::rest_store::headers_from_pairs;

    fn native_builder(server: &MockServer) -> MicrosoftAzureBuilder {
        MicrosoftAzureBuilder::new()
            .with_account("account")
            .with_container_name("container")
            .with_endpoint(server.uri())
            .with_allow_http(true)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
    }

    fn fixed_headers(name: &str, value: &str) -> Arc<dyn AuthHeaderProvider> {
        Arc::new(StaticHeaderProvider::from_pairs([(name.to_owned(), value.to_owned())]).unwrap())
    }

    fn blob_response() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("etag", "\"test-etag\"")
            .insert_header("last-modified", "Tue, 05 Nov 2024 15:01:15 GMT")
            .set_body_string("data")
    }

    #[tokio::test]
    async fn caller_bearer_uses_native_blob_without_native_authorization() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/container/table/blob"))
            .and(header("authorization", "Bearer caller-token"))
            .respond_with(blob_response())
            .expect(1)
            .mount(&server)
            .await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server),
            Some(fixed_headers("authorization", "Bearer caller-token")),
            BlobHeaderMode::CallerHeaders,
        )
        .unwrap();
        assert_eq!(
            store
                .get_opts(&Path::from("table/blob"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "data"
        );
    }

    #[derive(Debug, Default)]
    struct ChangingHeaders(AtomicUsize);

    impl AuthHeaderProvider for ChangingHeaders {
        fn headers(&self) -> ObjectStoreResult<HeaderMap> {
            let number = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            headers_from_pairs([("x-ms-fabric-actor-token".to_owned(), number.to_string())])
        }
    }

    fn list_xml(name: &str, marker: &str) -> String {
        format!("<EnumerationResults><Blobs><Blob><Name>{name}</Name><Properties><Last-Modified>Tue, 05 Nov 2024 15:01:15 GMT</Last-Modified><Content-Length>4</Content-Length><Content-Type>application/octet-stream</Content-Type><Etag>test-etag</Etag></Properties></Blob></Blobs><NextMarker>{marker}</NextMarker></EnumerationResults>")
    }

    fn factory_options(server: &MockServer) -> HashMap<String, String> {
        HashMap::from([
            ("azure_storage_account_name".into(), "account".into()),
            ("azure_storage_endpoint".into(), server.uri()),
            ("azure_allow_http".into(), "true".into()),
            ("azure_storage_token".into(), "native-token".into()),
            ("retry.max_retries".into(), "0".into()),
        ])
    }

    #[tokio::test]
    async fn logical_url_lists_native_xml_pages_and_reads_escaped_keys_without_double_container() {
        let server = MockServer::start().await;
        let provider = Arc::new(ChangingHeaders::default());
        for (number, marker, name, next) in [
            ("1", None, "table/a &amp; b.txt", "page+2/="),
            ("2", Some("page+2/="), "table/c.txt", ""),
        ] {
            let mut mock = Mock::given(method("GET"))
                .and(path("/container"))
                .and(query_param("comp", "list"))
                .and(query_param("restype", "container"))
                .and(query_param("prefix", "table/"))
                .and(header("x-ms-fabric-actor-token", number));
            if let Some(marker) = marker {
                mock = mock.and(query_param("marker", marker));
            }
            mock.respond_with(ResponseTemplate::new(200).set_body_string(list_xml(name, next)))
                .expect(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/container/table/a%20&%20b.txt"))
            .and(header("x-ms-fabric-actor-token", "3"))
            .respond_with(blob_response())
            .expect(1)
            .mount(&server)
            .await;
        let store = azure_blob_store_from_url_opts(
            &Url::parse("az://container/table/").unwrap(),
            &factory_options(&server),
            Some(provider.clone()),
        )
        .unwrap();
        let objects = store
            .list(Some(&Path::from("table")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            objects
                .iter()
                .map(|object| object.location.as_ref())
                .collect::<Vec<_>>(),
            ["table/a & b.txt", "table/c.txt"]
        );
        store.get(&objects[0].location).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert!(requests
            .iter()
            .all(|request| request.headers["authorization"] == "Bearer native-token"));
        assert_eq!(provider.0.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn native_identity_on_storage_origin_is_not_given_caller_headers() {
        let server = MockServer::start().await;
        let provider = Arc::new(ChangingHeaders::default());
        Mock::given(method("GET")).and(path("/identity"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "native-token", "expires_on": (chrono::Utc::now().timestamp() + 3600).to_string()
            }))).expect(1).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/container/blob"))
            .and(header("authorization", "Bearer native-token"))
            .respond_with(blob_response())
            .expect(2)
            .mount(&server)
            .await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_msi_endpoint(format!("{}/identity", server.uri())),
            Some(provider.clone()),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        assert_eq!(provider.0.load(Ordering::SeqCst), 0);
        for _ in 0..2 {
            store.get(&Path::from("blob")).await.unwrap();
        }
        let requests = server.received_requests().await.unwrap();
        let identity = requests
            .iter()
            .find(|request| request.url.path() == "/identity")
            .unwrap();
        assert!(!identity.headers.contains_key("x-ms-fabric-actor-token"));
        assert_eq!(provider.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn native_head_range_create_and_batch_keep_protocol_and_native_auth() {
        let server = MockServer::start().await;
        let provider = Arc::new(ChangingHeaders::default());
        Mock::given(method("HEAD"))
            .and(path("/container/blob"))
            .respond_with(blob_response().insert_header("content-length", "4"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/container/blob"))
            .and(header("range", "bytes=1-2"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "test-etag")
                    .insert_header("last-modified", "Tue, 05 Nov 2024 15:01:15 GMT")
                    .insert_header("content-range", "bytes 1-2/4")
                    .set_body_string("at"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/container/blob"))
            .and(header("if-none-match", "*"))
            .and(header("x-ms-blob-type", "BlockBlob"))
            .respond_with(ResponseTemplate::new(201).insert_header("etag", "test-etag"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST")).and(query_param("comp", "batch"))
            .respond_with(ResponseTemplate::new(202).set_body_raw(
                b"--batch-response\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n--batch-response--\r\n".to_vec(),
                "multipart/mixed; boundary=batch-response"))
            .expect(1).mount(&server).await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_bearer_token_authorization("native-token"),
            Some(provider.clone()),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        let location = Path::from("blob");
        assert_eq!(store.head(&location).await.unwrap().size, 4);
        assert_eq!(
            store
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(1..3)),
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "at"
        );
        store
            .put_opts(
                &location,
                "data".into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store.delete(&location).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(provider.0.load(Ordering::SeqCst), 4);
        assert!(requests
            .iter()
            .all(|request| request.headers["authorization"] == "Bearer native-token"));
        let batch = requests.last().unwrap();
        let body = std::str::from_utf8(&batch.body)
            .unwrap()
            .to_ascii_lowercase();
        assert!(body.contains("authorization: bearer native-token"));
        assert!(!body.contains("x-ms-fabric-actor-token"));
    }

    #[rstest]
    #[case(409)]
    #[case(412)]
    #[tokio::test]
    async fn native_create_conflicts_keep_selected_object_store_mapping(#[case] status: u16) {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(header("if-none-match", "*"))
            .respond_with(ResponseTemplate::new(status))
            .expect(1)
            .mount(&server)
            .await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_bearer_token_authorization("native-token"),
            None,
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        let error = store
            .put_opts(
                &Path::from("blob"),
                "data".into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        if status == 409 {
            assert!(matches!(error, ObjectStoreError::AlreadyExists { .. }));
        } else {
            #[cfg(all(feature = "arrow-59", not(feature = "arrow-60")))]
            assert!(matches!(error, ObjectStoreError::Precondition { .. }));
            #[cfg(feature = "arrow-60")]
            assert!(matches!(error, ObjectStoreError::AlreadyExists { .. }));
        }
    }

    #[tokio::test]
    async fn native_block_upload_retries_and_blocklist_get_current_headers() {
        let server = MockServer::start().await;
        let provider = Arc::new(ChangingHeaders::default());
        for (number, status) in [("1", 503), ("2", 201)] {
            Mock::given(method("PUT"))
                .and(query_param("comp", "block"))
                .and(header("x-ms-fabric-actor-token", number))
                .respond_with(ResponseTemplate::new(status))
                .expect(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(query_param("comp", "blocklist"))
            .and(header("x-ms-fabric-actor-token", "3"))
            .respond_with(ResponseTemplate::new(201).insert_header("etag", "test-etag"))
            .expect(1)
            .mount(&server)
            .await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server)
                .with_bearer_token_authorization("native-token")
                .with_retry(RetryConfig {
                    max_retries: 1,
                    ..Default::default()
                }),
            Some(provider.clone()),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        let mut upload = store
            .put_multipart(&Path::from("table/blob"))
            .await
            .unwrap();
        upload.put_part("data".into()).await.unwrap();
        upload.complete().await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(provider.0.load(Ordering::SeqCst), 3);
        assert!(requests
            .iter()
            .all(
                |request| request.headers["authorization"] == "Bearer native-token"
                    && request
                        .headers
                        .get_all("x-ms-fabric-actor-token")
                        .iter()
                        .count()
                        == 1
            ));
        assert!(std::str::from_utf8(&requests[2].body)
            .unwrap()
            .contains("<BlockList>"));
    }

    #[rstest]
    #[case("x-custom-actor", true)]
    #[case("x-ms-fabric-actor-token", false)]
    #[case("x-msfoo", false)]
    #[case("content-type", false)]
    #[tokio::test]
    async fn shared_key_rejects_signed_additions_before_dispatch(
        #[case] name: &str,
        #[case] allowed: bool,
    ) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(blob_response())
            .expect(u64::from(allowed))
            .mount(&server)
            .await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_access_key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            Some(fixed_headers(name, "context")),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        assert_eq!(store.get(&Path::from("blob")).await.is_ok(), allowed);
        if allowed {
            let requests = server.received_requests().await.unwrap();
            assert!(requests[0].headers["authorization"]
                .to_str()
                .unwrap()
                .starts_with("SharedKey account:"));
        }
    }

    #[rstest]
    #[case(BlobHeaderMode::NativeCredentials)]
    #[case(BlobHeaderMode::CallerHeaders)]
    #[tokio::test]
    async fn native_or_request_uri_sas_keeps_query_authorization(#[case] mode: BlobHeaderMode) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("sig", "native-sas"))
            .respond_with(blob_response())
            .expect(1)
            .mount(&server)
            .await;
        let builder = match mode {
            BlobHeaderMode::NativeCredentials => native_builder(&server)
                .with_sas_authorization(vec![("sig".into(), "native-sas".into())]),
            BlobHeaderMode::CallerHeaders => {
                native_builder(&server).with_endpoint(format!("{}?sig=native-sas", server.uri()))
            }
        };
        let store = build_azure_blob_store_with_headers(
            builder,
            Some(fixed_headers("x-custom-actor", "context")),
            mode,
        )
        .unwrap();
        store.get(&Path::from("blob")).await.unwrap();
        assert!(!server.received_requests().await.unwrap()[0]
            .headers
            .contains_key("authorization"));
    }

    #[rstest]
    #[case("", "")]
    #[case("authorization", "Bearer ")]
    #[case("authorization", "SharedKey account:signature")]
    #[case("authorization", "Bearer token extra")]
    #[case("authorization", "Basic value")]
    #[tokio::test]
    async fn caller_missing_or_invalid_auth_never_falls_back_to_native_or_anonymous(
        #[case] name: &str,
        #[case] value: &str,
    ) {
        let server = MockServer::start().await;
        let provider: Arc<dyn AuthHeaderProvider> = if name.is_empty() {
            Arc::new(StaticHeaderProvider::new(HeaderMap::new()))
        } else {
            fixed_headers(name, value)
        };
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_msi_endpoint(format!("{}/identity", server.uri())),
            Some(provider),
            BlobHeaderMode::CallerHeaders,
        )
        .unwrap();
        assert!(store.get(&Path::from("blob")).await.is_err());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn caller_batch_delete_fails_before_dispatch() {
        let server = MockServer::start().await;
        let store = build_azure_blob_store_with_headers(
            native_builder(&server),
            Some(fixed_headers("authorization", "Bearer caller-token")),
            BlobHeaderMode::CallerHeaders,
        )
        .unwrap();
        assert!(store.delete(&Path::from("blob")).await.is_err());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[rstest]
    #[case("unknown", "x")]
    #[case("blob.auth_mode", "invalid")]
    #[case("retry.max_retries", "nope")]
    #[case("tls.timeout_secs", "10")]
    #[case("tls.cert_path", "cert")]
    #[case("tls.key_path", "key")]
    #[case("tls.ca_path", "ca")]
    #[case("tls.dns_override", "host=127.0.0.1:80")]
    #[case("put.verify_on_ambiguous", "false")]
    #[case("header.invalid name", "value")]
    #[case("header.x-custom", "bad\nvalue")]
    #[test]
    fn factory_rejects_invalid_options_before_client_construction(
        #[case] key: &str,
        #[case] value: &str,
    ) {
        let options = HashMap::from([(key.to_owned(), value.to_owned())]);
        assert!(azure_blob_store_from_url_opts(
            &Url::parse("az://container/table/").unwrap(),
            &options,
            None
        )
        .is_err());
    }

    #[rstest]
    #[case("https://service/container/table/")]
    #[case("abfs://container/table/")]
    #[case("az://container/table")]
    #[case("az://container/table/?sig=secret")]
    #[test]
    fn factory_rejects_nonlogical_table_namespace(#[case] url: &str) {
        assert!(
            azure_blob_store_from_url_opts(&Url::parse(url).unwrap(), &HashMap::new(), None)
                .is_err()
        );
    }

    #[rstest]
    #[case("authorization", "none")]
    #[case("host", "none")]
    #[case("content-length", "none")]
    #[case("transfer-encoding", "none")]
    #[case("connection", "none")]
    #[case("keep-alive", "none")]
    #[case("proxy-authenticate", "none")]
    #[case("proxy-authorization", "none")]
    #[case("proxy-connection", "none")]
    #[case("te", "none")]
    #[case("trailer", "none")]
    #[case("upgrade", "none")]
    #[case("x-custom", "request")]
    #[case("x-custom", "defaults")]
    #[case("x-custom", "request-connection")]
    #[case("x-custom", "default-connection")]
    #[case("x-custom", "duplicate")]
    #[test]
    fn unsafe_header_additions_are_nonretryable(#[case] name: &str, #[case] collision: &str) {
        let mut request = HttpRequest::new(bytes::Bytes::new().into());
        let mut defaults = HeaderMap::new();
        let mut headers = headers_from_pairs([(name.to_owned(), "custom".to_owned())]).unwrap();
        match collision {
            "request" => {
                request
                    .headers_mut()
                    .insert("x-custom", "existing".parse().unwrap());
            }
            "defaults" => {
                defaults.insert("x-custom", "existing".parse().unwrap());
            }
            "request-connection" => {
                request
                    .headers_mut()
                    .insert(CONNECTION, "x-custom".parse().unwrap());
            }
            "default-connection" => {
                defaults.insert(CONNECTION, "x-custom".parse().unwrap());
            }
            "duplicate" => {
                headers.append("x-custom", "second".parse().unwrap());
            }
            _ => {}
        }
        assert_eq!(
            validate_headers(
                &request,
                &defaults,
                &headers,
                BlobHeaderMode::NativeCredentials
            )
            .unwrap_err()
            .kind(),
            HttpErrorKind::Unknown
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn factory_none_or_static_headers_preserve_native_auth(#[case] static_headers: bool) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("authorization", "Bearer native-token"))
            .respond_with(blob_response())
            .expect(1)
            .mount(&server)
            .await;
        let mut options = factory_options(&server);
        if static_headers {
            options.insert("header.x-custom-actor".into(), "context".into());
        }
        let store = azure_blob_store_from_url_opts(
            &Url::parse("az://container/table/").unwrap(),
            &options,
            None,
        )
        .unwrap();
        store.get(&Path::from("table/blob")).await.unwrap();
        assert_eq!(
            server.received_requests().await.unwrap()[0]
                .headers
                .contains_key("x-custom-actor"),
            static_headers
        );
    }

    #[test]
    fn factory_rejects_duplicate_static_header_names_and_caller_without_provider() {
        let url = Url::parse("az://container/table/").unwrap();
        for options in [
            HashMap::from([
                ("header.X-Actor".into(), "1".into()),
                ("header.x-actor".into(), "2".into()),
            ]),
            HashMap::from([("blob.auth_mode".into(), "headers".into())]),
        ] {
            assert!(azure_blob_store_from_url_opts(&url, &options, None).is_err());
        }
    }

    #[rstest]
    #[case(BlobHeaderMode::NativeCredentials)]
    #[case(BlobHeaderMode::CallerHeaders)]
    #[tokio::test]
    async fn factory_native_retries_refresh_headers_without_new_cache_policy(
        #[case] mode: BlobHeaderMode,
    ) {
        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let provider = Arc::new(super::super::RefreshingHeaderProvider::new(move || {
            let number = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let mut headers =
                headers_from_pairs([("x-ms-fabric-actor-token".into(), number.to_string())])?;
            if mode == BlobHeaderMode::CallerHeaders {
                headers.insert(
                    AUTHORIZATION,
                    format!("Bearer caller-{number}").parse().unwrap(),
                );
            }
            Ok((headers, None))
        }));
        for (number, status) in [("1", 503), ("2", 200)] {
            Mock::given(method("GET"))
                .and(header("x-ms-fabric-actor-token", number))
                .respond_with(if status == 200 {
                    blob_response()
                } else {
                    ResponseTemplate::new(status)
                })
                .expect(1)
                .mount(&server)
                .await;
        }
        let mut options = factory_options(&server);
        options.insert("retry.max_retries".into(), "1".into());
        options.insert(
            "blob.auth_mode".into(),
            if mode == BlobHeaderMode::CallerHeaders {
                "headers"
            } else {
                "native"
            }
            .into(),
        );
        let store = azure_blob_store_from_url_opts(
            &Url::parse("az://container/table/").unwrap(),
            &options,
            Some(provider),
        )
        .unwrap();
        store.get(&Path::from("table/blob")).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let requests = server.received_requests().await.unwrap();
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(
                request
                    .headers
                    .get_all("x-ms-fabric-actor-token")
                    .iter()
                    .count(),
                1
            );
            let expected = if mode == BlobHeaderMode::CallerHeaders {
                format!("Bearer caller-{}", index + 1)
            } else {
                "Bearer native-token".into()
            };
            assert_eq!(request.headers["authorization"], expected);
        }
    }

    #[tokio::test]
    async fn provider_failure_is_private_nonretryable_and_never_dispatched() {
        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let provider = Arc::new(super::super::RefreshingHeaderProvider::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Err(ObjectStoreError::Generic {
                store: "test",
                source: "private-provider-detail".into(),
            })
        }));
        let store = build_azure_blob_store_with_headers(
            native_builder(&server)
                .with_bearer_token_authorization("native-token")
                .with_retry(RetryConfig {
                    max_retries: 2,
                    ..Default::default()
                }),
            Some(provider),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        let error = store.get(&Path::from("blob")).await.unwrap_err();
        assert!(!error.to_string().contains("private-provider-detail"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn concurrent_native_requests_share_the_provider_without_header_accumulation() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(blob_response())
            .expect(4)
            .mount(&server)
            .await;
        let provider = Arc::new(ChangingHeaders::default());
        let store = build_azure_blob_store_with_headers(
            native_builder(&server).with_bearer_token_authorization("native-token"),
            Some(provider.clone()),
            BlobHeaderMode::NativeCredentials,
        )
        .unwrap();
        let location = Path::from("blob");
        futures::future::try_join_all((0..4).map(|_| store.get(&location)))
            .await
            .unwrap();
        assert_eq!(provider.0.load(Ordering::SeqCst), 4);
        let requests = server.received_requests().await.unwrap();
        let mut values: Vec<_> = requests
            .iter()
            .map(|request| {
                request.headers["x-ms-fabric-actor-token"]
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        values.sort();
        assert_eq!(values, ["1", "2", "3", "4"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_native_engine_snapshots_and_commit_use_updated_ready_bearer_and_actor() {
        let server = MockServer::start().await;
        let actor = Arc::new(AtomicUsize::new(1));
        let current = actor.clone();
        let provider = Arc::new(super::super::RefreshingHeaderProvider::new(move || {
            let number = current.load(Ordering::SeqCst);
            Ok((
                headers_from_pairs([
                    ("authorization".into(), format!("Bearer caller-{number}")),
                    ("x-ms-fabric-actor-token".into(), number.to_string()),
                ])?,
                None,
            ))
        }));
        let seed = test_utils::actions_to_string(vec![test_utils::TestAction::Metadata]);
        Mock::given(method("GET"))
            .and(path("/container"))
            .and(query_param("comp", "list"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(list_xml("table/_delta_log/00000000000000000000.json", "")),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/container/table/_delta_log/00000000000000000000.json",
            ))
            .respond_with(blob_response().set_body_string(seed))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/container/table/_delta_log/_last_checkpoint"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/container/table/_delta_log/00000000000000000001.json",
            ))
            .and(header("if-none-match", "*"))
            .and(header("authorization", "Bearer caller-2"))
            .and(header("x-ms-fabric-actor-token", "2"))
            .respond_with(ResponseTemplate::new(201).insert_header("etag", "test-etag"))
            .expect(1)
            .mount(&server)
            .await;
        let mut options = factory_options(&server);
        options.insert("blob.auth_mode".into(), "headers".into());
        let url = Url::parse("az://container/table/").unwrap();
        let store = azure_blob_store_from_url_opts(&url, &options, Some(provider)).unwrap();
        let engine = crate::DefaultEngineBuilder::new(Arc::new(store)).build();
        let first = delta_kernel::Snapshot::builder_for(url.as_str())
            .build(&engine)
            .unwrap();
        assert_eq!(first.version(), 0);
        let split = server.received_requests().await.unwrap().len();
        actor.store(2, Ordering::SeqCst);
        let second = delta_kernel::Snapshot::builder_for(url.as_str())
            .build(&engine)
            .unwrap();
        assert_eq!(second.version(), 0);
        let committed = second
            .transaction(
                Box::new(delta_kernel::committer::FileSystemCommitter::new()),
                &engine,
            )
            .unwrap()
            .with_engine_info("native-header-test")
            .commit(&engine)
            .unwrap()
            .unwrap_committed();
        assert_eq!(committed.commit_version(), 1);
        let requests = server.received_requests().await.unwrap();
        assert!(split > 0 && requests.len() > split);
        for (index, request) in requests.iter().enumerate() {
            let number = if index < split { 1 } else { 2 };
            assert_eq!(
                request.headers["authorization"],
                format!("Bearer caller-{number}")
            );
            assert_eq!(
                request.headers["x-ms-fabric-actor-token"],
                number.to_string()
            );
            assert!(!request.url.path().contains("/container/container/"));
        }
    }
}
