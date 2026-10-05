use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use delta_kernel::object_store::azure::{
    AzureConfigKey, AzureCredentialProvider, MicrosoftAzureBuilder,
};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{self, Error, ObjectStore, ObjectStoreScheme};
use delta_kernel::KernelError as DeltaError;
use url::Url;

/// Alias for convenience
type ClosureReturn = Result<(Box<dyn ObjectStore>, Path), Error>;
/// This type alias makes it easier to reference the handler closure(s)
///
/// It uses a HashMap<String, String> which _must_ be converted in [store_from_url_opts]
/// because we cannot use generics in this scenario.
type HandlerClosure = Arc<dyn Fn(&Url, HashMap<String, String>) -> ClosureReturn + Send + Sync>;
/// hashmap containing scheme => handler fn mappings to allow consumers of delta-kernel-rs provide
/// their own url opts parsers for different scemes
type Handlers = HashMap<String, HandlerClosure>;
/// The URL_REGISTRY contains the custom URL scheme handlers that will parse URL options
static URL_REGISTRY: LazyLock<RwLock<Handlers>> = LazyLock::new(|| RwLock::new(HashMap::default()));

/// Insert a new URL handler for [store_from_url_opts] with the given `scheme`. This allows
/// users to provide their own custom URL handler to plug new
/// [delta_kernel::object_store::ObjectStore] instances into delta-kernel, which is used by
/// [store_from_url_opts] to parse the URL.
pub fn insert_url_handler(
    scheme: impl AsRef<str>,
    handler_closure: HandlerClosure,
) -> Result<(), DeltaError> {
    let Ok(mut registry) = URL_REGISTRY.write() else {
        return Err(DeltaError::generic(
            "failed to acquire lock for adding a URL handler!",
        ));
    };
    registry.insert(scheme.as_ref().into(), handler_closure);
    Ok(())
}

/// Create an [`ObjectStore`] from a URL.
///
/// Returns an `Arc<dyn ObjectStore>` ready to use with [`crate::DefaultEngine`].
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use delta_kernel_default_engine::storage::store_from_url;
/// # use delta_kernel::Result;
/// # fn example() -> Result<()> {
/// let url = Url::parse("file:///path/to/table")?;
/// let store = store_from_url(&url)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url(url: &Url) -> delta_kernel::Result<Arc<dyn ObjectStore>> {
    store_from_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Create an [`ObjectStore`] from a URL with custom options.
///
/// Returns an `Arc<dyn ObjectStore>` ready to use with [`crate::DefaultEngine`].
///
/// This function checks for custom URL handlers registered via [`insert_url_handler`]
/// before falling back to [`object_store`]'s default behavior.
///
/// # Example
///
/// ```rust
/// # use url::Url;
/// # use std::collections::HashMap;
/// # use delta_kernel_default_engine::storage::store_from_url_opts;
/// # use delta_kernel::Result;
/// # fn example() -> Result<()> {
/// let url = Url::parse("s3://my-bucket/path/to/table")?;
/// let options = HashMap::from([("region", "us-west-2")]);
/// let store = store_from_url_opts(&url, options)?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url_opts<I, K, V>(
    url: &Url,
    options: I,
) -> delta_kernel::Result<Arc<dyn ObjectStore>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    // First attempt to use any schemes registered via insert_url_handler,
    // falling back to the default behavior of delta_kernel::object_store::parse_url_opts
    let (store, _path) = if let Ok(handlers) = URL_REGISTRY.read() {
        if let Some(handler) = handlers.get(url.scheme()) {
            let options = options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.into()))
                .collect();
            handler(url, options)?
        } else {
            object_store::parse_url_opts(url, options)?
        }
    } else {
        object_store::parse_url_opts(url, options)?
    };

    Ok(Arc::new(store))
}

/// Create a store for `url` and `options` with an optional Azure credential provider.
///
/// With `credentials` set to `None`, delegates to [`store_from_url_opts`] unchanged. With a
/// provider, returns a stock Azure store retaining that provider. Recognized option keys are
/// case-insensitive; unknown keys are ignored. URL account/container settings take precedence
/// over options, and client/endpoint settings retain their stock behavior.
///
/// # Errors
///
/// Rejects custom URL handlers, non-Azure URLs, competing authentication options (including
/// credential type overrides), and emulator or unsigned-request options unless explicitly false.
/// Returns an error if the URL registry cannot be read or the stock Azure builder fails.
/// Provider acquisition errors propagate from subsequent store operations without auth fallback.
///
/// # Examples
///
/// ```rust
/// # use url::Url;
/// # use delta_kernel::object_store::azure::AzureCredentialProvider;
/// # use delta_kernel_default_engine::storage::store_from_url_opts_with_azure_credentials;
/// # fn example(credentials: AzureCredentialProvider) -> delta_kernel::Result<()> {
/// let url = Url::parse("abfss://container@account.dfs.core.windows.net/table/")?;
/// let store = store_from_url_opts_with_azure_credentials(
///     &url,
///     [("azure_timeout", "30s")],
///     Some(credentials),
/// )?;
/// # Ok(())
/// # }
/// ```
pub fn store_from_url_opts_with_azure_credentials<I, K, V>(
    url: &Url,
    options: I,
    credentials: Option<AzureCredentialProvider>,
) -> delta_kernel::Result<Arc<dyn ObjectStore>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let Some(credentials) = credentials else {
        return store_from_url_opts(url, options);
    };

    let handlers = URL_REGISTRY
        .read()
        .map_err(|_| DeltaError::generic("failed to read URL handlers for Azure credentials"))?;
    if handlers.contains_key(url.scheme()) {
        return Err(DeltaError::generic(
            "Azure credentials cannot be used with a custom URL handler",
        ));
    }
    drop(handlers);

    let (scheme, _path) = ObjectStoreScheme::parse(url).map_err(object_store::Error::from)?;
    if scheme != ObjectStoreScheme::MicrosoftAzure {
        return Err(DeltaError::generic(
            "Azure credentials require an Azure storage URL",
        ));
    }

    let builder = options.into_iter().try_fold(
        MicrosoftAzureBuilder::new().with_url(url.to_string()),
        |builder, (key, value)| -> delta_kernel::Result<_> {
            let Ok(key) = key.as_ref().to_ascii_lowercase().parse::<AzureConfigKey>() else {
                return Ok(builder);
            };
            let value = value.into();
            match key {
                AzureConfigKey::AccountName
                | AzureConfigKey::ContainerName
                | AzureConfigKey::Endpoint
                | AzureConfigKey::UseFabricEndpoint
                | AzureConfigKey::DisableTagging
                | AzureConfigKey::Client(_) => {}
                #[cfg(feature = "arrow-60")]
                AzureConfigKey::EncryptionKey => {}
                AzureConfigKey::UseEmulator | AzureConfigKey::SkipSignature => {
                    if !matches!(
                        value.to_ascii_lowercase().as_str(),
                        "0" | "false" | "off" | "no" | "n"
                    ) {
                        return Err(DeltaError::generic(format!(
                            "Azure credential provider requires {} to be false",
                            key.as_ref()
                        )));
                    }
                }
                _ => {
                    return Err(DeltaError::generic(format!(
                        "Azure credential provider conflicts with {}",
                        key.as_ref()
                    )));
                }
            }
            Ok(builder.with_config(key, value))
        },
    )?;

    Ok(Arc::new(builder.with_credentials(credentials).build()?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use delta_kernel::object_store::azure::{AzureCredential, AzureCredentialProvider};
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{self, CredentialProvider, ObjectStore, ObjectStoreExt};
    #[cfg(all(feature = "arrow-59", not(feature = "arrow-60")))]
    use hdfs_native_object_store_13::HdfsObjectStoreBuilder;
    #[cfg(feature = "arrow-60")]
    use hdfs_native_object_store_14::HdfsObjectStoreBuilder;
    use rstest::rstest;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        insert_url_handler, store_from_url_opts, store_from_url_opts_with_azure_credentials,
        URL_REGISTRY,
    };
    use crate::*;

    #[derive(Debug)]
    struct TestAzureCredentialProvider {
        credential: Arc<AzureCredential>,
        calls: AtomicUsize,
        fail: bool,
    }

    impl TestAzureCredentialProvider {
        fn new(fail: bool) -> Self {
            Self {
                credential: Arc::new(AzureCredential::BearerToken("u1-test-token".into())),
                calls: AtomicUsize::new(0),
                fail,
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for TestAzureCredentialProvider {
        type Credential = AzureCredential;

        async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(object_store::Error::Generic {
                    store: "U1 test provider",
                    source: "test credential acquisition failed".into(),
                });
            }
            Ok(Arc::clone(&self.credential))
        }
    }

    fn azure_credentials() -> AzureCredentialProvider {
        Arc::new(TestAzureCredentialProvider::new(false))
    }

    #[rstest]
    fn no_provider_matches_stock_construction(
        #[values(
            "file:///u1-storage-test/",
            "memory:///",
            "unknown:///",
            "az://container"
        )]
        url: &str,
    ) {
        let url = Url::parse(url).unwrap();
        let options = [
            ("unknown_key", "ignored"),
            ("bearer_token", "static-test-token"),
        ];
        let stock = store_from_url_opts(&url, options);
        let actual = store_from_url_opts_with_azure_credentials(&url, options, None);
        match (stock, actual) {
            (Ok(stock), Ok(actual)) => assert_eq!(stock.to_string(), actual.to_string()),
            (Err(stock), Err(actual)) => assert_eq!(stock.to_string(), actual.to_string()),
            _ => panic!("no-provider construction must match the stock result"),
        }
    }

    #[rstest]
    fn azure_provider_preserves_url_precedence_and_unknown_options(
        #[values(
            "az://container@account.blob.core.windows.net/table/",
            "abfs://container@account.dfs.core.windows.net/table/",
            "abfss://container@account.dfs.core.windows.net/table/",
            "https://account.blob.core.windows.net/container/table/",
            "https://account.dfs.core.windows.net/container/table/",
            "https://account.blob.fabric.microsoft.com/container/table/",
            "https://account.dfs.fabric.microsoft.com/container/table/"
        )]
        url: &str,
    ) {
        let url = Url::parse(url).unwrap();
        let provider = Arc::new(TestAzureCredentialProvider::new(false));
        let options = [
            ("AZURE_STORAGE_ACCOUNT_NAME", "ignored-account"),
            ("AZURE_CONTAINER_NAME", "ignored-container"),
            ("unknown_key", "ignored"),
            ("azure_timeout", "2s"),
        ];
        let store =
            store_from_url_opts_with_azure_credentials(&url, options, Some(provider.clone()))
                .unwrap();
        assert_eq!(
            store.to_string(),
            "MicrosoftAzure { account: account, container: container }"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn azure_provider_accepts_root_aliases_and_account_options(
        #[values("az", "azure", "adl", "abfs", "abfss")] scheme: &str,
        #[values("account_name", "AZURE_STORAGE_ACCOUNT_NAME")] account_key: &str,
    ) {
        let url = Url::parse(&format!("{scheme}://container/table/")).unwrap();
        let store = store_from_url_opts_with_azure_credentials(
            &url,
            [
                (account_key, "account"),
                ("container_name", "ignored-container"),
            ],
            Some(azure_credentials()),
        )
        .unwrap();
        assert_eq!(
            store.to_string(),
            "MicrosoftAzure { account: account, container: container }"
        );
    }

    #[rstest]
    fn azure_provider_preserves_stock_invalid_url_errors(
        #[values(
            "azure://container@account.blob.core.windows.net/table/",
            "adl://container@account.dfs.core.windows.net/table/",
            "https://account.extra.blob.core.windows.net/container/table/"
        )]
        url: &str,
    ) {
        let url = Url::parse(url).unwrap();
        let stock = store_from_url_opts(
            &url,
            [
                ("account_name", "account"),
                ("bearer_token", "static-test-token"),
            ],
        )
        .unwrap_err();
        let actual = store_from_url_opts_with_azure_credentials(
            &url,
            [("account_name", "account")],
            Some(azure_credentials()),
        )
        .unwrap_err();
        assert_eq!(stock.to_string(), actual.to_string());
    }

    #[rstest]
    #[tokio::test]
    async fn azure_provider_authorizes_requests_with_stock_endpoint_and_client_options(
        #[values("azure_storage_endpoint", "AZURE_ENDPOINT", "endpoint")] endpoint_key: &str,
        #[values("allow_http", "AZURE_ALLOW_HTTP")] allow_http_key: &str,
    ) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/prefix/container/table/blob"))
            .and(header("authorization", "Bearer u1-test-token"))
            .and(header("user-agent", "u1-storage-test"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"data".to_vec())
                    .insert_header("etag", "\"u1-test-etag\"")
                    .insert_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            )
            .expect(2)
            .mount(&server)
            .await;
        let provider = Arc::new(TestAzureCredentialProvider::new(false));
        let store = store_from_url_opts_with_azure_credentials(
            &Url::parse("abfss://container@account.dfs.core.windows.net/table/").unwrap(),
            [
                (endpoint_key, format!("{}/prefix", server.uri())),
                (allow_http_key, "TRUE".into()),
                ("azure_user_agent", "u1-storage-test".into()),
                ("azure_timeout", "2s".into()),
                ("unknown_auth_option", "ignored".into()),
            ],
            Some(provider.clone()),
        )
        .unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        for _ in 0..2 {
            let data = store
                .get(&Path::from("table/blob"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.as_ref(), b"data");
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn azure_provider_failure_sends_no_request_and_does_not_fall_back() {
        let server = MockServer::start().await;
        let provider = Arc::new(TestAzureCredentialProvider::new(true));
        let store = store_from_url_opts_with_azure_credentials(
            &Url::parse("az://container").unwrap(),
            [
                ("account_name", "account".into()),
                ("endpoint", server.uri()),
                ("allow_http", "true".into()),
            ],
            Some(provider.clone()),
        )
        .unwrap();
        let error = store.get(&Path::from("blob")).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("test credential acquisition failed"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_provider_reads_local_files_like_stock() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("blob");
        std::fs::write(&file, b"local-data").unwrap();
        let url = Url::from_file_path(file).unwrap();
        let location = Path::from_url_path(url.path()).unwrap();
        let options = [("unknown_key", "ignored")];
        let stock = store_from_url_opts(&url, options).unwrap();
        let actual = store_from_url_opts_with_azure_credentials(&url, options, None).unwrap();
        let stock_data = stock.get(&location).await.unwrap().bytes().await.unwrap();
        let actual_data = actual.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(stock_data.as_ref(), b"local-data");
        assert_eq!(actual_data, stock_data);
    }

    #[test]
    fn azure_provider_rejects_custom_handlers_without_invoking_them() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        insert_url_handler(
            "u1-azure-credentials-test",
            Arc::new(move |url, _options| {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                Ok((
                    Box::new(object_store::memory::InMemory::new()),
                    Path::parse(url.path())?,
                ))
            }),
        )
        .unwrap();
        let url = Url::parse("u1-azure-credentials-test://container/table/").unwrap();
        let error = store_from_url_opts_with_azure_credentials(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Some(azure_credentials()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("custom URL handler"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        store_from_url_opts_with_azure_credentials(&url, std::iter::empty::<(&str, &str)>(), None)
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "arrow-60")]
    #[rstest]
    fn azure_provider_rejects_explicit_credential_type(
        #[values("credential_type", "AZURE_CREDENTIAL_TYPE")] key: &str,
        #[values("auto", "bearer_token", "managed_identity", "sensitive-test-value")] value: &str,
    ) {
        let error = store_from_url_opts_with_azure_credentials(
            &Url::parse("az://container").unwrap(),
            [("account_name", "account"), (key, value)],
            Some(azure_credentials()),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("conflicts with azure_credential_type"));
        assert!(!error.to_string().contains("sensitive-test-value"));
    }

    #[cfg(all(feature = "arrow-59", not(feature = "arrow-60")))]
    #[rstest]
    fn azure_provider_ignores_unrecognized_credential_type(
        #[values("credential_type", "AZURE_CREDENTIAL_TYPE")] key: &str,
    ) {
        let store = store_from_url_opts_with_azure_credentials(
            &Url::parse("az://container").unwrap(),
            [("account_name", "account"), (key, "ignored")],
            Some(azure_credentials()),
        )
        .unwrap();
        assert_eq!(
            store.to_string(),
            "MicrosoftAzure { account: account, container: container }"
        );
    }

    #[rstest]
    fn azure_provider_rejects_non_azure_urls(
        #[values(
            "file:///u1-storage-test/",
            "memory:///",
            "s3://bucket/table/",
            "gs://bucket/table/",
            "https://example.com/table/",
            "http://account.blob.core.windows.net/container/"
        )]
        url: &str,
    ) {
        let error = store_from_url_opts_with_azure_credentials(
            &Url::parse(url).unwrap(),
            std::iter::empty::<(&str, &str)>(),
            Some(azure_credentials()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("require an Azure storage URL"));
    }

    #[rstest]
    fn azure_provider_rejects_competing_auth_without_exposing_values(
        #[values(
            "AZURE_STORAGE_ACCOUNT_KEY",
            "access_key",
            "azure_client_secret",
            "client_id",
            "tenant_id",
            "authority_host",
            "sas_token",
            "bearer_token",
            "azure_msi_endpoint",
            "object_id",
            "msi_resource_id",
            "federated_token_file",
            "azure_use_azure_cli",
            "fabric_token_service_url",
            "fabric_workload_host",
            "fabric_session_token",
            "fabric_cluster_identifier"
        )]
        key: &str,
    ) {
        let error = store_from_url_opts_with_azure_credentials(
            &Url::parse("az://container").unwrap(),
            [(key, "sensitive-test-value")],
            Some(azure_credentials()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("conflicts with"));
        assert!(!error.to_string().contains("sensitive-test-value"));
    }

    #[rstest]
    fn azure_provider_requires_auth_bypass_options_to_be_false(
        #[values(
            "use_emulator",
            "AZURE_STORAGE_USE_EMULATOR",
            "skip_signature",
            "AZURE_SKIP_SIGNATURE"
        )]
        key: &str,
        #[values(
            "false", "FALSE", "0", "OFF", "no", "N", "true", "TRUE", "1", "ON", "yes", "Y",
            "invalid"
        )]
        value: &str,
    ) {
        let result = store_from_url_opts_with_azure_credentials(
            &Url::parse("az://container").unwrap(),
            [("account_name", "account"), (key, value)],
            Some(azure_credentials()),
        );
        let is_false = matches!(
            value.to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no" | "n"
        );
        assert_eq!(result.is_ok(), is_false);
        if let Err(error) = result {
            assert!(error.to_string().contains("to be false"));
            assert!(!error.to_string().contains("invalid"));
        }
    }

    /// Example funciton of doing testing of a custom [HdfsObjectStore] construction
    fn parse_url_opts_hdfs_native<I, K, V>(
        url: &Url,
        options: I,
    ) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let options_map = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into()));
        let store = HdfsObjectStoreBuilder::new()
            .with_url(url.as_str())
            .with_config(options_map)
            .build()?;
        let path = Path::parse(url.path())?;
        Ok((Box::new(store), path))
    }

    #[test]
    fn test_add_hdfs_scheme() {
        let scheme = "hdfs";
        if let Ok(handlers) = URL_REGISTRY.read() {
            assert!(handlers.get(scheme).is_none());
        } else {
            panic!("Failed to read the RwLock for the registry");
        }
        insert_url_handler(scheme, Arc::new(parse_url_opts_hdfs_native))
            .expect("Failed to add new URL scheme handler");

        if let Ok(handlers) = URL_REGISTRY.read() {
            assert!(handlers.get(scheme).is_some());
        } else {
            panic!("Failed to read the RwLock for the registry");
        }

        let url: Url = Url::parse("hdfs://example").expect("Failed to parse URL");
        let options: HashMap<String, String> = HashMap::default();
        // Currently constructing an [HdfsObjectStore] won't work if there isn't an actual HDFS
        // to connect to, so the only way to really verify that we got the object store we
        // expected is to inspect the `store` on the error v_v
        match store_from_url_opts(&url, options) {
            Err(delta_kernel::KernelError::ObjectStore(object_store::Error::Generic {
                store,
                source: _,
            })) => {
                assert_eq!(store, "HdfsObjectStore");
            }
            Err(unexpected) => panic!("Unexpected error happened: {unexpected:?}"),
            Ok(_) => {
                panic!("Expected to get an error when constructing an HdfsObjectStore, but something didn't work as expected! Either the parse_url_opts_hdfs_native function didn't get called, or the hdfs-native-object-store no longer errors when it cannot connect to HDFS");
            }
        }
    }
}
