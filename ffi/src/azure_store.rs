use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::object_store::azure::{
    AzureConfigKey, AzureCredentialProvider, MicrosoftAzureBuilder,
};
use delta_kernel::object_store::{self, ObjectStore, ObjectStoreScheme};
use delta_kernel::{KernelError, Result};
use url::Url;

pub(crate) fn build(
    url: &Url,
    options: HashMap<String, String>,
    provider: AzureCredentialProvider,
) -> Result<Arc<dyn ObjectStore>> {
    let (scheme, _) = ObjectStoreScheme::parse(url).map_err(object_store::Error::from)?;
    if scheme != ObjectStoreScheme::MicrosoftAzure {
        return Err(KernelError::generic(
            "Azure credentials require an Azure storage URL",
        ));
    }
    let mut builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
    for (key, value) in options {
        let Ok(key) = key.to_ascii_lowercase().parse::<AzureConfigKey>() else {
            continue;
        };
        builder = builder.with_config(key, value);
    }
    Ok(Arc::new(builder.with_credentials(provider).build()?))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use delta_kernel::object_store::azure::AzureCredential;
    use delta_kernel::object_store::CredentialProvider;
    use rstest::rstest;

    use super::*;

    const SECRET: &str = "Secret-not-a-real-credential";

    #[derive(Debug, Default)]
    struct TestProvider(AtomicUsize);

    #[async_trait]
    impl CredentialProvider for TestProvider {
        type Credential = AzureCredential;

        async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(AzureCredential::BearerToken(
                "azure-store-test-token".into(),
            )))
        }
    }

    fn build_store(url: &str, options: &[(&str, &str)]) -> Result<Arc<dyn ObjectStore>> {
        let provider = Arc::new(TestProvider::default());
        let options = std::iter::once(("AZURE_STORAGE_ACCOUNT_NAME", "configured"))
            .chain(options.iter().copied())
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let result = build(&Url::parse(url).unwrap(), options, provider.clone());
        assert_eq!(provider.0.load(Ordering::Relaxed), 0);
        result
    }

    #[rstest]
    #[case("az://container/table/", "configured")]
    #[case("azure://container/table/", "configured")]
    #[case("adl://container/table/", "configured")]
    #[case("abfss://container@account.dfs.core.windows.net/table/", "account")]
    #[case("https://account.blob.core.windows.net/container/table/", "account")]
    #[case("https://account.dfs.core.windows.net/container/table/", "account")]
    #[cfg_attr(
        miri,
        ignore = "Safe Azure client construction; no unsafe FFI exercised"
    )]
    fn built_in_azure_urls_preserve_url_precedence(#[case] url: &str, #[case] account: &str) {
        let store = build_store(
            url,
            &[
                ("AZURE_CONTAINER_NAME", "ignored-container"),
                ("AZURE_TIMEOUT", "2s"),
                ("AZURE_ENDPOINT", "https://example.invalid/prefix"),
                ("unknown_auth_option", SECRET),
            ],
        )
        .unwrap();
        assert_eq!(
            store.to_string(),
            format!("MicrosoftAzure {{ account: {account}, container: container }}")
        );
    }

    #[rstest]
    #[case("AZURE_STORAGE_ACCOUNT_KEY", SECRET)]
    #[case("SaS_ToKeN", SECRET)]
    #[case("bearer_token", SECRET)]
    #[case("Azure_Client_Secret", SECRET)]
    #[case("azure_client_id", "unused-client")]
    #[case("azure_tenant_id", "unused-tenant")]
    #[case("authority_host", "unused-not-a-url")]
    #[case("azure_msi_endpoint", "unused-not-a-url")]
    #[case("object_id", "unused-id")]
    #[case("federated_token_file", "unused-nonexistent-file")]
    #[case("azure_use_azure_cli", "false")]
    #[case("azure_use_azure_cli", "unused-invalid-boolean")]
    #[case("fabric_token_service_url", "unused-not-a-url")]
    #[case("AZURE_CREDENTIAL_TYPE", "auto")]
    #[case("credential_type", "unused-invalid-selector")]
    #[cfg_attr(
        miri,
        ignore = "Safe Azure client construction; no unsafe FFI exercised"
    )]
    fn custom_provider_options_match_native_precedence(#[case] key: &str, #[case] value: &str) {
        let url = "az://container/table/";
        let mut native = MicrosoftAzureBuilder::new()
            .with_url(url)
            .with_config(AzureConfigKey::AccountName, "configured");
        if let Ok(key) = key.to_ascii_lowercase().parse::<AzureConfigKey>() {
            native = native.with_config(key, value);
        }
        let provider = Arc::new(TestProvider::default());
        let native = native.with_credentials(provider.clone()).build();
        let ffi = build_store(url, &[(key, value)]);
        assert!(
            native.is_ok(),
            "native custom-provider construction must accept {key}"
        );
        assert_eq!(ffi.is_ok(), native.is_ok());
        assert_eq!(ffi.unwrap().to_string(), native.unwrap().to_string());
        assert_eq!(provider.0.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case("use_emulator", "true")]
    #[case("AZURE_STORAGE_USE_EMULATOR", "FALSE")]
    #[case("use_emulator", "on")]
    #[case("use_emulator", "not-a-boolean")]
    #[case("skip_signature", "true")]
    #[case("AZURE_SKIP_SIGNATURE", "off")]
    #[case("skip_signature", "YES")]
    #[case("skip_signature", "not-a-boolean")]
    #[cfg_attr(
        miri,
        ignore = "Safe Azure client construction; no unsafe FFI exercised"
    )]
    fn auth_bypass_options_match_native_builder(#[case] key: &str, #[case] value: &str) {
        let url = "az://container/table/";
        let provider = Arc::new(TestProvider::default());
        let native = MicrosoftAzureBuilder::new()
            .with_url(url)
            .with_config(AzureConfigKey::AccountName, "configured")
            .with_config(key.to_ascii_lowercase().parse().unwrap(), value)
            .with_credentials(provider.clone())
            .build();
        let ffi = build_store(url, &[(key, value)]);
        match (native, ffi) {
            (Ok(native), Ok(ffi)) => assert_eq!(native.to_string(), ffi.to_string()),
            (Err(native), Err(ffi)) => {
                assert_eq!(KernelError::from(native).to_string(), ffi.to_string());
            }
            _ => panic!("native and FFI construction must agree for {key}={value}"),
        }
        assert_eq!(provider.0.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    fn non_azure_urls_require_the_built_in_azure_backend(
        #[values("file:///table/", "https://example.com/table/")] url: &str,
    ) {
        let error = build_store(url, &[]).unwrap_err();
        assert_eq!(
            error.to_string(),
            KernelError::generic("Azure credentials require an Azure storage URL").to_string()
        );
    }

    #[test]
    fn unknown_scheme_preserves_stock_parse_error() {
        let url = "custom-azure://container/table/";
        let error = ObjectStoreScheme::parse(&Url::parse(url).unwrap()).unwrap_err();
        let expected = KernelError::from(object_store::Error::from(error));
        assert_eq!(
            build_store(url, &[]).unwrap_err().to_string(),
            expected.to_string()
        );
    }
}
