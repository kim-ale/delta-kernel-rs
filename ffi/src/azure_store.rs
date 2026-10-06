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
                    return Err(KernelError::generic(format!(
                        "Azure credential provider requires {} to be false",
                        key.as_ref()
                    )));
                }
            }
            _ => {
                return Err(KernelError::generic(format!(
                    "Azure credential provider conflicts with {}",
                    key.as_ref()
                )));
            }
        }
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
    fn recognized_auth_options_conflict_without_exposing_values(
        #[values(
            "AZURE_STORAGE_ACCOUNT_KEY",
            "SaS_ToKeN",
            "bearer_token",
            "Azure_Client_Secret",
            "azure_msi_endpoint",
            "azure_use_azure_cli"
        )]
        key: &str,
    ) {
        let error = build_store("az://container/table/", &[(key, SECRET)]).unwrap_err();
        assert!(error.to_string().contains("conflicts with"));
        assert!(!error.to_string().contains(SECRET));
    }

    #[rstest]
    #[case("use_emulator", "true", false)]
    #[case("AZURE_STORAGE_USE_EMULATOR", "FALSE", true)]
    #[case("skip_signature", SECRET, false)]
    #[case("AZURE_SKIP_SIGNATURE", "off", true)]
    #[cfg_attr(
        miri,
        ignore = "Safe Azure client construction; no unsafe FFI exercised"
    )]
    fn auth_bypass_options_require_false(
        #[case] key: &str,
        #[case] value: &str,
        #[case] accepted: bool,
    ) {
        let result = build_store("az://container/table/", &[(key, value)]);
        assert_eq!(result.is_ok(), accepted);
        if let Err(error) = result {
            assert!(error.to_string().contains("to be false"));
            assert!(!error.to_string().contains(value));
        }
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

    #[test]
    #[cfg_attr(
        miri,
        ignore = "Safe Azure client construction; no unsafe FFI exercised"
    )]
    fn credential_type_follows_native_config_key_support() {
        let result = build_store(
            "az://container/table/",
            &[("AZURE_CREDENTIAL_TYPE", SECRET)],
        );
        #[cfg(feature = "arrow-60")]
        {
            let error = result.unwrap_err();
            assert!(error
                .to_string()
                .contains("conflicts with azure_credential_type"));
            assert!(!error.to_string().contains(SECRET));
        }
        #[cfg(not(feature = "arrow-60"))]
        assert!(result.is_ok());
    }
}
