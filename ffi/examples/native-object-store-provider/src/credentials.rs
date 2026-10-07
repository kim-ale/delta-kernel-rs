use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use object_store::azure::AzureCredential;
use object_store::CredentialProvider;

static CREDENTIAL_REQUESTS: AtomicU64 = AtomicU64::new(0);
static MAX_GENERATION: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct CustomCredentialProvider;

#[async_trait]
impl CredentialProvider for CustomCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let calls = CREDENTIAL_REQUESTS.fetch_add(1, Ordering::SeqCst);
        let generation = calls / 2 + 1;
        MAX_GENERATION.fetch_max(generation, Ordering::SeqCst);
        Ok(Arc::new(AzureCredential::BearerToken(format!(
            "native-token-{generation}"
        ))))
    }
}

pub(crate) fn request_count() -> u64 {
    CREDENTIAL_REQUESTS.load(Ordering::SeqCst)
}

pub(crate) fn generation() -> u64 {
    MAX_GENERATION.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_provider_automatically_rotates_bearer_tokens_every_two_retrievals() {
        let provider = CustomCredentialProvider;
        let baseline = request_count();
        for offset in 0..5 {
            let credential = provider.get_credential().await.unwrap();
            let expected = (baseline + offset) / 2 + 1;
            assert_eq!(
                *credential,
                AzureCredential::BearerToken(format!("native-token-{expected}"))
            );
        }
        assert_eq!(request_count() - baseline, 5);
        assert_eq!(generation(), (baseline + 4) / 2 + 1);
    }
}
