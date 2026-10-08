use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{MultipartUpload, PutPayload, PutResult, Result, UploadPart};
use reqwest::header::HeaderMap;
use reqwest::Method;

use super::{
    ensure, error, insert_header, put_result, unsupported, xml, AzureBlobRestStore, MAX_PART_SIZE,
};

#[derive(Debug, PartialEq)]
enum PartState {
    Pending,
    Success,
    Failed,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Open,
    Publishing,
    Closed,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    parts: Vec<(String, PartState)>,
}

#[derive(Debug)]
pub(super) struct BlobUpload {
    store: AzureBlobRestStore,
    location: Path,
    seed: [u8; 16],
    state: Arc<Mutex<State>>,
}

impl BlobUpload {
    pub(super) fn new(store: AzureBlobRestStore, location: Path) -> Result<Self> {
        store.object_url(&location)?;
        Ok(Self {
            store,
            location,
            seed: *uuid::Uuid::new_v4().as_bytes(),
            state: Arc::new(Mutex::new(State {
                phase: Phase::Open,
                parts: Vec::new(),
            })),
        })
    }
}

impl Drop for BlobUpload {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.phase = Phase::Closed;
        }
    }
}

#[async_trait]
impl MultipartUpload for BlobUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let registration = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|_| error("multipart state poisoned"))?;
            if state.phase != Phase::Open {
                return Err(error("multipart upload is not open"));
            }
            if state.parts.len() >= 50_000 {
                state.phase = Phase::Closed;
                return Err(unsupported("more than 50000 Blob blocks"));
            }
            let index = state.parts.len();
            let mut bytes = [0u8; 24];
            bytes[..16].copy_from_slice(&self.seed);
            bytes[16..].copy_from_slice(&(index as u64).to_be_bytes());
            let id = base64::engine::general_purpose::STANDARD.encode(bytes);
            state.parts.push((id.clone(), PartState::Pending));
            Ok((index, id))
        })();
        let store = self.store.clone();
        let location = self.location.clone();
        let state = self.state.clone();
        Box::pin(async move {
            let (index, id) = registration?;
            let result = async {
                if data.content_length() > MAX_PART_SIZE {
                    return Err(unsupported("Blob block larger than 64MiB"));
                }
                if state
                    .lock()
                    .map_err(|_| error("multipart state poisoned"))?
                    .phase
                    != Phase::Open
                {
                    return Err(error("multipart upload was aborted or closed"));
                }
                let mut url = store.object_url(&location)?;
                url.query_pairs_mut()
                    .append_pair("comp", "block")
                    .append_pair("blockid", &id);
                ensure(
                    store
                        .send(Method::PUT, url, HeaderMap::new(), Some(data.into()), true)
                        .await?,
                    location.as_ref(),
                    false,
                )?;
                Ok(())
            }
            .await;
            let mut state = state
                .lock()
                .map_err(|_| error("multipart state poisoned"))?;
            state.parts[index].1 = if result.is_ok() {
                PartState::Success
            } else {
                PartState::Failed
            };
            if state.phase == Phase::Closed {
                return Err(error("multipart upload was aborted or closed"));
            }
            result
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let ids = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| error("multipart state poisoned"))?;
            if state.phase != Phase::Open {
                return Err(error("multipart upload is not open"));
            }
            if state
                .parts
                .iter()
                .any(|(_, part)| *part != PartState::Success)
            {
                return Err(error(
                    "multipart publication requires every invoked part to succeed",
                ));
            }
            state.phase = Phase::Publishing;
            state
                .parts
                .iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        let result = async {
            let body = xml::block_list(&ids)?;
            let mut url = self.store.object_url(&self.location)?;
            url.query_pairs_mut().append_pair("comp", "blocklist");
            let mut protocol = HeaderMap::new();
            insert_header(&mut protocol, "content-type", "application/xml")?;
            let response = ensure(
                self.store
                    .send(Method::PUT, url, protocol, Some(body.into()), true)
                    .await?,
                self.location.as_ref(),
                false,
            )?;
            put_result(response.headers())
        }
        .await;
        self.state
            .lock()
            .map_err(|_| error("multipart state poisoned"))?
            .phase = Phase::Closed;
        result
    }

    async fn abort(&mut self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| error("multipart state poisoned"))?
            .phase = Phase::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_store::StaticHeaderProvider;

    #[test]
    #[cfg_attr(miri, ignore = "constructs reqwest/rustls client; no unsafe")]
    fn azure_blob_rejected_block_count_prevents_partial_publication() {
        let store = AzureBlobRestStore::new(
            url::Url::parse("https://example.test/container?sig=test").unwrap(),
            reqwest::Client::new(),
            Arc::new(StaticHeaderProvider::new(HeaderMap::new())),
        )
        .unwrap();
        let mut upload = BlobUpload::new(store, Path::from("table/a")).unwrap();
        upload.state.lock().unwrap().parts = (0..50_000)
            .map(|_| (String::new(), PartState::Success))
            .collect();
        assert!(futures::executor::block_on(upload.put_part(bytes::Bytes::new().into())).is_err());
        assert!(futures::executor::block_on(upload.complete()).is_err());
    }
}
