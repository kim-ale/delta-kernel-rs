use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::Snapshot;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::executor::tokio::TokioMultiThreadExecutor;
use crate::DefaultEngineBuilder;

const MODIFIED: &str = "Wed, 07 Oct 2026 10:00:00 GMT";

#[rstest::rstest]
#[case("*")]
#[case("\"old\", \"other\"")]
#[tokio::test]
async fn azure_blob_suffix_pins_head_version_under_broad_caller_condition(#[case] condition: &str) {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/container/blob"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "10")
                .insert_header("etag", "\"old\""),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/container/blob"))
        .and(wiremock::matchers::header("if-match", "\"old\""))
        .and(wiremock::matchers::header("range", "bytes=8-9"))
        .respond_with(
            ResponseTemplate::new(412).insert_header("x-ms-error-code", "ConditionNotMet"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let auth = super::super::StaticHeaderProvider::from_pairs([(
        "authorization".into(),
        "Bearer ready-token".into(),
    )])
    .unwrap();
    let store = AzureBlobRestStore::new(
        Url::parse(&format!("{}/container", server.uri())).unwrap(),
        Client::new(),
        Arc::new(auth),
    )
    .unwrap();
    let result = store
        .get_opts(
            &Path::from("blob"),
            GetOptions {
                if_match: Some(condition.into()),
                range: Some(GetRange::Suffix(2)),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(Error::Precondition { .. })));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].headers["if-match"].to_str().unwrap(), condition);
}

#[derive(Default)]
struct BlobState {
    objects: BTreeMap<String, Bytes>,
    blocks: HashMap<(String, String), Bytes>,
    operations: Vec<(String, String, String, usize)>,
}

#[derive(Clone, Default)]
struct BlobService(Arc<Mutex<BlobState>>);

#[derive(Serialize)]
#[serde(rename = "EnumerationResults")]
struct ServiceListing {
    #[serde(rename = "Blobs")]
    blobs: ServiceBlobs,
    #[serde(rename = "NextMarker")]
    marker: String,
}
#[derive(Serialize)]
struct ServiceBlobs {
    #[serde(rename = "Blob")]
    blobs: Vec<ServiceBlob>,
}
#[derive(Serialize)]
struct ServiceBlob {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Properties")]
    properties: ServiceProperties,
}
#[derive(Serialize)]
struct ServiceProperties {
    #[serde(rename = "Content-Length")]
    size: usize,
    #[serde(rename = "Last-Modified")]
    modified: &'static str,
    #[serde(rename = "Etag")]
    etag: &'static str,
}
#[derive(Deserialize)]
struct PublishedBlocks {
    #[serde(rename = "Latest", default)]
    ids: Vec<String>,
}

impl Respond for BlobService {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let query: HashMap<_, _> = request
            .url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let path = Path::from_url_path(request.url.path()).unwrap().to_string();
        let path = path.strip_prefix("container/").unwrap_or(&path).to_string();
        let comp = query.get("comp").cloned().unwrap_or_default();
        assert!(request.headers.contains_key("authorization") || query.contains_key("sig"));
        assert_eq!(request.headers.get("x-ms-version").unwrap(), "2021-12-02");
        let mut state = self.0.lock().unwrap();
        state.operations.push((
            request.method.to_string(),
            path.clone(),
            comp.clone(),
            request.body.len(),
        ));
        match (request.method.as_str(), comp.as_str()) {
            ("GET", "list") => {
                assert_eq!(request.url.path(), "/container");
                assert_eq!(query.get("restype").unwrap(), "container");
                let prefix = query.get("prefix").unwrap();
                let blobs = state
                    .objects
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .map(|(key, bytes)| ServiceBlob {
                        name: key.clone(),
                        properties: ServiceProperties {
                            size: bytes.len(),
                            modified: MODIFIED,
                            etag: "\"stored\"",
                        },
                    })
                    .collect();
                let xml = quick_xml::se::to_string(&ServiceListing {
                    blobs: ServiceBlobs { blobs },
                    marker: String::new(),
                })
                .unwrap();
                ResponseTemplate::new(200).set_body_raw(xml, "application/xml")
            }
            ("PUT", "block") => {
                state.blocks.insert(
                    (path, query.get("blockid").unwrap().clone()),
                    request.body.clone().into(),
                );
                ResponseTemplate::new(201)
            }
            ("PUT", "blocklist") => {
                assert_eq!(
                    request.headers.get("content-type").unwrap(),
                    "application/xml"
                );
                let blocklist: PublishedBlocks =
                    quick_xml::de::from_reader(request.body.as_slice()).unwrap();
                let mut body = Vec::new();
                for id in blocklist.ids {
                    body.extend_from_slice(state.blocks.get(&(path.clone(), id)).unwrap());
                }
                state.objects.insert(path, body.into());
                ResponseTemplate::new(201)
                    .insert_header("etag", "\"stored\"")
                    .insert_header("x-ms-version-id", "version-1")
            }
            ("PUT", "") => {
                assert_eq!(request.headers.get("x-ms-blob-type").unwrap(), "BlockBlob");
                if request
                    .headers
                    .get("if-none-match")
                    .is_some_and(|value| value == "*")
                    && state.objects.contains_key(&path)
                {
                    return ResponseTemplate::new(412)
                        .insert_header("x-ms-error-code", "ConditionNotMet");
                }
                state.objects.insert(path, request.body.clone().into());
                ResponseTemplate::new(201).insert_header("etag", "\"stored\"")
            }
            ("GET" | "HEAD", "") => {
                let Some(body) = state.objects.get(&path) else {
                    return ResponseTemplate::new(404);
                };
                if request
                    .headers
                    .get("if-none-match")
                    .is_some_and(|value| value == "\"stored\"")
                {
                    return ResponseTemplate::new(304);
                }
                if request
                    .headers
                    .get("if-match")
                    .is_some_and(|value| value != "\"stored\"" && value != "*")
                {
                    return ResponseTemplate::new(412);
                }
                let mut response = ResponseTemplate::new(200)
                    .insert_header("last-modified", MODIFIED)
                    .insert_header("etag", "\"stored\"");
                if request.method == "HEAD" {
                    return response.insert_header("content-length", body.len().to_string());
                }
                if let Some(range) = request.headers.get("range") {
                    let (start, end) = range
                        .to_str()
                        .unwrap()
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let start: usize = start.parse().unwrap();
                    let end = if end.is_empty() {
                        body.len()
                    } else {
                        (end.parse::<usize>().unwrap() + 1).min(body.len())
                    };
                    if start >= body.len() {
                        return ResponseTemplate::new(416);
                    }
                    response = ResponseTemplate::new(206)
                        .insert_header("last-modified", MODIFIED)
                        .insert_header("etag", "\"stored\"")
                        .insert_header(
                            "content-range",
                            format!("bytes {start}-{}/{}", end - 1, body.len()),
                        );
                    return response.set_body_bytes(body.slice(start..end).to_vec());
                }
                response.set_body_bytes(body.to_vec())
            }
            ("DELETE", "") => {
                if state.objects.remove(&path).is_some() {
                    ResponseTemplate::new(202)
                } else {
                    ResponseTemplate::new(404)
                }
            }
            _ => ResponseTemplate::new(400),
        }
    }
}

async fn stateful_store() -> (MockServer, AzureBlobRestStore, BlobService) {
    let server = MockServer::start().await;
    let service = BlobService::default();
    Mock::given(wiremock::matchers::any())
        .respond_with(service.clone())
        .mount(&server)
        .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let auth = super::super::RefreshingHeaderProvider::new(move || {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        Ok((
            super::super::headers_from_pairs([
                ("authorization".into(), format!("Bearer token-{call}")),
                ("x-ms-fabric-actor".into(), format!("actor-{call}")),
            ])?,
            None,
        ))
    });
    let store = AzureBlobRestStore::new(
        Url::parse(&format!("{}/container", server.uri())).unwrap(),
        Client::new(),
        Arc::new(auth),
    )
    .unwrap();
    (server, store, service)
}

fn entropy_property(size: usize) -> String {
    let mut seed = 0x27a916cb486d053fu64;
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    (0..size)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            ALPHABET[(seed & 63) as usize] as char
        })
        .collect()
}

#[rstest::rstest]
#[case::small(128, false)]
#[case::large(24 * 1024 * 1024, true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    miri,
    ignore = "local HTTP server and real parquet serialization; no unsafe"
)]
async fn azure_blob_real_kernel_commit_checkpoint_and_reload(
    #[case] property_size: usize,
    #[case] multipart: bool,
) {
    let (server, store, service) = stateful_store().await;
    let store = Arc::new(store);
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );
    let property = entropy_property(property_size);
    let expected = property.clone();
    let engine_for_write = engine.clone();
    let snapshot = tokio::task::spawn_blocking(move || {
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("id", DataType::INTEGER)]).unwrap(),
        );
        create_table("az://container/table/", schema, "BlobRestTest")
            .with_table_properties([("test.entropy", property)])
            .build(
                engine_for_write.as_ref(),
                Box::new(FileSystemCommitter::new()),
            )
            .unwrap()
            .commit(engine_for_write.as_ref())
            .unwrap()
            .unwrap_committed();
        let snapshot = Snapshot::builder_for("az://container/table/")
            .build(engine_for_write.as_ref())
            .unwrap();
        snapshot
            .checkpoint(engine_for_write.as_ref(), None)
            .unwrap();
        snapshot
    })
    .await
    .unwrap();
    assert_eq!(snapshot.version(), 0);
    let checkpoint = {
        let state = service.0.lock().unwrap();
        state
            .objects
            .iter()
            .find(|(key, _)| key.ends_with(".checkpoint.parquet"))
            .map(|(key, body)| (key.clone(), body.len()))
            .unwrap()
    };
    assert_eq!(
        checkpoint.1 > 10 * 1024 * 1024,
        multipart,
        "actual serialized checkpoint size: {}",
        checkpoint.1
    );
    {
        let state = service.0.lock().unwrap();
        let blocks = state
            .operations
            .iter()
            .filter(|(_, _, comp, _)| comp == "block")
            .count();
        let publishes = state
            .operations
            .iter()
            .filter(|(_, _, comp, _)| comp == "blocklist")
            .count();
        assert_eq!(blocks > 0, multipart);
        assert_eq!(publishes, usize::from(multipart));
        assert!(state
            .operations
            .iter()
            .all(|(_, path, _, _)| !path.starts_with("container/table")));
        println!("Blob checkpoint evidence: serialized={} bytes, blocks={blocks}, blocklists={publishes}", checkpoint.1);
    }
    store
        .delete(&Path::from("table/_delta_log/00000000000000000000.json"))
        .await
        .unwrap();
    let engine_for_reload = engine.clone();
    let reloaded = tokio::task::spawn_blocking(move || {
        Snapshot::builder_for("az://container/table/")
            .build(engine_for_reload.as_ref())
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(reloaded.version(), 0);
    assert_eq!(
        reloaded
            .table_configuration()
            .metadata()
            .configuration()
            .get("test.entropy"),
        Some(&expected)
    );
    let requests = server.received_requests().await.unwrap();
    let mut tokens = HashSet::new();
    for request in &requests {
        let index = request
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("Bearer token-")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert!(tokens.insert(index));
        assert_eq!(
            request.headers.get("x-ms-fabric-actor").unwrap(),
            &format!("actor-{index}")
        );
    }
    assert_eq!(tokens, (0..requests.len()).collect());
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_concurrent_create_is_atomic_and_read_delete_roundtrip() {
    let (_server, store, service) = stateful_store().await;
    let location = Path::parse("table/percent% snow \u{96ea} ?#").unwrap();
    let (first, second) = tokio::join!(
        store.put_opts(&location, "first".into(), PutMode::Create.into()),
        store.put_opts(&location, "second".into(), PutMode::Create.into())
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(Error::AlreadyExists { .. })
    ));
    let head = store.head(&location).await.unwrap();
    assert_eq!(head.e_tag.as_deref(), Some("\"stored\""));
    let body = store.get(&location).await.unwrap().bytes().await.unwrap();
    let suffix = store
        .get_opts(
            &location,
            GetOptions {
                range: Some(GetRange::Suffix(2)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(suffix, body.slice(body.len() - 2..));
    store.delete(&location).await.unwrap();
    assert!(service.0.lock().unwrap().objects.is_empty());
}

fn store_for(server: &MockServer) -> AzureBlobRestStore {
    AzureBlobRestStore::new(
        Url::parse(&format!("{}/container?sig=test", server.uri())).unwrap(),
        Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        Arc::new(super::super::StaticHeaderProvider::new(HeaderMap::new())),
    )
    .unwrap()
}

#[rstest::rstest]
#[case(206, "bytes 2-4/10", true)]
#[case(200, "bytes 2-4/10", false)]
#[case(206, "bytes 2-5/10", false)]
#[case(206, "bytes 2-4/4", false)]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_range_requires_exact_partial_response(
    #[case] status: u16,
    #[case] range: &str,
    #[case] success: bool,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::header("range", "bytes=2-4"))
        .respond_with(
            ResponseTemplate::new(status)
                .insert_header("content-range", range)
                .insert_header("etag", "\"server\"")
                .set_body_bytes(b"abc".as_slice()),
        )
        .mount(&server)
        .await;
    let result = store_for(&server)
        .get_opts(
            &Path::from("table/a"),
            GetOptions {
                range: Some(GetRange::Bounded(2..5)),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(result.is_ok(), success);
    if let Ok(result) = result {
        assert_eq!(result.meta.size, 10);
        assert_eq!(result.meta.e_tag.as_deref(), Some("\"server\""));
    }
}

#[rstest::rstest]
#[case(409, "BlobAlreadyExists", true)]
#[case(412, "ConditionNotMet", true)]
#[case(409, "LeaseAlreadyPresent", false)]
#[case(302, "", false)]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_create_maps_only_blob_conflicts(
    #[case] status: u16,
    #[case] code: &str,
    #[case] conflict: bool,
) {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(wiremock::matchers::header("x-ms-blob-type", "BlockBlob"))
        .and(wiremock::matchers::header("if-none-match", "*"))
        .respond_with(ResponseTemplate::new(status).insert_header("x-ms-error-code", code))
        .expect(1)
        .mount(&server)
        .await;
    let result = store_for(&server)
        .with_max_retries(2)
        .put_opts(
            &Path::from("table/a"),
            "body".into(),
            PutMode::Create.into(),
        )
        .await;
    assert_eq!(matches!(result, Err(Error::AlreadyExists { .. })), conflict);
    assert!(result.is_err());
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_container_listing_preserves_sas_prefix_and_short_page_marker() {
    let server = MockServer::start().await;
    let page = |name: &str, marker: &str| {
        format!("<EnumerationResults><Blobs><Blob><Name>{name}</Name><Properties><Content-Length>4</Content-Length><Last-Modified>Wed, 07 Oct 2026 10:00:00 GMT</Last-Modified><Etag>&quot;etag&quot;</Etag></Properties></Blob></Blobs><NextMarker>{marker}</NextMarker></EnumerationResults>")
    };
    Mock::given(method("GET"))
        .and(path("/container"))
        .and(query_param("comp", "list"))
        .and(query_param("prefix", "table/"))
        .and(query_param("sig", "a+b/c="))
        .respond_with(ResponseTemplate::new(200).set_body_string(page("table/a% ?#", "next+token")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(query_param("marker", "next+token"))
        .and(query_param("sig", "a+b/c="))
        .respond_with(ResponseTemplate::new(200).set_body_string(page("table/z", "")))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    let store = AzureBlobRestStore::new(
        Url::parse(&format!("{}/container/?sig=a%2Bb%2Fc%3D", server.uri())).unwrap(),
        Client::new(),
        Arc::new(super::super::StaticHeaderProvider::new(HeaderMap::new())),
    )
    .unwrap();
    let objects: Vec<_> = store
        .list(Some(&Path::from("table/")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0].location.as_ref(), "table/a% ?#");
    assert_eq!(objects[0].e_tag.as_deref(), Some("\"etag\""));
    assert!(!format!("{store:?} {store}").contains("a+b"));
    assert_eq!(
        store
            .object_url(&Path::parse("table/a% ?#").unwrap())
            .unwrap()
            .path(),
        "/container/table/a%25%20%3F%23"
    );
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_multipart_preserves_invocation_order_and_abort_never_deletes() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(query_param("comp", "block"))
        .respond_with(ResponseTemplate::new(201))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(query_param("comp", "blocklist"))
        .respond_with(ResponseTemplate::new(201).insert_header("etag", "\"published\""))
        .expect(1)
        .mount(&server)
        .await;
    let store = store_for(&server);
    let mut upload = store.put_multipart(&Path::from("table/a")).await.unwrap();
    let first = upload.put_part("first".into());
    let second = upload.put_part("second".into());
    assert!(upload.complete().await.is_err());
    second.await.unwrap();
    first.await.unwrap();
    let result = upload.complete().await.unwrap();
    assert_eq!(result.e_tag.as_deref(), Some("\"published\""));
    assert!(upload.complete().await.is_err());
    let requests = server.received_requests().await.unwrap();
    let ids = requests
        .iter()
        .filter(|request| {
            request
                .url
                .query_pairs()
                .any(|(key, value)| key == "comp" && value == "block")
        })
        .map(|request| {
            request
                .url
                .query_pairs()
                .find(|(key, _)| key == "blockid")
                .unwrap()
                .1
                .into_owned()
        })
        .collect::<Vec<_>>();
    let expected = xml::block_list(&[ids[1].clone(), ids[0].clone()]).unwrap();
    assert_eq!(
        String::from_utf8(requests.last().unwrap().body.clone()).unwrap(),
        expected
    );
    assert_eq!(ids[0].len(), ids[1].len());
    let mut aborted = store.put_multipart(&Path::from("table/a")).await.unwrap();
    let pending = aborted.put_part("ignored".into());
    aborted.abort().await.unwrap();
    assert!(pending.await.is_err());
    assert!(aborted.complete().await.is_err());
    assert!(server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .all(|request| request.method != "DELETE"));
}

#[tokio::test]
async fn azure_blob_multipart_concurrent_parts_publish_in_invocation_order() {
    let (_server, store, service) = stateful_store().await;
    let location = Path::from("table/concurrent");
    let mut upload = store.put_multipart(&location).await.unwrap();
    let first = upload.put_part("first".into());
    let second = upload.put_part("second".into());
    tokio::try_join!(second, first).unwrap();
    upload.complete().await.unwrap();
    assert_eq!(
        service.0.lock().unwrap().objects[location.as_ref()].as_ref(),
        b"firstsecond"
    );
}

#[tokio::test]
async fn azure_blob_multipart_cancelled_inflight_part_cannot_publish() {
    let server = MockServer::start().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    Mock::given(method("PUT"))
        .and(query_param("comp", "block"))
        .respond_with(move |_: &Request| {
            notify.notify_one();
            ResponseTemplate::new(201).set_delay(std::time::Duration::from_secs(60))
        })
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(query_param("comp", "blocklist"))
        .respond_with(ResponseTemplate::new(201))
        .expect(0)
        .mount(&server)
        .await;
    let store = store_for(&server);
    let mut upload = store
        .put_multipart(&Path::from("table/cancelled"))
        .await
        .unwrap();
    let part = tokio::spawn(upload.put_part("pending".into()));
    started.notified().await;
    part.abort();
    assert!(part.await.unwrap_err().is_cancelled());
    assert!(upload.complete().await.is_err());
    upload.abort().await.unwrap();
    assert!(upload.complete().await.is_err());
}

#[rstest::rstest]
#[case("azure_storage_account_name", "account")]
#[case("retry.max_retries", "nope")]
#[case("tls.timeout_secs", "nope")]
#[case("put.verify_on_ambiguous", "true")]
#[case("put.verify_on_ambiguous", "nope")]
#[test]
fn azure_blob_factory_rejects_unsupported_or_invalid_options(
    #[case] key: &str,
    #[case] value: &str,
) {
    let options = HashMap::from([(key.to_string(), value.to_string())]);
    assert!(build_azure_blob_rest_store(
        &Url::parse("https://example.test/container?sig=secret").unwrap(),
        &options,
        None
    )
    .is_err());
}

#[rstest::rstest]
#[case("<EnumerationResults><Blobs/></EnumerationResults>", true)]
#[case("<EnumerationResults/>", false)]
#[case("<Wrong><Blobs/></Wrong>", false)]
#[case("<EnumerationResults><Blobs/></EnumerationResults><Wrong/>", false)]
#[case("<EnumerationResults><Blobs>", false)]
#[case("<EnumerationResults><Blobs><Blob><Name Encoded=\"true\">x%2Fy</Name><Properties><Content-Length>1</Content-Length><Last-Modified>Wed, 07 Oct 2026 10:00:00 GMT</Last-Modified><Etag>e</Etag></Properties></Blob></Blobs></EnumerationResults>", false)]
#[case("<EnumerationResults><Blobs><Blob><Name>x</Name><Properties><Content-Length>nope</Content-Length><Last-Modified>Wed, 07 Oct 2026 10:00:00 GMT</Last-Modified><Etag>e</Etag></Properties></Blob></Blobs></EnumerationResults>", false)]
#[case("<EnumerationResults><Blobs><Blob><Name>x</Name><Properties><Content-Length>1</Content-Length><Last-Modified>nope</Last-Modified><Etag>e</Etag></Properties></Blob></Blobs></EnumerationResults>", false)]
#[test]
fn azure_blob_xml_requires_valid_schema_and_metadata(#[case] body: &str, #[case] valid: bool) {
    assert_eq!(xml::parse_list(body.as_bytes()).is_ok(), valid);
}

#[rstest::rstest]
#[case("https://example.test/")]
#[case("https://user:secret@example.test/container")]
#[case("https://example.test/container/table")]
#[case("https://example.test//container")]
#[case("https://example.test/container//")]
#[case("https://example.test/container?comp=list&sig=secret")]
#[case("https://example.test/container?sig=secret&sig=other")]
#[case("https://example.test/container#secret")]
#[test]
fn azure_blob_invalid_container_errors_never_echo_secrets(#[case] value: &str) {
    let error = validate_container_url(Url::parse(value).unwrap()).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("secret"));
}

#[rstest::rstest]
#[case("host", "evil")]
#[case("content-length", "0")]
#[case("connection", "x-actor")]
#[case("if-none-match", "*")]
#[case("range", "bytes=0-1")]
#[case("x-ms-version", "bad")]
#[case("x-ms-blob-type", "PageBlob")]
#[case("x-ms-if-tags", "secret")]
#[case("authorization", "SharedKey account:secret")]
#[case("authorization", "Basic secret")]
#[case("authorization", "Bearer ")]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_reserved_headers_and_non_bearer_auth_fail_before_dispatch(
    #[case] name: &str,
    #[case] value: &str,
) {
    let server = MockServer::start().await;
    let auth =
        super::super::StaticHeaderProvider::from_pairs([(name.into(), value.into())]).unwrap();
    let store = AzureBlobRestStore::new(
        Url::parse(&format!("{}/container?sig=test", server.uri())).unwrap(),
        Client::new(),
        Arc::new(auth),
    )
    .unwrap();
    let error = store.head(&Path::from("table/a")).await.unwrap_err();
    assert!(!error.to_string().contains("secret"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_missing_auth_and_provider_failure_never_dispatch() {
    let server = MockServer::start().await;
    let url = Url::parse(&format!("{}/container", server.uri())).unwrap();
    let store = AzureBlobRestStore::new(
        url.clone(),
        Client::new(),
        Arc::new(super::super::StaticHeaderProvider::new(HeaderMap::new())),
    )
    .unwrap();
    assert!(store.head(&Path::from("table/a")).await.is_err());
    let auth = super::super::RefreshingHeaderProvider::new(|| Err(error("producer secret")));
    let store = AzureBlobRestStore::new(url, Client::new(), Arc::new(auth)).unwrap();
    let error = store.head(&Path::from("table/a")).await.unwrap_err();
    assert!(!error.to_string().contains("secret"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[rstest::rstest]
#[case(304, true)]
#[case(412, false)]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_get_sends_conditions_and_version_and_maps_status(
    #[case] status: u16,
    #[case] not_modified: bool,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("versionid", "v+1"))
        .and(wiremock::matchers::header("if-match", "\"etag\""))
        .and(|request: &Request| {
            request
                .headers
                .get("if-unmodified-since")
                .is_some_and(|value| value == MODIFIED)
                && request
                    .headers
                    .get("if-modified-since")
                    .is_some_and(|value| value == MODIFIED)
        })
        .and(wiremock::matchers::header("if-none-match", "\"other\""))
        .respond_with(ResponseTemplate::new(status))
        .expect(1)
        .mount(&server)
        .await;
    let date = chrono::DateTime::parse_from_rfc2822(MODIFIED)
        .unwrap()
        .with_timezone(&chrono::Utc);
    let result = store_for(&server)
        .get_opts(
            &Path::from("table/a"),
            GetOptions {
                if_match: Some("\"etag\"".into()),
                if_none_match: Some("\"other\"".into()),
                if_modified_since: Some(date),
                if_unmodified_since: Some(date),
                version: Some("v+1".into()),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        matches!(result, Err(Error::NotModified { .. })),
        not_modified
    );
    assert_eq!(
        matches!(result, Err(Error::Precondition { .. })),
        !not_modified
    );
}

#[rstest::rstest]
#[case(None, None, false)]
#[case(Some("nope"), None, false)]
#[case(Some("5"), Some("not-a-date"), false)]
#[case(Some("5"), Some(MODIFIED), true)]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_head_rejects_missing_length_or_invalid_metadata(
    #[case] length: Option<&str>,
    #[case] modified: Option<&str>,
    #[case] valid: bool,
) {
    let server = MockServer::start().await;
    let mut response = ResponseTemplate::new(200);
    if let Some(length) = length {
        response = response.insert_header("content-length", length);
    }
    if let Some(modified) = modified {
        response = response.insert_header("last-modified", modified);
    }
    Mock::given(method("HEAD"))
        .respond_with(response)
        .mount(&server)
        .await;
    assert_eq!(
        store_for(&server)
            .head(&Path::from("table/a"))
            .await
            .is_ok(),
        valid
    );
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_listing_detects_marker_cycle_and_filters_recursive_offset() {
    let server = MockServer::start().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let count = counter.clone();
    Mock::given(method("GET")).respond_with(move |_: &Request| {
        let index = count.fetch_add(1, Ordering::SeqCst);
        let marker = ["a", "b", "a"][index.min(2)];
        ResponseTemplate::new(200).set_body_string(format!("<EnumerationResults><Blobs/><NextMarker>{marker}</NextMarker></EnumerationResults>"))
    }).mount(&server).await;
    assert!(store_for(&server)
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .is_err());
    assert_eq!(counter.load(Ordering::SeqCst), 3);
    let (_server, store, service) = stateful_store().await;
    service.0.lock().unwrap().objects.extend([
        ("table/a".into(), Bytes::new()),
        ("table/nested/b".into(), Bytes::new()),
        ("table/z".into(), Bytes::new()),
        ("tablesibling/a".into(), Bytes::new()),
    ]);
    let entries = store
        .list_with_offset(Some(&Path::from("table")), &Path::from("table/a"))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|meta| meta.location.as_ref())
            .collect::<Vec<_>>(),
        ["table/nested/b", "table/z"]
    );
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_retry_refreshes_bearer_and_actor_and_create_never_retries() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    Mock::given(method("GET"))
        .respond_with(move |_: &Request| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503)
            } else {
                ResponseTemplate::new(200).set_body_bytes(b"ok".as_slice())
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let auth = super::super::RefreshingHeaderProvider::new(move || {
        let index = count.fetch_add(1, Ordering::SeqCst);
        Ok((
            super::super::headers_from_pairs([
                ("authorization".into(), format!("Bearer token-{index}")),
                ("x-fabric-actor".into(), format!("actor-{index}")),
            ])?,
            None,
        ))
    });
    let store = AzureBlobRestStore::new(
        Url::parse(&format!("{}/container", server.uri())).unwrap(),
        Client::new(),
        Arc::new(auth),
    )
    .unwrap()
    .with_max_retries(2);
    assert_eq!(
        store
            .get(&Path::from("table/a"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "ok"
    );
    assert!(store
        .put_opts(
            &Path::from("table/a"),
            "body".into(),
            PutMode::Create.into()
        )
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    for (index, request) in server.received_requests().await.unwrap().iter().enumerate() {
        assert_eq!(
            request.headers.get("authorization").unwrap(),
            &format!("Bearer token-{index}")
        );
        assert_eq!(
            request.headers.get("x-fabric-actor").unwrap(),
            &format!("actor-{index}")
        );
    }
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_failed_or_dropped_parts_never_publish() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(query_param("comp", "block"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let store = store_for(&server);
    let mut upload = store.put_multipart(&Path::from("table/a")).await.unwrap();
    assert!(upload.put_part("failed".into()).await.is_err());
    assert!(upload.complete().await.is_err());
    upload.abort().await.unwrap();
    let mut dropped = store.put_multipart(&Path::from("table/a")).await.unwrap();
    let pending = dropped.put_part("pending".into());
    drop(dropped);
    assert!(pending.await.is_err());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[rstest::rstest]
#[case(0, 0)]
#[case(0, 1)]
#[case(5, 0)]
#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_empty_suffix_is_an_error_without_get(#[case] size: u64, #[case] suffix: u64) {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", size.to_string())
                .insert_header("etag", "\"stored\""),
        )
        .expect(1)
        .mount(&server)
        .await;
    assert!(store_for(&server)
        .get_opts(
            &Path::from("table/a"),
            GetOptions {
                range: Some(GetRange::Suffix(suffix)),
                ..Default::default()
            }
        )
        .await
        .is_err());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
#[cfg_attr(
    miri,
    ignore = "local HTTP server is unavailable under Miri; no unsafe"
)]
async fn azure_blob_sas_and_context_are_preserved_on_every_operation() {
    let server = MockServer::start().await;
    let service = BlobService::default();
    Mock::given(wiremock::matchers::any())
        .respond_with(service)
        .mount(&server)
        .await;
    let auth = super::super::StaticHeaderProvider::from_pairs([(
        "x-fabric-actor".into(),
        "context".into(),
    )])
    .unwrap();
    let store = AzureBlobRestStore::new(
        Url::parse(&format!(
            "{}/container/?sig=a%2Bb%2Fc%3D&sv=2021-12-02",
            server.uri()
        ))
        .unwrap(),
        Client::new(),
        Arc::new(auth),
    )
    .unwrap();
    let location = Path::parse("table/percent% \u{96ea} ?#").unwrap();
    store.put(&location, "original".into()).await.unwrap();
    assert_eq!(
        store.get(&location).await.unwrap().bytes().await.unwrap(),
        "original"
    );
    assert_eq!(store.head(&location).await.unwrap().size, 8);
    assert_eq!(
        store
            .list(Some(&Path::from("table")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );
    let mut upload = store.put_multipart(&location).await.unwrap();
    upload.put_part("part".into()).await.unwrap();
    let result = upload.complete().await.unwrap();
    assert_eq!(result.version.as_deref(), Some("version-1"));
    store.delete(&location).await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 7);
    for request in requests {
        assert_eq!(
            request
                .url
                .query_pairs()
                .find(|(key, _)| key == "sig")
                .unwrap()
                .1,
            "a+b/c="
        );
        assert_eq!(request.headers.get("x-fabric-actor").unwrap(), "context");
    }
}

#[rstest::rstest]
#[case::tags(0)]
#[case::attributes(1)]
#[case::extensions(2)]
#[test]
fn azure_blob_write_options_are_never_silently_dropped(#[case] kind: u8) {
    let mut options = PutOptions::default();
    match kind {
        0 => options.tags.push("key", "value"),
        1 => {
            options
                .attributes
                .insert(object_store::Attribute::ContentType, "text/plain".into());
        }
        2 => {
            options.extensions.insert(1u32);
        }
        _ => unreachable!(),
    }
    assert!(validate_write_options(
        &options.tags,
        &options.attributes,
        options.extensions.is_empty()
    )
    .is_err());
}
