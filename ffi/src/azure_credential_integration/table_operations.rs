use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::AtomicBool;

use delta_kernel::arrow::array::{Int64Array, StringArray};
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_changes::TableChanges;
use delta_kernel::Snapshot;
use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
use wiremock::{Request, Respond};

use super::*;
use crate::{checkpoint_snapshot, free_snapshot, FfiCheckpointWriteResult, SharedSnapshot};

const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
const BLOB_PREFIX: &str = "/prefix/container/";

#[derive(Clone, Default)]
struct AzureTable {
    blobs: Arc<Mutex<BTreeMap<String, Bytes>>>,
    first_list_delay_ms: u64,
    list_started: Arc<AtomicBool>,
}

impl AzureTable {
    fn generated(runtime: &Runtime, log_state: LogState) -> Self {
        let table = TestTableBuilder::new()
            .with_log_state(log_state)
            .with_features(FeatureSet::new().change_data_feed())
            .with_schema(Arc::new(
                StructType::try_new([StructField::nullable("id", DataType::LONG)]).unwrap(),
            ))
            .with_data(1, 3)
            .build()
            .unwrap();
        let blobs = runtime.block_on(async {
            let mut files = table.store().list(None);
            let mut blobs = BTreeMap::new();
            while let Some(file) =
                std::future::poll_fn(|context| files.as_mut().poll_next(context)).await
            {
                let file = file.unwrap();
                let bytes = table
                    .store()
                    .get(&file.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                blobs.insert(format!("table/{}", file.location), bytes);
            }
            blobs
        });
        Self {
            blobs: Arc::new(Mutex::new(blobs)),
            ..Self::default()
        }
    }

    fn metadata(status: u16, bytes: &Bytes) -> ResponseTemplate {
        ResponseTemplate::new(status)
            .insert_header("etag", format!("\"{}\"", bytes.len()))
            .insert_header("last-modified", LAST_MODIFIED)
            .insert_header("content-length", bytes.len().to_string())
    }

    fn missing() -> ResponseTemplate {
        ResponseTemplate::new(404)
            .insert_header("x-ms-error-code", "BlobNotFound")
            .set_body_string("<Error><Code>BlobNotFound</Code></Error>")
    }
}

impl Respond for AzureTable {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let authorization = request
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(matches!(authorization, "Bearer token-A" | "Bearer token-B"));
        let query: BTreeMap<_, _> = request.url.query_pairs().into_owned().collect();
        let mut blobs = self.blobs.lock().unwrap();
        if query.get("comp").map(String::as_str) == Some("list") {
            assert_eq!(request.method.as_str(), "GET");
            let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
            let start = query.get("startFrom").map(String::as_str).unwrap_or("");
            let entries = blobs
                .iter()
                .filter(|(name, _)| name.starts_with(prefix) && name.as_str() >= start)
                .map(|(name, bytes)| {
                    format!(
                        "<Blob><Name>{name}</Name><Properties><Last-Modified>{LAST_MODIFIED}</Last-Modified><Etag>\"{}\"</Etag><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType></Properties></Blob>",
                        bytes.len(), bytes.len()
                    )
                })
                .collect::<String>();
            let response = ResponseTemplate::new(200).set_body_string(format!(
                "<EnumerationResults><Blobs>{entries}</Blobs><NextMarker/></EnumerationResults>"
            ));
            return if !self.list_started.swap(true, Ordering::SeqCst) {
                response.set_delay(Duration::from_millis(self.first_list_delay_ms))
            } else {
                response
            };
        }
        let Some(key) = request.url.path().strip_prefix(BLOB_PREFIX) else {
            return Self::missing();
        };
        match request.method.as_str() {
            "PUT" => {
                if request.headers.get("if-none-match").is_some() && blobs.contains_key(key) {
                    return ResponseTemplate::new(412)
                        .insert_header("x-ms-error-code", "BlobAlreadyExists");
                }
                let bytes = Bytes::copy_from_slice(&request.body);
                let response = Self::metadata(201, &bytes);
                blobs.insert(key.to_owned(), bytes);
                response
            }
            "HEAD" | "GET" => {
                let Some(bytes) = blobs.get(key) else {
                    return Self::missing();
                };
                if request.method.as_str() == "HEAD" {
                    return Self::metadata(200, bytes);
                }
                let range = request
                    .headers
                    .get("range")
                    .or_else(|| request.headers.get("x-ms-range"));
                if let Some(range) = range {
                    let range = range.to_str().unwrap().strip_prefix("bytes=").unwrap();
                    let (start, end) = range.split_once('-').unwrap();
                    let start = start.parse::<usize>().unwrap();
                    let end = if end.is_empty() {
                        bytes.len() - 1
                    } else {
                        end.parse::<usize>().unwrap().min(bytes.len() - 1)
                    };
                    let body = bytes.slice(start..=end);
                    Self::metadata(206, &body)
                        .insert_header(
                            "content-range",
                            format!("bytes {start}-{end}/{}", bytes.len()),
                        )
                        .set_body_bytes(body.to_vec())
                } else {
                    Self::metadata(200, bytes).set_body_bytes(bytes.to_vec())
                }
            }
            verb => panic!("unexpected Azure table operation: {verb} {}", request.url),
        }
    }
}

fn mounted_table(runtime: &Runtime, log_state: LogState) -> (MockServer, AzureTable) {
    mounted_table_with_delay(runtime, log_state, 0)
}

fn mounted_table_with_delay(
    runtime: &Runtime,
    log_state: LogState,
    first_list_delay_ms: u64,
) -> (MockServer, AzureTable) {
    let mut table = AzureTable::generated(runtime, log_state);
    table.first_list_delay_ms = first_list_delay_ms;
    let server = runtime.block_on(MockServer::start());
    runtime.block_on(
        Mock::given(|_: &Request| true)
            .respond_with(table.clone())
            .mount(&server),
    );
    (server, table)
}

fn table_callback_counts() -> Arc<CallbackCounts> {
    Arc::new(CallbackCounts {
        first_lifetime_ms: AtomicI64::new(5_000),
        ..CallbackCounts::default()
    })
}

fn wait_for_expiry(runtime: &Runtime, counts: &CallbackCounts) {
    let callbacks = counts.starts.load(Ordering::SeqCst);
    let wait_ms = (counts.first_expiry_ms.load(Ordering::SeqCst) - unix_ms()).max(0) as u64 + 25;
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(wait_ms)).await });
    assert_eq!(counts.starts.load(Ordering::SeqCst), callbacks);
}

fn assert_authorization_since(runtime: &Runtime, server: &MockServer, start: usize, token: &str) {
    let requests = runtime.block_on(server.received_requests()).unwrap();
    assert!(
        requests.len() > start,
        "table operation must perform Azure HTTP I/O"
    );
    for request in &requests[start..] {
        assert_eq!(
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            token
        );
    }
}

#[rstest]
#[cfg_attr(
    miri,
    ignore = "HTTP/Tokio/Parquet; unsafe covered by owned_callbacks_consume_token_on_success_and_failure, caller_provider_free_leaves_builder_reference_until_abandonment, engine_handle_borrow_and_free_keep_ownership, tests::engine_builder, tests::engine_builder_with_option_returns_builder"
)]
fn retained_cdc_iterator_renews_after_caller_engine_free(
    #[values(false, true)] multithreaded: bool,
) {
    let runtime = http_runtime();
    let (server, _table) = mounted_table(&runtime, LogState::with_latest_version(1));
    let counts = table_callback_counts();
    let engine = build_azure_engine(
        &format!("{}/prefix", server.uri()),
        &counts,
        CallbackOutcome::Renew,
        multithreaded,
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let identity = Arc::downgrade(&kernel_engine);
    let snapshot = Snapshot::builder_for(TABLE_URL)
        .build(kernel_engine.as_ref())
        .unwrap();
    assert_eq!(snapshot.version(), 1);
    let scan = TableChanges::try_new(
        Url::parse(TABLE_URL).unwrap(),
        kernel_engine.as_ref(),
        1,
        Some(1),
    )
    .unwrap()
    .into_scan_builder()
    .build()
    .unwrap();
    let mut iterator = scan.execute(kernel_engine.clone()).unwrap();
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    assert_refreshes(&counts, 1, 0);
    assert_authorization_since(&runtime, &server, 0, "Bearer token-A");
    unsafe { free_engine(engine) };
    drop(kernel_engine);
    assert!(identity.upgrade().is_some());
    assert_refreshes(&counts, 1, 0);
    let renewed_start = runtime.block_on(server.received_requests()).unwrap().len();
    wait_for_expiry(&runtime, &counts);
    assert_refreshes(&counts, 1, 0);
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        renewed_start
    );
    let mut rows = 0;
    for data in iterator.by_ref() {
        let batch = data.unwrap().try_into_record_batch().unwrap();
        let versions = batch
            .column_by_name("_commit_version")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let changes = batch
            .column_by_name("_change_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(versions.value(row), 1);
            assert_eq!(changes.value(row), "insert");
        }
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
    assert_eq!(snapshot.version(), 1);
    assert_refreshes(&counts, 2, 0);
    assert_authorization_since(&runtime, &server, renewed_start, "Bearer token-B");
    drop(iterator);
    drop(scan);
    assert!(identity.upgrade().is_none());
    assert_refreshes(&counts, 2, 1);
}

#[rstest]
#[case(0)]
#[case(1_200)]
#[cfg_attr(
    miri,
    ignore = "HTTP/Tokio/Parquet; unsafe covered by owned_callbacks_consume_token_on_success_and_failure, caller_provider_free_leaves_builder_reference_until_abandonment, engine_handle_borrow_and_free_keep_ownership, tests::engine_builder, tests::engine_builder_with_option_returns_builder, tests::test_setting_multithread_executor"
)]
fn original_snapshot_checkpoint_renews_on_same_ffi_engine(#[case] first_list_delay_ms: u64) {
    let runtime = http_runtime();
    let (server, table) = mounted_table_with_delay(
        &runtime,
        LogState::with_latest_version(1),
        first_list_delay_ms,
    );
    let counts = table_callback_counts();
    let engine = build_azure_engine(
        &format!("{}/prefix", server.uri()),
        &counts,
        CallbackOutcome::Renew,
        true,
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let identity = Arc::downgrade(&kernel_engine);
    let snapshot = Snapshot::builder_for(TABLE_URL)
        .build(kernel_engine.as_ref())
        .unwrap();
    assert_eq!(snapshot.version(), 1);
    let snapshot_handle: Handle<SharedSnapshot> = snapshot.clone().into();
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    assert_refreshes(&counts, 1, 0);
    assert_authorization_since(&runtime, &server, 0, "Bearer token-A");
    let renewed_start = runtime.block_on(server.received_requests()).unwrap().len();
    wait_for_expiry(&runtime, &counts);
    assert_refreshes(&counts, 1, 0);
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        renewed_start
    );

    let result = ok_or_panic(unsafe {
        checkpoint_snapshot(snapshot_handle.shallow_copy(), engine.shallow_copy(), None)
    });
    let written = match result {
        FfiCheckpointWriteResult::Written(snapshot) => snapshot,
        FfiCheckpointWriteResult::AlreadyExists(snapshot) => {
            unsafe { free_snapshot(snapshot) };
            panic!("renewed checkpoint must write a new checkpoint");
        }
    };
    assert_eq!(unsafe { written.as_ref() }.version(), 1);
    assert_eq!(snapshot.version(), 1);
    assert!(std::ptr::eq(snapshot.as_ref(), unsafe {
        snapshot_handle.as_ref()
    }));
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    assert_refreshes(&counts, 2, 0);
    assert_authorization_since(&runtime, &server, renewed_start, "Bearer token-B");
    {
        let blobs = table.blobs.lock().unwrap();
        let checkpoint = blobs
            .get("table/_delta_log/00000000000000000001.checkpoint.parquet")
            .unwrap();
        assert!(checkpoint.starts_with(b"PAR1"));
        assert!(checkpoint.ends_with(b"PAR1"));
        let last_checkpoint: serde_json::Value =
            serde_json::from_slice(blobs.get("table/_delta_log/_last_checkpoint").unwrap())
                .unwrap();
        assert_eq!(last_checkpoint["version"], 1);
        assert_eq!(
            last_checkpoint["sizeInBytes"].as_u64(),
            Some(checkpoint.len() as u64)
        );
    }
    let checkpoint_end = runtime.block_on(server.received_requests()).unwrap().len();
    let repeated = ok_or_panic(unsafe {
        checkpoint_snapshot(written.shallow_copy(), engine.shallow_copy(), None)
    });
    match repeated {
        FfiCheckpointWriteResult::AlreadyExists(snapshot) => unsafe { free_snapshot(snapshot) },
        FfiCheckpointWriteResult::Written(snapshot) => {
            unsafe { free_snapshot(snapshot) };
            panic!("returned checkpoint snapshot must report AlreadyExists");
        }
    }
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        checkpoint_end
    );
    assert_refreshes(&counts, 2, 0);
    unsafe { free_snapshot(written) };
    unsafe { free_snapshot(snapshot_handle) };
    drop(snapshot);
    drop(kernel_engine);
    assert!(identity.upgrade().is_some());
    assert_refreshes(&counts, 2, 0);
    unsafe { free_engine(engine) };
    assert!(identity.upgrade().is_none());
    assert_refreshes(&counts, 2, 1);
}

#[rstest]
#[cfg_attr(
    miri,
    ignore = "HTTP/Tokio/Parquet; unsafe covered by owned_callbacks_consume_token_on_success_and_failure, caller_provider_free_leaves_builder_reference_until_abandonment, engine_handle_borrow_and_free_keep_ownership, tests::engine_builder, tests::engine_builder_with_option_returns_builder"
)]
fn staged_blind_append_renews_on_same_ffi_engine(#[values(false, true)] multithreaded: bool) {
    let runtime = http_runtime();
    let (server, table) = mounted_table(&runtime, LogState::with_latest_version(1));
    let file_bytes = {
        let mut blobs = table.blobs.lock().unwrap();
        let bytes = blobs
            .iter()
            .find(|(name, _)| {
                name.starts_with("table/")
                    && !name.starts_with("table/_delta_log/")
                    && name.ends_with(".parquet")
            })
            .map(|(_, bytes)| bytes.clone())
            .unwrap();
        blobs.insert("table/renewed.parquet".to_owned(), bytes.clone());
        bytes
    };
    let counts = table_callback_counts();
    let engine = build_azure_engine(
        &format!("{}/prefix", server.uri()),
        &counts,
        CallbackOutcome::Renew,
        multithreaded,
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let identity = Arc::downgrade(&kernel_engine);
    let snapshot = Snapshot::builder_for(TABLE_URL)
        .build(kernel_engine.as_ref())
        .unwrap();
    assert_eq!(snapshot.version(), 1);
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    assert_refreshes(&counts, 1, 0);
    assert_authorization_since(&runtime, &server, 0, "Bearer token-A");
    let renewed_start = runtime.block_on(server.received_requests()).unwrap().len();
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), kernel_engine.as_ref())
        .unwrap()
        .with_engine_info("azure-renewal")
        .with_blind_append();
    let metadata = serde_json::json!({
        "path": "renewed.parquet",
        "partitionValues": {},
        "size": file_bytes.len(),
        "modificationTime": unix_ms(),
        "stats": {"numRecords": 3}
    });
    let schema = txn.add_files_schema().as_ref().try_into_arrow().unwrap();
    let mut reader = ReaderBuilder::new(Arc::new(schema))
        .build(Cursor::new(metadata.to_string().into_bytes()))
        .unwrap();
    txn.add_files(Box::new(ArrowEngineData::new(
        reader.next().unwrap().unwrap(),
    )));
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        renewed_start
    );
    assert_refreshes(&counts, 1, 0);
    wait_for_expiry(&runtime, &counts);
    assert_eq!(
        runtime.block_on(server.received_requests()).unwrap().len(),
        renewed_start
    );
    assert_refreshes(&counts, 1, 0);

    let committed = txn
        .commit(kernel_engine.as_ref())
        .unwrap()
        .unwrap_committed();
    assert_eq!(committed.commit_version(), 2);
    let post_commit_snapshot = committed.post_commit_snapshot().unwrap().clone();
    assert_eq!(post_commit_snapshot.version(), 2);
    assert_eq!(snapshot.version(), 1);
    assert!(!Arc::ptr_eq(&snapshot, &post_commit_snapshot));
    assert!(Arc::ptr_eq(
        &kernel_engine,
        &unsafe { engine.as_ref() }.engine()
    ));
    assert_refreshes(&counts, 2, 0);
    assert_authorization_since(&runtime, &server, renewed_start, "Bearer token-B");
    {
        let blobs = table.blobs.lock().unwrap();
        assert_eq!(blobs.get("table/renewed.parquet").unwrap(), &file_bytes);
        let commit = blobs
            .get("table/_delta_log/00000000000000000002.json")
            .unwrap();
        let actions = serde_json::Deserializer::from_slice(commit)
            .into_iter::<serde_json::Value>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let adds: Vec<_> = actions
            .iter()
            .filter_map(|action| action.get("add"))
            .collect();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0]["path"], "renewed.parquet");
        assert_eq!(adds[0]["size"].as_u64(), Some(file_bytes.len() as u64));
        assert_eq!(adds[0]["dataChange"], true);
        let commit_info = actions
            .iter()
            .find_map(|action| action.get("commitInfo"))
            .unwrap();
        assert_eq!(commit_info["isBlindAppend"], true);
    }
    drop(post_commit_snapshot);
    drop(committed);
    drop(snapshot);
    drop(kernel_engine);
    assert!(identity.upgrade().is_some());
    assert_refreshes(&counts, 2, 0);
    unsafe { free_engine(engine) };
    assert!(identity.upgrade().is_none());
    assert_refreshes(&counts, 2, 1);
}
