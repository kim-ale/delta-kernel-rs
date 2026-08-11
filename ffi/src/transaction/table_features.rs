use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::EvaluationHandlerExtension as _;

use super::{commit_result_to_committed_handle, ExclusiveCommittedTransaction};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    DeltaResult, ExternEngine, KernelStringSlice, SharedExternEngine, SharedSnapshot, Snapshot,
    TryFromStringSlice,
};

/// A borrowed custom CommitInfo string entry.
///
/// The key and value are copied during [`add_table_features`] and are not retained after the call.
#[repr(C)]
pub struct FfiCommitInfoEntry {
    pub key: KernelStringSlice,
    pub value: KernelStringSlice,
}

/// Add table features and commit the resulting Protocol-only ALTER transaction.
///
/// The supplied snapshot is borrowed; the caller retains ownership. Feature names and custom
/// metadata are copied during the call. Each feature name must exactly match a table feature known
/// to the kernel. The returned committed-transaction handle exposes the committed version and an
/// independently owned post-commit snapshot through the existing accessors.
///
/// A null feature or metadata pointer is accepted only when its corresponding count is zero.
/// Custom metadata keys must be unique. Kernel-owned CommitInfo fields remain authoritative when
/// custom metadata uses the same names.
///
/// # Safety
///
/// Caller is responsible for passing valid snapshot and engine handles. For each nonzero count,
/// the corresponding pointer must reference that many contiguous values. Every feature name and
/// metadata string must either have a valid UTF-8 buffer for its declared length or be null with
/// length zero.
#[no_mangle]
pub unsafe extern "C" fn add_table_features(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    feature_names: *const KernelStringSlice,
    feature_count: usize,
    allow_protocol_versions_increase: bool,
    custom_metadata: *const FfiCommitInfoEntry,
    custom_metadata_count: usize,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    let extern_engine = unsafe { engine.as_ref() };
    let features = unsafe { collect_table_features(feature_names, feature_count) };
    let custom_metadata =
        unsafe { collect_commit_info_entries(custom_metadata, custom_metadata_count) };
    add_table_features_impl(
        snapshot,
        extern_engine,
        features,
        allow_protocol_versions_increase,
        custom_metadata,
    )
    .into_extern_result(&extern_engine)
}

fn add_table_features_impl(
    snapshot: Arc<Snapshot>,
    extern_engine: &dyn ExternEngine,
    features: DeltaResult<Vec<TableFeature>>,
    allow_protocol_versions_increase: bool,
    custom_metadata: DeltaResult<Vec<(String, String)>>,
) -> DeltaResult<Handle<ExclusiveCommittedTransaction>> {
    let engine = extern_engine.engine();
    let mut features = features?.into_iter();
    let first_feature = features.next().ok_or_else(|| {
        delta_kernel::Error::invalid_protocol("At least one table feature must be requested")
    })?;
    let builder = snapshot
        .alter_table()
        .add_table_feature(first_feature)
        .with_allow_protocol_versions_increase(allow_protocol_versions_increase);
    let builder = features.fold(builder, |builder, feature| {
        builder.add_table_feature(feature)
    });
    let mut transaction = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let custom_metadata = custom_metadata?;
    if !custom_metadata.is_empty() {
        let schema: SchemaRef =
            Arc::new(StructType::try_new(custom_metadata.iter().map(
                |(key, _)| StructField::not_null(key.clone(), DataType::STRING),
            ))?);
        let values: Vec<_> = custom_metadata
            .into_iter()
            .map(|(_, value)| Scalar::String(value))
            .collect();
        let data = engine
            .evaluation_handler()
            .create_one(schema.clone(), &values)?;
        transaction = transaction.with_commit_info(data, schema);
    }

    commit_result_to_committed_handle(transaction.commit(engine.as_ref()))
}

/// Copy and validate requested feature names before converting them to kernel features.
///
/// # Safety
///
/// For a nonzero count, `feature_names` must point to `feature_count` readable string slices. Each
/// string must satisfy [`copy_borrowed_string`]'s safety contract.
unsafe fn collect_table_features(
    feature_names: *const KernelStringSlice,
    feature_count: usize,
) -> DeltaResult<Vec<TableFeature>> {
    if feature_count == 0 {
        return Ok(Vec::new());
    }
    if feature_names.is_null() {
        return Err(delta_kernel::Error::generic(
            "feature_names must not be null when feature_count is nonzero",
        ));
    }

    let mut requested_names = HashSet::with_capacity(feature_count);
    unsafe { std::slice::from_raw_parts(feature_names, feature_count) }
        .iter()
        .map(|feature_name| {
            let feature_name = unsafe { copy_borrowed_string(feature_name, "feature name") }?;
            if !requested_names.insert(feature_name.clone()) {
                return Err(delta_kernel::Error::generic(format!(
                    "duplicate requested table feature name: {feature_name}"
                )));
            }

            let feature = TableFeature::from(feature_name.clone());
            if matches!(feature, TableFeature::Unknown(_)) {
                return Err(delta_kernel::Error::unsupported(format!(
                    "Table feature '{feature_name}' has an unknown feature type and is unsupported for addition"
                )));
            }
            Ok(feature)
        })
        .collect()
}

/// Copy and validate custom CommitInfo entries.
///
/// # Safety
///
/// For a nonzero count, `entries` must point to `entry_count` readable entries. Each string must
/// satisfy [`copy_borrowed_string`]'s safety contract.
unsafe fn collect_commit_info_entries(
    entries: *const FfiCommitInfoEntry,
    entry_count: usize,
) -> DeltaResult<Vec<(String, String)>> {
    if entry_count == 0 {
        return Ok(Vec::new());
    }
    if entries.is_null() {
        return Err(delta_kernel::Error::generic(
            "custom_metadata must not be null when custom_metadata_count is nonzero",
        ));
    }

    let mut keys = HashSet::with_capacity(entry_count);
    unsafe { std::slice::from_raw_parts(entries, entry_count) }
        .iter()
        .map(|entry| {
            let key = unsafe { copy_borrowed_string(&entry.key, "custom metadata key") }?;
            if !keys.insert(key.clone()) {
                return Err(delta_kernel::Error::generic(format!(
                    "duplicate custom metadata key: {key}"
                )));
            }
            let value = unsafe { copy_borrowed_string(&entry.value, "custom metadata value") }?;
            Ok((key, value))
        })
        .collect()
}

/// Copy one borrowed CommitInfo string while handling null empty strings without constructing an
/// invalid Rust slice.
///
/// # Safety
///
/// A non-null pointer must reference `slice.len` readable bytes. A null pointer is valid only when
/// the declared length is zero.
unsafe fn copy_borrowed_string(slice: &KernelStringSlice, field_name: &str) -> DeltaResult<String> {
    if slice.ptr.is_null() {
        if slice.len == 0 {
            return Ok(String::new());
        }
        return Err(delta_kernel::Error::generic(format!(
            "{field_name} must not be null when its length is nonzero"
        )));
    }
    unsafe { TryFromStringSlice::try_from_slice(slice) }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::sync::Arc;

    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
    use delta_kernel::schema::try_schema;
    use delta_kernel::table_features::TableFeature;
    use itertools::Itertools;
    use serde_json::{json, Deserializer};
    use tempfile::tempdir;

    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{build_snapshot, ok_or_panic, recover_error};
    use crate::tests::get_default_engine;
    use crate::transaction::{
        committed_transaction_post_commit_snapshot, committed_transaction_version,
        free_committed_transaction,
    };
    use crate::{
        free_engine, free_snapshot, kernel_string_slice, version, OptionalValue,
        SharedExternEngine, SharedSnapshot, Url,
    };

    async fn setup_add_table_features_test(
        table_name: &str,
    ) -> Result<
        (
            tempfile::TempDir,
            Url,
            Arc<DynObjectStore>,
            Handle<SharedExternEngine>,
            Handle<SharedSnapshot>,
        ),
        Box<dyn std::error::Error>,
    > {
        setup_add_table_features_test_with_protocol(table_name, 1, 1).await
    }

    async fn setup_add_table_features_test_with_protocol(
        table_name: &str,
        min_reader_version: i32,
        min_writer_version: i32,
    ) -> Result<
        (
            tempfile::TempDir,
            Url,
            Arc<DynObjectStore>,
            Handle<SharedExternEngine>,
            Handle<SharedSnapshot>,
        ),
        Box<dyn std::error::Error>,
    > {
        let tmp_dir = tempdir()?;
        let tmp_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();
        let schema = Arc::new(try_schema! { nullable "id": INTEGER }?);
        let (store, _test_engine, table_location) =
            test_utils::engine_store_setup(table_name, Some(&tmp_dir_url));
        let table_url = test_utils::create_table(
            store.clone(),
            table_location,
            schema,
            &[],
            false,
            vec![],
            vec![],
        )
        .await?;
        if (min_reader_version, min_writer_version) != (1, 1) {
            let commit_url = table_url.join("_delta_log/00000000000000000000.json")?;
            let commit_path = Path::from_url_path(commit_url.path())?;
            let data = store.get(&commit_path).await?.bytes().await?;
            let data = String::from_utf8(data.to_vec())?;
            let original_protocol = r#""minReaderVersion":1,"minWriterVersion":1"#;
            assert!(data.contains(original_protocol));
            let replacement_protocol = format!(
                r#""minReaderVersion":{min_reader_version},"minWriterVersion":{min_writer_version}"#
            );
            let data = data.replace(original_protocol, &replacement_protocol);
            store.put(&commit_path, data.into()).await?;
        }
        let table_path = table_url.to_file_path().unwrap();
        let table_path = table_path.to_str().unwrap();
        let engine = get_default_engine(table_path);
        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_path), engine.shallow_copy()) };
        Ok((tmp_dir, table_url, store, engine, snapshot))
    }

    fn feature_name_slices(feature_names: &[&str]) -> Vec<KernelStringSlice> {
        feature_names
            .iter()
            .map(|feature_name| unsafe { KernelStringSlice::new_unsafe(feature_name) })
            .collect()
    }

    async fn read_add_table_features_commit(
        store: &DynObjectStore,
        table_url: &Url,
        version: u64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        let commit_url = table_url.join(&format!("_delta_log/{version:020}.json"))?;
        let data = store.get(&Path::from_url_path(commit_url.path())?).await?;
        Ok(Deserializer::from_slice(&data.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?)
    }

    fn assert_add_table_features_error_contains<T>(
        result: ExternResult<T>,
        expected_type: KernelError,
        expected_message: &str,
    ) {
        let ExternResult::Err(error) = result else {
            panic!("expected add_table_features to return an error");
        };
        let error = unsafe { recover_error(error) };
        assert_eq!(error.etype, expected_type);
        assert!(
            error.message.contains(expected_message),
            "expected '{}' to contain '{expected_message}'",
            error.message
        );
    }

    fn assert_feature_commit_absent(table_url: &Url) {
        let commit_path = table_url
            .to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.json");
        assert!(!commit_path.exists(), "failed request must not commit");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_commits_append_only_with_custom_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, store, engine, snapshot) =
            setup_add_table_features_test("feature_metadata").await?;
        let feature_names = feature_name_slices(&["appendOnly"]);
        let custom_key = "customKey";
        let custom_value = "customValue";
        let operation = "operation";
        let stale_operation = "STALE OPERATION";
        let kernel_version = "kernelVersion";
        let stale_kernel_version = "v0.0.0";
        let custom_metadata = [
            FfiCommitInfoEntry {
                key: kernel_string_slice!(custom_key),
                value: kernel_string_slice!(custom_value),
            },
            FfiCommitInfoEntry {
                key: kernel_string_slice!(operation),
                value: kernel_string_slice!(stale_operation),
            },
            FfiCommitInfoEntry {
                key: kernel_string_slice!(kernel_version),
                value: kernel_string_slice!(stale_kernel_version),
            },
        ];

        let committed = ok_or_panic(unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                custom_metadata.as_ptr(),
                custom_metadata.len(),
            )
        });
        assert_eq!(unsafe { committed_transaction_version(&committed) }, 1);

        let actions = read_add_table_features_commit(&store, &table_url, 1).await?;
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0]["commitInfo"]["customKey"], "customValue");
        assert_eq!(actions[0]["commitInfo"]["operation"], "ADD FEATURE");
        assert_ne!(actions[0]["commitInfo"]["kernelVersion"], "v0.0.0");
        assert_eq!(actions[1]["protocol"]["minReaderVersion"], 1);
        assert_eq!(actions[1]["protocol"]["minWriterVersion"], 7);
        assert_eq!(actions[1]["protocol"]["readerFeatures"], json!(null));
        assert_eq!(
            actions[1]["protocol"]["writerFeatures"],
            json!(["appendOnly"])
        );

        unsafe { free_committed_transaction(committed) };
        assert_eq!(unsafe { version(snapshot.shallow_copy()) }, 0);
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_commits_deletion_vectors_to_both_feature_lists(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, store, engine, snapshot) =
            setup_add_table_features_test("feature_reader_writer").await?;
        let feature_names = feature_name_slices(&["deletionVectors"]);

        let committed = ok_or_panic(unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                ptr::null(),
                0,
            )
        });

        let actions = read_add_table_features_commit(&store, &table_url, 1).await?;
        assert_eq!(actions[1]["protocol"]["minReaderVersion"], 3);
        assert_eq!(actions[1]["protocol"]["minWriterVersion"], 7);
        assert_eq!(
            actions[1]["protocol"]["readerFeatures"],
            json!(["deletionVectors"])
        );
        assert_eq!(
            actions[1]["protocol"]["writerFeatures"],
            json!(["deletionVectors"])
        );

        unsafe { free_committed_transaction(committed) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_keeps_column_mapping_on_legacy_reader_two(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, store, engine, snapshot) =
            setup_add_table_features_test_with_protocol("feature_column_mapping", 2, 1).await?;
        let feature_names = feature_name_slices(&["columnMapping"]);

        let committed = ok_or_panic(unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                ptr::null(),
                0,
            )
        });

        let actions = read_add_table_features_commit(&store, &table_url, 1).await?;
        assert_eq!(actions[1]["protocol"]["minReaderVersion"], 2);
        assert_eq!(actions[1]["protocol"]["minWriterVersion"], 7);
        assert_eq!(actions[1]["protocol"]["readerFeatures"], json!(null));
        assert_eq!(
            actions[1]["protocol"]["writerFeatures"],
            json!(["columnMapping"])
        );

        unsafe { free_committed_transaction(committed) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_commits_multiple_known_features(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, store, engine, snapshot) =
            setup_add_table_features_test("feature_multiple").await?;
        let feature_names = feature_name_slices(&["appendOnly", "deletionVectors"]);

        let committed = ok_or_panic(unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                ptr::null(),
                0,
            )
        });

        let actions = read_add_table_features_commit(&store, &table_url, 1).await?;
        assert_eq!(
            actions[1]["protocol"]["readerFeatures"],
            json!(["deletionVectors"])
        );
        assert_eq!(
            actions[1]["protocol"]["writerFeatures"],
            json!(["appendOnly", "deletionVectors"])
        );

        unsafe { free_committed_transaction(committed) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_refuses_protocol_increase_without_commit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, _store, engine, snapshot) =
            setup_add_table_features_test("feature_refusal").await?;
        let feature_names = feature_name_slices(&["deletionVectors"]);

        let result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                false,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            result,
            KernelError::InvalidProtocolError,
            "set allow_protocol_versions_increase to true",
        );
        assert_feature_commit_absent(&table_url);
        assert_eq!(unsafe { version(snapshot.shallow_copy()) }, 0);

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_rejects_invalid_feature_inputs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, _store, engine, snapshot) =
            setup_add_table_features_test("feature_invalid_inputs").await?;
        let feature_names = feature_name_slices(&["appendOnly"]);
        let unknown_name = "futureFeature-EXACT";
        let unknown_feature = feature_name_slices(&[unknown_name]);

        let unknown_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                unknown_feature.as_ptr(),
                unknown_feature.len(),
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            unknown_result,
            KernelError::UnsupportedError,
            unknown_name,
        );

        let empty_features = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                ptr::null(),
                0,
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            empty_features,
            KernelError::InvalidProtocolError,
            "At least one table feature must be requested",
        );

        let null_features = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                ptr::null(),
                1,
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            null_features,
            KernelError::GenericError,
            "feature_names must not be null",
        );

        let invalid_utf8 = [0xffu8];
        let invalid_utf8_feature = [KernelStringSlice {
            ptr: invalid_utf8.as_ptr().cast(),
            len: invalid_utf8.len(),
        }];
        let invalid_utf8_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                invalid_utf8_feature.as_ptr(),
                invalid_utf8_feature.len(),
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            invalid_utf8_result,
            KernelError::Utf8Error,
            "invalid utf-8 sequence",
        );

        let null_string_feature = [KernelStringSlice {
            ptr: ptr::null(),
            len: 1,
        }];
        let null_string_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                null_string_feature.as_ptr(),
                null_string_feature.len(),
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            null_string_result,
            KernelError::GenericError,
            "feature name must not be null",
        );

        let duplicate_features = feature_name_slices(&["appendOnly", "appendOnly"]);
        let duplicate_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                duplicate_features.as_ptr(),
                duplicate_features.len(),
                true,
                ptr::null(),
                0,
            )
        };
        assert_add_table_features_error_contains(
            duplicate_result,
            KernelError::GenericError,
            "duplicate requested table feature name: appendOnly",
        );

        let null_metadata = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                ptr::null(),
                1,
            )
        };
        assert_add_table_features_error_contains(
            null_metadata,
            KernelError::GenericError,
            "custom_metadata must not be null",
        );

        assert_feature_commit_absent(&table_url);
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_rejects_invalid_metadata_without_commit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, table_url, _store, engine, snapshot) =
            setup_add_table_features_test("feature_invalid_metadata").await?;
        let feature_names = feature_name_slices(&["appendOnly"]);
        let duplicate_key = "duplicate";
        let first_value = "first";
        let second_value = "second";
        let duplicate_metadata = [
            FfiCommitInfoEntry {
                key: kernel_string_slice!(duplicate_key),
                value: kernel_string_slice!(first_value),
            },
            FfiCommitInfoEntry {
                key: kernel_string_slice!(duplicate_key),
                value: kernel_string_slice!(second_value),
            },
        ];
        let duplicate_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                duplicate_metadata.as_ptr(),
                duplicate_metadata.len(),
            )
        };
        assert_add_table_features_error_contains(
            duplicate_result,
            KernelError::GenericError,
            "duplicate custom metadata key: duplicate",
        );

        let invalid_utf8 = [0xffu8];
        let value = "value";
        let invalid_metadata = [FfiCommitInfoEntry {
            key: KernelStringSlice {
                ptr: invalid_utf8.as_ptr().cast(),
                len: invalid_utf8.len(),
            },
            value: kernel_string_slice!(value),
        }];
        let invalid_utf8_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                invalid_metadata.as_ptr(),
                invalid_metadata.len(),
            )
        };
        assert_add_table_features_error_contains(
            invalid_utf8_result,
            KernelError::Utf8Error,
            "invalid utf-8 sequence",
        );

        let value = "value";
        let null_string_metadata = [FfiCommitInfoEntry {
            key: KernelStringSlice {
                ptr: ptr::null(),
                len: 1,
            },
            value: kernel_string_slice!(value),
        }];
        let null_string_result = unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                null_string_metadata.as_ptr(),
                null_string_metadata.len(),
            )
        };
        assert_add_table_features_error_contains(
            null_string_result,
            KernelError::GenericError,
            "custom metadata key must not be null",
        );

        assert_feature_commit_absent(&table_url);
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_add_table_features_post_commit_snapshot_handles_are_independent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_tmp_dir, _table_url, _store, engine, snapshot) =
            setup_add_table_features_test("feature_snapshot_ownership").await?;
        let feature_names = feature_name_slices(&["deletionVectors"]);
        let committed = ok_or_panic(unsafe {
            add_table_features(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                feature_names.as_ptr(),
                feature_names.len(),
                true,
                ptr::null(),
                0,
            )
        });

        let post_commit_snapshot =
            match unsafe { committed_transaction_post_commit_snapshot(&committed) } {
                OptionalValue::Some(snapshot) => snapshot,
                OptionalValue::None => {
                    panic!("feature commit should produce a post-commit snapshot")
                }
            };
        let second_post_commit_snapshot =
            match unsafe { committed_transaction_post_commit_snapshot(&committed) } {
                OptionalValue::Some(snapshot) => snapshot,
                OptionalValue::None => {
                    panic!("feature commit should produce an independent post-commit snapshot")
                }
            };
        unsafe { free_committed_transaction(committed) };

        assert_eq!(unsafe { version(snapshot.shallow_copy()) }, 0);
        assert_eq!(unsafe { version(post_commit_snapshot.shallow_copy()) }, 1);
        assert_eq!(
            unsafe { version(second_post_commit_snapshot.shallow_copy()) },
            1
        );
        let post_commit_snapshot_ref = unsafe { post_commit_snapshot.as_ref() };
        assert!(post_commit_snapshot_ref
            .table_configuration()
            .is_feature_supported(&TableFeature::DeletionVectors));

        unsafe { free_snapshot(second_post_commit_snapshot) };
        unsafe { free_snapshot(post_commit_snapshot) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
