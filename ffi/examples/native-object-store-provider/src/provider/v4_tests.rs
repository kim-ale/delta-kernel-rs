mod v4 {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Barrier;
    use std::time::Duration;

    use futures::FutureExt;
    use object_store::{Attribute, Attributes, CopyMode, RenameTargetMode, TagSet, UploadPart};

    use super::*;

    #[derive(Debug, Default)]
    pub(super) struct UploadProbe {
        invoked: Mutex<Vec<usize>>,
        waited: Mutex<Vec<usize>>,
        part_drops: AtomicU64,
        upload_drops: AtomicU64,
        completes: AtomicU64,
        aborts: AtomicU64,
        fail_part: Option<usize>,
        barrier: Option<Arc<Barrier>>,
    }

    #[derive(Debug)]
    pub(super) struct SpyUpload {
        inner: Box<dyn MultipartUpload>,
        probe: Arc<UploadProbe>,
        next_part: usize,
    }

    impl SpyUpload {
        pub(super) fn new(inner: Box<dyn MultipartUpload>, probe: Arc<UploadProbe>) -> Self {
            Self {
                inner,
                probe,
                next_part: 0,
            }
        }
    }

    struct PartDrop(Arc<UploadProbe>);

    impl Drop for PartDrop {
        fn drop(&mut self) {
            self.0.part_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for SpyUpload {
        fn drop(&mut self) {
            self.probe.upload_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl MultipartUpload for SpyUpload {
        fn put_part(&mut self, payload: PutPayload) -> UploadPart {
            assert!(tokio::runtime::Handle::try_current().is_ok());
            let index = self.next_part;
            self.next_part += 1;
            self.probe.invoked.lock().unwrap().push(index);
            let native = self.inner.put_part(payload);
            let probe = self.probe.clone();
            let drop_guard = PartDrop(probe.clone());
            async move {
                let _drop_guard = drop_guard;
                probe.waited.lock().unwrap().push(index);
                if let Some(barrier) = &probe.barrier {
                    barrier.wait();
                }
                if probe.fail_part == Some(index) {
                    return Err(object_store::Error::Precondition {
                        path: "part".into(),
                        source: "native part failure".into(),
                    });
                }
                native.await
            }
            .boxed()
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.probe.completes.fetch_add(1, Ordering::SeqCst);
            let mut result = self.inner.complete().await?;
            result.version = Some(String::new());
            Ok(result)
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.probe.aborts.fetch_add(1, Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    #[derive(Default, Debug)]
    struct Output {
        etags: Vec<Option<String>>,
        versions: Vec<Option<String>>,
        bodies: Vec<(usize, Vec<u8>)>,
        attributes: Vec<(String, String)>,
        objects: Vec<OwnedMetadata>,
        prefixes: Vec<String>,
        deletes: Vec<(Option<String>, i32)>,
        reject_at: Option<usize>,
    }

    impl Output {
        fn context(&mut self) -> *mut c_void {
            ptr::from_mut(self).cast()
        }
    }

    unsafe fn own_optional(value: KernelNativeStringSliceV1) -> Option<String> {
        unsafe { marshalling::copy_optional(value, &mut 0) }.unwrap()
    }

    unsafe extern "C" fn result_sink(
        context: *mut c_void,
        result: *const KernelNativePutResultV4,
    ) -> i32 {
        let output = unsafe { &mut *context.cast::<Output>() };
        let result = unsafe { &*result };
        output.etags.push(unsafe { own_optional(result.e_tag) });
        output
            .versions
            .push(unsafe { own_optional(result.version) });
        if output.reject_at.is_some() {
            1
        } else {
            0
        }
    }

    unsafe extern "C" fn get_sink(
        context: *mut c_void,
        meta: *const KernelNativeObjectMetaV4,
        body: KernelNativeByteSliceV1,
        _start: u64,
        _end: u64,
        attributes: *const KernelNativeKeyValueV4,
        count: usize,
    ) -> i32 {
        let output = unsafe { &mut *context.cast::<Output>() };
        let metadata = unsafe { &*meta };
        output.objects.push(unsafe { own_metadata(meta) }.unwrap());
        output.etags.push(unsafe { own_optional(metadata.e_tag) });
        output
            .versions
            .push(unsafe { own_optional(metadata.version) });
        output
            .bodies
            .push((0, unsafe { copy_bytes(body) }.unwrap()));
        for pair in
            unsafe { marshalling::array(attributes, count, KERNEL_NATIVE_MAX_COLLECTION) }.unwrap()
        {
            output.attributes.push((
                unsafe { copy_string(pair.key) }.unwrap(),
                unsafe { copy_string(pair.value) }.unwrap(),
            ));
        }
        0
    }

    unsafe extern "C" fn ranges_sink(
        context: *mut c_void,
        index: usize,
        body: KernelNativeByteSliceV1,
    ) -> i32 {
        let output = unsafe { &mut *context.cast::<Output>() };
        assert_eq!(index, output.bodies.len());
        output
            .bodies
            .push((index, unsafe { copy_bytes(body) }.unwrap()));
        if output.reject_at == Some(index) {
            -1
        } else {
            0
        }
    }

    unsafe extern "C" fn deletes_sink(
        context: *mut c_void,
        path: KernelNativeStringSliceV1,
        status: i32,
    ) -> i32 {
        let output = unsafe { &mut *context.cast::<Output>() };
        let index = output.deletes.len();
        output.deletes.push((unsafe { own_optional(path) }, status));
        if output.reject_at == Some(index) {
            -1
        } else {
            0
        }
    }

    unsafe extern "C" fn delimiter_sink(
        context: *mut c_void,
        objects: *const KernelNativeObjectMetaV4,
        objects_len: usize,
        prefixes: *const KernelNativeStringSliceV1,
        prefixes_len: usize,
    ) -> i32 {
        let output = unsafe { &mut *context.cast::<Output>() };
        for meta in
            unsafe { marshalling::array(objects, objects_len, KERNEL_NATIVE_MAX_COLLECTION) }
                .unwrap()
        {
            output.objects.push(unsafe { own_metadata(meta) }.unwrap());
            output.etags.push(unsafe { own_optional(meta.e_tag) });
            output.versions.push(unsafe { own_optional(meta.version) });
        }
        for prefix in
            unsafe { marshalling::array(prefixes, prefixes_len, KERNEL_NATIVE_MAX_COLLECTION) }
                .unwrap()
        {
            output
                .prefixes
                .push(unsafe { copy_string(*prefix) }.unwrap());
        }
        if output.reject_at.is_some() {
            -1
        } else {
            0
        }
    }

    fn pairs() -> Vec<(String, String)> {
        [
            ("content-disposition", "inline"),
            ("content-encoding", "gzip"),
            ("content-language", "en"),
            ("content-type", "text/plain"),
            ("cache-control", "no-cache"),
            ("storage-class", "COOL"),
            ("metadata:empty", ""),
            ("metadata:unicode", "a&b=+%\u{03bb}"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .to_vec()
    }

    fn write(
        fixture: &Fixture,
        path: &str,
        body: &[u8],
        options: KernelNativeWriteOptionsV4,
    ) -> (i32, Output) {
        let mut output = Output::default();
        let status = unsafe {
            fixture.0.put.unwrap()(
                fixture.0.context,
                string(path),
                bytes(body),
                options,
                output.context(),
                result_sink,
            )
        };
        (status, output)
    }

    fn read(fixture: &Fixture, path: &str, options: KernelNativeGetOptionsV4) -> (i32, Output) {
        let mut output = Output::default();
        let status = unsafe {
            fixture.0.get.unwrap()(
                fixture.0.context,
                string(path),
                options,
                output.context(),
                get_sink,
            )
        };
        (status, output)
    }

    fn ranges(fixture: &Fixture, input: &[KernelNativeRangeV4], output: &mut Output) -> i32 {
        unsafe {
            fixture.0.get_ranges.unwrap()(
                fixture.0.context,
                string("data"),
                input.as_ptr(),
                input.len(),
                output.context(),
                ranges_sink,
            )
        }
    }

    fn delete(fixture: &Fixture, paths: &[KernelNativeStringSliceV1], output: &mut Output) -> i32 {
        unsafe {
            fixture.0.delete_batch.unwrap()(
                fixture.0.context,
                paths.as_ptr(),
                paths.len(),
                output.context(),
                deletes_sink,
            )
        }
    }

    fn delimiter(fixture: &Fixture, prefix: &str, output: &mut Output) -> i32 {
        unsafe {
            fixture.0.list_delimiter.unwrap()(
                fixture.0.context,
                string(prefix),
                output.context(),
                delimiter_sink,
            )
        }
    }

    struct Upload(*mut c_void);
    struct Part(*mut c_void);
    unsafe impl Send for Part {}

    impl Drop for Upload {
        fn drop(&mut self) {
            unsafe { multipart::close(self.0) };
        }
    }
    impl Drop for Part {
        fn drop(&mut self) {
            unsafe { multipart::part_close(self.0) };
        }
    }
    impl Upload {
        fn open(fixture: &Fixture, path: &str, options: KernelNativeWriteOptionsV4) -> Self {
            let mut upload = ptr::null_mut();
            assert_eq!(
                unsafe {
                    fixture.0.multipart_open.unwrap()(
                        fixture.0.context,
                        string(path),
                        options,
                        &mut upload,
                    )
                },
                0
            );
            Self(upload)
        }
        fn part(&self, body: &[u8]) -> Part {
            let mut part = ptr::null_mut();
            assert_eq!(
                unsafe { multipart::part_open(self.0, bytes(body), &mut part) },
                0
            );
            Part(part)
        }
        fn complete(&self) -> (i32, Output) {
            let mut output = Output::default();
            let status = unsafe { multipart::complete(self.0, output.context(), result_sink) };
            (status, output)
        }
    }
    impl Part {
        fn wait(&mut self) -> i32 {
            unsafe { multipart::part_wait(self.0) }
        }
    }

    #[test]
    fn batch_ranges_enter_native_once_preserving_unsorted_disjoint_duplicates_and_indexes() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        assert_eq!(fixture.put("data", b"0123456789", 0), 0);
        let input =
            [(7, 9), (0, 2), (4, 5), (7, 9)].map(|(start, end)| KernelNativeRangeV4 { start, end });
        let mut output = Output::default();
        assert_eq!(ranges(&fixture, &input, &mut output), 0);
        assert_eq!(
            output.bodies,
            [
                (0, b"78".to_vec()),
                (1, b"01".to_vec()),
                (2, b"4".to_vec()),
                (3, b"78".to_vec())
            ]
        );
        assert_eq!(
            *store.range_requests.lock().unwrap(),
            [(Path::from("data"), vec![7..9, 0..2, 4..5, 7..9])]
        );
        assert!(store.gets.lock().unwrap().is_empty());
        let store = Arc::new(StoreSpy {
            range_results: Some(vec![Bytes::from_static(b"0123"), Bytes::new()]),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let input = [
            KernelNativeRangeV4 { start: 0, end: 4 },
            KernelNativeRangeV4 { start: 2, end: 2 },
        ];
        let mut output = Output::default();
        assert_eq!(ranges(&fixture, &input, &mut output), 0);
        assert_eq!(output.bodies, [(0, b"0123".to_vec()), (1, Vec::new())]);
        assert_eq!(
            *store.range_requests.lock().unwrap(),
            [(Path::from("data"), vec![0..4, 2..2])]
        );
    }

    #[test]
    fn ranges_validate_all_native_results_before_sink_and_failed_sinks_stop_serial_emission() {
        let _serial = TEST_LOCK.lock().unwrap();
        let input = [
            KernelNativeRangeV4 { start: 0, end: 2 },
            KernelNativeRangeV4 { start: 4, end: 6 },
        ];
        for results in [
            vec![Bytes::new()],
            vec![Bytes::new(), Bytes::from_static(b"xxx")],
            vec![Bytes::new(), Bytes::new(), Bytes::new()],
        ] {
            let store = Arc::new(StoreSpy {
                range_results: Some(results),
                ..Default::default()
            });
            let fixture = Fixture::from_store(store.clone());
            let mut output = Output::default();
            assert_eq!(ranges(&fixture, &input, &mut output), 3);
            assert!(output.bodies.is_empty());
            assert_eq!(store.range_requests.lock().unwrap().len(), 1);
        }
        let fixture = Fixture::memory();
        fixture.put("data", b"0123456789", 0);
        let mut output = Output {
            reject_at: Some(0),
            ..Default::default()
        };
        let status = ranges(&fixture, &input, &mut output);
        assert_eq!((status, output.bodies.len()), (3, 1));
        if status != 0 {
            output.bodies.clear();
        }
        assert!(output.bodies.is_empty());
    }

    #[test]
    fn native_delete_stream_is_called_once_and_errors_do_not_stop_later_successes() {
        let _serial = TEST_LOCK.lock().unwrap();
        let input = [string("missing"), string("second"), string("third")];
        for case in ["mixed", "aggregate"] {
            let store = Arc::new(StoreSpy {
                delete_case: case,
                ..Default::default()
            });
            let fixture = Fixture::from_store(store.clone());
            let mut output = Output::default();
            assert_eq!(delete(&fixture, &input, &mut output), 0);
            assert_eq!(store.deletes.load(Ordering::SeqCst), 1);
            assert_eq!(
                *store.delete_inputs.lock().unwrap(),
                [
                    Path::from("missing"),
                    Path::from("second"),
                    Path::from("third")
                ]
            );
            if case == "aggregate" {
                assert_eq!(output.deletes, [(Some("missing".into()), 0), (None, 3)]);
            } else {
                assert_eq!(
                    output.deletes,
                    [
                        (Some("missing".into()), 1),
                        (Some("second".into()), 0),
                        (None, 3),
                        (Some("third".into()), 0)
                    ]
                );
            }
        }
        let fixture = Fixture::memory();
        fixture.put("second", b"value", 0);
        assert_eq!(delete(&fixture, &input, &mut Output::default()), 0);
        assert_eq!(fixture.get("second").0, 1);
    }

    #[test]
    fn copy_rename_call_native_methods_with_atomic_target_modes_and_memory_effects() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        fixture.put("source", b"value", 0);
        for (rename, from, to, mode, expected) in [
            (false, "source", "copy", 1, 0),
            (false, "source", "copy", 1, 2),
            (false, "missing", "absent", 0, 1),
            (false, "source", "copy", 0, 0),
            (true, "source", "copy", 1, 2),
            (true, "source", "moved", 1, 0),
            (true, "missing", "absent", 0, 1),
            (true, "moved", "copy", 0, 0),
        ] {
            let callback = if rename {
                fixture.0.rename
            } else {
                fixture.0.copy
            }
            .unwrap();
            assert_eq!(
                unsafe { callback(fixture.0.context, string(from), string(to), mode) },
                expected
            );
        }
        assert_eq!(fixture.get("source").0, 1);
        assert_eq!(fixture.get("moved").0, 1);
        assert_eq!(fixture.get("copy").1.bodies, [b"value"]);
        let copies = store.copies.lock().unwrap();
        assert_eq!(copies.len(), 4);
        assert_eq!(
            (&copies[0].0, &copies[0].1, copies[0].2.mode),
            (&Path::from("source"), &Path::from("copy"), CopyMode::Create)
        );
        assert_eq!(copies[3].2.mode, CopyMode::Overwrite);
        let renames = store.renames.lock().unwrap();
        assert_eq!(renames.len(), 4);
        assert_eq!(renames[0].2.target_mode, RenameTargetMode::Create);
        assert_eq!(renames[3].2.target_mode, RenameTargetMode::Overwrite);
    }

    #[test]
    fn delimiter_enters_native_directly_and_preserves_objects_prefixes_and_order() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        for path in ["dir/file", "dir/nested/a", "dir/nested/b", "other"] {
            fixture.put(path, b"x", 0);
        }
        let mut output = Output::default();
        assert_eq!(delimiter(&fixture, "dir", &mut output), 0);
        assert_eq!(
            output
                .objects
                .iter()
                .map(|meta| meta.location.as_str())
                .collect::<Vec<_>>(),
            ["dir/file"]
        );
        assert_eq!(output.prefixes, ["dir/nested"]);
        assert_eq!(
            *store.delimiter_requests.lock().unwrap(),
            [Some(Path::from("dir"))]
        );
        assert!(store.list_requests.lock().unwrap().is_empty());
        let meta = runtime()
            .unwrap()
            .block_on(store.inner.head(&Path::from("dir/file")))
            .unwrap();
        let objects = ["dir/z", "dir/a"].map(|path| ObjectMeta {
            location: Path::from(path),
            ..meta.clone()
        });
        let store = Arc::new(StoreSpy {
            delimiter_override: Some(ListResult {
                objects: objects.to_vec(),
                common_prefixes: vec![Path::from("dir/zoo"), Path::from("dir/apple")],
                extensions: Default::default(),
            }),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let mut output = Output::default();
        assert_eq!(delimiter(&fixture, "dir", &mut output), 0);
        assert_eq!(
            output
                .objects
                .iter()
                .map(|meta| meta.location.as_str())
                .collect::<Vec<_>>(),
            ["dir/z", "dir/a"]
        );
        assert_eq!(output.prefixes, ["dir/zoo", "dir/apple"]);
    }

    #[test]
    fn put_get_preserve_all_attributes_native_etags_and_present_empty_versions() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy {
            response_version: true,
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let attributes = pairs();
        let attributes_view = marshalling::pair_views(&attributes);
        let tags = vec![
            ("key&=+".to_owned(), "value %&=+".to_owned()),
            ("empty".to_owned(), String::new()),
            ("\u{03bb}".to_owned(), "\u{1f600}".to_owned()),
        ];
        let tags_view = marshalling::pair_views(&tags);
        let mut options = write_options(0);
        options.attributes = attributes_view.as_ptr();
        options.attributes_len = attributes_view.len();
        options.tags = tags_view.as_ptr();
        options.tags_len = tags_view.len();
        let (status, written) = write(&fixture, "data", b"value", options);
        assert_eq!(status, 0);
        let (status, read) = read(&fixture, "data", v4_get(Default::default()));
        assert_eq!(status, 0);
        assert_eq!(read.bodies, [(0, b"value".to_vec())]);
        assert!(written.etags[0].is_some());
        assert_eq!(read.etags, written.etags);
        assert_eq!(written.versions, [Some(String::new())]);
        assert_eq!(read.versions, written.versions);
        assert_eq!(
            read.attributes
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>(),
            attributes.into_iter().collect()
        );
        let native = store.put_options.lock().unwrap();
        let mut expected = TagSet::default();
        for (key, value) in tags {
            expected.push(&key, &value);
        }
        assert_eq!(native[0].tags.encoded(), expected.encoded());
        assert_eq!(native[0].attributes.len(), 8);
    }

    #[test]
    fn conditional_get_and_update_put_use_native_preconditions_without_provider_policy() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        let (_, written) = write(&fixture, "data", b"before", write_options(0));
        let etag = written.etags[0].as_deref().unwrap();
        let mut options = v4_get(Default::default());
        options.if_match = string("wrong");
        assert_eq!(read(&fixture, "data", options).0, 5);
        options.if_match = optional_string(None);
        options.if_none_match = string(etag);
        assert_eq!(read(&fixture, "data", options).0, 6);
        options.if_none_match = optional_string(None);
        options.if_match = string(etag);
        assert_eq!(read(&fixture, "data", options).0, 0);
        let mut update = write_options(2);
        update.e_tag = string("wrong");
        assert_eq!(write(&fixture, "data", b"failed", update).0, 5);
        update.e_tag = string(etag);
        assert_eq!(write(&fixture, "data", b"after", update).0, 0);
        assert_eq!(fixture.get("data").1.bodies, [b"after"]);
        assert!(
            matches!(&store.put_modes.lock().unwrap()[2], PutMode::Update(version)
            if version.e_tag.as_deref() == Some(etag) && version.version.is_none())
        );
        update.e_tag = optional_string(None);
        update.version = string("");
        let _ = write(&fixture, "data", b"native-decides", update);
        assert!(
            matches!(store.put_modes.lock().unwrap().last().unwrap(), PutMode::Update(version)
            if version.e_tag.is_none() && version.version.as_deref() == Some(""))
        );
        update.e_tag = string("");
        update.version = string("");
        let _ = write(&fixture, "data", b"both-fields", update);
        assert!(
            matches!(store.put_modes.lock().unwrap().last().unwrap(), PutMode::Update(version)
            if version.e_tag.as_deref() == Some("") && version.version.as_deref() == Some(""))
        );
    }

    #[test]
    fn get_conditions_keep_nanoseconds_versions_and_optional_empty_values() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        fixture.put("data", b"value", 0);
        let mut options = v4_get(Default::default());
        options.version = string("");
        options.if_match = string("");
        options.if_none_match = string("");
        options.time_flags = 3;
        options.modified_seconds = -1;
        options.modified_nanos = 123_456_789;
        options.unmodified_seconds = 1;
        options.unmodified_nanos = 987_654_321;
        let _ = read(&fixture, "data", options);
        let observed = store.gets.lock().unwrap();
        let observed = &observed[0];
        assert_eq!(
            (
                observed.version.as_deref(),
                observed.if_match.as_deref(),
                observed.if_none_match.as_deref()
            ),
            (Some(""), Some(""), Some(""))
        );
        assert_eq!(observed.if_modified_since.unwrap().timestamp(), -1);
        assert_eq!(
            observed.if_modified_since.unwrap().timestamp_subsec_nanos(),
            123_456_789
        );
        assert_eq!(
            observed
                .if_unmodified_since
                .unwrap()
                .timestamp_subsec_nanos(),
            987_654_321
        );
    }

    #[test]
    fn multipart_invokes_parts_before_polling_reverse_waits_preserve_body_and_complete_metadata() {
        let _serial = TEST_LOCK.lock().unwrap();
        let probe = Arc::new(UploadProbe::default());
        let store = Arc::new(StoreSpy {
            upload_probe: Some(probe.clone()),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let attributes = pairs();
        let attributes = marshalling::pair_views(&attributes);
        let tags = vec![("key".into(), "value".into())];
        let tags = marshalling::pair_views(&tags);
        let mut options = write_options(0);
        options.attributes = attributes.as_ptr();
        options.attributes_len = attributes.len();
        options.tags = tags.as_ptr();
        options.tags_len = tags.len();
        let upload = Upload::open(&fixture, "data", options);
        let mut first = upload.part(b"first");
        let mut second = upload.part(b"second");
        assert_eq!(*probe.invoked.lock().unwrap(), [0, 1]);
        assert!(probe.waited.lock().unwrap().is_empty());
        assert_eq!(second.wait(), 0);
        assert_eq!(first.wait(), 0);
        assert_eq!(*probe.waited.lock().unwrap(), [1, 0]);
        assert_eq!(first.wait(), 3);
        let (status, result) = upload.complete();
        assert_eq!(status, 0);
        assert!(result.etags[0].is_some());
        assert_eq!(result.versions, [Some(String::new())]);
        let (status, read) = read(&fixture, "data", v4_get(Default::default()));
        assert_eq!(status, 0);
        assert_eq!(read.bodies, [(0, b"firstsecond".to_vec())]);
        assert_eq!(read.etags, result.etags);
        assert_eq!(read.attributes.len(), 8);
        let options = store.multipart_options.lock().unwrap();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].0, Path::from("data"));
        let mut expected = TagSet::default();
        expected.push("key", "value");
        assert_eq!(options[0].1.tags.encoded(), expected.encoded());
        assert_eq!(options[0].1.attributes.len(), 8);
        drop(upload);
        assert_eq!(probe.upload_drops.load(Ordering::SeqCst), 0);
        drop(first);
        drop(second);
        assert_eq!(probe.part_drops.load(Ordering::SeqCst), 2);
        assert_eq!(probe.upload_drops.load(Ordering::SeqCst), 1);
        assert_eq!(probe.completes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn multipart_abort_is_explicit_and_native_not_implemented_leaves_output_untouched() {
        let _serial = TEST_LOCK.lock().unwrap();
        let probe = Arc::new(UploadProbe::default());
        let store = Arc::new(StoreSpy {
            upload_probe: Some(probe.clone()),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let upload = Upload::open(&fixture, "aborted", write_options(0));
        let mut part = upload.part(b"not-visible");
        assert_eq!(part.wait(), 0);
        drop(part);
        assert_eq!(unsafe { multipart::abort(upload.0) }, 0);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.get("aborted").0, 1);
        drop(upload);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        let fixture = Fixture::from_store(Arc::new(StoreSpy {
            multipart_unimplemented: true,
            ..Default::default()
        }));
        let mut output = fixture.0.context;
        assert_eq!(
            unsafe {
                multipart::open(
                    fixture.0.context,
                    string("unsupported"),
                    write_options(0),
                    &mut output,
                )
            },
            7
        );
        assert_eq!(output, fixture.0.context);
    }

    #[test]
    fn failed_and_unpolled_parts_retain_parent_after_upload_close_and_drop_inside_other_runtime() {
        let _serial = TEST_LOCK.lock().unwrap();
        let probe = Arc::new(UploadProbe {
            fail_part: Some(0),
            ..Default::default()
        });
        let store = Arc::new(StoreSpy {
            upload_probe: Some(probe.clone()),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let upload = Upload::open(&fixture, "data", write_options(0));
        let mut failed = upload.part(b"first");
        let unpolled = upload.part(b"second");
        drop(upload);
        assert_eq!(probe.upload_drops.load(Ordering::SeqCst), 0);
        assert_eq!(failed.wait(), 5);
        assert_eq!(failed.wait(), 3);
        assert_eq!(probe.part_drops.load(Ordering::SeqCst), 1);
        drop(fixture);
        assert_eq!(Arc::strong_count(&store), 2);
        std::thread::spawn(move || {
            let other = Builder::new_current_thread().enable_all().build().unwrap();
            other.block_on(async move {
                drop(failed);
                drop(unpolled);
            });
        })
        .join()
        .unwrap();
        assert_eq!(probe.part_drops.load(Ordering::SeqCst), 2);
        assert_eq!(probe.upload_drops.load(Ordering::SeqCst), 1);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&store), 1);
        assert_eq!(Fixture::memory().get(INITIAL_PATH).0, 0);
    }

    #[test]
    fn multipart_part_futures_poll_concurrently_without_holding_upload_mutex() {
        let _serial = TEST_LOCK.lock().unwrap();
        let probe = Arc::new(UploadProbe {
            barrier: Some(Arc::new(Barrier::new(2))),
            ..Default::default()
        });
        let fixture = Fixture::from_store(Arc::new(StoreSpy {
            upload_probe: Some(probe.clone()),
            ..Default::default()
        }));
        let upload = Upload::open(&fixture, "data", write_options(0));
        let mut first = upload.part(b"a");
        let mut second = upload.part(b"b");
        let first = std::thread::spawn(move || first.wait());
        let second = std::thread::spawn(move || second.wait());
        assert_eq!(first.join().unwrap(), 0);
        assert_eq!(second.join().unwrap(), 0);
        assert_eq!(upload.complete().0, 0);
        assert_eq!(fixture.get("data").1.bodies, [b"ab"]);
    }

    #[test]
    fn descriptor_all_slots_are_present_and_listing_keeps_optional_metadata() {
        let _serial = TEST_LOCK.lock().unwrap();
        let fixture = Fixture::memory();
        assert!(fixture.0.get.is_some() && fixture.0.get_ranges.is_some());
        assert!(
            fixture.0.list_open.is_some()
                && fixture.0.list_next.is_some()
                && fixture.0.list_close.is_some()
        );
        assert!(
            fixture.0.list_delimiter.is_some()
                && fixture.0.put.is_some()
                && fixture.0.delete_batch.is_some()
        );
        assert!(fixture.0.copy.is_some() && fixture.0.rename.is_some());
        assert!(fixture.0.multipart_open.is_some() && fixture.0.multipart_part_open.is_some());
        assert!(
            fixture.0.multipart_part_wait.is_some() && fixture.0.multipart_part_close.is_some()
        );
        assert!(fixture.0.multipart_complete.is_some() && fixture.0.multipart_abort.is_some());
        assert!(fixture.0.multipart_close.is_some() && fixture.0.release.is_some());
        let context = create_memory().unwrap();
        let mut meta = runtime()
            .unwrap()
            .block_on(context.store.head(&Path::from(INITIAL_PATH)))
            .unwrap();
        meta.e_tag = Some(String::new());
        meta.version = None;
        let fixture = Fixture::from_store(Arc::new(StoreSpy {
            listing_override: Some(vec![meta]),
            ..Default::default()
        }));
        let (_, capture) = fixture.list("");
        assert_eq!(capture.metadata[0].e_tag.as_deref(), Some(""));
        assert_eq!(capture.metadata[0].version, None);
    }

    #[test]
    fn malformed_arrays_ranges_and_collection_bounds_are_rejected_before_native_entry() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        let mut output = Output::default();
        for (pointer, count, expected) in [
            (ptr::null(), 1, 3),
            (std::ptr::NonNull::<u8>::dangling().as_ptr().cast(), 1, 3),
            (ptr::null(), KERNEL_NATIVE_MAX_COLLECTION + 1, 4),
        ] {
            assert_eq!(
                unsafe {
                    fixture.0.get_ranges.unwrap()(
                        fixture.0.context,
                        string("data"),
                        pointer,
                        count,
                        output.context(),
                        ranges_sink,
                    )
                },
                expected
            );
        }
        for (pointer, count, expected) in [
            (ptr::null(), 1, 3),
            (std::ptr::NonNull::<u8>::dangling().as_ptr().cast(), 1, 3),
            (ptr::null(), 129, 4),
        ] {
            assert_eq!(
                unsafe {
                    fixture.0.delete_batch.unwrap()(
                        fixture.0.context,
                        pointer,
                        count,
                        output.context(),
                        deletes_sink,
                    )
                },
                expected
            );
        }
        let invalid_range = KernelNativeRangeV4 { start: 2, end: 1 };
        assert_eq!(ranges(&fixture, &[invalid_range], &mut output), 3);
        let invalid_utf8 = [255];
        for tags in [true, false] {
            for (pointer, count, expected) in [
                (ptr::null(), 1, 3),
                (std::ptr::NonNull::<u8>::dangling().as_ptr().cast(), 1, 3),
                (ptr::null(), KERNEL_NATIVE_MAX_COLLECTION + 1, 4),
            ] {
                let mut options = write_options(0);
                if tags {
                    options.tags = pointer;
                    options.tags_len = count;
                } else {
                    options.attributes = pointer;
                    options.attributes_len = count;
                }
                assert_eq!(write(&fixture, "data", b"x", options).0, expected);
            }
            let pair = KernelNativeKeyValueV4 {
                key: string("content-type"),
                value: KernelNativeStringSliceV1 {
                    ptr: invalid_utf8.as_ptr().cast(),
                    len: 1,
                },
            };
            let mut options = write_options(0);
            if tags {
                options.tags = &pair;
                options.tags_len = 1;
            } else {
                options.attributes = &pair;
                options.attributes_len = 1;
            }
            assert_eq!(write(&fixture, "data", b"x", options).0, 3);
        }
        assert!(output.bodies.is_empty() && output.deletes.is_empty());
        assert!(store.range_requests.lock().unwrap().is_empty());
        assert!(store.put_options.lock().unwrap().is_empty());
        assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn get_conditions_reject_invalid_flags_timestamps_and_optional_slices_before_native_entry() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        let mut invalid = Vec::new();
        let mut options = v4_get(Default::default());
        options.time_flags = 4;
        invalid.push(options);
        options = v4_get(Default::default());
        options.modified_nanos = 1_000_000_000;
        invalid.push(options);
        options = v4_get(Default::default());
        options.unmodified_nanos = 1_000_000_000;
        invalid.push(options);
        options = v4_get(Default::default());
        options.time_flags = 1;
        options.modified_seconds = i64::MAX;
        invalid.push(options);
        options = v4_get(Default::default());
        options.time_flags = 2;
        options.unmodified_seconds = i64::MIN;
        invalid.push(options);
        options = v4_get(Default::default());
        options.if_match = KernelNativeStringSliceV1 {
            ptr: ptr::null(),
            len: 1,
        };
        invalid.push(options);
        for options in invalid {
            assert_eq!(read(&fixture, "data", options).0, 3);
        }
        let mut options = v4_get(Default::default());
        options.version = KernelNativeStringSliceV1 {
            ptr: ptr::null(),
            len: MAX_PATH_BYTES + 1,
        };
        assert_eq!(read(&fixture, "data", options).0, 4);
        assert!(store.gets.lock().unwrap().is_empty());
    }

    #[test]
    fn write_bounds_unknown_duplicate_attributes_and_multipart_update_fields_fail_before_native_entry(
    ) {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let fixture = Fixture::from_store(store.clone());
        let mut output = Output::default();
        let pair = KernelNativeKeyValueV4 {
            key: string("content-type"),
            value: string("text/plain"),
        };
        let mut options = write_options(0);
        options.attributes = &pair;
        options.attributes_len = 1;
        assert_eq!(
            unsafe {
                fixture.0.put.unwrap()(
                    fixture.0.context,
                    string("data"),
                    KernelNativeByteSliceV1 {
                        ptr: ptr::null(),
                        len: MAX_BODY_BYTES,
                    },
                    options,
                    output.context(),
                    result_sink,
                )
            },
            4
        );
        let duplicates = [pair, pair];
        options.attributes = duplicates.as_ptr();
        options.attributes_len = 2;
        assert_eq!(write(&fixture, "data", b"x", options).0, 3);
        let unknown = KernelNativeKeyValueV4 {
            key: string("unknown"),
            value: string(""),
        };
        options.attributes = &unknown;
        options.attributes_len = 1;
        assert_eq!(write(&fixture, "data", b"x", options).0, 4);
        let oversized = KernelNativeKeyValueV4 {
            key: pair.key,
            value: KernelNativeStringSliceV1 {
                ptr: ptr::null(),
                len: MAX_PATH_BYTES + 1,
            },
        };
        options.attributes = &oversized;
        assert_eq!(write(&fixture, "data", b"x", options).0, 4);
        let mut budget = MAX_BODY_BYTES - 1;
        assert_eq!(charge(&mut budget, 2), Err(4));
        assert_eq!(charge(&mut 0, usize::MAX), Err(4));
        for mut options in [
            write_options(1),
            write_options(2),
            write_options(0),
            write_options(0),
        ] {
            if options.mode == 0 {
                options.e_tag = string("");
            }
            let mut pointer = fixture.0.context;
            assert_eq!(
                unsafe {
                    multipart::open(fixture.0.context, string("data"), options, &mut pointer)
                },
                4
            );
            assert_eq!(pointer, fixture.0.context);
        }
        let mut options = write_options(0);
        options.version = string("");
        let mut pointer = fixture.0.context;
        assert_eq!(
            unsafe { multipart::open(fixture.0.context, string("data"), options, &mut pointer) },
            4
        );
        assert!(store.put_options.lock().unwrap().is_empty());
        assert!(store.multipart_options.lock().unwrap().is_empty());
        assert!(output.etags.is_empty());
    }

    #[test]
    fn aggregate_range_output_and_delimiter_collections_fail_without_truncation_or_sink_calls() {
        let _serial = TEST_LOCK.lock().unwrap();
        let body = Bytes::from(vec![0; MAX_BODY_BYTES / 2]);
        let store = Arc::new(StoreSpy {
            range_results: Some(vec![body.clone(), body.clone(), body]),
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let input = [KernelNativeRangeV4 {
            start: 0,
            end: (MAX_BODY_BYTES / 2) as u64,
        }; 3];
        let mut output = Output::default();
        assert_eq!(ranges(&fixture, &input, &mut output), 4);
        assert!(output.bodies.is_empty());
        let context = create_memory().unwrap();
        let meta = runtime()
            .unwrap()
            .block_on(context.store.head(&Path::from(INITIAL_PATH)))
            .unwrap();
        for objects in [true, false] {
            let result = ListResult {
                objects: if objects {
                    vec![meta.clone(); 4097]
                } else {
                    vec![]
                },
                common_prefixes: if objects {
                    vec![]
                } else {
                    vec![Path::from("dir"); 4097]
                },
                extensions: Default::default(),
            };
            let fixture = Fixture::from_store(Arc::new(StoreSpy {
                delimiter_override: Some(result),
                ..Default::default()
            }));
            assert_eq!(delimiter(&fixture, "", &mut output), 4);
            assert!(output.objects.is_empty() && output.prefixes.is_empty());
        }
    }

    #[test]
    fn response_attribute_limits_are_checked_before_head_payload_or_sink_and_optional_results_keep_empty(
    ) {
        let _serial = TEST_LOCK.lock().unwrap();
        let oversized = "x".repeat(MAX_PATH_BYTES + 1);
        for attributes in [
            Attributes::from_iter([(Attribute::ContentType, oversized)]),
            (0..4097)
                .map(|index| (Attribute::Metadata(format!("key{index}").into()), "x"))
                .collect(),
        ] {
            let store = Arc::new(StoreSpy {
                response_attributes: Some(attributes),
                forbid_body: true,
                ..Default::default()
            });
            let fixture = Fixture::from_store(store.clone());
            fixture.put("data", b"x", 0);
            let mut options = v4_get(Default::default());
            options.base.head = 1;
            let (status, output) = read(&fixture, "data", options);
            assert_eq!(status, 4);
            assert!(output.objects.is_empty());
            assert_eq!(store.body_polls.load(Ordering::SeqCst), 0);
        }
        let store = InMemory::new();
        let mut result = runtime()
            .unwrap()
            .block_on(store.put(&Path::from("data"), Bytes::new().into()))
            .unwrap();
        result.e_tag = Some(String::new());
        result.version = None;
        let mut output = Output::default();
        assert_eq!(
            unsafe { marshalling::put_result(&result, output.context(), result_sink) },
            Ok(())
        );
        assert_eq!(output.etags, [Some(String::new())]);
        assert_eq!(output.versions, [None]);
    }

    #[test]
    fn sink_failure_is_generic_and_host_discards_partial_results_without_provider_rollback() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy {
            delete_case: "mixed",
            ..Default::default()
        });
        let fixture = Fixture::from_store(store.clone());
        let input = [string("one"), string("two"), string("three")];
        let mut output = Output {
            reject_at: Some(1),
            ..Default::default()
        };
        let status = delete(&fixture, &input, &mut output);
        assert_eq!((status, output.deletes.len()), (3, 2));
        if status != 0 {
            output.deletes.clear();
        }
        assert!(output.deletes.is_empty());
        let mut output = Output {
            reject_at: Some(0),
            ..Default::default()
        };
        assert_eq!(
            unsafe {
                fixture.0.put.unwrap()(
                    fixture.0.context,
                    string("data"),
                    bytes(b"written"),
                    write_options(0),
                    output.context(),
                    result_sink,
                )
            },
            3
        );
        output.etags.clear();
        output.versions.clear();
        assert_eq!(fixture.get("data").1.bodies, [b"written"]);
        let upload = Upload::open(&fixture, "multipart", write_options(0));
        let mut part = upload.part(b"value");
        assert_eq!(part.wait(), 0);
        assert_eq!(
            unsafe { multipart::complete(upload.0, output.context(), result_sink) },
            3
        );
        assert_eq!(fixture.get("multipart").1.bodies, [b"value"]);
    }

    #[test]
    fn multipart_invalid_handles_and_parts_leave_output_untouched_and_close_does_not_poll() {
        let _serial = TEST_LOCK.lock().unwrap();
        let probe = Arc::new(UploadProbe::default());
        let fixture = Fixture::from_store(Arc::new(StoreSpy {
            upload_probe: Some(probe.clone()),
            ..Default::default()
        }));
        let upload = Upload::open(&fixture, "data", write_options(0));
        let mut output = upload.0;
        for (body, expected) in [
            (
                KernelNativeByteSliceV1 {
                    ptr: ptr::null(),
                    len: 1,
                },
                3,
            ),
            (
                KernelNativeByteSliceV1 {
                    ptr: ptr::null(),
                    len: MAX_BODY_BYTES + 1,
                },
                4,
            ),
        ] {
            assert_eq!(
                unsafe { multipart::part_open(upload.0, body, &mut output) },
                expected
            );
            assert_eq!(output, upload.0);
        }
        let misaligned = std::ptr::NonNull::<u8>::dangling().as_ptr().cast();
        for handle in [ptr::null_mut(), misaligned] {
            assert_eq!(unsafe { multipart::part_wait(handle) }, 3);
            assert_eq!(unsafe { multipart::abort(handle) }, 3);
            assert_eq!(
                unsafe { multipart::part_open(handle, bytes(b"x"), &mut output) },
                3
            );
            assert_eq!(output, upload.0);
        }
        assert_eq!(
            unsafe { multipart::part_open(upload.0, bytes(b"x"), ptr::null_mut()) },
            3
        );
        let part = upload.part(b"not-polled");
        drop(part);
        drop(upload);
        assert!(probe.waited.lock().unwrap().is_empty());
        assert_eq!(probe.part_drops.load(Ordering::SeqCst), 1);
        assert_eq!(probe.upload_drops.load(Ordering::SeqCst), 1);
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.get("data").0, 1);
    }

    #[test]
    fn concurrent_provider_calls_keep_each_sink_serial_and_independent() {
        let _serial = TEST_LOCK.lock().unwrap();
        let store = Arc::new(StoreSpy::default());
        let workers = (0..8)
            .map(|index| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let fixture = Fixture::from_store(store);
                    let path = format!("data/{index}");
                    let body = index.to_string();
                    assert_eq!(fixture.put(&path, body.as_bytes(), 0), 0);
                    assert_eq!(fixture.get(&path).1.bodies, [body.as_bytes()]);
                    let mut output = Output::default();
                    assert_eq!(delete(&fixture, &[string(&path)], &mut output), 0);
                    assert_eq!(output.deletes, [(Some(path), 0)]);
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(store.put_options.lock().unwrap().len(), 8);
        assert_eq!(store.gets.lock().unwrap().len(), 8);
        assert_eq!(store.deletes.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn azure_loopback_gets_use_native_rotating_credentials_on_the_wire() {
        const CHILD_MARKER: &str = "NATIVE_PROVIDER_LOOPBACK_TEST_CHILD";
        if std::env::var_os(CHILD_MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "provider::tests::v4::azure_loopback_gets_use_native_rotating_credentials_on_the_wire",
                    "--nocapture",
                ])
                .env(CHILD_MARKER, "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stdout.contains("running 1 test"), "{stdout}\n{stderr}");
            assert!(output.status.success(), "{stdout}\n{stderr}");
            return;
        }
        let _serial = TEST_LOCK.lock().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut authorizations = Vec::new();
            for _ in 0..5 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut chunk = [0; 4096];
                    let length = socket.read(&mut chunk).unwrap();
                    assert_ne!(length, 0);
                    request.extend_from_slice(&chunk[..length]);
                }
                let request = String::from_utf8(request).unwrap();
                let authorization = request
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("authorization")
                            .then(|| value.trim().to_owned())
                    })
                    .unwrap();
                authorizations.push(authorization);
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"loop\"\r\nLast-Modified: Wed, 07 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\ndata").unwrap();
            }
            authorizations
        });
        let mut descriptor = MaybeUninit::uninit();
        assert_eq!(
            unsafe { prototype_create_azure(string(&endpoint), descriptor.as_mut_ptr()) },
            0
        );
        let fixture = Fixture(unsafe { descriptor.assume_init() });
        for _ in 0..5 {
            assert_eq!(fixture.get("data").1.bodies, [b"data"]);
        }
        let headers = server.join().unwrap();
        assert!(headers
            .iter()
            .all(|header| header.starts_with("Bearer native-token-")));
        assert_ne!(headers.first(), headers.last());
    }
}
