#[derive(Debug, PartialEq, Eq)]
struct GetConditions {
    if_match: Option<String>,
    if_none_match: Option<String>,
    version: Option<String>,
    modified: Option<(i64, u32)>,
    unmodified: Option<(i64, u32)>,
}

impl GetConditions {
    unsafe fn copy(options: KernelNativeGetOptionsV4) -> Self {
        Self {
            if_match: unsafe { copy_optional_string(options.if_match) }.unwrap(),
            if_none_match: unsafe { copy_optional_string(options.if_none_match) }.unwrap(),
            version: unsafe { copy_optional_string(options.version) }.unwrap(),
            modified: (options.time_flags & 1 != 0)
                .then_some((options.modified_seconds, options.modified_nanos)),
            unmodified: (options.time_flags & 2 != 0)
                .then_some((options.unmodified_seconds, options.unmodified_nanos)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct WriteRequest {
    mode: u32,
    e_tag: Option<String>,
    version: Option<String>,
    tags: Vec<(String, String)>,
    attributes: Vec<(String, String)>,
}

impl WriteRequest {
    unsafe fn copy(options: KernelNativeWriteOptionsV4) -> Self {
        let pairs = |ptr: *const KernelNativeKeyValueV4, len| {
            if len == 0 {
                return Vec::new();
            }
            unsafe { std::slice::from_raw_parts(ptr, len) }
                .iter()
                .map(|pair| {
                    (unsafe { input_string(pair.key) }, unsafe {
                        input_string(pair.value)
                    })
                })
                .collect()
        };
        let mut attributes = pairs(options.attributes, options.attributes_len);
        attributes.sort();
        Self {
            mode: options.mode,
            e_tag: unsafe { copy_optional_string(options.e_tag) }.unwrap(),
            version: unsafe { copy_optional_string(options.version) }.unwrap(),
            tags: pairs(options.tags, options.tags_len),
            attributes,
        }
    }
}

fn response_attributes(metadata: bool) -> Vec<(String, String)> {
    if !metadata {
        return Vec::new();
    }
    [
        ("content-disposition", "inline"),
        ("content-encoding", "gzip"),
        ("content-language", "en"),
        ("content-type", "application/json"),
        ("cache-control", "no-cache"),
        ("storage-class", "COOL"),
        ("metadata:user+key", "value +&=%"),
        ("metadata:empty", ""),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value.into()))
    .collect()
}

fn attribute_views(pairs: &[(String, String)]) -> Vec<KernelNativeKeyValueV4> {
    pairs
        .iter()
        .map(|(key, value)| KernelNativeKeyValueV4 {
            key: string_slice(key),
            value: string_slice(value),
        })
        .collect()
}

fn malformed_attributes(
    case: &str,
    attributes: &[KernelNativeKeyValueV4],
) -> (*const KernelNativeKeyValueV4, usize) {
    match case {
        "attributes_null" => (std::ptr::null(), 1),
        "attributes_align" => (NonNull::<u8>::dangling().as_ptr().cast(), 1),
        "attributes_count" => (attributes.as_ptr(), KERNEL_NATIVE_MAX_COLLECTION + 1),
        _ => (attributes.as_ptr(), attributes.len()),
    }
}

unsafe fn fixture_put_result(
    metadata: bool,
    case: &str,
    context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
) -> i32 {
    if case == "result_missing" {
        return KERNEL_NATIVE_STATUS_OK;
    }
    let e_tag = metadata.then(|| "put-etag".to_string());
    let version = metadata.then(|| "put-version".to_string());
    let mut result = KernelNativePutResultV4 {
        e_tag: marshalling::optional_string_slice(e_tag.as_deref()),
        version: marshalling::optional_string_slice(version.as_deref()),
    };
    if case == "result_string" {
        result.version = KernelNativeStringSliceV1 {
            ptr: std::ptr::null(),
            len: 1,
        };
    }
    let pointer = match case {
        "result_null" => std::ptr::null(),
        "result_align" => NonNull::<u8>::dangling().as_ptr().cast(),
        _ => &result,
    };
    let status = unsafe { sink(context, pointer) };
    if status == 0 && case == "result_duplicate" {
        return unsafe { sink(context, pointer) };
    }
    status
}

unsafe extern "C" fn provider_ranges(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    ranges: *const KernelNativeRangeV4,
    count: usize,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, usize, KernelNativeByteSliceV1) -> i32,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
    provider
        .probe
        .range_paths
        .lock()
        .unwrap()
        .push(unsafe { input_string(path) });
    let ranges = unsafe { std::slice::from_raw_parts(ranges, count) };
    provider
        .probe
        .range_requests
        .lock()
        .unwrap()
        .push(ranges.iter().map(|range| range.start..range.end).collect());
    if provider.status != 0 {
        return provider.status;
    }
    let count = if provider.v4_case == "ranges_missing" {
        count.saturating_sub(1)
    } else {
        count
    };
    for (index, range) in ranges.iter().take(count).enumerate() {
        let body = b"data";
        let start = (range.start as usize).min(body.len());
        let end = (range.end as usize).min(body.len());
        let slice = &body[start..end];
        let mut bytes = KernelNativeByteSliceV1 {
            ptr: slice.as_ptr(),
            len: slice.len(),
        };
        match provider.v4_case {
            "ranges_null" => {
                bytes.ptr = std::ptr::null();
                bytes.len = 1;
            }
            "ranges_long" => bytes.len = (range.end - range.start) as usize + 1,
            _ => {}
        }
        let index = if provider.v4_case == "ranges_order" {
            index + 1
        } else {
            index
        };
        let status = unsafe { sink(sink_context, index, bytes) };
        if status != 0 {
            return status;
        }
        if provider.v4_case == "ranges_duplicate" {
            return unsafe { sink(sink_context, index, bytes) };
        }
    }
    KERNEL_NATIVE_STATUS_OK
}

unsafe extern "C" fn provider_delimiter(
    context: *mut c_void,
    prefix: KernelNativeStringSliceV1,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(
        *mut c_void,
        *const KernelNativeObjectMetaV4,
        usize,
        *const KernelNativeStringSliceV1,
        usize,
    ) -> i32,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
    provider
        .probe
        .delimiter_requests
        .lock()
        .unwrap()
        .push(unsafe { input_string(prefix) });
    if provider.status != 0 {
        return provider.status;
    }
    if provider.v4_case == "delimiter_missing" {
        return 0;
    }
    let name = "table/object".to_string();
    let e_tag = "delimiter-etag".to_string();
    let version = "delimiter-version".to_string();
    let mut meta = fixture_meta(KernelNativeObjectMetaV1 {
        location: string_slice(&name),
        size: MAX_BODY_BYTES as u64 + 1,
        last_modified_unix_ms: -1,
    });
    meta.e_tag = string_slice(&e_tag);
    meta.version = string_slice(&version);
    let names = ["table/z".to_string(), "table/a".to_string()];
    let prefixes: Vec<_> = names.iter().map(|name| string_slice(name)).collect();
    let objects_ptr: *const KernelNativeObjectMetaV4 = if provider.v4_case == "delimiter_align" {
        NonNull::<u8>::dangling().as_ptr().cast()
    } else {
        &meta
    };
    let objects_len = if provider.v4_case == "delimiter_count" {
        KERNEL_NATIVE_MAX_COLLECTION + 1
    } else {
        1
    };
    let prefixes_ptr = if provider.v4_case == "delimiter_null" {
        std::ptr::null()
    } else {
        prefixes.as_ptr()
    };
    let status = unsafe {
        sink(
            sink_context,
            objects_ptr,
            objects_len,
            prefixes_ptr,
            prefixes.len(),
        )
    };
    if status == 0 && provider.v4_case == "delimiter_duplicate" {
        return unsafe {
            sink(
                sink_context,
                objects_ptr,
                objects_len,
                prefixes_ptr,
                prefixes.len(),
            )
        };
    }
    status
}

unsafe fn record_transfer(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
    rename: bool,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
    provider.probe.transfers.lock().unwrap().push((
        rename,
        unsafe { input_string(from) },
        unsafe { input_string(to) },
        mode,
    ));
    provider.status
}

unsafe extern "C" fn provider_copy(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
) -> i32 {
    unsafe { record_transfer(context, from, to, mode, false) }
}

unsafe extern "C" fn provider_rename(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
) -> i32 {
    unsafe { record_transfer(context, from, to, mode, true) }
}

struct FixtureUpload {
    probe: Arc<Probe>,
    status: i32,
    case: &'static str,
    metadata: bool,
    gate: Option<Arc<CallGate>>,
}

struct FixturePart {
    probe: Arc<Probe>,
    index: usize,
    status: i32,
    gate: Option<Arc<CallGate>>,
}

unsafe extern "C" fn provider_multipart_open(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    options: KernelNativeWriteOptionsV4,
    output: *mut *mut c_void,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.io_calls.fetch_add(1, Ordering::SeqCst);
    provider
        .probe
        .upload_paths
        .lock()
        .unwrap()
        .push(unsafe { input_string(path) });
    provider
        .probe
        .write_options
        .lock()
        .unwrap()
        .push(unsafe { WriteRequest::copy(options) });
    if provider.status != 0 {
        return provider.status;
    }
    if provider.v4_case == "open_gate" {
        let gate = provider.multipart_gate.as_ref().unwrap();
        gate.entered.send(()).unwrap();
        gate.resume.lock().unwrap().recv().unwrap();
    }
    if provider.v4_case == "upload_null" {
        return 0;
    }
    provider.probe.upload_opens.fetch_add(1, Ordering::SeqCst);
    unsafe {
        *output = Box::into_raw(Box::new(FixtureUpload {
            probe: provider.probe.clone(),
            status: provider.status,
            case: provider.v4_case,
            metadata: provider.metadata,
            gate: provider.multipart_gate.clone(),
        }))
        .cast();
    }
    0
}

unsafe extern "C" fn provider_part_open(
    upload: *mut c_void,
    bytes: KernelNativeByteSliceV1,
    output: *mut *mut c_void,
) -> i32 {
    let upload = unsafe { &*upload.cast::<FixtureUpload>() };
    if upload.case == "part_open_error" {
        return KERNEL_NATIVE_STATUS_PRECONDITION;
    }
    if upload.case == "part_null" {
        return 0;
    }
    let mut parts = upload.probe.parts.lock().unwrap();
    let index = parts.len();
    let body = if bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    };
    parts.push(body);
    unsafe {
        *output = Box::into_raw(Box::new(FixturePart {
            probe: upload.probe.clone(),
            index,
            status: if upload.case == "part_wait_error" {
                KERNEL_NATIVE_STATUS_GENERIC
            } else {
                0
            },
            gate: (upload.case == "wait_gate")
                .then(|| upload.gate.clone())
                .flatten(),
        }))
        .cast();
    }
    0
}

unsafe extern "C" fn provider_part_wait(part: *mut c_void) -> i32 {
    let part = unsafe { &*part.cast::<FixturePart>() };
    part.probe.part_waits.lock().unwrap().push(part.index);
    if let Some(gate) = &part.gate {
        gate.entered.send(()).unwrap();
        gate.resume.lock().unwrap().recv().unwrap();
    }
    part.status
}

unsafe extern "C" fn provider_part_close(part: *mut c_void) {
    let part = unsafe { Box::from_raw(part.cast::<FixturePart>()) };
    assert_eq!(part.probe.upload_closes.load(Ordering::SeqCst), 0);
    assert_eq!(part.probe.releases.load(Ordering::SeqCst), 0);
    part.probe.part_closes.fetch_add(1, Ordering::SeqCst);
}

unsafe extern "C" fn provider_complete(
    upload: *mut c_void,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
) -> i32 {
    let upload = unsafe { &*upload.cast::<FixtureUpload>() };
    upload.probe.completes.fetch_add(1, Ordering::SeqCst);
    if upload.case == "complete_error" {
        return KERNEL_NATIVE_STATUS_PRECONDITION;
    }
    if upload.case == "complete_gate" {
        let gate = upload.gate.as_ref().unwrap();
        gate.entered.send(()).unwrap();
        gate.resume.lock().unwrap().recv().unwrap();
    }
    unsafe { fixture_put_result(upload.metadata, upload.case, sink_context, sink) }
}

unsafe extern "C" fn provider_abort(upload: *mut c_void) -> i32 {
    let upload = unsafe { &*upload.cast::<FixtureUpload>() };
    upload.probe.aborts.fetch_add(1, Ordering::SeqCst);
    if upload.case == "abort_error" {
        return KERNEL_NATIVE_STATUS_NOT_IMPLEMENTED;
    }
    if upload.case == "abort_gate" {
        let gate = upload.gate.as_ref().unwrap();
        gate.entered.send(()).unwrap();
        gate.resume.lock().unwrap().recv().unwrap();
    }
    upload.status
}

unsafe extern "C" fn provider_upload_close(upload: *mut c_void) {
    let upload = unsafe { Box::from_raw(upload.cast::<FixtureUpload>()) };
    assert_eq!(upload.probe.releases.load(Ordering::SeqCst), 0);
    upload.probe.upload_closes.fetch_add(1, Ordering::SeqCst);
}

#[tokio::test]
async fn v4_native_range_batch_preserves_exact_request_order_without_get_coalescing() {
    let (handle, probe) = handle_for(Provider::default());
    let ranges = [2..4, 0..1, 0..1, 2..2, 1..20];
    let bodies = unsafe { handle.as_ref() }
        .get_ranges(&Path::from("table/a"), &ranges)
        .await
        .unwrap();
    assert_eq!(
        bodies,
        vec![
            Bytes::from_static(b"ta"),
            Bytes::from_static(b"d"),
            Bytes::from_static(b"d"),
            Bytes::new(),
            Bytes::from_static(b"ata")
        ]
    );
    assert_eq!(*probe.range_requests.lock().unwrap(), vec![ranges.to_vec()]);
    assert_eq!(*probe.range_paths.lock().unwrap(), vec!["table/a"]);
    assert!(probe.get_requests.lock().unwrap().is_empty());
    unsafe { free_native_object_store(handle) };
}

#[tokio::test]
async fn v4_empty_native_range_is_forwarded_without_get_fallback() {
    let (handle, probe) = handle_for(Provider::default());
    let range = 2..2;
    let bodies = unsafe { handle.as_ref() }
        .get_ranges(&Path::from("table/a"), std::slice::from_ref(&range))
        .await
        .unwrap();
    unsafe { free_native_object_store(handle) };
    assert_eq!(bodies, vec![Bytes::new()]);
    assert_eq!(*probe.range_requests.lock().unwrap(), vec![vec![2..2]]);
    assert!(probe.get_requests.lock().unwrap().is_empty());
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case("ranges_missing")]
#[case("ranges_order")]
#[case("ranges_duplicate")]
#[case("ranges_null")]
#[case("ranges_long")]
#[tokio::test]
async fn malformed_native_range_results_fail_without_partial_bodies(#[case] v4_case: &'static str) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        ..Default::default()
    });
    assert!(unsafe { handle.as_ref() }
        .get_ranges(&Path::from("table/a"), &[0..2, 2..4])
        .await
        .is_err());
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_ranges_empty_batch_is_forwarded_and_oversized_or_invalid_arrays_are_rejected() {
    let (handle, probe) = handle_for(Provider::default());
    let store = unsafe { handle.as_ref() };
    assert!(store
        .get_ranges(&Path::from("table/a"), &[])
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .get_ranges(
            &Path::from("table/a"),
            &vec![0..1; KERNEL_NATIVE_MAX_COLLECTION + 1]
        )
        .await
        .is_err());
    assert!(store
        .get_ranges(&Path::from("table/a"), &[Range { start: 2, end: 1 }])
        .await
        .is_err());
    assert_eq!(probe.range_requests.lock().unwrap().len(), 1);
    unsafe { free_native_object_store(handle) };
}

#[rstest]
#[case(1, vec![1])]
#[case(128, vec![128])]
#[case(300, vec![128, 128, 44])]
#[tokio::test]
async fn native_delete_runs_use_one_batch_callback_per_bounded_chunk(
    #[case] count: usize,
    #[case] batches: Vec<usize>,
) {
    let (handle, probe) = handle_for(Provider::default());
    let paths: Vec<_> = (0..count)
        .rev()
        .map(|index| Path::from(format!("table/{index}")))
        .collect();
    let output = unsafe { handle.as_ref() }
        .delete_stream(stream::iter(paths.iter().cloned().map(Ok).collect::<Vec<_>>()).boxed())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(output, paths);
    assert_eq!(*probe.delete_batches.lock().unwrap(), batches);
    unsafe { free_native_object_store(handle) };
}

#[tokio::test]
async fn native_delete_preserves_upstream_errors_and_flushes_ready_input_before_pending() {
    let (handle, probe) = handle_for(Provider::default());
    let input = stream::iter([
        Ok(Path::from("a")),
        Ok(Path::from("b")),
        Err(not_supported("upstream-marker")),
        Ok(Path::from("c")),
    ])
    .chain(stream::pending())
    .boxed();
    let mut output = unsafe { handle.as_ref() }.delete_stream(input);
    assert_eq!(output.next().await.unwrap().unwrap(), Path::from("a"));
    assert_eq!(output.next().await.unwrap().unwrap(), Path::from("b"));
    assert!(
        matches!(output.next().await.unwrap(), Err(ObjectStoreError::NotSupported { source }) if source.to_string().contains("upstream-marker"))
    );
    assert_eq!(output.next().await.unwrap().unwrap(), Path::from("c"));
    assert_eq!(*probe.delete_batches.lock().unwrap(), vec![2, 1]);
    drop(output);
    unsafe { free_native_object_store(handle) };
}

#[rstest]
#[case("aggregate", true)]
#[case("delete_partial_failure", true)]
#[case("delete_null", false)]
#[case("delete_missing", false)]
#[case("delete_extra", false)]
#[tokio::test]
async fn native_delete_accepts_aggregate_errors_but_rejects_malformed_sinks(
    #[case] v4_case: &'static str,
    #[case] aggregate: bool,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        ..Default::default()
    });
    let results = unsafe { handle.as_ref() }
        .delete_stream(stream::iter([Ok(Path::from("a")), Ok(Path::from("b"))]).boxed())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    if aggregate {
        assert!(
            matches!(&results[0], Err(ObjectStoreError::Generic { source, .. }) if source.to_string() == "native object store callback failed")
        );
    }
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

unsafe extern "C" fn mixed_delete_results(
    context: *mut c_void,
    paths: *const KernelNativeStringSliceV1,
    count: usize,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1, i32) -> i32,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.delete_batches.lock().unwrap().push(count);
    let paths = unsafe { std::slice::from_raw_parts(paths, count) };
    let results = [
        (paths[0], KERNEL_NATIVE_STATUS_NOT_FOUND),
        (paths[1], KERNEL_NATIVE_STATUS_OK),
        (
            marshalling::optional_string_slice(None),
            KERNEL_NATIVE_STATUS_PRECONDITION,
        ),
        (paths[2], KERNEL_NATIVE_STATUS_OK),
    ];
    for (path, status) in results {
        let accepted = unsafe { sink(sink_context, path, status) };
        if accepted != KERNEL_NATIVE_STATUS_OK {
            return accepted;
        }
    }
    if provider.v4_case == "delete_extra" {
        return unsafe { sink(sink_context, paths[0], KERNEL_NATIVE_STATUS_OK) };
    }
    KERNEL_NATIVE_STATUS_OK
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn v4_delete_preserves_interleaved_aggregate_errors_but_rejects_extra_path_results(
    #[case] extra_path: bool,
) {
    let (mut descriptor, probe) = descriptor_for(Provider {
        v4_case: if extra_path { "delete_extra" } else { "" },
        ..Default::default()
    });
    descriptor.delete_batch = Some(mixed_delete_results);
    let handle = ok_or_panic(unsafe { get_native_object_store(&descriptor, allocate_err) });
    let results = unsafe { handle.as_ref() }
        .delete_stream(stream::iter(["a", "b", "c"].map(|path| Ok(Path::from(path)))).boxed())
        .collect::<Vec<_>>()
        .await;
    unsafe { free_native_object_store(handle) };
    if extra_path {
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    } else {
        assert_eq!(results.len(), 4);
        assert!(matches!(
            &results[0],
            Err(ObjectStoreError::NotFound { path, .. }) if path == "a"
        ));
        assert_eq!(results[1].as_ref().unwrap(), &Path::from("b"));
        assert!(matches!(
            &results[2],
            Err(ObjectStoreError::Precondition { .. })
        ));
        assert_eq!(results[3].as_ref().unwrap(), &Path::from("c"));
    }
    assert_eq!(*probe.delete_batches.lock().unwrap(), vec![3]);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

unsafe extern "C" fn bounded_delete_results(
    context: *mut c_void,
    paths: *const KernelNativeStringSliceV1,
    count: usize,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1, i32) -> i32,
) -> i32 {
    let provider = unsafe { &*context.cast::<Provider>() };
    provider.probe.delete_batches.lock().unwrap().push(count);
    let paths = unsafe { std::slice::from_raw_parts(paths, count) };
    for path in paths {
        for (path, status) in [
            (*path, KERNEL_NATIVE_STATUS_OK),
            (
                marshalling::optional_string_slice(None),
                KERNEL_NATIVE_STATUS_GENERIC,
            ),
        ] {
            let accepted = unsafe { sink(sink_context, path, status) };
            if accepted != KERNEL_NATIVE_STATUS_OK {
                return accepted;
            }
        }
    }
    match provider.v4_case {
        "aggregate_overflow" => unsafe {
            sink(
                sink_context,
                marshalling::optional_string_slice(None),
                KERNEL_NATIVE_STATUS_GENERIC,
            )
        },
        "path_overflow" => unsafe { sink(sink_context, paths[0], KERNEL_NATIVE_STATUS_OK) },
        "outer_failure" => KERNEL_NATIVE_STATUS_GENERIC,
        _ => KERNEL_NATIVE_STATUS_OK,
    }
}

#[rstest]
#[case("")]
#[case("aggregate_overflow")]
#[case("path_overflow")]
#[case("outer_failure")]
#[tokio::test]
async fn v4_delete_path_and_aggregate_limits_are_independent_and_failures_discard_partial_output(
    #[case] v4_case: &'static str,
) {
    let (mut descriptor, probe) = descriptor_for(Provider {
        v4_case,
        ..Default::default()
    });
    descriptor.delete_batch = Some(bounded_delete_results);
    let handle = ok_or_panic(unsafe { get_native_object_store(&descriptor, allocate_err) });
    let paths: Vec<_> = (0..MAX_LIST_ITEMS)
        .map(|index| Path::from(format!("table/{index}")))
        .collect();
    let results = unsafe { handle.as_ref() }
        .delete_stream(stream::iter(paths.clone().into_iter().map(Ok)).boxed())
        .collect::<Vec<_>>()
        .await;
    unsafe { free_native_object_store(handle) };
    if v4_case.is_empty() {
        assert_eq!(results.len(), 2 * MAX_LIST_ITEMS);
        for (results, path) in results.chunks_exact(2).zip(&paths) {
            assert_eq!(results[0].as_ref().unwrap(), path);
            assert!(matches!(&results[1], Err(ObjectStoreError::Generic { .. })));
        }
    } else {
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], Err(ObjectStoreError::Generic { .. })));
        if v4_case == "outer_failure" {
            assert!(matches!(
                &results[0],
                Err(ObjectStoreError::Generic { source, .. })
                    if source.to_string() == "native object store callback failed"
            ));
        }
    }
    assert_eq!(*probe.delete_batches.lock().unwrap(), vec![MAX_LIST_ITEMS]);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case(false, false)]
#[case(false, true)]
#[case(true, false)]
#[case(true, true)]
#[tokio::test]
async fn copy_and_rename_forward_native_modes_without_copy_delete_emulation(
    #[case] rename: bool,
    #[case] create: bool,
) {
    let (handle, probe) = handle_for(Provider::default());
    let from = Path::from("table/a");
    let to = Path::from("table/b");
    let store = unsafe { handle.as_ref() };
    if rename {
        store
            .rename_opts(
                &from,
                &to,
                RenameOptions::new().with_target_mode(if create {
                    RenameTargetMode::Create
                } else {
                    RenameTargetMode::Overwrite
                }),
            )
            .await
            .unwrap();
    } else {
        store
            .copy_opts(
                &from,
                &to,
                CopyOptions::new().with_mode(if create {
                    CopyMode::Create
                } else {
                    CopyMode::Overwrite
                }),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        *probe.transfers.lock().unwrap(),
        vec![(rename, from.to_string(), to.to_string(), u32::from(create))]
    );
    assert!(probe.delete_batches.lock().unwrap().is_empty());
    unsafe { free_native_object_store(handle) };
}

#[tokio::test]
async fn native_delimiter_and_cursor_results_deep_copy_versions_and_preserve_prefix_order() {
    let (handle, probe) = handle_for(Provider {
        metadata: true,
        ..Default::default()
    });
    let store = unsafe { handle.as_ref() };
    let result = store
        .list_with_delimiter(Some(&Path::from("table")))
        .await
        .unwrap();
    let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    unsafe { free_native_object_store(handle) };
    assert_eq!(
        result.common_prefixes,
        vec![Path::from("table/z"), Path::from("table/a")]
    );
    assert_eq!(result.objects[0].e_tag.as_deref(), Some("delimiter-etag"));
    assert_eq!(
        result.objects[0].version.as_deref(),
        Some("delimiter-version")
    );
    assert!(result.objects[0].size > MAX_BODY_BYTES as u64);
    assert_eq!(objects[0].e_tag.as_deref(), Some("list-etag"));
    assert_eq!(objects[0].version.as_deref(), Some("list-version"));
    assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
    assert_eq!(*probe.delimiter_requests.lock().unwrap(), vec!["table"]);
}

#[rstest]
#[case("delimiter_missing")]
#[case("delimiter_duplicate")]
#[case("delimiter_align")]
#[case("delimiter_count")]
#[case("delimiter_null")]
#[tokio::test]
async fn malformed_delimiter_arrays_or_sink_counts_are_rejected(#[case] v4_case: &'static str) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        ..Default::default()
    });
    assert!(unsafe { handle.as_ref() }
        .list_with_delimiter(None)
        .await
        .is_err());
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn conditional_get_forwards_nanosecond_times_and_optional_strings_without_local_prechecks() {
    let (handle, probe) = handle_for(Provider {
        metadata: true,
        ..Default::default()
    });
    let store = unsafe { handle.clone_as_arc() };
    let result = store
        .get_opts(
            &Path::from("table/a"),
            GetOptions {
                if_match: Some("deliberately-not-the-response-etag".into()),
                if_none_match: Some(String::new()),
                version: Some("v+1".into()),
                if_modified_since: DateTime::from_timestamp(-1, 999_999_999),
                if_unmodified_since: DateTime::from_timestamp(1, 123_456_789),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    unsafe { free_native_object_store(handle) };
    drop(store);
    assert_eq!(
        *probe.get_conditions.lock().unwrap(),
        vec![GetConditions {
            if_match: Some("deliberately-not-the-response-etag".into()),
            if_none_match: Some(String::new()),
            version: Some("v+1".into()),
            modified: Some((-1, 999_999_999)),
            unmodified: Some((1, 123_456_789)),
        }]
    );
    assert_eq!(result.meta.e_tag.as_deref(), Some("native-etag"));
    assert_eq!(result.meta.version.as_deref(), Some(""));
    assert_eq!(result.attributes.len(), 8);
    assert_eq!(
        result
            .attributes
            .get(&object_store::Attribute::StorageClass)
            .unwrap()
            .as_ref(),
        "COOL"
    );
    assert_eq!(result.bytes().await.unwrap().as_ref(), b"data");
}

#[rstest]
#[case("attributes_null")]
#[case("attributes_align")]
#[case("attributes_count")]
#[case("optional_null")]
#[case("optional_utf8")]
#[tokio::test]
async fn malformed_v4_get_attributes_and_optional_strings_are_rejected(
    #[case] v4_case: &'static str,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        metadata: true,
        ..Default::default()
    });
    assert!(unsafe { handle.as_ref() }
        .get_opts(&Path::from("table/a"), Default::default())
        .await
        .is_err());
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

fn tagged_attributes() -> (TagSet, Attributes) {
    let mut tags = TagSet::default();
    tags.push("key +&=", "value +&=%");
    tags.push("empty", "");
    let attributes = Attributes::from_iter([
        (object_store::Attribute::ContentType, "application/json"),
        (object_store::Attribute::StorageClass, "COOL"),
        (
            object_store::Attribute::Metadata("user+key".into()),
            "value +&=%",
        ),
    ]);
    (tags, attributes)
}

#[tokio::test]
async fn update_put_and_multipart_options_preserve_encoded_tags_attributes_and_put_metadata() {
    let (handle, probe) = handle_for(Provider {
        metadata: true,
        ..Default::default()
    });
    let store = unsafe { handle.as_ref() };
    let (tags, attributes) = tagged_attributes();
    let result = store
        .put_opts(
            &Path::from("table/a"),
            b"data".as_slice().into(),
            PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some("etag".into()),
                    version: Some(String::new()),
                }),
                tags: tags.clone(),
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut upload = store
        .put_multipart_opts(
            &Path::from("table/b"),
            PutMultipartOptions {
                tags,
                attributes,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let complete = upload.complete().await.unwrap();
    drop(upload);
    unsafe { free_native_object_store(handle) };
    assert_eq!(result.e_tag.as_deref(), Some("put-etag"));
    assert_eq!(result.version.as_deref(), Some("put-version"));
    assert_eq!(complete.e_tag, result.e_tag);
    assert_eq!(complete.version, result.version);
    let requests = probe.write_options.lock().unwrap();
    assert_eq!(requests[0].mode, 2);
    assert_eq!(requests[0].e_tag.as_deref(), Some("etag"));
    assert_eq!(requests[0].version.as_deref(), Some(""));
    assert_eq!(
        requests[0].tags,
        vec![
            ("key +&=".into(), "value +&=%".into()),
            ("empty".into(), "".into())
        ]
    );
    assert_eq!(
        requests[0].attributes,
        vec![
            ("content-type".into(), "application/json".into()),
            ("metadata:user+key".into(), "value +&=%".into()),
            ("storage-class".into(), "COOL".into())
        ]
    );
    assert_eq!(requests[1].tags, requests[0].tags);
    assert_eq!(requests[1].attributes, requests[0].attributes);
    assert_eq!(requests[1].mode, 0);
    assert_eq!(requests[1].e_tag, None);
    assert_eq!(requests[1].version, None);
    assert_eq!(*probe.upload_paths.lock().unwrap(), vec!["table/b"]);
}

#[rstest]
#[case(MAX_PATH_BYTES - 9, true)]
#[case(MAX_PATH_BYTES - 8, false)]
#[case(MAX_PATH_BYTES + 1, false)]
#[tokio::test]
async fn v4_metadata_keys_include_the_prefix_in_the_limit_and_reject_oversized_input_before_io(
    #[case] key_length: usize,
    #[case] accepted: bool,
) {
    let (handle, probe) = handle_for(Provider::default());
    let attributes = Attributes::from_iter([(
        object_store::Attribute::Metadata("k".repeat(key_length).into()),
        "value",
    )]);
    let store = unsafe { handle.as_ref() };
    let put = store
        .put_opts(
            &Path::from("table/a"),
            PutPayload::new(),
            PutOptions {
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await;
    let multipart = store
        .put_multipart_opts(
            &Path::from("table/b"),
            PutMultipartOptions {
                attributes,
                ..Default::default()
            },
        )
        .await;
    if accepted {
        put.unwrap();
        drop(multipart.unwrap());
        let requests = probe.write_options.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert_eq!(request.attributes.len(), 1);
            assert_eq!(request.attributes[0].0.len(), MAX_PATH_BYTES);
            assert!(request.attributes[0].0.starts_with("metadata:"));
            assert_eq!(request.attributes[0].1, "value");
        }
    } else {
        assert!(matches!(put, Err(ObjectStoreError::NotSupported { .. })));
        assert!(matches!(
            multipart,
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(probe.write_options.lock().unwrap().is_empty());
    }
    assert_eq!(
        probe.io_calls.load(Ordering::SeqCst),
        2 * usize::from(accepted)
    );
    assert_eq!(
        probe.upload_closes.load(Ordering::SeqCst),
        usize::from(accepted)
    );
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case("result_missing")]
#[case("result_duplicate")]
#[case("result_null")]
#[case("result_align")]
#[case("result_string")]
#[tokio::test]
async fn malformed_put_and_completion_result_sinks_fail_and_close_upload(
    #[case] v4_case: &'static str,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        metadata: true,
        ..Default::default()
    });
    let store = unsafe { handle.as_ref() };
    assert!(store
        .put_opts(
            &Path::from("table/a"),
            PutPayload::new(),
            Default::default()
        )
        .await
        .is_err());
    let mut upload = store
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    assert!(upload.complete().await.is_err());
    drop(upload);
    assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    unsafe { free_native_object_store(handle) };
}

#[tokio::test]
async fn multipart_invocation_order_is_synchronous_even_when_part_futures_poll_out_of_order() {
    let (handle, probe) = handle_for(Provider {
        metadata: true,
        ..Default::default()
    });
    let mut upload = unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    unsafe { free_native_object_store(handle) };
    let first = upload.put_part(b"first".as_slice().into());
    let second = upload.put_part(b"second".as_slice().into());
    assert_eq!(
        *probe.parts.lock().unwrap(),
        vec![b"first".to_vec(), b"second".to_vec()]
    );
    assert!(probe.part_waits.lock().unwrap().is_empty());
    second.await.unwrap();
    first.await.unwrap();
    assert_eq!(*probe.part_waits.lock().unwrap(), vec![1, 0]);
    let result = upload.complete().await.unwrap();
    assert_eq!(result.version.as_deref(), Some("put-version"));
    drop(upload);
    assert_eq!(probe.part_closes.load(Ordering::SeqCst), 2);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn unpolled_parts_retain_upload_and_context_and_drop_never_implicitly_aborts(
    #[case] explicit_abort: bool,
) {
    let (handle, probe) = handle_for(Provider::default());
    let mut upload = unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    let part = upload.put_part(PutPayload::new());
    if explicit_abort {
        upload.abort().await.unwrap();
    }
    unsafe { free_native_object_store(handle) };
    drop(upload);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
    drop(part);
    assert_eq!(probe.part_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.aborts.load(Ordering::SeqCst),
        usize::from(explicit_abort)
    );
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case("part_open_error", 0)]
#[case("part_null", 0)]
#[case("part_wait_error", 1)]
#[tokio::test]
async fn multipart_part_errors_close_only_successfully_opened_handles(
    #[case] v4_case: &'static str,
    #[case] closes: usize,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        ..Default::default()
    });
    let mut upload = unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    assert!(upload.put_part(PutPayload::new()).await.is_err());
    drop(upload);
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.part_closes.load(Ordering::SeqCst), closes);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case("wait_gate")]
#[case("complete_gate")]
#[case("abort_gate")]
#[tokio::test]
async fn cancelled_multipart_workers_defer_part_upload_and_context_release_until_callback_returns(
    #[case] v4_case: &'static str,
) {
    let (entered, started) = std::sync::mpsc::channel();
    let (resume, waiting) = std::sync::mpsc::channel();
    let (released, finished) = std::sync::mpsc::channel();
    let (handle, probe) = handle_for(Provider {
        v4_case,
        multipart_gate: Some(Arc::new(CallGate {
            entered,
            resume: Mutex::new(waiting),
        })),
        ..Default::default()
    });
    *probe.released.lock().unwrap() = Some(released);
    let mut upload = unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    unsafe { free_native_object_store(handle) };
    let operation = if v4_case == "wait_gate" {
        let part = upload.put_part(PutPayload::new());
        drop(upload);
        tokio::spawn(part)
    } else {
        tokio::spawn(async move {
            if v4_case == "complete_gate" {
                upload.complete().await.map(|_| ())
            } else {
                upload.abort().await
            }
        })
    };
    tokio::task::spawn_blocking(move || started.recv().unwrap())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert_eq!(probe.part_closes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    tokio::task::spawn_blocking(move || finished.recv().unwrap())
        .await
        .unwrap();
    assert_eq!(
        probe.part_closes.load(Ordering::SeqCst),
        usize::from(v4_case == "wait_gate")
    );
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case(KERNEL_NATIVE_STATUS_PRECONDITION)]
#[case(KERNEL_NATIVE_STATUS_NOT_MODIFIED)]
#[case(KERNEL_NATIVE_STATUS_NOT_IMPLEMENTED)]
#[tokio::test]
async fn native_conditional_and_operation_errors_are_not_replaced_by_adapter_prechecks(
    #[case] status: i32,
) {
    let (handle, probe) = handle_for(Provider {
        status,
        ..Default::default()
    });
    let store = unsafe { handle.as_ref() };
    let options = GetOptions {
        if_match: Some("etag".into()),
        version: Some("version".into()),
        ..Default::default()
    };
    let range = 0..1;
    let results = [
        store
            .get_opts(&Path::from("table/a"), options)
            .await
            .map(|_| ()),
        store
            .copy_opts(
                &Path::from("table/a"),
                &Path::from("table/b"),
                Default::default(),
            )
            .await,
        store
            .rename_opts(
                &Path::from("table/a"),
                &Path::from("table/b"),
                Default::default(),
            )
            .await,
        store.list_with_delimiter(None).await.map(|_| ()),
        store
            .get_ranges(&Path::from("table/a"), std::slice::from_ref(&range))
            .await
            .map(|_| ()),
    ];
    for result in results {
        match status {
            KERNEL_NATIVE_STATUS_PRECONDITION => {
                assert!(matches!(result, Err(ObjectStoreError::Precondition { .. })))
            }
            KERNEL_NATIVE_STATUS_NOT_MODIFIED => {
                assert!(matches!(result, Err(ObjectStoreError::NotModified { .. })))
            }
            _ => assert!(matches!(
                result,
                Err(ObjectStoreError::NotImplemented { .. })
            )),
        }
    }
    assert_eq!(probe.io_calls.load(Ordering::SeqCst), 5);
    unsafe { free_native_object_store(handle) };
}

#[tokio::test]
async fn copy_rename_and_multipart_request_extensions_are_explicitly_rejected_without_io() {
    let (handle, probe) = handle_for(Provider::default());
    let store = unsafe { handle.as_ref() };
    let path = Path::from("table/a");
    let mut extensions = object_store::Extensions::new();
    extensions.insert(1u32);
    assert!(matches!(
        store
            .copy_opts(
                &path,
                &path,
                CopyOptions::new().with_extensions(extensions.clone())
            )
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert!(matches!(
        store
            .rename_opts(
                &path,
                &path,
                RenameOptions::new().with_extensions(extensions.clone())
            )
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert!(matches!(
        store
            .put_multipart_opts(
                &path,
                PutMultipartOptions {
                    extensions,
                    ..Default::default()
                }
            )
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
    unsafe { free_native_object_store(handle) };
}

#[rstest]
#[case("upload_null", KERNEL_NATIVE_STATUS_OK)]
#[case("", KERNEL_NATIVE_STATUS_NOT_IMPLEMENTED)]
#[tokio::test]
async fn failed_or_null_multipart_open_does_not_close_unowned_handles(
    #[case] v4_case: &'static str,
    #[case] status: i32,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        status,
        ..Default::default()
    });
    assert!(unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .is_err());
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 0);
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[rstest]
#[case("complete_error")]
#[case("abort_error")]
#[tokio::test]
async fn multipart_completion_and_abort_delegate_native_errors_without_implicit_cleanup(
    #[case] v4_case: &'static str,
) {
    let (handle, probe) = handle_for(Provider {
        v4_case,
        ..Default::default()
    });
    let mut upload = unsafe { handle.as_ref() }
        .put_multipart_opts(&Path::from("table/a"), Default::default())
        .await
        .unwrap();
    if v4_case == "complete_error" {
        assert!(
            matches!(upload.complete().await, Err(ObjectStoreError::Precondition { path, .. }) if path == "table/a")
        );
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    } else {
        assert!(matches!(
            upload.abort().await,
            Err(ObjectStoreError::NotImplemented { .. })
        ));
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
    }
    drop(upload);
    unsafe { free_native_object_store(handle) };
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_multipart_open_closes_successful_upload_after_callback_returns() {
    let (entered, started) = std::sync::mpsc::channel();
    let (resume, waiting) = std::sync::mpsc::channel();
    let (released, finished) = std::sync::mpsc::channel();
    let (handle, probe) = handle_for(Provider {
        v4_case: "open_gate",
        multipart_gate: Some(Arc::new(CallGate {
            entered,
            resume: Mutex::new(waiting),
        })),
        ..Default::default()
    });
    *probe.released.lock().unwrap() = Some(released);
    let store = unsafe { handle.clone_as_arc() };
    let operation = tokio::spawn(async move {
        store
            .put_multipart_opts(&Path::from("table/a"), Default::default())
            .await
    });
    unsafe { free_native_object_store(handle) };
    tokio::task::spawn_blocking(move || started.recv().unwrap())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert_eq!(probe.releases.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    tokio::task::spawn_blocking(move || finished.recv().unwrap())
        .await
        .unwrap();
    assert_eq!(probe.upload_opens.load(Ordering::SeqCst), 1);
    assert_eq!(probe.upload_closes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unrepresentable_get_conditions_write_arrays_and_multipart_parts_are_bounded_before_io() {
    let (handle, probe) = handle_for(Provider::default());
    let store = unsafe { handle.as_ref() };
    let path = Path::from("table/a");
    let leap = DateTime::from_timestamp(59, 1_000_000_000).unwrap();
    assert!(matches!(
        store
            .get_opts(
                &path,
                GetOptions {
                    if_modified_since: Some(leap),
                    ..Default::default()
                }
            )
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert!(matches!(
        store
            .get_opts(
                &path,
                GetOptions {
                    version: Some("x".repeat(MAX_PATH_BYTES + 1)),
                    ..Default::default()
                }
            )
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    let mut tags = TagSet::default();
    for _ in 0..=KERNEL_NATIVE_MAX_COLLECTION {
        tags.push("key", "value");
    }
    assert!(matches!(
        store.put_opts(&path, PutPayload::new(), tags.into()).await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    let attributes = Attributes::from_iter((0..=KERNEL_NATIVE_MAX_COLLECTION).map(|index| {
        (
            object_store::Attribute::Metadata(index.to_string().into()),
            "value",
        )
    }));
    assert!(matches!(
        store
            .put_opts(&path, PutPayload::new(), attributes.into())
            .await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    let options = PutOptions {
        mode: PutMode::Update(object_store::UpdateVersion {
            e_tag: Some("x".repeat(MAX_PATH_BYTES + 1)),
            version: None,
        }),
        ..Default::default()
    };
    assert!(matches!(
        store.put_opts(&path, PutPayload::new(), options).await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert_eq!(probe.io_calls.load(Ordering::SeqCst), 0);
    let mut upload = store
        .put_multipart_opts(&path, Default::default())
        .await
        .unwrap();
    let chunk = Bytes::from_static(&[0; 1024 * 1024]);
    let payload: PutPayload = std::iter::repeat_n(chunk, 65).collect();
    assert!(matches!(
        upload.put_part(payload).await,
        Err(ObjectStoreError::NotSupported { .. })
    ));
    assert!(probe.parts.lock().unwrap().is_empty());
    drop(upload);
    unsafe { free_native_object_store(handle) };
}
