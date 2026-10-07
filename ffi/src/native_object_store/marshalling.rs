use object_store::Attribute;

use super::*;

pub(super) struct OwnedWriteOptions {
    mode: PutMode,
    tags: Vec<(String, String)>,
    attributes: Vec<(String, String)>,
    request_bytes: usize,
}

impl OwnedWriteOptions {
    pub(super) fn from_put(options: PutOptions) -> ObjectStoreResult<Self> {
        if !options.extensions.is_empty() {
            return Err(not_supported("PUT request extensions"));
        }
        Self::new(options.mode, options.tags, options.attributes)
    }

    pub(super) fn from_multipart(options: PutMultipartOptions) -> ObjectStoreResult<Self> {
        if !options.extensions.is_empty() {
            return Err(not_supported("multipart request extensions"));
        }
        Self::new(PutMode::Overwrite, options.tags, options.attributes)
    }

    fn new(mode: PutMode, tags: TagSet, attributes: Attributes) -> ObjectStoreResult<Self> {
        let mut budget = 0;
        if let PutMode::Update(version) = &mode {
            for value in [&version.e_tag, &version.version].into_iter().flatten() {
                validate_input_path(value)?;
                charge(&mut budget, value.len())?;
            }
        }
        let mut pairs = Vec::new();
        for (key, value) in url::form_urlencoded::parse(tags.encoded().as_bytes()) {
            if pairs.len() >= KERNEL_NATIVE_MAX_COLLECTION {
                return Err(not_supported("more than 4096 tags"));
            }
            validate_input_path(&key)?;
            validate_input_path(&value)?;
            charge(&mut budget, key.len() + value.len())?;
            pairs.push((key.into_owned(), value.into_owned()));
        }
        if attributes.len() > KERNEL_NATIVE_MAX_COLLECTION {
            return Err(not_supported("more than 4096 attributes"));
        }
        let attributes = attributes
            .iter()
            .map(|(key, value)| {
                let key = match key {
                    Attribute::ContentDisposition => "content-disposition".into(),
                    Attribute::ContentEncoding => "content-encoding".into(),
                    Attribute::ContentLanguage => "content-language".into(),
                    Attribute::ContentType => "content-type".into(),
                    Attribute::CacheControl => "cache-control".into(),
                    Attribute::StorageClass => "storage-class".into(),
                    Attribute::Metadata(key) => {
                        if key.len() > MAX_PATH_BYTES - 9 {
                            return Err(not_supported("metadata key larger than 64 KiB"));
                        }
                        format!("metadata:{key}")
                    }
                    _ => return Err(not_supported("unknown native attribute")),
                };
                validate_input_path(&key)?;
                validate_input_path(value.as_ref())?;
                charge(&mut budget, key.len() + value.as_ref().len())?;
                Ok((key, value.as_ref().to_string()))
            })
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        Ok(Self {
            mode,
            tags: pairs,
            attributes,
            request_bytes: budget,
        })
    }

    pub(super) fn validate_payload(&self, length: usize) -> ObjectStoreResult<()> {
        self.request_bytes
            .checked_add(length)
            .filter(|size| *size <= MAX_BODY_BYTES)
            .ok_or_else(|| not_supported("PUT requests larger than 64 MiB in aggregate"))?;
        Ok(())
    }

    pub(super) fn with_view<T>(&self, call: impl FnOnce(KernelNativeWriteOptionsV4) -> T) -> T {
        let view = |pairs: &[(String, String)]| {
            pairs
                .iter()
                .map(|(key, value)| KernelNativeKeyValueV4 {
                    key: string_slice(key),
                    value: string_slice(value),
                })
                .collect::<Vec<_>>()
        };
        let tags = view(&self.tags);
        let attributes = view(&self.attributes);
        let (mode, e_tag, version) = match &self.mode {
            PutMode::Overwrite => (0, None, None),
            PutMode::Create => (1, None, None),
            PutMode::Update(update) => (2, update.e_tag.as_deref(), update.version.as_deref()),
        };
        call(KernelNativeWriteOptionsV4 {
            mode,
            e_tag: optional_string_slice(e_tag),
            version: optional_string_slice(version),
            tags: tags.as_ptr(),
            tags_len: tags.len(),
            attributes: attributes.as_ptr(),
            attributes_len: attributes.len(),
        })
    }
}

pub(super) fn optional_string_slice(value: Option<&str>) -> KernelNativeStringSliceV1 {
    value.map_or(
        KernelNativeStringSliceV1 {
            ptr: std::ptr::null(),
            len: 0,
        },
        string_slice,
    )
}

pub(super) fn charge(budget: &mut usize, bytes: usize) -> ObjectStoreResult<()> {
    *budget = budget
        .checked_add(bytes)
        .filter(|size| *size <= MAX_BODY_BYTES)
        .ok_or_else(|| generic_error("native output exceeded 64 MiB aggregate limit"))?;
    Ok(())
}

pub(super) fn meta_bytes(meta: &ObjectMeta) -> usize {
    meta.location.as_ref().len()
        + meta.e_tag.as_ref().map_or(0, String::len)
        + meta.version.as_ref().map_or(0, String::len)
}

pub(super) unsafe fn copy_string(value: KernelNativeStringSliceV1) -> ObjectStoreResult<String> {
    if value.len > MAX_PATH_BYTES || (value.len != 0 && value.ptr.is_null()) {
        return Err(generic_error(
            "invalid native UTF-8 string pointer or length",
        ));
    }
    if value.len == 0 {
        return Ok(String::new());
    }
    let bytes = unsafe { std::slice::from_raw_parts(value.ptr.cast::<u8>(), value.len) };
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| generic_error("native string is not UTF-8"))
}

pub(super) unsafe fn copy_optional_string(
    value: KernelNativeStringSliceV1,
) -> ObjectStoreResult<Option<String>> {
    if value.ptr.is_null() && value.len == 0 {
        return Ok(None);
    }
    unsafe { copy_string(value) }.map(Some)
}

unsafe fn array<'a, T>(ptr: *const T, len: usize, limit: usize) -> ObjectStoreResult<&'a [T]> {
    if len > limit || (len != 0 && (ptr.is_null() || !ptr.is_aligned())) {
        return Err(generic_error("invalid native array pointer or count"));
    }
    if len == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(super) unsafe fn copy_attributes(
    ptr: *const KernelNativeKeyValueV4,
    len: usize,
    budget: &mut usize,
) -> ObjectStoreResult<Attributes> {
    let mut attributes = Attributes::new();
    for pair in unsafe { array(ptr, len, KERNEL_NATIVE_MAX_COLLECTION) }? {
        let key = unsafe { copy_string(pair.key) }?;
        let value = unsafe { copy_string(pair.value) }?;
        charge(budget, key.len() + value.len())?;
        let attribute = match key.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            key if key.starts_with("metadata:") => Attribute::Metadata(key[9..].to_string().into()),
            _ => return Err(not_supported("unknown native response attribute")),
        };
        if attributes.insert(attribute, value.into()).is_some() {
            return Err(generic_error("duplicate native response attribute"));
        }
    }
    Ok(attributes)
}

#[derive(Default)]
pub(super) struct PutSinkState {
    called: bool,
    output: Option<PutResult>,
    error: Option<ObjectStoreError>,
}

impl PutSinkState {
    pub(super) fn finish(self, status: i32, path: &str) -> ObjectStoreResult<PutResult> {
        if let Some(error) = self.error {
            return Err(error);
        }
        check_status(status, path)?;
        self.output
            .ok_or_else(|| generic_error("native PUT result sink was not called exactly once"))
    }
}

pub(super) unsafe extern "C" fn put_sink(
    context: *mut c_void,
    result: *const KernelNativePutResultV4,
) -> i32 {
    let state = unsafe { &mut *context.cast::<PutSinkState>() };
    let output = (|| {
        if state.called {
            return Err(generic_error("duplicate native PUT result sink"));
        }
        state.called = true;
        if result.is_null() || !result.is_aligned() {
            return Err(generic_error("invalid native PUT result pointer"));
        }
        let result = unsafe { &*result };
        let mut output = object_store::delta_kernel_compat::empty_put_result();
        output.e_tag = unsafe { copy_optional_string(result.e_tag) }?;
        output.version = unsafe { copy_optional_string(result.version) }?;
        Ok(output)
    })();
    match output {
        Ok(output) => {
            state.output = Some(output);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

struct RangesSinkState<'a> {
    ranges: &'a [Range<u64>],
    bodies: Vec<Bytes>,
    bytes: usize,
    error: Option<ObjectStoreError>,
}

unsafe extern "C" fn ranges_sink(
    context: *mut c_void,
    index: usize,
    body: KernelNativeByteSliceV1,
) -> i32 {
    let state = unsafe { &mut *context.cast::<RangesSinkState<'_>>() };
    if state.error.is_some() {
        return KERNEL_NATIVE_STATUS_GENERIC;
    }
    let output = (|| {
        let range = state
            .ranges
            .get(index)
            .ok_or_else(|| generic_error("native range result index out of bounds"))?;
        if index != state.bodies.len()
            || body.len as u64 > range.end - range.start
            || (body.len != 0 && body.ptr.is_null())
        {
            return Err(generic_error(
                "invalid native range result order, body or pointer",
            ));
        }
        charge(&mut state.bytes, body.len)?;
        Ok(if body.len == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(body.ptr, body.len) })
        })
    })();
    match output {
        Ok(body) => {
            state.bodies.push(body);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

struct DeleteSinkState {
    limit: usize,
    path_results: usize,
    aggregate_results: usize,
    output: Vec<ObjectStoreResult<Path>>,
    error: Option<ObjectStoreError>,
    bytes: usize,
}

unsafe extern "C" fn delete_sink(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    status: i32,
) -> i32 {
    let state = unsafe { &mut *context.cast::<DeleteSinkState>() };
    if state.error.is_some() {
        return KERNEL_NATIVE_STATUS_GENERIC;
    }
    let output = (|| {
        if status == KERNEL_NATIVE_STATUS_OK && path.ptr.is_null() {
            return Err(generic_error("native DELETE success requires a path"));
        }
        if path.ptr.is_null() && path.len == 0 {
            state.aggregate_results += 1;
            if state.aggregate_results > MAX_LIST_ITEMS {
                return Err(generic_error(
                    "native DELETE exceeded aggregate error limit",
                ));
            }
        } else {
            state.path_results += 1;
            if state.path_results > state.limit {
                return Err(generic_error(
                    "native DELETE exceeded per-path result limit",
                ));
            }
        }
        let path = unsafe { copy_string(path) }?;
        charge(&mut state.bytes, path.len())?;
        let result = check_status(status, &path).and_then(|()| {
            let parsed =
                Path::parse(&path).map_err(|_| generic_error("invalid native DELETE path"))?;
            if parsed.as_ref() != path {
                return Err(generic_error("native DELETE path is not store-relative"));
            }
            Ok(parsed)
        });
        Ok(result)
    })();
    match output {
        Ok(result) => {
            state.output.push(result);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

pub(super) async fn flush_deletes(
    store: &NativeObjectStore,
    run: &mut Vec<Path>,
    output: &mut Vec<ObjectStoreResult<Path>>,
) {
    if run.is_empty() {
        return;
    }
    let paths = std::mem::take(run);
    let store = store.clone();
    match run_blocking(move || store.delete_batch_sync(&paths)).await {
        Ok(results) => output.extend(results),
        Err(error) => output.push(Err(error)),
    }
}

#[derive(Default)]
struct DelimiterSinkState {
    called: bool,
    output: Option<ListResult>,
    error: Option<ObjectStoreError>,
}

unsafe extern "C" fn delimiter_sink(
    context: *mut c_void,
    objects: *const KernelNativeObjectMetaV4,
    objects_len: usize,
    prefixes: *const KernelNativeStringSliceV1,
    prefixes_len: usize,
) -> i32 {
    let state = unsafe { &mut *context.cast::<DelimiterSinkState>() };
    let output = (|| {
        if state.called {
            return Err(generic_error("duplicate native delimiter sink"));
        }
        state.called = true;
        let objects = unsafe { array(objects, objects_len, KERNEL_NATIVE_MAX_COLLECTION) }?;
        let prefixes = unsafe { array(prefixes, prefixes_len, KERNEL_NATIVE_MAX_COLLECTION) }?;
        let mut budget = 0;
        let objects = objects
            .iter()
            .map(|meta| {
                let meta = unsafe { copy_meta(meta) }?;
                charge(&mut budget, meta_bytes(&meta))?;
                Ok(meta)
            })
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        let common_prefixes = prefixes
            .iter()
            .map(|prefix| {
                let prefix = unsafe { copy_string(*prefix) }?;
                charge(&mut budget, prefix.len())?;
                let parsed = Path::parse(&prefix)
                    .map_err(|_| generic_error("invalid native delimiter prefix"))?;
                if parsed.as_ref() != prefix {
                    return Err(generic_error(
                        "native delimiter prefix is not store-relative",
                    ));
                }
                Ok(parsed)
            })
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        Ok(ListResult {
            common_prefixes,
            objects,
            #[cfg(feature = "arrow-60")]
            extensions: Default::default(),
        })
    })();
    match output {
        Ok(output) => {
            state.output = Some(output);
            KERNEL_NATIVE_STATUS_OK
        }
        Err(error) => {
            state.error = Some(error);
            KERNEL_NATIVE_STATUS_GENERIC
        }
    }
}

impl NativeObjectStore {
    pub(super) fn get_ranges_sync(
        &self,
        path: &str,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        if ranges.len() > KERNEL_NATIVE_MAX_COLLECTION {
            return Err(not_supported("more than 4096 ranges"));
        }
        for range in ranges {
            if range.start > range.end {
                return Err(generic_error("invalid requested GET range"));
            }
        }
        let input: Vec<_> = ranges
            .iter()
            .map(|range| KernelNativeRangeV4 {
                start: range.start,
                end: range.end,
            })
            .collect();
        let mut state = RangesSinkState {
            ranges,
            bodies: Vec::new(),
            bytes: 0,
            error: None,
        };
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .get_ranges
            .ok_or_else(|| generic_error("native GET ranges callback is missing"))?;
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(path),
                input.as_ptr(),
                input.len(),
                (&mut state as *mut RangesSinkState<'_>).cast(),
                ranges_sink,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, path)?;
        if state.bodies.len() != ranges.len() {
            return Err(generic_error("native GET ranges result count mismatch"));
        }
        Ok(state.bodies)
    }

    fn delete_batch_sync(&self, paths: &[Path]) -> ObjectStoreResult<Vec<ObjectStoreResult<Path>>> {
        if paths.len() > MAX_LIST_ITEMS {
            return Err(generic_error("native DELETE input batch exceeds 128"));
        }
        let input: Vec<_> = paths
            .iter()
            .map(|path| string_slice(path.as_ref()))
            .collect();
        let mut state = DeleteSinkState {
            limit: paths.len(),
            path_results: 0,
            aggregate_results: 0,
            output: Vec::new(),
            error: None,
            bytes: 0,
        };
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .delete_batch
            .ok_or_else(|| generic_error("native DELETE callback is missing"))?;
        let status = unsafe {
            callback(
                descriptor.context,
                input.as_ptr(),
                input.len(),
                (&mut state as *mut DeleteSinkState).cast(),
                delete_sink,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, paths.first().map_or("", |path| path.as_ref()))?;
        if state.output.len() < paths.len() && state.output.iter().all(Result::is_ok) {
            return Err(generic_error(
                "native DELETE missing results without an aggregate error",
            ));
        }
        Ok(state.output)
    }

    pub(super) fn delimiter_sync(&self, prefix: &str) -> ObjectStoreResult<ListResult> {
        validate_input_path(prefix)?;
        let descriptor = &self.context.descriptor;
        let callback = descriptor
            .list_delimiter
            .ok_or_else(|| generic_error("native delimiter callback is missing"))?;
        let mut state = DelimiterSinkState::default();
        let status = unsafe {
            callback(
                descriptor.context,
                string_slice(prefix),
                (&mut state as *mut DelimiterSinkState).cast(),
                delimiter_sink,
            )
        };
        if let Some(error) = state.error {
            return Err(error);
        }
        check_status(status, prefix)?;
        state
            .output
            .ok_or_else(|| generic_error("native delimiter sink was not called exactly once"))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("future-unknown", true)]
    #[case("storage-class", false)]
    #[case("metadata:empty", false)]
    fn native_response_attributes_are_preserved_or_explicitly_unsupported(
        #[case] key: &str,
        #[case] unknown: bool,
    ) {
        let pairs = [KernelNativeKeyValueV4 {
            key: string_slice(key),
            value: string_slice(""),
        }];
        let result = unsafe { copy_attributes(pairs.as_ptr(), pairs.len(), &mut 0) };
        if unknown {
            assert!(matches!(result, Err(ObjectStoreError::NotSupported { .. })));
        } else {
            assert_eq!(result.unwrap().len(), 1);
        }
    }

    #[test]
    fn attributes_reject_duplicate_keys_invalid_utf8_and_aggregate_output_overflow() {
        let pair = KernelNativeKeyValueV4 {
            key: string_slice("content-type"),
            value: string_slice("text/plain"),
        };
        let pairs = [pair, pair];
        assert!(unsafe { copy_attributes(pairs.as_ptr(), 2, &mut 0) }.is_err());
        let mut budget = MAX_BODY_BYTES;
        assert!(unsafe { copy_attributes(pairs.as_ptr(), 1, &mut budget) }.is_err());
        let invalid = [0xffu8];
        let pairs = [KernelNativeKeyValueV4 {
            key: pair.key,
            value: KernelNativeStringSliceV1 {
                ptr: invalid.as_ptr().cast(),
                len: 1,
            },
        }];
        assert!(unsafe { copy_attributes(pairs.as_ptr(), 1, &mut 0) }.is_err());
    }

    #[rstest]
    #[case(false, 0, None)]
    #[case(true, 0, Some(""))]
    #[case(true, 4, Some("etag"))]
    fn optional_native_strings_preserve_null_vs_present_empty(
        #[case] present: bool,
        #[case] length: usize,
        #[case] expected: Option<&str>,
    ) {
        let string = KernelNativeStringSliceV1 {
            ptr: if present {
                b"etag".as_ptr().cast()
            } else {
                std::ptr::null()
            },
            len: length,
        };
        assert_eq!(
            unsafe { copy_optional_string(string) }.unwrap().as_deref(),
            expected
        );
    }

    #[test]
    fn range_sink_rejects_aggregate_body_limit_before_reading_or_allocating() {
        let range = 0..2;
        let mut state = RangesSinkState {
            ranges: std::slice::from_ref(&range),
            bodies: Vec::new(),
            bytes: MAX_BODY_BYTES - 1,
            error: None,
        };
        let status = unsafe {
            ranges_sink(
                (&mut state as *mut RangesSinkState<'_>).cast(),
                0,
                KernelNativeByteSliceV1 {
                    ptr: b"12".as_ptr(),
                    len: 2,
                },
            )
        };
        assert_eq!(status, KERNEL_NATIVE_STATUS_GENERIC);
        assert!(state.bodies.is_empty());
        assert!(state.error.is_some());
    }

    #[test]
    fn all_native_attribute_keys_round_trip_without_losing_storage_class_or_metadata() {
        let attributes = Attributes::from_iter([
            (Attribute::ContentDisposition, "inline"),
            (Attribute::ContentEncoding, "gzip"),
            (Attribute::ContentLanguage, "en"),
            (Attribute::ContentType, "text/plain"),
            (Attribute::CacheControl, "no-cache"),
            (Attribute::StorageClass, "COOL"),
            (Attribute::Metadata("key".into()), ""),
        ]);
        let options = OwnedWriteOptions::from_put(attributes.clone().into()).unwrap();
        options.with_view(|view| {
            let copied =
                unsafe { copy_attributes(view.attributes, view.attributes_len, &mut 0) }.unwrap();
            assert_eq!(copied, attributes);
        });
    }

    #[test]
    fn put_body_and_write_options_share_one_aggregate_request_budget() {
        let mut tags = TagSet::default();
        tags.push("key", "value");
        let options = OwnedWriteOptions::from_put(tags.into()).unwrap();
        assert!(options.validate_payload(MAX_BODY_BYTES - 8).is_ok());
        assert!(matches!(
            options.validate_payload(MAX_BODY_BYTES - 7),
            Err(ObjectStoreError::NotSupported { .. })
        ));
        assert!(options.validate_payload(usize::MAX).is_err());
    }
}
