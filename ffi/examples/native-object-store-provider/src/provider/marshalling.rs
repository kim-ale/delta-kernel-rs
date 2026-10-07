use chrono::{DateTime, Utc};
use object_store::{Attribute, Attributes, PutResult, TagSet, UpdateVersion};

use super::*;

pub(super) struct WriteOptions {
    pub(super) mode: PutMode,
    pub(super) tags: TagSet,
    pub(super) attributes: Attributes,
}

pub(super) fn string_view(value: &str) -> KernelNativeStringSliceV1 {
    KernelNativeStringSliceV1 {
        ptr: value.as_ptr().cast(),
        len: value.len(),
    }
}

pub(super) fn optional_string(value: Option<&str>) -> KernelNativeStringSliceV1 {
    value.map_or(
        KernelNativeStringSliceV1 {
            ptr: std::ptr::null(),
            len: 0,
        },
        string_view,
    )
}

pub(super) fn charge(budget: &mut usize, bytes: usize) -> Result<(), i32> {
    *budget = budget
        .checked_add(bytes)
        .filter(|size| *size <= MAX_BODY_BYTES)
        .ok_or(KERNEL_NATIVE_STATUS_NOT_SUPPORTED)?;
    Ok(())
}

fn check_string(value: &str, budget: &mut usize) -> Result<(), i32> {
    if value.len() > MAX_PATH_BYTES {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    charge(budget, value.len())
}

pub(super) unsafe fn array<'input, Element>(
    pointer: *const Element,
    count: usize,
    limit: usize,
) -> Result<&'input [Element], i32> {
    if count > limit {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    if count == 0 {
        return Ok(&[]);
    }
    if pointer.is_null()
        || !pointer.is_aligned()
        || count
            .checked_mul(size_of::<Element>())
            .is_none_or(|size| size > isize::MAX as usize)
    {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    Ok(unsafe { slice::from_raw_parts(pointer, count) })
}

pub(super) unsafe fn copy_optional(
    value: KernelNativeStringSliceV1,
    budget: &mut usize,
) -> Result<Option<String>, i32> {
    charge(budget, value.len)?;
    if value.ptr.is_null() && value.len == 0 {
        return Ok(None);
    }
    unsafe { copy_string(value) }.map(Some)
}

pub(super) unsafe fn get_options(
    options: KernelNativeGetOptionsV4,
    budget: &mut usize,
) -> Result<GetOptions, i32> {
    if options.time_flags & !3 != 0
        || options.modified_nanos >= 1_000_000_000
        || options.unmodified_nanos >= 1_000_000_000
    {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    let mut result = native_get_options(options.base)?;
    result.if_match = unsafe { copy_optional(options.if_match, budget) }?;
    result.if_none_match = unsafe { copy_optional(options.if_none_match, budget) }?;
    result.version = unsafe { copy_optional(options.version, budget) }?;
    let timestamp = |present, seconds, nanos| {
        if present {
            DateTime::<Utc>::from_timestamp(seconds, nanos)
                .map(Some)
                .ok_or(KERNEL_NATIVE_STATUS_GENERIC)
        } else {
            Ok(None)
        }
    };
    result.if_modified_since = timestamp(
        options.time_flags & 1 != 0,
        options.modified_seconds,
        options.modified_nanos,
    )?;
    result.if_unmodified_since = timestamp(
        options.time_flags & 2 != 0,
        options.unmodified_seconds,
        options.unmodified_nanos,
    )?;
    Ok(result)
}

fn attribute(key: String) -> Result<Attribute, i32> {
    Ok(match key.as_str() {
        "content-disposition" => Attribute::ContentDisposition,
        "content-encoding" => Attribute::ContentEncoding,
        "content-language" => Attribute::ContentLanguage,
        "content-type" => Attribute::ContentType,
        "cache-control" => Attribute::CacheControl,
        "storage-class" => Attribute::StorageClass,
        key if key.starts_with("metadata:") => Attribute::Metadata(key[9..].to_owned().into()),
        _ => return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED),
    })
}

pub(super) unsafe fn write_options(
    options: KernelNativeWriteOptionsV4,
    multipart: bool,
    budget: &mut usize,
) -> Result<WriteOptions, i32> {
    let e_tag = unsafe { copy_optional(options.e_tag, budget) }?;
    let version = unsafe { copy_optional(options.version, budget) }?;
    if (multipart && options.mode != KERNEL_NATIVE_PUT_OVERWRITE)
        || (options.mode != KERNEL_NATIVE_PUT_UPDATE && (e_tag.is_some() || version.is_some()))
    {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    let mode = match options.mode {
        KERNEL_NATIVE_PUT_OVERWRITE => PutMode::Overwrite,
        KERNEL_NATIVE_PUT_CREATE => PutMode::Create,
        KERNEL_NATIVE_PUT_UPDATE if !multipart => PutMode::Update(UpdateVersion { e_tag, version }),
        _ => return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED),
    };
    let mut tags = TagSet::default();
    for pair in unsafe { array(options.tags, options.tags_len, KERNEL_NATIVE_MAX_COLLECTION) }? {
        charge(budget, pair.key.len)?;
        charge(budget, pair.value.len)?;
        let key = unsafe { copy_string(pair.key) }?;
        let value = unsafe { copy_string(pair.value) }?;
        tags.push(&key, &value);
    }
    let mut attributes = Attributes::new();
    for pair in unsafe {
        array(
            options.attributes,
            options.attributes_len,
            KERNEL_NATIVE_MAX_COLLECTION,
        )
    }? {
        charge(budget, pair.key.len)?;
        charge(budget, pair.value.len)?;
        let key = unsafe { copy_string(pair.key) }?;
        let value = unsafe { copy_string(pair.value) }?;
        if attributes.insert(attribute(key)?, value.into()).is_some() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
    }
    Ok(WriteOptions {
        mode,
        tags,
        attributes,
    })
}

pub(super) fn check_metadata(meta: &ObjectMeta, budget: &mut usize) -> Result<(), i32> {
    check_string(meta.location.as_ref(), budget)?;
    for value in [&meta.e_tag, &meta.version].into_iter().flatten() {
        check_string(value, budget)?;
    }
    Ok(())
}

pub(super) fn borrowed_metadata(meta: &ObjectMeta) -> KernelNativeObjectMetaV4 {
    KernelNativeObjectMetaV4 {
        base: KernelNativeObjectMetaV1 {
            location: string_view(meta.location.as_ref()),
            size: meta.size,
            last_modified_unix_ms: meta.last_modified.timestamp_millis(),
        },
        e_tag: optional_string(meta.e_tag.as_deref()),
        version: optional_string(meta.version.as_deref()),
    }
}

pub(super) fn attribute_pairs(
    attributes: &Attributes,
    budget: &mut usize,
) -> Result<Vec<(String, String)>, i32> {
    if attributes.len() > KERNEL_NATIVE_MAX_COLLECTION {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    attributes
        .iter()
        .map(|(key, value)| {
            let key = match key {
                Attribute::ContentDisposition => "content-disposition".to_owned(),
                Attribute::ContentEncoding => "content-encoding".to_owned(),
                Attribute::ContentLanguage => "content-language".to_owned(),
                Attribute::ContentType => "content-type".to_owned(),
                Attribute::CacheControl => "cache-control".to_owned(),
                Attribute::StorageClass => "storage-class".to_owned(),
                Attribute::Metadata(key) => {
                    if key.len() > MAX_PATH_BYTES - 9 {
                        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
                    }
                    format!("metadata:{key}")
                }
                _ => return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED),
            };
            check_string(&key, budget)?;
            check_string(value.as_ref(), budget)?;
            Ok((key, value.as_ref().to_owned()))
        })
        .collect()
}

pub(super) fn pair_views(pairs: &[(String, String)]) -> Vec<KernelNativeKeyValueV4> {
    pairs
        .iter()
        .map(|(key, value)| KernelNativeKeyValueV4 {
            key: string_view(key),
            value: string_view(value),
        })
        .collect()
}

pub(super) unsafe fn put_result(
    result: &PutResult,
    context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
) -> Result<(), i32> {
    let mut budget = 0;
    for value in [&result.e_tag, &result.version].into_iter().flatten() {
        check_string(value, &mut budget)?;
    }
    let result = KernelNativePutResultV4 {
        e_tag: optional_string(result.e_tag.as_deref()),
        version: optional_string(result.version.as_deref()),
    };
    sink_status(unsafe { sink(context, &result) })
}
