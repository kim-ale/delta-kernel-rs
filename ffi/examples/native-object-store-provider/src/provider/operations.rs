use futures::{stream, StreamExt};
use object_store::{CopyMode, CopyOptions, RenameOptions, RenameTargetMode};

use super::*;

pub(super) unsafe extern "C" fn get_ranges(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    ranges: *const KernelNativeRangeV4,
    count: usize,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, usize, KernelNativeByteSliceV1) -> i32,
) -> i32 {
    guarded(|| {
        GETS.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let context = unsafe { context_ref(context) }?;
        let path =
            Path::parse(unsafe { copy_path(path) }?).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let ranges = unsafe { marshalling::array(ranges, count, KERNEL_NATIVE_MAX_COLLECTION) }?;
        let ranges = ranges
            .iter()
            .map(|range| {
                if range.start > range.end {
                    return Err(KERNEL_NATIVE_STATUS_GENERIC);
                }
                Ok(range.start..range.end)
            })
            .collect::<Result<Vec<_>, i32>>()?;
        let bodies = runtime()?
            .block_on(context.store.get_ranges(&path, &ranges))
            .map_err(error_status)?;
        if bodies.len() != ranges.len() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let mut budget = 0;
        for (body, range) in bodies.iter().zip(&ranges) {
            if body.len() as u64 > range.end - range.start {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
            charge(&mut budget, body.len())?;
        }
        for (index, body) in bodies.iter().enumerate() {
            sink_status(unsafe {
                sink(
                    sink_context,
                    index,
                    KernelNativeByteSliceV1 {
                        ptr: body.as_ptr(),
                        len: body.len(),
                    },
                )
            })?;
        }
        Ok(())
    })
}

pub(super) unsafe extern "C" fn delete_batch(
    context: *mut c_void,
    paths: *const KernelNativeStringSliceV1,
    count: usize,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, KernelNativeStringSliceV1, i32) -> i32,
) -> i32 {
    guarded(|| {
        DELETES.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let context = unsafe { context_ref(context) }?;
        let input = unsafe { marshalling::array(paths, count, MAX_LIST_ITEMS) }?;
        let mut budget = 0;
        let paths = input
            .iter()
            .map(|path| {
                charge(&mut budget, path.len)?;
                Path::parse(unsafe { copy_path(*path) }?).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)
            })
            .collect::<Result<Vec<_>, i32>>()?;
        runtime()?.block_on(async {
            let mut results = context
                .store
                .delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed());
            let mut budget = 0;
            let mut path_results = 0;
            let mut aggregate_results = 0;
            while let Some(result) = results.next().await {
                let (path, status) = match &result {
                    Ok(path) => (Some(path.as_ref()), KERNEL_NATIVE_STATUS_OK),
                    Err(error) => (error_path(error), error_status_ref(error)),
                };
                if path.is_some() {
                    path_results += 1;
                    if path_results > count {
                        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
                    }
                } else {
                    aggregate_results += 1;
                    if aggregate_results > MAX_LIST_ITEMS {
                        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
                    }
                }
                if let Some(path) = path {
                    if path.len() > MAX_PATH_BYTES {
                        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
                    }
                    charge(&mut budget, path.len())?;
                }
                sink_status(unsafe { sink(sink_context, optional_string(path), status) })?;
            }
            Ok(())
        })
    })
}

fn error_path(error: &object_store::Error) -> Option<&str> {
    match error {
        object_store::Error::NotFound { path, .. }
        | object_store::Error::AlreadyExists { path, .. }
        | object_store::Error::Precondition { path, .. }
        | object_store::Error::NotModified { path, .. }
        | object_store::Error::PermissionDenied { path, .. }
        | object_store::Error::Unauthenticated { path, .. } => Some(path),
        _ => None,
    }
}

pub(super) fn error_status_ref(error: &object_store::Error) -> i32 {
    match error {
        object_store::Error::NotFound { .. } => KERNEL_NATIVE_STATUS_NOT_FOUND,
        object_store::Error::AlreadyExists { .. } => KERNEL_NATIVE_STATUS_ALREADY_EXISTS,
        object_store::Error::NotSupported { .. } => KERNEL_NATIVE_STATUS_NOT_SUPPORTED,
        object_store::Error::Precondition { .. } => KERNEL_NATIVE_STATUS_PRECONDITION,
        object_store::Error::NotModified { .. } => KERNEL_NATIVE_STATUS_NOT_MODIFIED,
        object_store::Error::NotImplemented { .. } => KERNEL_NATIVE_STATUS_NOT_IMPLEMENTED,
        _ => KERNEL_NATIVE_STATUS_GENERIC,
    }
}

pub(super) unsafe extern "C" fn list_delimiter(
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
    guarded(|| {
        LISTS.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let context = unsafe { context_ref(context) }?;
        let prefix = unsafe { copy_path(prefix) }?;
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(Path::parse(prefix).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?)
        };
        let result = runtime()?
            .block_on(context.store.list_with_delimiter(prefix.as_ref()))
            .map_err(error_status)?;
        if result.objects.len() > KERNEL_NATIVE_MAX_COLLECTION
            || result.common_prefixes.len() > KERNEL_NATIVE_MAX_COLLECTION
        {
            return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
        }
        let mut budget = 0;
        for meta in &result.objects {
            check_metadata(meta, &mut budget)?;
        }
        for prefix in &result.common_prefixes {
            if prefix.as_ref().len() > MAX_PATH_BYTES {
                return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
            }
            charge(&mut budget, prefix.as_ref().len())?;
        }
        let objects = result
            .objects
            .iter()
            .map(borrowed_metadata)
            .collect::<Vec<_>>();
        let prefixes = result
            .common_prefixes
            .iter()
            .map(|prefix| string_view(prefix.as_ref()))
            .collect::<Vec<_>>();
        sink_status(unsafe {
            sink(
                sink_context,
                objects.as_ptr(),
                objects.len(),
                prefixes.as_ptr(),
                prefixes.len(),
            )
        })
    })
}

unsafe fn transfer(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
    rename: bool,
) -> Result<(), i32> {
    let context = unsafe { context_ref(context) }?;
    let from =
        Path::parse(unsafe { copy_path(from) }?).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
    let to = Path::parse(unsafe { copy_path(to) }?).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
    if mode > 1 {
        return Err(KERNEL_NATIVE_STATUS_NOT_SUPPORTED);
    }
    if rename {
        runtime()?
            .block_on(context.store.rename_opts(
                &from,
                &to,
                RenameOptions {
                    target_mode: if mode == 0 {
                        RenameTargetMode::Overwrite
                    } else {
                        RenameTargetMode::Create
                    },
                    ..Default::default()
                },
            ))
            .map_err(error_status)
    } else {
        runtime()?
            .block_on(context.store.copy_opts(
                &from,
                &to,
                CopyOptions {
                    mode: if mode == 0 {
                        CopyMode::Overwrite
                    } else {
                        CopyMode::Create
                    },
                    ..Default::default()
                },
            ))
            .map_err(error_status)
    }
}

pub(super) unsafe extern "C" fn copy(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        unsafe { transfer(context, from, to, mode, false) }
    })
}

pub(super) unsafe extern "C" fn rename(
    context: *mut c_void,
    from: KernelNativeStringSliceV1,
    to: KernelNativeStringSliceV1,
    mode: u32,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        unsafe { transfer(context, from, to, mode, true) }
    })
}
