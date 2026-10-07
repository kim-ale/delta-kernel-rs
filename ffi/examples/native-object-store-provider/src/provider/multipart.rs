use std::sync::Mutex;

use object_store::{MultipartUpload, PutMultipartOptions, UploadPart};

use super::*;

struct UploadState {
    upload: Box<dyn MultipartUpload>,
    _store: Arc<dyn ObjectStore>,
}

struct ProviderUpload {
    state: Arc<Mutex<UploadState>>,
}

struct ProviderPart {
    future: Option<UploadPart>,
    _parent: Arc<Mutex<UploadState>>,
}

unsafe fn upload_ref<'handle>(upload: *mut c_void) -> Result<&'handle ProviderUpload, i32> {
    if upload.is_null() || !upload.cast::<ProviderUpload>().is_aligned() {
        return Err(KERNEL_NATIVE_STATUS_GENERIC);
    }
    Ok(unsafe { &*upload.cast::<ProviderUpload>() })
}

pub(super) unsafe extern "C" fn open(
    context: *mut c_void,
    path: KernelNativeStringSliceV1,
    options: KernelNativeWriteOptionsV4,
    output: *mut *mut c_void,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        if output.is_null() || !output.is_aligned() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let context = unsafe { context_ref(context) }?;
        let path =
            Path::parse(unsafe { copy_path(path) }?).map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let mut budget = path.as_ref().len();
        let options = unsafe { marshalling::write_options(options, true, &mut budget) }?;
        let upload = runtime()?
            .block_on(context.store.put_multipart_opts(
                &path,
                PutMultipartOptions {
                    tags: options.tags,
                    attributes: options.attributes,
                    ..Default::default()
                },
            ))
            .map_err(error_status)?;
        let upload = Box::new(ProviderUpload {
            state: Arc::new(Mutex::new(UploadState {
                upload,
                _store: context.store.clone(),
            })),
        });
        unsafe { output.write(Box::into_raw(upload).cast()) };
        Ok(())
    })
}

pub(super) unsafe extern "C" fn part_open(
    upload: *mut c_void,
    body: KernelNativeByteSliceV1,
    output: *mut *mut c_void,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        if output.is_null() || !output.is_aligned() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let upload = unsafe { upload_ref(upload) }?;
        charge(&mut 0, body.len)?;
        let body = Bytes::from(unsafe { copy_bytes(body) }?);
        let entered = runtime()?.enter();
        let future = {
            let mut state = upload
                .state
                .lock()
                .map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
            state.upload.put_part(body.into())
        };
        drop(entered);
        let part = Box::new(ProviderPart {
            future: Some(future),
            _parent: upload.state.clone(),
        });
        unsafe { output.write(Box::into_raw(part).cast()) };
        Ok(())
    })
}

pub(super) unsafe extern "C" fn part_wait(part: *mut c_void) -> i32 {
    guarded(|| {
        if part.is_null() || !part.cast::<ProviderPart>().is_aligned() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let part = unsafe { &mut *part.cast::<ProviderPart>() };
        let future = part.future.take().ok_or(KERNEL_NATIVE_STATUS_GENERIC)?;
        runtime()?.block_on(future).map_err(error_status)
    })
}

pub(super) unsafe extern "C" fn part_close(part: *mut c_void) {
    guarded(|| {
        if !part.is_null() {
            if !part.cast::<ProviderPart>().is_aligned() {
                return Err(KERNEL_NATIVE_STATUS_GENERIC);
            }
            drop(unsafe { Box::from_raw(part.cast::<ProviderPart>()) });
        }
        Ok(())
    });
}

pub(super) unsafe extern "C" fn complete(
    upload: *mut c_void,
    sink_context: *mut c_void,
    sink: unsafe extern "C" fn(*mut c_void, *const KernelNativePutResultV4) -> i32,
) -> i32 {
    guarded(|| {
        PUTS.fetch_add(1, Ordering::SeqCst);
        if sink_context.is_null() {
            return Err(KERNEL_NATIVE_STATUS_GENERIC);
        }
        let upload = unsafe { upload_ref(upload) }?;
        let mut state = upload
            .state
            .lock()
            .map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        let result = runtime()?
            .block_on(state.upload.complete())
            .map_err(error_status)?;
        unsafe { marshalling::put_result(&result, sink_context, sink) }
    })
}

pub(super) unsafe extern "C" fn abort(upload: *mut c_void) -> i32 {
    guarded(|| {
        let upload = unsafe { upload_ref(upload) }?;
        let mut state = upload
            .state
            .lock()
            .map_err(|_| KERNEL_NATIVE_STATUS_GENERIC)?;
        runtime()?
            .block_on(state.upload.abort())
            .map_err(error_status)
    })
}

pub(super) unsafe extern "C" fn close(upload: *mut c_void) {
    guarded(|| {
        if !upload.is_null() {
            let _ = unsafe { upload_ref(upload) }?;
            drop(unsafe { Box::from_raw(upload.cast::<ProviderUpload>()) });
        }
        Ok(())
    });
}
