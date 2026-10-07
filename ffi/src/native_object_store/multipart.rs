use std::sync::Mutex;

use futures::FutureExt;
use object_store::UploadPart;

use super::*;

struct NativeUpload {
    store: NativeObjectStore,
    pointer: NonNull<c_void>,
    path: String,
}

// SAFETY: the provider allows moving uploads between threads. All upload callbacks are
// serialized by the parent mutex, and children retain the parent until their close returns.
unsafe impl Send for NativeUpload {}

impl Drop for NativeUpload {
    fn drop(&mut self) {
        if let Some(close) = self.store.context.descriptor.multipart_close {
            unsafe { close(self.pointer.as_ptr()) };
        }
    }
}

pub(super) struct NativeMultipart {
    upload: Arc<Mutex<NativeUpload>>,
}

impl fmt::Debug for NativeMultipart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeMultipart")
    }
}

struct NativePart {
    _parent: Arc<Mutex<NativeUpload>>,
    pointer: NonNull<c_void>,
    store: NativeObjectStore,
    path: String,
}

// SAFETY: a part has one owner, moved into the blocking wait. Close happens only after
// wait returns, including cancellation. Its parent Arc prevents upload/context destruction.
unsafe impl Send for NativePart {}

impl Drop for NativePart {
    fn drop(&mut self) {
        if let Some(close) = self.store.context.descriptor.multipart_part_close {
            unsafe { close(self.pointer.as_ptr()) };
        }
    }
}

impl NativeMultipart {
    pub(super) fn open(
        store: NativeObjectStore,
        path: String,
        options: OwnedWriteOptions,
    ) -> ObjectStoreResult<Self> {
        let descriptor = &store.context.descriptor;
        let callback = descriptor
            .multipart_open
            .ok_or_else(|| generic_error("native multipart open callback is missing"))?;
        let mut pointer = std::ptr::null_mut();
        let status = options.with_view(|options| unsafe {
            callback(
                descriptor.context,
                string_slice(&path),
                options,
                &mut pointer,
            )
        });
        check_status(status, &path)?;
        let pointer = NonNull::new(pointer)
            .ok_or_else(|| generic_error("native multipart opened a null upload"))?;
        Ok(Self {
            upload: Arc::new(Mutex::new(NativeUpload {
                store,
                pointer,
                path,
            })),
        })
    }

    fn open_part(&mut self, payload: PutPayload) -> ObjectStoreResult<NativePart> {
        let length = payload.iter().try_fold(0usize, |size, chunk| {
            size.checked_add(chunk.len())
                .filter(|size| *size <= MAX_BODY_BYTES)
                .ok_or_else(|| not_supported("multipart parts larger than 64 MiB"))
        })?;
        let mut bytes = Vec::with_capacity(length);
        for chunk in payload {
            bytes.extend_from_slice(&chunk);
        }
        let upload = self
            .upload
            .lock()
            .map_err(|_| generic_error("native upload lock poisoned"))?;
        let callback = upload
            .store
            .context
            .descriptor
            .multipart_part_open
            .ok_or_else(|| generic_error("native multipart part open callback is missing"))?;
        let mut pointer = std::ptr::null_mut();
        let status = unsafe {
            callback(
                upload.pointer.as_ptr(),
                KernelNativeByteSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                },
                &mut pointer,
            )
        };
        check_status(status, &upload.path)?;
        let pointer = NonNull::new(pointer)
            .ok_or_else(|| generic_error("native multipart opened a null part"))?;
        Ok(NativePart {
            _parent: self.upload.clone(),
            pointer,
            store: upload.store.clone(),
            path: upload.path.clone(),
        })
    }
}

#[async_trait]
impl MultipartUpload for NativeMultipart {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        match self.open_part(payload) {
            Err(error) => std::future::ready(Err(error)).boxed(),
            Ok(part) => async move {
                run_blocking(move || {
                    let callback = part
                        .store
                        .context
                        .descriptor
                        .multipart_part_wait
                        .ok_or_else(|| {
                            generic_error("native multipart wait callback is missing")
                        })?;
                    let status = unsafe { callback(part.pointer.as_ptr()) };
                    let result = check_status(status, &part.path);
                    drop(part);
                    result
                })
                .await
            }
            .boxed(),
        }
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let parent = self.upload.clone();
        run_blocking(move || {
            let upload = parent
                .lock()
                .map_err(|_| generic_error("native upload lock poisoned"))?;
            let callback = upload
                .store
                .context
                .descriptor
                .multipart_complete
                .ok_or_else(|| generic_error("native multipart complete callback is missing"))?;
            let mut state = PutSinkState::default();
            let status = unsafe {
                callback(
                    upload.pointer.as_ptr(),
                    (&mut state as *mut PutSinkState).cast(),
                    marshalling::put_sink,
                )
            };
            state.finish(status, &upload.path)
        })
        .await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        let parent = self.upload.clone();
        run_blocking(move || {
            let upload = parent
                .lock()
                .map_err(|_| generic_error("native upload lock poisoned"))?;
            let callback = upload
                .store
                .context
                .descriptor
                .multipart_abort
                .ok_or_else(|| generic_error("native multipart abort callback is missing"))?;
            check_status(unsafe { callback(upload.pointer.as_ptr()) }, &upload.path)
        })
        .await
    }
}
