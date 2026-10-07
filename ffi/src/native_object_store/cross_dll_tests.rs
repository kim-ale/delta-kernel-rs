use std::ffi::{c_char, c_void, CStr};
use std::mem::MaybeUninit;
use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;

use delta_kernel::object_store::ObjectStoreExt;

use super::*;
use crate::ffi_test_utils::{allocate_err, ok_or_panic};

#[link(name = "kernel32")]
extern "system" {
    fn LoadLibraryW(path: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const c_char) -> *mut c_void;
}

type CreateMemoryFn = unsafe extern "C" fn(*mut KernelNativeObjectStoreDescriptorV4) -> i32;
type CounterFn = unsafe extern "C" fn() -> u64;

#[derive(Clone, Copy)]
struct ProviderLibrary {
    create_memory: CreateMemoryFn,
    callback_count: CounterFn,
}

struct StoreOwner {
    descriptor: Option<KernelNativeObjectStoreDescriptorV4>,
    handle: Option<Handle<SharedNativeObjectStore>>,
}

impl Drop for StoreOwner {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            unsafe { free_native_object_store(handle) };
        }
        if let Some(descriptor) = self.descriptor.take() {
            unsafe { descriptor.release.unwrap()(descriptor.context) };
        }
    }
}

impl ProviderLibrary {
    fn load_for_process_lifetime() -> Self {
        let path = std::env::var_os("DELTA_KERNEL_NATIVE_STORE_PROVIDER_DLL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .join("target-provider/debug/native_object_store_provider.dll")
            });
        let path = path.canonicalize().unwrap_or_else(|error| {
            panic!(
                "provider DLL is required at {}: {error}; build ffi/examples/native-object-store-provider first or set DELTA_KERNEL_NATIVE_STORE_PROVIDER_DLL",
                path.display()
            )
        });
        let wide_path: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let module = unsafe { LoadLibraryW(wide_path.as_ptr()) };
        assert!(
            !module.is_null(),
            "could not load {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        let factory = Self::symbol(module, c"prototype_create_memory");
        let counter = Self::symbol(module, c"prototype_callback_count");
        Self {
            create_memory: unsafe { std::mem::transmute::<*mut c_void, CreateMemoryFn>(factory) },
            callback_count: unsafe { std::mem::transmute::<*mut c_void, CounterFn>(counter) },
        }
    }

    fn symbol(module: *mut c_void, name: &CStr) -> *mut c_void {
        let symbol = unsafe { GetProcAddress(module, name.as_ptr()) };
        assert!(
            !symbol.is_null(),
            "provider DLL is missing {}: {}",
            name.to_string_lossy(),
            std::io::Error::last_os_error()
        );
        symbol
    }

    fn adopt_memory(self) -> Arc<NativeObjectStore> {
        let mut descriptor = MaybeUninit::uninit();
        assert_eq!(
            unsafe { (self.create_memory)(descriptor.as_mut_ptr()) },
            KERNEL_NATIVE_STATUS_OK,
            "independent provider memory factory failed"
        );
        let mut owner = StoreOwner {
            descriptor: Some(unsafe { descriptor.assume_init() }),
            handle: None,
        };
        let descriptor = owner.descriptor.as_ref().unwrap();
        assert_eq!(descriptor.abi_version, KERNEL_NATIVE_STORE_ABI_V4);
        assert_eq!(
            descriptor.struct_size as usize,
            size_of::<KernelNativeObjectStoreDescriptorV4>()
        );
        owner.handle = Some(ok_or_panic(unsafe {
            get_native_object_store(descriptor, allocate_err)
        }));
        owner.descriptor = None;
        unsafe { owner.handle.as_ref().unwrap().clone_as_arc() }
    }
}

fn write_metadata(content_type: &'static str) -> (TagSet, Attributes) {
    let mut tags = TagSet::default();
    tags.push("key +&=", "value +&=%");
    tags.push("empty", "");
    let attributes = Attributes::from_iter([
        (object_store::Attribute::ContentType, content_type),
        (
            object_store::Attribute::Metadata("user+key".into()),
            "value +&=%",
        ),
        (object_store::Attribute::Metadata("empty".into()), ""),
    ]);
    (tags, attributes)
}

async fn verify_native_batches_and_cursor(store: &NativeObjectStore) {
    let data_path = Path::from("cross-dll/ranges/data");
    store
        .put_opts(
            &data_path,
            b"0123456789".as_slice().into(),
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_ranges(&data_path, &[7..9, 0..2, 7..9])
            .await
            .unwrap(),
        vec![
            Bytes::from_static(b"78"),
            Bytes::from_static(b"01"),
            Bytes::from_static(b"78"),
        ]
    );
    let paths: Vec<_> = (0..130)
        .map(|index| Path::from(format!("cross-dll/batch/{index:03}")))
        .collect();
    for (index, path) in paths.iter().enumerate() {
        store
            .put_opts(
                path,
                format!("body-{index}").into_bytes().into(),
                Default::default(),
            )
            .await
            .unwrap();
    }
    let prefix = Path::from("cross-dll/batch");
    let listed = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        listed.iter().map(|meta| &meta.location).collect::<Vec<_>>(),
        paths.iter().collect::<Vec<_>>()
    );
    assert!(listed.iter().all(|meta| meta.e_tag.is_some()));
    let tail = store
        .list_with_offset(Some(&prefix), &paths[127])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        tail.iter().map(|meta| &meta.location).collect::<Vec<_>>(),
        paths[128..].iter().collect::<Vec<_>>()
    );
    assert_eq!(
        store.get(&paths[129]).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"body-129")
    );
    let requested: Vec<_> = paths.into_iter().rev().collect();
    let deleted = store
        .delete_stream(stream::iter(requested.clone().into_iter().map(Ok)).boxed())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(deleted, requested);
    assert!(store.list(Some(&prefix)).next().await.is_none());
    assert!(matches!(
        store.head(&requested[0]).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.head(&requested[129]).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
}

async fn verify_copy_rename_and_delimiter(store: &NativeObjectStore) {
    let source = Path::from("cross-dll/transfer/source");
    let copied = Path::from("cross-dll/transfer/copied");
    let renamed = Path::from("cross-dll/transfer/renamed");
    let (tags, attributes) = write_metadata("application/octet-stream");
    store
        .put_opts(
            &source,
            b"source".as_slice().into(),
            PutOptions {
                tags,
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let create = CopyOptions::new().with_mode(CopyMode::Create);
    store
        .copy_opts(&source, &copied, create.clone())
        .await
        .unwrap();
    assert!(matches!(
        store.copy_opts(&source, &copied, create).await,
        Err(ObjectStoreError::AlreadyExists { .. })
    ));
    let original = store.get(&source).await.unwrap();
    assert_eq!(original.attributes, attributes);
    assert_eq!(original.bytes().await.unwrap().as_ref(), b"source");
    let duplicate = store.get(&copied).await.unwrap();
    assert_eq!(duplicate.attributes, attributes);
    assert_eq!(duplicate.bytes().await.unwrap().as_ref(), b"source");
    store
        .put_opts(&copied, b"old".as_slice().into(), Default::default())
        .await
        .unwrap();
    store
        .copy_opts(
            &source,
            &copied,
            CopyOptions::new().with_mode(CopyMode::Overwrite),
        )
        .await
        .unwrap();
    let overwrite = store.get(&copied).await.unwrap();
    assert_eq!(overwrite.attributes, attributes);
    assert_eq!(overwrite.bytes().await.unwrap().as_ref(), b"source");
    let create = RenameOptions::new().with_target_mode(RenameTargetMode::Create);
    assert!(matches!(
        store.rename_opts(&source, &copied, create.clone()).await,
        Err(ObjectStoreError::AlreadyExists { .. })
    ));
    store.rename_opts(&copied, &renamed, create).await.unwrap();
    assert!(matches!(
        store.head(&copied).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
    store
        .put_opts(&renamed, b"old".as_slice().into(), Default::default())
        .await
        .unwrap();
    store
        .rename_opts(
            &source,
            &renamed,
            RenameOptions::new().with_target_mode(RenameTargetMode::Overwrite),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.head(&source).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
    let moved = store.get(&renamed).await.unwrap();
    assert_eq!(moved.attributes, attributes);
    assert_eq!(moved.bytes().await.unwrap().as_ref(), b"source");

    for path in [
        "cross-dll/tree/root",
        "cross-dll/tree/a/leaf",
        "cross-dll/tree/b/leaf",
        "cross-dll/tree-sibling/excluded",
    ] {
        store
            .put_opts(
                &Path::from(path),
                b"leaf".as_slice().into(),
                Default::default(),
            )
            .await
            .unwrap();
    }
    let mut listing = store
        .list_with_delimiter(Some(&Path::from("cross-dll/tree")))
        .await
        .unwrap();
    listing.common_prefixes.sort();
    assert_eq!(
        listing.common_prefixes,
        vec![
            Path::from("cross-dll/tree/a"),
            Path::from("cross-dll/tree/b")
        ]
    );
    assert_eq!(listing.objects.len(), 1);
    assert_eq!(
        listing.objects[0].location,
        Path::from("cross-dll/tree/root")
    );
    assert_eq!(listing.objects[0].size, 4);
    assert!(listing.objects[0].e_tag.is_some());
}

async fn verify_multipart_conditions_and_metadata(store: &NativeObjectStore) {
    let path = Path::from("cross-dll/multipart/data");
    let (tags, attributes) = write_metadata("application/octet-stream");
    let mut upload = store
        .put_multipart_opts(
            &path,
            PutMultipartOptions {
                tags,
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let first = upload.put_part(b"abc".as_slice().into());
    let second = upload.put_part(b"def".as_slice().into());
    second.await.unwrap();
    first.await.unwrap();
    let completed = upload.complete().await.unwrap();
    drop(upload);
    let result = store.get(&path).await.unwrap();
    assert_eq!(result.attributes, attributes);
    assert_eq!(result.meta.e_tag, completed.e_tag);
    assert_eq!(result.meta.version, completed.version);
    assert_eq!(result.meta.size, 6);
    let e_tag = result
        .meta
        .e_tag
        .clone()
        .expect("memory GET must return an ETag");
    let version = result.meta.version.clone();
    assert_eq!(result.bytes().await.unwrap().as_ref(), b"abcdef");
    let head = store.head(&path).await.unwrap();
    assert_eq!(head.size, 6);
    assert_eq!(head.e_tag.as_deref(), Some(e_tag.as_str()));
    let ranged = store
        .get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Bounded(1..5)),
                if_match: Some(e_tag.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ranged.range, 1..5);
    assert_eq!(ranged.attributes, attributes);
    assert_eq!(ranged.meta.e_tag.as_deref(), Some(e_tag.as_str()));
    assert_eq!(ranged.bytes().await.unwrap().as_ref(), b"bcde");
    assert!(matches!(
        store
            .get_opts(
                &path,
                GetOptions {
                    if_match: Some("invalid-etag".into()),
                    ..Default::default()
                },
            )
            .await,
        Err(ObjectStoreError::Precondition { .. })
    ));
    assert!(matches!(
        store
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: Some(e_tag.clone()),
                    ..Default::default()
                },
            )
            .await,
        Err(ObjectStoreError::NotModified { .. })
    ));
    let (tags, updated_attributes) = write_metadata("application/json");
    let updated = store
        .put_opts(
            &path,
            b"updated".as_slice().into(),
            PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(e_tag.clone()),
                    version,
                }),
                tags,
                attributes: updated_attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(updated.e_tag.is_some());
    assert_ne!(updated.e_tag.as_deref(), Some(e_tag.as_str()));
    assert!(matches!(
        store
            .put_opts(
                &path,
                b"rejected".as_slice().into(),
                PutOptions {
                    mode: PutMode::Update(object_store::UpdateVersion {
                        e_tag: Some(e_tag),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await,
        Err(ObjectStoreError::Precondition { .. })
    ));
    let result = store.get(&path).await.unwrap();
    assert_eq!(result.meta.e_tag, updated.e_tag);
    assert_eq!(result.meta.version, updated.version);
    assert_eq!(result.attributes, updated_attributes);
    assert_eq!(result.bytes().await.unwrap().as_ref(), b"updated");

    let aborted_path = Path::from("cross-dll/multipart/aborted");
    let mut aborted = store
        .put_multipart_opts(&aborted_path, Default::default())
        .await
        .unwrap();
    aborted
        .put_part(b"discard".as_slice().into())
        .await
        .unwrap();
    aborted.abort().await.unwrap();
    drop(aborted);
    assert!(matches!(
        store.head(&aborted_path).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
}

#[tokio::test]
async fn independently_compiled_v4_dll_preserves_native_object_store_behavior_after_caller_free() {
    let provider = ProviderLibrary::load_for_process_lifetime();
    let callbacks_before = unsafe { (provider.callback_count)() };
    let store = tokio::task::spawn_blocking(move || provider.adopt_memory())
        .await
        .unwrap();
    verify_native_batches_and_cursor(&store).await;
    verify_copy_rename_and_delimiter(&store).await;
    verify_multipart_conditions_and_metadata(&store).await;
    assert!(unsafe { (provider.callback_count)() } > callbacks_before);
    let copied_result = store
        .get(&Path::from("cross-dll/multipart/data"))
        .await
        .unwrap();
    drop(store);
    assert_eq!(copied_result.bytes().await.unwrap().as_ref(), b"updated");
}
