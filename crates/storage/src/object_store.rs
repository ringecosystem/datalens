use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::mpsc,
};

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Region},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
};
use datalens_core::{DatalensError, DatalensErrorKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
}

pub trait ObjectStore: Clone + Send + Sync {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError>;
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError>;
    fn exists(&self, key: &str) -> Result<bool, DatalensError>;
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError>;
    fn delete(&self, key: &str) -> Result<(), DatalensError>;
}

pub fn validate_object_key(key: &str) -> Result<(), DatalensError> {
    if key.trim().is_empty()
        || key.contains('\\')
        || Path::new(key).is_absolute()
        || key
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "object key must be a safe relative path",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &str) -> Result<PathBuf, DatalensError> {
        validate_object_key(key)?;
        Ok(self.root.join(key))
    }
}

impl ObjectStore for LocalObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        let path = self.path(key)?;
        fs::read(&path).map_err(|error| {
            let message = if error.kind() == std::io::ErrorKind::NotFound {
                format!("object not found {key}")
            } else {
                format!("read object {}: {error}", path.display())
            };
            DatalensError::new(DatalensErrorKind::StorageReadFailure, message)
        })
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        let path = self.path(key)?;
        let parent = path.parent().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("object {key} has no parent directory"),
            )
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("create object directory {}: {error}", parent.display()),
            )
        })?;
        let temp_path = path.with_extension(format!(
            "tmp-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        fs::write(&temp_path, bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write object temp {}: {error}", temp_path.display()),
            )
        })?;
        fs::rename(&temp_path, &path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("finalize object {}: {error}", path.display()),
            )
        })
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        let path = self.path(key)?;
        match fs::metadata(&path) {
            Ok(metadata) => Ok(metadata.is_file()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("stat object {}: {error}", path.display()),
            )),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        validate_object_key(prefix)?;
        let prefix_path = self.root.join(prefix);
        if !prefix_path.exists() {
            return Ok(Vec::new());
        }
        let mut objects = Vec::new();
        collect_local_objects(&self.root, &prefix_path, &mut objects)?;
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        let path = self.path(key)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("delete object {}: {error}", path.display()),
            )),
        }
    }
}

fn collect_local_objects(
    root: &Path,
    path: &Path,
    objects: &mut Vec<ObjectMetadata>,
) -> Result<(), DatalensError> {
    for entry in fs::read_dir(path).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("list object directory {}: {error}", path.display()),
        )
    })? {
        let entry = entry.map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("read object directory entry {}: {error}", path.display()),
            )
        })?;
        let metadata = entry.metadata().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("stat object {}: {error}", entry.path().display()),
            )
        })?;
        if metadata.is_dir() {
            collect_local_objects(root, &entry.path(), objects)?;
        } else if metadata.is_file() {
            let key = entry
                .path()
                .strip_prefix(root)
                .map_err(|error| DatalensError::internal(format!("strip object root: {error}")))?
                .to_string_lossy()
                .replace('\\', "/");
            objects.push(ObjectMetadata {
                key,
                size: metadata.len(),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3ObjectStoreConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default = "default_s3_region")]
    pub region: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
}

fn default_s3_region() -> String {
    "auto".to_owned()
}

#[derive(Clone)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    prefix: Option<String>,
    runtime: S3Runtime,
}

type S3RuntimeJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;

#[derive(Clone)]
struct S3Runtime {
    sender: mpsc::Sender<S3RuntimeJob>,
}

impl std::fmt::Debug for S3Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Runtime").finish_non_exhaustive()
    }
}

impl S3Runtime {
    fn new() -> Result<Self, DatalensError> {
        let runtime = tokio::runtime::Runtime::new().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("create S3 runtime: {error}"),
            )
        })?;
        let (sender, receiver) = mpsc::channel::<S3RuntimeJob>();
        std::thread::Builder::new()
            .name("datalens-s3-runtime".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job(&runtime);
                }
            })
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("start S3 runtime thread: {error}"),
                )
            })?;
        Ok(Self { sender })
    }

    fn block_on<F, T>(&self, future: F) -> Result<T, DatalensError>
    where
        F: Future<Output = Result<T, DatalensError>> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .send(Box::new(move |runtime| {
                let _ = sender.send(runtime.block_on(future));
            }))
            .map_err(|_| DatalensError::internal("S3 runtime thread stopped"))?;
        receiver
            .recv()
            .map_err(|_| DatalensError::internal("S3 runtime thread stopped"))?
    }
}

impl std::fmt::Debug for S3ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ObjectStore")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3ObjectStore {
    pub fn from_config(config: S3ObjectStoreConfig) -> Result<Self, DatalensError> {
        if config.bucket.trim().is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage.s3.bucket must not be empty",
            ));
        }
        let prefix = normalize_prefix(config.prefix.as_deref())?;
        let runtime = S3Runtime::new()?;
        let region = if config.region.trim().is_empty() {
            "auto".to_owned()
        } else {
            config.region
        };
        let endpoint_url = config.endpoint_url;
        let force_path_style = config.force_path_style;
        let client = runtime.block_on(async move {
            let mut loader =
                aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
            if let Some(endpoint_url) = endpoint_url.as_ref() {
                if endpoint_url.trim().is_empty() {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "storage.s3.endpoint_url must not be empty when set",
                    ));
                }
                loader = loader.endpoint_url(endpoint_url);
            }
            let shared = loader.load().await;
            let conf = aws_sdk_s3::config::Builder::from(&shared)
                .force_path_style(force_path_style)
                .build();
            Ok(Client::from_conf(conf))
        })?;
        Ok(Self {
            client,
            bucket: config.bucket,
            prefix,
            runtime,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    fn key(&self, key: &str) -> Result<String, DatalensError> {
        validate_object_key(key)?;
        Ok(match &self.prefix {
            Some(prefix) => format!("{prefix}/{key}"),
            None => key.to_owned(),
        })
    }
}

impl ObjectStore for S3ObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        let key = self.key(key)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on(async move {
            let object = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|error| {
                    let message = if service_error_code(&error).is_some_and(is_not_found_code) {
                        format!("object not found {key}")
                    } else {
                        format!("S3 get object {key}: {error}")
                    };
                    DatalensError::new(DatalensErrorKind::StorageReadFailure, message)
                })?;
            let bytes = object.body.collect().await.map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("S3 read object body {key}: {error}"),
                )
            })?;
            Ok(bytes.into_bytes().to_vec())
        })
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        let key = self.key(key)?;
        let bytes = bytes.to_vec();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(ByteStream::from(bytes))
                .send()
                .await
                .map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageWriteFailure,
                        format!("S3 put object {key}: {error}"),
                    )
                })?;
            Ok(())
        })
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        let key = self.key(key)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on(async move {
            match client.head_object().bucket(&bucket).key(&key).send().await {
                Ok(_) => Ok(true),
                Err(error) if service_error_code(&error).is_some_and(is_not_found_code) => {
                    Ok(false)
                }
                Err(error) => Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("S3 head object {key}: {error}"),
                )),
            }
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        let prefix_key = self.key(prefix)?;
        let strip_prefix = self.prefix.clone().map(|prefix| format!("{prefix}/"));
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on(async move {
            let mut continuation = None;
            let mut objects = Vec::new();
            loop {
                let response = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix_key)
                    .set_continuation_token(continuation)
                    .send()
                    .await
                    .map_err(|error| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            format!("S3 list objects {prefix_key}: {error}"),
                        )
                    })?;
                for object in response.contents() {
                    let Some(key) = object.key() else {
                        continue;
                    };
                    let key = strip_prefix
                        .as_ref()
                        .and_then(|prefix| key.strip_prefix(prefix))
                        .unwrap_or(key)
                        .to_owned();
                    objects.push(ObjectMetadata {
                        key,
                        size: object.size().unwrap_or_default().max(0) as u64,
                    });
                }
                if response.is_truncated().unwrap_or(false) {
                    continuation = response.next_continuation_token().map(ToOwned::to_owned);
                } else {
                    break;
                }
            }
            Ok(objects)
        })
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        let key = self.key(key)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageWriteFailure,
                        format!("S3 delete object {key}: {error}"),
                    )
                })?;
            Ok(())
        })
    }
}

fn service_error_code<E, R>(error: &aws_sdk_s3::error::SdkError<E, R>) -> Option<&str>
where
    E: ProvideErrorMetadata,
{
    error.as_service_error().and_then(|error| error.code())
}

fn is_not_found_code(code: &str) -> bool {
    matches!(code, "NoSuchKey" | "NotFound" | "404")
}

fn normalize_prefix(prefix: Option<&str>) -> Result<Option<String>, DatalensError> {
    let Some(prefix) = prefix else {
        return Ok(None);
    };
    let prefix = prefix.trim().trim_matches('/');
    if prefix.is_empty() {
        return Ok(None);
    }
    validate_object_key(prefix)?;
    Ok(Some(prefix.to_owned()))
}
