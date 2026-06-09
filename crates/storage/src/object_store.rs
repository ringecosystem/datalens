use std::{
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::Instant,
};

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Region},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
};
use datalens_core::{DatalensError, DatalensErrorKind};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectListPage {
    pub objects: Vec<ObjectMetadata>,
    pub has_more: bool,
}

pub trait ObjectStore: Clone + Send + Sync {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError>;
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError>;
    fn exists(&self, key: &str) -> Result<bool, DatalensError>;
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError>;
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError>;
    fn delete(&self, key: &str) -> Result<(), DatalensError>;
}

pub fn validate_object_key(key: &str) -> Result<(), DatalensError> {
    if key.trim().is_empty()
        || key.contains('\\')
        || Path::new(key).is_absolute()
        || key.split('/').any(|segment| {
            segment.is_empty() || segment == "." || segment == ".." || segment == ".datalens-tmp"
        })
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
        let temp_parent = local_temp_parent(&self.root);
        fs::create_dir_all(&temp_parent).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "create temp object directory {}: {error}",
                    temp_parent.display()
                ),
            )
        })?;
        let temp_path = create_local_temp_file(&temp_parent, bytes)?;
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

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        validate_object_key(prefix)?;
        if let Some(start_after) = start_after {
            validate_object_key(start_after)?;
        }
        if limit == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "object list page limit must be greater than zero",
            ));
        }
        let prefix_path = self.root.join(prefix);
        if !prefix_path.exists() {
            return Ok(ObjectListPage {
                objects: Vec::new(),
                has_more: false,
            });
        }
        let mut objects = Vec::new();
        collect_local_objects_page(
            &self.root,
            &prefix_path,
            start_after,
            limit.saturating_add(1),
            &mut objects,
        )?;
        let has_more = objects.len() > limit;
        objects.truncate(limit);
        Ok(ObjectListPage { objects, has_more })
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

fn collect_local_objects_page(
    root: &Path,
    path: &Path,
    start_after: Option<&str>,
    limit: usize,
    objects: &mut Vec<ObjectMetadata>,
) -> Result<(), DatalensError> {
    if objects.len() >= limit {
        return Ok(());
    }
    let mut entries = fs::read_dir(path)
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("list object directory {}: {error}", path.display()),
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("read object directory entry {}: {error}", path.display()),
            )
        })?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if objects.len() >= limit {
            break;
        }
        let metadata = entry.metadata().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("stat object {}: {error}", entry.path().display()),
            )
        })?;
        if metadata.is_dir() {
            collect_local_objects_page(root, &entry.path(), start_after, limit, objects)?;
        } else if metadata.is_file() {
            let key = entry
                .path()
                .strip_prefix(root)
                .map_err(|error| DatalensError::internal(format!("strip object root: {error}")))?
                .to_string_lossy()
                .replace('\\', "/");
            if start_after.is_none_or(|cursor| key.as_str() > cursor) {
                objects.push(ObjectMetadata {
                    key,
                    size: metadata.len(),
                });
            }
        }
    }
    Ok(())
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

fn local_temp_parent(root: &Path) -> PathBuf {
    root.join(".datalens-tmp")
}

fn create_local_temp_file(parent: &Path, bytes: &[u8]) -> Result<PathBuf, DatalensError> {
    for attempt in 0..1024u16 {
        let path = parent.join(format!(
            "object-{}-{}-{attempt}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageWriteFailure,
                    format!("create object temp {}: {error}", path.display()),
                ));
            }
        };
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&path);
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write object temp {}: {error}", path.display()),
            ));
        }
        return Ok(path);
    }
    Err(DatalensError::new(
        DatalensErrorKind::StorageWriteFailure,
        format!(
            "create object temp under {}: exhausted unique name attempts",
            parent.display()
        ),
    ))
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

const DEFAULT_S3_RUNTIME_WORKER_THREADS: usize = 4;
const DEFAULT_S3_MAX_CONCURRENT_OPERATIONS: usize = 16;

#[derive(Clone)]
struct S3Runtime {
    inner: Arc<S3RuntimeInner>,
}

struct S3RuntimeInner {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for S3Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Runtime").finish_non_exhaustive()
    }
}

impl S3Runtime {
    fn new() -> Result<Self, DatalensError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(DEFAULT_S3_RUNTIME_WORKER_THREADS)
            .thread_name("datalens-s3-runtime")
            .build()
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("create S3 runtime: {error}"),
                )
            })?;
        Ok(Self {
            inner: Arc::new(S3RuntimeInner {
                runtime: Mutex::new(Some(runtime)),
                semaphore: Arc::new(Semaphore::new(DEFAULT_S3_MAX_CONCURRENT_OPERATIONS)),
            }),
        })
    }

    fn block_on_operation<F, T>(
        &self,
        operation: &'static str,
        key: String,
        future: F,
    ) -> Result<T, DatalensError>
    where
        F: Future<Output = Result<T, DatalensError>> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1);
        let semaphore = self.inner.semaphore.clone();
        let handle = {
            let runtime = self
                .inner
                .runtime
                .lock()
                .map_err(|_| DatalensError::internal("S3 runtime lock poisoned"))?;
            runtime
                .as_ref()
                .ok_or_else(|| DatalensError::internal("S3 runtime stopped"))?
                .handle()
                .clone()
        };
        let queued_at = Instant::now();
        handle.spawn(async move {
            let permit = semaphore.acquire_owned().await.map_err(|error| {
                DatalensError::internal(format!("acquire S3 runtime permit: {error}"))
            });
            let queue_ms = queued_at.elapsed().as_millis();
            let run_started = Instant::now();
            let result = match permit {
                Ok(permit) => {
                    let _permit = permit;
                    future.await
                }
                Err(error) => Err(error),
            };
            let run_ms = run_started.elapsed().as_millis();
            match &result {
                Ok(_) => log::debug!(
                    "s3 operation completed operation={} key={} queue_ms={} run_ms={}",
                    operation,
                    key,
                    queue_ms,
                    run_ms
                ),
                Err(error) => log::warn!(
                    "s3 operation failed operation={} key={} kind={:?} queue_ms={} run_ms={}",
                    operation,
                    key,
                    error.kind,
                    queue_ms,
                    run_ms
                ),
            }
            let _ = sender.send(result);
        });
        receiver
            .recv()
            .map_err(|_| DatalensError::internal("S3 runtime stopped"))?
    }
}

impl Drop for S3RuntimeInner {
    fn drop(&mut self) {
        let Ok(runtime) = self.runtime.get_mut() else {
            return;
        };
        let Some(runtime) = runtime.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("datalens-s3-runtime-drop".to_owned())
                .spawn(move || drop(runtime));
        } else {
            drop(runtime);
        }
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
        let client =
            runtime.block_on_operation("config_load", config.bucket.clone(), async move {
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
        let log_key = key.clone();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on_operation("get", log_key, async move {
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
        let log_key = key.clone();
        let bytes = bytes.to_vec();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime.block_on_operation("put", log_key, async move {
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
        let log_key = key.clone();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .block_on_operation("exists", log_key, async move {
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
        let log_key = prefix_key.clone();
        let strip_prefix = self.prefix.clone().map(|prefix| format!("{prefix}/"));
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .block_on_operation("list", log_key, async move {
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

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        if limit == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "object list page limit must be greater than zero",
            ));
        }
        let prefix_key = self.key(prefix)?;
        let start_after_key = start_after.map(|key| self.key(key)).transpose()?;
        let log_key = prefix_key.clone();
        let strip_prefix = self.prefix.clone().map(|prefix| format!("{prefix}/"));
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .block_on_operation("list_page", log_key, async move {
                let response = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix_key)
                    .set_start_after(start_after_key)
                    .max_keys(limit.saturating_add(1).min(i32::MAX as usize) as i32)
                    .send()
                    .await
                    .map_err(|error| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            format!("S3 list objects {prefix_key}: {error}"),
                        )
                    })?;
                let mut objects = Vec::new();
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
                let has_more = objects.len() > limit || response.is_truncated().unwrap_or(false);
                objects.truncate(limit);
                Ok(ObjectListPage { objects, has_more })
            })
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        let key = self.key(key)?;
        let log_key = key.clone();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .block_on_operation("delete", log_key, async move {
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

#[cfg(test)]
mod tests {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use datalens_core::DatalensError;

    use super::S3Runtime;

    #[test]
    fn test_s3_runtime_runs_independent_operations_concurrently() {
        let runtime = S3Runtime::new().expect("runtime");
        let start = Instant::now();
        let mut handles = Vec::new();

        for _ in 0..4 {
            let runtime = runtime.clone();
            handles.push(thread::spawn(move || {
                runtime.block_on_operation("test", "delay".to_owned(), async {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    Ok::<(), DatalensError>(())
                })
            }));
        }

        for handle in handles {
            handle.join().expect("runtime worker").expect("operation");
        }

        assert!(
            start.elapsed() < Duration::from_millis(650),
            "S3 runtime serialized independent operations"
        );
    }
}
