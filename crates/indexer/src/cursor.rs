use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use datalens_chain::{AdapterKey, DatasetSelector};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, EvmLogFilter, LedgerRange,
};
use serde::{Deserialize, Serialize};

use crate::{
    IndexChunk, IndexCursor, IndexFailureState, IndexJobId, IndexRetryPolicy,
    runtime::IndexCursorRepository,
};

const FILE_CURSOR_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct FileIndexCursorStore {
    root: Arc<PathBuf>,
}

impl FileIndexCursorStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: Arc::new(root.as_ref().to_path_buf()),
        }
    }

    fn cursor_path(&self, job_id: &IndexJobId) -> PathBuf {
        self.root
            .join(format!("{}.json", encode_cursor_filename(job_id.as_str())))
    }
}

impl IndexCursorRepository for FileIndexCursorStore {
    fn load(&self, job_id: &IndexJobId) -> Result<Option<IndexCursor>, DatalensError> {
        let path = self.cursor_path(job_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("read index cursor {}: {error}", path.display()),
                ));
            }
        };
        let stored = serde_json::from_slice::<StoredIndexCursor>(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("decode index cursor {}: {error}", path.display()),
            )
        })?;
        stored.into_cursor()
    }

    fn save(&self, cursor: &IndexCursor) -> Result<(), DatalensError> {
        fs::create_dir_all(self.root.as_ref()).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "create index cursor directory {}: {error}",
                    self.root.display()
                ),
            )
        })?;
        let path = self.cursor_path(&cursor.job_id);
        let temp_path = self.root.join(format!(
            ".{}.{}.tmp",
            encode_cursor_filename(cursor.job_id.as_str()),
            temp_suffix()
        ));
        let bytes = serde_json::to_vec_pretty(&StoredIndexCursor::from_cursor(cursor))
            .map_err(|error| DatalensError::internal(format!("encode index cursor: {error}")))?;
        let mut file = fs::File::create(&temp_path).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "create index cursor temp file {}: {error}",
                    temp_path.display()
                ),
            )
        })?;
        file.write_all(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "write index cursor temp file {}: {error}",
                    temp_path.display()
                ),
            )
        })?;
        file.sync_all().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "flush index cursor temp file {}: {error}",
                    temp_path.display()
                ),
            )
        })?;
        drop(file);
        fs::rename(&temp_path, &path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "rename index cursor temp file {} to {}: {error}",
                    temp_path.display(),
                    path.display()
                ),
            )
        })?;
        let _ = fs::File::open(self.root.as_ref()).and_then(|dir| dir.sync_all());
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredIndexCursor {
    schema_version: u32,
    cursor: StoredCursor,
}

impl StoredIndexCursor {
    fn from_cursor(cursor: &IndexCursor) -> Self {
        Self {
            schema_version: FILE_CURSOR_SCHEMA_VERSION,
            cursor: StoredCursor::from_cursor(cursor),
        }
    }

    fn into_cursor(self) -> Result<Option<IndexCursor>, DatalensError> {
        if self.schema_version != FILE_CURSOR_SCHEMA_VERSION {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!(
                    "unsupported index cursor schema version {}",
                    self.schema_version
                ),
            ));
        }
        Ok(Some(self.cursor.into_cursor()?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredCursor {
    job_id: String,
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector: StoredDatasetSelector,
    range_kind: datalens_chain::HeightRangeKind,
    next_height: u64,
    completed_chunks: Vec<u64>,
    completed_ranges: Vec<LedgerRange>,
    failure_state: Option<StoredFailureState>,
    next_chunk_ordinal: u64,
    last_checkpointed_range: Option<LedgerRange>,
}

impl StoredCursor {
    fn from_cursor(cursor: &IndexCursor) -> Self {
        Self {
            job_id: cursor.job_id.as_str().to_owned(),
            chain: cursor.chain.clone(),
            dataset_key: cursor.dataset_key.clone(),
            selector: StoredDatasetSelector::from_selector(&cursor.selector),
            range_kind: cursor.range_kind.clone(),
            next_height: cursor.next_height,
            completed_chunks: cursor.completed_chunks.clone(),
            completed_ranges: cursor.completed_ranges.clone(),
            failure_state: cursor
                .failure_state
                .as_ref()
                .map(StoredFailureState::from_failure_state),
            next_chunk_ordinal: cursor.next_chunk_ordinal,
            last_checkpointed_range: cursor.last_checkpointed_range.clone(),
        }
    }

    fn into_cursor(self) -> Result<IndexCursor, DatalensError> {
        Ok(IndexCursor {
            job_id: IndexJobId::new(self.job_id)?,
            chain: self.chain,
            dataset_key: self.dataset_key,
            selector: self.selector.into_selector()?,
            range_kind: self.range_kind,
            next_height: self.next_height,
            completed_chunks: self.completed_chunks,
            completed_ranges: self.completed_ranges,
            failure_state: self
                .failure_state
                .map(StoredFailureState::into_failure_state)
                .transpose()?,
            next_chunk_ordinal: self.next_chunk_ordinal,
            last_checkpointed_range: self.last_checkpointed_range,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredFailureState {
    chunk: StoredChunk,
    error_kind: DatalensErrorKind,
    message: String,
}

impl StoredFailureState {
    fn from_failure_state(failure_state: &IndexFailureState) -> Self {
        Self {
            chunk: StoredChunk::from_chunk(&failure_state.chunk),
            error_kind: failure_state.error_kind.clone(),
            message: failure_state.message.clone(),
        }
    }

    fn into_failure_state(self) -> Result<IndexFailureState, DatalensError> {
        Ok(IndexFailureState {
            chunk: self.chunk.into_chunk()?,
            error_kind: self.error_kind,
            message: self.message,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredChunk {
    ordinal: u64,
    dataset_key: DatasetKey,
    selector: StoredDatasetSelector,
    range: LedgerRange,
    retry_policy: StoredRetryPolicy,
}

impl StoredChunk {
    fn from_chunk(chunk: &IndexChunk) -> Self {
        Self {
            ordinal: chunk.ordinal,
            dataset_key: chunk.dataset_key.clone(),
            selector: StoredDatasetSelector::from_selector(&chunk.selector),
            range: chunk.range.clone(),
            retry_policy: StoredRetryPolicy::from_retry_policy(&chunk.retry_policy),
        }
    }

    fn into_chunk(self) -> Result<IndexChunk, DatalensError> {
        Ok(IndexChunk {
            ordinal: self.ordinal,
            dataset_key: self.dataset_key,
            selector: self.selector.into_selector()?,
            range: self.range,
            retry_policy: self.retry_policy.into_retry_policy(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredRetryPolicy {
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl StoredRetryPolicy {
    fn from_retry_policy(retry_policy: &IndexRetryPolicy) -> Self {
        Self {
            max_attempts: retry_policy.max_attempts,
            initial_backoff_ms: retry_policy.initial_backoff_ms,
            max_backoff_ms: retry_policy.max_backoff_ms,
        }
    }

    fn into_retry_policy(self) -> IndexRetryPolicy {
        IndexRetryPolicy {
            max_attempts: self.max_attempts,
            initial_backoff_ms: self.initial_backoff_ms,
            max_backoff_ms: self.max_backoff_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredDatasetSelector {
    All,
    EvmLogs {
        filter: EvmLogFilter,
    },
    Other {
        selector_kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

impl StoredDatasetSelector {
    fn from_selector(selector: &DatasetSelector) -> Self {
        match selector {
            DatasetSelector::All => Self::All,
            DatasetSelector::EvmLogs(filter) => Self::EvmLogs {
                filter: filter.clone(),
            },
            DatasetSelector::Other {
                kind,
                fingerprint,
                canonical_key,
            } => Self::Other {
                selector_kind: kind.as_str().to_owned(),
                fingerprint: fingerprint.clone(),
                canonical_key: canonical_key.clone(),
            },
        }
    }

    fn into_selector(self) -> Result<DatasetSelector, DatalensError> {
        match self {
            Self::All => Ok(DatasetSelector::all()),
            Self::EvmLogs { filter } => Ok(DatasetSelector::EvmLogs(filter)),
            Self::Other {
                selector_kind,
                fingerprint,
                canonical_key,
            } => DatasetSelector::try_other(
                AdapterKey::try_new(selector_kind)?,
                fingerprint,
                canonical_key,
            ),
        }
    }
}

fn encode_cursor_filename(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn temp_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}.{}", std::process::id(), nanos)
}
