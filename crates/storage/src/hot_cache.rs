use datalens_chain::DatasetSelector;
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryDataFinality, QueryRows, QuerySegmentSource,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::{
    LocalObjectStore, ObjectEncoding, ObjectStore, checksum_hex, decode_object_rows,
    encode_object_rows, filter_rows, object_encoding_for_dataset, range_kind_key,
    validate_object_key, verify_manifest_object_metadata,
};

pub const HOT_CACHE_SCHEMA_VERSION: &str = "hot-cache-v1";
const HOT_CACHE_PREFIX: &str = "hot-cache";

#[derive(Clone, Debug)]
pub struct HotCacheStorage<S> {
    object_store: S,
}

pub type LocalHotCacheStorage = HotCacheStorage<LocalObjectStore>;

impl HotCacheStorage<LocalObjectStore> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            object_store: LocalObjectStore::new(root),
        }
    }

    pub fn root(&self) -> &Path {
        self.object_store.root()
    }
}

impl<S> HotCacheStorage<S>
where
    S: ObjectStore,
{
    pub fn from_object_store(object_store: S) -> Self {
        Self { object_store }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    pub fn write_rows(
        &self,
        request: HotCacheWriteRequest<'_>,
    ) -> Result<HotCacheWriteOutcome, DatalensError> {
        let HotCacheWriteRequest {
            chain,
            dataset_key,
            selector,
            range,
            rows,
            metadata,
        } = request;

        if rows.dataset_key() != &dataset_key {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "dataset rows key does not match hot cache dataset key",
            ));
        }
        if metadata.height < range.start() || metadata.height > range.end() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "hot cache metadata height must be inside the written range",
            ));
        }

        let encoding = object_encoding_for_dataset(&dataset_key);
        let object_key = hot_object_key(
            chain,
            &dataset_key,
            &range,
            selector,
            metadata.height,
            &metadata.block_hash,
            encoding,
        )?;
        let metadata_key = hot_metadata_key(
            chain,
            &dataset_key,
            &range,
            selector,
            metadata.height,
            &metadata.block_hash,
        )?;
        let bytes = encode_object_rows(encoding, rows)?;

        if metadata.candidate_status == HotCacheCandidateStatus::Active {
            self.demote_active_candidates(
                &hot_logical_prefix(chain, &dataset_key, &range, selector, metadata.height)?,
                &metadata_key,
            )?;
        }

        self.object_store.put(&object_key, &bytes)?;

        let metadata = HotCacheEntryMetadata {
            row_count: rows.row_count(),
            object_size_bytes: bytes.len() as u64,
            checksum: checksum_hex(&bytes),
            checksum_algorithm: "sha256".to_owned(),
            schema_version: HOT_CACHE_SCHEMA_VERSION.to_owned(),
            object_encoding: Some(encoding),
            object_key: object_key.clone(),
            metadata_key: metadata_key.clone(),
            chain: Some(chain.clone()),
            dataset_key: Some(dataset_key),
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            range: Some(range),
            source: QuerySegmentSource::HotCache,
            query_finality: metadata.finality_status.query_finality(),
            ..metadata
        };
        let metadata_bytes = serde_json::to_vec_pretty(&metadata).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode hot cache metadata: {error}"),
            )
        })?;
        self.object_store.put(&metadata_key, &metadata_bytes)?;

        Ok(HotCacheWriteOutcome {
            object_key,
            metadata_key,
            row_count: rows.row_count(),
            object_size_bytes: bytes.len() as u64,
            checksum: metadata.checksum,
            checksum_algorithm: metadata.checksum_algorithm,
        })
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range_kind: LedgerRangeKind,
        start: u64,
        end: u64,
    ) -> Result<HotCacheReadOutcome, DatalensError> {
        let selector_fingerprint = selector.fingerprint();
        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        let mut metadata = Vec::new();
        for entry in self.list_entries(chain, range_kind, start, end)? {
            if entry.dataset_key.as_ref() != Some(dataset_key)
                || entry.selector_fingerprint != selector_fingerprint
                || entry.candidate_status != HotCacheCandidateStatus::Active
            {
                continue;
            }
            let Some(entry_range) = entry.range.clone() else {
                continue;
            };
            let query_range = LedgerRange::try_new(entry_range.kind(), start, end)?;
            if entry_range.intersection(&query_range).is_none() {
                continue;
            }
            let bytes = self.object_store.get(&entry.object_key)?;
            verify_hot_object_metadata(&entry, &bytes)?;
            let encoding = entry.object_encoding.ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!(
                        "hot cache metadata missing object encoding {}",
                        entry.metadata_key
                    ),
                )
            })?;
            let object_rows = decode_object_rows(encoding, dataset_key.clone(), &bytes)?;
            let object_rows = filter_rows(object_rows, query_range);
            rows.try_append(object_rows.into_rows())?;
            metadata.push(entry);
        }
        rows.sort();
        Ok(HotCacheReadOutcome {
            rows: DatasetRows::new(dataset_key.clone(), rows)?,
            metadata,
        })
    }

    pub fn list_entries(
        &self,
        chain: &ChainIdentity,
        range_kind: LedgerRangeKind,
        start: u64,
        end: u64,
    ) -> Result<Vec<HotCacheEntryMetadata>, DatalensError> {
        let prefix = format!(
            "{HOT_CACHE_PREFIX}/chains/{}/ranges/{}",
            chain.key_prefix(),
            range_kind_key(range_kind)
        );
        let mut entries = Vec::new();
        for object in self.object_store.list(&prefix)? {
            if !object.key.ends_with(".metadata.json") {
                continue;
            }
            let entry = self.read_metadata(&object.key)?;
            if entry.height < start || entry.height > end {
                continue;
            }
            entries.push(entry);
        }
        entries.sort_by_key(|entry| {
            (
                entry.height,
                entry.block_hash.clone(),
                entry
                    .dataset_key
                    .as_ref()
                    .map(DatasetKey::as_str)
                    .unwrap_or("")
                    .to_owned(),
                entry.selector_fingerprint.clone(),
            )
        });
        Ok(entries)
    }

    pub fn read_metadata(
        &self,
        metadata_key: &str,
    ) -> Result<HotCacheEntryMetadata, DatalensError> {
        let bytes = self.object_store.get(metadata_key)?;
        let metadata =
            serde_json::from_slice::<HotCacheEntryMetadata>(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode hot cache metadata {metadata_key}: {error}"),
                )
            })?;
        metadata.validate(metadata_key)?;
        Ok(metadata)
    }

    pub fn cleanup(
        &self,
        now_unix_seconds: u64,
        policy: HotCacheRetentionPolicy,
    ) -> Result<HotCacheCleanupReport, DatalensError> {
        let mut deleted_entries = 0;
        for object in self.object_store.list(HOT_CACHE_PREFIX)? {
            if !object.key.ends_with(".metadata.json") {
                continue;
            }
            let entry = self.read_metadata(&object.key)?;
            let expired = now_unix_seconds.saturating_sub(entry.observed_at_unix_seconds)
                > policy.max_age_seconds;
            if !expired {
                continue;
            }
            if policy.retain_active_candidates
                && entry.candidate_status == HotCacheCandidateStatus::Active
            {
                continue;
            }
            self.object_store.delete(&entry.object_key)?;
            self.object_store.delete(&entry.metadata_key)?;
            deleted_entries += 1;
        }
        Ok(HotCacheCleanupReport { deleted_entries })
    }

    fn demote_active_candidates(
        &self,
        logical_prefix: &str,
        current_metadata_key: &str,
    ) -> Result<(), DatalensError> {
        for object in self.object_store.list(logical_prefix)? {
            if object.key == current_metadata_key || !object.key.ends_with(".metadata.json") {
                continue;
            }
            let mut entry = self.read_metadata(&object.key)?;
            if entry.candidate_status != HotCacheCandidateStatus::Active {
                continue;
            }
            entry.candidate_status = HotCacheCandidateStatus::Candidate;
            let bytes = serde_json::to_vec_pretty(&entry).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode demoted hot cache metadata: {error}"),
                )
            })?;
            self.object_store.put(&entry.metadata_key, &bytes)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct HotCacheWriteRequest<'a> {
    pub chain: &'a ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: &'a DatasetSelector,
    pub range: LedgerRange,
    pub rows: &'a DatasetRows,
    pub metadata: HotCacheEntryMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotCacheWriteOutcome {
    pub object_key: String,
    pub metadata_key: String,
    pub row_count: usize,
    pub object_size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotCacheReadOutcome {
    pub rows: DatasetRows,
    pub metadata: Vec<HotCacheEntryMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotCacheEntryMetadata {
    pub block_hash: String,
    pub parent_hash: String,
    pub height: u64,
    pub observed_at_unix_seconds: u64,
    pub source_provider: String,
    pub finality_status: HotCacheFinalityStatus,
    pub row_count: usize,
    pub object_size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
    pub candidate_status: HotCacheCandidateStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_branch: Option<String>,
    pub eligible_for_promotion: bool,
    pub schema_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_encoding: Option<ObjectEncoding>,
    pub object_key: String,
    pub metadata_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_key: Option<DatasetKey>,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<LedgerRange>,
    pub source: QuerySegmentSource,
    pub query_finality: QueryDataFinality,
}

impl HotCacheEntryMetadata {
    fn validate(&self, metadata_key: &str) -> Result<(), DatalensError> {
        validate_object_key(metadata_key)?;
        validate_object_key(&self.object_key)?;
        validate_object_key(&self.metadata_key)?;
        if self.metadata_key != metadata_key {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("hot cache metadata key mismatch {metadata_key}"),
            ));
        }
        if self.schema_version != HOT_CACHE_SCHEMA_VERSION {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!(
                    "unsupported hot cache schema version {} in {metadata_key}",
                    self.schema_version
                ),
            ));
        }
        if self.source != QuerySegmentSource::HotCache {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata source must be hot_cache",
            ));
        }
        if self.query_finality != self.finality_status.query_finality() {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata query finality does not match finality status",
            ));
        }
        if self.row_count == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache data object metadata must have row_count greater than zero",
            ));
        }
        if self.checksum_algorithm != "sha256" {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!(
                    "hot cache metadata unsupported checksum algorithm {}",
                    self.checksum_algorithm
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotCacheFinalityStatus {
    Finalized,
    Safe,
    Unsafe,
    Latest,
}

impl HotCacheFinalityStatus {
    fn query_finality(self) -> QueryDataFinality {
        match self {
            Self::Finalized => QueryDataFinality::Finalized,
            Self::Safe => QueryDataFinality::Safe,
            Self::Unsafe => QueryDataFinality::Unsafe,
            Self::Latest => QueryDataFinality::Latest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotCacheCandidateStatus {
    Active,
    Candidate,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HotCacheRetentionPolicy {
    pub max_age_seconds: u64,
    pub retain_active_candidates: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HotCacheCleanupReport {
    pub deleted_entries: usize,
}

fn hot_object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
    block_hash: &str,
    encoding: ObjectEncoding,
) -> Result<String, DatalensError> {
    let hash = hot_hash_key(block_hash)?;
    let key = format!(
        "{}/{}/{}.rows{}",
        hot_logical_prefix(chain, dataset_key, range, selector, height)?,
        hash,
        hash,
        encoding.extension()
    );
    validate_object_key(&key)?;
    Ok(key)
}

fn hot_metadata_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
    block_hash: &str,
) -> Result<String, DatalensError> {
    let hash = hot_hash_key(block_hash)?;
    let key = format!(
        "{}/{}/{}.metadata.json",
        hot_logical_prefix(chain, dataset_key, range, selector, height)?,
        hash,
        hash
    );
    validate_object_key(&key)?;
    Ok(key)
}

fn hot_logical_prefix(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
) -> Result<String, DatalensError> {
    let prefix = format!(
        "{HOT_CACHE_PREFIX}/chains/{}/ranges/{}/{:#020}-{:#020}/datasets/{}/{}/{}/height-{:#020}",
        chain.key_prefix(),
        range_kind_key(range.kind()),
        range.start(),
        range.end(),
        dataset_key.as_str(),
        HOT_CACHE_SCHEMA_VERSION,
        selector.fingerprint(),
        height
    )
    .replace("0x", "");
    validate_object_key(&prefix)?;
    Ok(prefix)
}

fn hot_hash_key(block_hash: &str) -> Result<String, DatalensError> {
    let hash = block_hash
        .trim()
        .strip_prefix("0x")
        .unwrap_or(block_hash.trim());
    if hash.is_empty()
        || !hash.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        })
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "hot cache block hash must be a non-empty storage-safe value",
        ));
    }
    Ok(hash.to_owned())
}

fn verify_hot_object_metadata(
    metadata: &HotCacheEntryMetadata,
    bytes: &[u8],
) -> Result<(), DatalensError> {
    let entry = crate::ManifestEntry {
        chain: metadata.chain.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing chain identity",
            )
        })?,
        dataset_key: metadata.dataset_key.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing dataset key",
            )
        })?,
        range: metadata.range.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing range",
            )
        })?,
        selector_fingerprint: metadata.selector_fingerprint.clone(),
        selector_canonical_key: metadata.selector_canonical_key.clone(),
        finality_level: crate::ManifestFinalityLevel::Safe,
        object_key: Some(metadata.object_key.clone()),
        object_encoding: metadata.object_encoding,
        row_count: metadata.row_count,
        object_size_bytes: Some(metadata.object_size_bytes),
        checksum: Some(metadata.checksum.clone()),
        checksum_algorithm: Some(metadata.checksum_algorithm.clone()),
        written_at_unix_seconds: Some(metadata.observed_at_unix_seconds),
    };
    verify_manifest_object_metadata(&entry, &metadata.object_key, bytes)
}

fn empty_rows(dataset_key: DatasetKey) -> Result<DatasetRows, DatalensError> {
    let rows = match dataset_key.legacy_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    };
    DatasetRows::new(dataset_key, rows)
}
