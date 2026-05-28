use datalens_chain::{AdapterCapabilities, DatasetSelector, HeightRangeKind, ReorgSignal};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryDataFinality, QueryRows, QuerySegmentSource,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::{
    LocalObjectStore, ObjectEncoding, ObjectStore, checksum_hex, decode_object_rows,
    encode_object_rows, filter_rows, merge_ranges, object_encoding_for_dataset, range_kind_key,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotCacheConfig {
    pub reorg_window: u64,
}

impl Default for HotCacheConfig {
    fn default() -> Self {
        Self { reorg_window: 64 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotWriteRequest<'a> {
    pub chain: &'a ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: &'a DatasetSelector,
    pub range: LedgerRange,
    pub rows: &'a DatasetRows,
    pub reorg_signals: &'a [ReorgSignal],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotWriteOutcome {
    pub range: LedgerRange,
    pub row_count: usize,
    pub reorg: Option<HotReorgOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotReorgOutcome {
    pub reason: HotReorgReason,
    pub rollback_height: u64,
    pub stale_entries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotReorgReason {
    ParentMismatch,
    SameHeightDifferentHash,
    ProviderCanonicalHashChanged,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotManifest {
    #[serde(default)]
    pub entries: Vec<HotManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotManifestEntry {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub status: HotEntryStatus,
    pub object_key: String,
    pub row_count: usize,
    pub blocks: Vec<HotBlockMetadata>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotEntryStatus {
    Active,
    Stale,
    InactiveCandidate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotBlockMetadata {
    pub range_kind: LedgerRangeKind,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

impl From<&ReorgSignal> for HotBlockMetadata {
    fn from(signal: &ReorgSignal) -> Self {
        Self {
            range_kind: signal.range_kind.clone(),
            height: signal.height,
            hash: signal.hash.clone(),
            parent_hash: signal.parent_hash.clone(),
            timestamp: signal.timestamp,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HotCache<S> {
    object_store: S,
    config: HotCacheConfig,
}

impl<S> HotCache<S>
where
    S: ObjectStore,
{
    pub fn new(object_store: S, config: HotCacheConfig) -> Self {
        Self {
            object_store,
            config,
        }
    }

    pub fn validate_adapter_support(
        capabilities: &AdapterCapabilities,
        dataset_key: &DatasetKey,
    ) -> Result<(), DatalensError> {
        let Some(dataset) = capabilities.dataset(dataset_key) else {
            return Err(DatalensError::unsupported(
                "adapter does not support dataset for hot cache reorg detection",
            ));
        };
        if !dataset.supports_reorg_signals()
            || !dataset.supports_canonical_block_lookup()
            || !dataset.supports_latest_reorg_signal()
        {
            return Err(DatalensError::unsupported(
                "adapter does not expose hot cache reorg detection signals",
            ));
        }
        Ok(())
    }

    pub fn manifest(&self, chain: &ChainIdentity) -> Result<HotManifest, DatalensError> {
        let key = hot_manifest_key(chain);
        if !self.object_store.exists(&key)? {
            return Ok(HotManifest::default());
        }
        let bytes = self.object_store.get(&key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::storage_read(format!("decode hot manifest {key}: {error}"))
        })
    }

    pub fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        let selector_fingerprint = selector.fingerprint();
        let ranges = self
            .manifest(chain)?
            .entries
            .into_iter()
            .filter(|entry| {
                entry.status == HotEntryStatus::Active
                    && entry.chain == *chain
                    && entry.dataset_key == *dataset_key
                    && entry.selector_fingerprint == selector_fingerprint
            })
            .filter_map(|entry| entry.range.intersection(&range))
            .collect::<Vec<_>>();
        Ok(merge_ranges(ranges))
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        let selector_fingerprint = selector.fingerprint();
        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        for entry in self.manifest(chain)?.entries {
            if entry.status != HotEntryStatus::Active
                || entry.chain != *chain
                || entry.dataset_key != *dataset_key
                || entry.selector_fingerprint != selector_fingerprint
                || entry.range.intersection(&range).is_none()
            {
                continue;
            }
            if entry.blocks.is_empty() {
                return Err(DatalensError::storage_read(
                    "active hot cache entry is missing reorg metadata",
                ));
            }
            let bytes = self.object_store.get(&entry.object_key)?;
            let cached = serde_json::from_slice::<DatasetRows>(&bytes).map_err(|error| {
                DatalensError::storage_read(format!(
                    "decode hot cache rows {}: {error}",
                    entry.object_key
                ))
            })?;
            let cached = filter_rows(cached, range.clone());
            rows.try_append(cached.into_rows())?;
        }
        rows.sort();
        DatasetRows::new(dataset_key.clone(), rows)
    }

    pub fn write_rows(
        &self,
        request: HotWriteRequest<'_>,
    ) -> Result<HotWriteOutcome, DatalensError> {
        if request.rows.dataset_key() != &request.dataset_key {
            return Err(DatalensError::internal(
                "dataset rows key does not match hot cache dataset key",
            ));
        }
        validate_hot_signals(&request.range, request.reorg_signals)?;

        let mut manifest = self.manifest(request.chain)?;
        let reorg = detect_reorg(
            &manifest,
            request.chain,
            request.range.kind(),
            request.reorg_signals,
        );
        let reorg = match reorg {
            Some((reason, rollback_height)) => {
                validate_reorg_window(
                    &manifest,
                    request.chain,
                    rollback_height,
                    self.config.reorg_window,
                )?;
                let stale_entries = mark_stale_from(
                    &mut manifest,
                    request.chain,
                    request.range.kind(),
                    rollback_height,
                );
                Some(HotReorgOutcome {
                    reason,
                    rollback_height,
                    stale_entries,
                })
            }
            None => None,
        };

        let object_key = hot_object_key(
            request.chain,
            &request.dataset_key,
            &request.range,
            &request.selector.fingerprint(),
            request.reorg_signals,
        );
        let bytes = serde_json::to_vec(request.rows)
            .map_err(|error| DatalensError::internal(format!("encode hot rows: {error}")))?;
        self.object_store.put(&object_key, &bytes)?;

        manifest.entries.push(HotManifestEntry {
            chain: request.chain.clone(),
            dataset_key: request.dataset_key.clone(),
            range: request.range.clone(),
            selector_fingerprint: request.selector.fingerprint(),
            selector_canonical_key: request.selector.canonical_key(),
            status: HotEntryStatus::Active,
            object_key,
            row_count: request.rows.row_count(),
            blocks: request
                .reorg_signals
                .iter()
                .map(HotBlockMetadata::from)
                .collect(),
        });
        self.write_manifest(request.chain, &manifest)?;

        Ok(HotWriteOutcome {
            range: request.range,
            row_count: request.rows.row_count(),
            reorg,
        })
    }

    fn write_manifest(
        &self,
        chain: &ChainIdentity,
        manifest: &HotManifest,
    ) -> Result<(), DatalensError> {
        let key = hot_manifest_key(chain);
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| DatalensError::internal(format!("encode hot manifest: {error}")))?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write hot manifest {key}: {}", error.message),
            )
        })
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
        let object_key = hot_storage_object_key(
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
            source: QuerySegmentSource::Hot,
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

    pub fn mark_promoted(
        &self,
        metadata_keys: &[String],
        promoted_at_unix_seconds: u64,
    ) -> Result<(), DatalensError> {
        for metadata_key in metadata_keys {
            let mut entry = self.read_metadata(metadata_key)?;
            entry.promoted_at_unix_seconds = Some(promoted_at_unix_seconds);
            entry.eligible_for_promotion = false;
            let bytes = serde_json::to_vec_pretty(&entry).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode promoted hot cache metadata: {error}"),
                )
            })?;
            self.object_store.put(metadata_key, &bytes)?;
        }
        Ok(())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_at_unix_seconds: Option<u64>,
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
        if self.source != QuerySegmentSource::Hot {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata source must be hot",
            ));
        }
        if self.query_finality != self.finality_status.query_finality() {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata query finality does not match finality status",
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

fn validate_hot_signals(range: &LedgerRange, signals: &[ReorgSignal]) -> Result<(), DatalensError> {
    if signals.len() != range.len() as usize {
        return Err(DatalensError::invalid_input(
            "hot cache writes require one reorg signal per height",
        ));
    }
    for (offset, signal) in signals.iter().enumerate() {
        let expected = range.start() + offset as u64;
        if signal.range_kind != range.kind() || signal.height != expected {
            return Err(DatalensError::invalid_input(
                "hot cache reorg signals must cover the written range in order",
            ));
        }
    }
    Ok(())
}

fn detect_reorg(
    manifest: &HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    signals: &[ReorgSignal],
) -> Option<(HotReorgReason, u64)> {
    for signal in signals {
        if let Some(existing) = active_block(manifest, chain, range_kind.clone(), signal.height)
            && existing.hash != signal.hash
        {
            return Some((HotReorgReason::SameHeightDifferentHash, signal.height));
        }
    }

    for pair in signals.windows(2) {
        if pair[1].parent_hash != pair[0].hash {
            return Some((HotReorgReason::ParentMismatch, pair[1].height));
        }
    }

    if let Some(first) = signals.first()
        && let Some(previous) = first
            .height
            .checked_sub(1)
            .and_then(|height| active_block(manifest, chain, range_kind, height))
        && first.parent_hash != previous.hash
    {
        return Some((HotReorgReason::ParentMismatch, first.height));
    }

    None
}

fn active_block<'a>(
    manifest: &'a HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    height: u64,
) -> Option<&'a HotBlockMetadata> {
    manifest
        .entries
        .iter()
        .rev()
        .filter(|entry| entry.status == HotEntryStatus::Active && entry.chain == *chain)
        .flat_map(|entry| entry.blocks.iter())
        .find(|block| block.range_kind == range_kind && block.height == height)
}

fn validate_reorg_window(
    manifest: &HotManifest,
    chain: &ChainIdentity,
    rollback_height: u64,
    reorg_window: u64,
) -> Result<(), DatalensError> {
    let Some(active_tip) = manifest
        .entries
        .iter()
        .filter(|entry| entry.status == HotEntryStatus::Active && entry.chain == *chain)
        .flat_map(|entry| entry.blocks.iter().map(|block| block.height))
        .max()
    else {
        return Ok(());
    };
    if rollback_height <= active_tip {
        let depth = active_tip - rollback_height + 1;
        if depth > reorg_window {
            return Err(DatalensError::provider_limit(format!(
                "hot reorg window exceeded: rollback depth {depth}, configured window {reorg_window}"
            )));
        }
    }
    Ok(())
}

fn mark_stale_from(
    manifest: &mut HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    rollback_height: u64,
) -> usize {
    let mut stale_entries = 0usize;
    for entry in &mut manifest.entries {
        if entry.status != HotEntryStatus::Active || entry.chain != *chain {
            continue;
        }
        let affected = entry
            .blocks
            .iter()
            .any(|block| block.range_kind == range_kind && block.height >= rollback_height);
        if affected {
            entry.status = HotEntryStatus::Stale;
            stale_entries += 1;
        }
    }
    stale_entries
}

fn hot_manifest_key(chain: &ChainIdentity) -> String {
    format!("hot/chains/{}/manifest.json", chain.key_prefix())
}

fn hot_object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector_fingerprint: &str,
    signals: &[ReorgSignal],
) -> String {
    let hash = signals
        .last()
        .map(|signal| signal.hash.as_str())
        .unwrap_or("empty");
    format!(
        "hot/chains/{}/datasets/{}/{}/{}/{}-{}-{}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
        range.start(),
        range.end(),
        storage_key_segment(hash),
    )
}

fn hot_storage_object_key(
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

fn storage_key_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn empty_rows(dataset_key: DatasetKey) -> Result<DatasetRows, DatalensError> {
    let rows = match dataset_key.legacy_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
        Some(datalens_core::Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
        Some(datalens_core::Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    };
    DatasetRows::new(dataset_key, rows)
}
