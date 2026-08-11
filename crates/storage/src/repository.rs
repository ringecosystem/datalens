use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    TopicFilter, missing_ranges,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, TryLockError},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::selector_coverage::{filter_evm_log_rows_for_selector, selector_coverage_candidates};
use crate::{compaction_queue, coverage_index, read_through_cache};

const STORAGE_READ_GET_PARALLELISM: usize = 8;

#[derive(Debug)]
/// Storage write request for one durable coverage segment. The caller must pass
/// only safe/finalized finality; storage enforces that before writing an object
/// key or empty coverage entry.
pub struct StorageWriteRequest<'a> {
    pub chain: &'a ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: &'a DatasetSelector,
    pub range: LedgerRange,
    pub rows: &'a DatasetRows,
    pub finality_level: FinalityLevel,
    pub record_empty_coverage: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageWriteOutcome {
    pub range: LedgerRange,
    pub row_count: usize,
    pub data_object: Option<StorageDataObject>,
    pub recorded_empty_coverage: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageDataObject {
    pub object_key: String,
    pub object_encoding: ObjectEncoding,
    pub object_compression: Option<ParquetCompression>,
    pub row_count: usize,
    pub object_size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
    pub written_at_unix_seconds: u64,
}

pub use crate::helpers::coverage_key;
pub(crate) use crate::helpers::{
    checksum_hex, decode_object_rows, empty_rows, encode_object_rows, filter_rows, intersect,
    manifest_key, manifest_segment_key, manifest_segment_prefix, manifest_version_key,
    merge_ranges, object_encoding_for_dataset, object_key, range_kind_key, unix_seconds_now,
    validate_existing_data_object, verify_manifest_object_metadata,
};
pub use crate::hot_cache::{
    HOT_CACHE_SCHEMA_VERSION, HotBlockMetadata, HotCache, HotCacheCandidateStatus,
    HotCacheCleanupReport, HotCacheConfig, HotCacheEntryMetadata, HotCacheFinalityStatus,
    HotCacheReadOutcome, HotCacheRetentionPolicy, HotCacheStorage, HotCacheWriteOutcome,
    HotCacheWriteRequest, HotEntryStatus, HotManifest, HotManifestEntry, HotReorgOutcome,
    HotReorgReason, HotWriteOutcome, HotWriteRequest, LocalHotCacheStorage,
};
pub use crate::maintenance::{
    CompactionCandidate, CoverageDeltaBacklogScope, MaintenanceCheckReport,
    MaintenanceCompactionConfig, MaintenanceCompactionPressure,
    MaintenanceCompactionPressureMonitor, MaintenanceCompactionReport,
    MaintenanceCompactionTickStatus, MaintenanceFragmentationReport, MaintenanceIssue,
    MaintenanceIssueKind, MaintenanceOperation, MaintenanceOperationMode, MaintenanceReport,
    MaintenanceRetentionReport, MaintenanceUsageLedgerReport, RetentionPolicy,
    UsageLedgerRollupModel,
};
pub use crate::manifest::{Manifest, ManifestEntry, ManifestFinalityLevel};
pub use crate::object_store::{
    LocalObjectStore, ObjectListPage, ObjectLockLease, ObjectMetadata, ObjectPutIfAbsentResult,
    ObjectStore, S3ObjectStore, S3ObjectStoreConfig, encode_object_lock_owner, validate_object_key,
};
pub use crate::query_activity::{QueryActivity, QueryActivityKey, QueryActivityRepository};
pub use crate::query_watermark::{QueryWatermark, QueryWatermarkKey, QueryWatermarkRepository};
pub use crate::read_through_cache::ReadThroughCacheConfig;
pub use crate::usage_ledger::{
    CacheOutcome, DurableWriteOutcome, FillOutcome, QueryOutcome, UsageLedgerEntry,
    UsageLedgerRepository, UsageLedgerStore,
};

#[derive(Clone, Debug)]
/// Durable object repository plus an in-process read-through cache. The
/// read-through cache only memoizes decoded objects for reads; it is separate
/// from writer staging and is never a source of manifest coverage.
pub struct DurableStorage<S> {
    object_store: S,
    read_through_cache: read_through_cache::ReadThroughCache,
    manifest_update_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    manifest_cache: Arc<Mutex<BTreeMap<String, ManifestCacheEntry>>>,
    config: DurableStorageConfig,
}

#[derive(Clone, Debug)]
struct ManifestCacheEntry {
    manifest: Manifest,
    version: Option<Vec<u8>>,
}

pub type LocalStorage = DurableStorage<LocalObjectStore>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStorageConfig {
    #[serde(default)]
    pub parquet_compression: ParquetCompression,
    #[serde(default = "default_legacy_coverage_index_write_enabled")]
    pub legacy_coverage_index_write_enabled: bool,
}

const fn default_legacy_coverage_index_write_enabled() -> bool {
    true
}

impl Default for DurableStorageConfig {
    fn default() -> Self {
        Self {
            parquet_compression: ParquetCompression::default(),
            legacy_coverage_index_write_enabled: default_legacy_coverage_index_write_enabled(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    #[default]
    None,
    Snappy,
    Zstd,
}

impl ParquetCompression {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Snappy => "snappy",
            Self::Zstd => "zstd",
        }
    }
}

fn object_compression_for_encoding(
    encoding: ObjectEncoding,
    parquet_compression: ParquetCompression,
) -> Option<ParquetCompression> {
    match encoding {
        ObjectEncoding::ParquetV1 => Some(parquet_compression),
        ObjectEncoding::Json => None,
    }
}

fn merged_selector_ranges(
    entries: &[ManifestEntry],
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Vec<LedgerRange> {
    let mut ranges = selector_coverage_candidates(entries, chain, dataset_key, selector, range)
        .into_iter()
        .flat_map(|candidate| candidate.ranges)
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.start());
    merge_ranges(ranges)
}

fn durable_finality_satisfies(
    entry: ManifestFinalityLevel,
    requested: ManifestFinalityLevel,
) -> bool {
    match requested {
        ManifestFinalityLevel::Safe => {
            matches!(
                entry,
                ManifestFinalityLevel::Safe | ManifestFinalityLevel::Finalized
            )
        }
        ManifestFinalityLevel::Finalized => entry == ManifestFinalityLevel::Finalized,
    }
}

fn should_repair_polluted_empty_evm_log_coverage(selector: &DatasetSelector) -> bool {
    let DatasetSelector::EvmLogs(filter) = selector else {
        return false;
    };
    filter.topics().is_empty()
        || filter
            .topics()
            .iter()
            .all(|topic| matches!(topic, TopicFilter::Wildcard))
}

fn replacement_scope_matches(existing: &ManifestEntry, replacement: &ManifestEntry) -> bool {
    existing.chain == replacement.chain
        && existing.dataset_key == replacement.dataset_key
        && existing.selector_fingerprint == replacement.selector_fingerprint
        && existing.selector_canonical_key == replacement.selector_canonical_key
        && existing.finality_level == replacement.finality_level
        && existing.range.kind() == replacement.range.kind()
}

fn can_merge_empty_coverage(left: &ManifestEntry, right: &ManifestEntry) -> bool {
    left.object_key.is_none()
        && right.object_key.is_none()
        && replacement_scope_matches(left, right)
        && left.range.end().saturating_add(1) >= right.range.start()
        && right.range.end().saturating_add(1) >= left.range.start()
}

fn split_entry_around_range(
    entry: ManifestEntry,
    replacement_range: &LedgerRange,
) -> Result<Vec<ManifestEntry>, DatalensError> {
    let mut entries = Vec::new();
    if entry.range.start() < replacement_range.start() {
        let mut left = entry.clone();
        left.range = LedgerRange::try_new(
            entry.range.kind(),
            entry.range.start(),
            replacement_range.start().saturating_sub(1),
        )?;
        entries.push(left);
    }
    if entry.range.end() > replacement_range.end() {
        let mut right = entry;
        right.range = LedgerRange::try_new(
            right.range.kind(),
            replacement_range.end().saturating_add(1),
            right.range.end(),
        )?;
        entries.push(right);
    }
    Ok(entries)
}

fn manifest_entry_read_key(
    entry: &ManifestEntry,
) -> (String, String, String, String, u64, u64, &'static str) {
    (
        entry.chain.key_prefix(),
        entry.dataset_key.as_str().to_owned(),
        entry.selector_fingerprint.clone(),
        range_kind_key(entry.range.kind()),
        entry.range.start(),
        entry.range.end(),
        entry.finality_level.as_str(),
    )
}

#[derive(Debug, Eq, PartialEq)]
struct StorageWriteLogContext {
    chain_key: String,
    dataset: String,
    selector_fingerprint: String,
    range_kind: String,
    start: u64,
    end: u64,
    row_count: usize,
    finality: &'static str,
    object_key_present: bool,
    coverage_kind: &'static str,
}

impl StorageWriteLogContext {
    fn from_entry(entry: &ManifestEntry) -> Self {
        let object_key_present = entry.object_key.is_some();
        Self {
            chain_key: entry.chain.key_prefix(),
            dataset: entry.dataset_key.as_str().to_owned(),
            selector_fingerprint: entry.selector_fingerprint.clone(),
            range_kind: range_kind_key(entry.range.kind()),
            start: entry.range.start(),
            end: entry.range.end(),
            row_count: entry.row_count,
            finality: entry.finality_level.as_str(),
            object_key_present,
            coverage_kind: if object_key_present {
                "data_object"
            } else {
                "empty_coverage"
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageCoveragePlan {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector_fingerprint: String,
    selector_canonical_key: String,
    range: LedgerRange,
    covered_ranges: Vec<LedgerRange>,
    index_entries: Option<Vec<ManifestEntry>>,
}

impl StorageCoveragePlan {
    fn from_covered_ranges(
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        covered_ranges: Vec<LedgerRange>,
    ) -> Self {
        Self {
            chain: chain.clone(),
            dataset_key: dataset_key.clone(),
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            range,
            covered_ranges,
            index_entries: None,
        }
    }

    fn from_index_entries(
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        covered_ranges: Vec<LedgerRange>,
        index_entries: Vec<ManifestEntry>,
    ) -> Self {
        Self {
            chain: chain.clone(),
            dataset_key: dataset_key.clone(),
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            range,
            covered_ranges,
            index_entries: Some(index_entries),
        }
    }

    pub fn covered_ranges(&self) -> &[LedgerRange] {
        &self.covered_ranges
    }

    fn validate_read(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: &LedgerRange,
    ) -> Result<(), DatalensError> {
        if self.chain != *chain
            || self.dataset_key != *dataset_key
            || self.selector_fingerprint != selector.fingerprint()
            || self.selector_canonical_key != selector.canonical_key()
            || self.range.kind() != range.kind()
            || self.range.start() > range.start()
            || self.range.end() < range.end()
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "storage coverage plan does not match read request",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct StorageReadPlan {
    coverage_entries_count: usize,
    data_object_count: usize,
    empty_coverage_count: usize,
    objects: Vec<StorageReadPlanObject>,
    reads: Vec<StorageReadPlanRead>,
}

struct StorageReadEntriesRequest<'a> {
    chain: &'a ChainIdentity,
    dataset_key: &'a DatasetKey,
    selector: &'a DatasetSelector,
    range: LedgerRange,
    finality_level: Option<ManifestFinalityLevel>,
    coverage_source: &'static str,
    entries: Vec<ManifestEntry>,
    coverage_started: Instant,
}

#[derive(Clone, Debug)]
struct StorageReadPlanObject {
    object_key: String,
    entry: ManifestEntry,
    encoding: ObjectEncoding,
}

#[derive(Clone, Debug)]
struct StorageReadPlanRead {
    object_key: String,
    ranges: Vec<LedgerRange>,
}

#[derive(Debug)]
struct FetchedReadPlanObject {
    object_key: String,
    entry: ManifestEntry,
    encoding: ObjectEncoding,
    bytes_len: usize,
    rows: DatasetRows,
}

impl StorageReadPlan {
    fn from_candidates(
        candidates: Vec<crate::selector_coverage::SelectorCoverageCandidate<'_>>,
    ) -> Result<Self, DatalensError> {
        let mut candidates = candidates;
        candidates.sort_by_key(|candidate| candidate.entry.object_key.is_none());
        let coverage_entries_count = candidates.len();
        let mut data_object_count = 0usize;
        let mut empty_coverage_count = 0usize;
        let mut seen_objects = BTreeSet::new();
        let mut emitted_ranges = Vec::new();
        let mut objects = Vec::new();
        let mut reads = Vec::new();

        for candidate in candidates {
            let ranges = candidate
                .ranges
                .into_iter()
                .flat_map(|range| missing_ranges(range, &emitted_ranges))
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                continue;
            }
            emitted_ranges = merge_ranges(
                emitted_ranges
                    .into_iter()
                    .chain(ranges.iter().cloned())
                    .collect(),
            );
            let Some(object_key) = candidate.entry.object_key.clone() else {
                empty_coverage_count += 1;
                continue;
            };
            data_object_count += 1;
            let Some(encoding) = candidate.entry.object_encoding else {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("manifest entry object {object_key} missing object_encoding"),
                ));
            };
            if seen_objects.insert(object_key.clone()) {
                objects.push(StorageReadPlanObject {
                    object_key: object_key.clone(),
                    entry: candidate.entry.clone(),
                    encoding,
                });
            }
            reads.push(StorageReadPlanRead { object_key, ranges });
        }

        Ok(Self {
            coverage_entries_count,
            data_object_count,
            empty_coverage_count,
            objects,
            reads,
        })
    }
}

impl DurableStorage<LocalObjectStore> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::new_with_config(root, DurableStorageConfig::default())
    }

    pub fn new_with_config(root: impl Into<PathBuf>, config: DurableStorageConfig) -> Self {
        Self {
            object_store: LocalObjectStore::new(root),
            read_through_cache: read_through_cache::ReadThroughCache::new(
                ReadThroughCacheConfig::default(),
            ),
            manifest_update_locks: Arc::new(Mutex::new(BTreeMap::new())),
            manifest_cache: Arc::new(Mutex::new(BTreeMap::new())),
            config,
        }
    }

    pub fn root(&self) -> &Path {
        self.object_store.root()
    }

    pub fn manifest_path(&self, chain: &ChainIdentity) -> PathBuf {
        self.root().join(manifest_key(chain))
    }
}

impl<S> DurableStorage<S>
where
    S: ObjectStore,
{
    pub fn from_object_store(object_store: S) -> Self {
        Self::from_object_store_with_config(object_store, DurableStorageConfig::default())
    }

    pub fn from_object_store_with_config(object_store: S, config: DurableStorageConfig) -> Self {
        Self::from_object_store_with_read_through_cache_and_storage_config(
            object_store,
            ReadThroughCacheConfig::default(),
            config,
        )
    }

    pub fn from_object_store_with_read_through_cache_config(
        object_store: S,
        read_through_cache_config: ReadThroughCacheConfig,
    ) -> Self {
        Self::from_object_store_with_read_through_cache_and_storage_config(
            object_store,
            read_through_cache_config,
            DurableStorageConfig::default(),
        )
    }

    pub fn from_object_store_with_read_through_cache_and_storage_config(
        object_store: S,
        read_through_cache_config: ReadThroughCacheConfig,
        config: DurableStorageConfig,
    ) -> Self {
        Self {
            object_store,
            read_through_cache: read_through_cache::ReadThroughCache::new(
                read_through_cache_config,
            ),
            manifest_update_locks: Arc::new(Mutex::new(BTreeMap::new())),
            manifest_cache: Arc::new(Mutex::new(BTreeMap::new())),
            config,
        }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    pub(crate) fn parquet_compression(&self) -> ParquetCompression {
        self.config.parquet_compression
    }

    fn legacy_coverage_index_write_enabled(&self) -> bool {
        self.config.legacy_coverage_index_write_enabled
    }

    pub fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        Ok(self
            .coverage_plan(chain, dataset_key, selector, range)?
            .covered_ranges()
            .to_vec())
    }

    pub fn coverage_plan(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<StorageCoveragePlan, DatalensError> {
        let started = Instant::now();
        if let Some(index_entries) = coverage_index::read_entries_for_query(
            &self.object_store,
            chain,
            dataset_key,
            selector,
            &range,
        )? {
            let covered =
                merged_selector_ranges(&index_entries, chain, dataset_key, selector, &range);
            let missing = missing_ranges(range.clone(), &covered);
            log::info!(
                "storage coverage lookup source=coverage_index chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} index_entries_count={} covered_range_count={} missing_range_count={} duration_ms={}",
                chain.key_prefix(),
                dataset_key.as_str(),
                selector.fingerprint(),
                range_kind_key(range.kind()),
                range.start(),
                range.end(),
                index_entries.len(),
                covered.len(),
                missing.len(),
                started.elapsed().as_millis()
            );
            return Ok(StorageCoveragePlan::from_index_entries(
                chain,
                dataset_key,
                selector,
                range,
                covered,
                index_entries,
            ));
        }

        log::info!(
            "storage coverage lookup source=coverage_index_absent chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} index_entries_count=0 covered_range_count=0 missing_range_count=1 duration_ms={}",
            chain.key_prefix(),
            dataset_key.as_str(),
            selector.fingerprint(),
            range_kind_key(range.kind()),
            range.start(),
            range.end(),
            started.elapsed().as_millis()
        );
        Ok(StorageCoveragePlan::from_covered_ranges(
            chain,
            dataset_key,
            selector,
            range,
            Vec::new(),
        ))
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.read_rows_with_finality_filter(chain, dataset_key, selector, range, None)
    }

    pub fn read_rows_with_coverage_plan(
        &self,
        coverage_plan: &StorageCoveragePlan,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        coverage_plan.validate_read(chain, dataset_key, selector, &range)?;
        if let Some(entries) = coverage_plan.index_entries.clone() {
            self.read_rows_with_entries(StorageReadEntriesRequest {
                chain,
                dataset_key,
                selector,
                range,
                finality_level: None,
                coverage_source: "coverage_plan",
                entries,
                coverage_started: Instant::now(),
            })
        } else {
            self.read_rows(chain, dataset_key, selector, range)
        }
    }

    pub fn read_rows_for_finality(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: FinalityLevel,
    ) -> Result<DatasetRows, DatalensError> {
        self.read_rows_with_finality_filter(
            chain,
            dataset_key,
            selector,
            range,
            Some(ManifestFinalityLevel::try_from(finality_level)?),
        )
    }

    fn read_rows_with_finality_filter(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: Option<ManifestFinalityLevel>,
    ) -> Result<DatasetRows, DatalensError> {
        log::debug!(
            "storage read dataset={} range={}-{}",
            dataset_key.as_str(),
            range.start(),
            range.end()
        );
        let coverage_started = Instant::now();
        let (coverage_source, entries) = if let Some(index_entries) =
            coverage_index::read_entries_for_query(
                &self.object_store,
                chain,
                dataset_key,
                selector,
                &range,
            )? {
            ("coverage_index", index_entries)
        } else {
            ("coverage_index_absent", Vec::new())
        };
        self.read_rows_with_entries(StorageReadEntriesRequest {
            chain,
            dataset_key,
            selector,
            range,
            finality_level,
            coverage_source,
            entries,
            coverage_started,
        })
    }

    fn read_rows_with_entries(
        &self,
        request: StorageReadEntriesRequest<'_>,
    ) -> Result<DatasetRows, DatalensError> {
        let StorageReadEntriesRequest {
            chain,
            dataset_key,
            selector,
            range,
            finality_level,
            coverage_source,
            entries,
            coverage_started,
        } = request;
        let mut entries = entries
            .into_iter()
            .filter(|entry| {
                finality_level
                    .map(|finality_level| {
                        durable_finality_satisfies(entry.finality_level, finality_level)
                    })
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        self.augment_evm_log_read_entries_for_polluted_empty_coverage(
            chain,
            dataset_key,
            selector,
            &range,
            finality_level,
            &mut entries,
        )?;
        let entries_count = entries.len();
        let covered = merged_selector_ranges(&entries, chain, dataset_key, selector, &range);
        let missing_range_count = missing_ranges(range.clone(), &covered).len();
        let candidates =
            selector_coverage_candidates(&entries, chain, dataset_key, selector, &range);
        let candidate_count = candidates.len();
        let data_object_count = candidates
            .iter()
            .filter(|candidate| candidate.entry.object_key.is_some())
            .count();
        let empty_coverage_count = candidate_count - data_object_count;
        log::info!(
            "storage read coverage lookup source={} chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} coverage_entries_count={} covered_range_count={} missing_range_count={} matched_entries_count={} data_object_count={} empty_coverage_count={} duration_ms={}",
            coverage_source,
            chain.key_prefix(),
            dataset_key.as_str(),
            selector.fingerprint(),
            range_kind_key(range.kind()),
            range.start(),
            range.end(),
            entries_count,
            covered.len(),
            missing_range_count,
            candidate_count,
            data_object_count,
            empty_coverage_count,
            coverage_started.elapsed().as_millis()
        );
        let plan = StorageReadPlan::from_candidates(candidates)?;
        let mut object_rows_by_key = BTreeMap::new();
        let mut cache_hits = 0usize;
        for object in &plan.objects {
            if let Some(rows) =
                self.read_through_cache
                    .get(&object.object_key, &object.entry, object.encoding)
            {
                cache_hits += 1;
                object_rows_by_key.insert(object.object_key.clone(), rows);
            }
        }
        let cache_misses = plan.objects.len().saturating_sub(cache_hits);
        let get_started = Instant::now();
        let fetched_objects = self.fetch_read_plan_objects(&plan, &object_rows_by_key)?;
        let object_get_count = fetched_objects.len();
        let object_get_bytes = fetched_objects
            .iter()
            .map(|object| object.bytes_len as u64)
            .sum::<u64>();
        for fetched in fetched_objects {
            self.read_through_cache.put(
                &fetched.object_key,
                &fetched.entry,
                fetched.encoding,
                fetched.rows.clone(),
            );
            object_rows_by_key.insert(fetched.object_key, fetched.rows);
        }
        log::info!(
            "storage read plan chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} coverage_entries_count={} data_object_count={} unique_object_count={} empty_coverage_count={} fragmented_read={} read_through_cache_hits={} read_through_cache_misses={} object_get_count={} object_get_bytes={} get_duration_ms={}",
            chain.key_prefix(),
            dataset_key.as_str(),
            selector.fingerprint(),
            range_kind_key(range.kind()),
            range.start(),
            range.end(),
            plan.coverage_entries_count,
            plan.data_object_count,
            plan.objects.len(),
            plan.empty_coverage_count,
            plan.objects.len() > 1,
            cache_hits,
            cache_misses,
            object_get_count,
            object_get_bytes,
            get_started.elapsed().as_millis()
        );

        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        for read in plan.reads {
            let object_rows = object_rows_by_key.get(&read.object_key).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!(
                        "storage read plan object {} was not loaded",
                        read.object_key
                    ),
                )
            })?;
            for candidate_range in read.ranges {
                let mut candidate_rows = filter_rows(object_rows.clone(), candidate_range);
                candidate_rows = filter_evm_log_rows_for_selector(candidate_rows, selector);
                rows.try_append(candidate_rows.into_rows())?;
            }
        }
        rows.sort();
        DatasetRows::new(dataset_key.clone(), rows)
    }

    fn fetch_read_plan_objects(
        &self,
        plan: &StorageReadPlan,
        cached: &BTreeMap<String, DatasetRows>,
    ) -> Result<Vec<FetchedReadPlanObject>, DatalensError> {
        let misses = plan
            .objects
            .iter()
            .filter(|object| !cached.contains_key(&object.object_key))
            .cloned()
            .collect::<Vec<_>>();
        let mut fetched = Vec::new();
        for chunk in misses.chunks(STORAGE_READ_GET_PARALLELISM.max(1)) {
            let mut chunk_results = std::thread::scope(|scope| {
                let handles = chunk
                    .iter()
                    .map(|object| {
                        scope.spawn(move || {
                            let bytes = self.object_store.get(&object.object_key)?;
                            verify_manifest_object_metadata(
                                &object.entry,
                                &object.object_key,
                                &bytes,
                            )?;
                            let rows = decode_object_rows(
                                object.encoding,
                                object.entry.dataset_key.clone(),
                                &bytes,
                            )
                            .map_err(|error| {
                                DatalensError::new(
                                    DatalensErrorKind::StorageReadFailure,
                                    format!(
                                        "decode cached object {}: {}",
                                        object.object_key, error.message
                                    ),
                                )
                            })?;
                            Ok(FetchedReadPlanObject {
                                object_key: object.object_key.clone(),
                                entry: object.entry.clone(),
                                encoding: object.encoding,
                                bytes_len: bytes.len(),
                                rows,
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().map_err(|_| {
                            DatalensError::new(
                                DatalensErrorKind::StorageReadFailure,
                                "storage read object worker panicked",
                            )
                        })?
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            fetched.append(&mut chunk_results);
        }
        Ok(fetched)
    }

    fn augment_evm_log_read_entries_for_polluted_empty_coverage(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: &LedgerRange,
        finality_level: Option<ManifestFinalityLevel>,
        entries: &mut Vec<ManifestEntry>,
    ) -> Result<(), DatalensError> {
        if *dataset_key != DatasetKey::evm_logs()
            || !matches!(selector, DatasetSelector::EvmLogs(_))
        {
            return Ok(());
        }
        if !should_repair_polluted_empty_evm_log_coverage(selector) {
            return Ok(());
        }

        let candidates = selector_coverage_candidates(entries, chain, dataset_key, selector, range);
        if !candidates
            .iter()
            .any(|candidate| candidate.entry.object_key.is_none())
        {
            return Ok(());
        }

        let existing_keys = entries
            .iter()
            .map(manifest_entry_read_key)
            .collect::<BTreeSet<_>>();
        let selector_fingerprint = selector.fingerprint();
        let selector_canonical_key = selector.canonical_key();
        let mut repaired_entries = coverage_index::read_compatible_evm_log_data_entries_for_query(
            &self.object_store,
            chain,
            dataset_key,
            selector,
            range,
        )?
        .into_iter()
        .filter(|entry| {
            finality_level
                .map(|finality_level| {
                    durable_finality_satisfies(entry.finality_level, finality_level)
                })
                .unwrap_or(true)
        })
        .filter(|entry| !existing_keys.contains(&manifest_entry_read_key(entry)))
        .map(|mut entry| {
            entry.selector_fingerprint = selector_fingerprint.clone();
            entry.selector_canonical_key = selector_canonical_key.clone();
            entry
        })
        .collect::<Vec<_>>();

        if repaired_entries.is_empty() {
            return Ok(());
        }
        log::warn!(
            "storage read augmented polluted empty evm log coverage chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} repaired_entries_count={}",
            chain.key_prefix(),
            dataset_key.as_str(),
            selector.fingerprint(),
            range_kind_key(range.kind()),
            range.start(),
            range.end(),
            repaired_entries.len()
        );
        entries.append(&mut repaired_entries);
        Ok(())
    }

    pub fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.write_rows_with_replacement(request, false)
    }

    pub fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.write_rows_with_replacement(request, true)
    }

    fn write_rows_with_replacement(
        &self,
        request: StorageWriteRequest<'_>,
        replace_existing: bool,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        let StorageWriteRequest {
            chain,
            dataset_key,
            selector,
            range,
            rows,
            finality_level,
            record_empty_coverage,
        } = request;

        if rows.dataset_key() != &dataset_key {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "dataset rows key does not match storage dataset key",
            ));
        }
        let finality_level = ManifestFinalityLevel::try_from(finality_level)?;
        if rows.row_count() == 0 && !record_empty_coverage {
            log::debug!(
                "storage skipped empty coverage dataset={} range={}-{}",
                dataset_key.as_str(),
                range.start(),
                range.end()
            );
            return Ok(StorageWriteOutcome {
                range,
                row_count: 0,
                data_object: None,
                recorded_empty_coverage: false,
            });
        }

        let selector_fingerprint = selector.fingerprint();
        let selector_canonical_key = selector.canonical_key();
        let (data_object, data_object_bytes) = if rows.row_count() == 0 {
            (None, None)
        } else {
            let encoding = object_encoding_for_dataset(&dataset_key);
            let object_key = object_key(
                chain,
                &dataset_key,
                range.clone(),
                &selector_fingerprint,
                encoding,
            );
            let bytes = encode_object_rows(encoding, rows, self.config.parquet_compression)?;
            let data_object = StorageDataObject {
                object_key,
                object_encoding: encoding,
                object_compression: object_compression_for_encoding(
                    encoding,
                    self.config.parquet_compression,
                ),
                row_count: rows.row_count(),
                object_size_bytes: bytes.len() as u64,
                checksum: checksum_hex(&bytes),
                checksum_algorithm: "sha256".to_owned(),
                written_at_unix_seconds: unix_seconds_now()?,
            };
            (Some(data_object), Some(bytes))
        };

        let entry = ManifestEntry {
            chain: chain.clone(),
            dataset_key: dataset_key.clone(),
            range,
            selector_fingerprint,
            selector_canonical_key,
            finality_level,
            object_key: data_object
                .as_ref()
                .map(|data_object| data_object.object_key.clone()),
            object_encoding: if rows.row_count() == 0 {
                None
            } else {
                Some(object_encoding_for_dataset(&dataset_key))
            },
            object_compression: data_object
                .as_ref()
                .and_then(|data_object| data_object.object_compression),
            row_count: rows.row_count(),
            object_size_bytes: data_object
                .as_ref()
                .map(|data_object| data_object.object_size_bytes),
            checksum: data_object
                .as_ref()
                .map(|data_object| data_object.checksum.clone()),
            checksum_algorithm: data_object
                .as_ref()
                .map(|data_object| data_object.checksum_algorithm.clone()),
            written_at_unix_seconds: data_object
                .as_ref()
                .map(|data_object| data_object.written_at_unix_seconds),
        };
        let log_context = StorageWriteLogContext::from_entry(&entry);
        let outcome_range = entry.range.clone();
        let row_count = rows.row_count();
        let recorded_empty_coverage = rows.row_count() == 0;
        log::info!(
            "storage wrote coverage chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} rows={} finality={} object_key_present={} coverage_kind={}",
            log_context.chain_key,
            log_context.dataset,
            log_context.selector_fingerprint,
            log_context.range_kind,
            log_context.start,
            log_context.end,
            log_context.row_count,
            log_context.finality,
            log_context.object_key_present,
            log_context.coverage_kind
        );
        let data_object = match (data_object, data_object_bytes) {
            (Some(data_object), Some(bytes)) => Some(self.write_data_object_manifest_entry(
                chain,
                entry,
                data_object,
                bytes,
                replace_existing,
            )?),
            _ => {
                if replace_existing {
                    let started = Instant::now();
                    let lock = self.manifest_update_lock(chain)?;
                    let _guard = self.lock_manifest_updates(&lock)?;
                    self.publish_replacement_entry_unlocked(chain, entry, started)?;
                } else {
                    self.write_manifest_entry(chain, entry)?;
                }
                None
            }
        };
        Ok(StorageWriteOutcome {
            range: outcome_range,
            row_count,
            data_object,
            recorded_empty_coverage,
        })
    }

    pub fn manifest_for_chain(&self, chain: &ChainIdentity) -> Result<Manifest, DatalensError> {
        let started = Instant::now();
        if let Some(manifest) = self.cached_manifest(chain)? {
            log::debug!(
                "storage manifest load source=manifest_cache chain_key={} entries_count={} duration_ms={}",
                chain.key_prefix(),
                manifest.entries.len(),
                started.elapsed().as_millis()
            );
            return Ok(manifest);
        }
        let mut manifest = Manifest::default();
        let mut base_entries = Vec::new();
        let key = manifest_key(chain);
        if self.object_store.exists(&key)? {
            let bytes = self.object_store.get(&key)?;
            let base_manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode manifest {key}: {error}"),
                )
            })?;
            base_entries = base_manifest.entries.clone();
            manifest.merge(base_manifest);
        }
        let mut segment_manifest = Manifest::default();
        let mut segment_objects_count = 0usize;
        for object in self.object_store.list(&manifest_segment_prefix(chain))? {
            if !is_manifest_segment_object(&object.key) {
                continue;
            }
            segment_objects_count += 1;
            let bytes = self.object_store.get(&object.key)?;
            let mut object_manifest: Manifest =
                serde_json::from_slice(&bytes).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!("decode manifest segment {}: {error}", object.key),
                    )
                })?;
            segment_manifest
                .entries
                .append(&mut object_manifest.entries);
        }
        let segment_entries_count = segment_manifest.entries.len();
        manifest.merge_filtering_shadowed_segments(segment_manifest, &base_entries);
        let entries_count = manifest.entries.len();
        self.cache_manifest(chain, manifest.clone())?;
        log::debug!(
            "storage manifest load source=legacy_manifest chain_key={} base_entries_count={} segment_objects_count={} segment_entries_count={} entries_count={} duration_ms={}",
            chain.key_prefix(),
            base_entries.len(),
            segment_objects_count,
            segment_entries_count,
            entries_count,
            started.elapsed().as_millis()
        );
        Ok(manifest)
    }

    pub fn manifest(&self) -> Result<Manifest, DatalensError> {
        let mut manifest = Manifest::default();
        let objects = self.object_store.list("chains")?;
        let mut base_entries = Vec::new();
        for object in objects
            .iter()
            .filter(|object| object.key.ends_with("/manifest.json"))
        {
            let bytes = self.object_store.get(&object.key)?;
            let chain_manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode manifest {}: {error}", object.key),
                )
            })?;
            base_entries.extend(chain_manifest.entries.clone());
            manifest.merge(chain_manifest);
        }
        let mut segment_manifest = Manifest::default();
        for object in objects
            .iter()
            .filter(|object| is_manifest_segment_object(&object.key))
        {
            let bytes = self.object_store.get(&object.key)?;
            let mut object_manifest: Manifest =
                serde_json::from_slice(&bytes).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!("decode manifest segment {}: {error}", object.key),
                    )
                })?;
            segment_manifest
                .entries
                .append(&mut object_manifest.entries);
        }
        manifest.merge_filtering_shadowed_segments(segment_manifest, &base_entries);
        Ok(manifest)
    }

    pub fn write_manifest(
        &self,
        chain: &ChainIdentity,
        manifest: &Manifest,
    ) -> Result<(), DatalensError> {
        let started = Instant::now();
        let lock = self.manifest_update_lock(chain)?;
        let _guard = self.lock_manifest_updates(&lock)?;
        self.write_manifest_unlocked(chain, manifest, started)
    }

    fn write_manifest_unlocked(
        &self,
        chain: &ChainIdentity,
        manifest: &Manifest,
        started: Instant,
    ) -> Result<(), DatalensError> {
        // Never publish a manifest that references a missing object; once
        // written, the manifest is what readers and planners trust.
        for entry in &manifest.entries {
            if let Some(object_key) = &entry.object_key
                && !self.object_store.exists(object_key)?
            {
                return Err(DatalensError::new(
                    DatalensErrorKind::ManifestUpdateFailure,
                    format!("manifest entry object not found {object_key}"),
                ));
            }
        }
        coverage_index::delete_chain(&self.object_store, chain)?;
        self.publish_manifest_unlocked(chain, manifest, "full", started)?;
        self.bump_manifest_version(chain)?;
        self.delete_manifest_segments_unlocked(chain)?;
        coverage_index::write_entries_with_legacy_index(
            &self.object_store,
            &manifest.entries,
            self.legacy_coverage_index_write_enabled(),
        )?;
        self.cache_manifest(chain, manifest.clone())?;
        Ok(())
    }

    fn publish_manifest_unlocked(
        &self,
        chain: &ChainIdentity,
        manifest: &Manifest,
        publish_kind: &'static str,
        started: Instant,
    ) -> Result<(), DatalensError> {
        let key = manifest_key(chain);
        let bytes = serde_json::to_vec_pretty(manifest).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode manifest: {error}"),
            )
        })?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write manifest {key}: {}", error.message),
            )
        })?;
        let entries_count = manifest.entries.len();
        let data_object_count = manifest
            .entries
            .iter()
            .filter(|entry| entry.object_key.is_some())
            .count();
        let empty_coverage_count = entries_count - data_object_count;
        log::info!(
            "storage published full manifest publish_kind={} chain_key={} entries_count={} data_object_count={} empty_coverage_count={} manifest_bytes={} duration_ms={}",
            publish_kind,
            chain.key_prefix(),
            entries_count,
            data_object_count,
            empty_coverage_count,
            bytes.len(),
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn delete_manifest_segments_unlocked(
        &self,
        chain: &ChainIdentity,
    ) -> Result<(), DatalensError> {
        for object in self.object_store.list(&manifest_segment_prefix(chain))? {
            if !is_manifest_segment_object(&object.key) {
                continue;
            }
            self.object_store.delete(&object.key).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ManifestUpdateFailure,
                    format!("delete manifest segment {}: {}", object.key, error.message),
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn write_manifest_entry(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
    ) -> Result<(), DatalensError> {
        let started = Instant::now();
        let lock = self.manifest_update_lock(chain)?;
        let _guard = self.lock_manifest_updates(&lock)?;
        if let Some(object_key) = entry.object_key.as_deref()
            && !self.object_store.exists(object_key)?
        {
            return Err(DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("manifest entry object not found {object_key}"),
            ));
        }
        if entry.object_key.is_none()
            && let Some(coalesced_entry) = self.coalesced_empty_coverage_entry(&entry)?
        {
            return self.publish_replacement_entry_unlocked(chain, coalesced_entry, started);
        }
        self.publish_manifest_segment_unlocked(chain, entry, started)
    }

    fn coalesced_empty_coverage_entry(
        &self,
        entry: &ManifestEntry,
    ) -> Result<Option<ManifestEntry>, DatalensError> {
        let probe_start = entry.range.start().saturating_sub(1);
        let probe_end = entry.range.end().saturating_add(1);
        let mut probe = entry.clone();
        probe.range = LedgerRange::try_new(entry.range.kind(), probe_start, probe_end)?;
        let mut coalesced = entry.clone();
        let mut found_neighbor = false;
        for existing in
            coverage_index::read_entries_for_replacement_scope(&self.object_store, &probe)?
        {
            if can_merge_empty_coverage(&coalesced, &existing) {
                coalesced.range = LedgerRange::try_new(
                    coalesced.range.kind(),
                    coalesced.range.start().min(existing.range.start()),
                    coalesced.range.end().max(existing.range.end()),
                )?;
                found_neighbor = true;
            }
        }
        if !found_neighbor {
            return Ok(None);
        }
        let current_entries =
            coverage_index::read_entries_for_replacement_scope(&self.object_store, &coalesced)?;
        if current_entries.iter().any(|existing| {
            replacement_scope_matches(existing, &coalesced)
                && existing.range.intersection(&coalesced.range).is_some()
                && existing.object_key.is_some()
        }) {
            return Ok(None);
        }
        Ok(Some(coalesced))
    }

    pub(crate) fn try_write_compaction_manifest_entry(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
        source_entries: &[ManifestEntry],
        validate_coverage_index_sources: bool,
        checkpoint: &dyn Fn() -> Result<(), DatalensError>,
    ) -> Result<bool, DatalensError> {
        let started = Instant::now();
        let lock = self.manifest_update_lock(chain)?;
        let Some(_guard) = self.try_lock_manifest_updates(&lock)? else {
            log::info!(
                "storage compaction manifest publish skipped reason=manifest_lock_busy chain_key={} range={}-{}",
                chain.key_prefix(),
                entry.range.start(),
                entry.range.end()
            );
            return Ok(false);
        };
        if let Some(object_key) = entry.object_key.as_deref()
            && !self.object_store.exists(object_key)?
        {
            return Err(DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("manifest entry object not found {object_key}"),
            ));
        }

        let current_entries =
            self.current_compaction_source_entries(chain, &entry, source_entries)?;
        if !compaction_source_entries_still_current(&current_entries, source_entries) {
            log::info!(
                "storage compaction manifest publish skipped reason=stale_manifest_sources chain_key={} range={}-{} source_entries_count={} current_entries_count={}",
                chain.key_prefix(),
                entry.range.start(),
                entry.range.end(),
                source_entries.len(),
                current_entries.len()
            );
            return Ok(false);
        }

        checkpoint()?;
        let legacy_coverage_index_write_enabled = self.legacy_coverage_index_write_enabled();
        let replacement = if legacy_coverage_index_write_enabled || validate_coverage_index_sources
        {
            let index_entries =
                coverage_index::read_entries_for_replacement_scope(&self.object_store, &entry)?;
            if !index_entries.is_empty()
                && !compaction_source_entries_still_current(&index_entries, source_entries)
            {
                log::info!(
                    "storage compaction manifest publish skipped reason=stale_coverage_index_sources chain_key={} range={}-{} source_entries_count={} index_entries_count={}",
                    chain.key_prefix(),
                    entry.range.start(),
                    entry.range.end(),
                    source_entries.len(),
                    index_entries.len()
                );
                return Ok(false);
            }
            if legacy_coverage_index_write_enabled {
                None
            } else {
                Some(coverage_index::replacement_from_replaced_entries(
                    index_entries,
                    &entry,
                )?)
            }
        } else {
            Some(coverage_index::replacement_from_replaced_entries(
                current_entries,
                &entry,
            )?)
        };
        self.publish_replacement_entry_unlocked_with_checkpoint(
            chain,
            entry,
            started,
            checkpoint,
            replacement,
        )?;
        Ok(true)
    }

    fn current_compaction_source_entries(
        &self,
        chain: &ChainIdentity,
        entry: &ManifestEntry,
        source_entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestEntry>, DatalensError> {
        let mut entries = Vec::new();
        for source_entry in source_entries {
            let key = manifest_segment_key(chain, source_entry);
            if !self.object_store.exists(&key)? {
                continue;
            }
            let bytes = self.object_store.get(&key)?;
            let segment: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode manifest segment {key}: {error}"),
                )
            })?;
            entries.extend(segment.entries.into_iter().filter(|current_entry| {
                replacement_scope_matches(current_entry, entry)
                    && current_entry.range.intersection(&entry.range).is_some()
            }));
        }
        let key = manifest_key(chain);
        if self.object_store.exists(&key)? {
            let bytes = self.object_store.get(&key)?;
            let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode manifest {key}: {error}"),
                )
            })?;
            entries.extend(manifest.entries.into_iter().filter(|current_entry| {
                replacement_scope_matches(current_entry, entry)
                    && current_entry.range.intersection(&entry.range).is_some()
            }));
        }
        let mut manifest = Manifest { entries };
        manifest.normalize();
        Ok(manifest.entries)
    }

    fn write_data_object_manifest_entry(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
        mut data_object: StorageDataObject,
        bytes: Vec<u8>,
        replace_existing: bool,
    ) -> Result<StorageDataObject, DatalensError> {
        let started = Instant::now();
        let lock = self.manifest_update_lock(chain)?;
        let _guard = self.lock_manifest_updates(&lock)?;

        if !replace_existing
            && let Some(existing) = self.exact_manifest_segment_entry(chain, &entry)?
        {
            validate_existing_data_object(&existing, &data_object)?;
            if existing.object_size_bytes.is_some()
                && existing.checksum.is_some()
                && existing.checksum_algorithm.is_some()
                && let Some(written_at_unix_seconds) = existing.written_at_unix_seconds
                && self.object_store.exists(&data_object.object_key)?
            {
                data_object.written_at_unix_seconds = written_at_unix_seconds;
                return Ok(data_object);
            }
        }

        if replace_existing || !self.existing_data_object_matches(&data_object)? {
            self.object_store.put(&data_object.object_key, &bytes)?;
        }
        if !self.object_store.exists(&data_object.object_key)? {
            return Err(DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("manifest entry object not found {}", data_object.object_key),
            ));
        }
        if replace_existing {
            self.publish_replacement_entry_unlocked(chain, entry, started)?;
        } else {
            self.publish_manifest_segment_unlocked(chain, entry, started)?;
        }
        Ok(data_object)
    }

    fn exact_manifest_segment_entry(
        &self,
        chain: &ChainIdentity,
        entry: &ManifestEntry,
    ) -> Result<Option<ManifestEntry>, DatalensError> {
        let key = manifest_segment_key(chain, entry);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        let bytes = self.object_store.get(&key)?;
        let segment: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode manifest segment {key}: {error}"),
            )
        })?;
        Ok(segment
            .find_logical(
                chain,
                &entry.dataset_key,
                &entry.selector_fingerprint,
                &entry.range,
                entry.finality_level,
            )
            .cloned())
    }

    fn existing_data_object_matches(
        &self,
        data_object: &StorageDataObject,
    ) -> Result<bool, DatalensError> {
        if !self.object_store.exists(&data_object.object_key)? {
            return Ok(false);
        }
        let existing_bytes = self.object_store.get(&data_object.object_key)?;
        if existing_bytes.len() as u64 != data_object.object_size_bytes
            || checksum_hex(&existing_bytes) != data_object.checksum
        {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "existing data object bytes differ for logical shard",
            ));
        }
        Ok(true)
    }

    fn publish_manifest_segment_unlocked(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
        started: Instant,
    ) -> Result<(), DatalensError> {
        let (key, bytes_len) = self.put_manifest_segment_object(chain, &entry)?;
        compaction_queue::write_entry(&self.object_store, chain, &entry)?;
        self.bump_manifest_version(chain)?;
        coverage_index::write_entry_with_legacy_index(
            &self.object_store,
            &entry,
            self.legacy_coverage_index_write_enabled(),
        )?;
        log::info!(
            "storage published manifest segment chain_key={} range={}-{} selector_fingerprint={} segment_key={} segment_bytes={} duration_ms={}",
            chain.key_prefix(),
            entry.range.start(),
            entry.range.end(),
            entry.selector_fingerprint,
            key,
            bytes_len,
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn put_manifest_segment_object(
        &self,
        chain: &ChainIdentity,
        entry: &ManifestEntry,
    ) -> Result<(String, usize), DatalensError> {
        let key = manifest_segment_key(chain, entry);
        let segment = Manifest {
            entries: vec![entry.clone()],
        };
        let bytes = serde_json::to_vec_pretty(&segment).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode manifest segment: {error}"),
            )
        })?;
        let bytes_len = bytes.len();
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write manifest segment {key}: {}", error.message),
            )
        })?;
        Ok((key, bytes_len))
    }

    fn publish_replacement_entry_unlocked(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
        started: Instant,
    ) -> Result<(), DatalensError> {
        self.publish_replacement_entry_unlocked_with_checkpoint(
            chain,
            entry,
            started,
            &|| Ok(()),
            None,
        )
    }

    fn publish_replacement_entry_unlocked_with_checkpoint(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
        started: Instant,
        checkpoint: &dyn Fn() -> Result<(), DatalensError>,
        replacement: Option<coverage_index::CoverageIndexReplacement>,
    ) -> Result<(), DatalensError> {
        checkpoint()?;
        let legacy_coverage_index_write_enabled = self.legacy_coverage_index_write_enabled();
        let update = match replacement {
            Some(replacement) => replacement,
            None => coverage_index::replace_entry_with_legacy_index(
                &self.object_store,
                &entry,
                legacy_coverage_index_write_enabled,
            )?,
        };
        let mut published_update = if legacy_coverage_index_write_enabled {
            None
        } else {
            checkpoint()?;
            Some(coverage_index::publish_replacement_with_legacy_index(
                &self.object_store,
                &entry,
                &update,
                false,
            )?)
        };
        let mut published_segment_keys = BTreeSet::new();
        let mut published_segment_count = 0usize;
        let mut published_segment_bytes = 0usize;
        let published_entries = published_update
            .as_ref()
            .map(|update| update.published_entries.as_slice())
            .unwrap_or(update.published_entries.as_slice());
        for published_entry in published_entries {
            checkpoint()?;
            let (key, bytes_len) = self.put_manifest_segment_object(chain, published_entry)?;
            published_segment_keys.insert(key);
            published_segment_count += 1;
            published_segment_bytes += bytes_len;
            compaction_queue::write_entry(&self.object_store, chain, published_entry)?;
        }
        let published_update = if let Some(published_update) = published_update.take() {
            published_update
        } else {
            checkpoint()?;
            coverage_index::publish_replacement_with_legacy_index(
                &self.object_store,
                &entry,
                &update,
                true,
            )?
        };
        let replaced_entries_count = published_update.replaced_entries.len();
        let mut old_segment_keys = BTreeSet::new();
        for replaced_entry in &published_update.replaced_entries {
            old_segment_keys.insert(manifest_segment_key(chain, replaced_entry));
        }
        for published_entry in &published_update.published_entries {
            let key = manifest_segment_key(chain, published_entry);
            if !published_segment_keys.contains(&key) {
                checkpoint()?;
                let (key, bytes_len) = self.put_manifest_segment_object(chain, published_entry)?;
                published_segment_keys.insert(key);
                published_segment_count += 1;
                published_segment_bytes += bytes_len;
                compaction_queue::write_entry(&self.object_store, chain, published_entry)?;
            }
        }
        let mut deleted_segment_count = 0usize;
        for key in old_segment_keys {
            if !published_segment_keys.contains(&key) {
                checkpoint()?;
                self.object_store.delete(&key).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::ManifestUpdateFailure,
                        format!("delete manifest segment {key}: {}", error.message),
                    )
                })?;
                deleted_segment_count += 1;
            }
        }
        checkpoint()?;
        let replaced_base_entries_count =
            self.rewrite_base_manifest_for_replacement_unlocked(chain, &entry)?;
        checkpoint()?;
        self.bump_manifest_version(chain)?;
        log::info!(
            "storage published replacement coverage chain_key={} dataset={} selector_fingerprint={} range_kind={} range={}-{} finality={} replaced_entries_count={} replaced_base_entries_count={} deleted_manifest_segments_count={} published_manifest_segments_count={} published_manifest_segment_bytes={} duration_ms={}",
            chain.key_prefix(),
            entry.dataset_key.as_str(),
            entry.selector_fingerprint,
            range_kind_key(entry.range.kind()),
            entry.range.start(),
            entry.range.end(),
            entry.finality_level.as_str(),
            replaced_entries_count,
            replaced_base_entries_count,
            deleted_segment_count,
            published_segment_count,
            published_segment_bytes,
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn rewrite_base_manifest_for_replacement_unlocked(
        &self,
        chain: &ChainIdentity,
        entry: &ManifestEntry,
    ) -> Result<usize, DatalensError> {
        let key = manifest_key(chain);
        if !self.object_store.exists(&key)? {
            return Ok(0);
        }
        let bytes = self.object_store.get(&key)?;
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode manifest {key}: {error}"),
            )
        })?;
        let mut entries = Vec::new();
        let mut replaced_entries_count = 0usize;
        for existing_entry in manifest.entries {
            if replacement_scope_matches(&existing_entry, entry)
                && existing_entry.range.intersection(&entry.range).is_some()
            {
                replaced_entries_count += 1;
                entries.extend(split_entry_around_range(existing_entry, &entry.range)?);
            } else {
                entries.push(existing_entry);
            }
        }
        if replaced_entries_count == 0 {
            return Ok(0);
        }
        let manifest = Manifest { entries };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode manifest: {error}"),
            )
        })?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write manifest {key}: {}", error.message),
            )
        })?;
        Ok(replaced_entries_count)
    }

    pub(crate) fn cached_manifest(
        &self,
        chain: &ChainIdentity,
    ) -> Result<Option<Manifest>, DatalensError> {
        let chain_key = chain.key_prefix();
        let current_version = self.manifest_version(chain)?;
        let cache = self
            .manifest_cache
            .lock()
            .map_err(|_| DatalensError::internal("manifest cache lock poisoned"))?;
        Ok(cache
            .get(&chain_key)
            .filter(|entry| entry.version == current_version)
            .map(|entry| entry.manifest.clone()))
    }

    fn cache_manifest(
        &self,
        chain: &ChainIdentity,
        manifest: Manifest,
    ) -> Result<(), DatalensError> {
        let version = self.manifest_version(chain)?;
        let chain_key = chain.key_prefix();
        let mut cache = self
            .manifest_cache
            .lock()
            .map_err(|_| DatalensError::internal("manifest cache lock poisoned"))?;
        cache.insert(chain_key, ManifestCacheEntry { manifest, version });
        let cache_chain_count = cache.len();
        let cache_entries_count: usize = cache
            .values()
            .map(|entry| entry.manifest.entries.len())
            .sum();
        log::debug!(
            "storage manifest cache updated chain_key={} cache_chain_count={} cache_entries_count={}",
            chain.key_prefix(),
            cache_chain_count,
            cache_entries_count
        );
        Ok(())
    }

    fn bump_manifest_version(&self, chain: &ChainIdentity) -> Result<(), DatalensError> {
        let key = manifest_version_key(chain);
        let value = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("system clock before unix epoch: {error}"),
                )
            })?
            .as_nanos()
            .to_string();
        self.object_store
            .put(&key, value.as_bytes())
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ManifestUpdateFailure,
                    format!("write manifest version {key}: {}", error.message),
                )
            })
    }

    fn manifest_version(&self, chain: &ChainIdentity) -> Result<Option<Vec<u8>>, DatalensError> {
        let key = manifest_version_key(chain);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        self.object_store.get(&key).map(Some)
    }

    fn manifest_update_lock(&self, chain: &ChainIdentity) -> Result<Arc<Mutex<()>>, DatalensError> {
        let chain_key = chain.key_prefix();
        let mut locks = self
            .manifest_update_locks
            .lock()
            .map_err(|_| DatalensError::internal("manifest update lock poisoned"))?;
        Ok(locks
            .entry(chain_key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }

    fn lock_manifest_updates<'a>(
        &self,
        lock: &'a Mutex<()>,
    ) -> Result<MutexGuard<'a, ()>, DatalensError> {
        lock.lock()
            .map_err(|_| DatalensError::internal("manifest update lock poisoned"))
    }

    fn try_lock_manifest_updates<'a>(
        &self,
        lock: &'a Mutex<()>,
    ) -> Result<Option<MutexGuard<'a, ()>>, DatalensError> {
        match lock.try_lock() {
            Ok(guard) => Ok(Some(guard)),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                Err(DatalensError::internal("manifest update lock poisoned"))
            }
        }
    }
}

fn is_manifest_segment_object(object_key: &str) -> bool {
    object_key.contains("/manifest-segments/")
        && object_key.ends_with(".json")
        && !object_key.contains("/_metadata/")
}

fn compaction_source_entries_still_current(
    current_entries: &[ManifestEntry],
    source_entries: &[ManifestEntry],
) -> bool {
    source_entries
        .iter()
        .all(|source| current_entries.iter().any(|current| current == source))
}

pub trait StorageRepository: Send + Sync {
    fn manifest(&self) -> Result<Manifest, DatalensError>;

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError>;

    fn coverage_plan(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<StorageCoveragePlan, DatalensError> {
        let covered_ranges = self.covered_ranges(chain, dataset_key, selector, range.clone())?;
        Ok(StorageCoveragePlan::from_covered_ranges(
            chain,
            dataset_key,
            selector,
            range,
            covered_ranges,
        ))
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError>;

    fn read_rows_with_coverage_plan(
        &self,
        coverage_plan: &StorageCoveragePlan,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        let _ = coverage_plan;
        self.read_rows(chain, dataset_key, selector, range)
    }

    fn read_rows_for_finality(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: FinalityLevel,
    ) -> Result<DatasetRows, DatalensError> {
        let _ = finality_level;
        self.read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError>;

    fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        let _ = request;
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "replacement write is not supported by this storage repository",
        ))
    }
}

impl<S> StorageRepository for DurableStorage<S>
where
    S: ObjectStore + 'static,
{
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        Self::manifest(self)
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        Self::covered_ranges(self, chain, dataset_key, selector, range)
    }

    fn coverage_plan(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<StorageCoveragePlan, DatalensError> {
        Self::coverage_plan(self, chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Self::read_rows(self, chain, dataset_key, selector, range)
    }

    fn read_rows_with_coverage_plan(
        &self,
        coverage_plan: &StorageCoveragePlan,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Self::read_rows_with_coverage_plan(self, coverage_plan, chain, dataset_key, selector, range)
    }

    fn read_rows_for_finality(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: FinalityLevel,
    ) -> Result<DatasetRows, DatalensError> {
        Self::read_rows_for_finality(self, chain, dataset_key, selector, range, finality_level)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Self::write_rows(self, request)
    }

    fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Self::write_rows_replacing_existing(self, request)
    }
}

impl StorageRepository for Box<dyn StorageRepository> {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        self.as_ref().manifest()
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        self.as_ref()
            .covered_ranges(chain, dataset_key, selector, range)
    }

    fn coverage_plan(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<StorageCoveragePlan, DatalensError> {
        self.as_ref()
            .coverage_plan(chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows(chain, dataset_key, selector, range)
    }

    fn read_rows_with_coverage_plan(
        &self,
        coverage_plan: &StorageCoveragePlan,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows_with_coverage_plan(
            coverage_plan,
            chain,
            dataset_key,
            selector,
            range,
        )
    }

    fn read_rows_for_finality(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: FinalityLevel,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref()
            .read_rows_for_finality(chain, dataset_key, selector, range, finality_level)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows(request)
    }

    fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows_replacing_existing(request)
    }
}

impl StorageRepository for Arc<dyn StorageRepository> {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        self.as_ref().manifest()
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        self.as_ref()
            .covered_ranges(chain, dataset_key, selector, range)
    }

    fn coverage_plan(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<StorageCoveragePlan, DatalensError> {
        self.as_ref()
            .coverage_plan(chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows(chain, dataset_key, selector, range)
    }

    fn read_rows_with_coverage_plan(
        &self,
        coverage_plan: &StorageCoveragePlan,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows_with_coverage_plan(
            coverage_plan,
            chain,
            dataset_key,
            selector,
            range,
        )
    }

    fn read_rows_for_finality(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality_level: FinalityLevel,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref()
            .read_rows_for_finality(chain, dataset_key, selector, range, finality_level)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows(request)
    }

    fn write_rows_replacing_existing(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows_replacing_existing(request)
    }
}

impl UsageLedgerRepository for Arc<dyn UsageLedgerRepository> {
    fn append(&self, entry: &UsageLedgerEntry) -> Result<(), DatalensError> {
        self.as_ref().append(entry)
    }

    fn read_application(
        &self,
        application_id: &str,
    ) -> Result<Vec<UsageLedgerEntry>, DatalensError> {
        self.as_ref().read_application(application_id)
    }
}

impl UsageLedgerRepository for Box<dyn UsageLedgerRepository> {
    fn append(&self, entry: &UsageLedgerEntry) -> Result<(), DatalensError> {
        self.as_ref().append(entry)
    }

    fn read_application(
        &self,
        application_id: &str,
    ) -> Result<Vec<UsageLedgerEntry>, DatalensError> {
        self.as_ref().read_application(application_id)
    }
}

impl QueryWatermarkRepository for Arc<dyn QueryWatermarkRepository> {
    fn update(&self, watermark: &QueryWatermark) -> Result<(), DatalensError> {
        self.as_ref().update(watermark)
    }

    fn read(&self, key: &QueryWatermarkKey) -> Result<Option<QueryWatermark>, DatalensError> {
        self.as_ref().read(key)
    }
}

impl QueryWatermarkRepository for Box<dyn QueryWatermarkRepository> {
    fn update(&self, watermark: &QueryWatermark) -> Result<(), DatalensError> {
        self.as_ref().update(watermark)
    }

    fn read(&self, key: &QueryWatermarkKey) -> Result<Option<QueryWatermark>, DatalensError> {
        self.as_ref().read(key)
    }
}

impl QueryActivityRepository for Arc<dyn QueryActivityRepository> {
    fn update(&self, activity: &QueryActivity) -> Result<(), DatalensError> {
        self.as_ref().update(activity)
    }

    fn read(&self, key: &QueryActivityKey) -> Result<Option<QueryActivity>, DatalensError> {
        self.as_ref().read(key)
    }
}

impl QueryActivityRepository for Box<dyn QueryActivityRepository> {
    fn update(&self, activity: &QueryActivity) -> Result<(), DatalensError> {
        self.as_ref().update(activity)
    }

    fn read(&self, key: &QueryActivityKey) -> Result<Option<QueryActivity>, DatalensError> {
        self.as_ref().read(key)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectEncoding {
    Json,
    ParquetV1,
}

impl ObjectEncoding {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::ParquetV1 => "parquet-v1",
        }
    }

    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Json => ".json",
            Self::ParquetV1 => ".parquet",
        }
    }

    pub(crate) fn validate_object_key(self, object_key: &str) -> Result<(), DatalensError> {
        let schema_segment = format!("/{}/", self.as_str());
        if !object_key.contains(&schema_segment) || !object_key.ends_with(self.extension()) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!(
                    "manifest object encoding {} does not match object key {object_key}",
                    self.as_str()
                ),
            ));
        }
        Ok(())
    }
}
