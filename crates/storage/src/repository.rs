use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::read_through_cache;
use crate::selector_coverage::{filter_evm_log_rows_for_selector, selector_coverage_candidates};

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
    manifest_key, merge_ranges, object_encoding_for_dataset, object_key, range_kind_key,
    unix_seconds_now, validate_existing_data_object, verify_manifest_object_metadata,
};
pub use crate::hot_cache::{
    HOT_CACHE_SCHEMA_VERSION, HotBlockMetadata, HotCache, HotCacheCandidateStatus,
    HotCacheCleanupReport, HotCacheConfig, HotCacheEntryMetadata, HotCacheFinalityStatus,
    HotCacheReadOutcome, HotCacheRetentionPolicy, HotCacheStorage, HotCacheWriteOutcome,
    HotCacheWriteRequest, HotEntryStatus, HotManifest, HotManifestEntry, HotReorgOutcome,
    HotReorgReason, HotWriteOutcome, HotWriteRequest, LocalHotCacheStorage,
};
pub use crate::maintenance::{
    CompactionCandidate, MaintenanceCheckReport, MaintenanceCompactionConfig,
    MaintenanceCompactionReport, MaintenanceIssue, MaintenanceIssueKind, MaintenanceOperation,
    MaintenanceOperationMode, MaintenanceReport, MaintenanceRetentionReport,
    MaintenanceUsageLedgerReport, RetentionPolicy, UsageLedgerRollupModel,
};
pub use crate::manifest::{Manifest, ManifestEntry, ManifestFinalityLevel};
pub use crate::object_store::{
    LocalObjectStore, ObjectMetadata, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
    validate_object_key,
};
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
    manifest_update_lock: Arc<Mutex<()>>,
    config: DurableStorageConfig,
}

pub type LocalStorage = DurableStorage<LocalObjectStore>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStorageConfig {
    #[serde(default)]
    pub parquet_compression: ParquetCompression,
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
            manifest_update_lock: Arc::new(Mutex::new(())),
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
            manifest_update_lock: Arc::new(Mutex::new(())),
            config,
        }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    pub fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        // Manifest entries are the sole coverage authority. Empty coverage and
        // data-object coverage both count here because both were provider
        // confirmed before entering the manifest.
        let manifest = self.manifest_for_chain(chain)?;
        let mut ranges =
            selector_coverage_candidates(&manifest.entries, chain, dataset_key, selector, &range)
                .into_iter()
                .flat_map(|candidate| candidate.ranges)
                .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.start());
        Ok(merge_ranges(ranges))
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        log::debug!(
            "storage read dataset={} range={}-{}",
            dataset_key.as_str(),
            range.start(),
            range.end()
        );
        let manifest = self.manifest_for_chain(chain)?;
        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        for candidate in
            selector_coverage_candidates(&manifest.entries, chain, dataset_key, selector, &range)
        {
            let entry = candidate.entry;
            let Some(object_key) = entry.object_key.as_deref() else {
                continue;
            };
            if !self.object_store.exists(object_key)? {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("manifest entry object not found {object_key}"),
                ));
            }
            let Some(encoding) = entry.object_encoding else {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("manifest entry object {object_key} missing object_encoding"),
                ));
            };
            let object_rows =
                if let Some(rows) = self.read_through_cache.get(object_key, entry, encoding) {
                    rows
                } else {
                    let bytes = self.object_store.get(object_key)?;
                    verify_manifest_object_metadata(entry, object_key, &bytes)?;
                    let object_rows = decode_object_rows(encoding, dataset_key.clone(), &bytes)
                        .map_err(|error| {
                            DatalensError::new(
                                DatalensErrorKind::StorageReadFailure,
                                format!("decode cached object {object_key}: {}", error.message),
                            )
                        })?;
                    self.read_through_cache
                        .put(object_key, entry, encoding, object_rows.clone());
                    object_rows
                };
            for candidate_range in candidate.ranges {
                let mut candidate_rows = filter_rows(object_rows.clone(), candidate_range);
                candidate_rows = filter_evm_log_rows_for_selector(candidate_rows, selector);
                rows.try_append(candidate_rows.into_rows())?;
            }
        }
        rows.sort();
        DatasetRows::new(dataset_key.clone(), rows)
    }

    pub fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
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
        let manifest = self.manifest_for_chain(chain)?;
        let data_object = if rows.row_count() == 0 {
            None
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
            if let Some(existing) = manifest.find_logical(
                chain,
                &dataset_key,
                &selector_fingerprint,
                &range,
                finality_level,
            ) {
                // Object keys are deterministic for a logical coverage segment;
                // an existing valid object is reused instead of rewriting bytes.
                validate_existing_data_object(existing, &data_object)?;
                if existing.object_size_bytes.is_some()
                    && existing.checksum.is_some()
                    && existing.checksum_algorithm.is_some()
                    && let Some(written_at_unix_seconds) = existing.written_at_unix_seconds
                    && self.object_store.exists(&data_object.object_key)?
                {
                    return Ok(StorageWriteOutcome {
                        range,
                        row_count: rows.row_count(),
                        data_object: Some(StorageDataObject {
                            written_at_unix_seconds,
                            ..data_object
                        }),
                        recorded_empty_coverage: false,
                    });
                }
            }
            let latest_manifest = self.manifest_for_chain(chain)?;
            if let Some(existing) = latest_manifest.find_logical(
                chain,
                &dataset_key,
                &selector_fingerprint,
                &range,
                finality_level,
            ) {
                // A concurrent writer may have published this logical coverage
                // after this write started. Reuse it before uploading bytes for
                // the deterministic object key.
                validate_existing_data_object(existing, &data_object)?;
                if existing.object_size_bytes.is_some()
                    && existing.checksum.is_some()
                    && existing.checksum_algorithm.is_some()
                    && let Some(written_at_unix_seconds) = existing.written_at_unix_seconds
                    && self.object_store.exists(&data_object.object_key)?
                {
                    return Ok(StorageWriteOutcome {
                        range,
                        row_count: rows.row_count(),
                        data_object: Some(StorageDataObject {
                            written_at_unix_seconds,
                            ..data_object
                        }),
                        recorded_empty_coverage: false,
                    });
                }
            }
            let object_key = data_object.object_key.clone();
            self.object_store.put(&object_key, &bytes)?;
            Some(data_object)
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
        let outcome = StorageWriteOutcome {
            range: entry.range.clone(),
            row_count: rows.row_count(),
            data_object,
            recorded_empty_coverage: rows.row_count() == 0,
        };
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
        self.write_manifest_entry(chain, entry)?;
        Ok(outcome)
    }

    pub(crate) fn manifest_for_chain(
        &self,
        chain: &ChainIdentity,
    ) -> Result<Manifest, DatalensError> {
        let key = manifest_key(chain);
        if !self.object_store.exists(&key)? {
            return Ok(Manifest::default());
        }
        let bytes = self.object_store.get(&key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode manifest {key}: {error}"),
            )
        })
    }

    pub fn manifest(&self) -> Result<Manifest, DatalensError> {
        let mut manifest = Manifest::default();
        for object in self.object_store.list("chains")? {
            if object.key.ends_with("/manifest.json") {
                let bytes = self.object_store.get(&object.key)?;
                let mut chain_manifest: Manifest =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            format!("decode manifest {}: {error}", object.key),
                        )
                    })?;
                manifest.entries.append(&mut chain_manifest.entries);
            }
        }
        Ok(manifest)
    }

    pub(crate) fn write_manifest(
        &self,
        chain: &ChainIdentity,
        manifest: &Manifest,
    ) -> Result<(), DatalensError> {
        let _guard = self.lock_manifest_updates()?;
        self.write_manifest_unlocked(chain, manifest)
    }

    fn write_manifest_unlocked(
        &self,
        chain: &ChainIdentity,
        manifest: &Manifest,
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
        })
    }

    fn write_manifest_entry(
        &self,
        chain: &ChainIdentity,
        entry: ManifestEntry,
    ) -> Result<(), DatalensError> {
        let _guard = self.lock_manifest_updates()?;
        let mut manifest = self.manifest_for_chain(chain)?;
        manifest.upsert(entry);
        self.write_manifest_unlocked(chain, &manifest)
    }

    fn lock_manifest_updates(&self) -> Result<MutexGuard<'_, ()>, DatalensError> {
        self.manifest_update_lock
            .lock()
            .map_err(|_| DatalensError::internal("manifest update lock poisoned"))
    }
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

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError>;

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError>;
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

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Self::read_rows(self, chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Self::write_rows(self, request)
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

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows(request)
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

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.as_ref().read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        self.as_ref().write_rows(request)
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
