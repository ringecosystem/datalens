use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::read_through_cache;

#[derive(Debug)]
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
pub struct DurableStorage<S> {
    object_store: S,
    read_through_cache: read_through_cache::ReadThroughCache,
}

pub type LocalStorage = DurableStorage<LocalObjectStore>;

impl DurableStorage<LocalObjectStore> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            object_store: LocalObjectStore::new(root),
            read_through_cache: read_through_cache::ReadThroughCache::new(
                ReadThroughCacheConfig::default(),
            ),
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
        Self::from_object_store_with_read_through_cache_config(
            object_store,
            ReadThroughCacheConfig::default(),
        )
    }

    pub fn from_object_store_with_read_through_cache_config(
        object_store: S,
        read_through_cache_config: ReadThroughCacheConfig,
    ) -> Self {
        Self {
            object_store,
            read_through_cache: read_through_cache::ReadThroughCache::new(
                read_through_cache_config,
            ),
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
        let selector_fingerprint = selector.fingerprint();
        let mut ranges = self
            .manifest_for_chain(chain)?
            .entries
            .into_iter()
            .filter(|entry| {
                entry.chain == *chain
                    && entry.dataset_key == *dataset_key
                    && entry.range.kind() == range.kind()
                    && entry.selector_fingerprint == selector_fingerprint
            })
            .filter_map(|entry| intersect(entry.range, range.clone()))
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
        let selector_fingerprint = selector.fingerprint();
        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        for entry in self.manifest_for_chain(chain)?.entries {
            if entry.chain != *chain
                || entry.dataset_key != *dataset_key
                || entry.range.kind() != range.kind()
                || entry.selector_fingerprint != selector_fingerprint
            {
                continue;
            }
            if intersect(entry.range.clone(), range.clone()).is_none() {
                continue;
            }
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
            let mut object_rows =
                if let Some(rows) = self.read_through_cache.get(object_key, &entry, encoding) {
                    rows
                } else {
                    let bytes = self.object_store.get(object_key)?;
                    verify_manifest_object_metadata(&entry, object_key, &bytes)?;
                    let object_rows = decode_object_rows(encoding, dataset_key.clone(), &bytes)
                        .map_err(|error| {
                            DatalensError::new(
                                DatalensErrorKind::StorageReadFailure,
                                format!("decode cached object {object_key}: {}", error.message),
                            )
                        })?;
                    self.read_through_cache
                        .put(object_key, &entry, encoding, object_rows.clone());
                    object_rows
                };
            object_rows = filter_rows(object_rows, range.clone());
            rows.try_append(object_rows.into_rows())?;
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
        let mut manifest = self.manifest_for_chain(chain)?;
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
            let bytes = encode_object_rows(encoding, rows)?;
            let data_object = StorageDataObject {
                object_key,
                object_encoding: encoding,
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
        let outcome = StorageWriteOutcome {
            range: entry.range.clone(),
            row_count: rows.row_count(),
            data_object,
            recorded_empty_coverage: rows.row_count() == 0,
        };
        manifest.upsert(entry);
        log::info!(
            "storage wrote coverage dataset={} range={}-{} rows={}",
            dataset_key.as_str(),
            manifest
                .entries
                .last()
                .expect("manifest entry")
                .range
                .start(),
            manifest.entries.last().expect("manifest entry").range.end(),
            rows.row_count()
        );
        self.write_manifest(chain, &manifest)?;
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
