//! Storage boundary for durable datalens objects and coverage metadata.

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryRows,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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

mod hot_cache;
mod maintenance;
mod manifest;
mod object_store;
mod parquet_codec;
mod usage_ledger;
pub use hot_cache::{
    HotBlockMetadata, HotCache, HotCacheConfig, HotEntryStatus, HotManifest, HotManifestEntry,
    HotReorgOutcome, HotReorgReason, HotWriteOutcome, HotWriteRequest,
};
pub use maintenance::{
    CompactionCandidate, MaintenanceCheckReport, MaintenanceCompactionReport, MaintenanceIssue,
    MaintenanceIssueKind, MaintenanceOperation, MaintenanceOperationMode, MaintenanceReport,
    MaintenanceRetentionReport, MaintenanceUsageLedgerReport, RetentionPolicy,
    UsageLedgerRollupModel,
};
pub use manifest::{Manifest, ManifestEntry, ManifestFinalityLevel};
pub use object_store::{
    LocalObjectStore, ObjectMetadata, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
    validate_object_key,
};
pub use usage_ledger::{
    CacheOutcome, FillOutcome, QueryOutcome, UsageLedgerEntry, UsageLedgerRepository,
    UsageLedgerStore,
};

#[derive(Clone, Debug)]
pub struct DurableStorage<S> {
    object_store: S,
}

pub type LocalStorage = DurableStorage<LocalObjectStore>;

impl DurableStorage<LocalObjectStore> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            object_store: LocalObjectStore::new(root),
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
        Self { object_store }
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
            let bytes = self.object_store.get(object_key)?;
            verify_manifest_object_metadata(&entry, object_key, &bytes)?;
            let encoding = entry.object_encoding.unwrap_or_else(|| {
                ObjectEncoding::from_object_key(object_key).unwrap_or(ObjectEncoding::Json)
            });
            let mut object_rows = decode_object_rows(encoding, dataset_key.clone(), &bytes)
                .map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!("decode cached object {object_key}: {}", error.message),
                    )
                })?;
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

    fn manifest_for_chain(&self, chain: &ChainIdentity) -> Result<Manifest, DatalensError> {
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

    fn write_manifest(
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectEncoding {
    Json,
    ParquetV1,
}

impl ObjectEncoding {
    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::ParquetV1 => "parquet-v1",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Json => ".json",
            Self::ParquetV1 => ".parquet",
        }
    }

    fn from_object_key(object_key: &str) -> Option<Self> {
        if object_key.contains("/parquet-v1/") || object_key.ends_with(".parquet") {
            Some(Self::ParquetV1)
        } else if object_key.contains("/json/") || object_key.ends_with(".json") {
            Some(Self::Json)
        } else {
            None
        }
    }

    fn validate_object_key(self, object_key: &str) -> Result<(), DatalensError> {
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

pub fn coverage_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: LedgerRangeKind,
    selector: &DatasetSelector,
) -> String {
    format!(
        "chains/{}/datasets/{}/{}/{}/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        object_encoding_for_dataset(dataset_key).as_str(),
        range_kind_key(range_kind),
        selector.fingerprint()
    )
}

fn object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: LedgerRange,
    selector_fingerprint: &str,
    encoding: ObjectEncoding,
) -> String {
    format!(
        "chains/{}/datasets/{}/{}/{}/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        encoding.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
    ) + &format!(
        "/{:#020}-{:#020}{}",
        range.start(),
        range.end(),
        encoding.extension()
    )
    .replace("0x", "")
}

fn manifest_key(chain: &ChainIdentity) -> String {
    format!("chains/{}/manifest.json", chain.key_prefix())
}

fn checksum_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

fn validate_existing_data_object(
    existing: &ManifestEntry,
    data_object: &StorageDataObject,
) -> Result<(), DatalensError> {
    if existing.object_key.as_deref() != Some(data_object.object_key.as_str())
        || existing.object_encoding != Some(data_object.object_encoding)
        || existing.row_count != data_object.row_count
        || existing
            .object_size_bytes
            .is_some_and(|size| size != data_object.object_size_bytes)
        || existing
            .checksum
            .as_deref()
            .is_some_and(|checksum| checksum != data_object.checksum)
        || existing
            .checksum_algorithm
            .as_deref()
            .is_some_and(|algorithm| algorithm != data_object.checksum_algorithm)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "existing manifest data object metadata differs for logical shard",
        ));
    }
    Ok(())
}

fn verify_manifest_object_metadata(
    entry: &ManifestEntry,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), DatalensError> {
    if let Some(expected_size) = entry.object_size_bytes {
        let actual_size = bytes.len() as u64;
        if actual_size != expected_size {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!(
                    "cached object {object_key} size mismatch: expected {expected_size} bytes, got {actual_size} bytes"
                ),
            ));
        }
    }

    match (
        entry.checksum.as_deref(),
        entry.checksum_algorithm.as_deref(),
    ) {
        (Some(expected_checksum), Some("sha256")) => {
            let actual_checksum = checksum_hex(bytes);
            if actual_checksum != expected_checksum {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("cached object {object_key} checksum mismatch for sha256"),
                ));
            }
        }
        (Some(_), Some(algorithm)) => {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("cached object {object_key} unknown checksum algorithm {algorithm}"),
            ));
        }
        (Some(_), None) | (None, Some(_)) => {
            log::debug!(
                "storage skipped incomplete cached object checksum metadata object_key={object_key}"
            );
        }
        (None, None) => {}
    }

    Ok(())
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn merge_ranges(mut ranges: Vec<LedgerRange>) -> Vec<LedgerRange> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|range| range.start());
    let mut merged = vec![ranges[0].clone()];
    for range in ranges.into_iter().skip(1) {
        let last = merged.last_mut().expect("merged range");
        if range.kind() == last.kind() && range.start() <= last.end().saturating_add(1) {
            let end = last.end().max(range.end());
            *last = LedgerRange::try_new(last.kind(), last.start(), end).expect("valid range");
        } else {
            merged.push(range);
        }
    }
    merged
}

fn intersect(left: LedgerRange, right: LedgerRange) -> Option<LedgerRange> {
    left.intersection(&right)
}

fn object_encoding_for_dataset(dataset_key: &DatasetKey) -> ObjectEncoding {
    match dataset_key.legacy_dataset() {
        Some(datalens_core::Dataset::Blocks | datalens_core::Dataset::Logs) => {
            ObjectEncoding::ParquetV1
        }
        None => ObjectEncoding::Json,
    }
}

fn encode_object_rows(
    encoding: ObjectEncoding,
    rows: &DatasetRows,
) -> Result<Vec<u8>, DatalensError> {
    match encoding {
        ObjectEncoding::Json => serde_json::to_vec(rows).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode cached rows: {error}"),
            )
        }),
        ObjectEncoding::ParquetV1 => parquet_codec::encode_rows(rows),
    }
}

fn decode_object_rows(
    encoding: ObjectEncoding,
    dataset_key: DatasetKey,
    bytes: &[u8],
) -> Result<DatasetRows, DatalensError> {
    match encoding {
        ObjectEncoding::Json => serde_json::from_slice(bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode json cached rows: {error}"),
            )
        }),
        ObjectEncoding::ParquetV1 => parquet_codec::decode_rows(dataset_key, bytes),
    }
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

fn filter_rows(rows: DatasetRows, range: LedgerRange) -> DatasetRows {
    let dataset_key = rows.dataset_key().clone();
    let Some(block_range) = range.block_range() else {
        return rows;
    };
    let rows = match rows.into_rows() {
        QueryRows::EvmBlocks(rows) => QueryRows::EvmBlocks(
            rows.into_iter()
                .filter(|row| block_range.contains(row.number))
                .collect(),
        ),
        QueryRows::EvmLogs(rows) => QueryRows::EvmLogs(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::AdapterJson { dataset_key, rows } => {
            QueryRows::AdapterJson { dataset_key, rows }
        }
    };
    DatasetRows::new(dataset_key, rows).expect("filtered rows keep dataset key")
}
