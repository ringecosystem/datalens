//! Storage boundary for durable datalens objects and coverage metadata.

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryRows,
};
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use std::path::{Path, PathBuf};

const OBJECT_SCHEMA_VERSION: &str = "json";

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

mod object_store;
pub use object_store::{
    LocalObjectStore, ObjectMetadata, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
    validate_object_key,
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
            let Some(object_key) = entry.object_key else {
                continue;
            };
            if !self.object_store.exists(&object_key)? {
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("manifest entry object not found {object_key}"),
                ));
            }
            let bytes = self.object_store.get(&object_key)?;
            let mut object_rows: DatasetRows = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode cached object {object_key}: {error}"),
                )
            })?;
            object_rows = filter_rows(object_rows, range.clone());
            rows.try_append(object_rows.into_rows())?;
        }
        rows.sort();
        DatasetRows::new(dataset_key.clone(), rows)
    }

    pub fn write_rows(&self, request: StorageWriteRequest<'_>) -> Result<(), DatalensError> {
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
            return Ok(());
        }

        let selector_fingerprint = selector.fingerprint();
        let selector_canonical_key = selector.canonical_key();
        let object_key = if rows.row_count() == 0 {
            None
        } else {
            let object_key = object_key(chain, &dataset_key, range.clone(), &selector_fingerprint);
            let bytes = serde_json::to_vec(rows).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode cached rows: {error}"),
                )
            })?;
            self.object_store.put(&object_key, &bytes)?;
            Some(object_key)
        };

        let mut manifest = self.manifest_for_chain(chain)?;
        let entry = ManifestEntry {
            chain: chain.clone(),
            dataset_key: dataset_key.clone(),
            range,
            selector_fingerprint,
            selector_canonical_key,
            finality_level,
            object_key,
            row_count: rows.row_count(),
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
        self.write_manifest(chain, &manifest)
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

    fn write_rows(&self, request: StorageWriteRequest<'_>) -> Result<(), DatalensError>;
}

impl<S> StorageRepository for DurableStorage<S>
where
    S: ObjectStore + 'static,
{
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

    fn write_rows(&self, request: StorageWriteRequest<'_>) -> Result<(), DatalensError> {
        Self::write_rows(self, request)
    }
}

impl StorageRepository for Box<dyn StorageRepository> {
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

    fn write_rows(&self, request: StorageWriteRequest<'_>) -> Result<(), DatalensError> {
        self.as_ref().write_rows(request)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
}

impl Manifest {
    fn upsert(&mut self, entry: ManifestEntry) {
        if let Some(existing) = self.entries.iter_mut().find(|existing| {
            existing.chain == entry.chain
                && existing.dataset_key == entry.dataset_key
                && existing.selector_fingerprint == entry.selector_fingerprint
                && existing.range == entry.range
                && existing.finality_level == entry.finality_level
        }) {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.entries.sort_by_key(|entry| {
            (
                entry.dataset_key.as_str().to_owned(),
                entry.range.kind().key(),
                entry.selector_fingerprint.clone(),
                entry.range.start(),
                entry.range.end(),
            )
        });
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestEntry {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub finality_level: ManifestFinalityLevel,
    pub object_key: Option<String>,
    pub row_count: usize,
}

#[derive(Deserialize)]
struct RawManifestEntry {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    range: LedgerRange,
    selector_fingerprint: String,
    selector_canonical_key: String,
    finality_level: ManifestFinalityLevel,
    object_key: Option<String>,
    row_count: usize,
}

impl<'de> Deserialize<'de> for ManifestEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawManifestEntry::deserialize(deserializer)?;
        ManifestEntry::try_from_raw(raw).map_err(D::Error::custom)
    }
}

impl ManifestEntry {
    fn try_from_raw(raw: RawManifestEntry) -> Result<Self, DatalensError> {
        validate_object_key(&raw.selector_fingerprint)?;
        validate_object_key(&raw.selector_canonical_key)?;
        if let Some(object_key) = raw.object_key.as_deref() {
            validate_object_key(object_key)?;
        }
        match raw.object_key {
            Some(object_key) => {
                if raw.row_count == 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage must have row_count greater than zero",
                    ));
                }
                Ok(Self {
                    chain: raw.chain,
                    dataset_key: raw.dataset_key,
                    range: raw.range,
                    selector_fingerprint: raw.selector_fingerprint,
                    selector_canonical_key: raw.selector_canonical_key,
                    finality_level: raw.finality_level,
                    object_key: Some(object_key),
                    row_count: raw.row_count,
                })
            }
            None => {
                if raw.row_count != 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "empty coverage must have row_count zero",
                    ));
                }
                Ok(Self {
                    chain: raw.chain,
                    dataset_key: raw.dataset_key,
                    range: raw.range,
                    selector_fingerprint: raw.selector_fingerprint,
                    selector_canonical_key: raw.selector_canonical_key,
                    finality_level: raw.finality_level,
                    object_key: None,
                    row_count: raw.row_count,
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestFinalityLevel {
    Safe,
    Finalized,
}

impl TryFrom<FinalityLevel> for ManifestFinalityLevel {
    type Error = DatalensError;

    fn try_from(value: FinalityLevel) -> Result<Self, Self::Error> {
        match value {
            FinalityLevel::Safe => Ok(Self::Safe),
            FinalityLevel::Finalized => Ok(Self::Finalized),
            FinalityLevel::Latest | FinalityLevel::ChainSpecific(_) => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable storage coverage requires safe or finalized finality",
            )),
        }
    }
}

pub fn missing_ranges(range: LedgerRange, covered: &[LedgerRange]) -> Vec<LedgerRange> {
    let mut missing = Vec::new();
    let mut cursor = range.start();
    for covered_range in covered {
        if covered_range.kind() != range.kind() || covered_range.end() < cursor {
            continue;
        }
        if covered_range.start() > range.end() {
            break;
        }
        if cursor < covered_range.start() {
            missing.push(
                LedgerRange::try_new(range.kind(), cursor, covered_range.start() - 1)
                    .expect("valid missing range"),
            );
        }
        cursor = cursor.max(covered_range.end().saturating_add(1));
        if cursor > range.end() {
            break;
        }
    }
    if cursor <= range.end() {
        missing.push(LedgerRange::try_new(range.kind(), cursor, range.end()).expect("valid range"));
    }
    missing
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
        OBJECT_SCHEMA_VERSION,
        range_kind_key(range_kind),
        selector.fingerprint()
    )
}

fn object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: LedgerRange,
    selector_fingerprint: &str,
) -> String {
    format!(
        "chains/{}/datasets/{}/{}/{}/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        OBJECT_SCHEMA_VERSION,
        range_kind_key(range.kind()),
        selector_fingerprint,
    ) + &format!("/{:#020}-{:#020}.json", range.start(), range.end()).replace("0x", "")
}

fn manifest_key(chain: &ChainIdentity) -> String {
    format!("chains/{}/manifest.json", chain.key_prefix())
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

trait LedgerRangeKindExt {
    fn key(self) -> String;
}

impl LedgerRangeKindExt for LedgerRangeKind {
    fn key(self) -> String {
        range_kind_key(self)
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
