//! Storage boundary for durable datalens objects and coverage metadata.

use std::{
    fs,
    path::{Path, PathBuf},
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, DatasetId, DatasetKey,
    DatasetRows, LedgerRange, LedgerRangeKind, QueryRows, TimeRange,
};
use serde::{Deserialize, Deserializer, Serialize, de::Error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageCoverage {
    level: CoverageLevel,
}

impl StorageCoverage {
    pub fn new(level: CoverageLevel) -> Self {
        Self { level }
    }

    pub fn level(&self) -> &CoverageLevel {
        &self.level
    }
}

pub trait Storage {
    fn coverage(&self, request: &StorageRequest) -> StorageCoverage;
}

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

#[derive(Clone, Debug, Default)]
pub struct InMemoryStorage;

impl Storage for InMemoryStorage {
    fn coverage(&self, _request: &StorageRequest) -> StorageCoverage {
        StorageCoverage::new(CoverageLevel::Missing)
    }
}

#[derive(Clone, Debug)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
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
            .manifest()?
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
        for entry in self.manifest()?.entries {
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
            let object = self.root.join(object_key);
            let bytes = fs::read(&object).map_err(|error| {
                log::warn!("failed to read cached object {}: {error}", object.display());
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("read cached object {}: {error}", object.display()),
                )
            })?;
            let mut object_rows: DatasetRows = serde_json::from_slice(&bytes).map_err(|error| {
                log::warn!(
                    "failed to decode cached object {}: {error}",
                    object.display()
                );
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode cached object {}: {error}", object.display()),
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

        fs::create_dir_all(&self.root).map_err(|error| {
            log::error!(
                "failed to create storage root {}: {error}",
                self.root.display()
            );
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("create storage root {}: {error}", self.root.display()),
            )
        })?;

        let selector_fingerprint = selector.fingerprint();
        let selector_canonical_key = selector.canonical_key();
        let object_key = if rows.row_count() == 0 {
            None
        } else {
            let coverage_key = coverage_key(chain, &dataset_key, range.kind(), selector);
            let object_key = format!(
                "objects/{}/{}-{}.json",
                coverage_key,
                range.start(),
                range.end()
            );
            let object_path = self.root.join(&object_key);
            if let Some(parent) = object_path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    log::error!(
                        "failed to create object directory {}: {error}",
                        parent.display()
                    );
                    DatalensError::new(
                        DatalensErrorKind::StorageWriteFailure,
                        format!("create object directory {}: {error}", parent.display()),
                    )
                })?;
            }
            let bytes = serde_json::to_vec(rows).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode cached rows: {error}"),
                )
            })?;
            fs::write(&object_path, bytes).map_err(|error| {
                log::error!(
                    "failed to write cached object {}: {error}",
                    object_path.display()
                );
                DatalensError::new(
                    DatalensErrorKind::StorageWriteFailure,
                    format!("write cached object {}: {error}", object_path.display()),
                )
            })?;
            Some(object_key)
        };

        let mut manifest = self.manifest()?;
        manifest.entries.push(ManifestEntry {
            chain: chain.clone(),
            dataset_key: dataset_key.clone(),
            range,
            selector_fingerprint,
            selector_canonical_key,
            finality_level,
            object_key,
            row_count: rows.row_count(),
        });
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
        self.write_manifest(&manifest)
    }

    fn manifest(&self) -> Result<Manifest, DatalensError> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(Manifest::default());
        }
        let bytes = fs::read(&path).map_err(|error| {
            log::warn!("failed to read manifest {}: {error}", path.display());
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("read manifest {}: {error}", path.display()),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            log::warn!("failed to decode manifest {}: {error}", path.display());
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode manifest {}: {error}", path.display()),
            )
        })
    }

    fn write_manifest(&self, manifest: &Manifest) -> Result<(), DatalensError> {
        let path = self.manifest_path();
        let bytes = serde_json::to_vec_pretty(manifest).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode manifest: {error}"),
            )
        })?;
        fs::write(&path, bytes).map_err(|error| {
            log::error!("failed to write manifest {}: {error}", path.display());
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write manifest {}: {error}", path.display()),
            )
        })
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("manifest.json")
    }
}

impl Storage for LocalStorage {
    fn coverage(&self, _request: &StorageRequest) -> StorageCoverage {
        StorageCoverage::new(CoverageLevel::Missing)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct Manifest {
    #[serde(default)]
    entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ManifestEntry {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    range: LedgerRange,
    selector_fingerprint: String,
    selector_canonical_key: String,
    finality_level: ManifestFinalityLevel,
    object_key: Option<String>,
    row_count: usize,
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
        validate_storage_key("selector fingerprint", &raw.selector_fingerprint)?;
        validate_storage_key("selector canonical key", &raw.selector_canonical_key)?;
        match raw.object_key {
            Some(object_key) => {
                if raw.row_count == 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage must have row_count greater than zero",
                    ));
                }
                if object_key.trim().is_empty() {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage must have a non-empty object key",
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
enum ManifestFinalityLevel {
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

fn coverage_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: LedgerRangeKind,
    selector: &DatasetSelector,
) -> String {
    format!(
        "chains/{}/datasets/{}/ranges/{}/selectors/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind_key(range_kind),
        selector.fingerprint()
    )
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn validate_storage_key(kind: &str, value: &str) -> Result<(), DatalensError> {
    if value.trim().is_empty()
        || value.contains('\\')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must be a relative storage key"),
        ));
    }
    Ok(())
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
        None => QueryRows::OtherJson(Vec::new()),
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
        QueryRows::TronEvents(rows) => QueryRows::TronEvents(rows),
        QueryRows::SolanaTransactions(rows) => QueryRows::SolanaTransactions(rows),
        QueryRows::SolanaInstructions(rows) => QueryRows::SolanaInstructions(rows),
        QueryRows::OtherJson(rows) => QueryRows::OtherJson(rows),
    };
    DatasetRows::new(dataset_key, rows).expect("filtered rows keep dataset key")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use datalens_chain::DatasetSelector;
    use datalens_chain::FinalityLevel;
    use datalens_core::{
        BlockHeader, ChainFamily, DatasetKey, DatasetRows, LedgerRange, LedgerRangeKind, LogFilter,
        LogRecord, NetworkId, QueryRows,
    };

    use super::*;

    #[test]
    fn test_log_filter_object_key_uses_compact_storage_safe_segment() {
        let storage = LocalStorage::new(temp_storage_root("compact-log-filter"));
        let range = LedgerRange::blocks(1, 1).expect("valid range");
        let filter = LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![None],
        };
        let chain = test_chain();
        let selector = DatasetSelector::try_evm_logs(filter).expect("valid selector");
        let rows = DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![
                LogRecord::try_new(
                    1,
                    "0xblock".to_owned(),
                    "0xtx".to_owned(),
                    0,
                    0,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    Vec::new(),
                    "0x".to_owned(),
                    false,
                )
                .unwrap(),
            ]),
        )
        .expect("dataset rows");

        storage
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_logs(),
                selector: &selector,
                range,
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write rows");

        let manifest = storage.manifest().expect("manifest");
        let entry = manifest.entries.first().expect("manifest entry");
        assert!(entry.selector_fingerprint.contains("evm-logs/addr-topic-"));
        assert!(!entry.selector_fingerprint.contains("0xaaaaaaaa"));
        assert!(
            entry
                .object_key
                .as_deref()
                .expect("object key")
                .contains("evm-logs/addr-topic-")
        );
    }

    #[test]
    fn test_manifest_deserialization_rejects_invalid_coverage_semantics() {
        let row_count_without_object = r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"safe",
                "object_key":null,
                "row_count":1
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(row_count_without_object).is_err());

        let object_without_rows = r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"safe",
                "object_key":"objects/logs/key/1-2.json",
                "row_count":0
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(object_without_rows).is_err());
    }

    #[test]
    fn test_manifest_deserialization_accepts_valid_coverage_semantics() {
        let empty = r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"safe",
                "object_key":null,
                "row_count":0
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(empty).is_ok());

        let data_object = r#"{
            "entries":[{
                "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                "dataset_key":{"family":"Evm","name":"logs"},
                "range":{"kind":{"kind":"block"},"start":1,"end":2},
                "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                "selector_canonical_key":"evm-logs/addr=*",
                "finality_level":"finalized",
                "object_key":"objects/logs/key/1-2.json",
                "row_count":1
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(data_object).is_ok());
    }

    #[test]
    fn test_covered_ranges_rejects_malformed_manifest_entries() {
        let storage = LocalStorage::new(temp_storage_root("malformed-manifest"));
        std::fs::write(
            storage.manifest_path(),
            r#"{
                "entries":[{
                    "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
                    "dataset_key":{"family":"Evm","name":"logs"},
                    "range":{"kind":{"kind":"block"},"start":1,"end":2},
                    "selector_fingerprint":"evm-logs/addr-topic-deadbeef",
                    "selector_canonical_key":"evm-logs/addr=*",
                    "finality_level":"safe",
                    "object_key":null,
                    "row_count":1
                }]
            }"#,
        )
        .expect("write manifest");

        let error = storage
            .covered_ranges(
                &test_chain(),
                &DatasetKey::evm_logs(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 2).expect("valid range"),
            )
            .expect_err("malformed manifest");

        assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    }

    #[test]
    fn test_read_rows_rejects_invalid_cached_log_record() {
        let storage = LocalStorage::new(temp_storage_root("invalid-cached-log"));
        let filter_key = coverage_key(
            &test_chain(),
            &DatasetKey::evm_logs(),
            LedgerRangeKind::Block,
            &DatasetSelector::all(),
        );
        let object_key = format!("objects/{filter_key}/1-1.json");
        let object_path = storage.root().join(&object_key);
        std::fs::create_dir_all(object_path.parent().expect("object parent"))
            .expect("create object dir");
        std::fs::write(
            &object_path,
            r#"{
                "dataset":"logs",
                "rows":[{
                    "block_number":1,
                    "block_hash":"0xblock",
                    "transaction_hash":"0xtx",
                    "transaction_index":0,
                    "log_index":0,
                    "address":"0xabc",
                    "topics":[],
                    "data":"0x",
                    "removed":false
                }]
            }"#,
        )
        .expect("write object");
        std::fs::write(
            storage.manifest_path(),
            format!(
                r#"{{
                    "entries":[{{
                        "dataset":"logs",
                        "chain":{{"family":"Evm","configured_name":"ethereum","network_id":{{"kind":"numeric","value":1}}}},
                        "dataset_key":{{"family":"Evm","name":"logs"}},
                        "range":{{"kind":{{"kind":"block"}},"start":1,"end":1}},
                        "selector_fingerprint":"all",
                        "selector_canonical_key":"all",
                        "finality_level":"safe",
                        "object_key":"{object_key}",
                        "row_count":1
                    }}]
                }}"#
            ),
        )
        .expect("write manifest");

        let error = storage
            .read_rows(
                &test_chain(),
                &DatasetKey::evm_logs(),
                &DatasetSelector::all(),
                LedgerRange::blocks(1, 1).expect("valid range"),
            )
            .expect_err("invalid cached log");

        assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    }

    fn temp_storage_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "datalens-storage-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create temp storage root");
        root
    }

    fn test_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
    }

    #[test]
    fn test_selector_coverage_key_includes_chain_dataset_and_stable_fingerprint() {
        let chain = ChainIdentity::expect_with_network_id(
            ChainFamily::Evm,
            "ethereum",
            NetworkId::numeric(1),
        );
        let selector = DatasetSelector::try_evm_logs(LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![None],
        })
        .expect("valid selector");

        let key = coverage_key(
            &chain,
            &DatasetKey::evm_logs(),
            LedgerRangeKind::Block,
            &selector,
        );

        assert!(key.starts_with("chains/evm/ethereum/1/datasets/evm.logs/ranges/block/selectors/"));
        assert!(key.contains("/evm-logs/addr-topic-"));
        assert!(!key.contains("0xaaaaaaaa"));
    }

    #[test]
    fn test_manifest_entry_records_chain_neutral_coverage_identity() {
        let storage = LocalStorage::new(temp_storage_root("chain-neutral-manifest"));
        let chain = test_chain();
        let selector = DatasetSelector::all();
        let range = LedgerRange::blocks(10, 12).expect("valid range");
        let rows = DatasetRows::new(
            DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![BlockHeader {
                number: 11,
                hash: "0xblock".to_owned(),
                parent_hash: "0xparent".to_owned(),
                timestamp: 1,
            }]),
        )
        .expect("dataset rows");

        storage
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_blocks(),
                selector: &selector,
                range: range.clone(),
                rows: &rows,
                finality_level: FinalityLevel::Finalized,
                record_empty_coverage: true,
            })
            .expect("write rows");

        let manifest = storage.manifest().expect("manifest");
        let entry = manifest.entries.first().expect("manifest entry");
        assert_eq!(entry.chain, chain);
        assert_eq!(entry.dataset_key, DatasetKey::evm_blocks());
        assert_eq!(entry.range, range);
        assert_eq!(entry.selector_fingerprint, "all");
        assert_eq!(entry.selector_canonical_key, "all");
        assert_eq!(entry.finality_level, ManifestFinalityLevel::Finalized);
        assert!(
            entry
                .object_key
                .as_deref()
                .expect("object key")
                .starts_with(
                    "objects/chains/evm/ethereum/1/datasets/evm.blocks/ranges/block/selectors/all/"
                )
        );
    }

    #[test]
    fn test_write_rows_rejects_latest_finality_for_durable_manifest() {
        let storage = LocalStorage::new(temp_storage_root("latest-finality"));
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");

        let error = storage
            .write_rows(StorageWriteRequest {
                chain: &test_chain(),
                dataset_key: DatasetKey::evm_logs(),
                selector: &DatasetSelector::all(),
                range: LedgerRange::blocks(1, 2).expect("valid range"),
                rows: &rows,
                finality_level: FinalityLevel::Latest,
                record_empty_coverage: true,
            })
            .expect_err("latest coverage rejected");

        assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    }

    #[test]
    fn test_empty_coverage_uses_chain_neutral_missing_ranges() {
        let storage = LocalStorage::new(temp_storage_root("chain-neutral-empty"));
        let chain = test_chain();
        let selector = DatasetSelector::all();
        let rows = DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
            .expect("dataset rows");

        storage
            .write_rows(StorageWriteRequest {
                chain: &chain,
                dataset_key: DatasetKey::evm_logs(),
                selector: &selector,
                range: LedgerRange::blocks(5, 7).expect("valid range"),
                rows: &rows,
                finality_level: FinalityLevel::Safe,
                record_empty_coverage: true,
            })
            .expect("write empty coverage");

        let covered = storage
            .covered_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &selector,
                LedgerRange::blocks(4, 8).expect("valid range"),
            )
            .expect("covered ranges");
        assert_eq!(
            covered,
            vec![LedgerRange::blocks(5, 7).expect("valid range")]
        );
        assert_eq!(
            missing_ranges(LedgerRange::blocks(4, 8).expect("valid range"), &covered),
            vec![
                LedgerRange::blocks(4, 4).expect("valid range"),
                LedgerRange::blocks(8, 8).expect("valid range"),
            ]
        );

        let manifest = storage.manifest().expect("manifest");
        let entry = manifest.entries.first().expect("manifest entry");
        assert_eq!(entry.object_key, None);
        assert_eq!(entry.row_count, 0);
        assert_eq!(entry.finality_level, ManifestFinalityLevel::Safe);
    }
}
