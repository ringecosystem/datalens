//! Storage boundary for durable datalens objects and coverage metadata.

use std::{
    fs,
    path::{Path, PathBuf},
};

use datalens_chain::DatasetSelector;
use datalens_core::{
    BlockRange, ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, Dataset, DatasetId,
    QueryRows, TimeRange,
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
        dataset: Dataset,
        selector: &DatasetSelector,
        range: BlockRange,
    ) -> Result<Vec<BlockRange>, DatalensError> {
        let filter_key = coverage_key(chain, dataset, selector);
        let mut ranges = self
            .manifest()?
            .entries
            .into_iter()
            .filter(|entry| entry.dataset == dataset && entry.filter_key == filter_key)
            .filter_map(|entry| intersect(entry.range, range))
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.from_block);
        Ok(merge_ranges(ranges))
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset: Dataset,
        selector: &DatasetSelector,
        range: BlockRange,
    ) -> Result<QueryRows, DatalensError> {
        log::debug!(
            "storage read dataset={} range={}-{}",
            dataset.as_str(),
            range.from_block,
            range.to_block
        );
        let filter_key = coverage_key(chain, dataset, selector);
        let mut rows = empty_rows(dataset);
        for entry in self.manifest()?.entries {
            if entry.dataset != dataset || entry.filter_key != filter_key {
                continue;
            }
            if intersect(entry.range, range).is_none() {
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
            let mut object_rows: QueryRows = serde_json::from_slice(&bytes).map_err(|error| {
                log::warn!(
                    "failed to decode cached object {}: {error}",
                    object.display()
                );
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode cached object {}: {error}", object.display()),
                )
            })?;
            object_rows = filter_rows(object_rows, range);
            rows.try_append(object_rows)?;
        }
        rows.sort();
        Ok(rows)
    }

    pub fn write_rows(
        &self,
        chain: &ChainIdentity,
        dataset: Dataset,
        selector: &DatasetSelector,
        range: BlockRange,
        rows: &QueryRows,
        record_empty_coverage: bool,
    ) -> Result<(), DatalensError> {
        if rows.row_count() == 0 && !record_empty_coverage {
            log::debug!(
                "storage skipped empty coverage dataset={} range={}-{}",
                dataset.as_str(),
                range.from_block,
                range.to_block
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

        let filter_key = coverage_key(chain, dataset, selector);
        let object_key = if rows.row_count() == 0 {
            None
        } else {
            let object_key = format!(
                "objects/{}/{}/{}-{}.json",
                dataset.as_str(),
                filter_key,
                range.from_block,
                range.to_block
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
            dataset,
            filter_key,
            range,
            object_key,
            row_count: rows.row_count(),
        });
        log::info!(
            "storage wrote coverage dataset={} range={}-{} rows={}",
            dataset.as_str(),
            range.from_block,
            range.to_block,
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
    dataset: Dataset,
    filter_key: String,
    range: BlockRange,
    object_key: Option<String>,
    row_count: usize,
}

impl<'de> Deserialize<'de> for ManifestEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawManifestEntry {
            dataset: Dataset,
            filter_key: String,
            range: BlockRange,
            object_key: Option<String>,
            row_count: usize,
        }

        let raw = RawManifestEntry::deserialize(deserializer)?;
        ManifestEntry::try_from_parts(
            raw.dataset,
            raw.filter_key,
            raw.range,
            raw.object_key,
            raw.row_count,
        )
        .map_err(D::Error::custom)
    }
}

impl ManifestEntry {
    fn try_from_parts(
        dataset: Dataset,
        filter_key: String,
        range: BlockRange,
        object_key: Option<String>,
        row_count: usize,
    ) -> Result<Self, DatalensError> {
        match object_key {
            Some(object_key) => {
                if row_count == 0 {
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
                    dataset,
                    filter_key,
                    range,
                    object_key: Some(object_key),
                    row_count,
                })
            }
            None => {
                if row_count != 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "empty coverage must have row_count zero",
                    ));
                }
                Ok(Self {
                    dataset,
                    filter_key,
                    range,
                    object_key: None,
                    row_count,
                })
            }
        }
    }
}

pub fn missing_ranges(range: BlockRange, covered: &[BlockRange]) -> Vec<BlockRange> {
    let mut missing = Vec::new();
    let mut cursor = range.from_block;
    for covered_range in covered {
        if covered_range.to_block < cursor {
            continue;
        }
        if covered_range.from_block > range.to_block {
            break;
        }
        if cursor < covered_range.from_block {
            missing.push(BlockRange::expect_new(cursor, covered_range.from_block - 1));
        }
        cursor = cursor.max(covered_range.to_block.saturating_add(1));
        if cursor > range.to_block {
            break;
        }
    }
    if cursor <= range.to_block {
        missing.push(BlockRange::expect_new(cursor, range.to_block));
    }
    missing
}

fn coverage_key(chain: &ChainIdentity, dataset: Dataset, selector: &DatasetSelector) -> String {
    format!(
        "chains/{}/datasets/{}/{}",
        chain.key_prefix(),
        dataset.as_str(),
        selector.fingerprint()
    )
}

fn merge_ranges(mut ranges: Vec<BlockRange>) -> Vec<BlockRange> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|range| range.from_block);
    let mut merged = vec![ranges[0]];
    for range in ranges.into_iter().skip(1) {
        let last = merged.last_mut().expect("merged range");
        if range.from_block <= last.to_block.saturating_add(1) {
            last.to_block = last.to_block.max(range.to_block);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn intersect(left: BlockRange, right: BlockRange) -> Option<BlockRange> {
    left.intersection(&right)
}

fn empty_rows(dataset: Dataset) -> QueryRows {
    match dataset {
        Dataset::Blocks => QueryRows::EvmBlocks(Vec::new()),
        Dataset::Logs => QueryRows::EvmLogs(Vec::new()),
    }
}

fn filter_rows(rows: QueryRows, range: BlockRange) -> QueryRows {
    match rows {
        QueryRows::EvmBlocks(rows) => QueryRows::EvmBlocks(
            rows.into_iter()
                .filter(|row| range.contains(row.number))
                .collect(),
        ),
        QueryRows::EvmLogs(rows) => QueryRows::EvmLogs(
            rows.into_iter()
                .filter(|row| range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::TronEvents(rows) => QueryRows::TronEvents(rows),
        QueryRows::SolanaTransactions(rows) => QueryRows::SolanaTransactions(rows),
        QueryRows::SolanaInstructions(rows) => QueryRows::SolanaInstructions(rows),
        QueryRows::OtherJson(rows) => QueryRows::OtherJson(rows),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use datalens_chain::DatasetSelector;
    use datalens_core::{ChainFamily, LogFilter, LogRecord, NetworkId, QueryRows};

    use super::*;

    #[test]
    fn test_log_filter_object_key_uses_compact_storage_safe_segment() {
        let storage = LocalStorage::new(temp_storage_root("compact-log-filter"));
        let range = BlockRange::expect_new(1, 1);
        let filter = LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![None],
        };
        let chain = test_chain();
        let selector = DatasetSelector::try_evm_logs(filter).expect("valid selector");
        let rows = QueryRows::EvmLogs(vec![
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
        ]);

        storage
            .write_rows(&chain, Dataset::Logs, &selector, range, &rows, true)
            .expect("write rows");

        let manifest = storage.manifest().expect("manifest");
        let entry = manifest.entries.first().expect("manifest entry");
        assert!(entry.filter_key.contains("/evm-logs/addr-topic-"));
        assert!(!entry.filter_key.contains("0xaaaaaaaa"));
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
                "dataset":"logs",
                "filter_key":"evm-logs/addr-topic-deadbeef",
                "range":{"from_block":1,"to_block":2},
                "object_key":null,
                "row_count":1
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(row_count_without_object).is_err());

        let object_without_rows = r#"{
            "entries":[{
                "dataset":"logs",
                "filter_key":"evm-logs/addr-topic-deadbeef",
                "range":{"from_block":1,"to_block":2},
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
                "dataset":"logs",
                "filter_key":"evm-logs/addr-topic-deadbeef",
                "range":{"from_block":1,"to_block":2},
                "object_key":null,
                "row_count":0
            }]
        }"#;
        assert!(serde_json::from_str::<Manifest>(empty).is_ok());

        let data_object = r#"{
            "entries":[{
                "dataset":"logs",
                "filter_key":"evm-logs/addr-topic-deadbeef",
                "range":{"from_block":1,"to_block":2},
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
                    "dataset":"logs",
                    "filter_key":"evm-logs/addr-topic-deadbeef",
                    "range":{"from_block":1,"to_block":2},
                    "object_key":null,
                    "row_count":1
                }]
            }"#,
        )
        .expect("write manifest");

        let error = storage
            .covered_ranges(
                &test_chain(),
                Dataset::Logs,
                &DatasetSelector::all(),
                BlockRange::expect_new(1, 2),
            )
            .expect_err("malformed manifest");

        assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    }

    #[test]
    fn test_read_rows_rejects_invalid_cached_log_record() {
        let storage = LocalStorage::new(temp_storage_root("invalid-cached-log"));
        let filter_key = coverage_key(&test_chain(), Dataset::Logs, &DatasetSelector::all());
        let object_key = format!("objects/logs/{filter_key}/1-1.json");
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
                        "filter_key":"{filter_key}",
                        "range":{{"from_block":1,"to_block":1}},
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
                Dataset::Logs,
                &DatasetSelector::all(),
                BlockRange::expect_new(1, 1),
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

        let key = coverage_key(&chain, Dataset::Logs, &selector);

        assert!(key.starts_with("chains/evm/ethereum/1/datasets/logs/"));
        assert!(key.contains("/evm-logs/addr-topic-"));
        assert!(!key.contains("0xaaaaaaaa"));
    }
}
