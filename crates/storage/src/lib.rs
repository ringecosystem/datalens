//! Storage boundary for durable datalens objects and coverage metadata.

use std::{
    fs,
    path::{Path, PathBuf},
};

use datalens_core::{
    BlockRange, ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, Dataset, DatasetId,
    EvmLogFilter, LogFilter, QueryRows, TimeRange,
};
use serde::{Deserialize, Serialize};

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
        dataset: Dataset,
        filter: Option<&LogFilter>,
        range: BlockRange,
    ) -> Result<Vec<BlockRange>, DatalensError> {
        let filter_key = coverage_key(dataset, filter)?;
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
        dataset: Dataset,
        filter: Option<&LogFilter>,
        range: BlockRange,
    ) -> Result<QueryRows, DatalensError> {
        let filter_key = coverage_key(dataset, filter)?;
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
                DatalensError::new(
                    DatalensErrorKind::StorageFailure,
                    format!("read cached object {}: {error}", object.display()),
                )
            })?;
            let mut object_rows: QueryRows = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageFailure,
                    format!("decode cached object {}: {error}", object.display()),
                )
            })?;
            object_rows = filter_rows(object_rows, range);
            rows.append(object_rows);
        }
        rows.sort();
        Ok(rows)
    }

    pub fn write_rows(
        &self,
        dataset: Dataset,
        filter: Option<&LogFilter>,
        range: BlockRange,
        rows: &QueryRows,
        record_empty_coverage: bool,
    ) -> Result<(), DatalensError> {
        if rows.row_count() == 0 && !record_empty_coverage {
            return Ok(());
        }

        fs::create_dir_all(&self.root).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageFailure,
                format!("create storage root {}: {error}", self.root.display()),
            )
        })?;

        let filter_key = coverage_key(dataset, filter)?;
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
                    DatalensError::new(
                        DatalensErrorKind::StorageFailure,
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
                DatalensError::new(
                    DatalensErrorKind::StorageFailure,
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
        self.write_manifest(&manifest)
    }

    fn manifest(&self) -> Result<Manifest, DatalensError> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(Manifest::default());
        }
        let bytes = fs::read(&path).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageFailure,
                format!("read manifest {}: {error}", path.display()),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageFailure,
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
            DatalensError::new(
                DatalensErrorKind::StorageFailure,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ManifestEntry {
    dataset: Dataset,
    filter_key: String,
    range: BlockRange,
    object_key: Option<String>,
    row_count: usize,
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
            missing.push(BlockRange::new(cursor, covered_range.from_block - 1));
        }
        cursor = cursor.max(covered_range.to_block.saturating_add(1));
        if cursor > range.to_block {
            break;
        }
    }
    if cursor <= range.to_block {
        missing.push(BlockRange::new(cursor, range.to_block));
    }
    missing
}

fn coverage_key(dataset: Dataset, filter: Option<&LogFilter>) -> Result<String, DatalensError> {
    Ok(match dataset {
        Dataset::Blocks => "all".to_owned(),
        Dataset::Logs => {
            let Some(filter) = filter else {
                return Ok("evm-logs/addr=*/topics=*".to_owned());
            };
            let filter = EvmLogFilter::try_from(filter)?;
            format!("evm-logs/{}", filter.canonical_key())
        }
    })
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
        Dataset::Blocks => QueryRows::Blocks(Vec::new()),
        Dataset::Logs => QueryRows::Logs(Vec::new()),
    }
}

fn filter_rows(rows: QueryRows, range: BlockRange) -> QueryRows {
    match rows {
        QueryRows::Blocks(rows) => QueryRows::Blocks(
            rows.into_iter()
                .filter(|row| range.contains(row.number))
                .collect(),
        ),
        QueryRows::Logs(rows) => QueryRows::Logs(
            rows.into_iter()
                .filter(|row| range.contains(row.block_number))
                .collect(),
        ),
    }
}
