use serde::{Deserialize, Deserializer, Serialize, de::Error};

use crate::{BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, EvmLogFilter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CoverageLevel {
    Covered,
    Partial,
    Missing,
}

pub const COVERAGE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoverageKey {
    chain: ChainIdentity,
    dataset: Dataset,
    schema_version: u16,
    coverage: CoverageShape,
}

impl<'de> Deserialize<'de> for CoverageKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawCoverageKey {
            chain: ChainIdentity,
            dataset: Dataset,
            schema_version: u16,
            coverage: CoverageShape,
        }

        let raw = RawCoverageKey::deserialize(deserializer)?;
        if raw.schema_version != COVERAGE_SCHEMA_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported coverage schema_version {}; only {COVERAGE_SCHEMA_VERSION} is supported",
                raw.schema_version
            )));
        }
        Ok(Self {
            chain: raw.chain,
            dataset: raw.dataset,
            schema_version: raw.schema_version,
            coverage: raw.coverage,
        })
    }
}

impl CoverageKey {
    pub fn full_blocks(chain: ChainIdentity) -> Self {
        Self {
            chain,
            dataset: Dataset::Blocks,
            schema_version: COVERAGE_SCHEMA_VERSION,
            coverage: CoverageShape::All,
        }
    }

    pub fn evm_logs(chain: ChainIdentity, filter: EvmLogFilter) -> Self {
        Self {
            chain,
            dataset: Dataset::Logs,
            schema_version: COVERAGE_SCHEMA_VERSION,
            coverage: CoverageShape::EvmLogs(filter),
        }
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn dataset(&self) -> Dataset {
        self.dataset
    }

    pub fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn coverage_key(&self) -> String {
        match &self.coverage {
            CoverageShape::All => "all".to_owned(),
            CoverageShape::EvmLogs(filter) => format!("evm-logs/{}", filter.compact_key()),
        }
    }

    pub fn canonical_coverage_key(&self) -> String {
        match &self.coverage {
            CoverageShape::All => "all".to_owned(),
            CoverageShape::EvmLogs(filter) => format!("evm-logs/{}", filter.canonical_key()),
        }
    }

    pub fn object_prefix(&self) -> String {
        format!(
            "chains/{}/datasets/{}/v{}/{}",
            self.chain.key_prefix(),
            self.dataset.as_str(),
            self.schema_version,
            self.coverage_key()
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", content = "filter", rename_all = "snake_case")]
pub enum CoverageShape {
    All,
    EvmLogs(EvmLogFilter),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageValue {
    DataObject,
    Empty,
}

#[derive(Deserialize)]
struct RawCoverageRecord {
    key: CoverageKey,
    range: BlockRange,
    row_count: usize,
    object_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawCoverageRecord")]
pub struct CoverageRecord {
    key: CoverageKey,
    range: BlockRange,
    row_count: usize,
    object_key: Option<String>,
}

impl TryFrom<RawCoverageRecord> for CoverageRecord {
    type Error = DatalensError;

    fn try_from(raw: RawCoverageRecord) -> Result<Self, Self::Error> {
        Self::try_from_parts(raw.key, raw.range, raw.row_count, raw.object_key)
    }
}

impl CoverageRecord {
    pub fn try_data_object(
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: impl Into<String>,
    ) -> Result<Self, DatalensError> {
        Self::try_from_parts(key, range, row_count, Some(object_key.into()))
    }

    pub fn try_empty(
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: Option<String>,
    ) -> Result<Self, DatalensError> {
        Self::try_from_parts(key, range, row_count, object_key)
    }

    fn try_from_parts(
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: Option<String>,
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
                    key,
                    range,
                    row_count,
                    object_key: Some(object_key),
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
                    key,
                    range,
                    row_count,
                    object_key: None,
                })
            }
        }
    }

    pub fn key(&self) -> &CoverageKey {
        &self.key
    }

    pub fn range(&self) -> BlockRange {
        self.range
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn object_key(&self) -> Option<&str> {
        self.object_key.as_deref()
    }

    pub fn value(&self) -> CoverageValue {
        if self.row_count > 0 && self.object_key.is_some() {
            CoverageValue::DataObject
        } else {
            CoverageValue::Empty
        }
    }

    /// First-stage coverage matching is exact by key; broader filters do not satisfy narrower filters.
    pub fn covers(&self, key: &CoverageKey, range: &BlockRange) -> bool {
        &self.key == key
            && self.range.from_block <= range.from_block
            && range.to_block <= self.range.to_block
    }
}
