//! Chain-neutral datalens vocabulary shared across workspace crates.

pub mod chain {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum ChainFamily {
        Evm,
        Other(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ChainIdentity {
        family: ChainFamily,
        id: String,
    }

    impl ChainIdentity {
        pub fn new(family: ChainFamily, id: impl Into<String>) -> Self {
            Self {
                family,
                id: id.into(),
            }
        }

        pub fn family(&self) -> ChainFamily {
            self.family.clone()
        }

        pub fn id(&self) -> &str {
            &self.id
        }
    }
}

pub mod range {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct TimeRange {
        start: u64,
        end: u64,
    }

    impl TimeRange {
        pub fn blocks(start: u64, end: u64) -> Self {
            Self { start, end }
        }

        pub fn start(&self) -> u64 {
            self.start
        }

        pub fn end(&self) -> u64 {
            self.end
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct BlockRange {
        pub from_block: u64,
        pub to_block: u64,
    }

    impl BlockRange {
        pub fn new(from_block: u64, to_block: u64) -> Self {
            Self {
                from_block,
                to_block,
            }
        }

        pub fn len(&self) -> u64 {
            self.to_block.saturating_sub(self.from_block) + 1
        }

        pub fn is_empty(&self) -> bool {
            self.from_block > self.to_block
        }

        pub fn contains(&self, block_number: u64) -> bool {
            self.from_block <= block_number && block_number <= self.to_block
        }
    }
}

pub mod dataset {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatasetId(String);

    impl DatasetId {
        pub fn new(id: impl Into<String>) -> Self {
            Self(id.into())
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Dataset {
        Blocks,
        Logs,
    }

    impl Dataset {
        pub fn as_str(&self) -> &'static str {
            match self {
                Self::Blocks => "blocks",
                Self::Logs => "logs",
            }
        }
    }
}

pub mod coverage {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum CoverageLevel {
        Covered,
        Partial,
        Missing,
    }
}

pub mod error {
    use std::{error::Error, fmt};

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum DatalensErrorKind {
        InvalidInput,
        InvalidRequest,
        Unsupported,
        ProviderFailure,
        ProviderLimit,
        ProviderTimeout,
        RateLimited,
        Unavailable,
        Persistence,
        StorageFailure,
        Internal,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatalensError {
        pub kind: DatalensErrorKind,
        pub message: String,
    }

    impl DatalensError {
        pub fn new(kind: DatalensErrorKind, message: impl Into<String>) -> Self {
            Self {
                kind,
                message: message.into(),
            }
        }
    }

    impl fmt::Display for DatalensError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:?}: {}", self.kind, self.message)
        }
    }

    impl Error for DatalensError {}
}

pub mod result {
    use serde::{Deserialize, Serialize};

    use crate::{DatasetId, TimeRange};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ResultEnvelope<T> {
        dataset: DatasetId,
        range: TimeRange,
        payload: T,
    }

    impl<T> ResultEnvelope<T> {
        pub fn ok(dataset: DatasetId, range: TimeRange, payload: T) -> Self {
            Self {
                dataset,
                range,
                payload,
            }
        }

        pub fn dataset(&self) -> &DatasetId {
            &self.dataset
        }

        pub fn range(&self) -> &TimeRange {
            &self.range
        }

        pub fn payload(&self) -> &T {
            &self.payload
        }
    }
}

pub mod query {
    use serde::{Deserialize, Serialize};

    use crate::{BlockRange, Dataset};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct BlockHeader {
        pub number: u64,
        pub hash: String,
        pub parent_hash: String,
        pub timestamp: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LogRecord {
        pub block_number: u64,
        pub block_hash: String,
        pub transaction_hash: String,
        pub transaction_index: u64,
        pub log_index: u64,
        pub address: String,
        pub topics: Vec<String>,
        pub data: String,
        pub removed: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LogFilter {
        #[serde(default)]
        pub addresses: Vec<String>,
        #[serde(default)]
        pub topics: Vec<Option<Vec<String>>>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct QueryRequest {
        pub chain: String,
        pub dataset: Dataset,
        pub range: BlockRange,
        pub filter: Option<LogFilter>,
        #[serde(default)]
        pub include_block: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct CacheSummary {
        pub hit_ranges: Vec<BlockRange>,
        pub missing_ranges: Vec<BlockRange>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "dataset", content = "rows", rename_all = "snake_case")]
    pub enum QueryRows {
        Blocks(Vec<BlockHeader>),
        Logs(Vec<LogRecord>),
    }

    impl QueryRows {
        pub fn row_count(&self) -> usize {
            match self {
                Self::Blocks(rows) => rows.len(),
                Self::Logs(rows) => rows.len(),
            }
        }

        pub fn append(&mut self, other: QueryRows) {
            match (self, other) {
                (Self::Blocks(left), Self::Blocks(mut right)) => left.append(&mut right),
                (Self::Logs(left), Self::Logs(mut right)) => left.append(&mut right),
                _ => {}
            }
        }

        pub fn sort(&mut self) {
            match self {
                Self::Blocks(rows) => rows.sort_by_key(|row| row.number),
                Self::Logs(rows) => rows.sort_by_key(|row| (row.block_number, row.log_index)),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct QueryResponse {
        pub chain: String,
        pub range: BlockRange,
        pub cache: CacheSummary,
        pub rows: QueryRows,
    }
}

pub use chain::{ChainFamily, ChainIdentity};
pub use coverage::CoverageLevel;
pub use dataset::{Dataset, DatasetId};
pub use error::{DatalensError, DatalensErrorKind};
pub use query::{
    BlockHeader, CacheSummary, LogFilter, LogRecord, QueryRequest, QueryResponse, QueryRows,
};
pub use range::{BlockRange, TimeRange};
pub use result::ResultEnvelope;
