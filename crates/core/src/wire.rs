//! HTTP request and response bodies. The SDK builds on these, so they stay serde-only.

use alloy_primitives::{Address, B256, Bytes};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{BlockRange, BlockRow, ChainName, Dataset, Error, Finality, LogRow, LogSelector};
use crate::{Heads, QueryFinality};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub chain: ChainName,
    pub dataset: Dataset,
    /// `evm.logs` only. Omitted means any address and any topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LogSelector>,
    pub range: BlockRange,
    #[serde(default)]
    pub finality: QueryFinality,
    /// Omitted means every base field. Enrichment fields are returned only when listed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<PageRequest>,
    /// Longest wait for fills. The server caps it at `query.max_wait_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub chain: ChainName,
    pub dataset: Dataset,
    pub requested: BlockRange,
    /// Contiguous prefix of `requested` starting at `requested.start`. Null when nothing is served.
    pub served: Option<BlockRange>,
    /// First block not served. Null when the whole request is served.
    pub next_start: Option<u64>,
    pub incomplete_reason: Option<IncompleteReason>,
    /// For `provider_error`, the provider error kind of the failed fill.
    #[serde(default)]
    pub incomplete_detail: Option<String>,
    pub segments: Vec<Segment>,
    /// [`LogRecord`] or [`BlockRecord`] objects, projected by `fields`.
    pub rows: Vec<Value>,
    pub page: PageInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteReason {
    Pending,
    ProviderError,
    BudgetExhausted,
    NotFollowedYet,
    /// `latest_only` serves only blocks above safe. The requested blocks at or below safe are
    /// not served, and re-polling will not serve them. `next_start` says whether hot blocks
    /// remain.
    BelowSafe,
}

impl IncompleteReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ProviderError => "provider_error",
            Self::BudgetExhausted => "budget_exhausted",
            Self::NotFollowedYet => "not_followed_yet",
            Self::BelowSafe => "below_safe",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentSource {
    Durable,
    Hot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
    pub source: SegmentSource,
    pub finality: Finality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<Anchor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    pub number: u64,
    pub hash: B256,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageInfo {
    pub next_cursor: Option<String>,
}

/// One `evm.logs` row on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub block_number: u64,
    pub block_hash: B256,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<u64>,
    pub transaction_hash: B256,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub finality: Finality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_from: Option<Address>,
}

impl From<LogRow> for LogRecord {
    fn from(row: LogRow) -> Self {
        Self {
            block_number: row.block_number,
            block_hash: row.block_hash,
            block_timestamp: None,
            transaction_hash: row.transaction_hash,
            transaction_index: row.transaction_index,
            log_index: row.log_index,
            address: row.address,
            topics: row.topics,
            data: row.data,
            finality: row.finality,
            transaction_from: None,
        }
    }
}

/// One `evm.blocks` row on the wire. Header fields beyond the four named ones keep
/// their JSON-RPC names, such as `gasUsed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRecord {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub finality: Finality,
    #[serde(flatten)]
    pub header: Map<String, Value>,
}

impl From<BlockRow> for BlockRecord {
    fn from(row: BlockRow) -> Self {
        let header = match row.header {
            Value::Object(header) => header,
            _ => Map::new(),
        };
        Self {
            number: row.number,
            hash: row.hash,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            finality: row.finality,
            header,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainHeads {
    pub chain: ChainName,
    pub latest: u64,
    pub latest_hash: B256,
    pub safe: u64,
    pub finalized: u64,
}

impl ChainHeads {
    pub fn new(chain: ChainName, heads: Heads) -> Self {
        Self {
            chain,
            latest: heads.latest,
            latest_hash: heads.latest_hash,
            safe: heads.safe,
            finalized: heads.finalized,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadsResponse {
    pub chains: Vec<ChainHeads>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ErrorInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub kind: String,
    pub message: String,
}

impl From<&Error> for ErrorBody {
    fn from(error: &Error) -> Self {
        let message = match error {
            Error::Unauthorized(message)
            | Error::Forbidden(message)
            | Error::InvalidInput(message)
            | Error::QuotaExceeded(message)
            | Error::Unsupported(message)
            | Error::Internal(message) => message.clone(),
        };
        Self {
            error: ErrorInfo {
                kind: error.kind().to_owned(),
                message,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillRequest {
    pub chain: ChainName,
    pub dataset: Dataset,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LogSelector>,
    pub range: BlockRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillResponse {
    pub selector_id: i64,
    /// One job per aligned chunk, in block order. An already active chunk keeps its job id.
    pub jobs: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageSpan {
    pub start: u64,
    pub end: u64,
    pub finality: Finality,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageResponse {
    pub chain: ChainName,
    pub dataset: Dataset,
    pub selector_id: i64,
    /// Coverage rows recorded for this exact selector.
    pub spans: Vec<CoverageSpan>,
    /// Within the requested range, including coverage of wider selectors.
    pub covered: Vec<BlockRange>,
    pub missing: Vec<BlockRange>,
    /// Follow progress of this exact selector. Null when nothing follows it.
    #[serde(default)]
    pub follow: Option<FollowInfo>,
}

/// Where the follower stands for one selector. The follower owns every block from
/// `from_block` on. The durable fill path owns everything below it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowInfo {
    pub from_block: u64,
    /// Highest block fetched into unsafe rows. Null before the first fetch.
    pub head_block: Option<u64>,
    pub head_hash: Option<B256>,
    /// Highest block promoted into durable coverage. Null before the first promotion.
    pub promoted_to: Option<u64>,
}

/// Body of `POST /v1/watch` and `DELETE /v1/watch`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchRequest {
    pub chain: ChainName,
    pub dataset: Dataset,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LogSelector>,
}

/// An explicit subscription lasts `ttl_ms` from this call. Calling again renews it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchResponse {
    pub chain: ChainName,
    pub dataset: Dataset,
    pub selector_id: i64,
    pub ttl_ms: u64,
    pub follow: FollowInfo,
}

/// Body of `POST /v1/admin/follow/resume`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowResumeRequest {
    pub chain: ChainName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowResumeResponse {
    pub chain: ChainName,
    /// Why follow was halted. `None` when it was already running.
    pub halt_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: i64,
    pub chain_id: i32,
    pub dataset: Dataset,
    pub selector_id: i64,
    pub range: BlockRange,
    pub purpose: String,
    pub application_id: String,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobsResponse {
    pub jobs: Vec<JobInfo>,
}
