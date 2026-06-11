//! Chain-neutral datalens vocabulary shared across workspace crates.

pub mod chain;

pub mod range;

pub mod dataset;

pub mod coverage;

pub mod error;

pub mod result;

pub mod query;

pub mod redaction;

pub use chain::{ChainFamily, ChainIdentity, NetworkId};
pub use coverage::CoverageLevel;
pub use dataset::{Dataset, DatasetId, DatasetKey};
pub use error::{DatalensError, DatalensErrorKind, QuotaErrorKind, QuotaErrorMetadata};
pub use query::{
    BlockHeader, DatasetRows, EvmBlockHeader, EvmLogFilter, EvmReceipt, EvmTransaction, LogFilter,
    LogRecord, QueryRows, QueryStrategy, TopicFilter,
};
pub use range::{BlockRange, LedgerRange, LedgerRangeKind, TimeRange, missing_ranges};
pub use redaction::{redact_url, redact_urls_in_text};
pub use result::{
    QueryDataFinality, QueryFinalityRequirement, QuerySegmentMetadata, QuerySegmentSource,
    ResultEnvelope,
};
