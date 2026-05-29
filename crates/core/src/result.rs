use serde::{Deserialize, Serialize};

use crate::{DatasetId, LedgerRange, TimeRange};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResultEnvelope<T> {
    dataset: DatasetId,
    range: TimeRange,
    payload: T,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuerySegmentSource {
    #[serde(alias = "durable_cache")]
    Durable,
    #[serde(alias = "hot_cache")]
    Hot,
    #[serde(alias = "live_provider")]
    Provider,
}

impl QuerySegmentSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Hot => "hot",
            Self::Provider => "provider",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryDataFinality {
    Finalized,
    Safe,
    Unsafe,
    Latest,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Caller-selected finality policy for native queries. `DurableOnly` may read
/// and fill only safe/finalized data, while hot policies can return provider
/// data that must not be persisted into durable coverage unless a plan marks a
/// segment cache-write safe.
pub enum QueryFinalityRequirement {
    #[default]
    DurableOnly,
    SafeToLatest,
    LatestOnly,
}

impl QueryFinalityRequirement {
    pub fn allows_hot(&self) -> bool {
        !matches!(self, Self::DurableOnly)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DurableOnly => "durable_only",
            Self::SafeToLatest => "safe_to_latest",
            Self::LatestOnly => "latest_only",
        }
    }
}

impl QueryDataFinality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Safe => "safe",
            Self::Unsafe => "unsafe",
            Self::Latest => "latest",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Per-range provenance returned with query coverage summaries. Consumers can
/// rely on source and finality being segment-local because mixed queries may
/// combine durable cache, hot read-through, and provider-filled ranges.
pub struct QuerySegmentMetadata {
    pub range: LedgerRange,
    pub source: QuerySegmentSource,
    pub finality: QueryDataFinality,
}

impl QuerySegmentMetadata {
    pub fn new(
        range: LedgerRange,
        source: QuerySegmentSource,
        finality: QueryDataFinality,
    ) -> Self {
        Self {
            range,
            source,
            finality,
        }
    }
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
