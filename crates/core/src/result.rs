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
    DurableCache,
    HotCache,
    LiveProvider,
}

impl QuerySegmentSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DurableCache => "durable_cache",
            Self::HotCache => "hot_cache",
            Self::LiveProvider => "live_provider",
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
