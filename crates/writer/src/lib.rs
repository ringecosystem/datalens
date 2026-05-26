//! Writer boundary for normalized chunk persistence.

use datalens_core::{ChainIdentity, DatalensErrorKind, DatasetId, TimeRange};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

impl WriteRequest {
    pub fn new(chain: ChainIdentity, dataset: DatasetId, range: TimeRange) -> Self {
        Self {
            chain,
            dataset,
            range,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteStatus {
    Persisted,
    Deferred,
}

impl WriteStatus {
    pub fn error_kind(&self) -> DatalensErrorKind {
        match self {
            Self::Persisted => DatalensErrorKind::Internal,
            Self::Deferred => DatalensErrorKind::UnsupportedDataset,
        }
    }
}
