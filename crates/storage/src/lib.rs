//! Storage boundary for durable datalens objects and coverage metadata.

use datalens_core::{ChainIdentity, CoverageLevel, DatasetId, TimeRange};

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
