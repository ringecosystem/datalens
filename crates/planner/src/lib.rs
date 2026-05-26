//! Query planning boundary for native datalens requests.

use datalens_core::{ChainIdentity, CoverageLevel, DatasetId, TimeRange};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

impl PlanRequest {
    pub fn new(chain: ChainIdentity, dataset: DatasetId, range: TimeRange) -> Self {
        Self {
            chain,
            dataset,
            range,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanStatus {
    Covered,
    Partial,
    Missing,
}

impl PlanStatus {
    pub fn coverage_level(&self) -> CoverageLevel {
        match self {
            Self::Covered => CoverageLevel::Covered,
            Self::Partial => CoverageLevel::Partial,
            Self::Missing => CoverageLevel::Missing,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanOutput {
    pub status: PlanStatus,
    pub required_datasets: Vec<DatasetId>,
}
