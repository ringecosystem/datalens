//! Chain-neutral adapter boundary for datalens chain sources.

use datalens_core::{ChainIdentity, DatasetId, ResultEnvelope, TimeRange};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    chain: ChainIdentity,
    datasets: Vec<DatasetId>,
}

impl AdapterCapabilities {
    pub fn new(chain: ChainIdentity) -> Self {
        Self {
            chain,
            datasets: Vec::new(),
        }
    }

    pub fn with_dataset(mut self, dataset: DatasetId) -> Self {
        self.datasets.push(dataset);
        self
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn datasets(&self) -> &[DatasetId] {
        &self.datasets
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResponse<T> {
    pub envelope: ResultEnvelope<T>,
}

pub trait ChainAdapter {
    fn capabilities(&self) -> AdapterCapabilities;
}
