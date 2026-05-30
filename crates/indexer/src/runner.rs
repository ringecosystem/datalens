use datalens_client::{DatalensClient, HttpTransport};

use crate::{IndexPlan, IndexerError, OutputSinkConfig};

#[derive(Clone)]
pub struct IndexRunner {
    plan: IndexPlan,
    output: OutputSinkConfig,
}

impl IndexRunner {
    pub fn new(plan: IndexPlan, output: OutputSinkConfig) -> Self {
        Self { plan, output }
    }

    pub fn plan(&self) -> &IndexPlan {
        &self.plan
    }

    pub fn output(&self) -> &OutputSinkConfig {
        &self.output
    }

    pub fn run<T>(&self, _client: &DatalensClient<T>) -> Result<IndexRunReport, IndexerError>
    where
        T: HttpTransport,
    {
        Err(IndexerError::Runner(
            "query execution is not implemented by this skeleton".to_owned(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRunReport {
    pub planned_queries: usize,
}
