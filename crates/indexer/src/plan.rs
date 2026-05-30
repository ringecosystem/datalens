use datalens_client::QueryRequest;

use crate::{DatalensIndexConfig, IndexerError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexPlan {
    application: String,
    queries: Vec<PlannedIndexQuery>,
}

impl IndexPlan {
    pub fn empty(application: impl Into<String>) -> Self {
        Self {
            application: application.into(),
            queries: Vec::new(),
        }
    }

    pub fn application(&self) -> &str {
        &self.application
    }

    pub fn queries(&self) -> &[PlannedIndexQuery] {
        &self.queries
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedIndexQuery {
    pub name: String,
    pub request: QueryRequest,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexPlanBuilder;

impl IndexPlanBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&self, config: &DatalensIndexConfig) -> Result<IndexPlan, IndexerError> {
        Ok(IndexPlan::empty(config.client.application.clone()))
    }
}
