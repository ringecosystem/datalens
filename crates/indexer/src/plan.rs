use std::collections::BTreeMap;

use datalens_client::QueryRequest;
use datalens_core::{
    ChainIdentity, DatalensError, DatasetKey, LedgerRange, NetworkId, QueryFinalityRequirement,
};

use crate::{DatalensIndexConfig, IndexChainConfig, IndexQueryConfig, IndexerError};

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
        let chains = configured_chains(&config.chains)?;
        let mut queries = Vec::with_capacity(config.queries.len());
        for query in &config.queries {
            queries.push(plan_query(query, &chains)?);
        }
        Ok(IndexPlan {
            application: non_empty("application", &config.application)?,
            queries,
        })
    }
}

fn configured_chains(
    chains: &[IndexChainConfig],
) -> Result<BTreeMap<String, ChainIdentity>, IndexerError> {
    let mut configured = BTreeMap::new();
    for chain in chains {
        let name = non_empty("chain name", &chain.name)?;
        let network = parse_network_id(&chain.network)?;
        let identity = ChainIdentity::try_new(chain.family.clone(), name.clone(), Some(network))
            .map_err(from_datalens_error)?;
        configured.insert(name, identity);
    }
    Ok(configured)
}

fn plan_query(
    query: &IndexQueryConfig,
    chains: &BTreeMap<String, ChainIdentity>,
) -> Result<PlannedIndexQuery, IndexerError> {
    let chain = chains
        .get(&query.chain)
        .cloned()
        .ok_or_else(|| IndexerError::Plan(format!("unknown query chain {}", query.chain)))?;
    let dataset_key = DatasetKey::parse(&query.dataset).map_err(from_datalens_error)?;
    let request = QueryRequest::new(
        chain,
        dataset_key,
        LedgerRange::from_block_range(query.range),
    )
    .with_finality(QueryFinalityRequirement::DurableOnly);
    Ok(PlannedIndexQuery {
        name: non_empty("query name", &query.name)?,
        request,
    })
}

fn parse_network_id(value: &str) -> Result<NetworkId, IndexerError> {
    let value = non_empty("network", value)?;
    match value.parse::<u64>() {
        Ok(value) => Ok(NetworkId::Numeric(value)),
        Err(_) => NetworkId::textual(value).map_err(from_datalens_error),
    }
}

fn non_empty(kind: &str, value: &str) -> Result<String, IndexerError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(IndexerError::Plan(format!("{kind} must not be empty")));
    }
    Ok(value.to_owned())
}

fn from_datalens_error(error: DatalensError) -> IndexerError {
    IndexerError::Plan(error.to_string())
}
