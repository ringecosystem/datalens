use datalens_core::{BlockRange, ChainFamily};
use serde::{Deserialize, Serialize};

use crate::IndexerError;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatalensIndexConfig {
    pub application: String,
    pub datalens_endpoint: String,
    #[serde(default)]
    pub chains: Vec<IndexChainConfig>,
    #[serde(default)]
    pub queries: Vec<IndexQueryConfig>,
    pub output: crate::OutputSinkConfig,
    pub checkpoint: crate::CheckpointPolicy,
}

impl DatalensIndexConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, IndexerError> {
        toml::from_str(input).map_err(|error| IndexerError::Config(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexChainConfig {
    pub name: String,
    pub family: ChainFamily,
    pub network: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexQueryConfig {
    pub name: String,
    pub chain: String,
    pub dataset: String,
    pub range: BlockRange,
}
