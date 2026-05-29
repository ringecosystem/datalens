use datalens_core::{ChainIdentity, LedgerRangeKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub chains: Vec<ChainDiscovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainDiscovery {
    pub identity: ChainIdentity,
    pub datasets: Vec<DatasetDiscovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetDiscovery {
    pub dataset_key: String,
    pub range_kinds: Vec<LedgerRangeKind>,
    pub selectors: Vec<String>,
    pub enabled: bool,
}
