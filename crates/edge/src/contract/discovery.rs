use datalens_core::{ChainIdentity, Dataset};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub chains: Vec<ChainDiscovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainDiscovery {
    pub identity: ChainIdentity,
    pub datasets: Vec<Dataset>,
}
