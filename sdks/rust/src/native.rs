use serde::{Deserialize, Serialize};

use crate::{DatalensClient, Error};

pub struct NativeClient<'a> {
    client: &'a DatalensClient,
}

impl<'a> NativeClient<'a> {
    pub(crate) fn new(client: &'a DatalensClient) -> Self {
        Self { client }
    }

    pub fn discovery(&self) -> Result<Discovery, Error> {
        let data: DiscoveryData = self
            .client
            .execute(DISCOVERY_QUERY, serde_json::json!({}))?;
        Ok(data.discovery)
    }

    pub fn query(&self, input: QueryInput) -> Result<QueryResponse, Error> {
        let data: QueryData = self.client.execute(QUERY_QUERY, QueryVariables { input })?;
        Ok(data.query)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainIdentityInput {
    pub family: ChainFamilyInput,
    pub configured_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_id: Option<NetworkIdInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainFamilyInput {
    pub kind: ChainFamilyKindInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainFamilyKindInput {
    Evm,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkIdInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub textual: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetKeyInput {
    pub family: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryInput {
    pub chain: ChainIdentityInput,
    pub dataset_key: DatasetKeyInput,
    pub selector: QuerySelectorInput,
    pub range: QueryRangeInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<FieldSelectionInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryRangeInput {
    pub kind: QueryRangeKindInput,
    pub start: i32,
    pub end: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryRangeKindInput {
    Block,
    Slot,
    Height,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySelectorInput {
    pub kind: SelectorKindInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evm_logs: Option<EvmLogsSelectorInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other: Option<OtherSelectorInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectorKindInput {
    All,
    EvmLogs,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvmLogsSelectorInput {
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub topics: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtherSelectorInput {
    pub kind: String,
    pub fingerprint: String,
    pub canonical_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldSelectionInput {
    #[serde(default)]
    pub include: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Discovery {
    pub chains: Vec<ChainDiscovery>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ChainDiscovery {
    pub identity: serde_json::Value,
    pub datasets: Vec<DatasetDiscovery>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDiscovery {
    pub dataset_key: String,
    pub range_kinds: serde_json::Value,
    pub selectors: Vec<String>,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub chain: serde_json::Value,
    pub dataset_key: String,
    pub range: serde_json::Value,
    pub cache: serde_json::Value,
    pub rows: serde_json::Value,
}

#[derive(Serialize)]
struct QueryVariables {
    input: QueryInput,
}

#[derive(Deserialize)]
struct DiscoveryData {
    discovery: Discovery,
}

#[derive(Deserialize)]
struct QueryData {
    query: QueryResponse,
}

const DISCOVERY_QUERY: &str = r#"
query {
  discovery {
    chains {
      identity
      datasets { datasetKey rangeKinds selectors enabled }
    }
  }
}
"#;

const QUERY_QUERY: &str = r#"
query($input: QueryInput!) {
  query(input: $input) {
    chain
    datasetKey
    range
    cache
    rows
  }
}
"#;
