use serde::{Deserialize, Serialize};

use crate::{DatalensClient, Error, client::NativeTransport};

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
        match self.client.native_transport() {
            NativeTransport::Rest => {
                let response: RestQueryResponse = self
                    .client
                    .post_json("/v1/query", &RestQueryInput::from(input))?;
                Ok(response.into())
            }
            NativeTransport::Graphql => {
                let data: QueryData = self.client.execute(QUERY_QUERY, QueryVariables { input })?;
                Ok(data.query)
            }
        }
    }

    pub fn chain_head(
        &self,
        chain: impl AsRef<str>,
        finality: Option<ChainHeadFinalityInput>,
    ) -> Result<ChainHeadResponse, Error> {
        match self.client.native_transport() {
            NativeTransport::Rest => {
                let chain = chain.as_ref();
                let finality = finality.map(|finality| finality.as_str());
                let query = finality
                    .as_ref()
                    .map(|finality| vec![("finality", *finality)])
                    .unwrap_or_default();
                self.client
                    .get_json(&["v1", "chains", chain, "head"], &query)
            }
            NativeTransport::Graphql => Err(Error::InvalidConfig(
                "native chain_head requires a REST datalens endpoint; clients created with with_graphql_endpoint cannot call the REST-only chain head API".to_owned(),
            )),
        }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainHeadFinalityInput {
    Latest,
    Safe,
    Finalized,
}

impl ChainHeadFinalityInput {
    fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }
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

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ChainHeadResponse {
    pub chain: serde_json::Value,
    pub height: u64,
    pub finality: String,
    pub range_kind: String,
    pub timestamp: Option<u64>,
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

#[derive(Serialize)]
struct RestQueryInput {
    chain: ChainIdentityInput,
    dataset_key: String,
    selector: RestQuerySelectorInput,
    range: RestQueryRangeInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    finality: Option<String>,
    fields: RestFieldSelectionInput,
}

impl From<QueryInput> for RestQueryInput {
    fn from(input: QueryInput) -> Self {
        Self {
            chain: input.chain,
            dataset_key: format!("{}.{}", input.dataset_key.family, input.dataset_key.name),
            selector: RestQuerySelectorInput::from(input.selector),
            range: RestQueryRangeInput::from(input.range),
            finality: input.finality,
            fields: input.fields.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum RestQuerySelectorInput {
    All,
    EvmLogs(EvmLogsSelectorInput),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

impl From<QuerySelectorInput> for RestQuerySelectorInput {
    fn from(input: QuerySelectorInput) -> Self {
        match input.kind {
            SelectorKindInput::All => Self::All,
            SelectorKindInput::EvmLogs => {
                Self::EvmLogs(input.evm_logs.unwrap_or_else(|| EvmLogsSelectorInput {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                }))
            }
            SelectorKindInput::Other => {
                let other = input.other.unwrap_or_else(|| OtherSelectorInput {
                    kind: "other".to_owned(),
                    fingerprint: String::new(),
                    canonical_key: String::new(),
                });
                Self::Other {
                    kind: other.kind,
                    fingerprint: other.fingerprint,
                    canonical_key: other.canonical_key,
                }
            }
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RestQueryRangeInput {
    Block { start: i32, end: i32 },
    Slot { start: i32, end: i32 },
    Height { start: i32, end: i32 },
}

impl From<QueryRangeInput> for RestQueryRangeInput {
    fn from(input: QueryRangeInput) -> Self {
        match input.kind {
            QueryRangeKindInput::Block => Self::Block {
                start: input.start,
                end: input.end,
            },
            QueryRangeKindInput::Slot => Self::Slot {
                start: input.start,
                end: input.end,
            },
            QueryRangeKindInput::Height => Self::Height {
                start: input.start,
                end: input.end,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum RestFieldSelectionInput {
    All(String),
    Include(FieldSelectionInput),
}

impl From<Option<FieldSelectionInput>> for RestFieldSelectionInput {
    fn from(fields: Option<FieldSelectionInput>) -> Self {
        fields.map_or_else(|| Self::All("all".to_owned()), Self::Include)
    }
}

#[derive(Deserialize)]
struct RestQueryResponse {
    chain: serde_json::Value,
    dataset_key: String,
    range: serde_json::Value,
    cache: serde_json::Value,
    rows: serde_json::Value,
}

impl From<RestQueryResponse> for QueryResponse {
    fn from(response: RestQueryResponse) -> Self {
        Self {
            chain: response.chain,
            dataset_key: response.dataset_key,
            range: response.range,
            cache: response.cache,
            rows: response.rows,
        }
    }
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
