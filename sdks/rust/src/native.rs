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
        if let Some(finality) = input.finality.as_deref()
            && is_provisional_query_finality(finality)
        {
            return Err(Error::Safety(format!(
                "native query finality {finality} requires query_provisional explicit opt-in"
            )));
        }
        self.query_inner(input, true)
    }

    pub fn query_provisional(&self, input: QueryInput) -> Result<QueryResponse, Error> {
        let Some(finality) = input.finality.as_deref() else {
            return Err(Error::Safety(
                "native provisional query requires explicit finality".to_owned(),
            ));
        };
        if !is_provisional_query_finality(finality) {
            return Err(Error::Safety(format!(
                "native provisional query finality {finality} is not provisional"
            )));
        }
        self.query_inner(input, false)
    }

    fn query_inner(
        &self,
        input: QueryInput,
        enforce_safe_query_finality: bool,
    ) -> Result<QueryResponse, Error> {
        let mut input = input;
        let enforce_durable_response = input
            .finality
            .as_deref()
            .is_none_or(|finality| finality == DEFAULT_QUERY_FINALITY);
        if enforce_safe_query_finality && input.finality.is_none() {
            if matches!(self.client.native_transport(), NativeTransport::Rest) {
                self.ensure_range_within_default_durable_head(&input)?;
            }
            input.finality = Some(DEFAULT_QUERY_FINALITY.to_owned());
        }

        match self.client.native_transport() {
            NativeTransport::Rest => {
                let request = RestQueryInput::try_from(input)?;
                let response: RestQueryResponse = self.client.post_json("/v1/query", &request)?;
                let response = response.into();
                if enforce_durable_response {
                    ensure_durable_response(&response)?;
                }
                Ok(response)
            }
            NativeTransport::Graphql => {
                let data: QueryData = self.client.execute(QUERY_QUERY, QueryVariables { input })?;
                if enforce_durable_response {
                    ensure_durable_response(&data.query)?;
                }
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

    pub fn latest_head(&self, chain: impl AsRef<str>) -> Result<ChainHeadResponse, Error> {
        self.chain_head(chain, Some(ChainHeadFinalityInput::Latest))
    }

    pub fn safe_head(&self, chain: impl AsRef<str>) -> Result<ChainHeadResponse, Error> {
        self.chain_head(chain, Some(ChainHeadFinalityInput::Safe))
    }

    pub fn finalized_head(&self, chain: impl AsRef<str>) -> Result<ChainHeadResponse, Error> {
        self.chain_head(chain, Some(ChainHeadFinalityInput::Finalized))
    }

    fn ensure_range_within_default_durable_head(&self, input: &QueryInput) -> Result<(), Error> {
        let head = match self.finalized_head(&input.chain.configured_name) {
            Ok(head) => head,
            Err(error) if should_fallback_to_safe_head(&error) => {
                self.safe_head(&input.chain.configured_name)?
            }
            Err(error) => return Err(error),
        };
        if !is_durable_finality(&head.finality) {
            return Err(Error::Safety(format!(
                "datalens durable head returned non-durable finality {}",
                head.finality
            )));
        }
        let range_kind = input.range.kind.as_str();
        if head.range_kind != range_kind {
            return Err(Error::Safety(format!(
                "datalens finalized head range kind {} does not match query range kind {range_kind}",
                head.range_kind
            )));
        }
        if input.range.end > head.height {
            return Err(Error::Safety(format!(
                "query range end {} exceeds {} head {} for chain {}",
                input.range.end, head.finality, head.height, input.chain.configured_name
            )));
        }
        Ok(())
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
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryRangeKindInput {
    Block,
    Slot,
    Height,
}

impl QueryRangeKindInput {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Slot => "slot",
            Self::Height => "height",
        }
    }
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

const DEFAULT_QUERY_FINALITY: &str = "durable_only";

fn ensure_durable_response(response: &QueryResponse) -> Result<(), Error> {
    let segments = response
        .cache
        .get("segments")
        .and_then(serde_json::Value::as_array);
    for segment in segments.into_iter().flatten() {
        let source = segment
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("<missing>");
        let finality = segment
            .get("finality")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("<missing>");
        if !is_durable_finality(finality) {
            return Err(Error::Safety(format!(
                "durable query returned non-durable segment with source {source} and finality {finality}"
            )));
        }
    }
    if has_nonempty_array(&response.cache, "hot_hit_ranges")
        || has_nonempty_array(&response.cache, "hotHitRanges")
    {
        return Err(Error::Safety(
            "durable query returned hot cache ranges".to_owned(),
        ));
    }
    Ok(())
}

fn has_nonempty_array(value: &serde_json::Value, key: &str) -> bool {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|items| !items.is_empty())
}

fn is_durable_finality(finality: &str) -> bool {
    matches!(finality, "safe" | "finalized")
}

fn is_provisional_query_finality(finality: &str) -> bool {
    matches!(finality, "latest_only" | "safe_to_latest")
}

fn should_fallback_to_safe_head(error: &Error) -> bool {
    let Some(api_error) = error.api_error() else {
        return false;
    };
    match api_error.kind {
        crate::ApiErrorKind::UnavailableHead => true,
        crate::ApiErrorKind::InvalidInput => {
            let message = api_error.message.to_ascii_lowercase();
            message.contains("finalized")
                && (message.contains("unavailable")
                    || message.contains("unsupported")
                    || message.contains("not supported"))
        }
        _ => false,
    }
}

#[derive(Serialize)]
struct RestQueryInput {
    chain: RestChainIdentityInput,
    dataset_key: String,
    selector: RestQuerySelectorInput,
    range: RestQueryRangeInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    finality: Option<String>,
    fields: RestFieldSelectionInput,
}

impl TryFrom<QueryInput> for RestQueryInput {
    type Error = Error;

    fn try_from(input: QueryInput) -> Result<Self, Self::Error> {
        Ok(Self {
            chain: RestChainIdentityInput::try_from(input.chain)?,
            dataset_key: format!("{}.{}", input.dataset_key.family, input.dataset_key.name),
            selector: RestQuerySelectorInput::from(input.selector),
            range: RestQueryRangeInput::from(input.range),
            finality: input.finality,
            fields: input.fields.into(),
        })
    }
}

#[derive(Serialize)]
struct RestChainIdentityInput {
    family: RestChainFamilyInput,
    configured_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    network_id: Option<RestNetworkIdInput>,
}

impl TryFrom<ChainIdentityInput> for RestChainIdentityInput {
    type Error = Error;

    fn try_from(input: ChainIdentityInput) -> Result<Self, Self::Error> {
        Ok(Self {
            family: RestChainFamilyInput::try_from(input.family)?,
            configured_name: input.configured_name,
            network_id: input
                .network_id
                .map(RestNetworkIdInput::try_from)
                .transpose()?,
        })
    }
}

#[derive(Serialize)]
enum RestChainFamilyInput {
    Evm,
    Other(String),
}

impl TryFrom<ChainFamilyInput> for RestChainFamilyInput {
    type Error = Error;

    fn try_from(input: ChainFamilyInput) -> Result<Self, Self::Error> {
        match input.kind {
            ChainFamilyKindInput::Evm => Ok(Self::Evm),
            ChainFamilyKindInput::Other => {
                let other = input.other.ok_or_else(|| {
                    Error::Encode("chain family other value is required".to_owned())
                })?;
                Ok(Self::Other(other))
            }
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum RestNetworkIdInput {
    Numeric(i32),
    Textual(String),
}

impl TryFrom<NetworkIdInput> for RestNetworkIdInput {
    type Error = Error;

    fn try_from(input: NetworkIdInput) -> Result<Self, Self::Error> {
        match (input.numeric, input.textual) {
            (Some(value), None) => Ok(Self::Numeric(value)),
            (None, Some(value)) => Ok(Self::Textual(value)),
            (None, None) => Err(Error::Encode(
                "network id numeric or textual value is required".to_owned(),
            )),
            (Some(_), Some(_)) => Err(Error::Encode(
                "network id must provide either numeric or textual, not both".to_owned(),
            )),
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
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
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
