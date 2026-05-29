//! Stable Rust client contract for datalens.

use std::{collections::BTreeMap, error::Error, fmt};

use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey,
    DatasetRows, LedgerRange, LedgerRangeKind, LogFilter, QueryDataFinality,
    QueryFinalityRequirement, QuerySegmentSource,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

pub const APPLICATION_IDENTITY_HEADER: &str = "x-datalens-application";
const DEFAULT_APPLICATION: &str = "unknown";

mod selectors;
pub use selectors::TronEventSelector;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensClientConfig {
    pub endpoint: String,
    pub application: Option<String>,
}

#[derive(Clone)]
pub struct DatalensClient<T = ReqwestTransport> {
    endpoint: String,
    application: String,
    transport: T,
}

impl DatalensClient<ReqwestTransport> {
    pub fn new(config: DatalensClientConfig) -> Result<Self, ClientError> {
        let transport = ReqwestTransport::new(config.endpoint.clone())?;
        Self::with_transport(config, transport)
    }
}

impl<T> DatalensClient<T>
where
    T: HttpTransport,
{
    pub fn with_transport(config: DatalensClientConfig, transport: T) -> Result<Self, ClientError> {
        let endpoint = config.endpoint.trim().trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(ClientError::InvalidConfig(
                "datalens endpoint must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            application: normalize_application(config.application),
            transport,
        })
    }

    pub fn discover(&self) -> Result<DiscoveryResponse, ClientError> {
        self.send_json("GET", "/v1/discovery", serde_json::Value::Null)
    }

    pub fn query_blocks(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
    ) -> Result<QueryResponse, ClientError> {
        self.query_evm_blocks(chain, range)
    }

    pub fn query_evm_blocks(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
    ) -> Result<QueryResponse, ClientError> {
        validate_evm_chain(&chain, "query_evm_blocks")?;
        self.query(QueryRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: QuerySelector::All,
            range: LedgerRange::from_block_range(range),
            finality: QueryFinalityRequirement::DurableOnly,
            fields: FieldSelection::All,
        })
    }

    pub fn query_blocks_with_options(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
        options: QueryOptions,
    ) -> Result<QueryResponse, ClientError> {
        self.query_evm_blocks_with_options(chain, range, options)
    }

    pub fn query_evm_blocks_with_options(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
        options: QueryOptions,
    ) -> Result<QueryResponse, ClientError> {
        validate_evm_chain(&chain, "query_evm_blocks_with_options")?;
        self.query(QueryRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: QuerySelector::All,
            range: LedgerRange::from_block_range(range),
            finality: options.finality,
            fields: FieldSelection::All,
        })
    }

    pub fn query_logs(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
        filter: LogFilter,
    ) -> Result<QueryResponse, ClientError> {
        self.query_evm_logs(chain, range, filter)
    }

    pub fn query_evm_logs(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
        filter: LogFilter,
    ) -> Result<QueryResponse, ClientError> {
        validate_evm_chain(&chain, "query_evm_logs")?;
        self.query(QueryRequest {
            chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: QuerySelector::EvmLogs(filter),
            range: LedgerRange::from_block_range(range),
            finality: QueryFinalityRequirement::DurableOnly,
            fields: FieldSelection::All,
        })
    }

    pub fn query_blocks_with_fallback(
        &self,
        _chain: ChainIdentity,
        _range: BlockRange,
        mode: FallbackMode,
    ) -> Result<QueryResponse, ClientError> {
        match mode {
            FallbackMode::None => Err(ClientError::UnsupportedFallback),
            FallbackMode::Rpc => Err(ClientError::UnsupportedFallback),
        }
    }

    pub fn query_dataset(
        &self,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: LedgerRange,
        selector: QuerySelector,
    ) -> Result<QueryResponse, ClientError> {
        self.query(QueryRequest::new(chain, dataset_key, range).with_selector(selector))
    }

    pub fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
        self.send_json("POST", "/v1/query", request)
    }

    fn send_json<B, R>(&self, method: &str, path: &str, body: B) -> Result<R, ClientError>
    where
        B: Serialize,
        R: DeserializeOwned,
    {
        let mut headers = BTreeMap::new();
        headers.insert(
            APPLICATION_IDENTITY_HEADER.to_owned(),
            self.application.clone(),
        );
        let request = HttpRequest {
            method: method.to_owned(),
            endpoint: self.endpoint.clone(),
            path: path.to_owned(),
            headers,
            body: serde_json::to_value(body).map_err(|error| {
                ClientError::Encode(format!("encode datalens request body: {error}"))
            })?,
        };
        let response = self.transport.send(request)?;
        decode_response(response)
    }
}

#[derive(Clone)]
pub struct ReqwestTransport {
    endpoint: String,
    client: reqwest::blocking::Client,
}

impl ReqwestTransport {
    pub fn new(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        Ok(Self {
            endpoint: endpoint.into().trim().trim_end_matches('/').to_owned(),
            client: reqwest::blocking::Client::new(),
        })
    }
}

impl HttpTransport for ReqwestTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ClientError> {
        let url = format!("{}{}", self.endpoint, request.path);
        let method = request.method.parse().map_err(|error| {
            ClientError::Transport(format!("parse HTTP method {}: {error}", request.method))
        })?;
        let mut builder = self.client.request(method, url);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if request.body != serde_json::Value::Null {
            builder = builder.json(&request.body);
        }
        let response = builder
            .send()
            .map_err(|error| ClientError::Transport(format!("send datalens request: {error}")))?;
        let status = response.status().as_u16();
        let body = response
            .json()
            .map_err(|error| ClientError::Decode(format!("decode datalens response: {error}")))?;
        Ok(HttpResponse { status, body })
    }
}

pub trait HttpTransport: Clone {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ClientError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRequest {
    pub method: String,
    pub endpoint: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: serde_json::Value,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

impl HttpResponse {
    pub fn json(status: u16, body: serde_json::Value) -> Self {
        Self { status, body }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub chain: ChainIdentity,
    #[serde(
        serialize_with = "serialize_dataset_key",
        deserialize_with = "deserialize_dataset_key"
    )]
    pub dataset_key: DatasetKey,
    pub selector: QuerySelector,
    #[serde(
        serialize_with = "serialize_ledger_range",
        deserialize_with = "deserialize_ledger_range"
    )]
    pub range: LedgerRange,
    pub finality: QueryFinalityRequirement,
    pub fields: FieldSelection,
}

impl QueryRequest {
    pub fn new(chain: ChainIdentity, dataset_key: DatasetKey, range: LedgerRange) -> Self {
        Self {
            chain,
            dataset_key,
            selector: QuerySelector::All,
            range,
            finality: QueryFinalityRequirement::DurableOnly,
            fields: FieldSelection::All,
        }
    }

    pub fn with_chain(mut self, chain: ChainIdentity) -> Self {
        self.chain = chain;
        self
    }

    pub fn with_dataset_key(mut self, dataset_key: DatasetKey) -> Self {
        self.dataset_key = dataset_key;
        self
    }

    pub fn with_selector(mut self, selector: QuerySelector) -> Self {
        self.selector = selector;
        self
    }

    pub fn with_range(mut self, range: LedgerRange) -> Self {
        self.range = range;
        self
    }

    pub fn with_finality(mut self, finality: QueryFinalityRequirement) -> Self {
        self.finality = finality;
        self
    }

    pub fn with_fields(mut self, fields: FieldSelection) -> Self {
        self.fields = fields;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryRange {
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
}

impl QueryRange {
    fn from_ledger_range(range: &LedgerRange) -> Result<Self, DatalensError> {
        let start = range.start();
        let end = range.end();
        match range.kind() {
            LedgerRangeKind::Block => Ok(Self::Block { start, end }),
            LedgerRangeKind::Slot => Ok(Self::Slot { start, end }),
            LedgerRangeKind::Height => Ok(Self::Height { start, end }),
            LedgerRangeKind::Other(kind) => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("ledger range kind {kind} is not supported by the query API"),
            )),
        }
    }

    fn into_ledger_range(self) -> Result<LedgerRange, DatalensError> {
        match self {
            Self::Block { start, end } => LedgerRange::blocks(start, end),
            Self::Slot { start, end } => LedgerRange::slots(start, end),
            Self::Height { start, end } => LedgerRange::heights(start, end),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QuerySelector {
    All,
    EvmLogs(LogFilter),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

impl QuerySelector {
    pub fn other(
        kind: impl Into<String>,
        fingerprint: impl Into<String>,
        canonical_key: impl Into<String>,
    ) -> Self {
        Self::Other {
            kind: kind.into(),
            fingerprint: fingerprint.into(),
            canonical_key: canonical_key.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSelection {
    #[default]
    All,
    Include(Vec<String>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryOptions {
    pub finality: QueryFinalityRequirement,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub chain: ChainIdentity,
    #[serde(
        serialize_with = "serialize_dataset_key",
        deserialize_with = "deserialize_dataset_key"
    )]
    pub dataset_key: DatasetKey,
    #[serde(
        serialize_with = "serialize_ledger_range",
        deserialize_with = "deserialize_ledger_range"
    )]
    pub range: LedgerRange,
    pub cache: CacheSummary,
    pub rows: DatasetRows,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheSummary {
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub hit_ranges: Vec<LedgerRange>,
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub missing_ranges: Vec<LedgerRange>,
    #[serde(default)]
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub durable_hit_ranges: Vec<LedgerRange>,
    #[serde(default)]
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub hot_hit_ranges: Vec<LedgerRange>,
    #[serde(default)]
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub provider_fill_ranges: Vec<LedgerRange>,
    #[serde(default)]
    #[serde(
        serialize_with = "serialize_ledger_ranges",
        deserialize_with = "deserialize_ledger_ranges"
    )]
    pub promotion_pending_ranges: Vec<LedgerRange>,
    #[serde(default)]
    pub segments: Vec<QuerySegment>,
}

impl CacheSummary {
    pub fn outcome(&self) -> CacheOutcome {
        match (self.hit_ranges.is_empty(), self.missing_ranges.is_empty()) {
            (false, true) => CacheOutcome::FullHit,
            (false, false) => CacheOutcome::PartialHit,
            (true, false) => CacheOutcome::Miss,
            (true, true) => CacheOutcome::Empty,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuerySegment {
    #[serde(
        serialize_with = "serialize_ledger_range",
        deserialize_with = "deserialize_ledger_range"
    )]
    pub range: LedgerRange,
    pub source: QuerySegmentSource,
    pub finality: QueryDataFinality,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheOutcome {
    FullHit,
    PartialHit,
    Miss,
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub chains: Vec<ChainDiscovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainDiscovery {
    pub identity: ChainIdentity,
    pub datasets: Vec<Dataset>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallbackMode {
    None,
    Rpc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    InvalidConfig(String),
    InvalidInput(String),
    Encode(String),
    Decode(String),
    Transport(String),
    Api {
        status: u16,
        kind: ApiErrorKind,
        message: String,
    },
    UnsupportedFallback,
}

impl ClientError {
    pub fn api_kind(&self) -> Option<ApiErrorKind> {
        match self {
            Self::Api { kind, .. } => Some(kind.clone()),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub fn is_unsupported_fallback(&self) -> bool {
        matches!(self, Self::UnsupportedFallback)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message)
            | Self::InvalidInput(message)
            | Self::Encode(message)
            | Self::Decode(message)
            | Self::Transport(message) => f.write_str(message),
            Self::Api {
                status,
                kind,
                message,
            } => write!(f, "datalens API error {status} {kind:?}: {message}"),
            Self::UnsupportedFallback => f.write_str("RPC fallback is not supported"),
        }
    }
}

impl Error for ClientError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiErrorKind {
    InvalidInput,
    InvalidRequest,
    UnsupportedDataset,
    UnsupportedHotQuery,
    ProviderFailure,
    ProviderLimit,
    ProviderTimeout,
    RateLimited,
    StorageReadFailure,
    StorageWriteFailure,
    ManifestUpdateFailure,
    Internal,
    Unknown(String),
}

#[derive(Deserialize)]
struct ApiErrorBody {
    error: ApiErrorDetail,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    kind: String,
    message: String,
}

fn validate_evm_chain(chain: &ChainIdentity, helper: &str) -> Result<(), ClientError> {
    if matches!(chain.family_ref(), ChainFamily::Evm) {
        return Ok(());
    }
    Err(ClientError::InvalidInput(format!(
        "{helper} only supports EVM chains; use query_dataset for non-EVM datasets"
    )))
}

fn serialize_dataset_key<S>(dataset_key: &DatasetKey, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(dataset_key.as_str())
}

fn deserialize_dataset_key<'de, D>(deserializer: D) -> Result<DatasetKey, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    DatasetKey::parse(value).map_err(serde::de::Error::custom)
}

fn serialize_ledger_range<S>(range: &LedgerRange, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    QueryRange::from_ledger_range(range)
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

fn deserialize_ledger_range<'de, D>(deserializer: D) -> Result<LedgerRange, D::Error>
where
    D: Deserializer<'de>,
{
    QueryRange::deserialize(deserializer)?
        .into_ledger_range()
        .map_err(serde::de::Error::custom)
}

fn serialize_ledger_ranges<S>(ranges: &[LedgerRange], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ranges
        .iter()
        .map(QueryRange::from_ledger_range)
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

fn deserialize_ledger_ranges<'de, D>(deserializer: D) -> Result<Vec<LedgerRange>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<QueryRange>::deserialize(deserializer)?
        .into_iter()
        .map(QueryRange::into_ledger_range)
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)
}

fn decode_response<R>(response: HttpResponse) -> Result<R, ClientError>
where
    R: DeserializeOwned,
{
    if (200..300).contains(&response.status) {
        serde_json::from_value(response.body)
            .map_err(|error| ClientError::Decode(format!("decode datalens response: {error}")))
    } else {
        let body: ApiErrorBody = serde_json::from_value(response.body).map_err(|error| {
            ClientError::Decode(format!("decode datalens error response: {error}"))
        })?;
        Err(ClientError::Api {
            status: response.status,
            kind: api_error_kind(&body.error.kind),
            message: body.error.message,
        })
    }
}

fn normalize_application(application: Option<String>) -> String {
    application
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|| DEFAULT_APPLICATION.to_owned())
}

fn api_error_kind(kind: &str) -> ApiErrorKind {
    match kind {
        "invalid_input" => ApiErrorKind::InvalidInput,
        "invalid_request" => ApiErrorKind::InvalidRequest,
        "unsupported_dataset" => ApiErrorKind::UnsupportedDataset,
        "unsupported_hot_query" => ApiErrorKind::UnsupportedHotQuery,
        "provider_failure" => ApiErrorKind::ProviderFailure,
        "provider_limit" => ApiErrorKind::ProviderLimit,
        "provider_timeout" => ApiErrorKind::ProviderTimeout,
        "rate_limited" => ApiErrorKind::RateLimited,
        "storage_read_failure" => ApiErrorKind::StorageReadFailure,
        "storage_write_failure" => ApiErrorKind::StorageWriteFailure,
        "manifest_update_failure" => ApiErrorKind::ManifestUpdateFailure,
        "internal" => ApiErrorKind::Internal,
        value => ApiErrorKind::Unknown(value.to_owned()),
    }
}

impl From<DatalensError> for ClientError {
    fn from(error: DatalensError) -> Self {
        Self::Decode(error.to_string())
    }
}
