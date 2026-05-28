//! Stable Rust client contract for datalens.

use std::{collections::BTreeMap, error::Error, fmt};

use datalens_core::{BlockRange, ChainIdentity, DatalensError, Dataset, LogFilter, QueryRows};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const APPLICATION_IDENTITY_HEADER: &str = "x-datalens-application";
const DEFAULT_APPLICATION: &str = "unknown";

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
        self.query(QueryRequest {
            chain,
            dataset: Dataset::Blocks,
            range,
            filter: None,
            include_block: false,
        })
    }

    pub fn query_logs(
        &self,
        chain: ChainIdentity,
        range: BlockRange,
        filter: LogFilter,
    ) -> Result<QueryResponse, ClientError> {
        self.query(QueryRequest {
            chain,
            dataset: Dataset::Logs,
            range,
            filter: Some(filter),
            include_block: false,
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

    fn query(&self, request: QueryRequest) -> Result<QueryResponse, ClientError> {
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
    pub dataset: Dataset,
    pub range: BlockRange,
    pub filter: Option<LogFilter>,
    pub include_block: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub chain: ChainIdentity,
    pub range: BlockRange,
    pub cache: CacheSummary,
    pub rows: QueryRows,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheSummary {
    pub hit_ranges: Vec<BlockRange>,
    pub missing_ranges: Vec<BlockRange>,
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
