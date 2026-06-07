use std::{error::Error as StdError, fmt};

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphqlError {
    pub message: String,
    #[serde(default)]
    pub locations: Vec<serde_json::Value>,
    #[serde(default)]
    pub path: Vec<serde_json::Value>,
    #[serde(default)]
    pub extensions: Option<serde_json::Value>,
}

impl GraphqlError {
    pub fn api_error(&self) -> Option<ApiError> {
        ApiError::from_graphql_error(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiError {
    pub status: Option<u16>,
    pub kind: ApiErrorKind,
    pub message: String,
    pub quota: Option<QuotaErrorMetadata>,
}

impl ApiError {
    pub(crate) fn from_rest_body(status: u16, body: &str) -> Option<Self> {
        let body: RestErrorBody = serde_json::from_str(body).ok()?;
        Some(Self {
            status: Some(status),
            kind: ApiErrorKind::from_wire(&body.error.kind),
            message: body.error.message,
            quota: body.error.quota.map(QuotaErrorMetadata::from_wire),
        })
    }

    fn from_graphql_error(error: &GraphqlError) -> Option<Self> {
        let extensions = error.extensions.as_ref()?;
        let kind = extensions.get("kind")?.as_str()?;
        let status = extensions
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .and_then(|status| u16::try_from(status).ok());
        let quota = extensions
            .get("quota")
            .filter(|quota| !quota.is_null())
            .and_then(|quota| serde_json::from_value::<QuotaErrorMetadataWire>(quota.clone()).ok())
            .map(QuotaErrorMetadata::from_wire);
        Some(Self {
            status,
            kind: ApiErrorKind::from_wire(kind),
            message: error.message.clone(),
            quota,
        })
    }

    pub fn is_retryable(&self) -> bool {
        match self.quota.as_ref().map(|quota| &quota.kind) {
            Some(QuotaErrorKind::RangeLimit | QuotaErrorKind::HotRangeLimit) => false,
            Some(QuotaErrorKind::RequestRateLimit | QuotaErrorKind::ConcurrentLimit) => true,
            Some(QuotaErrorKind::Unknown(_)) => matches!(self.kind, ApiErrorKind::RateLimited),
            None => false,
        }
    }

    pub fn retry_after_seconds(&self) -> Option<u64> {
        self.quota
            .as_ref()
            .and_then(|quota| quota.retry_after_seconds)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiErrorKind {
    AuthenticationFailed,
    InvalidInput,
    InvalidRequest,
    Unauthorized,
    UnsupportedChain,
    UnsupportedDataset,
    UnsupportedHotQuery,
    UnavailableHead,
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

impl ApiErrorKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::AuthenticationFailed => "authentication_failed",
            Self::InvalidInput => "invalid_input",
            Self::InvalidRequest => "invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::UnsupportedChain => "unsupported_chain",
            Self::UnsupportedDataset => "unsupported_dataset",
            Self::UnsupportedHotQuery => "unsupported_hot_query",
            Self::UnavailableHead => "unavailable_head",
            Self::ProviderFailure => "provider_failure",
            Self::ProviderLimit => "provider_limit",
            Self::ProviderTimeout => "provider_timeout",
            Self::RateLimited => "rate_limited",
            Self::StorageReadFailure => "storage_read_failure",
            Self::StorageWriteFailure => "storage_write_failure",
            Self::ManifestUpdateFailure => "manifest_update_failure",
            Self::Internal => "internal",
            Self::Unknown(kind) => kind.as_str(),
        }
    }

    fn from_wire(kind: &str) -> Self {
        match kind {
            "authentication_failed" => Self::AuthenticationFailed,
            "invalid_input" => Self::InvalidInput,
            "invalid_request" => Self::InvalidRequest,
            "unauthorized" => Self::Unauthorized,
            "unsupported_chain" => Self::UnsupportedChain,
            "unsupported_dataset" => Self::UnsupportedDataset,
            "unsupported_hot_query" => Self::UnsupportedHotQuery,
            "unavailable_head" => Self::UnavailableHead,
            "provider_failure" => Self::ProviderFailure,
            "provider_limit" => Self::ProviderLimit,
            "provider_timeout" => Self::ProviderTimeout,
            "rate_limited" => Self::RateLimited,
            "storage_read_failure" => Self::StorageReadFailure,
            "storage_write_failure" => Self::StorageWriteFailure,
            "manifest_update_failure" => Self::ManifestUpdateFailure,
            "internal" => Self::Internal,
            other => Self::Unknown(other.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotaErrorMetadata {
    pub kind: QuotaErrorKind,
    pub scope: String,
    pub limit: Option<u64>,
    pub requested: Option<u64>,
    pub observed: Option<u64>,
    pub retry_after_seconds: Option<u64>,
}

impl QuotaErrorMetadata {
    fn from_wire(wire: QuotaErrorMetadataWire) -> Self {
        Self {
            kind: QuotaErrorKind::from_wire(&wire.kind),
            scope: wire.scope,
            limit: wire.limit,
            requested: wire.requested,
            observed: wire.observed,
            retry_after_seconds: wire.retry_after_seconds,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuotaErrorKind {
    RangeLimit,
    HotRangeLimit,
    RequestRateLimit,
    ConcurrentLimit,
    Unknown(String),
}

impl QuotaErrorKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::RangeLimit => "range_limit",
            Self::HotRangeLimit => "hot_range_limit",
            Self::RequestRateLimit => "request_rate_limit",
            Self::ConcurrentLimit => "concurrent_limit",
            Self::Unknown(kind) => kind.as_str(),
        }
    }

    fn from_wire(kind: &str) -> Self {
        match kind {
            "range_limit" => Self::RangeLimit,
            "hot_range_limit" => Self::HotRangeLimit,
            "request_rate_limit" => Self::RequestRateLimit,
            "concurrent_limit" => Self::ConcurrentLimit,
            other => Self::Unknown(other.to_owned()),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidConfig(String),
    Encode(String),
    Decode(String),
    Safety(String),
    Transport(String),
    Unauthorized { status: u16, body: String },
    HttpStatus { status: u16, body: String },
    Graphql(Vec<GraphqlError>),
}

impl Error {
    pub fn api_error(&self) -> Option<ApiError> {
        match self {
            Self::Unauthorized { status, body } | Self::HttpStatus { status, body } => {
                ApiError::from_rest_body(*status, body)
            }
            Self::Graphql(errors) => errors.iter().find_map(GraphqlError::api_error),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.api_error().is_some_and(|error| error.is_retryable())
    }

    pub fn retry_after_seconds(&self) -> Option<u64> {
        self.api_error()
            .and_then(|error| error.retry_after_seconds())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message)
            | Self::Encode(message)
            | Self::Decode(message)
            | Self::Safety(message)
            | Self::Transport(message) => formatter.write_str(message),
            Self::Unauthorized { status, body } => {
                write!(formatter, "datalens auth error {status}: {body}")
            }
            Self::HttpStatus { status, body } => {
                write!(formatter, "datalens HTTP error {status}: {body}")
            }
            Self::Graphql(errors) => {
                let message = errors
                    .first()
                    .map(|error| error.message.as_str())
                    .unwrap_or("unknown GraphQL error");
                write!(formatter, "datalens GraphQL error: {message}")
            }
        }
    }
}

impl StdError for Error {}

#[derive(Deserialize)]
struct RestErrorBody {
    error: RestErrorDetail,
}

#[derive(Deserialize)]
struct RestErrorDetail {
    kind: String,
    message: String,
    quota: Option<QuotaErrorMetadataWire>,
}

#[derive(Deserialize)]
struct QuotaErrorMetadataWire {
    kind: String,
    scope: String,
    limit: Option<u64>,
    requested: Option<u64>,
    observed: Option<u64>,
    retry_after_seconds: Option<u64>,
}
