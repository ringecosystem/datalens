use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Stable error categories used by APIs, metrics, retries, and usage ledger
/// records. Variants distinguish caller/auth failures from provider and storage
/// failures so retry and attribution decisions do not parse message text.
pub enum DatalensErrorKind {
    AuthenticationFailed,
    InvalidInput,
    InvalidRequest,
    Unauthorized,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaErrorKind {
    RangeLimit,
    HotRangeLimit,
    RequestRateLimit,
    ConcurrentLimit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotaErrorMetadata {
    pub kind: QuotaErrorKind,
    pub scope: String,
    pub limit: Option<u64>,
    pub requested: Option<u64>,
    pub observed: Option<u64>,
    pub retry_after_seconds: Option<u64>,
}

impl DatalensErrorKind {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ProviderFailure
                | Self::ProviderTimeout
                | Self::RateLimited
                | Self::StorageReadFailure
                | Self::StorageWriteFailure
                | Self::ManifestUpdateFailure
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatalensError {
    pub kind: DatalensErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub quota: Option<Box<QuotaErrorMetadata>>,
}

impl DatalensError {
    pub fn new(kind: DatalensErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            quota: None,
        }
    }

    pub fn with_quota(mut self, quota: QuotaErrorMetadata) -> Self {
        self.quota = Some(Box::new(quota));
        self
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::InvalidInput, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::UnsupportedDataset, message)
    }

    pub fn unsupported_hot_query(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::UnsupportedHotQuery, message)
    }

    pub fn provider_limit(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::ProviderLimit, message)
    }

    pub fn provider_timeout(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::ProviderTimeout, message)
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::RateLimited, message)
    }

    pub fn storage_read(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::StorageReadFailure, message)
    }

    pub fn storage_write(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::StorageWriteFailure, message)
    }

    pub fn manifest_update(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::ManifestUpdateFailure, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::Internal, message)
    }

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

impl fmt::Display for DatalensError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for DatalensError {}
