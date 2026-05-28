use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DatalensErrorKind {
    AuthenticationFailed,
    InvalidInput,
    InvalidRequest,
    Unauthorized,
    UnsupportedDataset,
    ProviderFailure,
    ProviderLimit,
    ProviderTimeout,
    RateLimited,
    StorageReadFailure,
    StorageWriteFailure,
    ManifestUpdateFailure,
    Internal,
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
}

impl DatalensError {
    pub fn new(kind: DatalensErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::InvalidInput, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(DatalensErrorKind::UnsupportedDataset, message)
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
