use axum::http::StatusCode;
use datalens_core::{DatalensError, DatalensErrorKind, QuotaErrorMetadata};
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApiErrorBody {
    pub error: ApiErrorDetail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApiErrorDetail {
    pub kind: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<Box<QuotaErrorMetadata>>,
}

pub fn api_error_status(kind: &DatalensErrorKind) -> StatusCode {
    match kind {
        DatalensErrorKind::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        DatalensErrorKind::InvalidInput | DatalensErrorKind::InvalidRequest => {
            StatusCode::BAD_REQUEST
        }
        DatalensErrorKind::Unauthorized => StatusCode::FORBIDDEN,
        DatalensErrorKind::UnsupportedDataset | DatalensErrorKind::UnsupportedHotQuery => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        DatalensErrorKind::ProviderLimit | DatalensErrorKind::RateLimited => {
            StatusCode::TOO_MANY_REQUESTS
        }
        DatalensErrorKind::ProviderTimeout => StatusCode::GATEWAY_TIMEOUT,
        DatalensErrorKind::ProviderFailure => StatusCode::BAD_GATEWAY,
        DatalensErrorKind::StorageReadFailure
        | DatalensErrorKind::StorageWriteFailure
        | DatalensErrorKind::ManifestUpdateFailure
        | DatalensErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn api_error_body(error: DatalensError) -> ApiErrorBody {
    ApiErrorBody {
        error: ApiErrorDetail {
            kind: api_error_kind(&error.kind),
            message: error.message,
            quota: error.quota,
        },
    }
}

pub fn api_retry_after_seconds(error: &DatalensError) -> Option<u64> {
    error
        .quota
        .as_ref()
        .and_then(|quota| quota.retry_after_seconds)
}

pub fn api_error_kind(kind: &DatalensErrorKind) -> &'static str {
    match kind {
        DatalensErrorKind::AuthenticationFailed => "authentication_failed",
        DatalensErrorKind::InvalidInput => "invalid_input",
        DatalensErrorKind::InvalidRequest => "invalid_request",
        DatalensErrorKind::Unauthorized => "unauthorized",
        DatalensErrorKind::UnsupportedDataset => "unsupported_dataset",
        DatalensErrorKind::UnsupportedHotQuery => "unsupported_hot_query",
        DatalensErrorKind::ProviderFailure => "provider_failure",
        DatalensErrorKind::ProviderLimit => "provider_limit",
        DatalensErrorKind::ProviderTimeout => "provider_timeout",
        DatalensErrorKind::RateLimited => "rate_limited",
        DatalensErrorKind::StorageReadFailure => "storage_read_failure",
        DatalensErrorKind::StorageWriteFailure => "storage_write_failure",
        DatalensErrorKind::ManifestUpdateFailure => "manifest_update_failure",
        DatalensErrorKind::Internal => "internal",
    }
}
