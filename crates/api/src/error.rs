use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datalens_core::Error;
use datalens_core::wire::ErrorBody;

/// One HTTP status per stable error kind.
pub(crate) fn status_of(error: &Error) -> StatusCode {
    match error {
        Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        Error::Forbidden(_) => StatusCode::FORBIDDEN,
        Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
        Error::QuotaExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
        Error::Unsupported(_) => StatusCode::UNPROCESSABLE_ENTITY,
        Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) struct ApiError(pub(crate) Error);

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Error::Internal(message) = &self.0 {
            tracing::error!(%message, "request failed");
        }
        (status_of(&self.0), Json(ErrorBody::from(&self.0))).into_response()
    }
}

pub(crate) fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(body).map_err(|error| Error::InvalidInput(error.to_string()))
}
