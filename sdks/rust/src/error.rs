use std::time::Duration;

/// The stable `error.kind` of a datalens error body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApiErrorKind {
    Unauthorized,
    Forbidden,
    InvalidInput,
    QuotaExceeded,
    Unsupported,
    Internal,
    /// A kind this SDK does not know, or `http` for a body that is not a datalens error,
    /// such as a proxy's 502 page.
    Other(String),
}

impl ApiErrorKind {
    pub fn parse(kind: &str) -> Self {
        match kind {
            "unauthorized" => Self::Unauthorized,
            "forbidden" => Self::Forbidden,
            "invalid_input" => Self::InvalidInput,
            "quota_exceeded" => Self::QuotaExceeded,
            "unsupported" => Self::Unsupported,
            "internal" => Self::Internal,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::InvalidInput => "invalid_input",
            Self::QuotaExceeded => "quota_exceeded",
            Self::Unsupported => "unsupported",
            Self::Internal => "internal",
            Self::Other(kind) => kind,
        }
    }
}

impl std::fmt::Display for ApiErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server answered with a non-success status.
    #[error("datalens {status} {kind}: {message}")]
    Api {
        kind: ApiErrorKind,
        message: String,
        status: u16,
        /// From the `Retry-After` header, in seconds.
        retry_after: Option<Duration>,
    },
    /// The request did not complete: connect, timeout, or transport failure.
    #[error("datalens http: {0}")]
    Http(#[from] reqwest::Error),
    /// A success response whose body is not the expected JSON.
    #[error("datalens decode: {0}")]
    Decode(String),
}

impl Error {
    /// Rate limits, server errors, and transport failures. A 429 for a range above
    /// `max_range_blocks` is also retried; shrink the range instead.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Api { status, .. } => *status == 429 || *status >= 500,
            Self::Http(error) => !error.is_builder() && !error.is_redirect(),
            Self::Decode(_) => false,
        }
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn kind(&self) -> Option<&ApiErrorKind> {
        match self {
            Self::Api { kind, .. } => Some(kind),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            Self::Http(error) => error.status().map(|status| status.as_u16()),
            Self::Decode(_) => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
