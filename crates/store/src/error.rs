use datalens_core::Error as CoreError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("{0}")]
    NotFound(String),
    #[error("timed out: {0}")]
    Timeout(String),
    #[error("stored row is invalid: {0}")]
    Data(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

impl StoreError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub(crate) fn data(message: impl Into<String>) -> Self {
        Self::Data(message.into())
    }
}

impl From<StoreError> for CoreError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::InvalidInput(message) => Self::InvalidInput(message),
            StoreError::NotFound(message)
            | StoreError::Timeout(message)
            | StoreError::Data(message) => Self::Internal(message),
            StoreError::Sqlx(err) => Self::Internal(err.to_string()),
        }
    }
}
