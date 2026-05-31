use std::{error, fmt, io};

#[derive(Debug)]
pub enum AppError {
    Config(String),
    Database(rusqlite::Error),
    Io(io::Error),
    Json(serde_json::Error),
    Datalens(datalens_sdk::Error),
    Handler(String),
}

pub type AppResult<T> = Result<T, AppError>;

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "{message}"),
            Self::Database(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
            Self::Datalens(error) => write!(f, "{error}"),
            Self::Handler(message) => write!(f, "{message}"),
        }
    }
}

impl error::Error for AppError {}

impl From<rusqlite::Error> for AppError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<io::Error> for AppError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<datalens_sdk::Error> for AppError {
    fn from(error: datalens_sdk::Error) -> Self {
        Self::Datalens(error)
    }
}
