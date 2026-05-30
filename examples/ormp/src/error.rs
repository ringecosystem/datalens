use std::{error::Error, fmt};

#[derive(Debug)]
pub enum OrmpExampleError {
    MissingEnv(&'static str),
    InvalidEnv { name: &'static str, message: String },
    InvalidResponse(String),
    Datalens(datalens_core::DatalensError),
}

impl fmt::Display for OrmpExampleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnv(name) => write!(f, "missing required environment variable {name}"),
            Self::InvalidEnv { name, message } => {
                write!(f, "invalid environment variable {name}: {message}")
            }
            Self::InvalidResponse(message) => f.write_str(message),
            Self::Datalens(error) => write!(f, "{error}"),
        }
    }
}

impl Error for OrmpExampleError {}

impl From<datalens_core::DatalensError> for OrmpExampleError {
    fn from(error: datalens_core::DatalensError) -> Self {
        Self::Datalens(error)
    }
}
