use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexerError {
    Config(String),
    Plan(String),
    Runner(String),
}

impl fmt::Display for IndexerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(formatter, "invalid index config: {message}"),
            Self::Plan(message) => write!(formatter, "invalid index plan: {message}"),
            Self::Runner(message) => write!(formatter, "index runner error: {message}"),
        }
    }
}

impl Error for IndexerError {}
