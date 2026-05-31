use std::{error::Error as StdError, fmt};

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

#[derive(Debug)]
pub enum Error {
    InvalidConfig(String),
    Encode(String),
    Decode(String),
    Transport(String),
    Unauthorized { status: u16, body: String },
    HttpStatus { status: u16, body: String },
    Graphql(Vec<GraphqlError>),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message)
            | Self::Encode(message)
            | Self::Decode(message)
            | Self::Transport(message) => formatter.write_str(message),
            Self::Unauthorized { status, body } => {
                write!(formatter, "datalens GraphQL auth error {status}: {body}")
            }
            Self::HttpStatus { status, body } => {
                write!(formatter, "datalens GraphQL HTTP error {status}: {body}")
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
