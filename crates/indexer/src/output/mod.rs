use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSinkConfig {
    StdoutJson,
    FileJson { path: PathBuf },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRecord {
    pub query: String,
    pub payload: serde_json::Value,
}
