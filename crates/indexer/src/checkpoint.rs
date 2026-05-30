use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointPolicy {
    Disabled,
    File { path: PathBuf },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub query: String,
    pub last_completed_block: u64,
}
