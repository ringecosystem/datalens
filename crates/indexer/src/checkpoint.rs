use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{IndexerError, PlannedIndexTask};

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexCheckpointFile {
    version: u32,
    entries: BTreeMap<String, IndexCheckpointEntry>,
}

impl IndexCheckpointFile {
    pub(crate) fn empty() -> Self {
        Self {
            version: 1,
            entries: BTreeMap::new(),
        }
    }

    pub fn last_completed_block(&self, task: &PlannedIndexTask) -> Option<u64> {
        self.entries
            .get(&checkpoint_key(task))
            .map(|entry| entry.last_completed_block)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexCheckpointEntry {
    pub index: String,
    pub family: String,
    pub chain: String,
    pub chain_id: u64,
    pub dataset: String,
    pub range_kind: String,
    pub selector_fingerprint: String,
    pub last_completed_block: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCheckpointFileStore {
    path: PathBuf,
}

impl IndexCheckpointFileStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<IndexCheckpointFile, IndexerError> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                IndexerError::Runner(format!(
                    "checkpoint {} is corrupt or unreadable: {error}",
                    self.path.display()
                ))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(IndexCheckpointFile::empty())
            }
            Err(error) => Err(IndexerError::Runner(format!(
                "failed to read checkpoint {}: {error}",
                self.path.display()
            ))),
        }
    }

    pub fn advance(
        &self,
        task: &PlannedIndexTask,
        last_completed_block: u64,
    ) -> Result<(), IndexerError> {
        let mut checkpoint = self.load()?;
        checkpoint.entries.insert(
            checkpoint_key(task),
            IndexCheckpointEntry {
                index: task.index.clone(),
                family: task.family.clone(),
                chain: task.chain.clone(),
                chain_id: task.chain_id,
                dataset: task.dataset.clone(),
                range_kind: task.range.kind.clone(),
                selector_fingerprint: selector_fingerprint(task),
                last_completed_block,
            },
        );
        self.save(&checkpoint)
    }

    fn save(&self, checkpoint: &IndexCheckpointFile) -> Result<(), IndexerError> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                IndexerError::Runner(format!(
                    "failed to create checkpoint directory {}: {error}",
                    parent.display()
                ))
            })?;
        }

        let temp_path = self.path.with_extension(format!(
            "{}tmp",
            self.path
                .extension()
                .and_then(|extension| extension.to_str())
                .map(|extension| format!("{extension}."))
                .unwrap_or_default()
        ));
        let bytes = serde_json::to_vec_pretty(checkpoint).map_err(|error| {
            IndexerError::Runner(format!("failed to serialize checkpoint: {error}"))
        })?;
        fs::write(&temp_path, bytes).map_err(|error| {
            IndexerError::Runner(format!(
                "failed to write checkpoint temp file {}: {error}",
                temp_path.display()
            ))
        })?;
        fs::rename(&temp_path, &self.path).map_err(|error| {
            IndexerError::Runner(format!(
                "failed to replace checkpoint {}: {error}",
                self.path.display()
            ))
        })
    }
}

pub(crate) fn checkpoint_key(task: &PlannedIndexTask) -> String {
    [
        task.index.as_str(),
        task.family.as_str(),
        task.chain.as_str(),
        &task.chain_id.to_string(),
        task.dataset.as_str(),
        task.range.kind.as_str(),
        &selector_fingerprint(task),
    ]
    .join("|")
}

fn selector_fingerprint(task: &PlannedIndexTask) -> String {
    let canonical = serde_json::json!({
        "kind": task.selector.kind,
        "addresses": task.selector.addresses,
        "topics": task.selector.topics,
    })
    .to_string();
    stable_digest_prefix(&canonical)
}

fn stable_digest_prefix(value: &str) -> String {
    const PREFIX_BYTES: usize = 16;

    let digest = Sha256::digest(value.as_bytes());
    digest[..PREFIX_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}
