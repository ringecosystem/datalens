use serde::Serialize;

use crate::{OutputConfig, OutputSinkConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Jsonl,
}

impl OutputKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputWriteMode {
    AppendOnly,
    IdempotentUpsert,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OutputCapability {
    pub kind: OutputKind,
    pub supports_write: bool,
    pub supports_query: bool,
    pub supports_graphql: bool,
    pub write_mode: OutputWriteMode,
}

impl OutputCapability {
    pub const fn jsonl() -> Self {
        Self {
            kind: OutputKind::Jsonl,
            supports_write: true,
            supports_query: false,
            supports_graphql: false,
            write_mode: OutputWriteMode::AppendOnly,
        }
    }
}

impl OutputConfig {
    pub fn kind(&self) -> OutputKind {
        match self {
            Self::Jsonl { .. } => OutputKind::Jsonl,
        }
    }

    pub fn capability(&self) -> OutputCapability {
        match self {
            Self::Jsonl { .. } => OutputCapability::jsonl(),
        }
    }
}

impl OutputSinkConfig {
    pub fn kind(&self) -> OutputKind {
        match self {
            Self::StdoutJson | Self::FileJson { .. } => OutputKind::Jsonl,
        }
    }

    pub fn capability(&self) -> OutputCapability {
        match self {
            Self::StdoutJson | Self::FileJson { .. } => OutputCapability::jsonl(),
        }
    }
}
