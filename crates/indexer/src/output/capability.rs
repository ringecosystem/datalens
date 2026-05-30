use serde::Serialize;

use crate::{OutputConfig, OutputSinkConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Jsonl,
    Database,
    Webhook,
}

impl OutputKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Database => "database",
            Self::Webhook => "webhook",
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

    pub const fn database() -> Self {
        Self {
            kind: OutputKind::Database,
            supports_write: true,
            supports_query: true,
            supports_graphql: true,
            write_mode: OutputWriteMode::IdempotentUpsert,
        }
    }

    pub const fn webhook() -> Self {
        Self {
            kind: OutputKind::Webhook,
            supports_write: true,
            supports_query: false,
            supports_graphql: false,
            write_mode: OutputWriteMode::IdempotentUpsert,
        }
    }
}

impl OutputConfig {
    pub fn kind(&self) -> OutputKind {
        match self {
            Self::Jsonl { .. } => OutputKind::Jsonl,
            Self::Database { .. } => OutputKind::Database,
            Self::Webhook { .. } => OutputKind::Webhook,
        }
    }

    pub fn capability(&self) -> OutputCapability {
        match self {
            Self::Jsonl { .. } => OutputCapability::jsonl(),
            Self::Database { .. } => OutputCapability::database(),
            Self::Webhook { .. } => OutputCapability::webhook(),
        }
    }
}

impl OutputSinkConfig {
    pub fn kind(&self) -> OutputKind {
        match self {
            Self::StdoutJson | Self::FileJson { .. } => OutputKind::Jsonl,
            Self::DatabaseSqlite { .. } | Self::DatabasePostgres { .. } => OutputKind::Database,
            Self::Webhook { .. } => OutputKind::Webhook,
        }
    }

    pub fn capability(&self) -> OutputCapability {
        match self {
            Self::StdoutJson | Self::FileJson { .. } => OutputCapability::jsonl(),
            Self::DatabaseSqlite { .. } | Self::DatabasePostgres { .. } => {
                OutputCapability::database()
            }
            Self::Webhook { .. } => OutputCapability::webhook(),
        }
    }
}
