use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use datalens_core::{ChainId, ChainName, Dataset};
use serde::Deserialize;

use crate::interpolate::interpolate_value;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("environment variable {0} is not set")]
    MissingEnv(String),
    #[error("invalid placeholder name {0:?}")]
    InvalidPlaceholder(String),
    #[error("unterminated `${{` placeholder")]
    UnterminatedPlaceholder,
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// A string that must not appear in logs: tokens, database URLs, RPC URLs with API keys.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub follow: FollowConfig,
    pub metrics: MetricsConfig,
    pub chains: BTreeMap<ChainName, ChainConfig>,
    #[serde(default)]
    pub applications: Vec<ApplicationConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Api,
    Worker,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub roles: BTreeSet<Role>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub url: Secret,
    #[serde(default = "default_max_connections")]
    pub max_connections: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueryConfig {
    pub max_wait_ms: u64,
    pub page_limit: NonZeroU32,
    pub follow_attach_blocks: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_wait_ms: 15_000,
            page_limit: NonZeroU32::new(10_000).unwrap(),
            follow_attach_blocks: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FollowConfig {
    pub subscription_ttl_s: NonZeroU64,
    pub unsafe_retention_s: NonZeroU64,
}

impl Default for FollowConfig {
    fn default() -> Self {
        Self {
            subscription_ttl_s: NonZeroU64::new(900).unwrap(),
            unsafe_retention_s: NonZeroU64::new(86_400).unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    pub bearer_token: Secret,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub chain_id: ChainId,
    pub rpc_url: Secret,
    pub finality: FinalityConfig,
    #[serde(default = "default_tick_ms")]
    pub tick_ms: NonZeroU64,
    #[serde(default = "default_reorg_window_blocks")]
    pub reorg_window_blocks: NonZeroU64,
    pub chunk_blocks: NonZeroU64,
    pub max_get_logs_range_blocks: NonZeroU64,
    pub max_addresses_per_query: NonZeroUsize,
    pub provider: ProviderConfig,
}

/// How the head tracker derives `safe` and `finalized` from the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum FinalityConfig {
    /// `safe = latest - safe_lag_blocks`, `finalized = latest - finalized_lag_blocks`.
    Lag {
        safe_lag_blocks: u64,
        finalized_lag_blocks: u64,
    },
    /// The provider's `safe` and `finalized` block tags.
    RpcTags,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub rps: NonZeroU32,
    pub burst: NonZeroU32,
    pub daily_calls: Option<NonZeroU64>,
    pub max_in_flight: NonZeroU32,
    #[serde(default = "default_min_range_blocks")]
    pub min_range_blocks: NonZeroU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub id: String,
    pub token: Secret,
    pub chains: BTreeSet<ChainName>,
    pub datasets: BTreeSet<Dataset>,
    pub quota: QuotaConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaConfig {
    pub max_range_blocks: NonZeroU64,
    pub max_requests_per_minute: NonZeroU32,
    pub max_concurrent_requests: NonZeroU32,
    pub max_subscriptions: u32,
    pub max_fill_blocks_per_minute: Option<NonZeroU64>,
    #[serde(default)]
    pub allow_unfiltered_logs: bool,
}

fn default_max_connections() -> NonZeroU32 {
    NonZeroU32::new(32).unwrap()
}

fn default_tick_ms() -> NonZeroU64 {
    NonZeroU64::new(2_000).unwrap()
}

fn default_reorg_window_blocks() -> NonZeroU64 {
    NonZeroU64::new(512).unwrap()
}

fn default_min_range_blocks() -> NonZeroU64 {
    NonZeroU64::new(16).unwrap()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&text, &|name| std::env::var(name).ok())
    }

    /// Parses TOML, substituting `${NAME}` in string values from `env`, then validates.
    pub fn from_toml(
        text: &str,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let mut value: toml::Value = toml::from_str(text)?;
        interpolate_value(&mut value, env)?;
        let config: Config = value.try_into()?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));

        if self.server.roles.is_empty() {
            return invalid("server.roles must list at least one of api, worker".into());
        }

        let mut chain_ids = HashSet::new();
        for (name, chain) in &self.chains {
            if !chain_ids.insert(chain.chain_id) {
                return invalid(format!(
                    "chains.{name}: chain_id {} is used by another chain",
                    chain.chain_id
                ));
            }
            if let FinalityConfig::Lag {
                safe_lag_blocks,
                finalized_lag_blocks,
            } = chain.finality
                && safe_lag_blocks > finalized_lag_blocks
            {
                return invalid(format!(
                    "chains.{name}.finality: safe_lag_blocks {safe_lag_blocks} exceeds finalized_lag_blocks {finalized_lag_blocks}"
                ));
            }
        }

        let mut app_ids = HashSet::new();
        let mut tokens = HashSet::new();
        for app in &self.applications {
            if app.id.is_empty() {
                return invalid("applications: id must not be empty".into());
            }
            if !app_ids.insert(app.id.as_str()) {
                return invalid(format!("applications: duplicate id {:?}", app.id));
            }
            if app.token.expose().is_empty() {
                return invalid(format!("applications.{}: token must not be empty", app.id));
            }
            if !tokens.insert(app.token.expose()) {
                return invalid(format!(
                    "applications.{}: token is shared with another application",
                    app.id
                ));
            }
            if let Some(chain) = app.chains.iter().find(|c| !self.chains.contains_key(*c)) {
                return invalid(format!(
                    "applications.{}: unknown chain \"{chain}\"",
                    app.id
                ));
            }
        }
        Ok(())
    }
}
