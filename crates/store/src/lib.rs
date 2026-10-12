//! PostgreSQL persistence: schema migrations, chain partitions, and repositories.

mod blocks;
mod codec;
mod coverage;
mod error;
mod follow;
mod heads;
mod ids;
mod jobs;
mod logs;
mod notify;
mod selectors;
mod subscriptions;
mod tx_senders;
mod usage;

use std::sync::Arc;
use std::time::Duration;

use datalens_core::{ChainId, ChainName};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool};

pub use blocks::BlockCursor;
pub use coverage::longest_covered_prefix;
pub use error::StoreError;
pub use follow::FollowProgress;
pub use heads::HeadKind;
pub use ids::{JobId, SelectorId};
pub use jobs::{Job, JobPurpose, JobState, JobStatus};
pub use logs::LogCursor;
pub use subscriptions::{Subscription, SubscriptionSource};
pub use usage::{UsageDelta, UsageTotals};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub type PgTx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// Pool plus the repositories in this crate.
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
    notifier: Arc<notify::Notifier>,
}

impl Store {
    /// The first `wait_done` or `wait_head_change` keeps one pool connection for `LISTEN`
    /// until the pool closes. Every waiter on this store and its clones shares it.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            notifier: Arc::default(),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn ensure_chain(&self, id: ChainId, name: &ChainName) -> Result<(), StoreError> {
        ensure_chain(&self.pool, id, name)
            .await
            .map_err(StoreError::from)
    }
}

/// Serializes partition DDL across replicas that start at the same time.
const ENSURE_CHAIN_LOCK: i64 = 0x6461_7461_6c65_6e73;

pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await
}

pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

/// Registers the chain and creates its `evm_logs_<id>` and `evm_blocks_<id>` partitions.
/// Safe to call on every startup and from concurrent replicas.
pub async fn ensure_chain(pool: &PgPool, id: ChainId, name: &ChainName) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(ENSURE_CHAIN_LOCK)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "insert into chains (chain_id, name) values ($1, $2)
         on conflict (chain_id) do update set name = excluded.name",
    )
    .bind(id.get())
    .bind(name.as_str())
    .execute(&mut *tx)
    .await?;
    let id = id.get();
    for parent in ["evm_logs", "evm_blocks"] {
        sqlx::query(AssertSqlSafe(format!(
            "create table if not exists {parent}_{id} partition of {parent} for values in ({id})"
        )))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}
