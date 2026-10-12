//! PostgreSQL persistence: schema migrations and per-chain partitions.

use std::time::Duration;

use datalens_core::{ChainId, ChainName};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool};
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

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
