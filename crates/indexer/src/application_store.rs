use std::{
    fmt, io,
    path::{Path, PathBuf},
    str::FromStr,
};

use sqlx::{
    PgPool, Postgres, Sqlite, SqlitePool, Transaction,
    postgres::{PgPoolOptions, PgQueryResult},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteQueryResult},
};
use tokio::runtime::Runtime;

use crate::sdk::{CheckpointCursor, ProcessorError, ProcessorFuture};

pub trait ProcessorApplicationEntityStore: Send + Sync {
    type Transaction<'store>
    where
        Self: 'store;

    fn begin_transaction<'store>(
        &'store self,
    ) -> ProcessorFuture<'store, Result<Self::Transaction<'store>, ProcessorError>>;
}

pub struct SqliteApplicationEntityStore {
    pool: SqlitePool,
    url: String,
}

impl SqliteApplicationEntityStore {
    pub fn connect(url: &str) -> io::Result<Self> {
        Runtime::new()
            .map_err(io::Error::other)?
            .block_on(Self::connect_async(url))
    }

    pub async fn connect_async(url: &str) -> io::Result<Self> {
        ensure_sqlite_parent_dir(url)?;
        let options = SqliteConnectOptions::from_str(url)
            .map_err(io::Error::other)?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(io::Error::other)?;
        let store = Self {
            pool,
            url: url.to_owned(),
        };
        store.initialize_schema().await?;
        Ok(store)
    }

    pub async fn begin(&self) -> Result<SqliteApplicationEntityTransaction<'_>, ProcessorError> {
        Ok(SqliteApplicationEntityTransaction {
            transaction: self.pool.begin().await?,
        })
    }

    pub async fn checkpoint(&self, key: &str) -> Result<Option<CheckpointCursor>, ProcessorError> {
        let row = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT checkpoint_value
            FROM processor_checkpoints
            WHERE checkpoint_key = ?
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(value,)| CheckpointCursor::new(key, value)))
    }

    async fn initialize_schema(&self) -> io::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS processor_checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                checkpoint_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(io::Error::other)?;
        Ok(())
    }
}

impl ProcessorApplicationEntityStore for SqliteApplicationEntityStore {
    type Transaction<'store> = SqliteApplicationEntityTransaction<'store>;

    fn begin_transaction<'store>(
        &'store self,
    ) -> ProcessorFuture<'store, Result<Self::Transaction<'store>, ProcessorError>> {
        Box::pin(async move { self.begin().await })
    }
}

impl fmt::Debug for SqliteApplicationEntityStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteApplicationEntityStore")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

pub struct SqliteApplicationEntityTransaction<'transaction> {
    transaction: Transaction<'transaction, Sqlite>,
}

impl SqliteApplicationEntityTransaction<'_> {
    pub fn sqlite(&mut self) -> &mut sqlx::SqliteConnection {
        &mut self.transaction
    }

    pub async fn put_checkpoint(
        &mut self,
        cursor: &CheckpointCursor,
    ) -> Result<SqliteQueryResult, ProcessorError> {
        Ok(sqlx::query(
            r#"
            INSERT INTO processor_checkpoints (checkpoint_key, checkpoint_value, updated_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(checkpoint_key) DO UPDATE SET
                checkpoint_value = excluded.checkpoint_value,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(cursor.key())
        .bind(cursor.value())
        .execute(&mut *self.transaction)
        .await?)
    }

    pub async fn commit(self) -> Result<(), ProcessorError> {
        Ok(self.transaction.commit().await?)
    }

    pub async fn rollback(self) -> Result<(), ProcessorError> {
        Ok(self.transaction.rollback().await?)
    }
}

pub struct PostgresApplicationEntityStore {
    pool: PgPool,
    url: String,
}

impl PostgresApplicationEntityStore {
    pub fn connect(url: &str) -> io::Result<Self> {
        Runtime::new()
            .map_err(io::Error::other)?
            .block_on(Self::connect_async(url))
    }

    pub async fn connect_async(url: &str) -> io::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(io::Error::other)?;
        let store = Self {
            pool,
            url: url.to_owned(),
        };
        store.initialize_schema().await?;
        Ok(store)
    }

    pub async fn begin(&self) -> Result<PostgresApplicationEntityTransaction<'_>, ProcessorError> {
        Ok(PostgresApplicationEntityTransaction {
            transaction: self.pool.begin().await?,
        })
    }

    pub async fn checkpoint(&self, key: &str) -> Result<Option<CheckpointCursor>, ProcessorError> {
        let row = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT checkpoint_value
            FROM processor_checkpoints
            WHERE checkpoint_key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(value,)| CheckpointCursor::new(key, value)))
    }

    async fn initialize_schema(&self) -> io::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS processor_checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                checkpoint_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (
                    to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                )
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(io::Error::other)?;
        Ok(())
    }
}

impl ProcessorApplicationEntityStore for PostgresApplicationEntityStore {
    type Transaction<'store> = PostgresApplicationEntityTransaction<'store>;

    fn begin_transaction<'store>(
        &'store self,
    ) -> ProcessorFuture<'store, Result<Self::Transaction<'store>, ProcessorError>> {
        Box::pin(async move { self.begin().await })
    }
}

impl fmt::Debug for PostgresApplicationEntityStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresApplicationEntityStore")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

pub struct PostgresApplicationEntityTransaction<'transaction> {
    transaction: Transaction<'transaction, Postgres>,
}

impl PostgresApplicationEntityTransaction<'_> {
    pub fn postgres(&mut self) -> &mut sqlx::PgConnection {
        &mut self.transaction
    }

    pub async fn put_checkpoint(
        &mut self,
        cursor: &CheckpointCursor,
    ) -> Result<PgQueryResult, ProcessorError> {
        Ok(sqlx::query(
            r#"
            INSERT INTO processor_checkpoints (checkpoint_key, checkpoint_value, updated_at)
            VALUES ($1, $2, to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
            ON CONFLICT(checkpoint_key) DO UPDATE SET
                checkpoint_value = excluded.checkpoint_value,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(cursor.key())
        .bind(cursor.value())
        .execute(&mut *self.transaction)
        .await?)
    }

    pub async fn commit(self) -> Result<(), ProcessorError> {
        Ok(self.transaction.commit().await?)
    }

    pub async fn rollback(self) -> Result<(), ProcessorError> {
        Ok(self.transaction.rollback().await?)
    }
}

fn ensure_sqlite_parent_dir(url: &str) -> io::Result<()> {
    let Some(path) = sqlite_file_path(url) else {
        return Ok(());
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn sqlite_file_path(url: &str) -> Option<PathBuf> {
    if url == "sqlite::memory:" || url.contains("mode=memory") {
        return None;
    }
    let path = url.strip_prefix("sqlite:")?;
    let path = path.split_once('?').map(|(path, _)| path).unwrap_or(path);
    let path = path.strip_prefix("//").unwrap_or(path);
    if path.is_empty() {
        None
    } else {
        Some(Path::new(path).to_path_buf())
    }
}

fn redact_url(url: &str) -> String {
    let mut value = redact_credentials(url);
    if let Some((base, query)) = value.split_once('?') {
        value = format!("{base}?{}", redact_query(query));
    }
    value
}

fn redact_credentials(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_owned();
    };
    let Some((_, host)) = rest.split_once('@') else {
        return value.to_owned();
    };
    format!("{scheme}://<redacted>@{host}")
}

fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|part| {
            let Some((name, value)) = part.split_once('=') else {
                return part.to_owned();
            };
            if is_sensitive_query_name(name) {
                format!("{name}=<redacted>")
            } else {
                format!("{name}={value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn is_sensitive_query_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "password" | "token" | "key" | "secret" | "signature"
    )
}
