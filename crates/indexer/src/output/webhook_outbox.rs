use std::{io, path::Path, str::FromStr};

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::runtime::Runtime;

pub(super) struct WebhookOutboxStore {
    runtime: Runtime,
    pool: SqlitePool,
}

impl WebhookOutboxStore {
    pub(super) fn connect(path: &Path) -> io::Result<Self> {
        ensure_parent_dir(path)?;
        let runtime = Runtime::new().map_err(io::Error::other)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", path.display()))
            .map_err(io::Error::other)?
            .create_if_missing(true);
        let pool = runtime
            .block_on(async {
                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(options)
                    .await
            })
            .map_err(io::Error::other)?;
        let store = Self { runtime, pool };
        store.initialize_schema()?;
        Ok(store)
    }

    fn initialize_schema(&self) -> io::Result<()> {
        self.runtime
            .block_on(async {
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS webhook_outbox (
                        batch_id TEXT PRIMARY KEY,
                        idempotency_key TEXT NOT NULL,
                        status TEXT NOT NULL,
                        endpoint_url TEXT NOT NULL,
                        header_names_json TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                        last_attempt_at TEXT,
                        last_error TEXT NOT NULL DEFAULT ''
                    )
                    "#,
                )
                .execute(&self.pool)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS idx_webhook_outbox_status ON webhook_outbox(status, created_at)",
                )
                .execute(&self.pool)
                .await?;
                Ok::<(), sqlx::Error>(())
            })
            .map_err(io::Error::other)
    }

    pub(super) fn upsert_pending(&self, batch: WebhookOutboxBatch) -> io::Result<()> {
        self.runtime
            .block_on(async {
                sqlx::query(
                    r#"
                    INSERT INTO webhook_outbox (
                        batch_id,
                        idempotency_key,
                        status,
                        endpoint_url,
                        header_names_json,
                        payload_json
                    )
                    VALUES (?, ?, 'pending', ?, ?, ?)
                    ON CONFLICT(batch_id) DO NOTHING
                    "#,
                )
                .bind(batch.batch_id)
                .bind(batch.idempotency_key)
                .bind(batch.endpoint_url)
                .bind(batch.header_names_json)
                .bind(batch.payload_json)
                .execute(&self.pool)
                .await
            })
            .map(|_| ())
            .map_err(io::Error::other)
    }

    pub(super) fn pending_records(&self) -> io::Result<Vec<WebhookOutboxRecord>> {
        self.runtime
            .block_on(async {
                let rows = sqlx::query(
                    r#"
                    SELECT batch_id, payload_json
                    FROM webhook_outbox
                    WHERE status = 'pending'
                    ORDER BY created_at, batch_id
                    "#,
                )
                .fetch_all(&self.pool)
                .await?;
                rows.into_iter()
                    .map(|row| {
                        let payload_json: String = row.get("payload_json");
                        let payload = serde_json::from_str(&payload_json)
                            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
                        Ok(WebhookOutboxRecord {
                            batch_id: row.get("batch_id"),
                            payload,
                        })
                    })
                    .collect::<Result<Vec<_>, sqlx::Error>>()
            })
            .map_err(io::Error::other)
    }

    pub(super) fn attempt_count(&self, batch_id: &str) -> io::Result<usize> {
        self.runtime
            .block_on(async {
                sqlx::query("SELECT attempt_count FROM webhook_outbox WHERE batch_id = ?")
                    .bind(batch_id)
                    .fetch_one(&self.pool)
                    .await
            })
            .map(|row| {
                let attempts: i64 = row.get("attempt_count");
                usize::try_from(attempts).unwrap_or_default()
            })
            .map_err(io::Error::other)
    }

    pub(super) fn record_attempt(&self, batch_id: &str, error: &str) -> io::Result<()> {
        self.runtime
            .block_on(async {
                sqlx::query(
                    r#"
                    UPDATE webhook_outbox
                    SET attempt_count = attempt_count + 1,
                        last_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        last_error = ?
                    WHERE batch_id = ?
                    "#,
                )
                .bind(error)
                .bind(batch_id)
                .execute(&self.pool)
                .await
            })
            .map(|_| ())
            .map_err(io::Error::other)
    }

    pub(super) fn mark_dead_letter(&self, batch_id: &str, error: &str) -> io::Result<()> {
        self.runtime
            .block_on(async {
                sqlx::query(
                    r#"
                    UPDATE webhook_outbox
                    SET status = 'dead_letter',
                        last_error = ?
                    WHERE batch_id = ?
                    "#,
                )
                .bind(error)
                .bind(batch_id)
                .execute(&self.pool)
                .await
            })
            .map(|_| ())
            .map_err(io::Error::other)
    }

    pub(super) fn delete(&self, batch_id: &str) -> io::Result<()> {
        self.runtime
            .block_on(async {
                sqlx::query("DELETE FROM webhook_outbox WHERE batch_id = ?")
                    .bind(batch_id)
                    .execute(&self.pool)
                    .await
            })
            .map(|_| ())
            .map_err(io::Error::other)
    }
}

pub(super) struct WebhookOutboxBatch {
    pub(super) batch_id: String,
    pub(super) idempotency_key: String,
    pub(super) endpoint_url: String,
    pub(super) header_names_json: String,
    pub(super) payload_json: String,
}

pub(super) struct WebhookOutboxRecord {
    pub(super) batch_id: String,
    pub(super) payload: serde_json::Value,
}

fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
