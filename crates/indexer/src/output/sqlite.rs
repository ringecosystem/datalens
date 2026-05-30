use std::{
    io,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde_json::{Map, Value};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tokio::runtime::Runtime;

use crate::IndexerError;

use super::{
    IndexedRecord, OutputWriteReceipt, OutputWriteResult, OutputWriteSink, QueryableStore,
    StoreQuery, StoreQueryResult,
    event::{NormalizedIndexedEvent, max_position, row_payload_with_metadata},
    filter::StoreQueryFilter,
};

pub struct SqliteOutputStore {
    runtime: Runtime,
    pool: SqlitePool,
}

impl SqliteOutputStore {
    pub fn connect(url: &str) -> io::Result<Self> {
        ensure_sqlite_parent_dir(url)?;
        let runtime = Runtime::new().map_err(io::Error::other)?;
        let options = SqliteConnectOptions::from_str(url)
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
                    CREATE TABLE IF NOT EXISTS indexed_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        unique_key TEXT NOT NULL UNIQUE,
                        index_name TEXT NOT NULL,
                        chain_family TEXT NOT NULL,
                        chain_id INTEGER NOT NULL,
                        chain_name TEXT NOT NULL,
                        chain_identity TEXT NOT NULL,
                        dataset TEXT NOT NULL,
                        block_number INTEGER NOT NULL,
                        block_hash TEXT,
                        transaction_hash TEXT,
                        transaction_index INTEGER,
                        event_index INTEGER,
                        selector TEXT,
                        topics_json TEXT,
                        signature TEXT,
                        data_payload TEXT,
                        raw_payload TEXT NOT NULL,
                        removed INTEGER,
                        finality TEXT,
                        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    )
                    "#,
                )
                .execute(&self.pool)
                .await?;
                for statement in INDEX_STATEMENTS {
                    sqlx::query(*statement).execute(&self.pool).await?;
                }
                Ok::<(), sqlx::Error>(())
            })
            .map_err(io::Error::other)
    }
}

impl OutputWriteSink for SqliteOutputStore {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        self.runtime
            .block_on(write_records_sqlite(&self.pool, records))
            .map_err(io::Error::other)
    }
}

impl QueryableStore for SqliteOutputStore {
    fn query(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        self.runtime
            .block_on(query_records_sqlite(&self.pool, query))
            .map_err(|error| IndexerError::Runner(format!("sqlite query failed: {error}")))
    }
}

const INDEX_STATEMENTS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_index_name ON indexed_events(index_name)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_chain ON indexed_events(chain_family, chain_id, chain_name)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_dataset ON indexed_events(dataset)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_selector ON indexed_events(selector)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_block_range ON indexed_events(chain_identity, dataset, block_number)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_transaction ON indexed_events(transaction_hash)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_ordering ON indexed_events(chain_identity, dataset, block_number, transaction_index, event_index)",
];

async fn write_records_sqlite(
    pool: &SqlitePool,
    records: &[IndexedRecord],
) -> Result<OutputWriteResult, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let mut inserted_rows = 0;
    let mut highest_position = None;

    for record in records {
        let row = NormalizedIndexedEvent::from_record(record);
        let result = insert_record(&mut transaction, &row).await?;
        inserted_rows += usize::try_from(result.rows_affected()).unwrap_or_default();
        highest_position = max_position(highest_position, row.position.clone());
    }

    transaction.commit().await?;
    let skipped_or_replaced_rows = records.len().saturating_sub(inserted_rows);

    Ok(OutputWriteResult {
        written_rows: records.len(),
        receipt: Some(OutputWriteReceipt {
            accepted_rows: records.len(),
            inserted_rows,
            skipped_or_replaced_rows,
            highest_position: highest_position
                .clone()
                .map(|position| position.receipt_key),
            last_record: highest_position.map(|position| position.receipt_key),
        }),
    })
}

async fn insert_record(
    transaction: &mut Transaction<'_, Sqlite>,
    row: &NormalizedIndexedEvent,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO indexed_events (
            unique_key,
            index_name,
            chain_family,
            chain_id,
            chain_name,
            chain_identity,
            dataset,
            block_number,
            block_hash,
            transaction_hash,
            transaction_index,
            event_index,
            selector,
            topics_json,
            signature,
            data_payload,
            raw_payload,
            removed,
            finality
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&row.unique_key)
    .bind(&row.index_name)
    .bind(&row.chain_family)
    .bind(row.chain_id)
    .bind(&row.chain_name)
    .bind(&row.chain_identity)
    .bind(&row.dataset)
    .bind(row.block_number)
    .bind(&row.block_hash)
    .bind(&row.transaction_hash)
    .bind(row.transaction_index)
    .bind(row.event_index)
    .bind(&row.selector)
    .bind(&row.topics_json)
    .bind(&row.signature)
    .bind(&row.data_payload)
    .bind(&row.raw_payload)
    .bind(row.removed)
    .bind(&row.finality)
    .execute(&mut **transaction)
    .await
}

async fn query_records_sqlite(
    pool: &SqlitePool,
    query: StoreQuery,
) -> Result<StoreQueryResult, sqlx::Error> {
    let filter = StoreQueryFilter::from_query(&query);
    let mut builder = sqlx::QueryBuilder::new("SELECT * FROM indexed_events WHERE dataset = ");
    builder.push_bind(query.dataset);
    if let Some(index) = filter.index {
        builder.push(" AND index_name = ").push_bind(index);
    }
    if let Some(chain) = filter.chain {
        builder.push(" AND chain_name = ").push_bind(chain);
    }
    if let Some(chain_id) = filter.chain_id {
        builder.push(" AND chain_id = ").push_bind(chain_id);
    }
    if let Some(selector) = filter.selector {
        builder.push(" AND selector = ").push_bind(selector);
    }
    if let Some(from_block) = filter.from_block {
        builder.push(" AND block_number >= ").push_bind(from_block);
    }
    if let Some(to_block) = filter.to_block {
        builder.push(" AND block_number <= ").push_bind(to_block);
    }
    if let Some(transaction_hash) = filter.transaction_hash {
        builder
            .push(" AND transaction_hash = ")
            .push_bind(transaction_hash);
    }
    if let Some(signature) = filter.signature {
        builder.push(" AND signature = ").push_bind(signature);
    }
    if let Some(topic0) = filter.topic0 {
        builder
            .push(" AND (signature = ")
            .push_bind(topic0.clone())
            .push(" OR topics_json LIKE ")
            .push_bind(format!("[\"{topic0}\"%"))
            .push(")");
    }
    builder.push(" ORDER BY block_number, transaction_index, event_index, id");
    if let Some(limit) = filter.limit {
        builder
            .push(" LIMIT ")
            .push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
    }
    if let Some(offset) = filter.offset {
        builder
            .push(" OFFSET ")
            .push_bind(i64::try_from(offset).unwrap_or(i64::MAX));
    }

    let rows = builder
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(sqlite_row_to_json)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StoreQueryResult { rows })
}

fn sqlite_row_to_json(row: SqliteRow) -> Result<Value, sqlx::Error> {
    let raw_payload: String = row.try_get("raw_payload")?;
    let value = serde_json::from_str::<Value>(&raw_payload).unwrap_or(Value::Object(Map::new()));
    Ok(row_payload_with_metadata(
        value,
        row.try_get("index_name")?,
        row.try_get("chain_name")?,
        row.try_get("chain_family")?,
        row.try_get("chain_id")?,
        row.try_get("dataset")?,
        row.try_get("created_at")?,
    ))
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
