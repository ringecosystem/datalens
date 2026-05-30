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
    let filter = query.filter.as_object();
    let index = filter
        .and_then(|filter| filter.get("index"))
        .and_then(Value::as_str);
    let chain = filter
        .and_then(|filter| filter.get("chain"))
        .and_then(Value::as_str);
    let chain_id = filter
        .and_then(|filter| filter.get("chain_id"))
        .and_then(Value::as_u64)
        .and_then(|value| i64::try_from(value).ok());
    let selector = filter
        .and_then(|filter| filter.get("address").or_else(|| filter.get("selector")))
        .and_then(Value::as_str);
    let from_block = filter
        .and_then(|filter| filter.get("from_block"))
        .and_then(Value::as_u64)
        .and_then(|value| i64::try_from(value).ok());
    let to_block = filter
        .and_then(|filter| filter.get("to_block"))
        .and_then(Value::as_u64)
        .and_then(|value| i64::try_from(value).ok());
    let transaction_hash = filter
        .and_then(|filter| filter.get("transaction_hash"))
        .and_then(Value::as_str);

    let mut builder = sqlx::QueryBuilder::new("SELECT * FROM indexed_events WHERE dataset = ");
    builder.push_bind(query.dataset);
    if let Some(index) = index {
        builder.push(" AND index_name = ").push_bind(index);
    }
    if let Some(chain) = chain {
        builder.push(" AND chain_name = ").push_bind(chain);
    }
    if let Some(chain_id) = chain_id {
        builder.push(" AND chain_id = ").push_bind(chain_id);
    }
    if let Some(selector) = selector {
        builder.push(" AND selector = ").push_bind(selector);
    }
    if let Some(from_block) = from_block {
        builder.push(" AND block_number >= ").push_bind(from_block);
    }
    if let Some(to_block) = to_block {
        builder.push(" AND block_number <= ").push_bind(to_block);
    }
    if let Some(transaction_hash) = transaction_hash {
        builder
            .push(" AND transaction_hash = ")
            .push_bind(transaction_hash);
    }
    builder.push(" ORDER BY block_number, transaction_index, event_index, id");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(sqlite_row_to_json)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StoreQueryResult { rows })
}

#[derive(Clone, Debug)]
struct NormalizedIndexedEvent {
    unique_key: String,
    index_name: String,
    chain_family: String,
    chain_id: i64,
    chain_name: String,
    chain_identity: String,
    dataset: String,
    block_number: i64,
    block_hash: Option<String>,
    transaction_hash: Option<String>,
    transaction_index: Option<i64>,
    event_index: Option<i64>,
    selector: Option<String>,
    topics_json: Option<String>,
    signature: Option<String>,
    data_payload: Option<String>,
    raw_payload: String,
    removed: Option<i64>,
    finality: Option<String>,
    position: Option<EventPosition>,
}

impl NormalizedIndexedEvent {
    fn from_record(record: &IndexedRecord) -> Self {
        let chain_family = chain_family(record);
        let chain_identity = format!("{}:{}:{}", chain_family, record.chain, record.chain_id);
        let block_number = json_u64(&record.payload, "block_number")
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or_default();
        let transaction_index = json_u64(&record.payload, "transaction_index")
            .and_then(|value| i64::try_from(value).ok());
        let event_index =
            json_u64(&record.payload, "log_index").and_then(|value| i64::try_from(value).ok());
        let selector = json_string(&record.payload, "address")
            .or_else(|| json_string(&record.payload, "program"))
            .or_else(|| json_string(&record.payload, "account"))
            .or_else(|| json_string(&record.payload, "selector"));
        let topics_json = record
            .payload
            .get("topics")
            .map(|topics| topics.to_string());
        let signature = json_string(&record.payload, "signature").or_else(|| {
            record
                .payload
                .get("topics")
                .and_then(Value::as_array)
                .and_then(|topics| topics.first())
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        let block_hash = json_string(&record.payload, "block_hash");
        let transaction_hash = json_string(&record.payload, "transaction_hash");
        let position =
            EventPosition::new(&record.chain, block_number, transaction_index, event_index);
        let unique_key = unique_key(
            record,
            &chain_identity,
            block_hash.as_deref(),
            transaction_hash.as_deref(),
            event_index,
            selector.as_deref(),
        );

        Self {
            unique_key,
            index_name: record.index.clone(),
            chain_family,
            chain_id: i64::try_from(record.chain_id).unwrap_or(i64::MAX),
            chain_name: record.chain.clone(),
            chain_identity,
            dataset: record.dataset.clone(),
            block_number,
            block_hash,
            transaction_hash,
            transaction_index,
            event_index,
            selector,
            topics_json,
            signature,
            data_payload: json_string(&record.payload, "data"),
            raw_payload: record.payload.to_string(),
            removed: record
                .payload
                .get("removed")
                .and_then(Value::as_bool)
                .map(i64::from),
            finality: json_string(&record.payload, "finality"),
            position,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EventPosition {
    block_number: i64,
    transaction_index: i64,
    event_index: i64,
    receipt_key: String,
}

impl EventPosition {
    fn new(
        chain: &str,
        block_number: i64,
        transaction_index: Option<i64>,
        event_index: Option<i64>,
    ) -> Option<Self> {
        let transaction_index = transaction_index.unwrap_or(0);
        let event_index = event_index.unwrap_or(0);
        Some(Self {
            block_number,
            transaction_index,
            event_index,
            receipt_key: format!("{chain}:{block_number}:{transaction_index}:{event_index}"),
        })
    }
}

fn sqlite_row_to_json(row: SqliteRow) -> Result<Value, sqlx::Error> {
    let raw_payload: String = row.try_get("raw_payload")?;
    let mut value =
        serde_json::from_str::<Value>(&raw_payload).unwrap_or(Value::Object(Map::new()));
    let object = match &mut value {
        Value::Object(object) => object,
        _ => {
            value = Value::Object(Map::new());
            value.as_object_mut().expect("object value")
        }
    };

    object.insert(
        "index".to_owned(),
        Value::String(row.try_get("index_name")?),
    );
    object.insert(
        "chain".to_owned(),
        Value::String(row.try_get("chain_name")?),
    );
    object.insert(
        "chain_family".to_owned(),
        Value::String(row.try_get("chain_family")?),
    );
    object.insert(
        "chain_id".to_owned(),
        Value::from(row.try_get::<i64, _>("chain_id")?),
    );
    object.insert("dataset".to_owned(), Value::String(row.try_get("dataset")?));
    object.insert(
        "created_at".to_owned(),
        Value::String(row.try_get("created_at")?),
    );
    Ok(value)
}

fn unique_key(
    record: &IndexedRecord,
    chain_identity: &str,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    event_index: Option<i64>,
    selector: Option<&str>,
) -> String {
    if let Some(value) = json_string(&record.payload, "unique_key") {
        return format!("{chain_identity}:{}:{value}", record.dataset);
    }
    if let (Some(block_hash), Some(transaction_hash), Some(event_index)) =
        (block_hash, transaction_hash, event_index)
    {
        return format!(
            "{chain_identity}:{}:{block_hash}:{transaction_hash}:{event_index}",
            record.dataset
        );
    }
    format!(
        "{}:{}:{}:{}:{}:{}",
        chain_identity,
        record.dataset,
        json_u64(&record.payload, "block_number").unwrap_or_default(),
        json_u64(&record.payload, "transaction_index").unwrap_or_default(),
        event_index.unwrap_or_default(),
        selector.unwrap_or_default()
    )
}

fn max_position(
    current: Option<EventPosition>,
    next: Option<EventPosition>,
) -> Option<EventPosition> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn chain_family(record: &IndexedRecord) -> String {
    record
        .dataset
        .split_once('.')
        .map(|(family, _)| family.to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn json_u64(payload: &Value, field: &str) -> Option<u64> {
    payload.get(field).and_then(Value::as_u64)
}

fn json_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
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
