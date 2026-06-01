use std::io;

use serde_json::{Map, Value};
use sqlx::{
    PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};
use tokio::runtime::Runtime;

use crate::IndexerError;

use super::{
    IndexedRecord, OutputWriteReceipt, OutputWriteResult, OutputWriteSink, QueryableStore,
    StoreQuery, StoreQueryResult,
    event::{NormalizedIndexedEvent, RowPayloadMetadata, max_position, row_payload_with_metadata},
    filter::StoreQueryFilter,
};

pub struct PostgresOutputStore {
    runtime: Runtime,
    pool: PgPool,
}

impl PostgresOutputStore {
    pub fn connect(url: &str) -> io::Result<Self> {
        let runtime = Runtime::new().map_err(io::Error::other)?;
        let pool = runtime
            .block_on(async { PgPoolOptions::new().max_connections(5).connect(url).await })
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
                        id BIGSERIAL PRIMARY KEY,
                        unique_key TEXT NOT NULL UNIQUE,
                        index_name TEXT NOT NULL,
                        chain_family TEXT NOT NULL,
                        chain_id BIGINT NOT NULL,
                        chain_name TEXT NOT NULL,
                        chain_identity TEXT NOT NULL,
                        dataset TEXT NOT NULL,
                        block_number BIGINT NOT NULL,
                        block_hash TEXT,
                        parent_hash TEXT,
                        block_timestamp BIGINT,
                        transaction_hash TEXT,
                        transaction_index BIGINT,
                        event_index BIGINT,
                        selector TEXT,
                        topics_json TEXT,
                        signature TEXT,
                        topic0 TEXT,
                        event_name TEXT,
                        data_payload TEXT,
                        raw_payload TEXT NOT NULL,
                        removed BIGINT,
                        finality TEXT,
                        created_at TEXT NOT NULL DEFAULT (
                            to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        )
                    )
                    "#,
                )
                .execute(&self.pool)
                .await?;
                ensure_event_name_column_postgres(&self.pool).await?;
                ensure_topic0_column_postgres(&self.pool).await?;
                ensure_parent_hash_column_postgres(&self.pool).await?;
                ensure_block_timestamp_column_postgres(&self.pool).await?;
                backfill_topic0_postgres(&self.pool).await?;
                for statement in DROP_INDEX_STATEMENTS {
                    sqlx::query(*statement).execute(&self.pool).await?;
                }
                for statement in INDEX_STATEMENTS {
                    sqlx::query(*statement).execute(&self.pool).await?;
                }
                Ok::<(), sqlx::Error>(())
            })
            .map_err(io::Error::other)
    }
}

async fn ensure_event_name_column_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("ALTER TABLE indexed_events ADD COLUMN IF NOT EXISTS event_name TEXT")
        .execute(pool)
        .await?;
    Ok(())
}

async fn ensure_topic0_column_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("ALTER TABLE indexed_events ADD COLUMN IF NOT EXISTS topic0 TEXT")
        .execute(pool)
        .await?;
    Ok(())
}

async fn ensure_parent_hash_column_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("ALTER TABLE indexed_events ADD COLUMN IF NOT EXISTS parent_hash TEXT")
        .execute(pool)
        .await?;
    Ok(())
}

async fn ensure_block_timestamp_column_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("ALTER TABLE indexed_events ADD COLUMN IF NOT EXISTS block_timestamp BIGINT")
        .execute(pool)
        .await?;
    Ok(())
}

async fn backfill_topic0_postgres(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE indexed_events
        SET topic0 = topics_json::jsonb ->> 0
        WHERE topic0 IS NULL
          AND topics_json IS NOT NULL
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

impl OutputWriteSink for PostgresOutputStore {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        self.runtime
            .block_on(write_records_postgres(&self.pool, records))
            .map_err(io::Error::other)
    }
}

impl QueryableStore for PostgresOutputStore {
    fn query(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        self.runtime
            .block_on(query_records_postgres(&self.pool, query, false))
            .map_err(|error| IndexerError::Runner(format!("postgres query failed: {error}")))
    }

    fn query_decoded_events(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        self.runtime
            .block_on(query_records_postgres(&self.pool, query, true))
            .map_err(|error| {
                IndexerError::Runner(format!("postgres decoded query failed: {error}"))
            })
    }
}

const DROP_INDEX_STATEMENTS: &[&str] = &[
    "DROP INDEX IF EXISTS idx_indexed_events_pg_index_name",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_chain",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_dataset",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_selector",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_block_range",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_transaction",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_signature",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_event_name",
    "DROP INDEX IF EXISTS idx_indexed_events_pg_ordering",
];

const INDEX_STATEMENTS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_pg_query_page ON indexed_events(dataset, index_name, chain_name, chain_id, block_number, transaction_index, event_index, id)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_pg_selector_page ON indexed_events(dataset, index_name, chain_name, chain_id, selector, block_number, transaction_index, event_index, id)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_pg_topic0_page ON indexed_events(dataset, index_name, chain_name, chain_id, topic0, block_number, transaction_index, event_index, id)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_pg_event_name_page ON indexed_events(dataset, index_name, chain_name, chain_id, event_name, block_number, transaction_index, event_index, id)",
    "CREATE INDEX IF NOT EXISTS idx_indexed_events_pg_transaction_page ON indexed_events(dataset, index_name, chain_name, chain_id, transaction_hash, block_number, transaction_index, event_index, id)",
];

async fn write_records_postgres(
    pool: &PgPool,
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
            flushed_rows: records.len(),
            inserted_rows,
            skipped_or_replaced_rows,
            files_written: 0,
            batches_attempted: usize::from(!records.is_empty()),
            batches_delivered: usize::from(!records.is_empty()),
            highest_position: highest_position
                .clone()
                .map(|position| position.receipt_key),
            last_record: highest_position.map(|position| position.receipt_key),
        }),
    })
}

async fn insert_record(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NormalizedIndexedEvent,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO indexed_events (
            unique_key,
            index_name,
            chain_family,
            chain_id,
            chain_name,
            chain_identity,
            dataset,
            block_number,
            block_hash,
            parent_hash,
            block_timestamp,
            transaction_hash,
            transaction_index,
            event_index,
            selector,
            topics_json,
            signature,
            topic0,
            event_name,
            data_payload,
            raw_payload,
            removed,
            finality
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        ON CONFLICT (unique_key) DO NOTHING
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
    .bind(&row.parent_hash)
    .bind(row.block_timestamp)
    .bind(&row.transaction_hash)
    .bind(row.transaction_index)
    .bind(row.event_index)
    .bind(&row.selector)
    .bind(&row.topics_json)
    .bind(&row.signature)
    .bind(&row.topic0)
    .bind(&row.event_name)
    .bind(&row.data_payload)
    .bind(&row.raw_payload)
    .bind(row.removed)
    .bind(&row.finality)
    .execute(&mut **transaction)
    .await
}

async fn query_records_postgres(
    pool: &PgPool,
    query: StoreQuery,
    decoded_only: bool,
) -> Result<StoreQueryResult, sqlx::Error> {
    let filter = StoreQueryFilter::from_query(&query);
    let mut builder =
        sqlx::QueryBuilder::<Postgres>::new("SELECT * FROM indexed_events WHERE dataset = ");
    builder.push_bind(query.dataset);
    if decoded_only {
        builder
            .push(" AND raw_payload::jsonb ?| array['decoded', 'decode_status', 'decode_error']");
    }
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
    if let Some(event_name) = filter.event_name {
        builder.push(" AND event_name = ").push_bind(event_name);
    }
    if let Some(topic0) = filter.topic0 {
        builder.push(" AND topic0 = ").push_bind(topic0);
    }
    builder.push(" ORDER BY block_number, transaction_index, event_index, id");
    builder
        .push(" LIMIT ")
        .push_bind(i64::try_from(filter.limit).unwrap_or(i64::MAX));
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
        .map(postgres_row_to_json)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StoreQueryResult { rows })
}

fn postgres_row_to_json(row: PgRow) -> Result<Value, sqlx::Error> {
    let raw_payload: String = row.try_get("raw_payload")?;
    let value = serde_json::from_str::<Value>(&raw_payload).unwrap_or(Value::Object(Map::new()));
    Ok(row_payload_with_metadata(
        value,
        RowPayloadMetadata {
            index_name: row.try_get("index_name")?,
            chain_name: row.try_get("chain_name")?,
            chain_family: row.try_get("chain_family")?,
            chain_id: row.try_get("chain_id")?,
            dataset: row.try_get("dataset")?,
            parent_hash: row.try_get("parent_hash")?,
            block_timestamp: row.try_get("block_timestamp")?,
            created_at: row.try_get("created_at")?,
        },
    ))
}
