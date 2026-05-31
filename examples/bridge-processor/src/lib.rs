use std::{fmt, path::Path, str::FromStr};

use datalens_core::ChainIdentity;
use datalens_indexer::{
    ApplicationEntityQueryStore, ApplicationEntityReadQuery,
    sdk::{
        ApplicationChainReader, ApplicationDatabaseKind, ApplicationProcessor,
        ApplicationSchemaInitializer, ApplicationSchemaStore, ApplicationStore,
        ApplicationStoreTransaction, EventBatch, EventRecord, ProcessResult, ProcessorContext,
        ProcessorError, ProcessorFuture, SchemaInitializationContext,
        TransactionalApplicationStore,
    },
};
use serde_json::{Number, Value, json};
use sqlx::{
    AssertSqlSafe, Column, Row, Sqlite, SqlitePool, Transaction, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tokio::sync::Mutex;

mod graphql;
mod mock;

pub use graphql::BridgeGraphqlSchema;
pub use mock::MockBridgeMetadataReader;

#[derive(Clone, Debug, Default)]
pub struct BridgeProcessor;

impl ApplicationProcessor for BridgeProcessor {
    fn process<'a>(
        &'a self,
        context: &'a mut ProcessorContext<'a>,
        batch: &'a EventBatch,
    ) -> ProcessorFuture<'a, Result<ProcessResult, ProcessorError>> {
        Box::pin(async move {
            let store = context.store().ok_or_else(|| {
                ProcessorError::config("bridge processor requires an application store")
            })?;
            let mut processed = 0_usize;

            for record in batch.records() {
                let Some(event_name) = decoded_string(record, "event_name") else {
                    continue;
                };
                if event_name != "MessageSent" && event_name != "MessageDelivered" {
                    continue;
                }

                let route_name = if event_name == "MessageSent" {
                    read_route_name(context.chain_reader(), context.chain(), record).await?
                } else {
                    None
                };
                store
                    .upsert_json(
                        &format!("bridge_event:{}", record.source_key),
                        bridge_event_payload(context.chain(), record, &event_name, route_name)?,
                    )
                    .await?;
                processed += 1;
            }

            Ok(ProcessResult::success(batch.checkpoint_cursor().clone())
                .with_processed_records(processed))
        })
    }
}

async fn read_route_name(
    reader: Option<&(dyn ApplicationChainReader + Send + Sync)>,
    chain: &ChainIdentity,
    record: &EventRecord,
) -> Result<Option<String>, ProcessorError> {
    let Some(reader) = reader else {
        return Ok(None);
    };
    let destination_chain = decoded_u64(record, "destination_chain")?;
    let value = reader
        .read_json(chain, &format!("route:{destination_chain}"))
        .await?;
    Ok(value
        .get("route_name")
        .and_then(Value::as_str)
        .map(str::to_owned))
}

fn bridge_event_payload(
    chain: &ChainIdentity,
    record: &EventRecord,
    event_name: &str,
    route_name: Option<String>,
) -> Result<Value, ProcessorError> {
    let transaction_hash = record
        .payload
        .get("transaction_hash")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let event_position = record.ordering_key.event_position.unwrap_or_default();
    let decoded = record
        .decoded
        .as_ref()
        .ok_or_else(|| ProcessorError::data("bridge event requires decoded fields"))?;
    let mut value = json!({
        "chain": chain.key_prefix(),
        "source_key": record.source_key,
        "block_number": record.ordering_key.ledger_position,
        "transaction_hash": transaction_hash,
        "event_position": event_position,
        "event_name": event_name,
        "message_id": required_string(decoded, "message_id")?
    });

    if event_name == "MessageSent" {
        value["sender"] = json!(required_string(decoded, "sender")?);
        value["recipient"] = json!(required_string(decoded, "recipient")?);
        value["destination_chain"] = json!(required_u64(decoded, "destination_chain")?);
        value["amount"] = json!(required_u64(decoded, "amount")?);
        if let Some(route_name) = route_name {
            value["route_name"] = json!(route_name);
        }
    } else {
        value["relayer"] = json!(required_string(decoded, "relayer")?);
    }

    Ok(value)
}

pub struct BridgeSchemaInitializer;

impl ApplicationSchemaInitializer for BridgeSchemaInitializer {
    fn initialize_schema<'a>(
        &'a self,
        context: SchemaInitializationContext<'a>,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            match context.store().database_kind() {
                ApplicationDatabaseKind::Sqlite => context.store().execute_sql(SQLITE_SCHEMA).await,
                ApplicationDatabaseKind::Postgres => Err(ProcessorError::config(
                    "bridge processor example supports sqlite only",
                )),
            }
        })
    }
}

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS bridge_processed_events (
    chain TEXT NOT NULL,
    source_key TEXT NOT NULL,
    event_name TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    transaction_hash TEXT NOT NULL,
    event_position INTEGER NOT NULL,
    PRIMARY KEY (chain, source_key)
);

CREATE TABLE IF NOT EXISTS bridge_messages (
    chain TEXT NOT NULL,
    message_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    destination_chain INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL,
    route_name TEXT,
    sent_block INTEGER NOT NULL,
    delivered_block INTEGER,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (chain, message_id)
);

CREATE TABLE IF NOT EXISTS bridge_deliveries (
    chain TEXT NOT NULL,
    message_id TEXT NOT NULL,
    relayer TEXT NOT NULL,
    delivered_block INTEGER NOT NULL,
    transaction_hash TEXT NOT NULL,
    PRIMARY KEY (chain, message_id)
);

CREATE TABLE IF NOT EXISTS bridge_route_counters (
    chain TEXT NOT NULL,
    destination_chain INTEGER NOT NULL,
    route_name TEXT,
    sent_count INTEGER NOT NULL DEFAULT 0,
    delivered_count INTEGER NOT NULL DEFAULT 0,
    total_amount INTEGER NOT NULL DEFAULT 0,
    last_block INTEGER NOT NULL,
    PRIMARY KEY (chain, destination_chain)
);
"#;

#[derive(Clone)]
pub struct SqliteBridgeStore {
    pool: SqlitePool,
    url: String,
}

impl SqliteBridgeStore {
    pub async fn connect(url: &str) -> Result<Self, ProcessorError> {
        ensure_sqlite_parent_dir(url)?;
        let options = SqliteConnectOptions::from_str(url)
            .map_err(|error| ProcessorError::config(error.to_string()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
            url: url.to_owned(),
        })
    }

    pub async fn initialize_application_schema(
        &self,
        application: &str,
        index: &str,
        initializer: &dyn ApplicationSchemaInitializer,
    ) -> Result<(), ProcessorError> {
        initializer
            .initialize_schema(SchemaInitializationContext::new(application, index, self))
            .await
    }

    pub async fn begin(&self) -> Result<BridgeTransaction<'_>, ProcessorError> {
        Ok(BridgeTransaction {
            transaction: Mutex::new(Some(self.pool.begin().await?)),
        })
    }

    pub async fn messages(&self, account: Option<&str>) -> Result<Vec<Value>, ProcessorError> {
        let rows = if let Some(account) = account {
            sqlx::query(
                r#"
                SELECT * FROM bridge_messages
                WHERE sender = ? OR recipient = ?
                ORDER BY message_id
                "#,
            )
            .bind(account)
            .bind(account)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT * FROM bridge_messages ORDER BY message_id")
                .fetch_all(&self.pool)
                .await?
        };
        rows.into_iter().map(sqlite_row_json).collect()
    }

    pub async fn route_counters(&self) -> Result<Vec<Value>, ProcessorError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM bridge_route_counters
            ORDER BY destination_chain
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(sqlite_row_json).collect()
    }

    pub async fn processed_events(&self) -> Result<i64, ProcessorError> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM bridge_processed_events")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get("count")?)
    }
}

impl fmt::Debug for SqliteBridgeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteBridgeStore")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

impl TransactionalApplicationStore for SqliteBridgeStore {
    fn schema_store(&self) -> Option<&dyn ApplicationSchemaStore> {
        Some(self)
    }

    fn begin_transaction<'a>(
        &'a self,
    ) -> ProcessorFuture<
        'a,
        Result<Box<dyn ApplicationStoreTransaction + Send + Sync + 'a>, ProcessorError>,
    > {
        Box::pin(async move {
            Ok(Box::new(self.begin().await?)
                as Box<dyn ApplicationStoreTransaction + Send + Sync + 'a>)
        })
    }
}

impl ApplicationEntityQueryStore for SqliteBridgeStore {
    fn query_json<'store>(
        &'store self,
        query: ApplicationEntityReadQuery,
    ) -> ProcessorFuture<'store, Result<Vec<Value>, ProcessorError>> {
        Box::pin(async move {
            validate_read_statement(query.statement())?;
            let mut sql = sqlx::query(AssertSqlSafe(query.statement().to_owned()));
            for argument in query.arguments() {
                sql = bind_sqlite_json(sql, argument)?;
            }
            let rows = sql.fetch_all(&self.pool).await?;
            rows.into_iter().map(sqlite_row_json).collect()
        })
    }
}

impl ApplicationSchemaStore for SqliteBridgeStore {
    fn database_kind(&self) -> ApplicationDatabaseKind {
        ApplicationDatabaseKind::Sqlite
    }

    fn execute_sql<'a>(
        &'a self,
        statement: &'a str,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            sqlx::raw_sql(AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
            Ok(())
        })
    }
}

pub struct BridgeTransaction<'store> {
    transaction: Mutex<Option<Transaction<'store, Sqlite>>>,
}

impl ApplicationStore for BridgeTransaction<'_> {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        value: Value,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let source_key = key
                .strip_prefix("bridge_event:")
                .ok_or_else(|| ProcessorError::user("unsupported bridge entity key"))?;
            if required_string(&value, "source_key")? != source_key {
                return Err(ProcessorError::data(
                    "bridge event key does not match event payload",
                ));
            }

            let mut guard = self.transaction.lock().await;
            let transaction = guard
                .as_mut()
                .ok_or_else(|| ProcessorError::user("transaction is already closed"))?;
            if !insert_processed_event(transaction, &value).await? {
                return Ok(());
            }
            match required_string(&value, "event_name")? {
                "MessageSent" => apply_message_sent(transaction, &value).await,
                "MessageDelivered" => apply_message_delivered(transaction, &value).await,
                _ => Ok(()),
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let source_key = key
                .strip_prefix("bridge_event:")
                .ok_or_else(|| ProcessorError::user("unsupported bridge entity key"))?;
            let mut guard = self.transaction.lock().await;
            let transaction = guard
                .as_mut()
                .ok_or_else(|| ProcessorError::user("transaction is already closed"))?;
            sqlx::query("DELETE FROM bridge_processed_events WHERE source_key = ?")
                .bind(source_key)
                .execute(&mut **transaction)
                .await?;
            Ok(())
        })
    }
}

impl ApplicationStoreTransaction for BridgeTransaction<'_> {
    fn commit<'a>(&'a self) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let transaction = self
                .transaction
                .lock()
                .await
                .take()
                .ok_or_else(|| ProcessorError::user("transaction is already closed"))?;
            transaction.commit().await?;
            Ok(())
        })
    }

    fn rollback<'a>(&'a self) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            if let Some(transaction) = self.transaction.lock().await.take() {
                transaction.rollback().await?;
            }
            Ok(())
        })
    }
}

async fn insert_processed_event(
    transaction: &mut Transaction<'_, Sqlite>,
    value: &Value,
) -> Result<bool, ProcessorError> {
    let result = sqlx::query(
        r#"
        INSERT OR IGNORE INTO bridge_processed_events (
            chain, source_key, event_name, block_number, transaction_hash, event_position
        )
        VALUES (?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(required_string(value, "chain")?)
    .bind(required_string(value, "source_key")?)
    .bind(required_string(value, "event_name")?)
    .bind(required_i64(value, "block_number")?)
    .bind(required_string(value, "transaction_hash")?)
    .bind(required_i64(value, "event_position")?)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn apply_message_sent(
    transaction: &mut Transaction<'_, Sqlite>,
    value: &Value,
) -> Result<(), ProcessorError> {
    sqlx::query(
        r#"
        INSERT INTO bridge_messages (
            chain, message_id, sender, recipient, destination_chain, amount, status,
            route_name, sent_block, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'sent', ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(chain, message_id) DO NOTHING
        "#,
    )
    .bind(required_string(value, "chain")?)
    .bind(required_string(value, "message_id")?)
    .bind(required_string(value, "sender")?)
    .bind(required_string(value, "recipient")?)
    .bind(required_i64(value, "destination_chain")?)
    .bind(required_i64(value, "amount")?)
    .bind(optional_string(value, "route_name"))
    .bind(required_i64(value, "block_number")?)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO bridge_route_counters (
            chain, destination_chain, route_name, sent_count, delivered_count, total_amount, last_block
        )
        VALUES (?, ?, ?, 1, 0, ?, ?)
        ON CONFLICT(chain, destination_chain) DO UPDATE SET
            route_name = COALESCE(excluded.route_name, bridge_route_counters.route_name),
            sent_count = bridge_route_counters.sent_count + 1,
            total_amount = bridge_route_counters.total_amount + excluded.total_amount,
            last_block = excluded.last_block
        "#,
    )
    .bind(required_string(value, "chain")?)
    .bind(required_i64(value, "destination_chain")?)
    .bind(optional_string(value, "route_name"))
    .bind(required_i64(value, "amount")?)
    .bind(required_i64(value, "block_number")?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn apply_message_delivered(
    transaction: &mut Transaction<'_, Sqlite>,
    value: &Value,
) -> Result<(), ProcessorError> {
    sqlx::query(
        r#"
        INSERT INTO bridge_deliveries (
            chain, message_id, relayer, delivered_block, transaction_hash
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(chain, message_id) DO NOTHING
        "#,
    )
    .bind(required_string(value, "chain")?)
    .bind(required_string(value, "message_id")?)
    .bind(required_string(value, "relayer")?)
    .bind(required_i64(value, "block_number")?)
    .bind(required_string(value, "transaction_hash")?)
    .execute(&mut **transaction)
    .await?;

    let message = sqlx::query(
        r#"
        SELECT destination_chain
        FROM bridge_messages
        WHERE chain = ? AND message_id = ?
        "#,
    )
    .bind(required_string(value, "chain")?)
    .bind(required_string(value, "message_id")?)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(message) = message {
        let destination_chain: i64 = message.try_get("destination_chain")?;
        sqlx::query(
            r#"
            UPDATE bridge_messages
            SET status = 'delivered', delivered_block = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE chain = ? AND message_id = ?
            "#,
        )
        .bind(required_i64(value, "block_number")?)
        .bind(required_string(value, "chain")?)
        .bind(required_string(value, "message_id")?)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE bridge_route_counters
            SET delivered_count = delivered_count + 1, last_block = ?
            WHERE chain = ? AND destination_chain = ?
            "#,
        )
        .bind(required_i64(value, "block_number")?)
        .bind(required_string(value, "chain")?)
        .bind(destination_chain)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn decoded_string(record: &EventRecord, field: &str) -> Option<String> {
    record
        .decoded
        .as_ref()
        .and_then(|value| value.get(field))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn decoded_u64(record: &EventRecord, field: &str) -> Result<u64, ProcessorError> {
    let decoded = record
        .decoded
        .as_ref()
        .ok_or_else(|| ProcessorError::data("bridge event requires decoded fields"))?;
    required_u64(decoded, field)
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, ProcessorError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ProcessorError::data(format!("{field} must be a string")))
}

fn optional_string<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn required_u64(value: &Value, field: &str) -> Result<u64, ProcessorError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| ProcessorError::data(format!("{field} must be an unsigned integer")))
}

fn required_i64(value: &Value, field: &str) -> Result<i64, ProcessorError> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ProcessorError::data(format!("{field} must be an integer")))
}

fn validate_read_statement(statement: &str) -> Result<(), ProcessorError> {
    let trimmed = statement.trim_start();
    let first = trimmed
        .split(|character: char| character.is_whitespace() || character == '(')
        .find(|part| !part.is_empty())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if first != "select" {
        return Err(ProcessorError::user(
            "application entity GraphQL queries must be read-only SELECT statements",
        ));
    }
    if trimmed.trim_end().trim_end_matches(';').contains(';') {
        return Err(ProcessorError::user(
            "application entity GraphQL queries must contain a single statement",
        ));
    }
    Ok(())
}

fn sqlite_row_json(row: SqliteRow) -> Result<Value, ProcessorError> {
    let mut object = serde_json::Map::new();
    for column in row.columns() {
        let name = column.name();
        let value = row.try_get_raw(name)?;
        if value.is_null() {
            object.insert(name.to_owned(), Value::Null);
            continue;
        }
        let type_name = column.type_info().name().to_ascii_uppercase();
        let json_value = if type_name.contains("INT") {
            Value::Number(Number::from(row.try_get::<i64, _>(name)?))
        } else {
            Value::String(row.try_get::<String, _>(name)?)
        };
        object.insert(name.to_owned(), json_value);
    }
    Ok(Value::Object(object))
}

fn bind_sqlite_json<'query>(
    sql: sqlx::query::Query<'query, Sqlite, sqlx::sqlite::SqliteArguments>,
    value: &'query Value,
) -> Result<sqlx::query::Query<'query, Sqlite, sqlx::sqlite::SqliteArguments>, ProcessorError> {
    Ok(match value {
        Value::Null => sql.bind(Option::<String>::None),
        Value::Bool(value) => sql.bind(*value),
        Value::Number(value) => bind_number_sqlite(sql, value)?,
        Value::String(value) => sql.bind(value),
        Value::Array(_) | Value::Object(_) => sql.bind(value.to_string()),
    })
}

fn bind_number_sqlite<'query>(
    sql: sqlx::query::Query<'query, Sqlite, sqlx::sqlite::SqliteArguments>,
    value: &Number,
) -> Result<sqlx::query::Query<'query, Sqlite, sqlx::sqlite::SqliteArguments>, ProcessorError> {
    if let Some(value) = value.as_i64() {
        Ok(sql.bind(value))
    } else if let Some(value) = value.as_u64() {
        let value = i64::try_from(value)
            .map_err(|_| ProcessorError::user("application entity query integer is too large"))?;
        Ok(sql.bind(value))
    } else if let Some(value) = value.as_f64() {
        Ok(sql.bind(value))
    } else {
        Err(ProcessorError::user(
            "application entity query number is not supported",
        ))
    }
}

fn ensure_sqlite_parent_dir(url: &str) -> Result<(), ProcessorError> {
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

fn sqlite_file_path(url: &str) -> Option<&Path> {
    if url == "sqlite::memory:" || url.contains("mode=memory") {
        return None;
    }
    let path = url.strip_prefix("sqlite:")?;
    let path = path.split_once('?').map(|(path, _)| path).unwrap_or(path);
    let path = path.strip_prefix("//").unwrap_or(path);
    if path.is_empty() {
        None
    } else {
        Some(Path::new(path))
    }
}

fn redact_url(url: &str) -> String {
    if url.contains("://") {
        "<redacted>".to_owned()
    } else {
        url.to_owned()
    }
}
