use std::{fmt, path::PathBuf, str::FromStr, sync::Arc};

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object as DynamicObject, Schema as DynamicSchema,
    TypeRef,
};
use datalens_indexer::{
    ApplicationEntityQueryStore, ApplicationEntityReadQuery, ApplicationGraphqlSchemaContext,
    ApplicationGraphqlSchemaHook, IndexerError,
    sdk::{
        ApplicationDatabaseKind, ApplicationProcessor, ApplicationSchemaInitializer,
        ApplicationSchemaStore, ApplicationStore, ApplicationStoreTransaction, EventBatch,
        EventRecord, ProcessResult, ProcessorContext, ProcessorError, ProcessorFuture,
        SchemaInitializationContext, TransactionalApplicationStore,
    },
};
use serde::Deserialize;
use serde_json::{Map, Number, Value, json};
use sqlx::{
    AssertSqlSafe, Column, Row, Sqlite, SqlitePool, Transaction, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tokio::sync::Mutex;

const TRANSFER_TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventCounter {
    pub chain: String,
    pub event_name: String,
    pub total_count: i64,
    pub last_block: i64,
    pub last_source_key: String,
}

#[derive(Clone, Debug, Default)]
pub struct EventCounterProcessor {
    event_name: String,
}

impl EventCounterProcessor {
    pub fn new(event_name: impl Into<String>) -> Self {
        Self {
            event_name: event_name.into(),
        }
    }
}

impl ApplicationProcessor for EventCounterProcessor {
    fn process<'a>(
        &'a self,
        context: &'a mut ProcessorContext<'a>,
        batch: &'a EventBatch,
    ) -> ProcessorFuture<'a, Result<ProcessResult, ProcessorError>> {
        Box::pin(async move {
            let store = context.store().ok_or_else(|| {
                ProcessorError::config("event counter processor requires a store")
            })?;
            let target = if self.event_name.is_empty() {
                "Transfer"
            } else {
                self.event_name.as_str()
            };
            let mut total_count = 0_i64;
            let mut last_block = 0_i64;
            let mut last_source_key = String::new();

            for record in batch.records() {
                if event_name(record).as_deref() != Some(target) {
                    continue;
                }
                total_count += 1;
                last_block = i64::try_from(record.ordering_key.ledger_position)
                    .map_err(|_| ProcessorError::data("ledger position is too large"))?;
                last_source_key = record.source_key.clone();
            }

            if total_count > 0 {
                store
                    .upsert_json(
                        &counter_key(&batch.chain().key_prefix(), target),
                        json!({
                            "chain": batch.chain().key_prefix(),
                            "event_name": target,
                            "delta_count": total_count,
                            "last_block": last_block,
                            "last_source_key": last_source_key,
                        }),
                    )
                    .await?;
            }

            Ok(ProcessResult::success(batch.checkpoint_cursor().clone())
                .with_processed_records(usize::try_from(total_count).unwrap_or(usize::MAX)))
        })
    }
}

pub struct EventCounterSchemaInitializer;

impl ApplicationSchemaInitializer for EventCounterSchemaInitializer {
    fn initialize_schema<'a>(
        &'a self,
        context: SchemaInitializationContext<'a>,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            match context.store().database_kind() {
                ApplicationDatabaseKind::Sqlite => {
                    context
                        .store()
                        .execute_sql(SQLITE_EVENT_COUNTER_SCHEMA)
                        .await
                }
                ApplicationDatabaseKind::Postgres => Err(ProcessorError::config(
                    "event counter example store supports sqlite only",
                )),
            }
        })
    }
}

const SQLITE_EVENT_COUNTER_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS event_counters (
    chain TEXT NOT NULL,
    event_name TEXT NOT NULL,
    total_count INTEGER NOT NULL,
    last_block INTEGER NOT NULL,
    last_source_key TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (chain, event_name)
)
"#;

#[derive(Clone)]
pub struct SqliteEventCounterStore {
    pool: SqlitePool,
    url: String,
}

impl SqliteEventCounterStore {
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

    pub async fn begin(&self) -> Result<EventCounterTransaction<'_>, ProcessorError> {
        Ok(EventCounterTransaction {
            transaction: Mutex::new(Some(self.pool.begin().await?)),
        })
    }

    pub async fn increment_counter(
        &self,
        chain: &str,
        event_name: &str,
        delta_count: i64,
        last_block: i64,
        last_source_key: &str,
    ) -> Result<(), ProcessorError> {
        sqlx::query(
            r#"
            INSERT INTO event_counters (
                chain, event_name, total_count, last_block, last_source_key, updated_at
            )
            VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(chain, event_name) DO UPDATE SET
                total_count = event_counters.total_count + excluded.total_count,
                last_block = excluded.last_block,
                last_source_key = excluded.last_source_key,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(chain)
        .bind(event_name)
        .bind(delta_count)
        .bind(last_block)
        .bind(last_source_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn event_counters(
        &self,
        event_name: Option<&str>,
    ) -> Result<Vec<EventCounter>, ProcessorError> {
        let rows = if let Some(event_name) = event_name {
            sqlx::query(
                r#"
                SELECT chain, event_name, total_count, last_block, last_source_key
                FROM event_counters
                WHERE event_name = ?
                ORDER BY chain, event_name
                "#,
            )
            .bind(event_name)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT chain, event_name, total_count, last_block, last_source_key
                FROM event_counters
                ORDER BY chain, event_name
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(event_counter_from_row).collect()
    }
}

impl fmt::Debug for SqliteEventCounterStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteEventCounterStore")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

impl TransactionalApplicationStore for SqliteEventCounterStore {
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

impl ApplicationEntityQueryStore for SqliteEventCounterStore {
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

impl ApplicationSchemaStore for SqliteEventCounterStore {
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

pub struct EventCounterTransaction<'store> {
    transaction: Mutex<Option<Transaction<'store, Sqlite>>>,
}

impl ApplicationStore for EventCounterTransaction<'_> {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        value: Value,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let (chain, event_name) = parse_counter_key(key)?;
            let delta_count = required_i64(&value, "delta_count")?;
            let last_block = required_i64(&value, "last_block")?;
            let last_source_key = required_string(&value, "last_source_key")?;
            let value_chain = required_string(&value, "chain")?;
            let value_event_name = required_string(&value, "event_name")?;
            if value_chain != chain || value_event_name != event_name {
                return Err(ProcessorError::user(
                    "event counter key does not match counter payload",
                ));
            }

            let mut guard = self.transaction.lock().await;
            let transaction = guard
                .as_mut()
                .ok_or_else(|| ProcessorError::user("transaction is already closed"))?;
            sqlx::query(
                r#"
                INSERT INTO event_counters (
                    chain, event_name, total_count, last_block, last_source_key, updated_at
                )
                VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                ON CONFLICT(chain, event_name) DO UPDATE SET
                    total_count = event_counters.total_count + excluded.total_count,
                    last_block = excluded.last_block,
                    last_source_key = excluded.last_source_key,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(chain)
            .bind(event_name)
            .bind(delta_count)
            .bind(last_block)
            .bind(last_source_key)
            .execute(&mut **transaction)
            .await?;
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let (chain, event_name) = parse_counter_key(key)?;
            let mut guard = self.transaction.lock().await;
            let transaction = guard
                .as_mut()
                .ok_or_else(|| ProcessorError::user("transaction is already closed"))?;
            sqlx::query("DELETE FROM event_counters WHERE chain = ? AND event_name = ?")
                .bind(chain)
                .bind(event_name)
                .execute(&mut **transaction)
                .await?;
            Ok(())
        })
    }
}

impl ApplicationStoreTransaction for EventCounterTransaction<'_> {
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

pub struct EventCounterGraphqlSchema;

impl ApplicationGraphqlSchemaHook for EventCounterGraphqlSchema {
    fn build_schema(
        &self,
        context: ApplicationGraphqlSchemaContext,
    ) -> Result<DynamicSchema, IndexerError> {
        let counter = DynamicObject::new("EventCounter")
            .field(json_string_field("chain"))
            .field(json_string_field("eventName"))
            .field(json_i32_field("totalCount"))
            .field(json_i32_field("lastBlock"))
            .field(json_string_field("lastSourceKey"));
        let query = DynamicObject::new("Query").field(
            Field::new(
                "eventCounters",
                TypeRef::named_nn_list_nn("EventCounter"),
                |ctx| {
                    let store = ctx
                        .data::<Arc<dyn ApplicationEntityQueryStore>>()
                        .expect("entity store")
                        .clone();
                    let event_name = ctx
                        .args
                        .try_get("eventName")
                        .ok()
                        .and_then(|value| value.string().ok())
                        .map(str::to_owned);
                    FieldFuture::new(async move {
                        let mut query = ApplicationEntityReadQuery::new(
                            r#"
                            SELECT
                                chain,
                                event_name AS eventName,
                                total_count AS totalCount,
                                last_block AS lastBlock,
                                last_source_key AS lastSourceKey
                            FROM event_counters
                            "#,
                        );
                        if let Some(event_name) = event_name {
                            query = ApplicationEntityReadQuery::new(
                                r#"
                                SELECT
                                    chain,
                                    event_name AS eventName,
                                    total_count AS totalCount,
                                    last_block AS lastBlock,
                                    last_source_key AS lastSourceKey
                                FROM event_counters
                                WHERE event_name = ?
                                "#,
                            )
                            .bind(event_name);
                        }
                        let rows = store
                            .query_json(query)
                            .await
                            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
                        Ok(Some(FieldValue::list(
                            rows.into_iter().map(FieldValue::owned_any),
                        )))
                    })
                },
            )
            .argument(InputValue::new(
                "eventName",
                TypeRef::named(TypeRef::STRING),
            )),
        );

        DynamicSchema::build("Query", None, None)
            .data(context.entity_store())
            .register(query)
            .register(counter)
            .finish()
            .map_err(|error| IndexerError::Config(format!("event counter graphql schema: {error}")))
    }
}

#[derive(Debug, Deserialize)]
pub struct EventCounterExampleConfig {
    pub application_store: ApplicationStoreConfig,
}

#[derive(Debug, Deserialize)]
pub struct ApplicationStoreConfig {
    pub driver: String,
    pub url: String,
}

impl EventCounterExampleConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, ProcessorError> {
        toml::from_str(input).map_err(|error| ProcessorError::config(error.to_string()))
    }

    pub async fn connect_store(&self) -> Result<SqliteEventCounterStore, ProcessorError> {
        if self.application_store.driver != "sqlite" {
            return Err(ProcessorError::config(
                "event counter example application_store.driver must be sqlite",
            ));
        }
        SqliteEventCounterStore::connect(&self.application_store.url).await
    }
}

fn event_name(record: &EventRecord) -> Option<String> {
    record
        .decoded
        .as_ref()
        .and_then(decoded_event_name)
        .or_else(|| decoded_event_name(&record.payload))
        .or_else(|| topic_event_name(&record.payload))
}

fn decoded_event_name(value: &Value) -> Option<String> {
    value
        .get("event_name")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn topic_event_name(value: &Value) -> Option<String> {
    let topic0 = value
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)?;
    if topic0.eq_ignore_ascii_case(TRANSFER_TOPIC0) {
        Some("Transfer".to_owned())
    } else {
        None
    }
}

fn counter_key(chain: &str, event_name: &str) -> String {
    format!("event_counter:{chain}:{event_name}")
}

fn parse_counter_key(key: &str) -> Result<(&str, &str), ProcessorError> {
    let Some(rest) = key.strip_prefix("event_counter:") else {
        return Err(ProcessorError::user("unsupported event counter key"));
    };
    let Some((family, rest)) = rest.split_once('/') else {
        return Err(ProcessorError::user(
            "event counter key is missing chain family",
        ));
    };
    let Some((chain, rest)) = rest.split_once('/') else {
        return Err(ProcessorError::user(
            "event counter key is missing chain name",
        ));
    };
    let Some((network, event_name)) = rest.split_once(':') else {
        return Err(ProcessorError::user(
            "event counter key is missing event name",
        ));
    };
    let chain_end = "event_counter:".len() + family.len() + 1 + chain.len() + 1 + network.len();
    Ok((&key["event_counter:".len()..chain_end], event_name))
}

fn required_i64(value: &Value, field: &str) -> Result<i64, ProcessorError> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ProcessorError::data(format!("{field} must be an integer")))
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, ProcessorError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ProcessorError::data(format!("{field} must be a string")))
}

fn event_counter_from_row(row: SqliteRow) -> Result<EventCounter, ProcessorError> {
    Ok(EventCounter {
        chain: row.try_get("chain")?,
        event_name: row.try_get("event_name")?,
        total_count: row.try_get("total_count")?,
        last_block: row.try_get("last_block")?,
        last_source_key: row.try_get("last_source_key")?,
    })
}

fn json_string_field(name: &'static str) -> Field {
    Field::new(name, TypeRef::named_nn(TypeRef::STRING), move |ctx| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value(json_string(
                ctx.parent_value.try_downcast_ref::<Value>()?,
                name,
            ))))
        })
    })
}

fn json_i32_field(name: &'static str) -> Field {
    Field::new(name, TypeRef::named_nn(TypeRef::INT), move |ctx| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value(json_i32(
                ctx.parent_value.try_downcast_ref::<Value>()?,
                name,
            ))))
        })
    })
}

fn json_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn json_i32(value: &Value, field: &str) -> i32 {
    value
        .get(field)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
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
            "event counter GraphQL queries must be read-only SELECT statements",
        ));
    }
    if trimmed.trim_end().trim_end_matches(';').contains(';') {
        return Err(ProcessorError::user(
            "event counter GraphQL queries must contain a single statement",
        ));
    }
    Ok(())
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
            .map_err(|_| ProcessorError::user("event counter query integer is too large"))?;
        Ok(sql.bind(value))
    } else if let Some(value) = value.as_f64() {
        Ok(sql.bind(value))
    } else {
        Err(ProcessorError::user(
            "event counter query number is not supported",
        ))
    }
}

fn sqlite_row_json(row: SqliteRow) -> Result<Value, ProcessorError> {
    let mut object = Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        let value = if row.try_get_raw(index)?.is_null() {
            Value::Null
        } else {
            match column.type_info().name().to_ascii_uppercase().as_str() {
                "INTEGER" | "INT" | "BIGINT" => Value::from(row.try_get::<i64, _>(index)?),
                "REAL" | "FLOAT" | "DOUBLE" => Value::from(row.try_get::<f64, _>(index)?),
                "BOOLEAN" | "BOOL" => Value::from(row.try_get::<bool, _>(index)?),
                _ => Value::from(row.try_get::<String, _>(index)?),
            }
        };
        object.insert(column.name().to_owned(), value);
    }
    Ok(Value::Object(object))
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
        Some(PathBuf::from(path))
    }
}

fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((_, host)) = rest.split_once('@') else {
        return url.to_owned();
    };
    format!("{scheme}://<redacted>@{host}")
}
