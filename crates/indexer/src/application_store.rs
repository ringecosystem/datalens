use std::{
    fmt, io,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde_json::{Map, Number, Value};
use sqlx::{
    AssertSqlSafe, Column, PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction, TypeInfo,
    ValueRef,
    postgres::{PgPoolOptions, PgQueryResult},
    sqlite::SqliteRow,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteQueryResult},
};
use tokio::runtime::Runtime;

use crate::sdk::{
    ApplicationDatabaseKind, ApplicationSchemaInitializer, ApplicationSchemaStore,
    CheckpointCursor, ProcessorError, ProcessorFuture, SchemaInitializationContext,
};

pub trait ProcessorApplicationEntityStore: Send + Sync {
    type Transaction<'store>
    where
        Self: 'store;

    fn begin_transaction<'store>(
        &'store self,
    ) -> ProcessorFuture<'store, Result<Self::Transaction<'store>, ProcessorError>>;
}

pub trait ApplicationEntityQueryStore: Send + Sync {
    fn query_json<'store>(
        &'store self,
        query: ApplicationEntityReadQuery,
    ) -> ProcessorFuture<'store, Result<Vec<Value>, ProcessorError>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEntityReadQuery {
    statement: String,
    arguments: Vec<Value>,
}

impl ApplicationEntityReadQuery {
    pub fn new(statement: impl Into<String>) -> Self {
        Self {
            statement: statement.into(),
            arguments: Vec::new(),
        }
    }

    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.arguments.push(value.into());
        self
    }

    pub fn statement(&self) -> &str {
        &self.statement
    }

    pub fn arguments(&self) -> &[Value] {
        &self.arguments
    }
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
            FROM datalens_processor_checkpoints
            WHERE checkpoint_key = ?
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(value,)| CheckpointCursor::new(key, value)))
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

    async fn initialize_schema(&self) -> io::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS datalens_processor_checkpoints (
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

impl ApplicationEntityQueryStore for SqliteApplicationEntityStore {
    fn query_json<'store>(
        &'store self,
        query: ApplicationEntityReadQuery,
    ) -> ProcessorFuture<'store, Result<Vec<Value>, ProcessorError>> {
        Box::pin(async move {
            validate_read_statement(query.statement())?;
            let mut sql = sqlx::query(AssertSqlSafe(query.statement.clone()));
            for argument in query.arguments() {
                sql = bind_sqlite_json(sql, argument)?;
            }
            let rows = sql.fetch_all(&self.pool).await?;
            rows.into_iter().map(sqlite_row_json).collect()
        })
    }
}

impl ApplicationSchemaStore for SqliteApplicationEntityStore {
    fn database_kind(&self) -> ApplicationDatabaseKind {
        ApplicationDatabaseKind::Sqlite
    }

    fn execute_sql<'a>(
        &'a self,
        statement: &'a str,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
            Ok(())
        })
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
            INSERT INTO datalens_processor_checkpoints (checkpoint_key, checkpoint_value, updated_at)
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
            FROM datalens_processor_checkpoints
            WHERE checkpoint_key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(value,)| CheckpointCursor::new(key, value)))
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

    async fn initialize_schema(&self) -> io::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS datalens_processor_checkpoints (
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

impl ApplicationEntityQueryStore for PostgresApplicationEntityStore {
    fn query_json<'store>(
        &'store self,
        query: ApplicationEntityReadQuery,
    ) -> ProcessorFuture<'store, Result<Vec<Value>, ProcessorError>> {
        Box::pin(async move {
            validate_read_statement(query.statement())?;
            let mut sql = sqlx::query(AssertSqlSafe(query.statement.clone()));
            for argument in query.arguments() {
                sql = bind_postgres_json(sql, argument)?;
            }
            let rows = sql.fetch_all(&self.pool).await?;
            rows.into_iter().map(postgres_row_json).collect()
        })
    }
}

impl ApplicationSchemaStore for PostgresApplicationEntityStore {
    fn database_kind(&self) -> ApplicationDatabaseKind {
        ApplicationDatabaseKind::Postgres
    }

    fn execute_sql<'a>(
        &'a self,
        statement: &'a str,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
            Ok(())
        })
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
            INSERT INTO datalens_processor_checkpoints (checkpoint_key, checkpoint_value, updated_at)
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

fn bind_postgres_json<'query>(
    sql: sqlx::query::Query<'query, Postgres, sqlx::postgres::PgArguments>,
    value: &'query Value,
) -> Result<sqlx::query::Query<'query, Postgres, sqlx::postgres::PgArguments>, ProcessorError> {
    Ok(match value {
        Value::Null => sql.bind(Option::<String>::None),
        Value::Bool(value) => sql.bind(*value),
        Value::Number(value) => bind_number_postgres(sql, value)?,
        Value::String(value) => sql.bind(value),
        Value::Array(_) | Value::Object(_) => sql.bind(value.to_string()),
    })
}

fn bind_number_postgres<'query>(
    sql: sqlx::query::Query<'query, Postgres, sqlx::postgres::PgArguments>,
    value: &Number,
) -> Result<sqlx::query::Query<'query, Postgres, sqlx::postgres::PgArguments>, ProcessorError> {
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

fn postgres_row_json(row: sqlx::postgres::PgRow) -> Result<Value, ProcessorError> {
    let mut object = Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        let value = if row.try_get_raw(index)?.is_null() {
            Value::Null
        } else {
            match column.type_info().name() {
                "INT2" => Value::from(i64::from(row.try_get::<i16, _>(index)?)),
                "INT4" => Value::from(i64::from(row.try_get::<i32, _>(index)?)),
                "INT8" => Value::from(row.try_get::<i64, _>(index)?),
                "FLOAT4" => Value::from(f64::from(row.try_get::<f32, _>(index)?)),
                "FLOAT8" => Value::from(row.try_get::<f64, _>(index)?),
                "BOOL" => Value::from(row.try_get::<bool, _>(index)?),
                _ => Value::from(row.try_get::<String, _>(index)?),
            }
        };
        object.insert(column.name().to_owned(), value);
    }
    Ok(Value::Object(object))
}
