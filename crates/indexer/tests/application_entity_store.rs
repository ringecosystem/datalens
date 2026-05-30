use datalens_indexer::{
    CheckpointCursor, PostgresApplicationEntityStore, ProcessorError, SqliteApplicationEntityStore,
    sdk::{ApplicationSchemaInitializer, ProcessorFuture, SchemaInitializationContext},
};
use sqlx::Row;

#[test]
fn test_sqlite_application_entity_store_commits_rows_and_checkpoint_atomically() {
    let store = SqliteApplicationEntityStore::connect(&sqlite_test_url("commit-atomic"))
        .expect("sqlite application store connects");
    tokio_runtime().block_on(async {
        let cursor = CheckpointCursor::new("processor/payments/ethereum/logs", "block:42");

        let mut transaction = store.begin().await.expect("begin transaction");
        sqlx::query("CREATE TABLE payment_transfers (event_id TEXT PRIMARY KEY, amount INTEGER)")
            .execute(transaction.sqlite())
            .await
            .expect("create application table");
        sqlx::query("INSERT INTO payment_transfers (event_id, amount) VALUES (?, ?)")
            .bind("ethereum:42:0:1")
            .bind(7_i64)
            .execute(transaction.sqlite())
            .await
            .expect("insert application row");
        transaction
            .put_checkpoint(&cursor)
            .await
            .expect("put checkpoint");
        transaction.commit().await.expect("commit transaction");

        let mut transaction = store.begin().await.expect("begin read transaction");
        let row_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM payment_transfers")
            .fetch_one(transaction.sqlite())
            .await
            .expect("query application table")
            .get("count");
        transaction
            .rollback()
            .await
            .expect("rollback read transaction");

        assert_eq!(row_count, 1);
        assert_eq!(
            store
                .checkpoint(cursor.key())
                .await
                .expect("read checkpoint")
                .as_ref()
                .map(CheckpointCursor::value),
            Some("block:42")
        );
    });
}

#[test]
fn test_sqlite_application_entity_store_rolls_back_rows_and_checkpoint_on_error() {
    let store = SqliteApplicationEntityStore::connect(&sqlite_test_url("rollback"))
        .expect("sqlite application store connects");
    tokio_runtime().block_on(async {
        let mut transaction = store.begin().await.expect("begin schema transaction");
        sqlx::query("CREATE TABLE payment_transfers (event_id TEXT PRIMARY KEY, amount INTEGER)")
            .execute(transaction.sqlite())
            .await
            .expect("create application table");
        transaction
            .commit()
            .await
            .expect("commit schema transaction");
        let cursor = CheckpointCursor::new("processor/payments/ethereum/logs", "block:43");

        let mut transaction = store.begin().await.expect("begin transaction");
        sqlx::query("INSERT INTO payment_transfers (event_id, amount) VALUES (?, ?)")
            .bind("ethereum:43:0:1")
            .bind(9_i64)
            .execute(transaction.sqlite())
            .await
            .expect("insert application row");
        transaction
            .put_checkpoint(&cursor)
            .await
            .expect("put checkpoint");
        let error = ProcessorError::user("application invariant failed");
        transaction.rollback().await.expect("rollback transaction");

        let mut transaction = store.begin().await.expect("begin read transaction");
        let row_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM payment_transfers")
            .fetch_one(transaction.sqlite())
            .await
            .expect("query application table")
            .get("count");
        transaction
            .rollback()
            .await
            .expect("rollback read transaction");

        assert_eq!(error.kind().as_str(), "user processor error");
        assert_eq!(row_count, 0);
        assert_eq!(
            store
                .checkpoint(cursor.key())
                .await
                .expect("read checkpoint"),
            None
        );
    });
}

#[test]
fn test_sqlite_application_schema_initializer_creates_application_tables_idempotently() {
    let store = SqliteApplicationEntityStore::connect(&sqlite_test_url("schema-init"))
        .expect("sqlite application store connects");
    tokio_runtime().block_on(async {
        let initializer = SqlitePaymentSchemaInitializer;

        store
            .initialize_application_schema("payments", "transfers", &initializer)
            .await
            .expect("first schema initialization");
        store
            .initialize_application_schema("payments", "transfers", &initializer)
            .await
            .expect("second schema initialization");

        let mut transaction = store.begin().await.expect("begin read transaction");
        let row_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM payment_transfers")
            .fetch_one(transaction.sqlite())
            .await
            .expect("query application table")
            .get("count");
        transaction
            .rollback()
            .await
            .expect("rollback read transaction");

        assert_eq!(row_count, 0);
    });
}

#[test]
fn test_sqlite_application_schema_is_separate_from_datalens_metadata_schema() {
    let store = SqliteApplicationEntityStore::connect(&sqlite_test_url("schema-separation"))
        .expect("sqlite application store connects");
    tokio_runtime().block_on(async {
        store
            .initialize_application_schema("payments", "transfers", &SqlitePaymentSchemaInitializer)
            .await
            .expect("schema initialization");
        let mut transaction = store.begin().await.expect("begin read transaction");
        let tables = sqlite_table_names(transaction.sqlite()).await;
        transaction
            .rollback()
            .await
            .expect("rollback read transaction");

        assert!(tables.contains(&"payment_transfers".to_owned()));
        assert!(tables.contains(&"datalens_processor_checkpoints".to_owned()));
        assert!(!tables.contains(&"processor_checkpoints".to_owned()));
    });
}

#[tokio::test]
async fn test_postgres_application_entity_store_commits_rows_and_checkpoint_when_url_is_configured()
{
    let Some(url) = postgres_test_url() else {
        return;
    };
    let store = PostgresApplicationEntityStore::connect_async(&url)
        .await
        .expect("postgres store connects");
    let event_id = "ethereum:44:0:1";
    let cursor = CheckpointCursor::new("processor/payments/postgres/logs", "block:44");

    let mut transaction = store.begin().await.expect("begin transaction");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS payment_transfers (event_id TEXT PRIMARY KEY, amount BIGINT)",
    )
    .execute(transaction.postgres())
    .await
    .expect("create application table");
    sqlx::query("DELETE FROM payment_transfers WHERE event_id = $1")
        .bind(event_id)
        .execute(transaction.postgres())
        .await
        .expect("clear test row");
    sqlx::query("INSERT INTO payment_transfers (event_id, amount) VALUES ($1, $2)")
        .bind(event_id)
        .bind(11_i64)
        .execute(transaction.postgres())
        .await
        .expect("insert application row");
    transaction
        .put_checkpoint(&cursor)
        .await
        .expect("put checkpoint");
    transaction.commit().await.expect("commit transaction");

    let mut transaction = store.begin().await.expect("begin read transaction");
    let row_count: i64 =
        sqlx::query("SELECT COUNT(*) AS count FROM payment_transfers WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(transaction.postgres())
            .await
            .expect("query application table")
            .get("count");
    transaction
        .rollback()
        .await
        .expect("rollback read transaction");

    assert_eq!(row_count, 1);
    assert_eq!(
        store
            .checkpoint(cursor.key())
            .await
            .expect("read checkpoint")
            .as_ref()
            .map(CheckpointCursor::value),
        Some("block:44")
    );
}

#[test]
fn test_postgres_application_entity_store_debug_redacts_url_credentials_when_url_is_configured() {
    let Some(url) = postgres_test_url() else {
        return;
    };
    let store = PostgresApplicationEntityStore::connect(&url).expect("postgres store connects");
    let debug = format!("{store:?}");

    assert!(!debug.contains(&url));
    if url.contains('@') || url.contains("password=") {
        assert!(debug.contains("<redacted>"));
    }
}

fn postgres_test_url() -> Option<String> {
    match std::env::var("DATALENS_POSTGRES_TEST_URL") {
        Ok(url) => Some(url),
        Err(_) if std::env::var("DATALENS_REQUIRE_POSTGRES_TEST_URL").is_ok() => {
            panic!(
                "DATALENS_POSTGRES_TEST_URL must be set for PostgreSQL integration tests; \
                 start PostgreSQL with docker compose or export a test database URL"
            );
        }
        Err(_) => None,
    }
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("runtime")
}

fn sqlite_test_url(name: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "datalens-application-entity-store-{name}-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    format!("sqlite:{}", path.display())
}

struct SqlitePaymentSchemaInitializer;

impl ApplicationSchemaInitializer for SqlitePaymentSchemaInitializer {
    fn initialize_schema<'a>(
        &'a self,
        context: SchemaInitializationContext<'a>,
    ) -> ProcessorFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            assert_eq!(context.application(), "payments");
            assert_eq!(context.index(), "transfers");
            context
                .store()
                .execute_sql(
                    r#"
                    CREATE TABLE IF NOT EXISTS payment_transfers (
                        event_id TEXT PRIMARY KEY,
                        amount INTEGER NOT NULL
                    )
                    "#,
                )
                .await
        })
    }
}

async fn sqlite_table_names(connection: &mut sqlx::SqliteConnection) -> Vec<String> {
    sqlx::query(
        r#"
        SELECT name
        FROM sqlite_schema
        WHERE type = 'table'
        ORDER BY name
        "#,
    )
    .fetch_all(connection)
    .await
    .expect("list sqlite tables")
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect()
}
