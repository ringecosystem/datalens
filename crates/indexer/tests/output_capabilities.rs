use datalens_indexer::{
    DatabaseDriver, DatabaseOutputConfig, OutputConfig, OutputKind, OutputWriteMode,
    ParquetOutputConfig,
};

#[test]
fn test_jsonl_output_capability_is_write_only_append_only() {
    let output = OutputConfig::Jsonl {
        path: ".data/indexes/ormp/events.jsonl".into(),
    };
    let capability = output.capability();

    assert_eq!(capability.kind, OutputKind::Jsonl);
    assert!(capability.supports_write);
    assert!(!capability.supports_query);
    assert!(!capability.supports_graphql);
    assert_eq!(capability.write_mode, OutputWriteMode::AppendOnly);
}

#[test]
fn test_parquet_output_capability_is_write_only_batched_file_output() {
    let output = OutputConfig::Parquet {
        parquet: ParquetOutputConfig {
            path: ".data/indexes/ormp/parquet".into(),
            max_rows_per_file: Some(1000),
            max_bytes_per_file: None,
            partition_by: vec!["index".to_owned(), "chain_family".to_owned()],
            compression: Some("zstd".to_owned()),
        },
    };
    let capability = output.capability();

    assert_eq!(capability.kind, OutputKind::Parquet);
    assert!(capability.supports_write);
    assert!(!capability.supports_query);
    assert!(!capability.supports_graphql);
    assert_eq!(capability.write_mode, OutputWriteMode::BatchedFiles);
}

#[test]
fn test_sqlite_database_output_capability_is_queryable_idempotent() {
    let output = OutputConfig::Database {
        database: DatabaseOutputConfig {
            driver: DatabaseDriver::Sqlite,
            url: "sqlite:.data/indexes/ormp/index.db".to_owned(),
        },
    };
    let capability = output.capability();

    assert_eq!(capability.kind, OutputKind::Database);
    assert!(capability.supports_write);
    assert!(capability.supports_query);
    assert!(capability.supports_graphql);
    assert_eq!(capability.write_mode, OutputWriteMode::IdempotentUpsert);
}

#[test]
fn test_postgres_database_output_capability_is_queryable_idempotent() {
    let output = OutputConfig::Database {
        database: DatabaseOutputConfig {
            driver: DatabaseDriver::Postgres,
            url: "postgres://localhost/datalens".to_owned(),
        },
    };
    let capability = output.capability();

    assert_eq!(capability.kind, OutputKind::Database);
    assert!(capability.supports_write);
    assert!(capability.supports_query);
    assert!(capability.supports_graphql);
    assert_eq!(capability.write_mode, OutputWriteMode::IdempotentUpsert);
}
