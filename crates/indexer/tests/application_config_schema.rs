use datalens_indexer::{
    CheckpointPolicy, DatabaseDriver, DatabaseOutputConfig, DatalensIndexConfig,
    FinalityRequirement, IndexDataset, OutputConfig, ParquetOutputConfig, QueryServiceConfig,
    SourceConfig,
};

fn valid_config() -> &'static str {
    r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 1000

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 20009590
to_block = 20059589
addresses = ["0x0000000000000000000000000000000000000001"]
topics = ["0x0000000000000000000000000000000000000000000000000000000000000000"]

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#
}

fn parse_error(input: &str) -> String {
    DatalensIndexConfig::from_toml_str(input)
        .expect_err("config should fail")
        .to_string()
}

#[test]
fn test_parse_valid_toml_config_returns_typed_schema() {
    let config = DatalensIndexConfig::from_toml_str(valid_config()).expect("valid config");

    assert_eq!(config.client.endpoint, "http://127.0.0.1:3000");
    assert_eq!(config.client.application, "ormp");
    assert_eq!(config.client.token.env(), "PATH");
    assert_eq!(config.index.name, "ormp");
    assert_eq!(config.index.dataset, IndexDataset::EvmLogs);
    assert_eq!(config.index.finality, FinalityRequirement::Durable);
    assert_eq!(config.index.chunk_blocks, 1000);
    assert_eq!(config.sources.len(), 1);
    assert_eq!(
        config.sources[0],
        SourceConfig::Evm(datalens_indexer::EvmSourceConfig {
            chain: "ethereum".to_owned(),
            chain_id: 1,
            from_block: 20009590,
            to_block: Some(20059589),
            addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            topics: vec![
                "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned()
            ],
        })
    );
    assert_eq!(
        config.output,
        OutputConfig::Jsonl {
            path: ".data/indexes/ormp/events.jsonl".into()
        }
    );
    assert_eq!(config.query, QueryServiceConfig::default());
    assert_eq!(
        config.checkpoint,
        CheckpointPolicy::File {
            path: ".data/indexes/ormp/checkpoint.json".into()
        }
    );
}

#[test]
fn test_parse_valid_parquet_output_config() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        r#"[output]
kind = "parquet"

[output.parquet]
path = ".data/indexes/ormp/parquet"
max_rows_per_file = 5000
max_bytes_per_file = 134217728
partition_by = ["index", "chain_family", "chain_id", "dataset"]
compression = "zstd""#,
    );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid parquet config");

    assert_eq!(
        config.output,
        OutputConfig::Parquet {
            parquet: ParquetOutputConfig {
                path: ".data/indexes/ormp/parquet".into(),
                max_rows_per_file: Some(5000),
                max_bytes_per_file: Some(134217728),
                partition_by: vec![
                    "index".to_owned(),
                    "chain_family".to_owned(),
                    "chain_id".to_owned(),
                    "dataset".to_owned(),
                ],
                compression: Some("zstd".to_owned()),
            },
        }
    );
}

#[test]
fn test_parse_valid_toml_config_redacts_resolved_token_from_debug() {
    let config = DatalensIndexConfig::from_toml_str(valid_config()).expect("valid config");
    let token = std::env::var("PATH").expect("PATH should exist for tests");
    let debug = format!("{:?}", config.client.token);

    assert!(debug.contains("PATH"));
    assert!(!debug.contains(&token));
}

#[test]
fn test_parse_missing_fields_returns_field_oriented_error() {
    let error = parse_error(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
token_env = "PATH"
"#,
    );

    assert!(error.contains("index"), "{error}");
    assert!(error.contains("client.application"), "{error}");
}

#[test]
fn test_parse_invalid_source_range_returns_field_error() {
    let input = valid_config().replace("to_block = 20059589", "to_block = 20009589");
    let error = parse_error(&input);

    assert!(error.contains("sources[0].to_block"), "{error}");
    assert!(error.contains("from_block"), "{error}");
}

#[test]
fn test_parse_invalid_chunk_size_returns_field_error() {
    let input = valid_config().replace("chunk_blocks = 1000", "chunk_blocks = 0");
    let error = parse_error(&input);

    assert!(error.contains("index.chunk_blocks"), "{error}");
    assert!(error.contains("greater than 0"), "{error}");
}

#[test]
fn test_parse_unsupported_dataset_returns_field_error() {
    let input = valid_config().replace("dataset = \"evm.logs\"", "dataset = \"evm.blocks\"");
    let error = parse_error(&input);

    assert!(error.contains("index.dataset"), "{error}");
    assert!(error.contains("evm.logs"), "{error}");
}

#[test]
fn test_parse_unsupported_family_returns_field_error() {
    let input = valid_config().replace("family = \"evm\"", "family = \"tron\"");
    let error = parse_error(&input);

    assert!(error.contains("sources[0].family"), "{error}");
    assert!(error.contains("evm"), "{error}");
}

#[test]
fn test_parse_invalid_output_config_returns_field_error() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output.stdout]",
    );
    let error = parse_error(&input);

    assert!(error.contains("output"), "{error}");
    assert!(error.contains("jsonl"), "{error}");
}

#[test]
fn test_parse_rejects_query_service_with_jsonl_output() {
    let input = valid_config().replace("[checkpoint]", "[query]\nenabled = true\n\n[checkpoint]");
    let error = parse_error(&input);

    assert!(error.contains("query.enabled"), "{error}");
    assert!(error.contains("jsonl"), "{error}");
    assert!(
        error.contains("does not support query service mode"),
        "{error}"
    );
}

#[test]
fn test_parse_rejects_graphql_with_jsonl_output() {
    let input = valid_config().replace("[checkpoint]", "[query]\ngraphql = true\n\n[checkpoint]");
    let error = parse_error(&input);

    assert!(error.contains("query.graphql"), "{error}");
    assert!(error.contains("jsonl"), "{error}");
    assert!(error.contains("does not support GraphQL"), "{error}");
}

#[test]
fn test_parse_rejects_query_service_with_parquet_output() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"parquet\"\n\n[output.parquet]\npath = \".data/indexes/ormp/parquet\"",
        )
        .replace("[checkpoint]", "[query]\nenabled = true\ngraphql = true\n\n[checkpoint]");
    let error = parse_error(&input);

    assert!(error.contains("query.enabled"), "{error}");
    assert!(error.contains("query.graphql"), "{error}");
    assert!(error.contains("parquet"), "{error}");
    assert!(
        error.contains("does not support query service mode"),
        "{error}"
    );
    assert!(error.contains("does not support GraphQL"), "{error}");
}

#[test]
fn test_parse_valid_sqlite_database_output_allows_query_service() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
    )
    .replace("[checkpoint]", "[query]\nenabled = true\ngraphql = true\n\n[checkpoint]");

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid database config");

    assert_eq!(
        config.output,
        OutputConfig::Database {
            database: DatabaseOutputConfig {
                driver: DatabaseDriver::Sqlite,
                url: "sqlite:.data/indexes/ormp/index.db".to_owned(),
            },
        }
    );
    assert_eq!(
        config.query,
        QueryServiceConfig {
            enabled: true,
            graphql: true,
        }
    );
}

#[test]
fn test_parse_valid_postgres_database_output_allows_query_service() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"postgres\"\nurl = \"postgres://localhost/datalens\"",
    )
    .replace("[checkpoint]", "[query]\nenabled = true\ngraphql = true\n\n[checkpoint]");

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid database config");

    assert_eq!(
        config.output,
        OutputConfig::Database {
            database: DatabaseOutputConfig {
                driver: DatabaseDriver::Postgres,
                url: "postgres://localhost/datalens".to_owned(),
            },
        }
    );
    assert_eq!(
        config.query,
        QueryServiceConfig {
            enabled: true,
            graphql: true,
        }
    );
}

#[test]
fn test_parse_rejects_unsupported_database_driver() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"mysql\"\nurl = \"mysql://localhost/datalens\"",
    );
    let error = parse_error(&input);

    assert!(error.contains("output.database.driver"), "{error}");
    assert!(error.contains("sqlite"), "{error}");
    assert!(error.contains("postgres"), "{error}");
}

#[test]
fn test_parse_invalid_evm_address_returns_field_error() {
    let input = valid_config().replace(
        "0x0000000000000000000000000000000000000001",
        "0xnot-an-address",
    );
    let error = parse_error(&input);

    assert!(error.contains("sources[0].addresses[0]"), "{error}");
}
