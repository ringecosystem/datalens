use datalens_indexer::{
    CheckpointPolicy, DatabaseDriver, DatabaseOutputConfig, DatalensIndexConfig, DecodeConfig,
    DecodeEventConfig, DecodeEventInputConfig, FinalityRequirement, IndexDataset,
    MetricsServiceConfig, OutputConfig, ParquetOutputConfig, QueryProtocol, QueryServiceConfig,
    SourceConfig, WebhookHeaderConfig, WebhookOutboxConfig, WebhookOutputConfig,
    WebhookRetryConfig,
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
fn test_parse_valid_solana_and_tron_source_configs() {
    let input = valid_config()
        .replace(
            "dataset = \"evm.logs\"",
            "dataset = \"solana.transactions\"",
        )
        .replace(
            r#"[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 20009590
to_block = 20059589
addresses = ["0x0000000000000000000000000000000000000001"]
topics = ["0x0000000000000000000000000000000000000000000000000000000000000000"]"#,
            r#"[[sources]]
chain = "solana-mainnet"
family = "solana"
network_id = "mainnet-beta"
dataset = "solana.transactions"
from_slot = 20009590
to_slot = 20009599
selector = { kind = "program", value = "11111111111111111111111111111111" }

[[sources]]
chain = "tron-mainnet"
family = "tron"
chain_id = 728126428
dataset = "tron.events"
from_block = 60000000
to_block = 60000001
contracts = ["0x0000000000000000000000000000000000000001"]
events = ["Transfer"]"#,
        );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid config");

    assert_eq!(config.index.dataset, IndexDataset::SolanaTransactions);
    assert_eq!(
        config.sources[0],
        SourceConfig::Solana(datalens_indexer::SolanaSourceConfig {
            chain: "solana-mainnet".to_owned(),
            network_id: Some("mainnet-beta".to_owned()),
            dataset: IndexDataset::SolanaTransactions,
            from_slot: 20009590,
            to_slot: Some(20009599),
            selector: datalens_indexer::SolanaSelectorConfig::Program(
                "11111111111111111111111111111111".to_owned()
            ),
        })
    );
    assert_eq!(
        config.sources[1],
        SourceConfig::Tron(datalens_indexer::TronSourceConfig {
            chain: "tron-mainnet".to_owned(),
            chain_id: 728126428,
            dataset: IndexDataset::TronEvents,
            from_block: 60000000,
            to_block: Some(60000001),
            contracts: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            events: vec!["Transfer".to_owned()],
        })
    );
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
fn test_parse_expands_environment_placeholders_before_schema_validation() {
    let path = std::env::var("PATH").expect("PATH should exist for tests");

    let input = valid_config()
        .replace(
            "endpoint = \"http://127.0.0.1:3000\"",
            "endpoint = \"${PATH}\"",
        )
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"postgres\"\nurl = \"${PATH}\"",
        );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid database config");

    assert_eq!(config.client.endpoint, path);
    assert_eq!(
        config.output,
        OutputConfig::Database {
            database: DatabaseOutputConfig {
                driver: DatabaseDriver::Postgres,
                url: std::env::var("PATH").expect("PATH should exist for tests"),
            },
        }
    );
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
    let input = valid_config().replace("family = \"evm\"", "family = \"aptos\"");
    let error = parse_error(&input);

    assert!(error.contains("sources[0].family"), "{error}");
    assert!(error.contains("evm"), "{error}");
    assert!(error.contains("solana"), "{error}");
    assert!(error.contains("tron"), "{error}");
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
fn test_parse_allows_jsonl_daemon_without_query_service() {
    let config = DatalensIndexConfig::from_toml_str(valid_config()).expect("valid config");

    assert_eq!(
        datalens_indexer::validate_daemon_config(&config).expect("daemon config"),
        datalens_indexer::DaemonQueryMode::Disabled
    );
}

#[test]
fn test_parse_rejects_graphql_with_jsonl_output() {
    let input = valid_config().replace(
        "[checkpoint]",
        "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[checkpoint]",
    );
    let error = parse_error(&input);

    assert!(error.contains("query.protocol"), "{error}");
    assert!(error.contains("jsonl"), "{error}");
    assert!(
        error.contains("does not support graphql query service"),
        "{error}"
    );
}

#[test]
fn test_parse_rejects_query_service_with_parquet_output() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"parquet\"\n\n[output.parquet]\npath = \".data/indexes/ormp/parquet\"",
        )
        .replace(
            "[checkpoint]",
            "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[checkpoint]",
        );
    let error = parse_error(&input);

    assert!(error.contains("query.enabled"), "{error}");
    assert!(error.contains("query.protocol"), "{error}");
    assert!(error.contains("parquet"), "{error}");
    assert!(
        error.contains("does not support query service mode"),
        "{error}"
    );
    assert!(
        error.contains("does not support graphql query service"),
        "{error}"
    );
}

#[test]
fn test_parse_valid_sqlite_database_output_allows_query_service() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
    )
    .replace(
        "[checkpoint]",
        "[query]\nenabled = true\nprotocol = \"graphql\"\nbind = \"127.0.0.1:9100\"\npath = \"/query\"\nplayground = true\n\n[checkpoint]",
    );

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
            protocol: QueryProtocol::Graphql,
            bind: "127.0.0.1:9100".to_owned(),
            path: "/query".to_owned(),
            playground: true,
            metrics: MetricsServiceConfig::default(),
        }
    );
}

#[test]
fn test_parse_evm_decode_config_returns_minimal_event_abi_fragments() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
        )
        .replace(
            "[checkpoint]",
            r#"[decode]
enabled = true

[[decode.events]]
name = "MessageAccepted"
signature = "MessageAccepted(bytes32,uint256,address,address)"
topic0 = "0x9e6c1c44f7b2b36245897f9be35a5500f3a9e0d5b8f29f89dbf04b54053bb7d1"
contract = "ormp"

[[decode.events.inputs]]
name = "msgHash"
kind = "bytes32"
indexed = true

[[decode.events.inputs]]
name = "fromChainId"
kind = "uint256"
indexed = false

[checkpoint]"#,
        );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid decode config");

    assert_eq!(
        config.decode,
        DecodeConfig {
            enabled: true,
            events: vec![DecodeEventConfig {
                name: "MessageAccepted".to_owned(),
                signature: "MessageAccepted(bytes32,uint256,address,address)".to_owned(),
                topic0: "0x9e6c1c44f7b2b36245897f9be35a5500f3a9e0d5b8f29f89dbf04b54053bb7d1"
                    .to_owned(),
                contract: Some("ormp".to_owned()),
                inputs: vec![
                    DecodeEventInputConfig {
                        name: "msgHash".to_owned(),
                        kind: "bytes32".to_owned(),
                        indexed: true,
                    },
                    DecodeEventInputConfig {
                        name: "fromChainId".to_owned(),
                        kind: "uint256".to_owned(),
                        indexed: false,
                    },
                ],
            }],
        }
    );
}

#[test]
fn test_parse_sqlite_query_config_captures_bind_address() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
        )
        .replace(
            "[checkpoint]",
            "[query]\nenabled = true\nprotocol = \"graphql\"\nbind = \"127.0.0.1:0\"\n\n[checkpoint]",
        );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid database config");

    assert_eq!(config.query.bind, "127.0.0.1:0");
    assert_eq!(
        datalens_indexer::validate_daemon_config(&config).expect("daemon config"),
        datalens_indexer::DaemonQueryMode::Graphql
    );
}

#[test]
fn test_parse_query_metrics_config_supports_operator_token_env() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
        )
        .replace(
            "[checkpoint]",
            "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[query.metrics]\nenabled = true\npath = \"/metrics\"\ntoken_env = \"PATH\"\n\n[checkpoint]",
        );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid database config");

    assert_eq!(
        config.query.metrics,
        MetricsServiceConfig {
            enabled: true,
            path: "/metrics".to_owned(),
            bearer_token: Some(std::env::var("PATH").expect("PATH should exist for tests")),
        }
    );
}

#[test]
fn test_daemon_allows_postgres_query_service() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"postgres\"\nurl = \"postgres://localhost/datalens\"",
        )
        .replace("[checkpoint]", "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[checkpoint]");
    let config = DatalensIndexConfig::from_toml_str(&input).expect("database config parses");

    assert_eq!(
        datalens_indexer::validate_daemon_config(&config).expect("daemon config"),
        datalens_indexer::DaemonQueryMode::Graphql
    );
}

#[test]
fn test_parse_valid_postgres_database_output_allows_query_service() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"postgres\"\nurl = \"postgres://localhost/datalens\"",
    )
    .replace("[checkpoint]", "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[checkpoint]");

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
            protocol: QueryProtocol::Graphql,
            bind: "127.0.0.1:9090".to_owned(),
            path: "/graphql".to_owned(),
            playground: false,
            metrics: MetricsServiceConfig::default(),
        }
    );
}

#[test]
fn test_parse_valid_webhook_output_returns_typed_schema() {
    let input = valid_config().replace(
        "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
        r#"[output]
kind = "webhook"

[output.webhook]
url = "https://example.invalid/indexed-events"
timeout_ms = 2500
max_rows_per_request = 250
max_bytes_per_request = 65536
idempotency_key_header = "Idempotency-Key"

[output.webhook.retry]
max_attempts = 4
initial_backoff_ms = 25
max_backoff_ms = 250
retry_429 = true

[output.webhook.outbox]
enabled = true
path = ".data/indexes/ormp/webhook-outbox.sqlite"
max_attempts = 12

[[output.webhook.headers]]
name = "Authorization"
env = "PATH"

[[output.webhook.headers]]
name = "X-Datalens-Source"
value = "indexer"
"#,
    );

    let config = DatalensIndexConfig::from_toml_str(&input).expect("valid webhook config");

    assert_eq!(
        config.output,
        OutputConfig::Webhook {
            webhook: WebhookOutputConfig {
                url: "https://example.invalid/indexed-events".to_owned(),
                timeout_ms: 2500,
                max_rows_per_request: 250,
                max_bytes_per_request: 65536,
                retry: WebhookRetryConfig {
                    max_attempts: 4,
                    initial_backoff_ms: 25,
                    max_backoff_ms: 250,
                    retry_429: true,
                },
                idempotency_key_header: Some("Idempotency-Key".to_owned()),
                outbox: WebhookOutboxConfig {
                    enabled: true,
                    path: Some(".data/indexes/ormp/webhook-outbox.sqlite".into()),
                    max_attempts: 12,
                },
                headers: vec![
                    WebhookHeaderConfig {
                        name: "Authorization".to_owned(),
                        value: std::env::var("PATH").expect("PATH should exist for tests"),
                        secret: true,
                    },
                    WebhookHeaderConfig {
                        name: "X-Datalens-Source".to_owned(),
                        value: "indexer".to_owned(),
                        secret: false,
                    },
                ],
            },
        }
    );
}

#[test]
fn test_parse_rejects_query_service_with_webhook_output() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"webhook\"\n\n[output.webhook]\nurl = \"https://example.invalid/indexed-events\"",
        )
        .replace("[checkpoint]", "[query]\nenabled = true\n\n[checkpoint]");
    let error = parse_error(&input);

    assert!(error.contains("query.enabled"), "{error}");
    assert!(error.contains("webhook"), "{error}");
    assert!(
        error.contains("does not support query service mode"),
        "{error}"
    );
}

#[test]
fn test_parse_rejects_graphql_with_webhook_output() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"webhook\"\n\n[output.webhook]\nurl = \"https://example.invalid/indexed-events\"",
        )
        .replace(
            "[checkpoint]",
            "[query]\nenabled = true\nprotocol = \"graphql\"\n\n[checkpoint]",
        );
    let error = parse_error(&input);

    assert!(error.contains("query.protocol"), "{error}");
    assert!(error.contains("webhook"), "{error}");
    assert!(
        error.contains("does not support graphql query service"),
        "{error}"
    );
}

#[test]
fn test_parse_rejects_unsupported_query_protocol() {
    let input = valid_config().replace(
        "[checkpoint]",
        "[query]\nenabled = true\nprotocol = \"rest\"\n\n[checkpoint]",
    );
    let error = parse_error(&input);

    assert!(error.contains("query.protocol"), "{error}");
    assert!(error.contains("graphql"), "{error}");
}

#[test]
fn test_parse_rejects_query_path_without_leading_slash() {
    let input = valid_config()
        .replace(
            "[output.jsonl]\npath = \".data/indexes/ormp/events.jsonl\"",
            "[output]\nkind = \"database\"\n\n[output.database]\ndriver = \"sqlite\"\nurl = \"sqlite:.data/indexes/ormp/index.db\"",
        )
        .replace(
            "[checkpoint]",
            "[query]\nenabled = true\nprotocol = \"graphql\"\npath = \"graphql\"\n\n[checkpoint]",
        );
    let error = parse_error(&input);

    assert!(error.contains("query.path"), "{error}");
    assert!(error.contains("start with /"), "{error}");
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
