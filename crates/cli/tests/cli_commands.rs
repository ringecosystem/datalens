use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use clap::Parser;
use serde_json::{Value, json};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_cli::*;
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, LogRecord,
    NetworkId, QueryRows,
};
use datalens_storage::{LocalStorage, StorageWriteRequest};

#[test]
fn test_bare_cli_requires_subcommand_instead_of_serve_default() {
    assert!(Cli::try_parse_from(["datalens"]).is_err());
}

#[test]
fn test_serve_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "serve", "--config", "custom.toml"]);

    match cli.command {
        Command::Serve(command) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected serve command, got {command:?}"),
    }
}

#[test]
fn test_doctor_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "doctor", "--config", "custom.toml"]);

    match cli.command {
        Command::Doctor(command) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected doctor command, got {command:?}"),
    }
}

#[test]
fn test_query_blocks_accepts_chain_and_range() {
    let cli = Cli::parse_from([
        "datalens",
        "query",
        "blocks",
        "--config",
        "custom.toml",
        "--chain",
        "ethereum",
        "--from-block",
        "10",
        "--to-block",
        "12",
    ]);

    match cli.command {
        Command::Query(QueryCommand {
            command: QuerySubcommand::Blocks(command),
            ..
        }) => {
            assert_eq!(command.config, "custom.toml");
            assert_eq!(command.chain, "ethereum");
            assert_eq!(command.from_block, 10);
            assert_eq!(command.to_block, 12);
        }
        command => panic!("expected query blocks command, got {command:?}"),
    }
}

#[test]
fn test_query_logs_accepts_address_and_topic_filters() {
    let cli = Cli::parse_from([
        "datalens",
        "query",
        "logs",
        "--config",
        "custom.toml",
        "--chain",
        "ethereum",
        "--from-block",
        "10",
        "--to-block",
        "12",
        "--address",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--topic",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]);

    match cli.command {
        Command::Query(QueryCommand {
            command: QuerySubcommand::Logs(command),
            ..
        }) => {
            assert_eq!(command.config, "custom.toml");
            assert_eq!(command.addresses.len(), 1);
            assert_eq!(command.topics.len(), 1);
        }
        command => panic!("expected query logs command, got {command:?}"),
    }
}

#[test]
fn test_inspect_manifest_accepts_config_path_after_subcommand() {
    let cli = Cli::parse_from(["datalens", "inspect", "manifest", "--config", "custom.toml"]);

    match cli.command {
        Command::Inspect(InspectCommand {
            command: InspectSubcommand::Manifest(command),
        }) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected inspect manifest command, got {command:?}"),
    }
}

#[test]
fn test_inspect_coverage_accepts_config_path_after_subcommand() {
    let cli = Cli::parse_from(["datalens", "inspect", "coverage", "--config", "custom.toml"]);

    match cli.command {
        Command::Inspect(InspectCommand {
            command: InspectSubcommand::Coverage(command),
        }) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected inspect coverage command, got {command:?}"),
    }
}

#[test]
fn test_inspect_manifest_reads_local_storage_manifest() {
    let root = temp_storage_root("inspect-manifest");
    let storage = LocalStorage::new(&root);
    write_block_coverage(&storage, 10, 10);
    let config = write_config("inspect-manifest", &root);

    let output = inspect_summary(InspectCommand {
        command: InspectSubcommand::Manifest(ConfigCommand { config }),
    })
    .expect("inspect manifest");

    assert_eq!(output["status"], "ok");
    assert_eq!(output["read_only"], true);
    assert_eq!(output["manifest"]["entry_count"], 1);
    let entry = &output["manifest"]["entries"][0];
    assert_eq!(entry["chain"]["key"], "evm/ethereum/1");
    assert_eq!(entry["dataset"]["key"], "evm.blocks");
    assert_eq!(entry["selector"]["fingerprint"], "all");
    assert_eq!(entry["range"]["kind"], "block");
    assert_eq!(entry["range"]["start"], 10);
    assert_eq!(entry["range"]["end"], 10);
    assert_eq!(entry["finality"], "safe");
    assert_eq!(entry["coverage_type"], "data_object");
    assert_eq!(entry["row_count"], 1);
    assert!(entry["object"]["key"].as_str().is_some());
    assert!(entry["object"]["size_bytes"].as_u64().expect("size") > 0);
    assert_eq!(entry["object"]["checksum_algorithm"], "sha256");
    assert!(
        entry["object"]["written_at_unix_seconds"]
            .as_u64()
            .is_some()
    );
}

#[test]
fn test_inspect_coverage_distinguishes_empty_coverage() {
    let root = temp_storage_root("inspect-empty-coverage");
    let storage = LocalStorage::new(&root);
    write_empty_log_coverage(&storage, 20, 21);
    let config = write_config("inspect-empty-coverage", &root);

    let output = inspect_summary(InspectCommand {
        command: InspectSubcommand::Coverage(ConfigCommand { config }),
    })
    .expect("inspect coverage");

    assert_eq!(output["status"], "ok");
    assert_eq!(output["coverage"]["entry_count"], 1);
    assert_eq!(output["coverage"]["data_object_count"], 0);
    assert_eq!(output["coverage"]["empty_coverage_count"], 1);
    let entry = &output["coverage"]["entries"][0];
    assert_eq!(entry["coverage_type"], "empty");
    assert_eq!(entry["dataset"]["key"], "evm.logs");
    assert_eq!(entry["finality"], "finalized");
    assert_eq!(entry["row_count"], 0);
    assert_eq!(entry["object"], Value::Null);
}

#[test]
fn test_inspect_manifest_is_stable_when_manifest_is_missing() {
    let root = temp_storage_root("inspect-missing-manifest");
    let config = write_config("inspect-missing-manifest", &root);

    let output = inspect_summary(InspectCommand {
        command: InspectSubcommand::Manifest(ConfigCommand { config }),
    })
    .expect("inspect missing manifest");

    assert_eq!(output["status"], "ok");
    assert_eq!(output["manifest"]["entry_count"], 0);
    assert_eq!(output["manifest"]["entries"], json!([]));
}

#[test]
fn test_redact_url_hides_credentials_path_and_query() {
    assert_eq!(
        redact_url("https://user:secret@example.invalid/path?token=secret"),
        "https://example.invalid/<redacted>"
    );
}

#[test]
fn test_validate_config_rejects_zero_lag_finality_override() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"
        root = ".datalens/storage"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [chains.private]
        kind = "evm"
        chain_id = 31337
        rpc_urls = ["http://example.invalid"]

        [chains.private.finality]
        mode = "lag"
        safe_lag_blocks = 0
        finalized_lag_blocks = 0

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2
        "#,
    )
    .expect("config parses");

    let error = validate_config(&config).expect_err("zero lag rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("lag"));
}

#[test]
fn test_validate_config_accepts_s3_compatible_storage_backend() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "s3"

        [storage.s3]
        bucket = "datalens"
        prefix = "dev"
        region = "auto"
        endpoint_url = "http://localhost:9000"
        force_path_style = true

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [chains.private]
        kind = "evm"
        chain_id = 31337
        rpc_urls = ["http://example.invalid"]

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("s3-compatible config is valid");
}

#[test]
fn test_validate_config_keeps_legacy_storage_root_compatible() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"
        root = ".datalens/storage"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [chains.private]
        kind = "evm"
        chain_id = 31337
        rpc_urls = ["http://example.invalid"]

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("legacy local root config remains valid");
    assert_eq!(
        config.storage.local.expect("legacy local config").root,
        ".datalens/storage"
    );
}

#[test]
fn test_doctor_chain_summary_rejects_unknown_auto_finality_without_profile() {
    let (url, _requests) =
        start_rpc_server(vec![unsupported_tag_response(), unsupported_tag_response()]);
    let chain = ChainConfig {
        kind: "evm".to_owned(),
        chain_id: 999999,
        rpc_urls: vec![url],
        finality: FinalityConfig::Auto,
        datasets: datalens_api::config::DatasetsConfig {
            blocks: datalens_api::config::BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 10,
            },
            logs: datalens_api::config::LogsDatasetConfig {
                enabled: true,
                max_get_logs_range_blocks: 10,
                max_addresses_per_query: 2,
            },
        },
    };

    let error = doctor_chain_summary("unknown", &chain).expect_err("doctor finality error");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("durable cache writes"));
}

fn unsupported_tag_response() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": -32602,
            "message": "unsupported block tag"
        }
    })
}

fn start_rpc_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);
    let responses = Arc::new(Mutex::new(responses));

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("test server connection");
            let mut buffer = [0; 8192];
            let bytes = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..bytes]);
            let body = request.split("\r\n\r\n").nth(1).expect("request body");
            let request_json: Value = serde_json::from_str(body).expect("request JSON");
            request_log.lock().expect("request log").push(request_json);
            let response = responses.lock().expect("responses").remove(0);
            let response = response.to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                response.len(),
                response
            )
            .expect("write response");
        }
    });

    (format!("http://{address}"), requests)
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-cli-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn write_config(name: &str, storage_root: &std::path::Path) -> String {
    let config_path = storage_root.with_file_name(format!("{name}.toml"));
    std::fs::write(
        &config_path,
        format!(
            r#"
            [server]
            bind = "127.0.0.1:8080"

            [storage]
            backend = "local"
            root = "{}"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [chains.ethereum]
            kind = "evm"
            chain_id = 1
            rpc_urls = ["http://example.invalid"]

            [chains.ethereum.datasets.blocks]
            enabled = true
            max_batch_blocks = 10

            [chains.ethereum.datasets.logs]
            enabled = true
            max_get_logs_range_blocks = 10
            max_addresses_per_query = 2
            "#,
            storage_root.display()
        ),
    )
    .expect("write config");
    config_path.to_string_lossy().into_owned()
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

fn write_block_coverage(storage: &LocalStorage, start: u64, end: u64) {
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number: start,
            hash: format!("0xblock{start}"),
            parent_hash: "0xparent".to_owned(),
            timestamp: 1,
        }]),
    )
    .expect("block rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(start, end).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write block coverage");
}

fn write_empty_log_coverage(storage: &LocalStorage, start: u64, end: u64) {
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(Vec::<LogRecord>::new()),
    )
    .expect("log rows");
    storage
        .write_rows(StorageWriteRequest {
            chain: &test_chain(),
            dataset_key: DatasetKey::evm_logs(),
            selector: &DatasetSelector::all(),
            range: LedgerRange::blocks(start, end).expect("valid range"),
            rows: &rows,
            finality_level: FinalityLevel::Finalized,
            record_empty_coverage: true,
        })
        .expect("write empty log coverage");
}
