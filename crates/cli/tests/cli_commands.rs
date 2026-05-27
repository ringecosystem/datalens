use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use clap::Parser;
use serde_json::{Value, json};

use datalens_cli::*;

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
