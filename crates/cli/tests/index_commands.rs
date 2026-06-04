use std::{
    fs,
    path::PathBuf,
    process::Command as ProcessCommand,
    sync::{Arc, Mutex},
};

use clap::Parser;

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityLevel, HeightRangeKind, ProviderDiagnostics,
    SelectorKind,
};
use datalens_cli::*;
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatasetKey, DatasetRows,
    LedgerRange, NetworkId, QueryRows,
};
use datalens_solana::SolanaAdapter;
use datalens_storage::{LocalStorage, StorageWriteRequest};
use datalens_tron::{TronAdapter, TronFixtureProviderRpc};

#[test]
fn test_index_plan_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "index", "plan", "--config", "app.index.toml"]);

    match cli.command {
        Command::Index(command) => match *command {
            IndexCommand {
                command: IndexSubcommand::Plan(command),
            } => {
                assert_eq!(command.config, "app.index.toml");
            }
            command => panic!("expected index plan command, got {command:?}"),
        },
        command => panic!("expected index plan command, got {command:?}"),
    }
}

#[test]
fn test_index_run_accepts_config_path_and_checkpoint_options() {
    let cli = Cli::parse_from([
        "datalens",
        "index",
        "run",
        "--config",
        "evm.toml",
        "--from-start",
        "--no-checkpoint",
        "--dry-run",
    ]);

    match cli.command {
        Command::Index(command) => match *command {
            IndexCommand {
                command: IndexSubcommand::Run(command),
            } => {
                assert_eq!(command.config, "evm.toml");
                assert!(command.from_start);
                assert!(command.no_checkpoint);
                assert!(command.dry_run);
            }
            command => panic!("expected index run command, got {command:?}"),
        },
        command => panic!("expected index run command, got {command:?}"),
    }
}

#[test]
fn test_index_daemon_is_not_primary_cli_entrypoint() {
    assert!(
        Cli::try_parse_from(["datalens", "index", "daemon", "--config", "app.index.toml"]).is_err()
    );
}

#[test]
fn test_index_plan_prints_json_from_declarative_config() {
    let root = temp_storage_root("index-plan-json");
    let config_path = root.join("app.index.toml");
    std::fs::write(
        &config_path,
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 2

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 12
addresses = []
topics = []

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#,
    )
    .expect("write index config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "plan", "--config"])
        .arg(&config_path)
        .output()
        .expect("run datalens index plan");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&output.stdout).expect("plan JSON");

    assert_eq!(summary["application"], "ormp");
    assert_eq!(summary["tasks"].as_array().expect("tasks").len(), 2);
    assert_eq!(summary["tasks"][0]["label"], "ormp.000.000000");
    assert_eq!(summary["tasks"][1]["range"]["start"], 12);
    assert_eq!(summary["tasks"][1]["range"]["end"], 12);
}

#[test]
fn test_cache_backfill_accepts_required_inputs_and_dry_run() {
    let cli = Cli::parse_from([
        "datalens",
        "cache",
        "backfill",
        "--config",
        "custom.toml",
        "--chain",
        "ethereum",
        "--dataset",
        "blocks",
        "--range-kind",
        "block",
        "--range-start",
        "10",
        "--range-end",
        "12",
        "--application",
        "Indexer_App",
        "--dry-run",
        "--json",
    ]);

    match cli.command {
        Command::Cache(command) => match *command {
            CacheCommand {
                command: CacheWorkflowCommand::Backfill(command),
            } => {
                assert_eq!(command.common.config, "custom.toml");
                assert_eq!(command.common.chain, "ethereum");
                assert_eq!(command.common.datasets, vec!["blocks"]);
                assert_eq!(command.common.range_kind, "block");
                assert_eq!(command.common.range_start, 10);
                assert_eq!(command.common.range_end, 12);
                assert_eq!(command.common.application, "Indexer_App");
                assert!(command.dry_run);
                assert!(command.common.json);
            }
            command => panic!("expected cache backfill command, got {command:?}"),
        },
        command => panic!("expected cache backfill command, got {command:?}"),
    }
}

#[test]
fn test_local_rustfs_solana_smoke_commands_use_slot_range_kind() {
    let runbook = include_str!("../../../docs/runbook/local-rustfs.md");

    for workflow in ["backfill", "verify"] {
        let snippet = format!(
            "cargo run -p datalens-cli -- cache {workflow} \\\n  --config config/datalens.compose.toml \\\n  --chain solana-mainnet-beta \\\n  --dataset transactions \\\n  --range-kind slot"
        );
        assert!(
            runbook.contains(&snippet),
            "missing Solana {workflow} slot smoke command"
        );

        let cli = Cli::parse_from([
            "datalens",
            "cache",
            workflow,
            "--config",
            "config/datalens.compose.toml",
            "--chain",
            "solana-mainnet-beta",
            "--dataset",
            "transactions",
            "--range-kind",
            "slot",
            "--range-start",
            "250000000",
            "--range-end",
            "250000001",
            "--application",
            "live-smoke",
            "--json",
        ]);

        match cli.command {
            Command::Cache(cache) => {
                let CacheCommand { command } = *cache;
                let common = cache_test_common(&command);
                assert_eq!(common.chain, "solana-mainnet-beta");
                assert_eq!(common.datasets, vec!["transactions"]);
                assert_eq!(common.range_kind, "slot");
            }
            command => panic!("expected cache {workflow} command, got {command:?}"),
        }
    }
}

#[test]
fn test_index_backfill_is_not_accepted() {
    assert!(Cli::try_parse_from(["datalens", "index", "backfill"]).is_err());
}

#[test]
fn test_index_doctor_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "index", "doctor", "--config", "app.index.toml"]);

    match cli.command {
        Command::Index(command) => match *command {
            IndexCommand {
                command: IndexSubcommand::Doctor(command),
            } => {
                assert_eq!(command.config, "app.index.toml");
            }
            command => panic!("expected index doctor command, got {command:?}"),
        },
        command => panic!("expected index doctor command, got {command:?}"),
    }
}

#[test]
fn test_index_doctor_prints_stable_json_for_valid_config() {
    let root = temp_storage_root("index-doctor-valid");
    let config = write_declarative_index_config("index-doctor-valid", &root);

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config", &config])
        .env("DATALENS_INDEX_TOKEN", "super-secret-token")
        .output()
        .expect("run index doctor");

    assert!(
        output.status.success(),
        "index doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("index doctor JSON");
    assert_eq!(summary["status"], "ok");
    assert_eq!(summary["index"], "ormp");
    assert_eq!(summary["client"]["endpoint"], "http://127.0.0.1:3000");
    assert_eq!(summary["client"]["application"], "ormp");
    assert_eq!(summary["dataset"], "evm.logs");
    assert_eq!(summary["finality"], "durable");
    assert_eq!(summary["chunk_blocks"], 1000);
    assert_eq!(summary["source_count"], 1);
    assert_eq!(summary["sources"][0]["chain"], "ethereum");
    assert_eq!(summary["sources"][0]["family"], "evm");
    assert_eq!(summary["sources"][0]["chain_id"], 1);
    assert_eq!(summary["sources"][0]["from_block"], 20009590);
    assert_eq!(summary["sources"][0]["to_block"], 20059589);
    assert_eq!(summary["sources"][0]["addresses"], 2);
    assert_eq!(summary["sources"][0]["topics"], 0);
    assert_eq!(summary["output"]["kind"], "jsonl");
    assert_eq!(summary["output"]["path"], ".data/indexes/ormp/events.jsonl");
    assert_eq!(summary["output"]["capability"]["write"], true);
    assert_eq!(summary["output"]["capability"]["query"], false);
    assert_eq!(summary["output"]["capability"]["graphql"], false);
    assert_eq!(summary["output"]["capability"]["write_mode"], "append_only");
    assert_eq!(
        summary["checkpoint"]["path"],
        ".data/indexes/ormp/checkpoint.json"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains("super-secret-token"));
    assert!(!stderr.contains("super-secret-token"));
}

#[test]
fn test_index_doctor_reports_query_auth_without_printing_token() {
    let root = temp_storage_root("index-doctor-query-auth");
    let config_path = root.join("app.index.toml");
    std::fs::create_dir_all(&root).expect("create config root");
    std::fs::write(
        &config_path,
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "DATALENS_INDEX_TOKEN"

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
addresses = []
topics = []

[output]
kind = "database"

[output.database]
driver = "sqlite"
url = "sqlite:.data/indexes/ormp/index.db"

[query]
enabled = true
protocol = "graphql"
bind = "127.0.0.1:9090"
path = "/graphql"
playground = true

[query.auth]
enabled = true

[[query.auth.applications]]
id = "Query_App"
token_env = "DATALENS_QUERY_TOKEN"
max_requests_per_minute = 60
max_concurrent_requests = 2

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#,
    )
    .expect("write index config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config"])
        .arg(&config_path)
        .env("DATALENS_INDEX_TOKEN", "super-secret-index-token")
        .env("DATALENS_QUERY_TOKEN", "super-secret-query-token")
        .output()
        .expect("run index doctor");

    assert!(
        output.status.success(),
        "index doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("index doctor JSON");
    assert_eq!(summary["query"]["enabled"], true);
    assert_eq!(summary["query"]["auth"]["enabled"], true);
    assert_eq!(summary["query"]["auth"]["applications"], 1);
    assert_eq!(summary["query"]["auth"]["max_requests_per_minute"], 60);
    assert_eq!(summary["query"]["auth"]["max_concurrent_requests"], 2);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains("super-secret-index-token"));
    assert!(!stdout.contains("super-secret-query-token"));
    assert!(!stderr.contains("super-secret-index-token"));
    assert!(!stderr.contains("super-secret-query-token"));
}

#[test]
fn test_index_doctor_redacts_database_url_credentials() {
    let root = temp_storage_root("index-doctor-database-redaction");
    let config = write_declarative_index_config("index-doctor-database-redaction", &root);
    let input = fs::read_to_string(&config)
        .expect("read config")
        .replace(
            r#"[output.jsonl]
path = ".data/indexes/ormp/events.jsonl""#,
            r#"[output]
kind = "database"

[output.database]
driver = "postgres"
url = "postgres://indexer:database-password@db.example.invalid:5432/datalens?sslmode=require&password=query-password""#,
        );
    fs::write(&config, input).expect("write database config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config", &config])
        .env("DATALENS_INDEX_TOKEN", "super-secret-token")
        .output()
        .expect("run index doctor");

    assert!(
        output.status.success(),
        "index doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("index doctor JSON");
    assert_eq!(
        summary["output"]["database"]["url"],
        "postgres://<redacted>@db.example.invalid:5432/datalens?sslmode=require&password=<redacted>"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains("database-password"));
    assert!(!stdout.contains("query-password"));
    assert!(!stderr.contains("database-password"));
    assert!(!stderr.contains("query-password"));
}

#[test]
fn test_index_doctor_redacts_webhook_url_tokens_and_header_values() {
    let root = temp_storage_root("index-doctor-webhook-redaction");
    let config = write_declarative_index_config("index-doctor-webhook-redaction", &root);
    let input = fs::read_to_string(&config)
        .expect("read config")
        .replace(
            r#"[output.jsonl]
path = ".data/indexes/ormp/events.jsonl""#,
            r#"[output]
kind = "webhook"

[output.webhook]
url = "https://hooks.example.invalid/indexed-events?token=webhook-token&batch=evm&signature=webhook-signature"

[output.webhook.outbox]
enabled = true
path = ".data/indexes/ormp/webhook-outbox.sqlite"
max_attempts = 7

[[output.webhook.headers]]
name = "Authorization"
value = "Bearer header-token"

[[output.webhook.headers]]
name = "X-Datalens-Source"
value = "indexer""#,
        );
    fs::write(&config, input).expect("write webhook config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config", &config])
        .env("DATALENS_INDEX_TOKEN", "super-secret-token")
        .output()
        .expect("run index doctor");

    assert!(
        output.status.success(),
        "index doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("index doctor JSON");
    assert_eq!(
        summary["output"]["webhook"]["url"],
        "https://hooks.example.invalid/indexed-events?token=<redacted>&batch=evm&signature=<redacted>"
    );
    assert_eq!(
        summary["output"]["webhook"]["headers"][0]["name"],
        "Authorization"
    );
    assert_eq!(summary["output"]["webhook"]["headers"][0]["secret"], true);
    assert_eq!(
        summary["output"]["webhook"]["headers"][1]["name"],
        "X-Datalens-Source"
    );
    assert_eq!(summary["output"]["webhook"]["headers"][1]["secret"], false);
    assert_eq!(summary["output"]["webhook"]["outbox"]["enabled"], true);
    assert_eq!(
        summary["output"]["webhook"]["outbox"]["path"],
        ".data/indexes/ormp/webhook-outbox.sqlite"
    );
    assert_eq!(summary["output"]["webhook"]["outbox"]["max_attempts"], 7);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains("webhook-token"));
    assert!(!stdout.contains("webhook-signature"));
    assert!(!stdout.contains("header-token"));
    assert!(!stderr.contains("webhook-token"));
    assert!(!stderr.contains("webhook-signature"));
    assert!(!stderr.contains("header-token"));
}

#[test]
fn test_ormp_client_example_is_sdk_client_only() {
    let example_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join("ormp-client")
        .canonicalize()
        .expect("example dir");
    let manifest_path = example_dir.join("Cargo.toml");

    assert!(example_dir.join("README.md").is_file());
    assert!(manifest_path.is_file());
    assert!(!example_dir.join("ormp.index.toml").exists());

    let manifest: toml::Value =
        toml::from_str(&fs::read_to_string(&manifest_path).expect("read ORMP client manifest"))
            .expect("parse ORMP client manifest");
    assert_eq!(
        manifest["package"]["name"].as_str(),
        Some("datalens-example-ormp-client")
    );

    let dependencies = manifest["dependencies"]
        .as_table()
        .expect("dependencies table");
    assert!(dependencies.contains_key("datalens-sdk"));
    assert!(!dependencies.contains_key("datalens-indexer"));
    assert!(!dependencies.contains_key("datalens-runtime-indexer"));
    assert!(!dependencies.contains_key("datalens-processor-sdk"));

    let mut sources = vec![example_dir.join("src"), example_dir.join("tests")];
    while let Some(path) = sources.pop() {
        if path.is_dir() {
            for entry in fs::read_dir(&path).expect("read ORMP client source directory") {
                sources.push(entry.expect("read ORMP client source entry").path());
            }
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }

        let source = fs::read_to_string(&path).expect("read ORMP client source");
        let path = relative_path(&example_dir, path);
        assert!(!source.contains("datalens_indexer"), "{path}");
        assert!(!source.contains("datalens_runtime"), "{path}");
        assert!(!source.contains("datalens_processor"), "{path}");
    }
}

#[test]
fn test_application_client_readmes_document_native_datalens_runtime() {
    for example in ["ormp-client", "degov-client"] {
        let readme_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples")
            .join(example)
            .join("README.md");
        let readme = fs::read_to_string(&readme_path)
            .unwrap_or_else(|error| panic!("read {example} README: {error}"));

        assert!(
            readme.contains("query `datalens serve` through REST `/v1/query`"),
            "{example} README should describe the native datalens serve runtime"
        );
        assert!(
            readme.contains("docs/runbook/local-rustfs.md"),
            "{example} README should link the local runtime runbook"
        );
        assert!(
            readme.contains("docs/spec/production-runtime.md"),
            "{example} README should link the production runtime boundary"
        );
        assert!(
            readme.contains("DATALENS_ENDPOINT"),
            "{example} README should document the datalens endpoint"
        );
        assert!(
            !readme.contains("DATALENS_INDEX_GRAPHQL_URL"),
            "{example} README should not depend on the removed upstream index GraphQL URL"
        );
        assert!(
            !readme.contains("application-owned index GraphQL endpoint"),
            "{example} README should not describe the old application index GraphQL model"
        );
    }
}

fn relative_path(root: &std::path::Path, path: PathBuf) -> String {
    path.strip_prefix(root)
        .unwrap_or(&path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn test_compose_service_config_is_server_only() {
    let config_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/datalens.compose.toml")
        .canonicalize()
        .expect("compose service config");

    unsafe {
        std::env::set_var("DATALENS_S3_BUCKET", "datalens");
        std::env::set_var("DATALENS_S3_PREFIX", "local-compose");
        std::env::set_var("DATALENS_S3_REGION", "auto");
        std::env::set_var("DATALENS_S3_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("DATALENS_ETHEREUM_RPC_URL", "http://example.invalid");
        std::env::set_var(
            "DATALENS_ARBITRUM_RPC_URL",
            "http://example.invalid/arbitrum",
        );
        std::env::set_var("DATALENS_BASE_RPC_URL", "http://example.invalid/base");
        std::env::set_var(
            "DATALENS_DARWINIA_RPC_URL",
            "http://example.invalid/darwinia",
        );
        std::env::set_var("DATALENS_SOLANA_RPC_URL", "http://example.invalid/solana");
        std::env::set_var("DATALENS_TRON_RPC_URL", "http://example.invalid/tron");
        std::env::set_var("DATALENS_TRONGRID_API_KEY", "replace-with-trongrid-token");
        std::env::set_var("DATALENS_PUBLIC_APP_TOKEN", "replace-with-public-token");
        std::env::set_var("DATALENS_ORMP_TOKEN", "replace-with-ormp-token");
        std::env::set_var("DATALENS_DEGOV_TOKEN", "replace-with-degov-token");
        std::env::set_var("DATALENS_LIVE_SMOKE_TOKEN", "replace-with-live-smoke-token");
        std::env::set_var("DATALENS_METRICS_TOKEN", "replace-with-metrics-token");
    }

    let plan_output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["plan", "--config"])
        .arg(&config_path)
        .env("DATALENS_S3_BUCKET", "datalens")
        .env("DATALENS_S3_PREFIX", "local-compose")
        .env("DATALENS_S3_REGION", "auto")
        .env("DATALENS_S3_ENDPOINT_URL", "http://127.0.0.1:9000")
        .env("DATALENS_ETHEREUM_RPC_URL", "http://example.invalid")
        .env(
            "DATALENS_ARBITRUM_RPC_URL",
            "http://example.invalid/arbitrum",
        )
        .env("DATALENS_BASE_RPC_URL", "http://example.invalid/base")
        .env(
            "DATALENS_DARWINIA_RPC_URL",
            "http://example.invalid/darwinia",
        )
        .env("DATALENS_SOLANA_RPC_URL", "http://example.invalid/solana")
        .env("DATALENS_TRON_RPC_URL", "http://example.invalid/tron")
        .env("DATALENS_TRONGRID_API_KEY", "replace-with-trongrid-token")
        .env("DATALENS_PUBLIC_APP_TOKEN", "replace-with-public-token")
        .env("DATALENS_ORMP_TOKEN", "replace-with-ormp-token")
        .env("DATALENS_DEGOV_TOKEN", "replace-with-degov-token")
        .env("DATALENS_LIVE_SMOKE_TOKEN", "replace-with-live-smoke-token")
        .env("DATALENS_METRICS_TOKEN", "replace-with-metrics-token")
        .output()
        .expect("run compose service plan");

    assert!(
        plan_output.status.success(),
        "compose service plan failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&plan_output.stdout),
        String::from_utf8_lossy(&plan_output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&plan_output.stdout).expect("plan JSON");
    assert_eq!(plan["status"], "planned");
    assert_eq!(plan["service"]["query"]["index"]["graphql_enabled"], false);
    assert_eq!(plan["service"]["query"]["index"]["path"], "/index/graphql");
}

#[test]
fn test_live_smoke_index_configs_cover_solana_and_tron() {
    let config_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .canonicalize()
        .expect("config dir");
    let cases = [
        (
            "datalens.solana-live-smoke.index.toml",
            "solana-live-smoke",
            "solana.transactions",
            "solana",
        ),
        (
            "datalens.tron-live-smoke.index.toml",
            "tron-live-smoke",
            "tron.events",
            "tron",
        ),
    ];

    for (file, index_name, dataset, family) in cases {
        let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
            .args(["index", "doctor", "--config"])
            .arg(config_dir.join(file))
            .env("DATALENS_LIVE_SMOKE_TOKEN", "replace-with-live-smoke-token")
            .env(
                "DATALENS_INDEX_DATABASE_URL",
                "postgres://datalens:replace-with-password@postgres.example.invalid:5432/datalens",
            )
            .output()
            .expect("run live smoke index doctor");

        assert!(
            output.status.success(),
            "{file} index doctor failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let summary: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("index doctor JSON");
        assert_eq!(summary["index"], index_name);
        assert_eq!(summary["dataset"], dataset);
        assert_eq!(summary["source_count"], 1);
        assert_eq!(summary["sources"][0]["family"], family);
        assert_eq!(summary["chunk_blocks"], 2);
    }
}

#[test]
fn test_index_doctor_rejects_invalid_config_without_leaking_token() {
    let root = temp_storage_root("index-doctor-invalid");
    let config = write_declarative_index_config("index-doctor-invalid", &root);
    let invalid = fs::read_to_string(&config)
        .expect("read config")
        .replace("chunk_blocks = 1000", "chunk_blocks = 0");
    fs::write(&config, invalid).expect("write invalid config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config", &config])
        .env("DATALENS_INDEX_TOKEN", "super-secret-token")
        .output()
        .expect("run index doctor");

    assert!(
        !output.status.success(),
        "index doctor unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("index.chunk_blocks"), "{stderr}");
    assert!(!stdout.contains("super-secret-token"));
    assert!(!stderr.contains("super-secret-token"));
}

#[test]
fn test_cache_backfill_accepts_tron_event_names() {
    let cli = Cli::parse_from([
        "datalens",
        "cache",
        "backfill",
        "--config",
        "custom.toml",
        "--chain",
        "tron-mainnet",
        "--dataset",
        "events",
        "--range-kind",
        "block",
        "--range-start",
        "62251337",
        "--range-end",
        "82371640",
        "--application",
        "ormpindexer",
        "--address",
        "0x3Bc5362EC3a3DBc07292aEd4ef18Be18De02DA3a",
        "--address",
        "0x5C5c383FEbE62F377F8c0eA1de97F2a2Ba102e98",
        "--event-name",
        "MessageAccepted",
        "--event-name",
        "MessageDispatched",
        "--finality",
        "finalized",
        "--json",
    ]);

    match cli.command {
        Command::Cache(command) => match *command {
            CacheCommand {
                command: CacheWorkflowCommand::Backfill(command),
            } => {
                assert_eq!(command.common.chain, "tron-mainnet");
                assert_eq!(command.common.datasets, vec!["events"]);
                assert_eq!(command.common.addresses.len(), 2);
                assert_eq!(
                    command.common.event_names,
                    vec!["MessageAccepted", "MessageDispatched"]
                );
            }
            command => panic!("expected cache backfill command, got {command:?}"),
        },
        command => panic!("expected cache backfill command, got {command:?}"),
    }
}

#[test]
fn test_cache_backfill_tron_events_builds_tron_event_selector() {
    let root = temp_storage_root("index-tron-events-selector");
    let config = write_tron_config("index-tron-events-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "tron-mainnet".to_owned();
    common.datasets = vec!["events".to_owned()];
    common.addresses = vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()];
    common.event_names = vec!["Transfer".to_owned()];

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        IndexFixtureAdapter::default().with_chain(tron_chain()),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["selector_kind"], "tron_events");
    assert_eq!(
        output["datasets"][0]["selector_fingerprint"]
            .as_str()
            .expect("fingerprint")
            .split('/')
            .next(),
        Some("tron-events")
    );
}

#[test]
fn test_cache_backfill_tron_blocks_builds_tron_all_selector() {
    let root = temp_storage_root("index-tron-blocks-selector");
    let config = write_tron_config("index-tron-blocks-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "tron-mainnet".to_owned();
    common.datasets = vec!["blocks".to_owned()];

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        TronAdapter::with_provider(tron_chain(), TronFixtureProviderRpc),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["key"], "tron.blocks");
    assert_eq!(output["datasets"][0]["selector_kind"], "tron_all");
    assert_eq!(
        output["datasets"][0]["selector_fingerprint"],
        "tron-all/all"
    );
    assert_eq!(output["datasets"][0]["selector_canonical_key"], "all");
}

#[test]
fn test_cache_backfill_tron_blocks_runs_fixture_runtime() {
    let root = temp_storage_root("index-tron-blocks-runtime");
    let config = write_tron_config("index-tron-blocks-runtime", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "tron-mainnet".to_owned();
    common.datasets = vec!["blocks".to_owned()];

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: false,
        }),
        TronAdapter::with_provider(tron_chain(), TronFixtureProviderRpc)
            .with_max_block_range_len(2),
    )
    .expect("backfill");

    assert_eq!(output["status"], "completed");
    assert_eq!(output["accounting"]["chunks_written"], 2);
    assert_eq!(output["accounting"]["rows_written"], 3);
    assert_eq!(output["accounting"]["provider_calls"], 3);
}

#[test]
fn test_cache_backfill_solana_instructions_builds_program_selector() {
    let root = temp_storage_root("index-solana-program-selector");
    let config = write_solana_config("index-solana-program-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "solana-mainnet-beta".to_owned();
    common.datasets = vec!["instructions".to_owned()];
    common.range_kind = "slot".to_owned();
    common.program_id = Some("program1111111111111111111111111111111111".to_owned());

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        SolanaAdapter::with_fixture_defaults(),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["selector_kind"], "solana_program");
    assert_eq!(
        output["datasets"][0]["selector_canonical_key"],
        "program/program1111111111111111111111111111111111"
    );
}

#[test]
fn test_cache_backfill_solana_transactions_builds_address_selector() {
    let root = temp_storage_root("index-solana-address-selector");
    let config = write_solana_config("index-solana-address-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "solana-mainnet-beta".to_owned();
    common.datasets = vec!["transactions".to_owned()];
    common.range_kind = "slot".to_owned();
    common.addresses = vec!["Account111111111111111111111111111111111".to_owned()];

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        SolanaAdapter::with_fixture_defaults(),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["selector_kind"], "solana_address");
    assert_eq!(
        output["datasets"][0]["selector_canonical_key"],
        "address/Account111111111111111111111111111111111"
    );
}

#[test]
fn test_cache_backfill_solana_transactions_builds_signature_selector() {
    let root = temp_storage_root("index-solana-signature-selector");
    let config = write_solana_config("index-solana-signature-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "solana-mainnet-beta".to_owned();
    common.datasets = vec!["transactions".to_owned()];
    common.range_kind = "slot".to_owned();
    common.signature = Some("sigslot10".to_owned());

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        SolanaAdapter::with_fixture_defaults(),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["selector_kind"], "solana_signature");
    assert_eq!(
        output["datasets"][0]["selector_canonical_key"],
        "signature/sigslot10"
    );
}

#[test]
fn test_cache_backfill_solana_account_updates_builds_account_selector() {
    let root = temp_storage_root("index-solana-account-selector");
    let config = write_solana_config("index-solana-account-selector", &root);
    let mut common = cache_common(config, 10, 12);
    common.chain = "solana-mainnet-beta".to_owned();
    common.datasets = vec!["account_updates".to_owned()];
    common.range_kind = "slot".to_owned();
    common.account = Some("Account111111111111111111111111111111111".to_owned());

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        SolanaAdapter::with_fixture_defaults(),
    )
    .expect("dry-run");

    assert_eq!(output["datasets"][0]["selector_kind"], "solana_address");
    assert_eq!(
        output["datasets"][0]["selector_canonical_key"],
        "address/Account111111111111111111111111111111111"
    );
}

#[test]
fn test_cache_backfill_rejects_solana_selectors_for_blocks_and_slots() {
    for dataset in ["blocks", "slots"] {
        let root = temp_storage_root(&format!("index-solana-reject-{dataset}"));
        let config = write_solana_config(&format!("index-solana-reject-{dataset}"), &root);
        let mut common = cache_common(config, 10, 12);
        common.chain = "solana-mainnet-beta".to_owned();
        common.datasets = vec![dataset.to_owned()];
        common.range_kind = "slot".to_owned();
        common.program_id = Some("program1111111111111111111111111111111111".to_owned());

        let error = cache_summary_with_adapter(
            CacheWorkflowCommand::Backfill(CacheBackfillCommand {
                common,
                dry_run: true,
            }),
            SolanaAdapter::with_fixture_defaults(),
        )
        .expect_err("selector rejected");

        assert_eq!(error.kind, datalens_core::DatalensErrorKind::InvalidInput);
        assert!(
            error
                .message
                .contains("Solana blocks and slots only support all selectors")
        );
    }
}

#[test]
fn test_cache_resume_repair_and_verify_parse_required_inputs() {
    for workflow in ["resume", "repair", "verify"] {
        let cli = Cli::parse_from([
            "datalens",
            "cache",
            workflow,
            "--config",
            "custom.toml",
            "--chain",
            "ethereum",
            "--dataset",
            "blocks",
            "--range-kind",
            "block",
            "--range-start",
            "10",
            "--range-end",
            "12",
            "--application",
            "indexer",
        ]);

        match cli.command {
            Command::Cache(cache) => {
                let CacheCommand { command } = *cache;
                assert_eq!(cache_test_common(&command).config, "custom.toml");
                assert_eq!(cache_test_common(&command).chain, "ethereum");
                assert_eq!(cache_test_common(&command).datasets, vec!["blocks"]);
            }
            command => panic!("expected cache {workflow} command, got {command:?}"),
        }
    }
}

#[test]
fn test_index_config_loads_defaults_and_limits() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = "/tmp/datalens"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [index]
        default_chunk_range = 2
        max_concurrency = 1
        default_finality = "finalized"
        cursor_path = "/tmp/datalens-cursors"

        [index.retry]
        max_attempts = 4
        initial_backoff_ms = 5
        max_backoff_ms = 50

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
    )
    .expect("config parses");

    validate_config(&config).expect("index config is valid");
    assert_eq!(config.index.default_chunk_range, 2);
    assert_eq!(config.index.max_concurrency, 1);
    assert_eq!(config.index.default_finality, "finalized");
    assert_eq!(config.index.cursor_path, "/tmp/datalens-cursors");
    assert_eq!(config.index.retry.max_attempts, 4);
}

#[test]
fn test_index_config_accepts_trongrid_without_api_key() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = "/tmp/datalens"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [index]
        default_chunk_range = 2
        max_concurrency = 1
        default_finality = "finalized"
        cursor_path = "/tmp/datalens-cursors"

        [chains.tron-mainnet]
        kind = "tron"
        chain_id = 728126428
        rpc_urls = ["http://example.invalid/tron"]

        [chains.tron-mainnet.trongrid]
        enabled = true
        base_url = "https://api.trongrid.io"

        [chains.tron-mainnet.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.tron-mainnet.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("TronGrid config without key is valid");
    let chain = config.chains.get("tron-mainnet").expect("chain");
    assert!(chain.trongrid.enabled);
    assert!(chain.trongrid.api_key.is_none());
}

#[test]
fn test_cache_dry_run_plans_chunks_without_writing() {
    let root = temp_storage_root("index-dry-run");
    let config = write_config("index-dry-run", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common: cache_common(config, 1, 3),
            dry_run: true,
        }),
        adapter.clone(),
    )
    .expect("dry-run");

    assert_eq!(output["status"], "planned");
    assert_eq!(output["mode"], "backfill");
    assert_eq!(output["dry_run"], true);
    assert_eq!(output["plan"]["chunk_count"], 2);
    assert_eq!(adapter.calls(), Vec::<IndexSourceCall>::new());
    assert_eq!(
        LocalStorage::new(&root)
            .manifest()
            .expect("manifest")
            .entries
            .len(),
        0
    );
}

#[test]
fn test_cache_backfill_runs_fixture_runtime() {
    let root = temp_storage_root("index-backfill");
    let config = write_config("index-backfill", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common: cache_common(config, 1, 3),
            dry_run: false,
        }),
        adapter.clone(),
    )
    .expect("backfill");

    assert_eq!(output["status"], "completed");
    assert_eq!(output["accounting"]["chunks_written"], 2);
    assert_eq!(output["accounting"]["rows_written"], 3);
    assert_eq!(
        adapter.calls(),
        vec![
            IndexSourceCall::Blocks(BlockRange::expect_new(1, 2)),
            IndexSourceCall::Blocks(BlockRange::expect_new(3, 3)),
        ]
    );
    assert_eq!(
        LocalStorage::new(&root)
            .manifest()
            .expect("manifest")
            .entries
            .len(),
        2
    );
}

#[test]
fn test_cache_backfill_persists_cursor_under_configured_cursor_path() {
    let root = temp_storage_root("index-backfill-cursor");
    let config = write_config("index-backfill-cursor", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common: cache_common(config, 1, 3),
            dry_run: false,
        }),
        adapter,
    )
    .expect("backfill");

    let cursor_path = root.join("cursors");
    assert_eq!(
        output["cursor_path"],
        cursor_path.to_string_lossy().as_ref()
    );
    assert_eq!(
        cursor_path
            .read_dir()
            .expect("cursor dir")
            .map(|entry| entry.expect("cursor entry").path())
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
            .count(),
        1
    );
}

#[test]
fn test_cache_resume_uses_persisted_cursor_after_process_restart() {
    let root = temp_storage_root("index-resume-cursor");
    let config = write_config("index-resume-cursor", &root);
    let first_adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common: cache_common(config.clone(), 1, 2),
            dry_run: false,
        }),
        first_adapter,
    )
    .expect("backfill");

    let resumed_adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);
    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Resume(CacheResumeCommand {
            common: cache_common(config, 1, 2),
            dry_run: false,
        }),
        resumed_adapter.clone(),
    )
    .expect("resume");

    assert_eq!(output["accounting"]["chunks_planned"], 0);
    assert_eq!(
        resumed_adapter.calls(),
        Vec::<IndexSourceCall>::new(),
        "resume should skip manifest-covered chunks after a new CLI invocation"
    );
}

#[test]
fn test_cache_verify_does_not_persist_cursor() {
    let root = temp_storage_root("index-verify-cursor");
    let storage = LocalStorage::new(&root);
    write_block_coverage(&storage, 1, 2);
    let config = write_config("index-verify-cursor", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    cache_summary_with_adapter(
        CacheWorkflowCommand::Verify(CacheVerifyCommand {
            common: cache_common(config, 1, 2),
            verify_only: true,
        }),
        adapter,
    )
    .expect("verify");

    assert!(
        !root.join("cursors").exists(),
        "verify mode should not create cursor files"
    );
}

#[test]
fn test_cache_verify_does_not_write_data() {
    let root = temp_storage_root("index-verify");
    let storage = LocalStorage::new(&root);
    write_block_coverage(&storage, 1, 2);
    let config = write_config("index-verify", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    let output = cache_summary_with_adapter(
        CacheWorkflowCommand::Verify(CacheVerifyCommand {
            common: cache_common(config, 1, 2),
            verify_only: true,
        }),
        adapter.clone(),
    )
    .expect("verify");

    assert_eq!(output["status"], "completed");
    assert_eq!(output["mode"], "verify");
    assert_eq!(output["read_only"], true);
    assert_eq!(output["accounting"]["chunks_written"], 0);
    assert_eq!(adapter.calls(), Vec::<IndexSourceCall>::new());
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_validate_config_rejects_unsafe_index_finality() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = "/tmp/datalens"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [index]
        default_finality = "latest"

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
    )
    .expect("config parses");

    let error = validate_config(&config).expect_err("latest rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("safe or finalized"));
}

#[test]
fn test_cache_command_rejects_unsafe_finality_override() {
    let root = temp_storage_root("index-unsafe-finality");
    let config = write_config("index-unsafe-finality", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1)]);
    let mut common = cache_common(config, 1, 1);
    common.finality = Some("latest".to_owned());

    let error = cache_summary_with_adapter(
        CacheWorkflowCommand::Backfill(CacheBackfillCommand {
            common,
            dry_run: true,
        }),
        adapter,
    )
    .expect_err("latest rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("safe or finalized"));
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-cli-cache-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
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

            [storage.local]
            root = "{}"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [index]
            default_chunk_range = 2
            max_concurrency = 1
            default_finality = "finalized"
            cursor_path = "{}"

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
            storage_root.display(),
            storage_root.join("cursors").display()
        ),
    )
    .expect("write config");
    config_path.to_string_lossy().into_owned()
}

fn write_declarative_index_config(name: &str, storage_root: &std::path::Path) -> String {
    let config_path = storage_root.with_file_name(format!("{name}.index.toml"));
    fs::write(
        &config_path,
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "DATALENS_INDEX_TOKEN"

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
addresses = [
  "0x1111111111111111111111111111111111111111",
  "0x2222222222222222222222222222222222222222",
]
topics = []

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#,
    )
    .expect("write declarative index config");
    config_path.to_string_lossy().into_owned()
}

fn write_tron_config(name: &str, storage_root: &std::path::Path) -> String {
    let config_path = storage_root.with_file_name(format!("{name}.toml"));
    std::fs::write(
        &config_path,
        format!(
            r#"
            [server]
            bind = "127.0.0.1:8080"

            [storage]
            backend = "local"

            [storage.local]
            root = "{}"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [index]
            default_chunk_range = 2
            max_concurrency = 1
            default_finality = "finalized"
            cursor_path = "{}"

            [chains.tron-mainnet]
            kind = "tron"
            chain_id = 728126428
            rpc_urls = ["http://example.invalid"]

            [chains.tron-mainnet.datasets.blocks]
            enabled = true
            max_batch_blocks = 10

            [chains.tron-mainnet.datasets.logs]
            enabled = true
            max_get_logs_range_blocks = 10
            max_addresses_per_query = 2
            "#,
            storage_root.display(),
            storage_root.join("cursors").display()
        ),
    )
    .expect("write config");
    config_path.to_string_lossy().into_owned()
}

fn cache_common(config: String, start: u64, end: u64) -> CacheCommonCommand {
    CacheCommonCommand {
        config,
        chain: "ethereum".to_owned(),
        datasets: vec!["blocks".to_owned()],
        range_kind: "block".to_owned(),
        range_start: start,
        range_end: end,
        application: "indexer".to_owned(),
        finality: None,
        json: true,
        addresses: Vec::new(),
        account: None,
        program_id: None,
        signature: None,
        topics: Vec::new(),
        event_names: Vec::new(),
    }
}

fn write_solana_config(name: &str, storage_root: &std::path::Path) -> String {
    let config_path = storage_root.join(format!("{name}.toml"));
    std::fs::create_dir_all(storage_root).expect("create config root");
    std::fs::write(
        &config_path,
        format!(
            r#"
            [server]
            bind = "127.0.0.1:8080"

            [storage]
            backend = "local"

            [storage.local]
            root = "{}"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [index]
            default_chunk_range = 2
            max_concurrency = 1
            default_finality = "finalized"
            cursor_path = "{}"

            [chains.solana-mainnet-beta]
            kind = "solana"
            chain_id = 101
            rpc_urls = ["http://example.invalid"]

            [chains.solana-mainnet-beta.datasets.blocks]
            enabled = true
            max_batch_blocks = 10

            [chains.solana-mainnet-beta.datasets.logs]
            enabled = true
            max_get_logs_range_blocks = 10
            max_addresses_per_query = 2
            "#,
            storage_root.display(),
            storage_root.join("cursors").display()
        ),
    )
    .expect("write config");
    config_path.to_string_lossy().into_owned()
}

fn cache_test_common(command: &CacheWorkflowCommand) -> &CacheCommonCommand {
    match command {
        CacheWorkflowCommand::Backfill(command) => &command.common,
        CacheWorkflowCommand::Resume(command) => &command.common,
        CacheWorkflowCommand::Repair(command) => &command.common,
        CacheWorkflowCommand::Verify(command) => &command.common,
    }
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

fn tron_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(
        ChainFamily::Other("tron".to_owned()),
        "tron-mainnet",
        NetworkId::numeric(728126428),
    )
}

fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
        timestamp: number * 10,
    }
}

fn write_block_coverage(storage: &LocalStorage, start: u64, end: u64) {
    let rows = DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![block(start)]),
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum IndexSourceCall {
    Blocks(BlockRange),
}

#[derive(Clone)]
struct IndexFixtureAdapter {
    chain: ChainIdentity,
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<IndexSourceCall>>>,
}

impl Default for IndexFixtureAdapter {
    fn default() -> Self {
        Self {
            chain: test_chain(),
            blocks: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl IndexFixtureAdapter {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks") = blocks;
        self
    }

    fn with_chain(mut self, chain: ChainIdentity) -> Self {
        self.chain = chain;
        self
    }

    fn calls(&self) -> Vec<IndexSourceCall> {
        self.calls.lock().expect("calls").clone()
    }
}

impl ChainAdapter for IndexFixtureAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone()).with_dataset_capability(
            DatasetCapability::new(if self.chain.family() == ChainFamily::Evm {
                DatasetKey::evm_blocks()
            } else {
                DatasetKey::tron_events()
            })
            .with_selector(SelectorKind::All)
            .with_selector(SelectorKind::Other(
                datalens_chain::AdapterKey::try_new("tron_events").expect("selector"),
            ))
            .with_range(HeightRangeKind::Block)
            .with_max_range_len(2)
            .with_empty_coverage(true)
            .with_safe_height(true)
            .with_finalized_height(true)
            .with_range_split(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100).with_finality(FinalityLevel::Safe))
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100).with_finality(FinalityLevel::Finalized))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = request.range.block_range().expect("block range");
        self.calls
            .lock()
            .expect("calls")
            .push(IndexSourceCall::Blocks(range));
        let rows = self
            .blocks
            .lock()
            .expect("blocks")
            .iter()
            .filter(|block| request.range.contains(block.number))
            .cloned()
            .collect();
        ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            if self.chain.family() == ChainFamily::Evm {
                QueryRows::EvmBlocks(rows)
            } else {
                QueryRows::AdapterJson {
                    dataset_key: DatasetKey::tron_events(),
                    rows: Vec::new(),
                }
            },
        )
        .map(|response| {
            response.with_provider_diagnostics(ProviderDiagnostics {
                calls: 1,
                rows_scanned: 0,
                warnings: Vec::new(),
            })
        })
    }
}
