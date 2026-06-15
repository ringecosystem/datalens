use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command as ProcessCommand,
    sync::{Arc, Mutex},
    thread,
};

use clap::Parser;
use serde_json::{Value, json};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_cli::*;
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, LogRecord,
    NetworkId, QueryRows, QueryStrategy,
};
use datalens_storage::{
    CacheOutcome, FillOutcome, ObjectStore, QueryOutcome, QueryWatermark, QueryWatermarkKey,
    QueryWatermarkRepository, UsageLedgerEntry, UsageLedgerRepository, UsageLedgerStore,
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
        Command::Serve(command) => {
            assert_eq!(command.config, "custom.toml");
            assert_eq!(command.bind, None);
        }
        command => panic!("expected serve command, got {command:?}"),
    }
}

#[test]
fn test_serve_accepts_bind_override() {
    let cli = Cli::parse_from([
        "datalens",
        "serve",
        "--config",
        "config/datalens.compose.toml",
        "--bind",
        "127.0.0.1:3100",
    ]);

    match cli.command {
        Command::Serve(command) => {
            assert_eq!(command.config, "config/datalens.compose.toml");
            assert_eq!(command.bind.as_deref(), Some("127.0.0.1:3100"));
        }
        command => panic!("expected serve command, got {command:?}"),
    }
}

#[test]
fn test_serve_defaults_to_dev_server_config() {
    let cli = Cli::parse_from(["datalens", "serve"]);

    match cli.command {
        Command::Serve(command) => {
            assert_eq!(command.config, "config/datalens.dev.toml");
            assert_eq!(command.bind, None);
        }
        command => panic!("expected serve command, got {command:?}"),
    }
}

#[test]
fn test_serve_accepts_enable_graphql_flag() {
    assert!(Cli::try_parse_from(["datalens", "serve", "--enable-graphql"]).is_err());
}

#[test]
fn test_plan_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "plan", "--config", "custom.toml"]);

    match cli.command {
        Command::Plan(command) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected plan command, got {command:?}"),
    }
}

#[test]
fn test_plan_defaults_to_dev_server_config() {
    let cli = Cli::parse_from(["datalens", "plan"]);

    match cli.command {
        Command::Plan(command) => assert_eq!(command.config, "config/datalens.dev.toml"),
        command => panic!("expected plan command, got {command:?}"),
    }
}

#[test]
fn test_registry_migrate_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "registry", "migrate", "--config", "custom.toml"]);

    match cli.command {
        Command::Registry(RegistryCommand {
            command: RegistrySubcommand::Migrate(command),
        }) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected registry migrate command, got {command:?}"),
    }
}

#[test]
fn test_registry_migrate_reports_per_directory_counts() {
    let root = temp_storage_root("registry-migrate-counts");
    let config = write_registry_config("registry-migrate-counts", &root);
    write_test_object(
        &root.join("warmup/warmup/tasks/warmup-task.json"),
        b"warmup-task",
    );
    write_test_object(
        &root.join("warmup/warmup/cursors/warmup-task.json"),
        b"warmup-cursor",
    );
    write_test_object(
        &root.join("cache-repair/cache-repair/tasks/repair-task.json"),
        b"repair-task",
    );

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["registry", "migrate", "--config", &config])
        .current_dir(workspace_root())
        .output()
        .expect("run registry migrate");

    assert!(
        output.status.success(),
        "registry migrate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("registry migrate JSON");
    assert_eq!(summary["status"], "ok");
    assert_eq!(summary["warmup"]["tasks"]["copied"], 1);
    assert_eq!(summary["warmup"]["tasks"]["skipped"], 0);
    assert_eq!(summary["warmup"]["tasks"]["conflicts"], 0);
    assert_eq!(summary["warmup"]["tasks"]["failed"], 0);
    assert_eq!(summary["warmup"]["cursors"]["copied"], 1);
    assert_eq!(summary["cache_repair"]["tasks"]["copied"], 1);
    assert_eq!(
        std::fs::read(root.join("warmup/tasks/warmup-task.json")).expect("read clean warmup task"),
        b"warmup-task"
    );
    assert_eq!(
        std::fs::read(root.join("warmup/cursors/warmup-task.json"))
            .expect("read clean warmup cursor"),
        b"warmup-cursor"
    );
    assert_eq!(
        std::fs::read(root.join("cache-repair/tasks/repair-task.json"))
            .expect("read clean repair task"),
        b"repair-task"
    );
}

#[test]
fn test_registry_migrate_exits_nonzero_on_conflict_without_overwriting() {
    let root = temp_storage_root("registry-migrate-conflict");
    let config = write_registry_config("registry-migrate-conflict", &root);
    write_test_object(
        &root.join("warmup/warmup/tasks/warmup-task.json"),
        b"legacy",
    );
    write_test_object(&root.join("warmup/tasks/warmup-task.json"), b"clean");
    write_test_object(
        &root.join("warmup/warmup/cursors/warmup-task.json"),
        b"warmup-cursor",
    );
    write_test_object(
        &root.join("cache-repair/cache-repair/tasks/repair-task.json"),
        b"repair-task",
    );

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["registry", "migrate", "--config", &config])
        .current_dir(workspace_root())
        .output()
        .expect("run registry migrate");

    assert!(
        !output.status.success(),
        "registry migrate should fail on conflict\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("registry migrate JSON");
    assert_eq!(summary["status"], "failed");
    assert_eq!(summary["warmup"]["tasks"]["copied"], 0);
    assert_eq!(summary["warmup"]["tasks"]["skipped"], 0);
    assert_eq!(summary["warmup"]["tasks"]["conflicts"], 1);
    assert_eq!(summary["warmup"]["tasks"]["failed"], 0);
    assert_eq!(summary["warmup"]["cursors"]["copied"], 1);
    assert_eq!(summary["cache_repair"]["tasks"]["copied"], 1);
    assert_eq!(
        std::fs::read(root.join("warmup/tasks/warmup-task.json")).expect("read clean warmup task"),
        b"clean"
    );
    assert_eq!(
        std::fs::read(root.join("warmup/warmup/tasks/warmup-task.json"))
            .expect("read legacy warmup task"),
        b"legacy"
    );
}

#[test]
fn test_registry_migrate_keeps_newer_clean_registry_objects() {
    let root = temp_storage_root("registry-migrate-newer-clean");
    let config = write_registry_config("registry-migrate-newer-clean", &root);
    let legacy = json!({"id": "task", "updated_at": 10, "state": "queued"}).to_string();
    let clean = json!({"id": "task", "updated_at": 11, "state": "queued"}).to_string();
    write_test_object(
        &root.join("warmup/warmup/tasks/warmup-task.json"),
        legacy.as_bytes(),
    );
    write_test_object(
        &root.join("warmup/tasks/warmup-task.json"),
        clean.as_bytes(),
    );
    write_test_object(
        &root.join("warmup/warmup/cursors/warmup-task.json"),
        legacy.as_bytes(),
    );
    write_test_object(
        &root.join("warmup/cursors/warmup-task.json"),
        clean.as_bytes(),
    );
    write_test_object(
        &root.join("cache-repair/cache-repair/tasks/repair-task.json"),
        legacy.as_bytes(),
    );
    write_test_object(
        &root.join("cache-repair/tasks/repair-task.json"),
        clean.as_bytes(),
    );

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["registry", "migrate", "--config", &config])
        .current_dir(workspace_root())
        .output()
        .expect("run registry migrate");

    assert!(
        output.status.success(),
        "registry migrate should keep newer clean objects\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("registry migrate JSON");
    assert_eq!(summary["status"], "ok");
    assert_eq!(summary["warmup"]["tasks"]["copied"], 0);
    assert_eq!(summary["warmup"]["tasks"]["skipped"], 1);
    assert_eq!(summary["warmup"]["tasks"]["conflicts"], 0);
    assert_eq!(summary["warmup"]["cursors"]["skipped"], 1);
    assert_eq!(summary["cache_repair"]["tasks"]["skipped"], 1);
    assert_eq!(
        std::fs::read(root.join("warmup/tasks/warmup-task.json")).expect("read clean warmup task"),
        clean.as_bytes()
    );
    assert_eq!(
        std::fs::read(root.join("warmup/warmup/tasks/warmup-task.json"))
            .expect("read legacy warmup task"),
        legacy.as_bytes()
    );
}

#[test]
fn test_serve_uses_unified_query_config() {
    let config: DatalensConfig =
        toml::from_str(&minimal_config_text()).expect("minimal config should parse");
    let command = ServeCommand {
        config: "config/datalens.dev.toml".to_owned(),
        bind: None,
    };

    let edge = serve_edge_config(&config, &command);

    assert!(!edge.query.native.graphql_enabled);
    assert_eq!(edge.query.native.path, "/native/graphql");
    assert!(!edge.query.native.playground_enabled);
    assert!(!edge.query.index.graphql_enabled);
    assert_eq!(edge.query.index.path, "/index/graphql");
}

#[test]
fn test_validate_config_rejects_server_index_graphql_surface() {
    let config: DatalensConfig = toml::from_str(&minimal_config_text().replace(
        r#"
    [chains.ethereum]"#,
        r#"
    [query.index]
    graphql_enabled = true
    path = "/index/graphql"
    playground_enabled = true
    playground_path = "/index/graphiql"

    [chains.ethereum]"#,
    ))
    .expect("config parses");

    let error = validate_config(&config).expect_err("index graphql is not a server surface");

    assert!(error.message.contains("query.index.graphql_enabled"));
    assert!(error.message.contains("external application service"));
}

#[test]
fn test_authoritative_server_configs_parse_and_validate() {
    unsafe {
        std::env::set_var("DATALENS_ETHEREUM_RPC_URL", "http://example.invalid");
        std::env::set_var(
            "DATALENS_ETHEREUM_SECONDARY_RPC_URL",
            "http://example.invalid/secondary",
        );
        std::env::set_var("DATALENS_ARBITRUM_RPC_URL", "http://example.invalid");
        std::env::set_var("DATALENS_BASE_RPC_URL", "http://example.invalid");
        std::env::set_var("DATALENS_DARWINIA_RPC_URL", "http://example.invalid");
        std::env::set_var("DATALENS_SOLANA_RPC_URL", "http://example.invalid");
        std::env::set_var("DATALENS_TRON_RPC_URL", "http://example.invalid");
        std::env::set_var("DATALENS_TRONGRID_API_KEY", "");
        std::env::set_var("DATALENS_S3_BUCKET", "datalens");
        std::env::set_var("DATALENS_S3_PREFIX", "test");
        std::env::set_var("DATALENS_S3_REGION", "auto");
        std::env::set_var("DATALENS_S3_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("DATALENS_PUBLIC_APP_TOKEN", "replace-with-public-token");
        std::env::set_var("DATALENS_ORMP_TOKEN", "replace-with-ormp-token");
        std::env::set_var("DATALENS_LIVE_SMOKE_TOKEN", "replace-with-live-smoke-token");
        std::env::set_var("DATALENS_DEGOV_TOKEN", "replace-with-degov-token");
        std::env::set_var("DATALENS_METRICS_TOKEN", "replace-with-metrics-token");
    }

    for path in [
        "config/datalens.dev.toml",
        "config/datalens.compose.toml",
        "config/datalens.production.toml",
    ] {
        let config = DatalensConfig::from_file(workspace_root().join(path))
            .unwrap_or_else(|error| panic!("{path} should parse: {error}"));

        validate_config(&config).unwrap_or_else(|error| panic!("{path} should validate: {error}"));
    }
}

fn minimal_config_text() -> String {
    r#"
    [server]
    bind = "127.0.0.1:0"

    [storage]
    backend = "local"

    [storage.local]
    root = ".tmp/datalens-cli-test"

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
    "#
    .to_owned()
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
fn test_query_logs_accepts_topic0_any_of_filter() {
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
        "--topic0-any-of",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "--topic0-any-of",
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    ]);

    match cli.command {
        Command::Query(QueryCommand {
            command: QuerySubcommand::Logs(command),
            ..
        }) => {
            assert_eq!(command.topics.len(), 0);
            assert_eq!(command.topic0_any_of.len(), 2);
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
fn test_inspect_usage_accepts_application_and_config() {
    let cli = Cli::parse_from([
        "datalens",
        "inspect",
        "usage",
        "--config",
        "custom.toml",
        "--application",
        "analytics-api",
    ]);

    match cli.command {
        Command::Inspect(InspectCommand {
            command: InspectSubcommand::Usage(command),
        }) => {
            assert_eq!(command.config, "custom.toml");
            assert_eq!(command.application, "analytics-api");
        }
        command => panic!("expected inspect usage command, got {command:?}"),
    }
}

#[test]
fn test_inspect_maintenance_accepts_config_path_after_subcommand() {
    let cli = Cli::parse_from([
        "datalens",
        "inspect",
        "maintenance",
        "--config",
        "custom.toml",
    ]);

    match cli.command {
        Command::Inspect(InspectCommand {
            command: InspectSubcommand::Maintenance(command),
        }) => assert_eq!(command.config, "custom.toml"),
        command => panic!("expected inspect maintenance command, got {command:?}"),
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
    assert_eq!(entry["object"]["compression"], "none");
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
fn test_inspect_usage_reads_application_ledger() {
    let root = temp_storage_root("inspect-usage");
    let config = write_config("inspect-usage", &root);
    let ledger = UsageLedgerStore::new(datalens_storage::LocalObjectStore::new(&root));
    let selector = DatasetSelector::all();
    ledger
        .append(&UsageLedgerEntry::query_event(
            "analytics-api",
            test_chain(),
            DatasetKey::evm_blocks(),
            &selector,
            LedgerRange::blocks(30, 31).expect("valid range"),
            FinalityLevel::Safe,
            QueryOutcome::Filled,
            CacheOutcome::Miss,
            FillOutcome::Written,
            2,
        ))
        .expect("append usage");

    let output = inspect_summary(InspectCommand {
        command: InspectSubcommand::Usage(InspectUsageCommand {
            config,
            application: "analytics-api".to_owned(),
        }),
    })
    .expect("inspect usage");

    assert_eq!(output["status"], "ok");
    assert_eq!(output["usage"]["application"], "analytics-api");
    assert_eq!(output["usage"]["event_count"], 1);
    assert_eq!(
        output["usage"]["events"][0]["dataset_key"]["name"],
        "blocks"
    );
    assert_eq!(output["usage"]["events"][0]["range"]["start"], 30);
    assert_eq!(output["usage"]["events"][0]["query_outcome"], "filled");
    assert_eq!(output["usage"]["events"][0]["cache_outcome"], "miss");
    assert_eq!(output["usage"]["events"][0]["fill_outcome"], "written");
}

#[test]
fn test_inspect_maintenance_reports_stable_dry_run_json() {
    let root = temp_storage_root("inspect-maintenance");
    let storage = LocalStorage::new(&root);
    let object_key = write_block_coverage(&storage, 40, 40);
    storage
        .object_store()
        .delete(&object_key)
        .expect("delete object");
    let config = write_config("inspect-maintenance", &root);

    let output = inspect_summary(InspectCommand {
        command: InspectSubcommand::Maintenance(ConfigCommand { config }),
    })
    .expect("inspect maintenance");

    assert_eq!(output["status"], "ok");
    assert_eq!(output["read_only"], true);
    assert_eq!(output["maintenance"]["mode"], "dry_run");
    assert_eq!(output["maintenance"]["check"]["issue_count"], 1);
    assert_eq!(
        output["maintenance"]["check"]["issues"][0]["issue_kind"],
        "missing_object"
    );
    assert_eq!(
        output["maintenance"]["retention"]["policy"]["delete_current_manifest_objects"],
        false
    );
    assert_eq!(
        output["maintenance"]["usage_ledger"]["rollup_model"]["source"],
        "append_only_jsonl_events"
    );
}

#[test]
fn test_redact_url_hides_credentials_and_sensitive_query_values() {
    assert_eq!(
        redact_url("https://user:secret@example.invalid/path?token=secret&batch=evm"),
        "https://<redacted>@example.invalid/path?token=<redacted>&batch=evm"
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

        [storage.local]
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
fn test_config_parses_writer_staging_thresholds() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = ".datalens/storage"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 10
        record_empty_coverage = true

        [writer.staging]
        enabled = true
        min_rows = 20
        target_object_bytes = 2048
        max_staged_ranges = 4
        max_staged_rows = 100
        max_staged_age_ms = 5000
        flush_on_shutdown = true
        max_staged_bytes = 4096

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

    assert!(config.writer.staging.enabled);
    assert_eq!(config.writer.staging.min_rows, Some(20));
    assert_eq!(config.writer.staging.target_object_bytes, Some(2048));
    assert_eq!(config.writer.staging.max_staged_ranges, Some(4));
    assert_eq!(config.writer.staging.max_staged_rows, Some(100));
    assert_eq!(config.writer.staging.max_staged_age_ms, Some(5000));
    assert!(config.writer.staging.flush_on_shutdown);
    assert_eq!(config.writer.staging.max_staged_bytes, Some(4096));
    validate_config(&config).expect("staging config is valid");
}

#[test]
fn test_config_parses_warmup_follow_query_lookahead() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1
        follow_query_lookahead_blocks = 2048
        follow_query_start_offset_blocks = 512
        follow_query_start_offset_tiers_blocks = [5000, 3000, 1000]
        follow_query_catchup_threshold_blocks = 250
        follow_query_idle_threshold_blocks = 10
        follow_query_resume_threshold_blocks = 20
        query_activity_ttl_seconds = 600
        stale_running_ttl_ms = 120000

        [chains.ethereum]
        kind = "evm"
        chain_id = 1
        rpc_urls = ["http://example.invalid"]

        [chains.ethereum.warmup]
        follow_query_start_offset_blocks = 1000
        follow_query_start_offset_tiers_blocks = [4000, 2000, 750]
        follow_query_catchup_threshold_blocks = 300
        follow_query_idle_threshold_blocks = 30
        follow_query_resume_threshold_blocks = 60

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

    assert_eq!(config.warmup.follow_query_lookahead_blocks, 2048);
    assert_eq!(config.warmup.follow_query_start_offset_blocks, Some(512));
    assert_eq!(
        config.warmup.follow_query_start_offset_tiers_blocks,
        Some(vec![5000, 3000, 1000])
    );
    assert_eq!(config.warmup.follow_query_catchup_threshold_blocks, 250);
    assert_eq!(config.warmup.follow_query_idle_threshold_blocks, Some(10));
    assert_eq!(config.warmup.follow_query_resume_threshold_blocks, Some(20));
    assert_eq!(config.warmup.query_activity_ttl_seconds, 600);
    assert_eq!(config.warmup.stale_running_ttl_ms, 120_000);
    assert_eq!(
        config
            .chains
            .get("ethereum")
            .expect("ethereum chain")
            .warmup
            .follow_query_start_offset_blocks,
        Some(1000)
    );
    assert_eq!(
        config
            .chains
            .get("ethereum")
            .expect("ethereum chain")
            .warmup
            .follow_query_start_offset_tiers_blocks,
        Some(vec![4000, 2000, 750])
    );
    assert_eq!(
        config
            .chains
            .get("ethereum")
            .expect("ethereum chain")
            .warmup
            .follow_query_catchup_threshold_blocks,
        Some(300)
    );
    assert_eq!(
        config
            .chains
            .get("ethereum")
            .expect("ethereum chain")
            .warmup
            .follow_query_idle_threshold_blocks,
        Some(30)
    );
    assert_eq!(
        config
            .chains
            .get("ethereum")
            .expect("ethereum chain")
            .warmup
            .follow_query_resume_threshold_blocks,
        Some(60)
    );
    validate_config(&config).expect("warmup follow-query config is valid");
}

#[test]
fn test_config_parses_query_metadata_worker_settings() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.metadata]
        queue_capacity = 4096
        worker_threads = 8
        coalesced_capacity = 512

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

    assert_eq!(config.query.metadata.queue_capacity, 4096);
    assert_eq!(config.query.metadata.worker_threads, 8);
    assert_eq!(config.query.metadata.coalesced_capacity, 512);
    validate_config(&config).expect("query metadata worker config is valid");
}

#[test]
fn test_config_parses_query_durable_intent_worker_settings() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.durable_intents]
        worker_threads = 8
        claim_batch_size = 64

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

    assert!(config.query.durable_intents.enabled);
    assert_eq!(config.query.durable_intents.worker_threads, 8);
    assert_eq!(config.query.durable_intents.claim_batch_size, 64);
    validate_config(&config).expect("query durable intent worker config is valid");
}

#[test]
fn test_config_defaults_query_durable_intents_enabled() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

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
    )
    .expect("config parses");

    assert!(config.query.durable_intents.enabled);
    validate_config(&config).expect("default query durable intent config is valid");
}

#[test]
fn test_config_allows_disabling_query_durable_intents() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.durable_intents]
        enabled = false
        worker_threads = 0
        claim_batch_size = 0

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

    assert!(!config.query.durable_intents.enabled);
    validate_config(&config).expect("disabled query durable intent config is valid");
}

#[test]
fn test_config_accepts_query_durable_intent_cleanup_when_workers_disabled() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.durable_intents]
        enabled = false
        worker_threads = 0
        claim_batch_size = 0
        terminal_retention_seconds = 604800
        cleanup_max_scan = 2048
        cleanup_max_deletes = 512
        cleanup_interval_seconds = 60

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

    assert!(!config.query.durable_intents.enabled);
    assert_eq!(
        config.query.durable_intents.terminal_retention_seconds,
        Some(604800)
    );
    assert_eq!(config.query.durable_intents.cleanup_max_scan, 2048);
    assert_eq!(config.query.durable_intents.cleanup_max_deletes, 512);
    assert_eq!(config.query.durable_intents.cleanup_interval_seconds, 60);
    validate_config(&config).expect("cleanup config is valid");
}

#[test]
fn test_config_defaults_query_durable_intent_cleanup_disabled() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.durable_intents]
        enabled = false

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

    assert_eq!(
        config.query.durable_intents.terminal_retention_seconds,
        None
    );
    assert!(config.query.durable_intents.cleanup_max_scan > 0);
    assert!(config.query.durable_intents.cleanup_max_deletes > 0);
    assert!(config.query.durable_intents.cleanup_interval_seconds > 0);
    validate_config(&config).expect("default cleanup config is valid");
}

#[test]
fn test_validate_config_rejects_zero_query_durable_intent_cleanup_bounds() {
    for field in [
        "cleanup_max_scan",
        "cleanup_max_deletes",
        "cleanup_interval_seconds",
    ] {
        let config = toml::from_str::<DatalensConfig>(&format!(
            r#"
            [server]
            bind = "127.0.0.1:0"

            [storage]
            backend = "local"

            [storage.local]
            root = ".tmp/datalens-cli-test"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [query.durable_intents]
            enabled = false
            terminal_retention_seconds = 604800
            {field} = 0

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
            "#
        ))
        .expect("config parses");

        let error = validate_config(&config).expect_err("zero cleanup bound rejected");
        assert!(
            error.message.contains(field),
            "expected {field} validation error, got {error:?}"
        );
    }
}

#[test]
fn test_validate_config_rejects_zero_query_metadata_worker_threads() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.metadata]
        queue_capacity = 4096
        worker_threads = 0

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

    let error = validate_config(&config).expect_err("query metadata worker config rejected");
    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("query.metadata.worker_threads"));
}

#[test]
fn test_validate_config_rejects_zero_query_metadata_coalesced_capacity() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [query.metadata]
        queue_capacity = 4096
        worker_threads = 4
        coalesced_capacity = 0

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

    let error = validate_config(&config).expect_err("query metadata coalesced config rejected");
    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("query.metadata.coalesced_capacity"));
}

#[test]
fn test_validate_config_rejects_empty_follow_query_start_offset_tiers() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1
        follow_query_start_offset_tiers_blocks = []

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

    let error = validate_config(&config).expect_err("empty tiers rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("warmup.follow_query_start_offset_tiers_blocks")
    );
}

#[test]
fn test_validate_config_rejects_zero_chain_follow_query_start_offset_tier() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1

        [chains.ethereum]
        kind = "evm"
        chain_id = 1
        rpc_urls = ["http://example.invalid"]

        [chains.ethereum.warmup]
        follow_query_start_offset_tiers_blocks = [1000, 0]

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

    let error = validate_config(&config).expect_err("zero chain tier rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("chain ethereum warmup.follow_query_start_offset_tiers_blocks")
    );
}

#[test]
fn test_validate_config_rejects_zero_follow_query_start_offset() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1
        follow_query_start_offset_blocks = 0

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

    let error = validate_config(&config).expect_err("zero start offset rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("warmup.follow_query_start_offset_blocks")
    );
}

#[test]
fn test_validate_config_rejects_zero_chain_follow_query_start_offset() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1

        [chains.ethereum]
        kind = "evm"
        chain_id = 1
        rpc_urls = ["http://example.invalid"]

        [chains.ethereum.warmup]
        follow_query_start_offset_blocks = 0

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

    let error = validate_config(&config).expect_err("zero chain start offset rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("chain ethereum warmup.follow_query_start_offset_blocks")
    );
}

#[test]
fn test_validate_config_allows_zero_warmup_follow_query_lookahead() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1
        follow_query_lookahead_blocks = 0

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

    validate_config(&config).expect("zero lookahead means warm to safe head");
}

#[test]
fn test_validate_config_rejects_zero_warmup_stale_running_ttl() {
    let config = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-cli-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [warmup]
        enabled = true
        registry_path = ".tmp/datalens-warmup"
        scheduler_interval_ms = 1000
        max_global_tasks = 1
        max_per_chain_tasks = 1
        max_fetches_per_loop = 1
        stale_running_ttl_ms = 0

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

    let error = validate_config(&config).expect_err("zero stale running ttl is invalid");

    assert!(
        error
            .message
            .contains("warmup.stale_running_ttl_ms must be greater than zero")
    );
}

#[test]
fn test_build_query_watermarks_uses_configured_local_storage_root() {
    let root = temp_storage_root("query-watermark-builder");
    let config = toml::from_str::<DatalensConfig>(&format!(
        r#"
        [server]
        bind = "127.0.0.1:0"

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
        root.display()
    ))
    .expect("config parses");
    let watermarks = build_query_watermarks(&config).expect("build query watermark repository");
    let key = QueryWatermarkKey::new(
        "analytics-api",
        ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
            .expect("chain"),
        DatasetKey::evm_blocks(),
        &DatasetSelector::all(),
        datalens_core::LedgerRangeKind::Block,
    );

    watermarks
        .update(&QueryWatermark {
            key: key.clone(),
            latest_block: 42,
            updated_at_unix_seconds: 1,
        })
        .expect("write watermark");

    assert_eq!(
        watermarks
            .read(&key)
            .expect("read watermark")
            .expect("watermark")
            .latest_block,
        42
    );
    assert!(
        root.join("query-watermarks").exists(),
        "watermark builder should use the configured storage root"
    );
}

#[test]
fn test_validate_config_accepts_application_registry() {
    let config = toml::from_str::<DatalensConfig>(
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

        [edge.metrics]
        public = true

        [applications]
        required = true

        [[applications.applications]]
        id = "Indexer_App"
        name = "Indexer_App"
        enabled = true
        token = "secret-token"
        chains = ["ethereum"]
        datasets = ["evm.blocks"]
        operations = ["query"]

        [applications.applications.quota]
        max_query_range_blocks = 10
        max_requests_per_minute = 60
        max_concurrent_requests = 1

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
        max_addresses_per_query = 1
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("application registry is valid");
}

#[test]
fn test_validate_config_accepts_supported_application_dataset_keys() {
    let supported = [
        DatasetKey::evm_blocks(),
        DatasetKey::evm_transactions(),
        DatasetKey::evm_receipts(),
        DatasetKey::evm_logs(),
        DatasetKey::solana_slots(),
        DatasetKey::solana_blocks(),
        DatasetKey::solana_transactions(),
        DatasetKey::solana_instructions(),
        DatasetKey::solana_account_updates(),
        DatasetKey::tron_blocks(),
        DatasetKey::tron_transactions(),
        DatasetKey::tron_transaction_infos(),
        DatasetKey::tron_events(),
    ];
    let dataset_keys = supported
        .iter()
        .map(|dataset| format!(r#""{}""#, dataset.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let config = toml::from_str::<DatalensConfig>(&format!(
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

        [edge.metrics]
        public = true

        [applications]
        required = true

        [[applications.applications]]
        id = "indexer"
        name = "indexer"
        enabled = true
        token = "secret-token"
        chains = ["ethereum"]
        datasets = [{dataset_keys}]
        operations = ["query"]

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
        max_addresses_per_query = 1
        "#
    ))
    .expect("config parses");

    validate_config(&config).expect("application dataset keys are valid");
}

#[test]
fn test_validate_config_rejects_unknown_and_removed_application_dataset_keys() {
    for dataset in ["not-a-dataset", "blocks", "logs"] {
        let config = toml::from_str::<DatalensConfig>(&format!(
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

            [edge.metrics]
            public = true

            [applications]
            required = true

            [[applications.applications]]
            id = "indexer"
            name = "indexer"
            enabled = true
            token = "secret-token"
            chains = ["ethereum"]
            datasets = ["{dataset}"]
            operations = ["query"]

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
            max_addresses_per_query = 1
            "#
        ))
        .expect("config parses");

        let error = validate_config(&config).expect_err("dataset rejected");

        assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
        assert!(
            error
                .message
                .contains(&format!("references unknown dataset {dataset}"))
        );
    }
}

#[test]
fn test_validate_config_rejects_zero_application_hot_query_range_quota() {
    let config = toml::from_str::<DatalensConfig>(
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

        [edge.metrics]
        public = true

        [applications]
        required = true

        [[applications.applications]]
        id = "indexer"
        name = "indexer"
        enabled = true
        token = "secret-token"
        chains = ["ethereum"]
        datasets = ["evm.blocks"]
        operations = ["query"]

        [applications.applications.quota]
        max_hot_query_range_blocks = 0

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
        max_addresses_per_query = 1
        "#,
    )
    .expect("config parses");

    let error = validate_config(&config).expect_err("zero hot quota rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(
        error
            .message
            .contains("application indexer quota limits must be greater than zero")
    );
}

#[test]
fn test_validate_config_rejects_invalid_application_boundary_without_leaking_token() {
    let config = toml::from_str::<DatalensConfig>(
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

        [edge.metrics]
        public = true

        [applications]
        required = true

        [[applications.applications]]
        id = "../bad"
        name = "../bad"
        enabled = true
        token = "secret-token"
        chains = ["ethereum"]
        datasets = ["blocks"]

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
        max_addresses_per_query = 1
        "#,
    )
    .expect("config parses");

    let error = validate_config(&config).expect_err("invalid app rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("application id"));
    assert!(!error.message.contains("secret-token"));
}

#[test]
fn test_validate_config_rejects_top_level_storage_root() {
    let error = toml::from_str::<DatalensConfig>(
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
    .expect_err("top-level storage root is rejected");

    assert!(error.to_string().contains("unknown field `root`"));
}

#[test]
fn test_validate_config_accepts_evm_and_solana_chains_together() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = ".datalens/storage"

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
        rpc_urls = ["http://example.invalid/ethereum"]

        [chains.ethereum.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.ethereum.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2

        [chains.solana-mainnet-beta]
        kind = "solana"
        chain_id = 101
        rpc_urls = ["http://example.invalid/solana"]

        [chains.solana-mainnet-beta.datasets.blocks]
        enabled = false
        max_batch_blocks = 10

        [chains.solana-mainnet-beta.datasets.logs]
        enabled = false
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 1
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("mixed EVM and Solana config is valid");
}

#[test]
fn test_validate_config_rejects_enabled_evm_chain_with_empty_primary_rpc_url() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = ".datalens/storage"

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

        [chains.ethereum.rpc]
        primary_url = " "
        secondary_urls = ["http://secondary.example.invalid"]

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

    let error = validate_config(&config).expect_err("empty primary URL is rejected");

    assert!(error.to_string().contains("primary RPC URL"));
}

#[test]
fn test_validate_config_accepts_per_chain_log_query_strategy_differences() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = ".datalens/storage"

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
        rpc_urls = ["http://example.invalid/ethereum"]

        [chains.ethereum.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.ethereum.datasets.logs]
        enabled = true
        query_strategy = "provider_filter"
        max_get_logs_range_blocks = 10
        max_block_scan_range_blocks = 2
        max_addresses_per_query = 2

        [chains.private]
        kind = "evm"
        chain_id = 999
        rpc_urls = ["http://example.invalid/private"]

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        query_strategy = "block_range"
        max_get_logs_range_blocks = 10000
        max_block_scan_range_blocks = 1
        max_addresses_per_query = 2
        header_fetch_mode = "batch"
        header_fetch_concurrency = 3
        header_fetch_batch_size = 5
        header_cache_max_entries = 100
        header_durable_chunk_size_blocks = 25
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("config is valid");
    assert_eq!(
        config.chains["ethereum"].datasets.logs.query_strategy,
        QueryStrategy::ProviderFilter
    );
    assert_eq!(
        config.chains["private"].datasets.logs.query_strategy,
        QueryStrategy::BlockRange
    );
    assert_eq!(
        config.chains["ethereum"].datasets.logs.header_fetch_mode,
        "batch"
    );
    assert_eq!(
        config.chains["ethereum"]
            .datasets
            .logs
            .header_fetch_concurrency,
        8
    );
    assert_eq!(
        config.chains["ethereum"]
            .datasets
            .logs
            .header_fetch_batch_size,
        20
    );
    assert_eq!(
        config.chains["ethereum"]
            .datasets
            .logs
            .header_cache_max_entries,
        50_000
    );
    assert_eq!(
        config.chains["ethereum"]
            .datasets
            .logs
            .header_durable_chunk_size_blocks,
        1_000
    );
    assert_eq!(
        config.chains["private"].datasets.logs.header_fetch_mode,
        "batch"
    );
    assert_eq!(
        config.chains["private"]
            .datasets
            .logs
            .header_fetch_concurrency,
        3
    );
    assert_eq!(
        config.chains["private"]
            .datasets
            .logs
            .header_fetch_batch_size,
        5
    );
    assert_eq!(
        config.chains["private"]
            .datasets
            .logs
            .header_cache_max_entries,
        100
    );
    assert_eq!(
        config.chains["private"]
            .datasets
            .logs
            .header_durable_chunk_size_blocks,
        25
    );
}

#[test]
fn test_validate_config_rejects_zero_block_scan_range_limit() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
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
        chain_id = 999
        rpc_urls = ["http://example.invalid/private"]

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        query_strategy = "block_range"
        max_get_logs_range_blocks = 10000
        max_block_scan_range_blocks = 0
        max_addresses_per_query = 2
        "#,
    )
    .expect("config parses");

    let error = validate_config(&config).expect_err("zero block scan limit");

    assert!(error.message.contains("max_block_scan_range_blocks"));
}

#[test]
fn test_validate_config_rejects_invalid_log_header_metadata_config() {
    for (field, value) in [
        ("header_fetch_mode", "\"serial\""),
        ("header_fetch_concurrency", "0"),
        ("header_fetch_batch_size", "0"),
        ("header_cache_max_entries", "0"),
        ("header_durable_chunk_size_blocks", "0"),
    ] {
        let config: DatalensConfig = toml::from_str(&format!(
            r#"
            [server]
            bind = "127.0.0.1:8080"

            [storage]
            backend = "local"

            [storage.local]
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
            chain_id = 999
            rpc_urls = ["http://example.invalid/private"]

            [chains.private.datasets.blocks]
            enabled = true
            max_batch_blocks = 10

            [chains.private.datasets.logs]
            enabled = true
            max_get_logs_range_blocks = 10000
            max_addresses_per_query = 2
            {field} = {value}
            "#
        ))
        .expect("config parses");

        let error = validate_config(&config).expect_err("header metadata config rejected");

        assert!(
            error.message.contains(field),
            "expected {field} in error message, got {}",
            error.message
        );
    }
}

#[test]
fn test_config_parse_rejects_invalid_log_query_strategy() {
    let error = toml::from_str::<DatalensConfig>(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
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
        chain_id = 999
        rpc_urls = ["http://example.invalid/private"]

        [chains.private.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.private.datasets.logs]
        enabled = true
        query_strategy = "native_magic"
        max_get_logs_range_blocks = 10000
        max_block_scan_range_blocks = 1
        max_addresses_per_query = 2
        "#,
    )
    .expect_err("invalid query strategy");

    assert!(error.to_string().contains("query_strategy"));
    assert!(error.to_string().contains("provider_filter"));
}

#[test]
fn test_validate_config_accepts_tron_chain() {
    let config: DatalensConfig = toml::from_str(
        r#"
        [server]
        bind = "127.0.0.1:8080"

        [storage]
        backend = "local"

        [storage.local]
        root = ".datalens/storage"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        [chains.tron-mainnet]
        kind = "tron"
        chain_id = 728126428
        rpc_urls = ["http://example.invalid/tron"]

        [chains.tron-mainnet.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.tron-mainnet.datasets.logs]
        enabled = false
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 1
        "#,
    )
    .expect("config parses");

    validate_config(&config).expect("Tron config is valid");
}

#[test]
fn test_doctor_chain_summary_rejects_unknown_auto_finality_without_profile() {
    let (url, _requests) =
        start_rpc_server(vec![unsupported_tag_response(), unsupported_tag_response()]);
    let chain = ChainConfig {
        kind: "evm".to_owned(),
        chain_id: 999999,
        rpc_url: None,
        rpc_urls: vec![url],
        rpc: None,
        warmup: Default::default(),
        trongrid: Default::default(),
        finality: FinalityConfig::Auto,
        datasets: datalens_edge::config::DatasetsConfig {
            blocks: datalens_edge::config::BlocksDatasetConfig {
                enabled: true,
                max_batch_blocks: 10,
            },
            logs: datalens_edge::config::LogsDatasetConfig {
                enabled: true,
                reliability_enabled: true,
                receipt_fallback_enabled: true,
                query_strategy: Default::default(),
                max_get_logs_range_blocks: 10,
                max_block_scan_range_blocks: 10,
                max_addresses_per_query: 2,
                header_fetch_mode: "batch".to_owned(),
                header_fetch_concurrency: 8,
                header_fetch_batch_size: 20,
                header_cache_max_entries: 50_000,
                header_durable_chunk_size_blocks: 1_000,
            },
        },
    };

    let error = doctor_chain_summary("unknown", &chain).expect_err("doctor finality error");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("durable cache writes"));
}

#[test]
fn test_production_config_doctor_smoke_uses_nonsecret_environment() {
    let (url, _requests) = start_rpc_server(vec![json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0x100"
    })]);

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["doctor", "--config", "config/datalens.production.toml"])
        .current_dir(workspace_root())
        .env("DATALENS_ETHEREUM_RPC_URL", url)
        .env("DATALENS_S3_BUCKET", "datalens-production")
        .env("DATALENS_S3_PREFIX", "datalens")
        .env("DATALENS_S3_REGION", "auto")
        .env("DATALENS_S3_ENDPOINT_URL", "https://s3.example.invalid")
        .env("DATALENS_PUBLIC_APP_TOKEN", "replace-with-secret")
        .env("DATALENS_METRICS_TOKEN", "replace-with-secret")
        .env("AWS_ACCESS_KEY_ID", "replace-with-secret")
        .env("AWS_SECRET_ACCESS_KEY", "replace-with-secret")
        .output()
        .expect("run doctor binary");

    assert!(
        output.status.success(),
        "doctor failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(summary["status"], "ok");
    assert_eq!(summary["chains"][0]["finality"]["detected_height"], 160);
    assert_eq!(
        summary["chains"][0]["datasets"]["logs"]["query_strategy"],
        "provider_filter"
    );
    assert_eq!(
        summary["chains"][0]["datasets"]["logs"]["max_block_scan_range_blocks"],
        100
    );
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

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
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

fn write_registry_config(name: &str, storage_root: &std::path::Path) -> String {
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
            root = "{0}/storage"

            [planner]
            max_query_range_blocks = 100
            default_chunk_range_blocks = 10

            [writer]
            target_object_bytes = 1024
            min_object_rows = 1
            record_empty_coverage = true

            [warmup]
            registry_path = "{0}/warmup"

            [cache_repair]
            registry_path = "{0}/cache-repair"

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
    .expect("write registry config");
    config_path.to_string_lossy().into_owned()
}

fn write_test_object(path: &std::path::Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().expect("object parent")).expect("create object parent");
    std::fs::write(path, bytes).expect("write test object");
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

fn write_block_coverage(storage: &LocalStorage, start: u64, end: u64) -> String {
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
        .expect("write block coverage")
        .data_object
        .expect("data object")
        .object_key
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
