use datalens_chain::{AdapterCapabilities, ChainAdapter};
use datalens_core::{
    ChainFamily, ChainIdentity, CoverageLevel, DatasetId, DatasetKey, DatasetRows, LedgerRange,
    QueryRows, ResultEnvelope, TimeRange,
};
use datalens_edge::auth::AuthenticationHook;
use datalens_evm::EvmAdapterMetadata;
use datalens_planner::{PlanRequest, PlanStatus};
use datalens_writer::{
    DurableWriteRequest, DurableWriteResult, DurableWriteSegment, DurableWriter,
    DurableWriterConfig,
};

#[test]
fn workspace_exposes_architecture_boundaries() {
    let chain = ChainIdentity::expect_new(ChainFamily::Evm, "ethereum-mainnet");
    let dataset = DatasetId::expect_new("logs");
    let range = TimeRange::expect_blocks(1, 2);
    let envelope = ResultEnvelope::ok(dataset.clone(), range, Vec::<u8>::new());
    let capabilities = AdapterCapabilities::new(chain.clone()).with_dataset(DatasetKey::evm_logs());

    assert_eq!(chain.family(), ChainFamily::Evm);
    assert_eq!(range.start(), 1);
    assert_eq!(range.end(), 2);
    assert_eq!(envelope.dataset(), &dataset);
    assert_eq!(capabilities.chain(), &chain);
    assert_eq!(CoverageLevel::Missing, PlanStatus::Missing.coverage_level());

    fn assert_chain_adapter<T: ChainAdapter>() {}
    fn assert_storage_repository<T: datalens_storage::StorageRepository>() {}
    fn assert_auth_hook<T: AuthenticationHook>() {}

    let _ = EvmAdapterMetadata::default();
    let _ = PlanRequest::new(chain.clone(), dataset.clone(), range);
    let _writer = DurableWriter::new(
        datalens_storage::LocalStorage::new(std::env::temp_dir().join("datalens-smoke")),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );
    let _write_request = DurableWriteRequest {
        chain,
        dataset_key: DatasetKey::evm_logs(),
        selector: datalens_chain::DatasetSelector::all(),
        finality_level: datalens_chain::FinalityLevel::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(1, 2).expect("valid range"),
            rows: DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
                .expect("dataset rows"),
        }],
    };
    let _write_result = DurableWriteResult::default();

    let _chain_adapter_type_check = assert_chain_adapter::<datalens_evm::EvmAdapter>;
    let _storage_type_check = assert_storage_repository::<datalens_storage::LocalStorage>;
    let _auth_hook_type_check = assert_auth_hook::<datalens_edge::auth::NoAuthentication>;
}

#[test]
fn edge_public_contract_omits_placeholder_modules_and_block_only_query_dtos() {
    let edge_lib = include_str!("../../edge/src/lib.rs");
    let query_contract = include_str!("../../edge/src/contract/query.rs");
    let block_cache_summary = ["Cache", "Summary"].concat();
    let block_query_segment = ["Query", "Segment"].concat();

    assert!(!edge_lib.contains("pub mod compatibility;"));
    assert!(!edge_lib.contains("pub mod native;"));
    assert!(!edge_lib.contains("pub mod streaming;"));
    assert!(!edge_lib.contains(&format!("{block_cache_summary}, FieldSelectionApi")));
    assert!(!edge_lib.contains(&format!("QueryRangeApi, {block_query_segment},")));
    assert!(!query_contract.contains(&format!("pub struct {block_cache_summary} {{")));
    assert!(!query_contract.contains(&format!("pub struct {block_query_segment} {{")));
}

#[test]
fn serve_path_builds_registry_without_first_chain_selection() {
    let source = include_str!("../src/commands/serve.rs");

    assert!(source.contains("build_service_registry(&config)?"));
    assert!(!source.contains("fn first_chain("));
    assert!(!source.contains("application_index_config"));
    assert!(!source.contains("IndexDaemon"));
    assert!(!source.contains("with_extra_router"));
}

#[test]
fn cli_command_implementations_are_split_by_boundary() {
    let commands_mod = include_str!("../src/commands/mod.rs");
    let serve = include_str!("../src/commands/serve.rs");
    let doctor = include_str!("../src/commands/doctor.rs");
    let query = include_str!("../src/commands/query.rs");
    let inspect = include_str!("../src/commands/inspect.rs");
    let index = include_str!("../src/commands/index.rs");
    let helpers = include_str!("../src/commands/helpers.rs");
    let root = include_str!("../src/commands/root.rs");
    let lib = include_str!("../src/lib.rs");

    assert!(commands_mod.contains("mod serve;"));
    assert!(commands_mod.contains("mod doctor;"));
    assert!(commands_mod.contains("mod query;"));
    assert!(commands_mod.contains("mod inspect;"));
    assert!(commands_mod.contains("mod index;"));
    assert!(commands_mod.contains("mod root;"));
    assert!(!commands_mod.contains("fn serve_command("));
    assert!(!commands_mod.contains("fn inspect_summary("));
    assert!(!commands_mod.contains("pub struct Cli"));
    assert!(!lib.contains("fn "));

    for source in [serve, doctor, query, inspect, index, helpers, root] {
        assert!(source.lines().count() <= 800);
    }
}

#[test]
fn production_boundary_artifacts_are_declared() {
    let dockerfile = include_str!("../../../Dockerfile");
    let dockerignore = include_str!("../../../.dockerignore");
    let production_config = include_str!("../../../config/datalens.production.toml");
    let production_spec = include_str!("../../../docs/spec/production-runtime.md");
    let production_runbook = include_str!("../../../docs/runbook/production.md");
    let justfile = include_str!("../../../Justfile");

    assert!(dockerfile.contains("cargo build --locked --release --package datalens-cli"));
    assert!(dockerfile.contains("COPY --from=builder /app/target/release/datalens"));
    assert!(dockerfile.contains("USER datalens"));
    assert!(dockerignore.contains(".env"));
    assert!(dockerignore.contains("tests/fixtures"));
    assert!(dockerignore.contains(".tmp"));

    assert!(production_config.contains("rpc_urls = [\"${DATALENS_ETHEREUM_RPC_URL}\"]"));
    assert!(production_config.contains("token = \"${DATALENS_PUBLIC_APP_TOKEN}\""));
    assert!(production_config.contains("bucket = \"${DATALENS_S3_BUCKET}\""));
    assert!(production_config.contains("prefix = \"${DATALENS_S3_PREFIX}\""));

    assert!(production_spec.contains("Production release artifact"));
    assert!(production_spec.contains("inspect and maintenance writes"));
    assert!(production_runbook.contains("just release-check"));
    assert!(
        production_runbook.contains("datalens doctor --config config/datalens.production.toml")
    );
    assert!(justfile.contains("release-check:"));
    assert!(justfile.contains("container-smoke:"));
    assert!(justfile.contains("config-doctor-smoke:"));
}

#[test]
fn local_compose_deployment_artifacts_are_declared() {
    let compose = include_str!("../../../docker-compose.yml");
    let env_example = include_str!("../../../.env.example");
    let dev_config = include_str!("../../../config/datalens.dev.toml");
    let compose_config = include_str!("../../../config/datalens.compose.toml");
    let gitignore = include_str!("../../../.gitignore");
    let runbook = include_str!("../../../docs/runbook/local-rustfs.md");

    assert!(compose.contains("datalens-server:"));
    assert!(!compose.contains("datalens-index-daemon:"));
    assert!(compose.contains("condition: service_healthy"));

    assert!(env_example.contains("DATALENS_SERVER_CONFIG=/etc/datalens/datalens.toml"));
    assert!(env_example.contains("# Optional external application index service examples"));
    assert!(!env_example.contains("http://127.0.0.1:8080/graphql"));
    assert!(env_example.contains("DATALENS_INDEX_GRAPHQL_URL=http://127.0.0.1:3000/index/graphql"));
    assert!(env_example.contains("DATALENS_INDEX_DATABASE_URL="));
    assert!(env_example.contains("DATALENS_SOLANA_RPC_URL="));
    assert!(env_example.contains("DATALENS_TRON_RPC_URL="));
    assert!(env_example.contains("DATALENS_TRONGRID_API_KEY="));

    assert!(dev_config.contains("root = \".tmp/datalens-dev\""));
    assert!(compose_config.contains("bucket = \"${DATALENS_S3_BUCKET}\""));
    assert!(compose_config.contains("endpoint_url = \"${DATALENS_S3_ENDPOINT_URL}\""));
    assert!(compose_config.contains("token = \"${DATALENS_ORMP_TOKEN}\""));
    assert!(!compose_config.contains("[index.application"));
    assert!(!compose_config.contains(&format!("DATALENS_{}", "INDEX_DATABASE_URL")));
    assert!(compose_config.contains("[chains.solana-mainnet-beta]"));
    assert!(compose_config.contains("rpc_urls = [\"${DATALENS_SOLANA_RPC_URL}\"]"));
    assert!(compose_config.contains("[chains.tron-mainnet]"));
    assert!(compose_config.contains("rpc_urls = [\"${DATALENS_TRON_RPC_URL}\"]"));
    assert!(compose_config.contains("api_key = \"${DATALENS_TRONGRID_API_KEY}\""));
    assert!(
        compose_config
            .contains("chains = [\"ethereum\", \"solana-mainnet-beta\", \"tron-mainnet\"]")
    );
    assert!(compose_config.contains("datasets = [\"evm.blocks\", \"evm.logs\", \"solana.slots\", \"solana.transactions\", \"solana.instructions\", \"tron.blocks\", \"tron.events\"]"));

    assert!(gitignore.contains(".data/"));
    assert!(runbook.contains("docker compose up -d rustfs-init postgres"));
    assert!(runbook.contains("docker compose --profile datalens up -d --build"));
    assert!(runbook.contains("--chain solana-mainnet-beta"));
    assert!(runbook.contains("--chain tron-mainnet"));
}

#[test]
fn token_sdk_live_examples_match_compose_live_smoke_grants() {
    let compose_config = include_str!("../../../config/datalens.compose.toml");
    let go_example = include_str!("../../../examples/token-sdk-go/main.go");
    let typescript_example = include_str!("../../../examples/token-sdk-typescript/src/example.ts");
    let config: toml::Value = toml::from_str(compose_config).expect("parse compose config");

    let live_smoke = config["applications"]["applications"]
        .as_array()
        .expect("applications array")
        .iter()
        .find(|application| application["id"].as_str() == Some("live-smoke"))
        .expect("live-smoke application");
    let granted_chains = live_smoke["chains"].as_array().expect("live-smoke chains");
    let granted_datasets = live_smoke["datasets"]
        .as_array()
        .expect("live-smoke datasets");
    let solana_chain_id = config["chains"]["solana-mainnet-beta"]["chain_id"]
        .as_integer()
        .expect("solana chain id");

    assert!(
        granted_chains
            .iter()
            .any(|chain| chain.as_str() == Some("solana-mainnet-beta"))
    );
    assert!(
        granted_datasets
            .iter()
            .any(|dataset| dataset.as_str() == Some("solana.transactions"))
    );
    assert_eq!(solana_chain_id, 101);

    for source in [go_example, typescript_example] {
        assert!(source.contains("solana-mainnet-beta"));
        assert!(source.contains("transactions"));
        assert!(!source.contains("account_updates"));
    }
    assert!(go_example.contains("NetworkID:      &datalens.NetworkID{Numeric: intPtr(101)}"));
    assert!(!go_example.contains("Textual: \"mainnet-beta\""));
    assert!(typescript_example.contains("networkId: { numeric: 101 }"));
    assert!(!typescript_example.contains("textual: \"mainnet-beta\""));
}

#[test]
fn authoritative_server_config_files_are_declared() {
    for path in [
        "../../config/datalens.dev.toml",
        "../../config/datalens.compose.toml",
        "../../config/datalens.production.toml",
    ] {
        let source =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
                .expect("read authoritative server config");
        assert!(!source.contains("[index.application"));
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(!root.join("datalens.toml").exists());
    assert!(
        !root
            .join(
                ["config", &["datalens", "local", "toml"].join(".")]
                    .iter()
                    .collect::<std::path::PathBuf>(),
            )
            .exists()
    );
    assert!(
        !root
            .join(
                [
                    "examples",
                    "config",
                    &["datalens", "server", "production", "toml"].join(".")
                ]
                .iter()
                .collect::<std::path::PathBuf>(),
            )
            .exists()
    );
}
