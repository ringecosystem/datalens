use std::sync::{Arc, Mutex};

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
use datalens_storage::{LocalStorage, StorageWriteRequest};

#[test]
fn test_index_backfill_accepts_required_inputs_and_dry_run() {
    let cli = Cli::parse_from([
        "datalens",
        "index",
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
        Command::Index(IndexCommand {
            command: IndexSubcommand::Backfill(command),
        }) => {
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
        command => panic!("expected index backfill command, got {command:?}"),
    }
}

#[test]
fn test_index_resume_repair_and_verify_parse_required_inputs() {
    for workflow in ["resume", "repair", "verify"] {
        let cli = Cli::parse_from([
            "datalens",
            "index",
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
            Command::Index(IndexCommand { command }) => {
                assert_eq!(index_test_common(&command).config, "custom.toml");
                assert_eq!(index_test_common(&command).chain, "ethereum");
                assert_eq!(index_test_common(&command).datasets, vec!["blocks"]);
            }
            command => panic!("expected index {workflow} command, got {command:?}"),
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
fn test_index_dry_run_plans_chunks_without_writing() {
    let root = temp_storage_root("index-dry-run");
    let config = write_config("index-dry-run", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = index_summary_with_adapter(
        IndexWorkflowCommand::Backfill(IndexBackfillCommand {
            common: index_common(config, 1, 3),
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
fn test_index_backfill_runs_fixture_runtime() {
    let root = temp_storage_root("index-backfill");
    let config = write_config("index-backfill", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = index_summary_with_adapter(
        IndexWorkflowCommand::Backfill(IndexBackfillCommand {
            common: index_common(config, 1, 3),
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
fn test_index_backfill_persists_cursor_under_configured_cursor_path() {
    let root = temp_storage_root("index-backfill-cursor");
    let config = write_config("index-backfill-cursor", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2), block(3)]);

    let output = index_summary_with_adapter(
        IndexWorkflowCommand::Backfill(IndexBackfillCommand {
            common: index_common(config, 1, 3),
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
fn test_index_resume_uses_persisted_cursor_after_process_restart() {
    let root = temp_storage_root("index-resume-cursor");
    let config = write_config("index-resume-cursor", &root);
    let first_adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    index_summary_with_adapter(
        IndexWorkflowCommand::Backfill(IndexBackfillCommand {
            common: index_common(config.clone(), 1, 2),
            dry_run: false,
        }),
        first_adapter,
    )
    .expect("backfill");

    let resumed_adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);
    let output = index_summary_with_adapter(
        IndexWorkflowCommand::Resume(IndexResumeCommand {
            common: index_common(config, 1, 2),
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
fn test_index_verify_does_not_persist_cursor() {
    let root = temp_storage_root("index-verify-cursor");
    let storage = LocalStorage::new(&root);
    write_block_coverage(&storage, 1, 2);
    let config = write_config("index-verify-cursor", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    index_summary_with_adapter(
        IndexWorkflowCommand::Verify(IndexVerifyCommand {
            common: index_common(config, 1, 2),
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
fn test_index_verify_does_not_write_data() {
    let root = temp_storage_root("index-verify");
    let storage = LocalStorage::new(&root);
    write_block_coverage(&storage, 1, 2);
    let config = write_config("index-verify", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1), block(2)]);

    let output = index_summary_with_adapter(
        IndexWorkflowCommand::Verify(IndexVerifyCommand {
            common: index_common(config, 1, 2),
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
fn test_index_command_rejects_unsafe_finality_override() {
    let root = temp_storage_root("index-unsafe-finality");
    let config = write_config("index-unsafe-finality", &root);
    let adapter = IndexFixtureAdapter::default().with_blocks(vec![block(1)]);
    let mut common = index_common(config, 1, 1);
    common.finality = Some("latest".to_owned());

    let error = index_summary_with_adapter(
        IndexWorkflowCommand::Backfill(IndexBackfillCommand {
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
        "datalens-cli-index-{name}-{}",
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

fn index_common(config: String, start: u64, end: u64) -> IndexCommonCommand {
    IndexCommonCommand {
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
        topics: Vec::new(),
    }
}

fn index_test_common(command: &IndexWorkflowCommand) -> &IndexCommonCommand {
    match command {
        IndexWorkflowCommand::Backfill(command) => &command.common,
        IndexWorkflowCommand::Resume(command) => &command.common,
        IndexWorkflowCommand::Repair(command) => &command.common,
        IndexWorkflowCommand::Verify(command) => &command.common,
    }
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
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
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    calls: Arc<Mutex<Vec<IndexSourceCall>>>,
}

impl Default for IndexFixtureAdapter {
    fn default() -> Self {
        Self {
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

    fn calls(&self) -> Vec<IndexSourceCall> {
        self.calls.lock().expect("calls").clone()
    }
}

impl ChainAdapter for IndexFixtureAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(test_chain()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_blocks())
                .with_selector(SelectorKind::All)
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
            QueryRows::EvmBlocks(rows),
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
