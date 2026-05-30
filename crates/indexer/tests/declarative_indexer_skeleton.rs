use std::sync::{Arc, Mutex};

use datalens_client::{
    APPLICATION_IDENTITY_HEADER, AUTHORIZATION_HEADER, DatalensClient, HttpRequest, HttpResponse,
    HttpTransport,
};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexCheckpointFileStore, IndexPlanBuilder, IndexRunner,
    IndexRunnerOptions, OutputSinkConfig,
};

#[test]
fn test_index_config_builds_plan_without_executing_tasks() {
    let config = DatalensIndexConfig::from_toml_str(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp-watcher"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 1000

[[sources]]
chain = "ethereum-mainnet"
family = "evm"
chain_id = 1
from_block = 10
to_block = 20
addresses = []
topics = []

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#,
    )
    .expect("valid config");

    let plan = IndexPlanBuilder::new().build(&config).unwrap();

    assert_eq!(plan.application(), "ormp-watcher");
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].label, "ormp.000.000000");
}

#[test]
fn test_index_runner_is_constructed_from_plan_and_output_sink() {
    let plan = datalens_indexer::IndexPlan::empty("ormp-watcher");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson);

    assert_eq!(runner.plan().application(), "ormp-watcher");
    assert_eq!(runner.output(), &OutputSinkConfig::StdoutJson);
}

#[test]
fn test_index_runner_executes_evm_log_tasks_through_client() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let transport = QueueTransport::new(vec![
        HttpResponse::json(200, response_json(10, 11, 2, true)),
        HttpResponse::json(200, response_json(12, 12, 0, false)),
    ]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson);

    let report = runner.run(&client).expect("index run");

    assert_eq!(report.planned_queries, 2);
    assert_eq!(report.tasks.len(), 2);
    assert_eq!(report.tasks[0].label, "ormp.000.000000");
    assert_eq!(report.tasks[0].chain, "ethereum");
    assert_eq!(report.tasks[0].range.start, 10);
    assert_eq!(report.tasks[0].range.end, 11);
    assert_eq!(report.tasks[0].row_count, 2);
    assert!(report.tasks[0].full_durable_hit);
    assert_eq!(report.tasks[1].provider_fill_ranges[0].start, 12);
    assert!(!report.tasks[1].full_durable_hit);

    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/v1/query");
    assert_eq!(
        requests[0].header(APPLICATION_IDENTITY_HEADER),
        Some("ormp-watcher")
    );
    assert_eq!(
        requests[0].header(AUTHORIZATION_HEADER),
        Some("Bearer runner-token")
    );
    assert_eq!(
        requests[0].body,
        serde_json::json!({
            "chain": {
                "family": "Evm",
                "configured_name": "ethereum",
                "network_id": { "kind": "numeric", "value": 1 }
            },
            "dataset_key": "evm.logs",
            "selector": {
                "kind": "evm_logs",
                "value": {
                    "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                    "topics": [["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]]
                }
            },
            "range": { "kind": "block", "start": 10, "end": 11 },
            "finality": "durable_only",
            "fields": "all"
        })
    );
}

#[test]
fn test_index_runner_writes_evm_logs_to_jsonl_and_creates_parent_directories() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let output_path = temp_path("jsonl-create").join("nested/events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(10, 12, 2, true),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    let report = runner.run(&client).expect("index run");

    assert_eq!(report.tasks[0].row_count, 2);
    let lines = std::fs::read_to_string(&output_path).expect("jsonl output");
    let rows = lines
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        serde_json::json!({
            "index": "ormp",
            "chain": "ethereum",
            "chain_id": 1,
            "dataset": "evm.logs",
            "block_number": 10,
            "block_hash": "0x01",
            "transaction_hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "transaction_index": 0,
            "log_index": 0,
            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "topics": [],
            "data": "0x",
            "removed": false
        })
    );
}

#[test]
fn test_index_runner_appends_jsonl_rows_to_existing_file() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let output_path = temp_path("jsonl-append").join("events.jsonl");
    std::fs::write(&output_path, "{\"existing\":true}\n").expect("seed output");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(10, 12, 2, true),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    runner.run(&client).expect("index run");

    let lines = std::fs::read_to_string(&output_path).expect("jsonl output");
    assert_eq!(lines.lines().count(), 3);
    assert!(lines.starts_with("{\"existing\":true}\n"));
}

#[test]
fn test_index_runner_stops_on_first_task_failure() {
    let config = index_config(1);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let transport = QueueTransport::new(vec![HttpResponse::json(
        500,
        serde_json::json!({
            "error": {
                "kind": "internal",
                "message": "server failed"
            }
        }),
    )]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson);

    let error = runner.run(&client).expect_err("first failure stops run");

    assert!(error.to_string().contains("ormp.000.000000"));
    assert_eq!(transport.requests().len(), 1);
}

#[test]
fn test_index_runner_fails_when_jsonl_output_cannot_be_written() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let output_parent = temp_path("jsonl-output-failure").join("not-a-directory");
    std::fs::write(&output_parent, "file").expect("seed parent file");
    let output_path = output_parent.join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(10, 12, 1, true),
    )]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::FileJson { path: output_path });

    let error = runner.run(&client).expect_err("output failure stops run");

    assert!(error.to_string().contains("write jsonl output"), "{error}");
    assert_eq!(transport.requests().len(), 1);
}

#[test]
fn test_checkpoint_file_store_round_trips_completed_block_by_stable_task_key() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-roundtrip").join("checkpoint.json");
    let store = IndexCheckpointFileStore::new(&checkpoint_path);

    store
        .advance(&plan.tasks()[0], plan.tasks()[0].range.end)
        .expect("advance checkpoint");
    let loaded = IndexCheckpointFileStore::new(&checkpoint_path)
        .load()
        .expect("load checkpoint");

    assert_eq!(
        loaded
            .last_completed_block(&plan.tasks()[0])
            .expect("entry"),
        plan.tasks()[0].range.end
    );
    assert_eq!(loaded.last_completed_block(&plan.tasks()[1]), Some(11));
}

#[test]
fn test_index_runner_skips_task_fully_covered_by_checkpoint() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-skip").join("checkpoint.json");
    IndexCheckpointFileStore::new(&checkpoint_path)
        .advance(&plan.tasks()[0], plan.tasks()[0].range.end)
        .expect("seed checkpoint");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(12, 12, 0, false),
    )]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson)
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path));

    let report = runner.run(&client).expect("index run");

    assert_eq!(report.planned_queries, 2);
    assert_eq!(report.checkpoint_skipped_ranges.len(), 1);
    assert_eq!(report.checkpoint_skipped_ranges[0].range.start, 10);
    assert_eq!(report.checkpoint_skipped_ranges[0].range.end, 11);
    assert_eq!(report.tasks.len(), 1);
    assert_eq!(report.tasks[0].range.start, 12);
    assert_eq!(transport.requests().len(), 1);
    assert_eq!(transport.requests()[0].body["range"]["start"], 12);
}

#[test]
fn test_index_runner_writes_checkpoint_then_second_run_skips_completed_ranges() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-first-second").join("checkpoint.json");
    let first_transport = QueueTransport::new(vec![
        HttpResponse::json(200, response_json(10, 11, 0, true)),
        HttpResponse::json(200, response_json(12, 12, 0, true)),
    ]);
    let first_client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        first_transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan.clone(), OutputSinkConfig::StdoutJson)
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path.clone()));

    let first_report = runner.run(&first_client).expect("first index run");

    assert_eq!(first_report.tasks.len(), 2);
    assert_eq!(first_transport.requests().len(), 2);
    let checkpoint = IndexCheckpointFileStore::new(&checkpoint_path)
        .load()
        .expect("load checkpoint");
    assert_eq!(checkpoint.last_completed_block(&plan.tasks()[0]), Some(12));

    let second_transport = QueueTransport::new(Vec::new());
    let second_client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        second_transport.clone(),
    )
    .expect("client config");
    let second_runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson)
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path));

    let second_report = second_runner.run(&second_client).expect("second index run");

    assert_eq!(second_report.tasks.len(), 0);
    assert_eq!(second_report.checkpoint_skipped_ranges.len(), 2);
    assert_eq!(second_transport.requests().len(), 0);
}

#[test]
fn test_index_runner_resumes_partially_completed_task_from_next_block() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let task = plan.tasks()[0].clone();
    let checkpoint_path = temp_path("checkpoint-partial").join("checkpoint.json");
    IndexCheckpointFileStore::new(&checkpoint_path)
        .advance(&plan.tasks()[0], 11)
        .expect("seed checkpoint");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(12, 12, 0, false),
    )]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson)
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path.clone()));

    let report = runner.run(&client).expect("index run");

    assert_eq!(report.checkpoint_skipped_ranges.len(), 1);
    assert_eq!(report.checkpoint_skipped_ranges[0].range.start, 10);
    assert_eq!(report.checkpoint_skipped_ranges[0].range.end, 11);
    assert_eq!(report.tasks[0].range.start, 12);
    assert_eq!(report.tasks[0].range.end, 12);
    assert_eq!(transport.requests()[0].body["range"]["start"], 12);
    let checkpoint = IndexCheckpointFileStore::new(&checkpoint_path)
        .load()
        .expect("load checkpoint");
    assert_eq!(checkpoint.last_completed_block(&task), Some(12));
}

#[test]
fn test_index_runner_from_start_ignores_checkpoint() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-from-start").join("checkpoint.json");
    IndexCheckpointFileStore::new(&checkpoint_path)
        .advance(&plan.tasks()[0], plan.tasks()[0].range.end)
        .expect("seed checkpoint");
    let transport = QueueTransport::new(vec![
        HttpResponse::json(200, response_json(10, 11, 0, true)),
        HttpResponse::json(200, response_json(12, 12, 0, true)),
    ]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson).with_options(
        IndexRunnerOptions::default()
            .with_checkpoint_path(checkpoint_path)
            .with_from_start(true),
    );

    let report = runner.run(&client).expect("index run");

    assert_eq!(report.checkpoint_skipped_ranges.len(), 0);
    assert_eq!(report.tasks.len(), 2);
    assert_eq!(transport.requests().len(), 2);
    assert_eq!(transport.requests()[0].body["range"]["start"], 10);
}

#[test]
fn test_index_runner_dry_run_reports_checkpoint_skips_without_queries_or_writes() {
    let config = index_config(2);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-dry-run").join("checkpoint.json");
    IndexCheckpointFileStore::new(&checkpoint_path)
        .advance(&plan.tasks()[0], plan.tasks()[0].range.end)
        .expect("seed checkpoint");
    let transport = QueueTransport::new(Vec::new());
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson).with_options(
        IndexRunnerOptions::default()
            .with_checkpoint_path(checkpoint_path)
            .with_dry_run(true),
    );

    let report = runner.run(&client).expect("dry run");

    assert_eq!(report.planned_queries, 2);
    assert_eq!(report.checkpoint_skipped_ranges.len(), 1);
    assert_eq!(report.tasks.len(), 0);
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn test_index_runner_does_not_advance_checkpoint_when_output_write_fails() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-output-failure").join("checkpoint.json");
    let output_parent = temp_path("checkpoint-output-failure-jsonl").join("not-a-directory");
    std::fs::write(&output_parent, "file").expect("seed parent file");
    let output_path = output_parent.join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(10, 12, 1, true),
    )]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::FileJson { path: output_path })
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path.clone()));

    let error = runner.run(&client).expect_err("output failure stops run");

    assert!(error.to_string().contains("write jsonl output"), "{error}");
    assert!(
        !checkpoint_path.exists(),
        "checkpoint must not be created after output failure"
    );
}

#[test]
fn test_index_runner_reports_corrupt_checkpoint_file() {
    let config = index_config(10);
    let plan = IndexPlanBuilder::new().build(&config).unwrap();
    let checkpoint_path = temp_path("checkpoint-corrupt").join("checkpoint.json");
    std::fs::write(&checkpoint_path, "{not json").expect("seed corrupt checkpoint");
    let transport = QueueTransport::new(Vec::new());
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client config");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson)
        .with_options(IndexRunnerOptions::default().with_checkpoint_path(checkpoint_path));

    let error = runner
        .run(&client)
        .expect_err("corrupt checkpoint fails run");

    assert!(error.to_string().contains("checkpoint"), "{error}");
    assert_eq!(transport.requests().len(), 0);
}

fn index_config(chunk_blocks: u64) -> DatalensIndexConfig {
    unsafe {
        std::env::set_var("DATALENS_INDEX_TEST_TOKEN", "runner-token");
    }
    DatalensIndexConfig::from_toml_str(&format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp-watcher"
token_env = "DATALENS_INDEX_TEST_TOKEN"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = {chunk_blocks}

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 12
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#
    ))
    .expect("valid config")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}

fn response_json(start: u64, end: u64, row_count: usize, durable_hit: bool) -> serde_json::Value {
    let range = serde_json::json!({ "kind": "block", "start": start, "end": end });
    let (hit_ranges, missing_ranges, provider_fill_ranges) = if durable_hit {
        (vec![range.clone()], Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![range.clone()], vec![range.clone()])
    };
    serde_json::json!({
        "chain": {
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": { "kind": "numeric", "value": 1 }
        },
        "dataset_key": "evm.logs",
        "range": range,
        "cache": {
            "hit_ranges": hit_ranges,
            "missing_ranges": missing_ranges,
            "durable_hit_ranges": if durable_hit { vec![range] } else { Vec::<serde_json::Value>::new() },
            "hot_hit_ranges": [],
            "provider_fill_ranges": provider_fill_ranges,
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": serde_json::to_value(DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs((0..row_count).map(|index| {
                datalens_core::LogRecord::try_new(
                    start,
                    "0x01".to_owned(),
                    format!("0x{index:064x}"),
                    0,
                    index as u64,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    Vec::new(),
                    "0x".to_owned(),
                    false,
                )
                .expect("log record")
            }).collect()),
        ).expect("rows")).expect("rows json")
    })
}

#[derive(Clone)]
struct QueueTransport {
    responses: Arc<Mutex<Vec<HttpResponse>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl QueueTransport {
    fn new(responses: Vec<HttpResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().rev().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl HttpTransport for QueueTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.requests.lock().expect("requests lock").push(request);
        self.responses
            .lock()
            .expect("responses lock")
            .pop()
            .ok_or_else(|| datalens_client::ClientError::Transport("missing response".to_owned()))
    }
}
