use std::sync::{Arc, Mutex};

use datalens_client::{
    APPLICATION_IDENTITY_HEADER, AUTHORIZATION_HEADER, DatalensClient, HttpRequest, HttpResponse,
    HttpTransport,
};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{DatalensIndexConfig, IndexPlanBuilder, IndexRunner, OutputSinkConfig};

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
