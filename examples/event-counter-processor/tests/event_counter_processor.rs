use std::sync::Arc;

use async_graphql::Request as GraphqlRequest;
use datalens_client::{ClientError, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{
    ChainFamily, ChainIdentity, DatasetKey, DatasetRows, LedgerRange, LogRecord, NetworkId,
    QueryRows,
};
use datalens_event_counter_processor::{
    EventCounterGraphqlSchema, EventCounterProcessor, EventCounterSchemaInitializer,
    SqliteEventCounterStore,
};
use datalens_indexer::{
    ApplicationGraphqlSchemaContext, ApplicationGraphqlSchemaHook, CheckpointPolicy,
    IndexCheckpointFileStore, IndexPlanBuilder, ProcessorRunStatus, ProcessorRuntime,
    ProcessorRuntimeOptions,
    sdk::{
        ApplicationProcessor, ApplicationStoreTransaction, CheckpointCursor, EventBatch,
        EventOrderingKey, EventRecord, ProcessorContext,
    },
};
use serde_json::json;
use std::sync::Mutex;

#[tokio::test]
async fn test_processor_counts_decoded_transfer_events() {
    let store = SqliteEventCounterStore::connect("sqlite::memory:")
        .await
        .expect("store connects");
    store
        .initialize_application_schema(
            "example-counter",
            "transfers",
            &EventCounterSchemaInitializer,
        )
        .await
        .expect("schema initializes");
    let transaction = store.begin().await.expect("begin transaction");
    let chain =
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1));
    let batch = EventBatch::new(
        chain.clone(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(10, 11).unwrap(),
        CheckpointCursor::new("evm/ethereum/1/logs", "11"),
        vec![
            transfer_record(10, 0, "ethereum:10:0:0"),
            transfer_record(11, 0, "ethereum:11:0:0"),
            approval_record(11, 1, "ethereum:11:0:1"),
        ],
    );
    let mut context = ProcessorContext::new(
        "example-counter",
        "transfers",
        chain,
        batch.finalized_range().clone(),
    )
    .with_store(&transaction);

    let result = EventCounterProcessor::default()
        .process(&mut context, &batch)
        .await
        .expect("processor succeeds");
    transaction.commit().await.expect("commit transaction");

    assert_eq!(result.processed_records(), 2);
    assert_eq!(result.pending_checkpoint(), Some(batch.checkpoint_cursor()));
    let counters = store.event_counters(None).await.expect("query counters");
    assert_eq!(counters.len(), 1);
    assert_eq!(counters[0].chain, "evm/ethereum/1");
    assert_eq!(counters[0].event_name, "Transfer");
    assert_eq!(counters[0].total_count, 2);
    assert_eq!(counters[0].last_block, 11);
    assert_eq!(counters[0].last_source_key, "ethereum:11:0:0");
}

#[tokio::test]
async fn test_graphql_hook_queries_application_event_counters() {
    let store = SqliteEventCounterStore::connect("sqlite::memory:")
        .await
        .expect("store connects");
    store
        .initialize_application_schema(
            "example-counter",
            "transfers",
            &EventCounterSchemaInitializer,
        )
        .await
        .expect("schema initializes");
    store
        .increment_counter("evm/ethereum/1", "Transfer", 3, 42, "ethereum:42:0:0")
        .await
        .expect("insert counter");
    let schema = EventCounterGraphqlSchema
        .build_schema(ApplicationGraphqlSchemaContext::new(Arc::new(store)))
        .expect("schema builds");

    let response = schema
        .execute(GraphqlRequest::new(
            r#"
            {
              eventCounters(eventName: "Transfer") {
                chain
                eventName
                totalCount
                lastBlock
                lastSourceKey
              }
            }
            "#,
        ))
        .await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    let counters = body["eventCounters"].as_array().expect("counters array");
    assert_eq!(counters.len(), 1);
    assert_eq!(counters[0]["chain"], "evm/ethereum/1");
    assert_eq!(counters[0]["eventName"], "Transfer");
    assert_eq!(counters[0]["totalCount"], 3);
    assert_eq!(counters[0]["lastBlock"], 42);
}

#[tokio::test]
async fn test_runtime_commits_application_rows_before_checkpoint() {
    let root = unique_temp_dir("runtime");
    let checkpoint_path = root.join("checkpoint.json");
    let store = SqliteEventCounterStore::connect("sqlite::memory:")
        .await
        .expect("store connects");
    let config =
        datalens_indexer::DatalensIndexConfig::from_toml_str(&example_config(&checkpoint_path))
            .expect("config parses");
    let plan = IndexPlanBuilder::new().build(&config).expect("plan builds");
    let transport = datalens_indexer_test_transport(vec![runtime_response_json()]);
    let client = datalens_client::DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport,
    )
    .expect("client builds");

    let report = ProcessorRuntime::new(
        plan.clone(),
        EventCounterProcessor::default(),
        store.clone(),
    )
    .with_schema_initializer(EventCounterSchemaInitializer)
    .with_options(ProcessorRuntimeOptions::default().with_checkpoint_policy(
        CheckpointPolicy::File {
            path: checkpoint_path.clone(),
        },
    ))
    .run(&client)
    .await
    .expect("runtime runs");

    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(store.event_counters(None).await.expect("counters").len(), 1);
    assert_eq!(
        IndexCheckpointFileStore::new(&checkpoint_path)
            .load()
            .expect("checkpoint")
            .last_completed_block(&plan.tasks()[0]),
        Some(20)
    );
}

fn transfer_record(block: u64, log_index: u64, source_key: &str) -> EventRecord {
    EventRecord {
        source_key: source_key.to_owned(),
        ordering_key: EventOrderingKey::new(block, Some(0), Some(log_index)),
        payload: json!({
            "block_number": block,
            "transaction_hash": format!("0xtx{block:064x}"),
            "log_index": log_index
        }),
        decoded: Some(json!({
            "event_name": "Transfer",
            "signature": "Transfer(address,address,uint256)",
            "from": "0x0000000000000000000000000000000000000001",
            "to": "0x0000000000000000000000000000000000000002",
            "value": "7"
        })),
    }
}

fn approval_record(block: u64, log_index: u64, source_key: &str) -> EventRecord {
    EventRecord {
        source_key: source_key.to_owned(),
        ordering_key: EventOrderingKey::new(block, Some(0), Some(log_index)),
        payload: json!({ "block_number": block }),
        decoded: Some(json!({ "event_name": "Approval" })),
    }
}

fn example_config(checkpoint_path: &std::path::Path) -> String {
    format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "example-counter"
token_env = "PATH"

[index]
name = "transfers"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 10

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 20
to_block = 20
addresses = ["0x0000000000000000000000000000000000000001"]
topics = ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]

[output]
kind = "database"

[output.database]
driver = "sqlite"
url = "sqlite:.data/examples/event-counter/index.db"

[query]
enabled = true
protocol = "graphql"
bind = "127.0.0.1:9091"
path = "/graphql"
playground = true

[checkpoint]
path = "{}"
"#,
        checkpoint_path.display()
    )
}

fn runtime_response_json() -> serde_json::Value {
    let range = json!({ "kind": "block", "start": 20, "end": 20 });
    let rows = DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(vec![
            LogRecord::try_new(
                20,
                "0x0000000000000000000000000000000000000000000000000000000000000020".to_owned(),
                "0x0000000000000000000000000000000000000000000000000000000000001020".to_owned(),
                0,
                0,
                "0x0000000000000000000000000000000000000001",
                vec![
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_owned(),
                ],
                "0x".to_owned(),
                false,
            )
            .expect("log row"),
        ]),
    )
    .expect("dataset rows");
    json!({
        "chain": {
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": { "kind": "numeric", "value": 1 }
        },
        "dataset_key": "evm.logs",
        "range": range,
        "cache": {
            "hit_ranges": [range],
            "missing_ranges": [],
            "durable_hit_ranges": [{"kind":"block","start":20,"end":20}],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": serde_json::to_value(rows).expect("rows json")
    })
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "datalens-event-counter-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[derive(Clone)]
struct TestTransport {
    responses: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl TestTransport {
    fn new(responses: Vec<serde_json::Value>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().rev().collect())),
        }
    }
}

impl HttpTransport for TestTransport {
    fn send(&self, _request: HttpRequest) -> Result<HttpResponse, ClientError> {
        self.responses
            .lock()
            .unwrap()
            .pop()
            .map(|body| HttpResponse::json(200, body))
            .ok_or_else(|| ClientError::Transport("no queued response".to_owned()))
    }
}

fn datalens_indexer_test_transport(responses: Vec<serde_json::Value>) -> TestTransport {
    TestTransport::new(responses)
}
