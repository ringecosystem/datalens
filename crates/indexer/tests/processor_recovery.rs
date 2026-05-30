use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

use datalens_client::{DatalensClient, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexCheckpointFileStore, IndexPlan, IndexPlanBuilder,
    ProcessorFailureStage, ProcessorRunStatus, ProcessorRuntime, ProcessorRuntimeOptions,
    sdk::{
        ApplicationProcessor, ApplicationStore, ApplicationStoreTransaction, EventBatch,
        ProcessResult, ProcessorContext, ProcessorError, TransactionalApplicationStore,
    },
};
use serde_json::json;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[tokio::test]
async fn test_processor_failure_before_write_rolls_back_without_visible_rows_or_checkpoint() {
    let root = temp_path("fail-before-write");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(100, 101, 2);
    let store = DurableMapStore::default();
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(100, 101, vec![log_row(100, 0, 0)], true),
    ))]);

    let report = ProcessorRuntime::new(
        plan.clone(),
        RecordingProcessor::default().with_failure(FailureMode::BeforeWrite),
        store.clone(),
    )
    .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
    .run(&client(transport))
    .await
    .expect("processor failure is reported");

    assert_eq!(report.status, ProcessorRunStatus::Failed);
    assert_eq!(report.failures[0].stage, ProcessorFailureStage::Process);
    assert!(report.failures[0].message.contains("failed before write"));
    assert!(store.visible_rows().is_empty());
    assert_eq!(store.counts(), (1, 0, 1));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_begin_transaction_failure_does_not_advance_checkpoint_or_leave_rows() {
    let root = temp_path("begin-failure");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(110, 111, 2);
    let store = DurableMapStore::default().with_fail_begin(true);
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(110, 111, vec![log_row(110, 0, 0)], true),
    ))]);

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(transport))
        .await
        .expect("begin failure is reported");

    assert_eq!(report.status, ProcessorRunStatus::Failed);
    assert_eq!(
        report.failures[0].stage,
        ProcessorFailureStage::BeginTransaction
    );
    assert!(report.failures[0].message.contains("begin failed"));
    assert!(store.visible_rows().is_empty());
    assert_eq!(store.counts(), (1, 0, 0));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_retry_preserves_deterministic_order_after_processor_failure() {
    let root = temp_path("retry-order");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(120, 121, 2);
    let store = DurableMapStore::default();
    let first_processor = RecordingProcessor::default().with_failure(FailureMode::AfterWrites);
    let first_transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(
            120,
            121,
            vec![log_row(121, 1, 0), log_row(120, 1, 3), log_row(120, 0, 9)],
            true,
        ),
    ))]);

    let first = ProcessorRuntime::new(plan.clone(), first_processor.clone(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(first_transport))
        .await
        .expect("first run reports failure");

    assert_eq!(first.status, ProcessorRunStatus::Failed);
    assert!(store.visible_rows().is_empty());
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);

    let second_processor = RecordingProcessor::default();
    let second_transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(
            120,
            121,
            vec![log_row(120, 1, 3), log_row(121, 1, 0), log_row(120, 0, 9)],
            true,
        ),
    ))]);

    let second = ProcessorRuntime::new(plan.clone(), second_processor.clone(), store)
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(second_transport))
        .await
        .expect("retry run");

    let expected = vec!["ethereum:120:0:9", "ethereum:120:1:3", "ethereum:121:1:0"];
    assert_eq!(second.status, ProcessorRunStatus::Succeeded);
    assert_eq!(first_processor.seen_source_keys(), expected);
    assert_eq!(second_processor.seen_source_keys(), expected);
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(121)
    );
}

#[tokio::test]
async fn test_duplicate_events_are_idempotent_with_stable_event_identity_keys() {
    let root = temp_path("duplicate-idempotent");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(130, 130, 1);
    let store = DurableMapStore::default();
    let duplicate = log_row(130, 0, 7);
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(130, 130, vec![duplicate.clone(), duplicate], true),
    ))]);

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(transport))
        .await
        .expect("duplicate run");

    let rows = store.visible_rows();
    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(report.summary.processed_records, 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows["ethereum:130:0:7"]["block_number"], 130);
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(130)
    );
}

#[derive(Clone, Copy)]
enum FailureMode {
    BeforeWrite,
    AfterWrites,
}

#[derive(Clone, Default)]
struct RecordingProcessor {
    seen_source_keys: Arc<Mutex<Vec<String>>>,
    failure: Option<FailureMode>,
}

impl RecordingProcessor {
    fn with_failure(mut self, failure: FailureMode) -> Self {
        self.failure = Some(failure);
        self
    }

    fn seen_source_keys(&self) -> Vec<String> {
        self.seen_source_keys.lock().expect("seen lock").clone()
    }
}

impl ApplicationProcessor for RecordingProcessor {
    fn process<'a>(
        &'a self,
        context: &'a mut ProcessorContext<'a>,
        batch: &'a EventBatch,
    ) -> BoxFuture<'a, Result<ProcessResult, ProcessorError>> {
        Box::pin(async move {
            if matches!(self.failure, Some(FailureMode::BeforeWrite)) {
                return Err(ProcessorError::user("failed before write"));
            }
            for record in batch.records() {
                self.seen_source_keys
                    .lock()
                    .expect("seen lock")
                    .push(record.source_key.clone());
                context
                    .store()
                    .expect("transaction store")
                    .upsert_json(&record.source_key, record.payload.clone())
                    .await?;
            }
            if matches!(self.failure, Some(FailureMode::AfterWrites)) {
                return Err(ProcessorError::user("failed after writes"));
            }
            Ok(ProcessResult::success(batch.checkpoint_cursor().clone())
                .with_processed_records(batch.records().len()))
        })
    }
}

#[derive(Clone, Default)]
struct DurableMapStore {
    state: Arc<Mutex<DurableMapState>>,
}

impl DurableMapStore {
    fn with_fail_begin(self, fail_begin: bool) -> Self {
        self.state.lock().expect("state lock").fail_begin = fail_begin;
        self
    }

    fn visible_rows(&self) -> BTreeMap<String, serde_json::Value> {
        self.state.lock().expect("state lock").rows.clone()
    }

    fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock().expect("state lock");
        (state.begins, state.commits, state.rollbacks)
    }
}

#[derive(Default)]
struct DurableMapState {
    rows: BTreeMap<String, serde_json::Value>,
    begins: usize,
    commits: usize,
    rollbacks: usize,
    fail_begin: bool,
}

impl TransactionalApplicationStore for DurableMapStore {
    fn begin_transaction<'a>(
        &'a self,
    ) -> BoxFuture<
        'a,
        Result<Box<dyn ApplicationStoreTransaction + Send + Sync + 'a>, ProcessorError>,
    > {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            state.begins += 1;
            if state.fail_begin {
                return Err(ProcessorError::transient("begin failed"));
            }
            let tx: Box<dyn ApplicationStoreTransaction + Send + Sync> =
                Box::new(DurableMapTransaction {
                    state: self.state.clone(),
                    pending: Mutex::new(BTreeMap::new()),
                });
            Ok(tx)
        })
    }
}

struct DurableMapTransaction {
    state: Arc<Mutex<DurableMapState>>,
    pending: Mutex<BTreeMap<String, Option<serde_json::Value>>>,
}

impl ApplicationStore for DurableMapTransaction {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        value: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.pending
                .lock()
                .expect("pending lock")
                .insert(key.to_owned(), Some(value));
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.pending
                .lock()
                .expect("pending lock")
                .insert(key.to_owned(), None);
            Ok(())
        })
    }
}

impl ApplicationStoreTransaction for DurableMapTransaction {
    fn commit<'a>(&'a self) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            state.commits += 1;
            for (key, value) in self.pending.lock().expect("pending lock").iter() {
                if let Some(value) = value {
                    state.rows.insert(key.clone(), value.clone());
                } else {
                    state.rows.remove(key);
                }
            }
            Ok(())
        })
    }

    fn rollback<'a>(&'a self) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.state.lock().expect("state lock").rollbacks += 1;
            self.pending.lock().expect("pending lock").clear();
            Ok(())
        })
    }
}

#[derive(Clone)]
struct QueueTransport {
    responses: Arc<Mutex<Vec<Result<HttpResponse, datalens_client::ClientError>>>>,
}

impl QueueTransport {
    fn new(responses: Vec<Result<HttpResponse, datalens_client::ClientError>>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().rev().collect())),
        }
    }
}

impl HttpTransport for QueueTransport {
    fn send(&self, _request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.responses
            .lock()
            .expect("responses lock")
            .pop()
            .unwrap_or_else(|| {
                Err(datalens_client::ClientError::Transport(
                    "missing response".to_owned(),
                ))
            })
    }
}

fn client(transport: QueueTransport) -> DatalensClient<QueueTransport> {
    DatalensClient::with_transport(
        datalens_client::DatalensClientConfig {
            endpoint: "http://127.0.0.1:3000".to_owned(),
            application: Some("processor-recovery-tests".to_owned()),
            bearer_token: None,
        },
        transport,
    )
    .expect("client")
}

fn plan(start: u64, end: u64, chunk_blocks: u64) -> IndexPlan {
    let config = DatalensIndexConfig::from_toml_str(&format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "processor-recovery-tests"
token_env = "PATH"

[index]
name = "processor"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = {chunk_blocks}

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = {start}
to_block = {end}
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = []

[output.jsonl]
path = ".data/indexes/processor/events.jsonl"

[checkpoint]
path = ".data/indexes/processor/checkpoint.json"
"#
    ))
    .expect("valid config");
    IndexPlanBuilder::new().build(&config).expect("plan")
}

fn response_json(
    start: u64,
    end: u64,
    rows: Vec<datalens_core::LogRecord>,
    durable_hit: bool,
) -> serde_json::Value {
    let range = json!({ "kind": "block", "start": start, "end": end });
    let (hit_ranges, missing_ranges, provider_fill_ranges) = if durable_hit {
        (vec![range.clone()], Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![range.clone()], vec![range.clone()])
    };
    json!({
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
            QueryRows::EvmLogs(rows),
        ).expect("rows")).expect("rows json")
    })
}

fn log_row(block: u64, transaction_index: u64, log_index: u64) -> datalens_core::LogRecord {
    datalens_core::LogRecord::try_new(
        block,
        format!("0x{block:064x}"),
        format!("0x{block:032x}{transaction_index:016x}{log_index:016x}"),
        transaction_index,
        log_index,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Vec::new(),
        "0x".to_owned(),
        false,
    )
    .expect("log record")
}

fn checkpoint_completed_block(path: &Path, plan: &IndexPlan) -> Option<u64> {
    IndexCheckpointFileStore::new(path)
        .load()
        .expect("checkpoint")
        .last_completed_block(&plan.tasks()[0])
}

fn temp_path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-processor-recovery-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}
