use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

use datalens_client::{DatalensClient, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexCheckpointFileStore, IndexPlan, IndexPlanBuilder, ProcessorRunStatus,
    ProcessorRuntime, ProcessorRuntimeOptions,
    sdk::{
        ApplicationDatabaseKind, ApplicationProcessor, ApplicationSchemaInitializer,
        ApplicationSchemaStore, ApplicationStore, ApplicationStoreTransaction, EventBatch,
        ProcessResult, ProcessorContext, ProcessorError, SchemaInitializationContext,
        TransactionalApplicationStore,
    },
};
use serde_json::json;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[tokio::test]
async fn test_processor_runtime_commits_store_then_advances_checkpoint() {
    let root = temp_path("success");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(10, 11, 2);
    let store = RecordingTransactionalStore::default();
    let processor = RecordingProcessor::default();
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(10, 11, vec![log_row(10, 0, 0), log_row(11, 0, 0)], true),
    ))]);
    let client = client(transport);

    let report = ProcessorRuntime::new(plan.clone(), processor.clone(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("processor run");

    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(report.summary.executed_batches, 1);
    assert_eq!(report.summary.processed_records, 2);
    assert_eq!(
        store.committed_operations(),
        vec!["upsert event:10", "upsert event:11"]
    );
    assert_eq!(store.counts(), (1, 1, 0));
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(11)
    );
}

#[tokio::test]
async fn test_processor_runtime_runs_schema_initialization_before_first_query() {
    let root = temp_path("schema-before-query");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(12, 13, 2);
    let store = RecordingTransactionalStore::default();
    let initializer = RecordingSchemaInitializer::default()
        .with_statement("CREATE TABLE IF NOT EXISTS payment_transfers");
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(12, 13, vec![log_row(12, 0, 0)], true),
    ))]);
    let client = client(transport.clone());

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_schema_initializer(initializer)
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("processor run");

    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(
        store.schema_events(),
        vec![
            "execute sqlite processor-tests processor CREATE TABLE IF NOT EXISTS payment_transfers"
        ]
    );
    assert_eq!(request_ranges(&transport), vec![(12, 13)]);
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(13)
    );
}

#[tokio::test]
async fn test_processor_runtime_schema_initialization_is_run_on_each_start() {
    let root = temp_path("schema-repeat");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(14, 15, 2);
    let store = RecordingTransactionalStore::default();
    let initializer = RecordingSchemaInitializer::default()
        .with_statement("CREATE TABLE IF NOT EXISTS payment_transfers");
    let first_transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(14, 15, Vec::new(), true),
    ))]);
    let second_transport = QueueTransport::new(Vec::new());

    ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_schema_initializer(initializer.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(first_transport))
        .await
        .expect("first processor run");
    ProcessorRuntime::new(plan, RecordingProcessor::default(), store.clone())
        .with_schema_initializer(initializer)
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client(second_transport))
        .await
        .expect("second processor run");

    assert_eq!(
        store
            .schema_events()
            .iter()
            .filter(|event| {
                event.as_str()
                    == "execute sqlite processor-tests processor CREATE TABLE IF NOT EXISTS payment_transfers"
            })
            .count(),
        2
    );
}

#[tokio::test]
async fn test_processor_runtime_schema_initialization_failure_prevents_indexing() {
    let root = temp_path("schema-failure");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(16, 17, 2);
    let store = RecordingTransactionalStore::default();
    let initializer = RecordingSchemaInitializer::default()
        .with_statement("CREATE TABLE IF NOT EXISTS payment_transfers")
        .with_fail(true);
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(16, 17, vec![log_row(16, 0, 0)], true),
    ))]);
    let client = client(transport.clone());

    let error = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_schema_initializer(initializer)
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect_err("schema initialization should fail startup");

    assert!(
        error
            .to_string()
            .contains("processor schema initialization failed")
    );
    assert!(request_ranges(&transport).is_empty());
    assert_eq!(store.counts(), (0, 0, 0));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_processor_failure_rolls_back_and_does_not_advance_checkpoint() {
    let root = temp_path("processor-failure");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(20, 21, 2);
    let store = RecordingTransactionalStore::default();
    let processor = RecordingProcessor::default().with_fail_after_writes(true);
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(20, 21, vec![log_row(20, 0, 0)], true),
    ))]);
    let client = client(transport);

    let report = ProcessorRuntime::new(plan.clone(), processor, store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("processor failure is reported");

    assert_eq!(report.status, ProcessorRunStatus::Failed);
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0]
            .message
            .contains("processor rejected batch")
    );
    assert_eq!(store.committed_operations(), Vec::<String>::new());
    assert_eq!(store.counts(), (1, 0, 1));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_store_commit_failure_rolls_back_and_does_not_advance_checkpoint() {
    let root = temp_path("commit-failure");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(30, 31, 2);
    let store = RecordingTransactionalStore::default().with_fail_commit(true);
    let processor = RecordingProcessor::default();
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(30, 31, vec![log_row(30, 0, 0)], true),
    ))]);
    let client = client(transport);

    let report = ProcessorRuntime::new(plan.clone(), processor, store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("commit failure is reported");

    assert_eq!(report.status, ProcessorRunStatus::Failed);
    assert!(report.failures[0].message.contains("commit failed"));
    assert_eq!(store.committed_operations(), Vec::<String>::new());
    assert_eq!(store.counts(), (1, 1, 1));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_fetch_failure_does_not_begin_transaction_or_advance_checkpoint() {
    let root = temp_path("fetch-failure");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(40, 41, 2);
    let store = RecordingTransactionalStore::default();
    let transport = QueueTransport::new(vec![Err(datalens_client::ClientError::Transport(
        "query service unavailable".to_owned(),
    ))]);
    let client = client(transport);

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("fetch failure is reported");

    assert_eq!(report.status, ProcessorRunStatus::Failed);
    assert!(
        report.failures[0]
            .message
            .contains("query service unavailable")
    );
    assert_eq!(store.counts(), (0, 0, 0));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_processor_runtime_resumes_after_failed_batch() {
    let root = temp_path("resume");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(50, 53, 2);
    let store = RecordingTransactionalStore::default();
    let first_transport = QueueTransport::new(vec![
        Ok(HttpResponse::json(
            200,
            response_json(50, 51, vec![log_row(50, 0, 0)], true),
        )),
        Ok(HttpResponse::json(
            200,
            response_json(52, 53, vec![log_row(52, 0, 0)], true),
        )),
    ]);
    let first_client = client(first_transport.clone());

    let first = ProcessorRuntime::new(
        plan.clone(),
        RecordingProcessor::default().with_fail_on_start(52),
        store.clone(),
    )
    .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
    .run(&first_client)
    .await
    .expect("first run reports failure");

    assert_eq!(first.status, ProcessorRunStatus::Failed);
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(51)
    );
    assert_eq!(request_ranges(&first_transport), vec![(50, 51), (52, 53)]);

    let second_transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(52, 53, vec![log_row(52, 0, 0)], true),
    ))]);
    let second_client = client(second_transport.clone());
    let second = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store)
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&second_client)
        .await
        .expect("resume run");

    assert_eq!(second.status, ProcessorRunStatus::Succeeded);
    assert_eq!(second.summary.checkpoint_skipped_ranges, 1);
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(53)
    );
    assert_eq!(request_ranges(&second_transport), vec![(52, 53)]);
}

#[tokio::test]
async fn test_processor_runtime_dry_run_does_not_query_or_mutate() {
    let root = temp_path("dry-run");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(60, 61, 2);
    let store = RecordingTransactionalStore::default();
    let transport = QueueTransport::new(Vec::new());
    let client = client(transport.clone());

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_options(
            ProcessorRuntimeOptions::default()
                .with_checkpoint_path(&checkpoint_path)
                .with_dry_run(true),
        )
        .run(&client)
        .await
        .expect("dry run");

    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(report.summary.executed_batches, 0);
    assert!(transport.requests().is_empty());
    assert_eq!(store.counts(), (0, 0, 0));
    assert_eq!(checkpoint_completed_block(&checkpoint_path, &plan), None);
}

#[tokio::test]
async fn test_empty_batch_can_commit_checkpoint() {
    let root = temp_path("empty-batch");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(70, 71, 2);
    let store = RecordingTransactionalStore::default();
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(70, 71, Vec::new(), true),
    ))]);
    let client = client(transport);

    let report = ProcessorRuntime::new(plan.clone(), RecordingProcessor::default(), store.clone())
        .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(&checkpoint_path))
        .run(&client)
        .await
        .expect("empty batch run");

    assert_eq!(report.status, ProcessorRunStatus::Succeeded);
    assert_eq!(report.summary.processed_records, 0);
    assert_eq!(store.counts(), (1, 1, 0));
    assert_eq!(
        checkpoint_completed_block(&checkpoint_path, &plan),
        Some(71)
    );
}

#[tokio::test]
async fn test_processor_runtime_delivers_deterministically_ordered_records() {
    let root = temp_path("deterministic-ordering");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(80, 81, 2);
    let processor = RecordingProcessor::default();
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(
        200,
        response_json(
            80,
            81,
            vec![log_row(81, 1, 0), log_row(80, 1, 3), log_row(80, 0, 9)],
            true,
        ),
    ))]);
    let client = client(transport);

    ProcessorRuntime::new(
        plan,
        processor.clone(),
        RecordingTransactionalStore::default(),
    )
    .with_options(ProcessorRuntimeOptions::default().with_checkpoint_path(checkpoint_path))
    .run(&client)
    .await
    .expect("ordered run");

    assert_eq!(
        processor.seen_source_keys(),
        vec!["ethereum:80:0:9", "ethereum:80:1:3", "ethereum:81:1:0"]
    );
}

#[derive(Clone, Default)]
struct RecordingProcessor {
    seen_source_keys: Arc<Mutex<Vec<String>>>,
    fail_after_writes: bool,
    fail_on_start: Option<u64>,
}

#[derive(Clone, Default)]
struct RecordingSchemaInitializer {
    statement: Option<&'static str>,
    fail: bool,
}

impl RecordingSchemaInitializer {
    fn with_statement(mut self, statement: &'static str) -> Self {
        self.statement = Some(statement);
        self
    }

    fn with_fail(mut self, fail: bool) -> Self {
        self.fail = fail;
        self
    }
}

impl ApplicationSchemaInitializer for RecordingSchemaInitializer {
    fn initialize_schema<'a>(
        &'a self,
        context: SchemaInitializationContext<'a>,
    ) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            if self.fail {
                return Err(ProcessorError::user("application schema setup failed"));
            }
            if let Some(statement) = self.statement {
                let statement = format!(
                    "{} {} {} {statement}",
                    context.store().database_kind().as_str(),
                    context.application(),
                    context.index()
                );
                context.store().execute_sql(&statement).await?;
            }
            Ok(())
        })
    }
}

impl RecordingProcessor {
    fn with_fail_after_writes(mut self, fail_after_writes: bool) -> Self {
        self.fail_after_writes = fail_after_writes;
        self
    }

    fn with_fail_on_start(mut self, start: u64) -> Self {
        self.fail_on_start = Some(start);
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
            if self.fail_on_start == Some(batch.finalized_range().start()) {
                return Err(ProcessorError::user("processor rejected batch start"));
            }
            for record in batch.records() {
                self.seen_source_keys
                    .lock()
                    .expect("seen lock")
                    .push(record.source_key.clone());
                context
                    .store()
                    .expect("transaction store")
                    .upsert_json(
                        &format!("event:{}", record.ordering_key.ledger_position),
                        record.payload.clone(),
                    )
                    .await?;
            }
            if self.fail_after_writes {
                return Err(ProcessorError::user("processor rejected batch"));
            }
            Ok(ProcessResult::success(batch.checkpoint_cursor().clone())
                .with_processed_records(batch.records().len()))
        })
    }
}

#[derive(Clone, Default)]
struct RecordingTransactionalStore {
    state: Arc<Mutex<RecordingStoreState>>,
}

impl RecordingTransactionalStore {
    fn with_fail_commit(self, fail_commit: bool) -> Self {
        self.state.lock().expect("state lock").fail_commit = fail_commit;
        self
    }

    fn committed_operations(&self) -> Vec<String> {
        self.state.lock().expect("state lock").committed.clone()
    }

    fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock().expect("state lock");
        (state.begins, state.commits, state.rollbacks)
    }

    fn schema_events(&self) -> Vec<String> {
        self.state.lock().expect("state lock").schema_events.clone()
    }

    fn record_schema_event(&self, event: String) {
        self.state
            .lock()
            .expect("state lock")
            .schema_events
            .push(event);
    }
}

#[derive(Default)]
struct RecordingStoreState {
    committed: Vec<String>,
    schema_events: Vec<String>,
    begins: usize,
    commits: usize,
    rollbacks: usize,
    fail_commit: bool,
}

impl TransactionalApplicationStore for RecordingTransactionalStore {
    fn schema_store(&self) -> Option<&dyn ApplicationSchemaStore> {
        Some(self)
    }

    fn begin_transaction<'a>(
        &'a self,
    ) -> BoxFuture<
        'a,
        Result<Box<dyn ApplicationStoreTransaction + Send + Sync + 'a>, ProcessorError>,
    > {
        Box::pin(async move {
            self.state.lock().expect("state lock").begins += 1;
            let tx: Box<dyn ApplicationStoreTransaction + Send + Sync> =
                Box::new(RecordingTransaction {
                    state: self.state.clone(),
                    pending: Mutex::new(Vec::new()),
                });
            Ok(tx)
        })
    }
}

impl ApplicationSchemaStore for RecordingTransactionalStore {
    fn database_kind(&self) -> ApplicationDatabaseKind {
        ApplicationDatabaseKind::Sqlite
    }

    fn execute_sql<'a>(&'a self, statement: &'a str) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.record_schema_event(format!("execute {statement}"));
            Ok(())
        })
    }
}

struct RecordingTransaction {
    state: Arc<Mutex<RecordingStoreState>>,
    pending: Mutex<Vec<String>>,
}

impl ApplicationStore for RecordingTransaction {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        _value: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.pending
                .lock()
                .expect("pending lock")
                .push(format!("upsert {key}"));
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.pending
                .lock()
                .expect("pending lock")
                .push(format!("delete {key}"));
            Ok(())
        })
    }
}

impl ApplicationStoreTransaction for RecordingTransaction {
    fn commit<'a>(&'a self) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            state.commits += 1;
            if state.fail_commit {
                return Err(ProcessorError::transient("commit failed"));
            }
            state
                .committed
                .extend(self.pending.lock().expect("pending lock").iter().cloned());
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
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl QueueTransport {
    fn new(responses: Vec<Result<HttpResponse, datalens_client::ClientError>>) -> Self {
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
            application: Some("processor-tests".to_owned()),
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
application = "processor-tests"
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

fn request_ranges(transport: &QueueTransport) -> Vec<(u64, u64)> {
    transport
        .requests()
        .iter()
        .map(|request| {
            let range = &request.body["range"];
            (
                range["start"].as_u64().expect("range start"),
                range["end"].as_u64().expect("range end"),
            )
        })
        .collect()
}

fn checkpoint_completed_block(path: &Path, plan: &IndexPlan) -> Option<u64> {
    IndexCheckpointFileStore::new(path)
        .load()
        .expect("checkpoint")
        .last_completed_block(&plan.tasks()[0])
}

fn temp_path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-processor-runtime-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}
