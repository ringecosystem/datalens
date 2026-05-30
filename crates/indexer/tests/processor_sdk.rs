use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use datalens_core::{ChainFamily, ChainIdentity, DatasetKey, LedgerRange, NetworkId};
use datalens_indexer::sdk::{
    ApplicationChainReader, ApplicationProcessor, ApplicationStore, CheckpointCursor, EventBatch,
    EventOrdering, EventOrderingKey, EventRecord, ProcessResult, ProcessorContext, ProcessorError,
    ProcessorErrorKind, ProcessorMetrics,
};
use serde_json::json;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[test]
fn test_event_batch_carries_public_processor_metadata() {
    let chain =
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1));
    let dataset = DatasetKey::evm_logs();
    let range = LedgerRange::blocks(10, 12).unwrap();
    let cursor = CheckpointCursor::new("evm/ethereum/1/logs", "block:12");
    let later_record = EventRecord {
        source_key: "ethereum:11:0:0".to_owned(),
        ordering_key: EventOrderingKey::new(11, Some(0), Some(0)),
        payload: json!({ "block_number": 11, "transaction_index": 0, "log_index": 0 }),
        decoded: None,
    };
    let earlier_record = EventRecord {
        source_key: "ethereum:10:0:1".to_owned(),
        ordering_key: EventOrderingKey::new(10, Some(0), Some(1)),
        payload: json!({ "block_number": 10, "transaction_index": 0, "log_index": 1 }),
        decoded: Some(json!({ "event": "Transfer" })),
    };

    let batch = EventBatch::new(
        chain.clone(),
        dataset.clone(),
        range.clone(),
        cursor.clone(),
        vec![later_record, earlier_record],
    );

    assert_eq!(batch.chain(), &chain);
    assert_eq!(batch.dataset(), &dataset);
    assert_eq!(batch.finalized_range(), &range);
    assert_eq!(batch.checkpoint_cursor(), &cursor);
    assert_eq!(
        batch.records()[0].decoded.as_ref().unwrap()["event"],
        "Transfer"
    );
    assert_eq!(batch.records()[1].source_key, "ethereum:11:0:0");
    assert_eq!(batch.ordering(), EventOrdering::Deterministic);
    assert_eq!(
        batch.ordering_description(),
        "records are sorted by ledger position, transaction position, event position, then source key"
    );
}

#[test]
fn test_process_result_defers_checkpoint_to_runtime_commit_boundary() {
    let cursor = CheckpointCursor::new("evm/ethereum/1/logs", "block:12");
    let result = ProcessResult::success(cursor.clone()).with_processed_records(3);

    assert_eq!(result.processed_records(), 3);
    assert_eq!(result.pending_checkpoint(), Some(&cursor));
    assert!(
        result.checkpoint_requires_runtime_commit(),
        "processor success must not directly advance checkpoint"
    );

    let skipped = ProcessResult::skipped("no matching records");
    assert_eq!(skipped.processed_records(), 0);
    assert_eq!(skipped.pending_checkpoint(), None);
}

#[test]
fn test_processor_error_classification_and_redaction() {
    let transient =
        ProcessorError::transient("rpc timeout while reading https://node.invalid/key=secret");
    assert_eq!(
        transient.kind(),
        ProcessorErrorKind::TransientInfrastructure
    );
    assert!(transient.is_retryable());
    assert_eq!(
        transient.to_string(),
        "transient infrastructure error: rpc timeout while reading <redacted>"
    );

    let data_error = ProcessorError::data("malformed log topic");
    assert_eq!(data_error.kind(), ProcessorErrorKind::NonRetryableData);
    assert!(!data_error.is_retryable());

    let user_error = ProcessorError::user("application invariant failed");
    assert_eq!(user_error.kind(), ProcessorErrorKind::UserProcessor);
    assert!(!user_error.is_retryable());

    let config_error = ProcessorError::config("missing processor setting");
    assert_eq!(config_error.kind(), ProcessorErrorKind::Config);
    assert!(!config_error.is_retryable());
}

#[tokio::test]
async fn test_application_processor_uses_context_and_store_boundaries() {
    let store = RecordingStore::default();
    let chain_reader = StaticChainReader;
    let metrics = RecordingMetrics::default();
    let mut context = ProcessorContext::new(
        "payments",
        "transfers",
        ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
        LedgerRange::blocks(20, 24).unwrap(),
    )
    .with_store(&store)
    .with_chain_reader(&chain_reader)
    .with_metrics(&metrics);
    let batch = EventBatch::new(
        ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(20, 24).unwrap(),
        CheckpointCursor::new("evm/ethereum/logs", "block:24"),
        vec![EventRecord {
            source_key: "ethereum:20:0:0".to_owned(),
            ordering_key: EventOrderingKey::new(20, Some(0), Some(0)),
            payload: json!({ "block_number": 20 }),
            decoded: None,
        }],
    );

    let result = RecordingProcessor
        .process(&mut context, &batch)
        .await
        .unwrap();

    assert_eq!(result.processed_records(), 1);
    assert_eq!(store.operations(), vec!["upsert transfer:20"]);
    assert_eq!(
        metrics.counters(),
        vec![("processor.records".to_owned(), 1)]
    );
}

struct RecordingProcessor;

impl ApplicationProcessor for RecordingProcessor {
    fn process<'a>(
        &'a self,
        context: &'a mut ProcessorContext<'a>,
        batch: &'a EventBatch,
    ) -> BoxFuture<'a, Result<ProcessResult, ProcessorError>> {
        Box::pin(async move {
            let block = context
                .chain_reader()
                .unwrap()
                .read_json(&batch.chain().clone(), "block:20")
                .await?;
            context
                .store()
                .unwrap()
                .upsert_json("transfer:20", json!({ "block": block["number"] }))
                .await?;
            context
                .metrics()
                .unwrap()
                .increment_counter("processor.records", batch.records().len() as u64);
            Ok(ProcessResult::success(batch.checkpoint_cursor().clone())
                .with_processed_records(batch.records().len()))
        })
    }
}

#[derive(Default)]
struct RecordingStore {
    operations: Arc<Mutex<Vec<String>>>,
}

impl RecordingStore {
    fn operations(&self) -> Vec<String> {
        self.operations.lock().unwrap().clone()
    }
}

impl ApplicationStore for RecordingStore {
    fn upsert_json<'a>(
        &'a self,
        key: &'a str,
        _value: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.operations
                .lock()
                .unwrap()
                .push(format!("upsert {key}"));
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ProcessorError>> {
        Box::pin(async move {
            self.operations
                .lock()
                .unwrap()
                .push(format!("delete {key}"));
            Ok(())
        })
    }
}

struct StaticChainReader;

impl ApplicationChainReader for StaticChainReader {
    fn read_json<'a>(
        &'a self,
        _chain: &'a ChainIdentity,
        _key: &'a str,
    ) -> BoxFuture<'a, Result<serde_json::Value, ProcessorError>> {
        Box::pin(async { Ok(json!({ "number": 20 })) })
    }
}

#[derive(Default)]
struct RecordingMetrics {
    counters: Arc<Mutex<Vec<(String, u64)>>>,
}

impl RecordingMetrics {
    fn counters(&self) -> Vec<(String, u64)> {
        self.counters.lock().unwrap().clone()
    }
}

impl ProcessorMetrics for RecordingMetrics {
    fn increment_counter(&self, name: &str, value: u64) {
        self.counters.lock().unwrap().push((name.to_owned(), value));
    }
}
