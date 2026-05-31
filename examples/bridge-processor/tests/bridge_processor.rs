use async_graphql::Request as GraphqlRequest;
use datalens_bridge_processor::{
    BridgeGraphqlSchema, BridgeProcessor, BridgeSchemaInitializer, MockBridgeMetadataReader,
    SqliteBridgeStore,
};
use datalens_core::{ChainFamily, ChainIdentity, DatasetKey, LedgerRange, NetworkId};
use datalens_indexer::{
    ApplicationGraphqlSchemaContext, ApplicationGraphqlSchemaHook,
    sdk::{
        ApplicationProcessor, ApplicationStoreTransaction, CheckpointCursor, EventBatch,
        EventOrderingKey, EventRecord, ProcessorContext,
    },
};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn test_bridge_processor_projects_messages_deliveries_and_aggregates() {
    let store = initialized_store().await;
    let transaction = store.begin().await.expect("begin transaction");
    let reader = MockBridgeMetadataReader::new().with_route_name(42161, "ethereum-to-arbitrum");
    let batch = bridge_batch(vec![
        message_sent_record(100, 0, "msg-1", "alice", "bob", 42161, 50),
        message_delivered_record(101, 0, "msg-1", "relayer-a"),
        message_sent_record(102, 0, "msg-2", "carol", "dave", 10, 75),
    ]);
    let mut context = bridge_context(batch.finalized_range().clone())
        .with_store(&transaction)
        .with_chain_reader(&reader);

    let result = BridgeProcessor
        .process(&mut context, &batch)
        .await
        .expect("processor succeeds");
    transaction.commit().await.expect("commit transaction");

    assert_eq!(result.processed_records(), 3);
    assert_eq!(result.pending_checkpoint(), Some(batch.checkpoint_cursor()));
    assert_eq!(
        reader.requests(),
        vec!["evm/ethereum/1:route:42161", "evm/ethereum/1:route:10"]
    );

    let messages = store.messages(None).await.expect("messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["message_id"], "msg-1");
    assert_eq!(messages[0]["status"], "delivered");
    assert_eq!(messages[0]["route_name"], "ethereum-to-arbitrum");
    assert_eq!(messages[1]["message_id"], "msg-2");
    assert_eq!(messages[1]["status"], "sent");

    let routes = store.route_counters().await.expect("route counters");
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0]["sent_count"], 1);
    assert_eq!(routes[0]["delivered_count"], 0);
    assert_eq!(routes[1]["sent_count"], 1);
    assert_eq!(routes[1]["delivered_count"], 1);
}

#[tokio::test]
async fn test_bridge_processor_rolls_back_partial_batch_failure() {
    let store = initialized_store().await;
    let transaction = store.begin().await.expect("begin transaction");
    let batch = bridge_batch(vec![
        message_sent_record(100, 0, "msg-1", "alice", "bob", 42161, 50),
        invalid_message_sent_record(101, 0, "msg-2"),
    ]);
    let mut context = bridge_context(batch.finalized_range().clone()).with_store(&transaction);

    let error = BridgeProcessor
        .process(&mut context, &batch)
        .await
        .expect_err("processor rejects invalid event");
    transaction.rollback().await.expect("rollback transaction");

    assert!(error.to_string().contains("recipient must be a string"));
    assert!(store.messages(None).await.expect("messages").is_empty());
    assert!(
        store
            .route_counters()
            .await
            .expect("route counters")
            .is_empty()
    );
}

#[tokio::test]
async fn test_bridge_processor_skips_duplicate_events_after_restart() {
    let store = initialized_store().await;
    let batch = bridge_batch(vec![message_sent_record(
        100, 0, "msg-1", "alice", "bob", 42161, 50,
    )]);

    for _ in 0..2 {
        let transaction = store.begin().await.expect("begin transaction");
        let mut context = bridge_context(batch.finalized_range().clone()).with_store(&transaction);
        BridgeProcessor
            .process(&mut context, &batch)
            .await
            .expect("processor succeeds");
        transaction.commit().await.expect("commit transaction");
    }

    let messages = store.messages(None).await.expect("messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(store.processed_events().await.expect("processed events"), 1);
    let routes = store.route_counters().await.expect("route counters");
    assert_eq!(routes[0]["sent_count"], 1);
}

#[tokio::test]
async fn test_bridge_graphql_queries_application_entities() {
    let store = initialized_store().await;
    let transaction = store.begin().await.expect("begin transaction");
    let batch = bridge_batch(vec![
        message_sent_record(100, 0, "msg-1", "alice", "bob", 42161, 50),
        message_delivered_record(101, 0, "msg-1", "relayer-a"),
    ]);
    let mut context = bridge_context(batch.finalized_range().clone()).with_store(&transaction);
    BridgeProcessor
        .process(&mut context, &batch)
        .await
        .expect("processor succeeds");
    transaction.commit().await.expect("commit transaction");
    drop(transaction);

    let schema = BridgeGraphqlSchema
        .build_schema(ApplicationGraphqlSchemaContext::new(Arc::new(store)))
        .expect("schema builds");
    let response = schema
        .execute(GraphqlRequest::new(
            r#"
            {
              bridgeMessages(account: "bob") {
                messageId
                sender
                recipient
                destinationChain
                amount
                status
              }
              bridgeRouteCounters {
                destinationChain
                sentCount
                deliveredCount
              }
            }
            "#,
        ))
        .await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let body = response.data.into_json().expect("graphql data");
    assert_eq!(body["bridgeMessages"][0]["messageId"], "msg-1");
    assert_eq!(body["bridgeMessages"][0]["status"], "delivered");
    assert_eq!(body["bridgeRouteCounters"][0]["destinationChain"], 42161);
    assert_eq!(body["bridgeRouteCounters"][0]["sentCount"], 1);
    assert_eq!(body["bridgeRouteCounters"][0]["deliveredCount"], 1);
}

async fn initialized_store() -> SqliteBridgeStore {
    let store = SqliteBridgeStore::connect("sqlite::memory:")
        .await
        .expect("store connects");
    store
        .initialize_application_schema("bridge-example", "messages", &BridgeSchemaInitializer)
        .await
        .expect("schema initializes");
    store
}

fn bridge_context(range: LedgerRange) -> ProcessorContext<'static> {
    ProcessorContext::new("bridge-example", "messages", bridge_chain(), range)
}

fn bridge_batch(records: Vec<EventRecord>) -> EventBatch {
    EventBatch::new(
        bridge_chain(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(100, 102).unwrap(),
        CheckpointCursor::new("evm/ethereum/1/logs", "102"),
        records,
    )
}

fn bridge_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

fn message_sent_record(
    block: u64,
    log_index: u64,
    message_id: &str,
    sender: &str,
    recipient: &str,
    destination_chain: u64,
    amount: u64,
) -> EventRecord {
    EventRecord {
        source_key: format!("ethereum:{block}:0:{log_index}"),
        ordering_key: EventOrderingKey::new(block, Some(0), Some(log_index)),
        payload: json!({
            "transaction_hash": format!("0xtx{block:064x}"),
            "log_index": log_index
        }),
        decoded: Some(json!({
            "event_name": "MessageSent",
            "message_id": message_id,
            "sender": sender,
            "recipient": recipient,
            "destination_chain": destination_chain,
            "amount": amount
        })),
    }
}

fn invalid_message_sent_record(block: u64, log_index: u64, message_id: &str) -> EventRecord {
    EventRecord {
        source_key: format!("ethereum:{block}:0:{log_index}"),
        ordering_key: EventOrderingKey::new(block, Some(0), Some(log_index)),
        payload: json!({ "transaction_hash": format!("0xtx{block:064x}") }),
        decoded: Some(json!({
            "event_name": "MessageSent",
            "message_id": message_id,
            "sender": "alice",
            "destination_chain": 42161,
            "amount": 1
        })),
    }
}

fn message_delivered_record(
    block: u64,
    log_index: u64,
    message_id: &str,
    relayer: &str,
) -> EventRecord {
    EventRecord {
        source_key: format!("ethereum:{block}:0:{log_index}"),
        ordering_key: EventOrderingKey::new(block, Some(0), Some(log_index)),
        payload: json!({
            "transaction_hash": format!("0xtx{block:064x}"),
            "log_index": log_index
        }),
        decoded: Some(json!({
            "event_name": "MessageDelivered",
            "message_id": message_id,
            "relayer": relayer
        })),
    }
}
