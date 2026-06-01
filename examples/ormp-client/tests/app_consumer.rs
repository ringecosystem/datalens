use datalens_example_ormp_client::{
    RunSummary,
    config::AppConfig,
    datalens::DatalensOrmpClient,
    db::AppDatabase,
    handlers::message_accepted::{MessageAcceptedHandler, handle_message_accepted_page},
};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

const MESSAGE_ACCEPTED_TOPIC0: &str =
    "0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18";
const MESSAGE_ACCEPTED_MSG_HASH: &str =
    "0x60f5743a8b3bbe4e4bd99607b19985203a9310f4859e03912ed086f4d32bdff8";
const MESSAGE_ACCEPTED_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000013b2211a7ca45db2808f6db05557ce5347e3634e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000000002e0000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000001d874000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000a4394d1bca0000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa850000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa8500000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000008844866883501484100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

#[test]
fn test_migrations_create_application_tables_and_indexes() {
    let db = AppDatabase::open("sqlite::memory:").expect("open database");
    db.migrate().expect("run migrations");

    assert!(
        db.table_exists("consumer_checkpoints")
            .expect("checkpoint table")
    );
    assert!(db.table_exists("ormp_messages").expect("messages table"));
    assert!(
        db.index_exists("idx_ormp_messages_event_cursor")
            .expect("event cursor index")
    );
}

#[test]
fn test_handle_message_accepted_page_writes_business_rows_and_checkpoint() {
    let db = migrated_db();
    let handler = MessageAcceptedHandler::new("ormp-message-consumer");
    let page = message_page("cursor-2", true);

    let summary = handle_message_accepted_page(&db, &handler, page).expect("handle page");

    assert_eq!(summary.fetched_rows, 1);
    assert_eq!(summary.inserted_rows, 1);
    assert_eq!(summary.skipped_duplicates, 0);
    assert_eq!(summary.skipped_invalid, 0);
    assert_eq!(db.message_count().expect("message count"), 1);
    assert_eq!(
        db.checkpoint("ormp-message-consumer").expect("checkpoint"),
        Some("11".to_owned())
    );
}

#[test]
fn test_handle_message_accepted_page_is_idempotent_for_duplicate_events() {
    let db = migrated_db();
    let handler = MessageAcceptedHandler::new("ormp-message-consumer");
    handle_message_accepted_page(&db, &handler, message_page("cursor-2", true))
        .expect("first page");

    let summary = handle_message_accepted_page(&db, &handler, message_page("cursor-2", true))
        .expect("duplicate page");

    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.skipped_duplicates, 1);
    assert_eq!(db.message_count().expect("message count"), 1);
}

#[test]
fn test_checkpoint_does_not_advance_when_business_write_fails() {
    let db = migrated_db();
    let handler = MessageAcceptedHandler::new("ormp-message-consumer");
    db.insert_checkpoint("ormp-message-consumer", "cursor-1")
        .expect("seed checkpoint");
    db.transaction(|tx| {
        tx.execute(
            "INSERT INTO ormp_messages (
                message_hash,
                event_cursor,
                raw_event_json,
                created_at
            ) VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
            ("0xother", "11:0xtx:3", "{}"),
        )?;
        Ok(())
    })
    .expect("seed conflicting message");

    let err = handle_message_accepted_page(&db, &handler, message_page("cursor-2", true))
        .expect_err("unique cursor should fail");

    assert!(err.to_string().contains("event cursor already belongs"));
    assert_eq!(db.message_count().expect("message count"), 1);
    assert_eq!(
        db.checkpoint("ormp-message-consumer").expect("checkpoint"),
        Some("cursor-1".to_owned())
    );
}

#[test]
fn test_run_once_resumes_with_stored_checkpoint_cursor() {
    let db = migrated_db();
    db.insert_checkpoint("ormp-message-consumer", "11")
        .expect("seed checkpoint");
    let server = MockGraphqlServer::new(vec![rest_page("11:0xtx:3", true, false)]);
    let client = DatalensOrmpClient::new(sdk_client(&server));
    let config = test_config(&server, 10, Some(12), 2);

    let summary = datalens_example_ormp_client::run_once(&config, &db, &client).expect("run once");

    assert_eq!(
        summary,
        RunSummary {
            fetched_rows: 1,
            inserted_rows: 1,
            skipped_duplicates: 0,
            skipped_invalid: 0,
            checkpoint_cursor: Some("13".to_owned()),
            has_next_page: false,
        }
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body["range"]["start"], 11);
    assert_eq!(requests[0].body["range"]["end"], 12);
}

#[test]
fn test_run_until_complete_reads_all_chunks_and_advances_checkpoint() {
    let db = migrated_db();
    let server = MockGraphqlServer::new(vec![
        rest_page("10:0xtx-one:3", true, false),
        rest_page("11:0xtx-two:3", true, false),
    ]);
    let client = DatalensOrmpClient::new(sdk_client(&server));
    let config = test_config(&server, 10, Some(11), 1);

    let summary =
        datalens_example_ormp_client::run_until_complete(&config, &db, &client).expect("run all");

    assert_eq!(summary.fetched_rows, 2);
    assert_eq!(summary.inserted_rows, 2);
    assert_eq!(summary.skipped_duplicates, 0);
    assert_eq!(summary.skipped_invalid, 0);
    assert_eq!(summary.checkpoint_cursor.as_deref(), Some("12"));
    assert!(!summary.has_next_page);
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body["range"]["start"], 10);
    assert_eq!(requests[0].body["range"]["end"], 10);
    assert_eq!(requests[1].body["range"]["start"], 11);
    assert_eq!(requests[1].body["range"]["end"], 11);
}

#[test]
fn test_run_until_complete_with_reset_replays_duplicate_events() {
    let db = migrated_db();
    let server = MockGraphqlServer::new(vec![
        rest_page("10:0xtx-one:3", true, false),
        rest_page("10:0xtx-one:3", true, false),
    ]);
    let client = DatalensOrmpClient::new(sdk_client(&server));
    let mut config = test_config(&server, 10, Some(10), 1);
    config.reset_checkpoint = true;

    let first =
        datalens_example_ormp_client::run_until_complete(&config, &db, &client).expect("first run");
    let second = datalens_example_ormp_client::run_until_complete(&config, &db, &client)
        .expect("duplicate run");

    assert_eq!(first.inserted_rows, 1);
    assert_eq!(first.skipped_duplicates, 0);
    assert_eq!(second.inserted_rows, 0);
    assert_eq!(second.skipped_duplicates, 1);
    assert_eq!(second.skipped_invalid, 0);
    assert_eq!(db.message_count().expect("message count"), 1);
}

fn migrated_db() -> AppDatabase {
    let db = AppDatabase::open("sqlite::memory:").expect("open database");
    db.migrate().expect("run migrations");
    db
}

fn sdk_client(server: &MockGraphqlServer) -> DatalensClient {
    DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: Some("ormp-client-test".to_owned()),
        timeout: None,
        user_agent: Some("datalens-ormp-client-example-tests".to_owned()),
    })
    .expect("client config")
}

fn test_config(
    server: &MockGraphqlServer,
    start_block: i32,
    end_block: Option<i32>,
    chunk_size: u32,
) -> AppConfig {
    AppConfig {
        datalens_endpoint: server.endpoint(),
        token: None,
        application: "ormp-client-test".to_owned(),
        database_url: "sqlite::memory:".to_owned(),
        chain_name: "ethereum".to_owned(),
        chain_id: 1,
        dataset_family: "evm".to_owned(),
        dataset_name: "logs".to_owned(),
        contract_address: "0x13b2211a7ca45db2808f6db05557ce5347e3634e".to_owned(),
        event_topic0: MESSAGE_ACCEPTED_TOPIC0.to_owned(),
        event_signature: datalens_example_ormp_client::datalens::MESSAGE_ACCEPTED_SIGNATURE
            .to_owned(),
        start_block,
        end_block,
        chunk_size,
        reset_checkpoint: false,
        consumer_name: "ormp-message-consumer".to_owned(),
    }
}

fn message_page(
    cursor: &str,
    valid_data: bool,
) -> datalens_example_ormp_client::datalens::MessageAcceptedPage {
    let server = MockGraphqlServer::new(vec![rest_page(cursor, valid_data, false)]);
    let client = DatalensOrmpClient::new(sdk_client(&server));
    let config = test_config(&server, 10, Some(10), 1);
    client
        .fetch_message_accepted_page(&config, 10, 10)
        .expect("message page")
}

fn rest_page(cursor: &str, valid_data: bool, _has_next_page: bool) -> serde_json::Value {
    let block_number = cursor
        .split(':')
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(11);
    let message_hash = message_hash_for_cursor(cursor);
    json!({
        "chain": {"configuredName": "ethereum"},
        "dataset_key": "evm.logs",
        "range": {"kind": "block", "start": 10, "end": 10},
        "cache": {
            "hit_ranges": [],
            "missing_ranges": [],
            "durable_hit_ranges": [],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": {
            "dataset_key": "evm.logs",
            "rows": {
                "dataset": "logs",
                "rows": [{
                    "block_number": block_number,
                    "block_hash": "0xblock",
                    "transaction_hash": cursor.split(':').nth(1).unwrap_or("0xtx"),
                    "transaction_index": 0,
                    "log_index": 3,
                    "address": "0x13b2211a7ca45db2808f6db05557ce5347e3634e",
                    "topics": [MESSAGE_ACCEPTED_TOPIC0, message_hash],
                    "data": if valid_data { MESSAGE_ACCEPTED_DATA } else { "0x" },
                    "removed": false
                }]
            }
        }
    })
}

fn message_hash_for_cursor(cursor: &str) -> &'static str {
    if cursor.contains("two") {
        "0x70f5743a8b3bbe4e4bd99607b19985203a9310f4859e03912ed086f4d32bdff8"
    } else {
        MESSAGE_ACCEPTED_MSG_HASH
    }
}
