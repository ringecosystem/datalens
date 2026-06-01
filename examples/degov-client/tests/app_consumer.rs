use datalens_example_degov_client::{
    RunSummary,
    config::{AppConfig, DEFAULT_EVENT_TOPIC0},
    datalens::DatalensDegovClient,
    db::AppDatabase,
    handlers::vote_cast::{VoteCastHandler, handle_vote_cast_page},
};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

const VOTER_TOPIC: &str = "0x0000000000000000000000001111111111111111111111111111111111111111";
const VOTE_CAST_FOR_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002a0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000700000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000007626563617573650000000000000000000000000000000000000000000000000000";
const VOTE_CAST_AGAINST_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000007626563617573650000000000000000000000000000000000000000000000000000";
const VOTE_CAST_ABSTAIN_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002a0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000007626563617573650000000000000000000000000000000000000000000000000000";

#[test]
fn test_migrations_create_application_tables_and_indexes() {
    let db = AppDatabase::open("sqlite::memory:").expect("open database");
    db.migrate().expect("run migrations");

    assert!(
        db.table_exists("consumer_checkpoints")
            .expect("checkpoint table")
    );
    assert!(db.table_exists("degov_votes").expect("votes table"));
    assert!(db.table_exists("degov_proposals").expect("proposals table"));
    assert!(
        db.index_exists("idx_degov_votes_event_cursor")
            .expect("event cursor index")
    );
    assert!(
        db.index_exists("idx_degov_votes_proposal_id")
            .expect("proposal id index")
    );
}

#[test]
fn test_handle_vote_cast_page_writes_vote_projection_and_checkpoint() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    let page = vote_page("vote-cursor-2", VOTE_CAST_FOR_DATA);

    let summary = handle_vote_cast_page(&db, &handler, page).expect("handle page");

    assert_eq!(summary.fetched_rows, 1);
    assert_eq!(summary.inserted_rows, 1);
    assert_eq!(summary.skipped_duplicates, 0);
    assert_eq!(summary.skipped_invalid, 0);
    assert_eq!(summary.updated_proposals, 1);
    assert_eq!(db.vote_count().expect("vote count"), 1);
    assert_eq!(
        db.proposal_totals("42").expect("proposal totals"),
        Some((7, 0, 0))
    );
    assert_eq!(
        db.checkpoint("degov-vote-consumer").expect("checkpoint"),
        Some("101".to_owned())
    );
    let row = db
        .transaction(|tx| {
            Ok(tx.query_row(
                "SELECT proposal_id, voter, support, weight, reason, raw_event_json FROM degov_votes",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )?)
        })
        .expect("vote row");
    assert_eq!(row.0, "42");
    assert_eq!(row.1, "0x1111111111111111111111111111111111111111");
    assert_eq!(row.2, 1);
    assert_eq!(row.3, 7);
    assert_eq!(row.4, "because");
    let raw_event: serde_json::Value = serde_json::from_str(&row.5).expect("raw event json");
    assert_eq!(raw_event["decodeStatus"], "decoded");
    assert_eq!(raw_event["payload"]["data"], VOTE_CAST_FOR_DATA);
}

#[test]
fn test_handle_vote_cast_page_is_idempotent_for_duplicate_events() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", VOTE_CAST_FOR_DATA),
    )
    .expect("first page");

    let summary = handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", VOTE_CAST_FOR_DATA),
    )
    .expect("duplicate page");

    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.skipped_duplicates, 1);
    assert_eq!(summary.updated_proposals, 0);
    assert_eq!(db.vote_count().expect("vote count"), 1);
    assert_eq!(
        db.proposal_totals("42").expect("proposal totals"),
        Some((7, 0, 0))
    );
}

#[test]
fn test_handle_vote_cast_page_updates_support_buckets() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    handle_vote_cast_page(
        &db,
        &handler,
        degov_page(vec![
            vote_edge("for-cursor", VOTE_CAST_FOR_DATA),
            vote_edge("against-cursor", VOTE_CAST_AGAINST_DATA),
            vote_edge("abstain-cursor", VOTE_CAST_ABSTAIN_DATA),
        ]),
    )
    .expect("handle page");

    assert_eq!(
        db.proposal_totals("42").expect("proposal totals"),
        Some((7, 3, 2))
    );
}

#[test]
fn test_handle_vote_cast_page_skips_missing_required_fields() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    let summary = handle_vote_cast_page(&db, &handler, vote_page("vote-cursor-2", "0x1234"))
        .expect("handle page");

    assert_eq!(summary.fetched_rows, 1);
    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.skipped_invalid, 1);
    assert_eq!(summary.updated_proposals, 0);
    assert_eq!(db.vote_count().expect("vote count"), 0);
    assert_eq!(
        db.checkpoint("degov-vote-consumer").expect("checkpoint"),
        Some("101".to_owned())
    );
}

#[test]
fn test_checkpoint_does_not_advance_when_projection_write_fails() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    db.insert_checkpoint("degov-vote-consumer", "vote-cursor-1")
        .expect("seed checkpoint");
    db.transaction(|tx| {
        tx.execute(
            "INSERT INTO degov_votes (
                vote_key,
                proposal_id,
                support,
                weight,
                event_cursor,
                raw_event_json,
                created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)",
            (
                "vote-cursor-2",
                "different-proposal",
                1_i64,
                7_i64,
                "100:0xtx-vote-cursor-2:1",
                "{}",
            ),
        )?;
        Ok(())
    })
    .expect("seed conflicting vote");

    let err = handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", VOTE_CAST_FOR_DATA),
    )
    .expect_err("cursor conflict should fail");

    assert!(err.to_string().contains("event cursor already belongs"));
    assert_eq!(
        db.checkpoint("degov-vote-consumer").expect("checkpoint"),
        Some("vote-cursor-1".to_owned())
    );
}

#[test]
fn test_run_once_resumes_with_stored_checkpoint_cursor() {
    let db = migrated_db();
    db.insert_checkpoint("degov-vote-consumer", "101")
        .expect("seed checkpoint");
    let server = MockGraphqlServer::new(vec![graphql_page(
        vec![vote_edge("101:0xtx-vote-cursor-2:1", VOTE_CAST_FOR_DATA)],
        false,
    )]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    let config = test_config(&server, 100, Some(102), 2);

    let summary = datalens_example_degov_client::run_once(&config, &db, &client).expect("run once");

    assert_eq!(
        summary,
        RunSummary {
            fetched_rows: 1,
            inserted_rows: 1,
            skipped_duplicates: 0,
            skipped_invalid: 0,
            updated_proposals: 1,
            checkpoint_cursor: Some("103".to_owned()),
            has_next_page: false,
        }
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].variables["input"]["range"]["start"], 101);
    assert_eq!(requests[0].variables["input"]["range"]["end"], 102);
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
        application: Some("degov-client-test".to_owned()),
        timeout: None,
        user_agent: Some("datalens-degov-client-example-tests".to_owned()),
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
        application: "degov-client-test".to_owned(),
        database_url: "sqlite::memory:".to_owned(),
        chain_name: "ethereum".to_owned(),
        chain_id: 1,
        dataset_family: "evm".to_owned(),
        dataset_name: "logs".to_owned(),
        contract_address: "0xgovernor".to_owned(),
        event_topic0: DEFAULT_EVENT_TOPIC0.to_owned(),
        event_signature: datalens_example_degov_client::datalens::VOTE_CAST_SIGNATURE.to_owned(),
        start_block,
        end_block,
        chunk_size,
        reset_checkpoint: false,
        consumer_name: "degov-vote-consumer".to_owned(),
    }
}

fn vote_page(cursor: &str, data: &str) -> datalens_example_degov_client::datalens::VoteCastPage {
    let server = MockGraphqlServer::new(vec![graphql_page(vec![vote_edge(cursor, data)], false)]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    let config = test_config(&server, 100, Some(100), 1);
    client
        .fetch_vote_cast_page(&config, 100, 100)
        .expect("vote page")
}

fn degov_page(
    edges: Vec<serde_json::Value>,
) -> datalens_example_degov_client::datalens::VoteCastPage {
    let server = MockGraphqlServer::new(vec![graphql_page(edges, false)]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    let config = test_config(&server, 100, Some(100), 10);
    client
        .fetch_vote_cast_page(&config, 100, 100)
        .expect("vote page")
}

fn graphql_page(edges: Vec<serde_json::Value>, _has_next_page: bool) -> serde_json::Value {
    json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 100, "end": 100},
                "cache": {"hitRanges": [], "missingRanges": []},
                "rows": {
                    "dataset_key": "evm.logs",
                    "rows": {
                        "dataset": "logs",
                        "rows": edges
                    }
                }
            }
        }
    })
}

fn vote_edge(cursor: &str, data: &str) -> serde_json::Value {
    let transaction_hash = cursor
        .split(':')
        .nth(1)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("0xtx-{cursor}"));
    json!({
        "block_number": 100,
        "block_hash": "0xblock1",
        "transaction_hash": transaction_hash,
        "transaction_index": 0,
        "log_index": 1,
        "address": "0xgovernor",
        "topics": [DEFAULT_EVENT_TOPIC0, VOTER_TOPIC],
        "data": data,
        "removed": false
    })
}
