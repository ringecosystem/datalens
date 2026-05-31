use datalens_example_degov_client::{
    RunSummary,
    config::AppConfig,
    datalens::DatalensDegovClient,
    db::AppDatabase,
    handlers::vote_cast::{VoteCastHandler, handle_vote_cast_page},
};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

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
    let page = vote_page("vote-cursor-2", "42", Some(1), Some("7"));

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
        Some("vote-cursor-2".to_owned())
    );
}

#[test]
fn test_handle_vote_cast_page_is_idempotent_for_duplicate_events() {
    let db = migrated_db();
    let handler = VoteCastHandler::new("degov-vote-consumer");
    handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", "42", Some(1), Some("7")),
    )
    .expect("first page");

    let summary = handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", "42", Some(1), Some("7")),
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
            vote_edge("for-cursor", "42", Some(1), Some("7")),
            vote_edge("against-cursor", "42", Some(0), Some("3")),
            vote_edge("abstain-cursor", "42", Some(2), Some("2")),
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
    let summary = handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", "42", Some(1), None),
    )
    .expect("handle page");

    assert_eq!(summary.fetched_rows, 1);
    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.skipped_invalid, 1);
    assert_eq!(summary.updated_proposals, 0);
    assert_eq!(db.vote_count().expect("vote count"), 0);
    assert_eq!(
        db.checkpoint("degov-vote-consumer").expect("checkpoint"),
        Some("vote-cursor-2".to_owned())
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
                "vote-cursor-2",
                "{}",
            ),
        )?;
        Ok(())
    })
    .expect("seed conflicting vote");

    let err = handle_vote_cast_page(
        &db,
        &handler,
        vote_page("vote-cursor-2", "42", Some(1), Some("7")),
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
    db.insert_checkpoint("degov-vote-consumer", "vote-cursor-1")
        .expect("seed checkpoint");
    let server = MockGraphqlServer::new(vec![graphql_page(
        vec![vote_edge("vote-cursor-2", "42", Some(1), Some("7"))],
        false,
    )]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    let config = AppConfig {
        index_graphql_url: server.endpoint(),
        token: None,
        database_url: "sqlite::memory:".to_owned(),
        page_size: 2,
        start_cursor: None,
        consumer_name: "degov-vote-consumer".to_owned(),
    };

    let summary = datalens_example_degov_client::run_once(&config, &db, &client).expect("run once");

    assert_eq!(
        summary,
        RunSummary {
            fetched_rows: 1,
            inserted_rows: 1,
            skipped_duplicates: 0,
            skipped_invalid: 0,
            updated_proposals: 1,
            checkpoint_cursor: Some("vote-cursor-2".to_owned()),
            has_next_page: false,
        }
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].variables["after"], "vote-cursor-1");
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
        timeout: None,
        user_agent: Some("datalens-degov-client-example-tests".to_owned()),
    })
    .expect("client config")
}

fn vote_page(
    cursor: &str,
    proposal_id: &str,
    support: Option<u64>,
    weight: Option<&str>,
) -> datalens_example_degov_client::datalens::VoteCastPage {
    let server = MockGraphqlServer::new(vec![graphql_page(
        vec![vote_edge(cursor, proposal_id, support, weight)],
        false,
    )]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    client.fetch_vote_cast_page(None, 1).expect("vote page")
}

fn degov_page(
    edges: Vec<serde_json::Value>,
) -> datalens_example_degov_client::datalens::VoteCastPage {
    let server = MockGraphqlServer::new(vec![graphql_page(edges, false)]);
    let client = DatalensDegovClient::new(sdk_client(&server));
    client.fetch_vote_cast_page(None, 10).expect("vote page")
}

fn graphql_page(edges: Vec<serde_json::Value>, has_next_page: bool) -> serde_json::Value {
    let end_cursor = edges
        .last()
        .and_then(|edge| edge["cursor"].as_str())
        .unwrap_or_default();

    json!({
        "data": {
            "decodedEventsConnection": {
                "edges": edges,
                "nodes": [],
                "pageInfo": {
                    "endCursor": end_cursor,
                    "hasNextPage": has_next_page
                }
            }
        }
    })
}

fn vote_edge(
    cursor: &str,
    proposal_id: &str,
    support: Option<u64>,
    weight: Option<&str>,
) -> serde_json::Value {
    let transaction_hash = format!("0xtx-{cursor}");
    json!({
        "cursor": cursor,
        "node": {
            "indexName": "degov",
            "chain": "ethereum",
            "chainId": 1,
            "dataset": "evm.logs",
            "blockNumber": 100,
            "blockHash": "0xblock1",
            "transactionHash": transaction_hash,
            "transactionIndex": 0,
            "logIndex": 1,
            "address": "0xgovernor",
            "eventName": "VoteCast",
            "signature": "VoteCast(address,uint256,uint8,uint256,string)",
            "topic0": "0xtopic0",
            "decodedArgs": {
                "voter": "0xvoter",
                "proposalId": proposal_id,
                "support": support,
                "weight": weight,
                "reason": "because"
            },
            "decodeStatus": "decoded",
            "decodeError": null,
            "payload": {},
            "createdAt": "2026-05-31T00:00:00Z"
        }
    })
}
