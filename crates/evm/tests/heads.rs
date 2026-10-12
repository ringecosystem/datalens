use datalens_core::{B256, Heads};
use datalens_evm::HeadMode;
use datalens_provider::{ProviderError, Purpose, TokenWait};
use serde_json::json;

mod common;

use common::client;
use common::mock::{ItemOutcome, MockRpc};

const UNTIL: TokenWait = TokenWait::UntilToken;

fn header(number: u64) -> serde_json::Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": format!("0x{number:064x}"),
        "parentHash": format!("0x{:064x}", number.saturating_sub(1)),
        "timestamp": "0x65",
    })
}

fn hash_of(number: u64) -> B256 {
    format!("0x{number:064x}").parse().unwrap()
}

#[tokio::test]
async fn lag_mode_reads_latest_once() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| ItemOutcome::Result(header(1000)));
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let heads = evm
        .heads(
            HeadMode::Lag {
                safe_lag_blocks: 64,
                finalized_lag_blocks: 256,
            },
            Purpose::Head,
            None,
            UNTIL,
        )
        .await
        .unwrap();
    assert_eq!(
        heads,
        Heads {
            latest: 1000,
            latest_hash: hash_of(1000),
            safe: 936,
            finalized: 744,
        }
    );
    assert_eq!(mock.calls().len(), 1);
    assert_eq!(mock.calls()[0]["method"], "eth_getBlockByNumber");
    assert_eq!(mock.calls()[0]["params"], json!(["latest", false]));
}

#[tokio::test]
async fn tag_mode_is_one_batch_of_three() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let number = match item["params"][0].as_str() {
            Some("latest") => 1000,
            Some("safe") => 900,
            Some("finalized") => 800,
            _ => 0,
        };
        ItemOutcome::Result(header(number))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let heads = evm
        .heads(HeadMode::Tags, Purpose::Head, None, UNTIL)
        .await
        .unwrap();
    assert_eq!(
        heads,
        Heads {
            latest: 1000,
            latest_hash: hash_of(1000),
            safe: 900,
            finalized: 800,
        }
    );
    let calls = mock.calls();
    assert_eq!(calls.len(), 1);
    let batch = calls[0].as_array().unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0]["method"], "eth_getBlockByNumber");
    assert_eq!(batch[0]["params"], json!(["latest", false]));
    assert_eq!(batch[1]["params"], json!(["safe", false]));
    assert_eq!(batch[2]["params"], json!(["finalized", false]));
}

#[tokio::test]
async fn tag_mode_rejects_heads_that_are_out_of_order() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let number = match item["params"][0].as_str() {
            Some("latest") => 1000,
            Some("safe") => 1100,
            Some("finalized") => 800,
            _ => 0,
        };
        ItemOutcome::Result(header(number))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .heads(HeadMode::Tags, Purpose::Head, None, UNTIL)
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::fatal("head tags are out of order"));
}

#[tokio::test]
async fn a_missing_safe_tag_names_lag_mode() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| match item["params"][0].as_str() {
        Some("safe") => ItemOutcome::Error {
            code: -32_000,
            message: "unknown block".to_owned(),
        },
        Some("finalized") => ItemOutcome::Result(header(800)),
        _ => ItemOutcome::Result(header(1000)),
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .heads(HeadMode::Tags, Purpose::Head, None, UNTIL)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ProviderError::fatal("chain does not support safe tag; use lag mode")
    );
}
