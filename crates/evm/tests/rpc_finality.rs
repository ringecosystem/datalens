use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use datalens_chain::{
    CanonicalBlockRequest, ChainAdapter, ChainFetchRequest, ChainHeight, DatasetSelector,
    FinalityKind,
};
use datalens_core::NetworkId;
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange,
    LedgerRangeKind,
};
use serde_json::{Value, json};

use datalens_evm::*;

#[test]
fn test_classify_provider_error_maps_known_failure_modes() {
    assert_eq!(
        classify_provider_error(-32602, "invalid argument").kind,
        DatalensErrorKind::InvalidInput
    );
    assert_eq!(
        classify_provider_error(-32601, "method not found").kind,
        DatalensErrorKind::UnsupportedDataset
    );
    assert_eq!(
        classify_provider_error(-32000, "query returned more than 10000 results").kind,
        DatalensErrorKind::ProviderLimit
    );
    assert_eq!(
        classify_provider_error(-32000, "request timed out").kind,
        DatalensErrorKind::ProviderTimeout
    );
    assert_eq!(
        classify_provider_error(429, "too many requests").kind,
        DatalensErrorKind::RateLimited
    );
}

#[test]
fn test_parse_log_record_canonicalizes_provider_hex_values() {
    let record = parse_log_record(&json!({
        "blockNumber": "0xa",
        "blockHash": "0xblock",
        "transactionHash": "0xtx",
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "address": "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "topics": ["0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"],
        "data": "0x",
        "removed": false
    }))
    .expect("valid log");

    assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(
        record.topics,
        vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
    );
}

#[test]
fn test_parse_log_record_rejects_invalid_provider_hex_values() {
    let error = parse_log_record(&json!({
        "blockNumber": "0xa",
        "blockHash": "0xblock",
        "transactionHash": "0xtx",
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "address": "0xabc",
        "topics": [],
        "data": "0x",
        "removed": false
    }))
    .expect_err("invalid address");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
}

#[test]
fn test_fetch_rejects_chain_mismatch_before_provider_call() {
    let client = EvmRpcClient::with_chain(
        Vec::new(),
        ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
            .expect("valid chain"),
        EvmFinalityPolicy::Auto,
        10,
        10,
        10,
    );
    let request = ChainFetchRequest::new(
        ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
            .expect("valid chain"),
        DatasetKey::evm_blocks(),
        LedgerRange::from_block_range(BlockRange::expect_new(1, 1)),
        DatasetSelector::All,
    );

    let error = client.fetch(request).expect_err("chain mismatch");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_safe_height_prefers_rpc_finalized_tag() {
    let (url, requests) = start_rpc_server(vec![json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "number": "0x64"
        }
    })]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        10,
    );

    let height = client.cache_safe_height().expect("finalized height");

    assert_eq!(
        height,
        ChainHeight::block(100).with_finality(FinalityKind::Finalized)
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[0]["params"][0], "finalized");
}

#[test]
fn test_safe_height_uses_rpc_safe_when_finalized_tag_is_unsupported() {
    let (url, requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32602,
                "message": "unsupported block tag finalized"
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "number": "0x5a"
            }
        }),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        10,
    );

    let height = client.cache_safe_height().expect("safe height");

    assert_eq!(
        height,
        ChainHeight::block(90).with_finality(FinalityKind::Safe)
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["params"][0], "finalized");
    assert_eq!(requests[1]["params"][0], "safe");
}

#[test]
fn test_safe_height_uses_chain_profile_when_rpc_finality_tags_are_unsupported() {
    let (url, requests) = start_rpc_server(vec![
        unsupported_tag_response(),
        unsupported_tag_response(),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0xc8"
        }),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        10,
    );

    let height = client.cache_safe_height().expect("profile fallback height");

    assert_eq!(
        height,
        ChainHeight::block(72).with_finality(FinalityKind::Finalized)
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2]["method"], "eth_blockNumber");
}

#[test]
fn test_safe_height_rejects_unknown_chain_without_rpc_tags_or_override() {
    let (url, _requests) =
        start_rpc_server(vec![unsupported_tag_response(), unsupported_tag_response()]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ChainIdentity::try_new(
            ChainFamily::Evm,
            "unknown",
            Some(NetworkId::numeric(999999)),
        )
        .expect("valid chain"),
        EvmFinalityPolicy::Auto,
        10,
        10,
        10,
    );

    let error = client.cache_safe_height().expect_err("unknown finality");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("finality"));
}

#[test]
fn test_safe_height_uses_manual_lag_override_without_rpc_finality_tags() {
    let (url, requests) = start_rpc_server(vec![json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0xc8"
    })]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ChainIdentity::try_new(ChainFamily::Evm, "private", Some(NetworkId::numeric(31337)))
            .expect("valid chain"),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(64),
            finalized_lag_blocks: Some(128),
        },
        10,
        10,
        10,
    );

    let height = client.cache_safe_height().expect("manual lag height");

    assert_eq!(
        height,
        ChainHeight::block(72).with_finality(FinalityKind::Finalized)
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "eth_blockNumber");
}

#[test]
fn test_height_from_latest_lag_applies_lag_without_underflow() {
    assert_eq!(height_from_latest_lag(100, 12), 88);
    assert_eq!(height_from_latest_lag(10, 12), 0);
}

#[test]
fn test_canonical_block_fetches_provider_hash_at_height() {
    let (url, requests) = start_rpc_server(vec![json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "number": "0x0a",
            "hash": "0xblock",
            "parentHash": "0xparent",
            "timestamp": "0x01"
        }
    })]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        10,
    );

    let block = client
        .canonical_block(CanonicalBlockRequest {
            chain: ethereum_identity(),
            range_kind: LedgerRangeKind::Block,
            height: 10,
        })
        .expect("canonical block");

    assert_eq!(block.chain, ethereum_identity());
    assert_eq!(block.height, 10);
    assert_eq!(block.hash, "0xblock");
    assert_eq!(block.parent_hash, "0xparent");
    assert_eq!(block.finality, FinalityKind::Latest);
    let requests = requests.lock().expect("requests");
    assert_eq!(requests[0]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[0]["params"][0], "0xa");
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn unsupported_tag_response() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": -32602,
            "message": "unsupported block tag"
        }
    })
}

fn start_rpc_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);
    let responses = Arc::new(Mutex::new(responses));

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("test server connection");
            let mut buffer = [0; 8192];
            let bytes = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..bytes]);
            let body = request.split("\r\n\r\n").nth(1).expect("request body");
            let request_json: Value = serde_json::from_str(body).expect("request JSON");
            request_log.lock().expect("request log").push(request_json);
            let response = responses.lock().expect("responses").remove(0);
            let response = response.to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                response.len(),
                response
            )
            .expect("write response");
        }
    });

    (format!("http://{address}"), requests)
}
