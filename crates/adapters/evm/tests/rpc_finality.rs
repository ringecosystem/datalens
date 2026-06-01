use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    thread,
};

use datalens_chain::{
    CanonicalBlockRequest, ChainAdapter, ChainFetchRequest, ChainHeight, DatasetSelector,
    FinalityKind,
};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange,
    LedgerRangeKind, LogFilter, NetworkId, QueryRows, QueryStrategy,
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
fn test_transport_error_redacts_rpc_url_credentials() {
    let address = unused_local_address();
    let url = format!(
        "http://user:password@{address}/rpc/path?token=query-token&secret=query-secret&batch=evm"
    );
    let client = EvmRpcClient::new(vec![url]);

    let error = client.latest_height().expect_err("connect failure");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("http://<redacted>@"));
    assert!(error.message.contains("token=<redacted>"));
    assert!(error.message.contains("secret=<redacted>"));
    assert!(!error.message.contains("password"));
    assert!(!error.message.contains("query-token"));
    assert!(!error.message.contains("query-secret"));
}

#[test]
fn test_provider_error_redacts_rpc_url_credentials_in_message() {
    let error = classify_provider_error(
        -32000,
        "upstream failed at https://user:password@rpc.example.invalid/path?api_key=query-key&signature=query-signature",
    );

    assert!(
        error
            .message
            .contains("https://<redacted>@rpc.example.invalid/path")
    );
    assert!(error.message.contains("api_key=<redacted>"));
    assert!(error.message.contains("signature=<redacted>"));
    assert!(!error.message.contains("password"));
    assert!(!error.message.contains("query-key"));
    assert!(!error.message.contains("query-signature"));
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
fn test_parse_transaction_and_receipt_capture_durable_index_fields() {
    let transaction = parse_transaction(
        &json!({
            "hash": "0xtx",
            "blockNumber": "0xa",
            "blockHash": "0xblock",
            "transactionIndex": "0x1",
            "from": "0x1111111111111111111111111111111111111111",
            "to": null,
            "value": "0x2",
            "input": "0x1234",
            "nonce": "0x3",
            "gas": "0x5208",
            "gasPrice": "0x3b9aca00",
            "maxFeePerGas": "0x77359400",
            "maxPriorityFeePerGas": "0x59682f00",
            "type": "0x2"
        }),
        10,
        "0xblock",
    )
    .expect("transaction");

    assert_eq!(transaction.block_number, 10);
    assert_eq!(transaction.transaction_index, 1);
    assert_eq!(transaction.to, None);
    assert_eq!(transaction.gas, 21_000);

    let receipt = parse_receipt(&json!({
        "transactionHash": "0xtx",
        "blockNumber": "0xa",
        "blockHash": "0xblock",
        "transactionIndex": "0x1",
        "status": "0x1",
        "gasUsed": "0x5208",
        "cumulativeGasUsed": "0x5208",
        "effectiveGasPrice": "0x3b9aca00",
        "contractAddress": null,
        "logsBloom": "0x00"
    }))
    .expect("receipt");

    assert_eq!(receipt.transaction_hash, "0xtx");
    assert_eq!(receipt.status, Some(1));
    assert_eq!(receipt.contract_address, None);
    assert_eq!(receipt.gas_used, 21_000);
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
fn test_provider_filter_logs_use_eth_get_logs() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (url, requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [provider_log(10, "0xaaa", topic)]
        }),
        block_response(10, "0xaaa", "0xparent", 1),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        3,
        10,
    )
    .with_logs_query_strategy(QueryStrategy::ProviderFilter);
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![address.to_owned()],
        topics: vec![Some(vec![topic.to_owned()])],
    })
    .expect("filter");

    let response = client
        .fetch(ChainFetchRequest::new(
            ethereum_identity(),
            DatasetKey::evm_logs(),
            LedgerRange::from_block_range(BlockRange::expect_new(10, 10)),
            DatasetSelector::EvmLogs(filter),
        ))
        .expect("logs");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].address, address);
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent"));
    assert_eq!(rows[0].block_timestamp, Some(1));
    assert_eq!(response.provider_diagnostics.calls, 2);
    assert!(response.provider_diagnostics.warnings.is_empty());
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_getLogs");
    assert_eq!(requests[1]["method"], "eth_getBlockByNumber");
}

#[test]
fn test_block_range_logs_fetch_receipts_and_filter_locally_without_eth_get_logs() {
    let topic = transfer_topic();
    let matching_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let other_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let (url, requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": full_block(10, "0xaaa", ["0xtx0", "0xtx1"])
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": receipt(10, "0xaaa", "0xtx0", 0, vec![
                provider_log_with_address(10, "0xaaa", "0xtx0", 0, 0, matching_address, topic),
                provider_log_with_address(10, "0xaaa", "0xtx0", 0, 1, other_address, topic),
            ])
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": receipt(10, "0xaaa", "0xtx1", 1, vec![
                provider_log_with_address(10, "0xaaa", "0xtx1", 1, 0, matching_address, unrelated_topic()),
            ])
        }),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        3,
        10,
    )
    .with_logs_query_strategy(QueryStrategy::BlockRange);
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![matching_address.to_owned()],
        topics: vec![Some(vec![topic.to_owned()])],
    })
    .expect("filter");

    let response = client
        .fetch(ChainFetchRequest::new(
            ethereum_identity(),
            DatasetKey::evm_logs(),
            LedgerRange::from_block_range(BlockRange::expect_new(10, 10)),
            DatasetSelector::EvmLogs(filter),
        ))
        .expect("logs");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].transaction_hash, "0xtx0");
    assert_eq!(rows[0].log_index, 0);
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent"));
    assert_eq!(rows[0].block_timestamp, Some(1));
    assert_eq!(response.provider_diagnostics.calls, 3);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"evm block_range log query strategy used".to_owned())
    );
    let methods = requests
        .lock()
        .expect("requests")
        .iter()
        .map(|request| request["method"].as_str().expect("method").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        methods,
        vec![
            "eth_getBlockByNumber".to_owned(),
            "eth_getTransactionReceipt".to_owned(),
            "eth_getTransactionReceipt".to_owned()
        ]
    );
}

#[test]
fn test_block_range_logs_enforce_independent_block_scan_limit() {
    let client = EvmRpcClient::with_chain(
        Vec::new(),
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10_000,
        1,
        10,
    )
    .with_logs_query_strategy(QueryStrategy::BlockRange);
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: Vec::new(),
        topics: Vec::new(),
    })
    .expect("filter");

    let error = client
        .fetch(ChainFetchRequest::new(
            ethereum_identity(),
            DatasetKey::evm_logs(),
            LedgerRange::from_block_range(BlockRange::expect_new(10, 11)),
            DatasetSelector::EvmLogs(filter),
        ))
        .expect_err("range limit");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(error.message.contains("block scan"));
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

#[test]
fn test_fetch_evm_logs_enriches_block_metadata_once_per_block() {
    let block_hash = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let parent_hash = "0x0000000000000000000000000000000000000000000000000000000000000009";
    let topic = transfer_topic();
    let (url, requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                provider_log_with_index(10, block_hash, "0xtx0", 0, topic),
                provider_log_with_index(10, block_hash, "0xtx1", 1, topic)
            ]
        }),
        block_response(10, block_hash, parent_hash, 1_700_000_000),
    ]);
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
        10,
    );

    let logs = client
        .fetch_evm_logs(
            BlockRange::expect_new(10, 10),
            &datalens_core::EvmLogFilter::try_from(&datalens_core::LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .expect("filter"),
        )
        .expect("logs");

    assert_eq!(logs.len(), 2);
    assert!(
        logs.iter()
            .all(|log| log.parent_hash.as_deref() == Some(parent_hash))
    );
    assert!(
        logs.iter()
            .all(|log| log.block_timestamp == Some(1_700_000_000))
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_getLogs");
    assert_eq!(requests[1]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[1]["params"][0], "0xa");
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn unused_local_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused address");
    listener.local_addr().expect("unused address")
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

fn transfer_topic() -> &'static str {
    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
}

fn unrelated_topic() -> &'static str {
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
}

fn full_block<const N: usize>(number: u64, hash: &str, hashes: [&str; N]) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": hash,
        "parentHash": "0xparent",
        "timestamp": "0x1",
        "transactions": hashes
            .into_iter()
            .enumerate()
            .map(|(index, hash)| json!({
                "hash": hash,
                "blockNumber": format!("0x{number:x}"),
                "blockHash": hash,
                "transactionIndex": format!("0x{index:x}"),
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "value": "0x0",
                "input": "0x",
                "nonce": "0x0",
                "gas": "0x5208"
            }))
            .collect::<Vec<_>>()
    })
}

fn receipt(
    number: u64,
    block_hash: &str,
    tx_hash: &str,
    transaction_index: u64,
    logs: Vec<Value>,
) -> Value {
    json!({
        "transactionHash": tx_hash,
        "blockNumber": format!("0x{number:x}"),
        "blockHash": block_hash,
        "transactionIndex": format!("0x{transaction_index:x}"),
        "status": "0x1",
        "gasUsed": "0x5208",
        "cumulativeGasUsed": "0x5208",
        "logs": logs,
    })
}

fn block_response(number: u64, hash: &str, parent_hash: &str, timestamp: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": format!("0x{timestamp:x}")
        }
    })
}

fn provider_log(number: u64, block_hash: &str, topic: &str) -> Value {
    provider_log_with_address(
        number,
        block_hash,
        "0xtx",
        0,
        0,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        topic,
    )
}

fn provider_log_with_index(
    number: u64,
    block_hash: &str,
    tx_hash: &str,
    log_index: u64,
    topic: &str,
) -> Value {
    provider_log_with_address(
        number,
        block_hash,
        tx_hash,
        0,
        log_index,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        topic,
    )
}

fn provider_log_with_address(
    number: u64,
    block_hash: &str,
    tx_hash: &str,
    transaction_index: u64,
    log_index: u64,
    address: &str,
    topic: &str,
) -> Value {
    json!({
        "blockNumber": format!("0x{number:x}"),
        "blockHash": block_hash,
        "transactionHash": tx_hash,
        "transactionIndex": format!("0x{transaction_index:x}"),
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [topic],
        "data": "0x",
        "removed": false
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
