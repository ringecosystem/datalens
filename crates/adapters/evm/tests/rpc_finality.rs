use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
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
    .with_logs_query_strategy(QueryStrategy::ProviderFilter)
    .with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );
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
    assert_eq!(response.provider_diagnostics.calls, 1);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_provider_calls=0"))
    );
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_header_blocks=1"))
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_getLogs");
    assert_eq!(requests[1]["method"], "eth_getBlockByNumber");
}

#[test]
fn test_provider_filter_logs_do_not_use_secondary_rpc_provider() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (primary_url, primary_requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [provider_log(10, "0xaaa", topic)]
        }),
        block_response(10, "0xaaa", "0xparent", 1),
    ]);
    let (secondary_url, secondary_requests) = start_rpc_server(Vec::new());
    let expected_primary_url = primary_url.clone();
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        3,
        10,
    )
    .with_logs_query_strategy(QueryStrategy::ProviderFilter)
    .with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );
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

    assert_eq!(
        client.primary_provider_url(),
        Some(expected_primary_url.as_str())
    );
    assert_eq!(client.secondary_provider_urls().len(), 1);
    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(primary_requests.lock().expect("primary requests").len(), 2);
    assert!(
        secondary_requests
            .lock()
            .expect("secondary requests")
            .is_empty()
    );
}

#[test]
fn test_provider_filter_logs_reliability_recovers_primary_empty_bloom_candidate_from_secondary() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
    ]);
    let (secondary_url, secondary_requests) =
        start_rpc_server(vec![logs_response(vec![provider_log_with_address(
            10,
            &block_hash,
            "0xtx",
            0,
            7,
            address,
            topic,
        )])]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
    assert_eq!(rows[0].topics, vec![topic.to_owned()]);
    assert_eq!(rows[0].log_index, 7);
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[0].block_timestamp, Some(10));
    assert_eq!(response.provider_diagnostics.calls, 2);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_suspicious_blocks=1"))
    );
    assert_eq!(primary_requests.lock().expect("primary requests").len(), 2);
    let secondary_requests = secondary_requests.lock().expect("secondary requests");
    assert_eq!(secondary_requests.len(), 1);
    assert_eq!(secondary_requests[0]["method"], "eth_getLogs");
    assert_eq!(secondary_requests[0]["params"][0]["fromBlock"], "0xa");
    assert_eq!(secondary_requests[0]["params"][0]["toBlock"], "0xa");
}

#[test]
fn test_provider_filter_logs_reliability_merges_dedupes_and_sorts_secondary_rows() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let hash10 = block_hash(10);
    let hash11 = block_hash(11);
    let hash12 = block_hash(12);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let primary_10 = provider_log_with_address(10, &hash10, "0xtx10", 0, 0, address, topic);
    let primary_12 = provider_log_with_address(12, &hash12, "0xtx12", 0, 0, address, topic);
    let secondary_11 = provider_log_with_address(11, &hash11, "0xtx11", 0, 1, address, topic);
    let (primary_url, _primary_requests) = start_rpc_server(vec![
        logs_response(vec![primary_12.clone(), primary_10.clone()]),
        json!([
            block_batch_response_with_bloom(1, 10, &hash10, "0xparent10", 10, &bloom),
            block_batch_response_with_bloom(2, 11, &hash11, "0xparent11", 11, &bloom),
            block_batch_response_with_bloom(3, 12, &hash12, "0xparent12", 12, &bloom),
        ]),
    ]);
    let (secondary_url, secondary_requests) = start_rpc_server(vec![logs_response(vec![
        secondary_11.clone(),
        primary_12.clone(),
    ])]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![address.to_owned()],
        topics: vec![Some(vec![topic.to_owned()])],
    })
    .expect("filter");

    let response = client
        .fetch(ChainFetchRequest::new(
            ethereum_identity(),
            DatasetKey::evm_logs(),
            LedgerRange::from_block_range(BlockRange::expect_new(10, 12)),
            DatasetSelector::EvmLogs(filter),
        ))
        .expect("logs");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(
        rows.iter()
            .map(|log| (log.block_number, log.log_index))
            .collect::<Vec<_>>(),
        vec![(10, 0), (11, 1), (12, 0)]
    );
    assert_eq!(
        rows.iter()
            .map(|log| log.parent_hash.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("0xparent10"), Some("0xparent11"), Some("0xparent12")]
    );
    assert_eq!(response.provider_diagnostics.calls, 2);
    let secondary_requests = secondary_requests.lock().expect("secondary requests");
    assert_eq!(secondary_requests.len(), 1);
    assert_eq!(secondary_requests[0]["params"][0]["fromBlock"], "0xb");
    assert_eq!(secondary_requests[0]["params"][0]["toBlock"], "0xb");
}

#[test]
fn test_provider_filter_logs_reliability_disabled_preserves_primary_only_empty_coverage() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let (primary_url, primary_requests) = start_rpc_server(vec![logs_response(Vec::new())]);
    let (secondary_url, secondary_requests) =
        start_rpc_server(vec![logs_response(vec![provider_log_with_address(
            10,
            &block_hash,
            "0xtx",
            0,
            0,
            address,
            topic,
        )])]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    )
    .with_log_reliability_config(EvmLogReliabilityConfig::default().with_enabled(false));
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
    assert!(rows.is_empty());
    assert_eq!(response.provider_diagnostics.calls, 1);
    assert_eq!(primary_requests.lock().expect("primary requests").len(), 1);
    assert!(
        secondary_requests
            .lock()
            .expect("secondary requests")
            .is_empty()
    );
}

#[test]
fn test_provider_filter_logs_reliability_recovers_no_secondary_bloom_candidate_from_block_receipts()
{
    let vote_cast = topic_with_prefix("bb");
    let transfer = transfer_topic();
    let governor = "0x1111111111111111111111111111111111111111";
    let token = "0x2222222222222222222222222222222222222222";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(governor),
        EvmLogBloomInput::Address(token),
        EvmLogBloomInput::Topic(&vote_cast),
        EvmLogBloomInput::Topic(transfer),
    ])
    .expect("bloom")
    .as_hex();
    let vote_log = provider_log_with_address(10, &block_hash, "0xtx0", 0, 0, governor, &vote_cast);
    let transfer_log = provider_log_with_address(10, &block_hash, "0xtx1", 1, 1, token, transfer);
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        block_receipts_response(vec![
            receipt(10, &block_hash, "0xtx0", 0, vec![vote_log]),
            receipt(10, &block_hash, "0xtx1", 1, vec![transfer_log]),
        ]),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![governor.to_owned(), token.to_owned()],
        topics: vec![Some(vec![vote_cast.to_owned(), transfer.to_owned()])],
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
    assert_eq!(
        rows.iter()
            .map(|log| (log.address.as_str(), log.topics[0].as_str()))
            .collect::<Vec<_>>(),
        vec![(governor, vote_cast.as_str()), (token, transfer)]
    );
    assert!(
        rows.iter()
            .all(|log| log.parent_hash.as_deref() == Some("0xparent10"))
    );
    assert!(rows.iter().all(|log| log.block_timestamp == Some(10)));
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_receipt_fallback_calls=1"))
    );
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_receipt_recovered_blocks=1"))
    );
    let primary_requests = primary_requests.lock().expect("primary requests");
    assert_eq!(primary_requests.len(), 3);
    assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
    assert_eq!(primary_requests[2]["params"][0], "0xa");
}

#[test]
fn test_provider_filter_logs_reliability_recovers_secondary_empty_bloom_candidate_from_block_receipts()
 {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let receipt_log = provider_log_with_address(10, &block_hash, "0xtx", 0, 0, address, topic);
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        block_receipts_response(vec![receipt(10, &block_hash, "0xtx", 0, vec![receipt_log])]),
    ]);
    let (secondary_url, secondary_requests) = start_rpc_server(vec![logs_response(Vec::new())]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[0].block_timestamp, Some(10));
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_receipt_recovered_blocks=1"))
    );
    assert_eq!(
        secondary_requests.lock().expect("secondary requests").len(),
        1
    );
    let primary_requests = primary_requests.lock().expect("primary requests");
    assert_eq!(primary_requests.len(), 3);
    assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
}

#[test]
fn test_provider_filter_logs_reliability_falls_back_to_receipts_when_secondary_fails() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let receipt_log = provider_log_with_address(10, &block_hash, "0xtx", 0, 0, address, topic);
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        block_receipts_response(vec![receipt(10, &block_hash, "0xtx", 0, vec![receipt_log])]),
    ]);
    let (secondary_url, secondary_requests) = start_rpc_server(vec![provider_error_response(
        -32000,
        "secondary unavailable",
    )]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
        .expect("receipt fallback should recover after secondary failure");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].address, address);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_receipt_recovered_blocks=1"))
    );
    assert_eq!(
        secondary_requests.lock().expect("secondary requests").len(),
        1
    );
    let primary_requests = primary_requests.lock().expect("primary requests");
    assert_eq!(primary_requests.len(), 3);
    assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
}

#[test]
fn test_provider_filter_logs_reliability_falls_back_to_transaction_receipts_when_block_receipts_unsupported()
 {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let receipt_log = provider_log_with_address(10, &block_hash, "0xtx", 0, 0, address, topic);
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        unsupported_method_response("method not found"),
        block_with_transaction_hashes_response(10, &block_hash, "0xparent10", 10, vec!["0xtx"]),
        receipt_response(receipt(10, &block_hash, "0xtx", 0, vec![receipt_log])),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
    assert_eq!(rows[0].transaction_hash, "0xtx");
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("reliability_receipt_fallback_calls=3"))
    );
    let primary_requests = primary_requests.lock().expect("primary requests");
    assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
    assert_eq!(primary_requests[3]["method"], "eth_getBlockByNumber");
    assert_eq!(primary_requests[3]["params"][1], false);
    assert_eq!(primary_requests[4]["method"], "eth_getTransactionReceipt");
}

#[test]
fn test_provider_filter_logs_reliability_block_receipts_malformed_result_errors() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let (primary_url, primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "transactionHash": "0xtx"
            }
        }),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![address.to_owned()],
        topics: vec![Some(vec![topic.to_owned()])],
    })
    .expect("filter");

    let error = client
        .fetch(ChainFetchRequest::new(
            ethereum_identity(),
            DatasetKey::evm_logs(),
            LedgerRange::from_block_range(BlockRange::expect_new(10, 10)),
            DatasetSelector::EvmLogs(filter),
        ))
        .expect_err("malformed block receipts should fail");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(
        error
            .message
            .contains("invalid eth_getBlockReceipts result")
    );
    let primary_requests = primary_requests.lock().expect("primary requests");
    assert_eq!(primary_requests.len(), 3);
    assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
}

#[test]
fn test_provider_filter_logs_reliability_block_receipts_provider_errors_do_not_fallback_to_transaction_receipts()
 {
    for (response, expected_kind) in [
        (
            provider_error_response(429, "too many requests"),
            DatalensErrorKind::RateLimited,
        ),
        (
            provider_error_response(-32000, "query returned more than 10000 results"),
            DatalensErrorKind::ProviderLimit,
        ),
        (
            provider_error_response(-32000, "request timed out"),
            DatalensErrorKind::ProviderTimeout,
        ),
        (
            provider_error_response(-32000, "upstream failed"),
            DatalensErrorKind::ProviderFailure,
        ),
    ] {
        let topic = transfer_topic();
        let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash = block_hash(10);
        let bloom = EvmLogBloom::from_inputs([
            EvmLogBloomInput::Address(address),
            EvmLogBloomInput::Topic(topic),
        ])
        .expect("bloom")
        .as_hex();
        let (primary_url, primary_requests) = start_rpc_server(vec![
            logs_response(Vec::new()),
            json!([block_batch_response_with_bloom(
                1,
                10,
                &block_hash,
                "0xparent10",
                10,
                &bloom
            )]),
            response,
        ]);
        let client = EvmRpcClient::with_chain(
            vec![primary_url],
            ethereum_identity(),
            EvmFinalityPolicy::Lag {
                safe_lag_blocks: Some(2),
                finalized_lag_blocks: Some(4),
            },
            10,
            10,
            3,
            10,
        );
        let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
            addresses: vec![address.to_owned()],
            topics: vec![Some(vec![topic.to_owned()])],
        })
        .expect("filter");

        let error = client
            .fetch(ChainFetchRequest::new(
                ethereum_identity(),
                DatasetKey::evm_logs(),
                LedgerRange::from_block_range(BlockRange::expect_new(10, 10)),
                DatasetSelector::EvmLogs(filter),
            ))
            .expect_err("provider failure should not fallback");

        assert_eq!(error.kind, expected_kind);
        let primary_requests = primary_requests.lock().expect("primary requests");
        assert_eq!(primary_requests.len(), 3);
        assert_eq!(primary_requests[2]["method"], "eth_getBlockReceipts");
    }
}

#[test]
fn test_provider_filter_logs_reliability_receipt_fallback_applies_original_filter() {
    let topic = transfer_topic();
    let unrelated = unrelated_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let unrelated_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let matched_log = provider_log_with_address(10, &block_hash, "0xtx0", 0, 0, address, topic);
    let unrelated_log =
        provider_log_with_address(10, &block_hash, "0xtx1", 1, 1, unrelated_address, unrelated);
    let (primary_url, _primary_requests) = start_rpc_server(vec![
        logs_response(Vec::new()),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
        block_receipts_response(vec![
            receipt(10, &block_hash, "0xtx0", 0, vec![matched_log]),
            receipt(10, &block_hash, "0xtx1", 1, vec![unrelated_log]),
        ]),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![primary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
    assert_eq!(rows[0].topics, vec![topic.to_owned()]);
}

#[test]
fn test_provider_filter_logs_reliability_does_not_recheck_primary_covered_block() {
    let topic = transfer_topic();
    let address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash = block_hash(10);
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(address),
        EvmLogBloomInput::Topic(topic),
    ])
    .expect("bloom")
    .as_hex();
    let primary_log = provider_log_with_address(10, &block_hash, "0xtx0", 0, 0, address, topic);
    let (primary_url, _primary_requests) = start_rpc_server(vec![
        logs_response(vec![primary_log.clone()]),
        json!([block_batch_response_with_bloom(
            1,
            10,
            &block_hash,
            "0xparent10",
            10,
            &bloom
        )]),
    ]);
    let (secondary_url, secondary_requests) = start_rpc_server(Vec::new());
    let client = EvmRpcClient::with_chain(
        vec![primary_url, secondary_url],
        ethereum_identity(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        10,
        10,
        3,
        10,
    );
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
    assert_eq!(rows[0].transaction_index, 0);
    assert_eq!(rows[0].log_index, 0);
    assert!(
        rows.iter()
            .all(|log| log.parent_hash.as_deref() == Some("0xparent10"))
    );
    assert!(rows.iter().all(|log| log.block_timestamp == Some(10)));
    let secondary_requests = secondary_requests.lock().expect("secondary requests");
    assert!(
        secondary_requests.is_empty(),
        "primary-covered bloom-positive blocks should stay on the primary eth_getLogs fast path"
    );
}

#[test]
fn test_provider_filter_logs_support_degov_style_address_and_topic0_unions() {
    let governor = "0x1111111111111111111111111111111111111111";
    let token = "0x2222222222222222222222222222222222222222";
    let timelock = "0x3333333333333333333333333333333333333333";
    let proposal_created = topic_with_prefix("aa");
    let vote_cast = topic_with_prefix("bb");
    let transfer = topic_with_prefix("cc");
    let timelock_call_scheduled = topic_with_prefix("dd");
    let block_hash = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let (url, requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                provider_log_with_address(10, block_hash, "0xtx2", 2, 4, timelock, &timelock_call_scheduled),
                provider_log_with_address(10, block_hash, "0xtx0", 0, 1, governor, &proposal_created),
                provider_log_with_address(10, block_hash, "0xtx0", 0, 0, token, &transfer),
                provider_log_with_address(10, block_hash, "0xtx1", 1, 3, governor, &vote_cast)
            ]
        }),
        block_response(10, block_hash, "0xparent", 1),
    ]);
    let client = EvmRpcClient::with_chain(
        vec![url],
        ethereum_identity(),
        EvmFinalityPolicy::Auto,
        10,
        10,
        3,
        3,
    )
    .with_logs_query_strategy(QueryStrategy::ProviderFilter)
    .with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![governor.to_owned(), token.to_owned(), timelock.to_owned()],
        topics: vec![Some(vec![
            proposal_created.clone(),
            vote_cast.clone(),
            transfer.clone(),
            timelock_call_scheduled.clone(),
        ])],
    })
    .expect("filter");

    let logs = client
        .fetch_evm_logs(BlockRange::expect_new(10, 10), &filter)
        .expect("logs");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_getLogs");
    let eth_get_logs_filter = &requests[0]["params"][0];
    assert_eq!(
        eth_get_logs_filter["address"],
        json!([governor, token, timelock])
    );
    assert_eq!(
        eth_get_logs_filter["topics"],
        json!([[
            proposal_created,
            vote_cast,
            transfer,
            timelock_call_scheduled
        ]])
    );
    assert_eq!(
        logs.iter()
            .map(|log| (log.address.as_str(), log.topics[0].as_str()))
            .collect::<Vec<_>>(),
        vec![
            (token, transfer.as_str()),
            (governor, proposal_created.as_str()),
            (governor, vote_cast.as_str()),
            (timelock, timelock_call_scheduled.as_str()),
        ]
    );
    assert_eq!(
        logs.iter()
            .map(|log| (log.block_number, log.transaction_index, log.log_index))
            .collect::<Vec<_>>(),
        vec![(10, 0, 0), (10, 0, 1), (10, 1, 3), (10, 2, 4)]
    );
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
fn test_block_range_logs_match_address_and_topic0_unions_as_and_filters() {
    let governor = "0x1111111111111111111111111111111111111111";
    let token = "0x2222222222222222222222222222222222222222";
    let timelock = "0x3333333333333333333333333333333333333333";
    let other_address = "0x4444444444444444444444444444444444444444";
    let proposal_created = topic_with_prefix("aa");
    let transfer = topic_with_prefix("cc");
    let timelock_call_scheduled = topic_with_prefix("dd");
    let unrelated = topic_with_prefix("ee");
    let (url, _requests) = start_rpc_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": full_block(10, "0xaaa", ["0xtx0", "0xtx1"])
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": receipt(10, "0xaaa", "0xtx0", 0, vec![
                provider_log_with_address(10, "0xaaa", "0xtx0", 0, 4, other_address, &proposal_created),
                provider_log_with_address(10, "0xaaa", "0xtx0", 0, 2, governor, &unrelated),
                provider_log_with_address(10, "0xaaa", "0xtx0", 0, 1, governor, &proposal_created)
            ])
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": receipt(10, "0xaaa", "0xtx1", 1, vec![
                provider_log_with_address(10, "0xaaa", "0xtx1", 1, 3, token, &transfer),
                provider_log_with_address(10, "0xaaa", "0xtx1", 1, 0, timelock, &timelock_call_scheduled)
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
        3,
    )
    .with_logs_query_strategy(QueryStrategy::BlockRange);
    let filter = datalens_core::EvmLogFilter::try_from(LogFilter {
        addresses: vec![governor.to_owned(), token.to_owned(), timelock.to_owned()],
        topics: vec![Some(vec![
            proposal_created.clone(),
            transfer.clone(),
            timelock_call_scheduled.clone(),
        ])],
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
    assert_eq!(
        rows.iter()
            .map(|log| (log.address.as_str(), log.topics[0].as_str()))
            .collect::<Vec<_>>(),
        vec![
            (governor, proposal_created.as_str()),
            (timelock, timelock_call_scheduled.as_str()),
            (token, transfer.as_str()),
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
    )
    .with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
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

#[test]
fn test_provider_filter_log_headers_reuse_cache_for_same_block_hash() {
    let block_hash = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let topic = transfer_topic();
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![provider_log(10, block_hash, topic)]),
        block_response(10, block_hash, "0xparent", 1),
        logs_response(vec![provider_log(10, block_hash, topic)]),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );
    let request = logs_fetch_request(10, 10);

    let first = client.fetch(request.clone()).expect("first logs");
    let second = client.fetch(request).expect("second logs");

    assert_eq!(first.provider_diagnostics.calls, 2);
    assert!(
        first
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_cache_misses=1"))
    );
    assert_eq!(second.provider_diagnostics.calls, 1);
    assert!(
        second
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_cache_hits=1"))
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1]["method"], "eth_getBlockByNumber");
}

#[test]
fn test_provider_filter_log_headers_miss_cache_for_same_number_different_hash() {
    let first_hash = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let second_hash = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    let topic = transfer_topic();
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![provider_log(10, first_hash, topic)]),
        block_response(10, first_hash, "0xparent1", 1),
        logs_response(vec![provider_log(10, second_hash, topic)]),
        block_response(10, second_hash, "0xparent2", 2),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );
    let request = logs_fetch_request(10, 10);

    client.fetch(request.clone()).expect("first logs");
    let second = client.fetch(request).expect("second logs");

    assert_eq!(second.provider_diagnostics.calls, 2);
    assert!(
        second
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_cache_misses=1"))
    );
    let block_header_calls = requests
        .lock()
        .expect("requests")
        .iter()
        .filter(|request| request["method"] == "eth_getBlockByNumber")
        .count();
    assert_eq!(block_header_calls, 2);
}

#[test]
fn test_provider_filter_log_headers_preserve_hash_mismatch_failure() {
    let log_hash = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let header_hash = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    let (url, _requests) = start_rpc_server(vec![
        logs_response(vec![provider_log(10, log_hash, transfer_topic())]),
        block_response(10, header_hash, "0xparent", 1),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent),
    );

    let error = client
        .fetch(logs_fetch_request(10, 10))
        .expect_err("hash mismatch");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("does not match fetched block hash"));
    assert!(error.message.contains("block 10"));
}

#[test]
fn test_provider_filter_log_headers_batch_mode_preserves_hash_mismatch_failure() {
    let log_hash = block_hash(10);
    let header_hash = block_hash(11);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![provider_log(10, &log_hash, transfer_topic())]),
        json!([block_batch_response(1, 10, &header_hash, "0xparent", 10)]),
    ]);
    let client = evm_test_client(url);

    let error = client
        .fetch(logs_fetch_request(10, 10))
        .expect_err("hash mismatch");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("does not match fetched block hash"));
    assert!(error.message.contains("block 10"));
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert!(requests[1].is_array());
}

#[test]
fn test_provider_filter_log_headers_fetch_missing_blocks_with_bounded_concurrency() {
    let (url, requests, max_active) = start_concurrent_header_server(4, Duration::from_millis(75));
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent)
            .with_fetch_concurrency(2),
    );

    let response = client
        .fetch(logs_fetch_request(10, 13))
        .expect("concurrent header logs");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(
        rows.iter().map(|log| log.block_number).collect::<Vec<_>>(),
        vec![10, 11, 12, 13]
    );
    assert_eq!(response.provider_diagnostics.calls, 5);
    assert!(max_active.load(Ordering::SeqCst) <= 2);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_fetch_mode=concurrent"))
    );
    assert_eq!(requests.lock().expect("requests").len(), 5);
}

#[test]
fn test_provider_filter_log_header_error_includes_block_context() {
    let hash10 = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let hash11 = "0x000000000000000000000000000000000000000000000000000000000000000b";
    let (url, _requests) = start_rpc_server(vec![
        logs_response(vec![
            provider_log(10, hash10, transfer_topic()),
            provider_log(11, hash11, transfer_topic()),
        ]),
        block_response(10, hash10, "0xparent10", 10),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32000,
                "message": "request timed out"
            }
        }),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent)
            .with_fetch_concurrency(1),
    );

    let error = client
        .fetch(logs_fetch_request(10, 11))
        .expect_err("header error");

    assert_eq!(error.kind, DatalensErrorKind::ProviderTimeout);
    assert!(error.message.contains("log block 11"));
}

#[test]
fn test_provider_filter_log_headers_batch_mode_maps_out_of_order_responses_by_default() {
    let hash10 = block_hash(10);
    let hash11 = block_hash(11);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![
            provider_log(10, &hash10, transfer_topic()),
            provider_log(11, &hash11, transfer_topic()),
        ]),
        json!([
            block_batch_response(2, 11, &hash11, "0xparent11", 11),
            block_batch_response(1, 10, &hash10, "0xparent10", 10)
        ]),
    ]);
    let client = evm_test_client(url);

    let response = client
        .fetch(logs_fetch_request(10, 11))
        .expect("batch headers");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[1].parent_hash.as_deref(), Some("0xparent11"));
    assert_eq!(response.provider_diagnostics.calls, 2);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_fetch_mode=batch"))
    );
    let requests = requests.lock().expect("requests");
    assert!(requests[1].is_array());
}

#[test]
fn test_provider_filter_log_headers_batch_mode_falls_back_on_partial_error() {
    let hash10 = block_hash(10);
    let hash11 = block_hash(11);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![
            provider_log(10, &hash10, transfer_topic()),
            provider_log(11, &hash11, transfer_topic()),
        ]),
        json!([
            block_batch_response(1, 10, &hash10, "0xparent10", 10),
            {
                "jsonrpc": "2.0",
                "id": 2,
                "error": {
                    "code": -32000,
                    "message": "request timed out"
                }
            }
        ]),
        block_response(11, &hash11, "0xparent11", 11),
    ]);
    let client = evm_test_client(url);

    let response = client
        .fetch(logs_fetch_request(10, 11))
        .expect("fallback headers");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[1].parent_hash.as_deref(), Some("0xparent11"));
    assert_eq!(response.provider_diagnostics.calls, 3);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_fallback_mode=concurrent"))
    );
    let requests = requests.lock().expect("requests");
    assert!(requests[1].is_array());
    assert_eq!(requests[2]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[2]["params"][0], "0xb");
}

#[test]
fn test_provider_filter_log_headers_batch_mode_falls_back_on_incompatible_response() {
    let hash10 = block_hash(10);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![provider_log(10, &hash10, transfer_topic())]),
        block_response(10, &hash10, "0xparent10", 10),
        block_response(10, &hash10, "0xparent10", 10),
    ]);
    let client = evm_test_client(url);

    let response = client
        .fetch(logs_fetch_request(10, 10))
        .expect("fallback headers");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(response.provider_diagnostics.calls, 3);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .iter()
            .any(|warning| warning.contains("header_fallback_mode=concurrent"))
    );
    let requests = requests.lock().expect("requests");
    assert!(requests[1].is_array());
    assert_eq!(requests[2]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[2]["params"][0], "0xa");
}

#[test]
fn test_provider_filter_log_headers_batch_mode_falls_back_on_missing_response() {
    let hash10 = block_hash(10);
    let hash11 = block_hash(11);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![
            provider_log(10, &hash10, transfer_topic()),
            provider_log(11, &hash11, transfer_topic()),
        ]),
        json!([block_batch_response(1, 10, &hash10, "0xparent10", 10)]),
        block_response(11, &hash11, "0xparent11", 11),
    ]);
    let client = evm_test_client(url);

    let response = client
        .fetch(logs_fetch_request(10, 11))
        .expect("fallback headers");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[1].parent_hash.as_deref(), Some("0xparent11"));
    assert_eq!(response.provider_diagnostics.calls, 3);
    let requests = requests.lock().expect("requests");
    assert!(requests[1].is_array());
    assert_eq!(requests[2]["method"], "eth_getBlockByNumber");
    assert_eq!(requests[2]["params"][0], "0xb");
}

#[test]
fn test_provider_filter_log_headers_batch_mode_splits_by_batch_size() {
    let hashes = (10..=13).map(block_hash).collect::<Vec<_>>();
    let (url, requests) = start_rpc_server(vec![
        logs_response(
            (10..=13)
                .zip(hashes.iter())
                .map(|(number, hash)| provider_log(number, hash, transfer_topic()))
                .collect(),
        ),
        json!([
            block_batch_response(1, 10, &hashes[0], "0xparent10", 10),
            block_batch_response(2, 11, &hashes[1], "0xparent11", 11)
        ]),
        json!([
            block_batch_response(1, 12, &hashes[2], "0xparent12", 12),
            block_batch_response(2, 13, &hashes[3], "0xparent13", 13)
        ]),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Batch)
            .with_batch_size(2),
    );

    let response = client
        .fetch(logs_fetch_request(10, 13))
        .expect("split batch headers");

    assert_eq!(response.provider_diagnostics.calls, 3);
    let batch_lengths = requests
        .lock()
        .expect("requests")
        .iter()
        .skip(1)
        .map(|request| request.as_array().expect("batch request").len())
        .collect::<Vec<_>>();
    assert_eq!(batch_lengths, vec![2, 2]);
}

#[test]
fn test_provider_filter_log_headers_use_batch_mode_by_default() {
    let client = EvmRpcClient::new(Vec::new());

    assert_eq!(
        client.block_header_metadata_config().fetch_mode,
        EvmBlockHeaderFetchMode::Batch
    );
}

#[test]
fn test_provider_filter_log_headers_explicit_concurrent_mode_skips_batch() {
    let hash10 = block_hash(10);
    let hash11 = block_hash(11);
    let (url, requests) = start_rpc_server(vec![
        logs_response(vec![
            provider_log(10, &hash10, transfer_topic()),
            provider_log(11, &hash11, transfer_topic()),
        ]),
        block_response(10, &hash10, "0xparent10", 10),
        block_response(11, &hash11, "0xparent11", 11),
    ]);
    let client = evm_test_client(url).with_block_header_metadata_config(
        EvmBlockHeaderMetadataConfig::default()
            .with_fetch_mode(EvmBlockHeaderFetchMode::Concurrent)
            .with_fetch_concurrency(1),
    );

    let response = client
        .fetch(logs_fetch_request(10, 11))
        .expect("concurrent headers");

    let QueryRows::EvmLogs(rows) = response.rows.rows() else {
        panic!("expected EVM logs");
    };
    assert_eq!(rows[0].parent_hash.as_deref(), Some("0xparent10"));
    assert_eq!(rows[1].parent_hash.as_deref(), Some("0xparent11"));
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| !request.is_array()));
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn evm_test_client(url: String) -> EvmRpcClient {
    EvmRpcClient::with_chain(
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
    )
    .with_log_reliability_config(EvmLogReliabilityConfig::default().with_enabled(false))
}

fn logs_fetch_request(from_block: u64, to_block: u64) -> ChainFetchRequest {
    ChainFetchRequest::new(
        ethereum_identity(),
        DatasetKey::evm_logs(),
        LedgerRange::from_block_range(BlockRange::expect_new(from_block, to_block)),
        DatasetSelector::EvmLogs(
            datalens_core::EvmLogFilter::try_from(&datalens_core::LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .expect("filter"),
        ),
    )
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

fn unsupported_method_response(message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": -32601,
            "message": message
        }
    })
}

fn provider_error_response(code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn transfer_topic() -> &'static str {
    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
}

fn unrelated_topic() -> &'static str {
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
}

fn topic_with_prefix(prefix: &str) -> String {
    format!("0x{prefix}{}", "0".repeat(62))
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

fn block_receipts_response(receipts: Vec<Value>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": receipts
    })
}

fn receipt_response(receipt: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": receipt
    })
}

fn block_response(number: u64, hash: &str, parent_hash: &str, timestamp: u64) -> Value {
    block_response_with_bloom(
        number,
        hash,
        parent_hash,
        timestamp,
        &format!("0x{}", "0".repeat(512)),
    )
}

fn block_with_transaction_hashes_response(
    number: u64,
    hash: &str,
    parent_hash: &str,
    timestamp: u64,
    transactions: Vec<&str>,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": format!("0x{timestamp:x}"),
            "transactions": transactions
        }
    })
}

fn block_response_with_bloom(
    number: u64,
    hash: &str,
    parent_hash: &str,
    timestamp: u64,
    logs_bloom: &str,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": format!("0x{timestamp:x}"),
            "logsBloom": logs_bloom
        }
    })
}

fn logs_response(logs: Vec<Value>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": logs
    })
}

fn block_batch_response(
    id: u64,
    number: u64,
    hash: &str,
    parent_hash: &str,
    timestamp: u64,
) -> Value {
    block_batch_response_with_bloom(
        id,
        number,
        hash,
        parent_hash,
        timestamp,
        &format!("0x{}", "0".repeat(512)),
    )
}

fn block_batch_response_with_bloom(
    id: u64,
    number: u64,
    hash: &str,
    parent_hash: &str,
    timestamp: u64,
    logs_bloom: &str,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": format!("0x{timestamp:x}"),
            "logsBloom": logs_bloom
        }
    })
}

fn block_hash(number: u64) -> String {
    format!("0x{number:064x}")
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
            let request_json = read_http_json(&mut stream);
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

fn start_concurrent_header_server(
    block_count: u64,
    delay: Duration,
) -> (String, Arc<Mutex<Vec<Value>>>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let max_active = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let request_log = Arc::clone(&requests);
    let max_active_log = Arc::clone(&max_active);
    let active_log = Arc::clone(&active);

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("test server connection");
            let request_log = Arc::clone(&request_log);
            let max_active = Arc::clone(&max_active_log);
            let active = Arc::clone(&active_log);
            thread::spawn(move || {
                let request_json = read_http_json(&mut stream);
                request_log
                    .lock()
                    .expect("request log")
                    .push(request_json.clone());
                let response = if request_json["method"] == "eth_getLogs" {
                    logs_response(
                        (10..10 + block_count)
                            .map(|number| {
                                provider_log(number, &block_hash(number), transfer_topic())
                            })
                            .collect(),
                    )
                } else {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    thread::sleep(delay);
                    let number = request_json["params"][0]
                        .as_str()
                        .and_then(|value| {
                            u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
                        })
                        .expect("block number");
                    let response = block_response(
                        number,
                        &block_hash(number),
                        &format!("0xparent{number}"),
                        number,
                    );
                    active.fetch_sub(1, Ordering::SeqCst);
                    response
                };
                let response = response.to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    response.len(),
                    response
                )
                .expect("write response");
            });
        }
    });

    (format!("http://{address}"), requests, max_active)
}

fn read_http_json(stream: &mut TcpStream) -> Value {
    let mut buffer = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 1024];
        let bytes = stream.read(&mut chunk).expect("read request");
        assert!(bytes > 0, "connection closed before headers");
        buffer.extend_from_slice(&chunk[..bytes]);
        if let Some(index) = find_header_end(&buffer) {
            break index;
        }
    };
    let header_text = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .expect("content-length header");
    let body_start = header_end + 4;
    while buffer.len() < body_start + content_length {
        let mut chunk = [0; 1024];
        let bytes = stream.read(&mut chunk).expect("read request body");
        assert!(bytes > 0, "connection closed before request body");
        buffer.extend_from_slice(&chunk[..bytes]);
    }
    serde_json::from_slice(&buffer[body_start..body_start + content_length]).expect("request JSON")
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}
