use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    thread,
};

use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FinalityKind, HeightRangeKind,
};
use datalens_core::{
    DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, QueryRows, QueryStrategy,
};
use datalens_solana::{
    SolanaAdapter, SolanaFixtureRpc, SolanaHttpRpc, SolanaRpc, SolanaSignatureInfo,
    solana_address_selector, solana_all_selector, solana_program_selector,
    solana_signature_selector,
};
use serde_json::{Value, json};

#[test]
fn test_slots_fetch_skips_missing_slots_and_keeps_ordered_adapter_json_rows() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let request = ChainFetchRequest::new(
        adapter.capabilities().chain().clone(),
        DatasetKey::solana_slots(),
        LedgerRange::slots(10, 12).expect("valid range"),
        solana_all_selector().expect("selector"),
    );

    let response = adapter.fetch(request.clone()).expect("fetch slots");
    response
        .validate_for_request(&request)
        .expect("response matches request");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert_eq!(rows[0]["commitment"], "finalized");
    assert_eq!(rows[0]["blockhash"], "slot-10-hash");
    assert_eq!(rows[0]["previous_blockhash"], "slot-9-hash");
    assert_eq!(rows[0]["parent_slot"], 9);
    assert_eq!(rows[0]["transaction_count"], 1);
}

#[test]
fn test_program_selector_fetches_transactions_and_instructions() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let selector = solana_program_selector("program1111111111111111111111111111111111")
        .expect("program selector");
    let chain = adapter.capabilities().chain().clone();

    let transactions = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            selector.clone(),
        ))
        .expect("transactions");
    let QueryRows::AdapterJson { rows, .. } = transactions.rows.rows() else {
        panic!("expected transaction JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sig-slot-10");
    assert_eq!(rows[0]["selector_kind"], "solana_program");

    let instructions = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::solana_instructions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            selector,
        ))
        .expect("instructions");
    let QueryRows::AdapterJson { rows, .. } = instructions.rows.rows() else {
        panic!("expected instruction JSON rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["program_id"],
        "program1111111111111111111111111111111111"
    );
    assert_eq!(rows[0]["instruction_path"], "0");
    assert_eq!(rows[1]["instruction_path"], "0/0");
}

#[test]
fn test_block_range_strategy_skips_signature_discovery_and_scans_slots() {
    let provider = OptimizedSolanaRpc::default();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone())
        .with_query_strategy(QueryStrategy::BlockRange);
    let selector =
        solana_address_selector("Account111111111111111111111111111111111").expect("selector");

    let response = adapter
        .fetch(ChainFetchRequest::new(
            default_chain(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("range"),
            selector,
        ))
        .expect("transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sig-slot-10");
    assert_eq!(provider.signature_address_calls(), Vec::<String>::new());
    assert_eq!(provider.transaction_calls(), Vec::<String>::new());
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert_eq!(provider.block_calls(), vec![10, 12]);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana block_range query strategy used".to_owned())
    );
}

#[test]
fn test_all_selector_fetches_account_balance_updates() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_account_updates(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect("account updates");
    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected account update JSON rows");
    };

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["slot"], 10);
    assert_eq!(rows[0]["signature"], "sig-slot-10");
    assert_eq!(
        rows[0]["account"],
        "Account111111111111111111111111111111111"
    );
    assert_eq!(rows[0]["account_index"], 0);
    assert_eq!(rows[0]["update_kind"], "lamports");
    assert_eq!(rows[0]["lamports_before"], 1_000_000);
    assert_eq!(rows[0]["lamports_after"], 900_000);
    assert_eq!(rows[0]["lamports_delta"], -100_000);
    assert_eq!(rows[0]["source"], "getBlock.transaction.meta");
    assert_eq!(rows[0]["selector_kind"], "solana_all");
    assert_eq!(rows[0]["commitment"], "finalized");
    assert_eq!(rows[1]["update_kind"], "spl_token");
    assert_eq!(rows[1]["mint"], "TokenMint11111111111111111111111111111111");
    assert_eq!(rows[1]["token_amount_before"], "10");
    assert_eq!(rows[1]["token_amount_after"], "7");
}

#[test]
fn test_address_selector_uses_stable_storage_safe_fingerprint() {
    let first =
        solana_address_selector(" Account111111111111111111111111111111111 ").expect("selector");
    let second =
        solana_address_selector("Account111111111111111111111111111111111").expect("selector");

    assert_eq!(first, second);
    assert_eq!(
        first.canonical_key(),
        "address/Account111111111111111111111111111111111"
    );
    assert!(first.fingerprint().starts_with("solana-address/"));
    assert!(!first.fingerprint().contains("Account111"));
}

#[test]
fn test_finality_boundaries_are_slot_based_and_finalized_is_durable() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let latest = adapter.latest_height().expect("latest slot");
    let safe = adapter.cache_safe_height().expect("safe slot");
    let finalized = adapter.finalized_height().expect("finalized slot");

    assert_eq!(latest.range_kind, HeightRangeKind::Slot);
    assert_eq!(latest.value, 14);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.range_kind, HeightRangeKind::Slot);
    assert_eq!(safe.value, 12);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized, safe);
}

#[test]
fn test_unsupported_evm_and_block_requests_are_stable_errors() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::slots(10, 10).expect("valid range"),
            DatasetSelector::all(),
        ))
        .expect_err("evm logs unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::solana_slots(),
            LedgerRange::blocks(10, 10).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect_err("block ranges unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_provider_limit_is_classified_for_oversized_slot_ranges() {
    let adapter = SolanaAdapter::with_provider_limits(SolanaFixtureRpc, 2);
    let error = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_slots(),
            LedgerRange::slots(10, 13).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect_err("range too large");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
}

#[test]
fn test_signature_selector_uses_get_transaction_when_available() {
    let provider = OptimizedSolanaRpc::default();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_signature_selector("sigslot10").expect("selector"),
        ))
        .expect("transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sigslot10");
    assert_eq!(provider.transaction_calls(), vec!["sigslot10"]);
    assert_eq!(provider.blocks_with_limit_calls(), 0);
    assert_eq!(provider.block_calls(), Vec::<u64>::new());
}

#[test]
fn test_address_selector_discovers_signatures_before_fetching_transactions() {
    let provider = OptimizedSolanaRpc::default();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_account_updates(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("account updates");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        provider.signature_address_calls(),
        vec![
            "Account111111111111111111111111111111111",
            "Account111111111111111111111111111111111",
        ]
    );
    assert_eq!(provider.transaction_calls(), vec!["sigslot10"]);
    assert_eq!(provider.blocks_with_limit_calls(), 0);
}

#[test]
fn test_optimized_selector_fetch_falls_back_to_slot_scan_on_provider_limit() {
    let provider = OptimizedSolanaRpc::with_signature_discovery_error(
        DatalensErrorKind::ProviderLimit,
        "provider limit",
    );
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(provider.signature_address_calls().len(), 1);
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert_eq!(provider.block_calls(), vec![10, 12]);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana optimized selector fetch failed; fell back to slot scan".to_owned())
    );
}

#[test]
fn test_solana_http_rpc_classifies_429_as_rate_limited() {
    let url = start_rpc_server(vec![rpc_response(
        429,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": 429,
                "message": "too many requests"
            }
        }),
    )]);
    let error = SolanaHttpRpc::new(url)
        .get_slot(datalens_solana::SolanaCommitment::Finalized)
        .expect_err("rate limited");

    assert_eq!(error.kind, DatalensErrorKind::RateLimited);
}

#[test]
fn test_solana_http_rpc_classifies_unsupported_json_rpc_method() {
    let url = start_rpc_server(vec![rpc_response(
        200,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32601,
                "message": "method not found"
            }
        }),
    )]);
    let error = SolanaHttpRpc::new(url)
        .get_transaction("sigslot10", datalens_solana::SolanaCommitment::Finalized)
        .expect_err("unsupported method");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_solana_transport_error_redacts_rpc_url_credentials() {
    let address = unused_local_address();
    let url =
        format!("http://user:password@{address}/solana?token=query-token&password=query-password");

    let error = SolanaHttpRpc::new(url)
        .get_slot(datalens_solana::SolanaCommitment::Finalized)
        .expect_err("connect failure");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("http://<redacted>@"));
    assert!(error.message.contains("token=<redacted>"));
    assert!(error.message.contains("password=<redacted>"));
    assert!(!error.message.contains("user:password"));
    assert!(!error.message.contains("query-token"));
    assert!(!error.message.contains("query-password"));
}

#[test]
fn test_solana_provider_error_redacts_rpc_url_credentials_in_message() {
    let url = start_rpc_server(vec![rpc_response(
        200,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32000,
                "message": "backend failed at https://user:password@rpc.example.invalid/path?access_token=query-token&key=query-key"
            }
        }),
    )]);

    let error = SolanaHttpRpc::new(url)
        .get_slot(datalens_solana::SolanaCommitment::Finalized)
        .expect_err("provider error");

    assert!(
        error
            .message
            .contains("https://<redacted>@rpc.example.invalid/path")
    );
    assert!(error.message.contains("access_token=<redacted>"));
    assert!(error.message.contains("key=<redacted>"));
    assert!(!error.message.contains("password"));
    assert!(!error.message.contains("query-token"));
    assert!(!error.message.contains("query-key"));
}

#[test]
fn test_http_429_from_signature_discovery_falls_back_to_slot_scan() {
    let url = start_rpc_server(vec![
        rpc_response(
            429,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": 429,
                    "message": "too many requests"
                }
            }),
        ),
        rpc_response(200, json!({ "jsonrpc": "2.0", "id": 1, "result": [10] })),
        rpc_response(
            200,
            json!({ "jsonrpc": "2.0", "id": 1, "result": rpc_block(10) }),
        ),
    ]);
    let adapter = SolanaAdapter::with_provider(default_chain(), SolanaHttpRpc::new(url));
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("fallback transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sigslot10");
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana optimized selector fetch failed; fell back to slot scan".to_owned())
    );
}

#[test]
fn test_unsupported_get_transaction_falls_back_to_slot_scan() {
    let provider = OptimizedSolanaRpc::with_transaction_error(
        DatalensErrorKind::UnsupportedDataset,
        "method not found",
    );
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("fallback transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(provider.transaction_calls(), vec!["sigslot10"]);
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana optimized selector fetch failed; fell back to slot scan".to_owned())
    );
}

#[test]
fn test_malformed_optimized_transaction_response_is_fatal() {
    let provider = OptimizedSolanaRpc::with_transaction_error(
        DatalensErrorKind::ProviderFailure,
        "missing transaction slot",
    );
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let error = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_signature_selector("sigslot10").expect("selector"),
        ))
        .expect_err("malformed optimized response is fatal");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert_eq!(provider.blocks_with_limit_calls(), 0);
}

#[test]
fn test_signature_discovery_page_cap_falls_back_to_slot_scan() {
    let provider = OptimizedSolanaRpc::with_endless_newer_signature_pages();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("fallback transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert!(provider.signature_address_calls().len() > 1);
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana optimized selector fetch failed; fell back to slot scan".to_owned())
    );
}

#[derive(Clone, Default)]
struct OptimizedSolanaRpc {
    state: std::sync::Arc<std::sync::Mutex<OptimizedState>>,
}

#[derive(Default)]
struct OptimizedState {
    signature_address_calls: Vec<String>,
    transaction_calls: Vec<String>,
    blocks_with_limit_calls: u64,
    block_calls: Vec<u64>,
    signature_discovery_error: Option<DatalensErrorKind>,
    signature_discovery_message: String,
    transaction_error: Option<DatalensErrorKind>,
    transaction_error_message: String,
    endless_newer_signature_pages: bool,
}

impl OptimizedSolanaRpc {
    fn with_signature_discovery_error(kind: DatalensErrorKind, message: &str) -> Self {
        let provider = Self::default();
        {
            let mut state = provider.state.lock().expect("state");
            state.signature_discovery_error = Some(kind);
            state.signature_discovery_message = message.to_owned();
        }
        provider
    }

    fn with_transaction_error(kind: DatalensErrorKind, message: &str) -> Self {
        let provider = Self::default();
        {
            let mut state = provider.state.lock().expect("state");
            state.transaction_error = Some(kind);
            state.transaction_error_message = message.to_owned();
        }
        provider
    }

    fn with_endless_newer_signature_pages() -> Self {
        let provider = Self::default();
        provider
            .state
            .lock()
            .expect("state")
            .endless_newer_signature_pages = true;
        provider
    }

    fn signature_address_calls(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("state")
            .signature_address_calls
            .clone()
    }

    fn transaction_calls(&self) -> Vec<String> {
        self.state.lock().expect("state").transaction_calls.clone()
    }

    fn blocks_with_limit_calls(&self) -> u64 {
        self.state.lock().expect("state").blocks_with_limit_calls
    }

    fn block_calls(&self) -> Vec<u64> {
        self.state.lock().expect("state").block_calls.clone()
    }
}

impl datalens_solana::SolanaRpc for OptimizedSolanaRpc {
    fn get_slot(
        &self,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<u64, datalens_core::DatalensError> {
        SolanaFixtureRpc.get_slot(commitment)
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Vec<u64>, datalens_core::DatalensError> {
        self.state.lock().expect("state").blocks_with_limit_calls += 1;
        SolanaFixtureRpc.get_blocks_with_limit(start_slot, limit, commitment)
    }

    fn get_block(
        &self,
        slot: u64,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Option<datalens_solana::SolanaBlock>, datalens_core::DatalensError> {
        self.state.lock().expect("state").block_calls.push(slot);
        SolanaFixtureRpc.get_block(slot, commitment)
    }

    fn get_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        _until: Option<&str>,
        _limit: usize,
        _commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Vec<SolanaSignatureInfo>, datalens_core::DatalensError> {
        let mut state = self.state.lock().expect("state");
        state.signature_address_calls.push(address.to_owned());
        if let Some(kind) = state.signature_discovery_error.clone() {
            return Err(datalens_core::DatalensError::new(
                kind,
                state.signature_discovery_message.clone(),
            ));
        }
        if state.endless_newer_signature_pages {
            let page = state.signature_address_calls.len();
            return Ok(vec![SolanaSignatureInfo {
                signature: format!("newsig{page}"),
                slot: 100,
            }]);
        }
        if before.is_some() {
            return Ok(Vec::new());
        }
        Ok(vec![SolanaSignatureInfo {
            signature: "sigslot10".to_owned(),
            slot: 10,
        }])
    }

    fn get_transaction(
        &self,
        signature: &str,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Option<datalens_solana::SolanaTransactionWithSlot>, datalens_core::DatalensError>
    {
        self.state
            .lock()
            .expect("state")
            .transaction_calls
            .push(signature.to_owned());
        let state = self.state.lock().expect("state");
        if let Some(kind) = state.transaction_error.clone() {
            return Err(DatalensError::new(
                kind,
                state.transaction_error_message.clone(),
            ));
        }
        drop(state);
        let block = SolanaFixtureRpc
            .get_block(10, commitment)?
            .expect("fixture block");
        Ok(block
            .transactions
            .into_iter()
            .next()
            .map(|mut transaction| {
                transaction.signature = signature.to_owned();
                datalens_solana::SolanaTransactionWithSlot {
                    slot: block.slot,
                    block_time: block.block_time,
                    blockhash: block.blockhash,
                    transaction,
                    raw: block.raw,
                }
            }))
    }

    fn provider_name(&self) -> &'static str {
        "optimized-solana-fixture"
    }
}

fn default_chain() -> datalens_core::ChainIdentity {
    datalens_core::ChainIdentity::try_new(
        datalens_core::ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(datalens_core::NetworkId::textual("mainnet-beta").expect("network id")),
    )
    .expect("chain")
}

fn unused_local_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused address");
    listener.local_addr().expect("unused address")
}

struct RpcResponse {
    status: u16,
    body: Value,
}

fn rpc_response(status: u16, body: Value) -> RpcResponse {
    RpcResponse { status, body }
}

fn start_rpc_server(responses: Vec<RpcResponse>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let responses = std::sync::Arc::new(std::sync::Mutex::new(responses));

    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("test server connection");
            let mut buffer = [0; 8192];
            let _ = stream.read(&mut buffer).expect("read request");
            let response = responses.lock().expect("responses").remove(0);
            let body = response.body.to_string();
            write!(
                stream,
                "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                response.status,
                body.len(),
                body
            )
            .expect("write response");
        }
    });

    format!("http://{address}")
}

fn rpc_block(slot: u64) -> Value {
    json!({
        "blockHeight": 1000,
        "blockhash": format!("slot-{slot}-hash"),
        "previousBlockhash": format!("slot-{}-hash", slot.saturating_sub(1)),
        "parentSlot": slot.saturating_sub(1),
        "blockTime": 1700000010,
        "transactions": [{
            "transaction": {
                "signatures": ["sigslot10"],
                "message": {
                    "recentBlockhash": format!("slot-{slot}-hash"),
                    "accountKeys": [
                        "Account111111111111111111111111111111111",
                        "program1111111111111111111111111111111111"
                    ],
                    "instructions": []
                }
            },
            "meta": {
                "fee": 5000,
                "preBalances": [1000000, 1],
                "postBalances": [900000, 1],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "innerInstructions": []
            }
        }]
    })
}
