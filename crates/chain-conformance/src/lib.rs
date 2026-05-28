//! Reusable chain adapter conformance assertions for integration tests.

mod tron;
pub use tron::*;

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use datalens_chain::{
    AdapterKey, ChainAdapter, ChainFetchRequest, DatasetSelector, FetchContext, FinalityKind,
    HeightRangeKind, SelectorKind, validate_durable_range,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange, LogFilter, NetworkId,
    QueryRows,
};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct FixtureProvider {
    url: String,
    chain: ChainIdentity,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl FixtureProvider {
    pub fn evm() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture provider");
        let address = listener.local_addr().expect("fixture provider address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.expect("fixture provider connection");
                let mut buffer = [0; 8192];
                let bytes = stream.read(&mut buffer).expect("read fixture request");
                let request = String::from_utf8_lossy(&buffer[..bytes]);
                let body = request.split("\r\n\r\n").nth(1).expect("request body");
                let request_json: Value = serde_json::from_str(body).expect("request JSON");
                request_log
                    .lock()
                    .expect("request log")
                    .push(request_json.clone());
                write_response(&mut stream, evm_response(&request_json));
            }
        });

        Self {
            url: format!("http://{address}"),
            chain: ChainIdentity::try_new(
                ChainFamily::Evm,
                "ethereum",
                Some(NetworkId::numeric(1)),
            )
            .expect("valid chain"),
            requests,
        }
    }

    pub fn url(&self) -> String {
        self.url.clone()
    }

    pub fn chain(&self) -> ChainIdentity {
        self.chain.clone()
    }

    pub fn evm_log_selector(&self) -> DatasetSelector {
        DatasetSelector::try_evm_logs(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![Some(vec![
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ])],
        })
        .expect("valid selector")
    }

    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().expect("requests").clone()
    }
}

#[derive(Clone)]
pub struct SolanaFixtureProvider {
    chain: ChainIdentity,
}

impl SolanaFixtureProvider {
    pub fn solana() -> Self {
        Self {
            chain: ChainIdentity::try_new(
                ChainFamily::Other("solana".to_owned()),
                "solana-mainnet-beta",
                Some(NetworkId::textual("mainnet-beta").expect("valid network id")),
            )
            .expect("valid chain"),
        }
    }

    pub fn chain(&self) -> ChainIdentity {
        self.chain.clone()
    }

    pub fn all_selector(&self) -> DatasetSelector {
        DatasetSelector::try_other(
            AdapterKey::try_new("solana_all").expect("adapter key"),
            "solana-all/all",
            "all",
        )
        .expect("valid selector")
    }

    pub fn program_selector(&self) -> DatasetSelector {
        DatasetSelector::try_other(
            AdapterKey::try_new("solana_program").expect("adapter key"),
            "solana-program/959c1dfe13b28404",
            "program/program1111111111111111111111111111111111",
        )
        .expect("valid selector")
    }
}

pub fn assert_capability_conformance<A>(adapter: &A, expected_chain: ChainIdentity)
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.chain(), &expected_chain);
    assert!(capabilities.datasets().contains(&DatasetKey::evm_blocks()));
    assert!(capabilities.datasets().contains(&DatasetKey::evm_logs()));

    let blocks = capabilities
        .dataset(&DatasetKey::evm_blocks())
        .expect("blocks capability");
    assert!(blocks.supports_selector(SelectorKind::All));
    assert!(blocks.ranges().contains(&HeightRangeKind::Block));
    assert_eq!(blocks.max_range_len(), Some(2));
    assert!(blocks.supports_safe_height());
    assert!(blocks.supports_finalized_height());
    assert!(blocks.supports_empty_coverage());
    assert!(blocks.supports_range_split());
    assert!(blocks.supports_reorg_signals());
    assert!(blocks.supports_canonical_block_lookup());
    assert!(blocks.supports_latest_reorg_signal());

    let logs = capabilities
        .dataset(&DatasetKey::evm_logs())
        .expect("logs capability");
    assert!(logs.supports_selector(SelectorKind::EvmLogs));
    assert!(logs.ranges().contains(&HeightRangeKind::Block));
    assert_eq!(logs.max_range_len(), Some(3));
    assert_eq!(logs.max_addresses_per_query(), Some(1));
    assert!(logs.supports_safe_height());
    assert!(logs.supports_finalized_height());
    assert!(logs.supports_empty_coverage());
    assert!(logs.supports_range_split());
    assert!(logs.supports_reorg_signals());
    assert!(logs.supports_canonical_block_lookup());
    assert!(logs.supports_latest_reorg_signal());
}

pub fn assert_fetch_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let block_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_blocks(),
        LedgerRange::blocks(2, 3).expect("valid range"),
        DatasetSelector::All,
    );
    let blocks = adapter.fetch(block_request.clone()).expect("blocks");
    blocks
        .validate_for_request(&block_request)
        .expect("response matches request");
    let QueryRows::EvmBlocks(rows) = blocks.rows.rows() else {
        panic!("expected EVM block rows");
    };
    assert_eq!(
        rows.iter().map(|row| row.number).collect::<Vec<_>>(),
        [2, 3]
    );
    assert!(
        rows.iter()
            .all(|row| block_request.range.contains(row.number))
    );
    assert!(rows.iter().all(|row| !row.hash.is_empty()));
    assert!(rows.iter().all(|row| !row.parent_hash.is_empty()));
    assert!(rows.iter().all(|row| row.timestamp > 0));

    let log_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(2, 3).expect("valid range"),
        selector.clone(),
    );
    let logs = adapter.fetch(log_request.clone()).expect("logs");
    logs.validate_for_request(&log_request)
        .expect("response matches request");
    let QueryRows::EvmLogs(rows) = logs.rows.rows() else {
        panic!("expected EVM log rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| (row.block_number, row.transaction_index, row.log_index))
            .collect::<Vec<_>>(),
        [(2, 0, 0), (3, 0, 1)]
    );
    assert!(
        rows.iter()
            .all(|row| log_request.range.contains(row.block_number))
    );

    let empty_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(4, 4).expect("valid range"),
        selector,
    );
    let empty = adapter.fetch(empty_request.clone()).expect("empty logs");
    empty
        .validate_for_request(&empty_request)
        .expect("empty response confirms provider query");
    assert_eq!(empty.rows.row_count(), 0);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::tron_blocks(),
            LedgerRange::blocks(2, 2).expect("valid range"),
            DatasetSelector::All,
        ))
        .expect_err("unsupported dataset");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_blocks(),
            LedgerRange::slots(2, 2).expect("valid range"),
            DatasetSelector::All,
        ))
        .expect_err("unsupported range kind");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 4).expect("valid range"),
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: Vec::new(),
            })
            .expect("valid selector"),
        ))
        .expect_err("range limit");
    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::evm_logs(),
            LedgerRange::blocks(2, 3).expect("valid range"),
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: vec![
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    "0xcccccccccccccccccccccccccccccccccccccccc".to_owned(),
                ],
                topics: Vec::new(),
            })
            .expect("valid selector"),
        ))
        .expect_err("address limit");
    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
}

pub fn assert_finality_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let latest = adapter.latest_height().expect("latest height");
    let safe = adapter.cache_safe_height().expect("cache-safe height");
    let finalized = adapter.finalized_height().expect("finalized height");

    assert_eq!(latest.value, 8);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized.finality, FinalityKind::Finalized);
    assert!(safe.value <= latest.value);
    assert!(finalized.value <= latest.value);
    validate_durable_range(&LedgerRange::blocks(1, safe.value).unwrap(), &safe)
        .expect("safe height can authorize durable writes");
}

pub fn assert_reorg_signal_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let signal = adapter
        .reorg_signal(HeightRangeKind::Block, 3)
        .expect("block signal");
    assert_eq!(signal.height, 3);
    assert_eq!(signal.hash, block_hash(3));
    assert_eq!(signal.parent_hash, block_hash(2));
    assert!(signal.timestamp.is_some());

    let latest = adapter.latest_reorg_signal().expect("latest signal");
    assert_eq!(latest.height, 8);
    assert_eq!(latest.hash, block_hash(8));

    let error = adapter
        .reorg_signal(HeightRangeKind::Slot, 3)
        .expect_err("unsupported signal range");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

pub fn assert_metadata_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let request = ChainFetchRequest::new(
        chain,
        DatasetKey::evm_logs(),
        LedgerRange::blocks(2, 3).expect("valid range"),
        selector,
    )
    .with_context(FetchContext {
        request_id: Some("conformance-request".to_owned()),
        cache_write: true,
    });
    let response = adapter.fetch(request.clone()).expect("metadata response");

    response
        .validate_for_request(&request)
        .expect("response matches request");
    assert_eq!(response.source_metadata.provider, "evm-rpc");
    assert_eq!(
        response.source_metadata.request_id.as_deref(),
        Some("conformance-request")
    );
    assert!(response.provider_diagnostics.calls > 0);
    assert_eq!(response.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        response.coverage_selector.canonical_key(),
        request.selector.canonical_key()
    );
}

pub fn assert_solana_capability_conformance<A>(adapter: &A, expected_chain: ChainIdentity)
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.chain(), &expected_chain);
    assert!(
        capabilities
            .datasets()
            .contains(&DatasetKey::solana_slots())
    );
    assert!(
        capabilities
            .datasets()
            .contains(&DatasetKey::solana_transactions())
    );
    assert!(
        capabilities
            .datasets()
            .contains(&DatasetKey::solana_instructions())
    );

    let slots = capabilities
        .dataset(&DatasetKey::solana_slots())
        .expect("slots capability");
    assert!(slots.supports_selector(SelectorKind::Other(
        AdapterKey::try_new("solana_all").expect("adapter key")
    )));
    assert!(slots.ranges().contains(&HeightRangeKind::Slot));
    assert_eq!(slots.max_range_len(), Some(64));
    assert!(slots.supports_finalized_height());
    assert!(!slots.supports_safe_height());
    assert!(slots.supports_empty_coverage());
    assert!(slots.supports_range_split());
    assert!(slots.supports_reorg_signals());

    let transactions = capabilities
        .dataset(&DatasetKey::solana_transactions())
        .expect("transactions capability");
    assert!(transactions.supports_selector(SelectorKind::Other(
        AdapterKey::try_new("solana_program").expect("adapter key")
    )));
    assert!(transactions.supports_selector(SelectorKind::Other(
        AdapterKey::try_new("solana_address").expect("adapter key")
    )));
    assert!(transactions.ranges().contains(&HeightRangeKind::Slot));

    let instructions = capabilities
        .dataset(&DatasetKey::solana_instructions())
        .expect("instructions capability");
    assert!(instructions.supports_selector(SelectorKind::Other(
        AdapterKey::try_new("solana_program").expect("adapter key")
    )));
    assert!(instructions.ranges().contains(&HeightRangeKind::Slot));
}

pub fn assert_solana_fetch_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let all_selector = SolanaFixtureProvider::solana().all_selector();
    let slots_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::solana_slots(),
        LedgerRange::slots(10, 12).expect("valid range"),
        all_selector,
    );
    let slots = adapter.fetch(slots_request.clone()).expect("slots");
    slots
        .validate_for_request(&slots_request)
        .expect("response matches request");
    let QueryRows::AdapterJson { rows, .. } = slots.rows.rows() else {
        panic!("expected Solana adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        [10, 12]
    );
    assert!(rows.iter().all(|row| row["range_kind"] == "slot"));
    assert!(rows.iter().all(|row| row["commitment"] == "finalized"));
    assert!(rows.iter().all(|row| row["blockhash"].is_string()));
    assert!(rows.iter().all(|row| row["previous_blockhash"].is_string()));
    assert!(rows.iter().all(|row| row["parent_slot"].is_u64()));

    let transaction_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::solana_transactions(),
        LedgerRange::slots(10, 12).expect("valid range"),
        selector.clone(),
    );
    let transactions = adapter
        .fetch(transaction_request.clone())
        .expect("transactions");
    transactions
        .validate_for_request(&transaction_request)
        .expect("response matches request");
    let QueryRows::AdapterJson { rows, .. } = transactions.rows.rows() else {
        panic!("expected Solana transaction rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["slot"], 10);
    assert_eq!(rows[0]["signature"], "sig-slot-10");

    let empty_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::solana_transactions(),
        LedgerRange::slots(12, 12).expect("valid range"),
        selector.clone(),
    );
    let empty = adapter.fetch(empty_request.clone()).expect("empty");
    empty
        .validate_for_request(&empty_request)
        .expect("empty response matches request");
    assert_eq!(empty.rows.row_count(), 0);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::slots(10, 10).expect("valid range"),
            selector.clone(),
        ))
        .expect_err("unsupported dataset");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::solana_slots(),
            LedgerRange::blocks(10, 10).expect("valid range"),
            selector,
        ))
        .expect_err("unsupported range");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

pub fn assert_solana_finality_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let latest = adapter.latest_height().expect("latest slot");
    let safe = adapter.cache_safe_height().expect("cache-safe slot");
    let finalized = adapter.finalized_height().expect("finalized slot");

    assert_eq!(latest.range_kind, HeightRangeKind::Slot);
    assert_eq!(latest.value, 14);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.range_kind, HeightRangeKind::Slot);
    assert_eq!(safe.value, 12);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized, safe);
    validate_durable_range(&LedgerRange::slots(10, safe.value).unwrap(), &safe)
        .expect("finalized slot can authorize durable writes");
}

pub fn assert_solana_reorg_signal_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let signal = adapter
        .reorg_signal(HeightRangeKind::Slot, 12)
        .expect("slot signal");
    assert_eq!(signal.range_kind, HeightRangeKind::Slot);
    assert_eq!(signal.height, 12);
    assert_eq!(signal.hash, "slot-12-hash");
    assert_eq!(signal.parent_hash, "slot-10-hash");
    assert!(signal.timestamp.is_some());

    let latest = adapter.latest_reorg_signal().expect("latest signal");
    assert_eq!(latest.range_kind, HeightRangeKind::Slot);
    assert_eq!(latest.height, 14);
    assert_eq!(latest.hash, "slot-14-latest");

    let error = adapter
        .reorg_signal(HeightRangeKind::Block, 12)
        .expect_err("unsupported block signal");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

pub fn assert_solana_metadata_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let request = ChainFetchRequest::new(
        chain,
        DatasetKey::solana_transactions(),
        LedgerRange::slots(10, 12).expect("valid range"),
        selector,
    )
    .with_context(FetchContext {
        request_id: Some("conformance-request".to_owned()),
        cache_write: true,
    });
    let response = adapter.fetch(request.clone()).expect("metadata response");

    response
        .validate_for_request(&request)
        .expect("response matches request");
    assert_eq!(response.source_metadata.provider, "solana-fixture");
    assert_eq!(
        response.source_metadata.request_id.as_deref(),
        Some("conformance-request")
    );
    assert!(response.provider_diagnostics.calls > 0);
    assert_eq!(response.dataset_key, DatasetKey::solana_transactions());
    assert_eq!(
        response.coverage_selector.canonical_key(),
        request.selector.canonical_key()
    );
}

fn write_response(stream: &mut impl Write, response: Value) {
    let response = response.to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        response.len(),
        response
    )
    .expect("write fixture response");
}

fn evm_response(request: &Value) -> Value {
    let method = request["method"].as_str().expect("method");
    match method {
        "eth_blockNumber" => rpc_result(json!("0x8")),
        "eth_getBlockByNumber" => {
            let tag = request["params"][0].as_str().expect("block tag");
            if tag == "latest" {
                return rpc_result(evm_block(8));
            }
            let number =
                u64::from_str_radix(tag.trim_start_matches("0x"), 16).expect("hex block number");
            rpc_result(evm_block(number))
        }
        "eth_getLogs" => {
            let from = hex_param_number(request, "fromBlock");
            let to = hex_param_number(request, "toBlock");
            if from == 4 && to == 4 {
                rpc_result(json!([]))
            } else {
                rpc_result(json!([
                    evm_log(3, 0, 1),
                    evm_log(2, 0, 0),
                    evm_log(9, 0, 2)
                ]))
            }
        }
        _ => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32601,
                "message": "unsupported method"
            }
        }),
    }
}

fn rpc_result(result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result
    })
}

fn hex_param_number(request: &Value, key: &str) -> u64 {
    let value = request["params"][0][key].as_str().expect("hex parameter");
    u64::from_str_radix(value.trim_start_matches("0x"), 16).expect("hex number")
}

fn evm_block(number: u64) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": block_hash(number),
        "parentHash": if number == 0 { block_hash(0) } else { block_hash(number - 1) },
        "timestamp": format!("0x{:x}", 1_700_000_000 + number)
    })
}

fn evm_log(block_number: u64, transaction_index: u64, log_index: u64) -> Value {
    json!({
        "blockNumber": format!("0x{block_number:x}"),
        "blockHash": block_hash(block_number),
        "transactionHash": format!("0x{:064x}", 0x1000 + block_number),
        "transactionIndex": format!("0x{transaction_index:x}"),
        "logIndex": format!("0x{log_index:x}"),
        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics": ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
        "data": "0x",
        "removed": false
    })
}

fn block_hash(number: u64) -> String {
    format!("0x{:064x}", number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixture_model_can_express_reorg_conflict() {
        let canonical = reorg_fixture_block(10, "0xcanon", "0xparent");
        let alternate = reorg_fixture_block(10, "0xalternate", "0xotherparent");

        assert_eq!(canonical["number"], alternate["number"]);
        assert_ne!(canonical["hash"], alternate["hash"]);
        assert_ne!(canonical["parentHash"], alternate["parentHash"]);
    }

    fn reorg_fixture_block(number: u64, hash: &str, parent_hash: &str) -> Value {
        json!({
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "timestamp": "0x1"
        })
    }
}
