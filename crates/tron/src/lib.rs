//! Tron chain-family adapter boundary.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, AdapterKey, CanonicalBlock, CanonicalBlockRequest, ChainAdapter,
    ChainFetchRequest, ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector,
    FinalityKind, HeightRangeKind, ProviderDiagnostics, ReorgSignal, SelectorKind, SourceMetadata,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId, QueryRows,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod provider;
pub use provider::{TronFixtureProviderRpc, TronHttpProvider};

const TRON_ALL_KIND: &str = "tron_all";
const TRON_EVENTS_KIND: &str = "tron_events";
const FINALIZED: TronFinality = TronFinality::Finalized;
const LATEST: TronFinality = TronFinality::Latest;

type BlockCache = Arc<Mutex<HashMap<(u64, u64), Vec<TronBlock>>>>;
type TransactionInfoCache = Arc<Mutex<HashMap<String, Value>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TronFinality {
    Latest,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub witness_address: Option<String>,
    pub transaction_count: usize,
    pub raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronEventFilter {
    pub contract_addresses: Vec<String>,
    pub event_names: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronContractEventRequest {
    pub contract_address: String,
    pub event_name: Option<String>,
    pub range: LedgerRange,
    pub only_confirmed: bool,
    pub limit: usize,
    pub fingerprint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronContractEventPage {
    pub events: Vec<TronContractEvent>,
    pub next_fingerprint: Option<String>,
    pub provider_calls: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronContractEvent {
    pub contract_address: String,
    pub event_name: Option<String>,
    pub event_signature: Option<String>,
    pub indexed_fields: Vec<Value>,
    pub non_indexed_fields: Value,
    pub transaction_id: Option<String>,
    pub block_number: u64,
    pub block_hash: Option<String>,
    pub transaction_index: u64,
    pub event_index: u64,
    pub confirmed: bool,
    pub raw: Value,
}

pub trait TronProvider: Clone + Send + Sync + 'static {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError>;

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError>;

    fn get_transaction_info_by_id(&self, tx_id: &str) -> Result<Option<Value>, DatalensError>;

    fn supports_contract_event_query(&self) -> bool {
        false
    }

    fn get_contract_events(
        &self,
        _request: TronContractEventRequest,
    ) -> Result<TronContractEventPage, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "Tron provider does not support contract event queries",
        ))
    }

    fn provider_name(&self) -> &'static str;
}

#[derive(Clone)]
pub struct TronAdapter<P> {
    chain: ChainIdentity,
    provider: P,
    max_block_range_len: u64,
    block_cache: BlockCache,
    transaction_info_cache: TransactionInfoCache,
}

impl<P> TronAdapter<P>
where
    P: TronProvider,
{
    pub fn with_provider(chain: ChainIdentity, provider: P) -> Self {
        Self {
            chain,
            provider,
            max_block_range_len: 64,
            block_cache: Arc::new(Mutex::new(HashMap::new())),
            transaction_info_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_max_block_range_len(mut self, max_block_range_len: u64) -> Self {
        self.max_block_range_len = max_block_range_len.max(1);
        self
    }

    fn block_range(&self, request: &ChainFetchRequest) -> Result<LedgerRange, DatalensError> {
        if request.chain != self.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "request chain is not supported by Tron adapter",
            ));
        }
        if request.range.kind() != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron adapter only supports block ranges",
            ));
        }
        if request.range.len() > u128::from(self.max_block_range_len) {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderLimit,
                "request range exceeds Tron provider block range limit",
            ));
        }
        Ok(request.range.clone())
    }

    fn fetch_blocks_for_range(
        &self,
        range: &LedgerRange,
    ) -> Result<(Vec<TronBlock>, usize), DatalensError> {
        let cache_key = (range.start(), range.end());
        if let Some(blocks) = self
            .block_cache
            .lock()
            .expect("Tron block cache")
            .get(&cache_key)
            .cloned()
        {
            return Ok((blocks, 0));
        }
        let mut blocks = Vec::new();
        for number in range.start()..=range.end() {
            if let Some(block) = self.provider.get_block_by_number(number, FINALIZED)? {
                blocks.push(block);
            }
        }
        blocks.sort_by_key(|block| block.number);
        let provider_calls = range.len().min(u128::from(usize::MAX as u64)) as usize;
        self.block_cache
            .lock()
            .expect("Tron block cache")
            .insert(cache_key, blocks.clone());
        Ok((blocks, provider_calls))
    }

    fn fetch_transaction_infos(
        &self,
        transactions: &[TronTransactionRef],
    ) -> Result<(Vec<TronTransactionInfo>, usize), DatalensError> {
        let mut infos = Vec::new();
        let mut provider_calls = 0;
        for transaction in transactions {
            let raw = if let Some(raw) = self
                .transaction_info_cache
                .lock()
                .expect("Tron transaction info cache")
                .get(&transaction.tx_id)
                .cloned()
            {
                raw
            } else {
                let raw = self
                    .provider
                    .get_transaction_info_by_id(&transaction.tx_id)?
                    .ok_or_else(|| {
                        DatalensError::new(
                            DatalensErrorKind::ProviderFailure,
                            format!(
                                "provider returned no Tron transaction info for {}",
                                transaction.tx_id
                            ),
                        )
                    })?;
                provider_calls += 1;
                self.transaction_info_cache
                    .lock()
                    .expect("Tron transaction info cache")
                    .insert(transaction.tx_id.clone(), raw.clone());
                raw
            };
            infos.push(TronTransactionInfo {
                transaction: transaction.clone(),
                raw,
            });
        }
        Ok((infos, provider_calls))
    }

    fn metadata(
        &self,
        request: &ChainFetchRequest,
        calls: usize,
    ) -> (SourceMetadata, ProviderDiagnostics) {
        (
            SourceMetadata {
                provider: self.provider.provider_name().to_owned(),
                request_id: request.context.request_id.clone(),
            },
            ProviderDiagnostics {
                calls,
                rows_scanned: 0,
                warnings: Vec::new(),
            },
        )
    }
}

impl TronAdapter<TronFixtureProviderRpc> {
    pub fn with_fixture_defaults() -> Self {
        Self::with_provider(default_tron_chain(), TronFixtureProviderRpc)
            .with_max_block_range_len(3)
    }

    pub fn with_provider_limits(
        provider: TronFixtureProviderRpc,
        max_block_range_len: u64,
    ) -> Self {
        Self::with_provider(default_tron_chain(), provider)
            .with_max_block_range_len(max_block_range_len)
    }
}

impl<P> ChainAdapter for TronAdapter<P>
where
    P: TronProvider,
{
    fn capabilities(&self) -> AdapterCapabilities {
        let capability = |dataset_key| {
            DatasetCapability::new(dataset_key)
                .with_selector(SelectorKind::Other(adapter_key(TRON_ALL_KIND)))
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(self.max_block_range_len)
                .with_empty_coverage(true)
                .with_finalized_height(true)
                .with_range_split(true)
                .with_reorg_signals(true)
        };
        AdapterCapabilities::new(self.chain.clone())
            .with_dataset_capability(capability(DatasetKey::tron_blocks()))
            .with_dataset_capability(capability(DatasetKey::tron_transactions()))
            .with_dataset_capability(capability(DatasetKey::tron_transaction_infos()))
            .with_dataset_capability(
                capability(DatasetKey::tron_events())
                    .with_selector(SelectorKind::Other(adapter_key(TRON_EVENTS_KIND))),
            )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(
            ChainHeight::block(self.provider.latest_block(LATEST)?.number)
                .with_finality(FinalityKind::Latest),
        )
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        self.finalized_height()
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(
            ChainHeight::block(self.provider.latest_block(FINALIZED)?.number)
                .with_finality(FinalityKind::Finalized),
        )
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        if request.chain != self.chain || request.range_kind != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron canonical lookup only supports blocks for this chain",
            ));
        }
        let block = self
            .provider
            .get_block_by_number(request.height, FINALIZED)?
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "provider returned no finalized Tron block",
                )
            })?;
        Ok(CanonicalBlock {
            chain: request.chain,
            height: block.number,
            hash: block.hash,
            parent_hash: block.parent_hash,
            finality: FinalityKind::Finalized,
        })
    }

    fn reorg_signal(
        &self,
        range_kind: HeightRangeKind,
        height: u64,
    ) -> Result<ReorgSignal, DatalensError> {
        if range_kind != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron reorg signals only support blocks",
            ));
        }
        let block = self
            .provider
            .get_block_by_number(height, FINALIZED)?
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "provider returned no finalized Tron block",
                )
            })?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn latest_reorg_signal(&self) -> Result<ReorgSignal, DatalensError> {
        let block = self.provider.latest_block(LATEST)?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = self.block_range(&request)?;
        let capability = self
            .capabilities()
            .dataset(&request.dataset_key)
            .cloned()
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by Tron adapter",
                )
            })?;
        if !capability.supports_selector(request.selector.kind()) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "selector is not supported by Tron adapter",
            ));
        }
        if request.dataset_key == DatasetKey::tron_events()
            && matches!(request.selector.kind(), SelectorKind::Other(kind) if kind.as_str() == TRON_EVENTS_KIND)
            && self.provider.supports_contract_event_query()
        {
            match self.fetch_contract_events(&request, &range) {
                Ok((rows, calls)) => {
                    let rows = QueryRows::AdapterJson {
                        dataset_key: request.dataset_key.clone(),
                        rows,
                    };
                    let (source_metadata, provider_diagnostics) = self.metadata(&request, calls);
                    return Ok(ChainFetchResponse::try_new(
                        request.chain,
                        request.dataset_key,
                        request.range,
                        request.selector,
                        rows,
                    )?
                    .with_source_metadata(source_metadata)
                    .with_provider_diagnostics(provider_diagnostics));
                }
                Err(error) if should_fallback_from_contract_events(error.kind.clone()) => {}
                Err(error) => return Err(error),
            }
        }

        let (blocks, provider_calls) = self.fetch_blocks_for_range(&range)?;
        let transactions = transaction_refs(&blocks)?;
        let (rows, extra_calls) = if request.dataset_key == DatasetKey::tron_blocks() {
            (block_rows(&blocks), 0)
        } else if request.dataset_key == DatasetKey::tron_transactions() {
            (transaction_rows(&transactions), 0)
        } else if request.dataset_key == DatasetKey::tron_transaction_infos() {
            let (infos, info_calls) = self.fetch_transaction_infos(&transactions)?;
            (transaction_info_rows(&infos), info_calls)
        } else if request.dataset_key == DatasetKey::tron_events() {
            let (infos, info_calls) = self.fetch_transaction_infos(&transactions)?;
            (event_rows(&infos, &request.selector), info_calls)
        } else {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "dataset is not supported by Tron adapter",
            ));
        };
        let rows = QueryRows::AdapterJson {
            dataset_key: request.dataset_key.clone(),
            rows,
        };
        let (source_metadata, provider_diagnostics) =
            self.metadata(&request, provider_calls + extra_calls);
        Ok(ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            rows,
        )?
        .with_source_metadata(source_metadata)
        .with_provider_diagnostics(provider_diagnostics))
    }
}

pub fn tron_all_selector() -> Result<DatasetSelector, DatalensError> {
    DatasetSelector::try_other(adapter_key(TRON_ALL_KIND), "tron-all/all", "all")
}

pub fn tron_contract_selector(address: impl AsRef<str>) -> Result<DatasetSelector, DatalensError> {
    tron_event_selector(TronEventFilter {
        contract_addresses: vec![address.as_ref().to_owned()],
        event_names: Vec::new(),
    })
}

pub fn tron_event_selector(filter: TronEventFilter) -> Result<DatasetSelector, DatalensError> {
    let filter = NormalizedTronEventFilter::try_from(filter)?;
    let canonical_key = format!(
        "contracts/{}/events/{}",
        filter.contract_addresses.join("+"),
        if filter.event_names.is_empty() {
            "all".to_owned()
        } else {
            filter.event_names.join("+")
        }
    );
    let mut digest = Sha256::new();
    digest.update(canonical_key.as_bytes());
    let digest = digest.finalize();
    let fingerprint = format!("tron-events/{}", hex_prefix(&digest, 12));
    DatasetSelector::try_other(adapter_key(TRON_EVENTS_KIND), fingerprint, canonical_key)
}

pub fn normalize_tron_contract_address(address: &str) -> Result<String, DatalensError> {
    let address = address.trim();
    let hex = address.strip_prefix("0x").unwrap_or(address);
    if hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(format!("41{}", hex.to_ascii_lowercase()));
    }
    if hex.len() == 42 && hex.starts_with("41") && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(hex.to_ascii_lowercase());
    }
    if address.len() == 34
        && address.starts_with('T')
        && address.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Ok(address.to_owned());
    }
    Err(DatalensError::new(
        DatalensErrorKind::InvalidInput,
        "Tron contract address must be 20-byte hex, 41-prefixed hex, or base58",
    ))
}

fn adapter_key(value: &str) -> AdapterKey {
    AdapterKey::try_new(value).expect("valid Tron adapter key")
}

fn default_tron_chain() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron-mainnet",
        Some(NetworkId::textual("mainnet").expect("valid network id")),
    )
    .expect("valid Tron chain")
}

fn block_rows(blocks: &[TronBlock]) -> Vec<Value> {
    blocks
        .iter()
        .map(|block| {
            json!({
                "number": block.number,
                "range_kind": "block",
                "hash": block.hash,
                "parent_hash": block.parent_hash,
                "timestamp": block.timestamp,
                "witness_address": block.witness_address,
                "transaction_count": block.transaction_count,
                "finality": "finalized",
                "reorg": {
                    "hash": block.hash,
                    "parent_hash": block.parent_hash,
                },
                "source": {
                    "provider_block_id": block.hash,
                    "raw": block.raw,
                }
            })
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TronTransactionRef {
    tx_id: String,
    block_number: u64,
    block_hash: String,
    transaction_index: u64,
    raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TronTransactionInfo {
    transaction: TronTransactionRef,
    raw: Value,
}

fn transaction_refs(blocks: &[TronBlock]) -> Result<Vec<TronTransactionRef>, DatalensError> {
    let mut transactions = Vec::new();
    for block in blocks {
        let Some(raw_transactions) = block.raw.get("transactions").and_then(Value::as_array) else {
            continue;
        };
        for (index, transaction) in raw_transactions.iter().enumerate() {
            let tx_id = string_field(transaction, "txID", "Tron transaction missing txID")?;
            transactions.push(TronTransactionRef {
                tx_id,
                block_number: block.number,
                block_hash: block.hash.clone(),
                transaction_index: index as u64,
                raw: transaction.clone(),
            });
        }
    }
    transactions
        .sort_by_key(|transaction| (transaction.block_number, transaction.transaction_index));
    Ok(transactions)
}

fn transaction_rows(transactions: &[TronTransactionRef]) -> Vec<Value> {
    transactions
        .iter()
        .map(|transaction| {
            json!({
                "transaction_id": transaction.tx_id,
                "block_number": transaction.block_number,
                "block_hash": transaction.block_hash,
                "transaction_index": transaction.transaction_index,
                "contract_calls": contract_calls(&transaction.raw),
                "result": transaction.raw.get("ret").cloned().unwrap_or(Value::Null),
                "source": {
                    "raw": transaction.raw,
                },
            })
        })
        .collect()
}

fn transaction_info_rows(infos: &[TronTransactionInfo]) -> Vec<Value> {
    infos
        .iter()
        .map(|info| {
            json!({
                "transaction_id": info.transaction.tx_id,
                "block_number": info.transaction.block_number,
                "block_hash": info.transaction.block_hash,
                "transaction_index": info.transaction.transaction_index,
                "result": info.raw.get("receipt").cloned().unwrap_or(Value::Null),
                "fee": info.raw.get("fee").cloned().unwrap_or(Value::Null),
                "energy_usage_total": info.raw.get("receipt").and_then(|receipt| receipt.get("energy_usage_total")).cloned().unwrap_or(Value::Null),
                "net_usage": info.raw.get("receipt").and_then(|receipt| receipt.get("net_usage")).cloned().unwrap_or(Value::Null),
                "contract_result": info.raw.get("contractResult").cloned().unwrap_or(Value::Null),
                "logs": info.raw.get("log").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
                "source": {
                    "raw": info.raw,
                },
            })
        })
        .collect()
}

fn event_rows(infos: &[TronTransactionInfo], selector: &DatasetSelector) -> Vec<Value> {
    let filter = selector_event_filter(selector);
    let mut rows = Vec::new();
    for info in infos {
        let Some(logs) = info.raw.get("log").and_then(Value::as_array) else {
            continue;
        };
        for (index, log) in logs.iter().enumerate() {
            let topics = log
                .get("topics")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let contract_address = log
                .get("address")
                .and_then(Value::as_str)
                .and_then(|address| normalize_tron_contract_address(address).ok())
                .unwrap_or_else(|| {
                    log.get("address")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                });
            let event_name = event_name_from_signature(topics.first().and_then(Value::as_str));
            if !filter.matches(&contract_address, event_name.as_deref()) {
                continue;
            }
            rows.push(json!({
                "contract_address": contract_address,
                "event_signature": topics.first().cloned().unwrap_or(Value::Null),
                "event_name": event_name,
                "indexed_fields": Value::Array(topics),
                "non_indexed_fields": log.get("data").cloned().unwrap_or(Value::Null),
                "transaction_id": info.transaction.tx_id,
                "block_number": info.transaction.block_number,
                "block_hash": info.transaction.block_hash,
                "transaction_index": info.transaction.transaction_index,
                "event_index": index,
                "confirmed": true,
                "source": {
                    "provider": "tron_block_scan",
                    "raw": log,
                },
            }));
        }
    }
    rows
}

impl<P> TronAdapter<P>
where
    P: TronProvider,
{
    fn fetch_contract_events(
        &self,
        request: &ChainFetchRequest,
        range: &LedgerRange,
    ) -> Result<(Vec<Value>, usize), DatalensError> {
        let filter = selector_event_filter(&request.selector);
        let mut rows = Vec::new();
        let mut calls = 0;
        for contract_address in &filter.contract_addresses {
            let event_names = if filter.event_names.is_empty() {
                vec![None]
            } else {
                filter.event_names.iter().cloned().map(Some).collect()
            };
            for event_name in event_names {
                let mut fingerprint = None;
                loop {
                    let page = self
                        .provider
                        .get_contract_events(TronContractEventRequest {
                            contract_address: contract_address.clone(),
                            event_name: event_name.clone(),
                            range: range.clone(),
                            only_confirmed: true,
                            limit: 200,
                            fingerprint: fingerprint.clone(),
                        })?;
                    calls += page.provider_calls;
                    rows.extend(
                        page.events
                            .into_iter()
                            .filter(|event| {
                                filter.matches(&event.contract_address, event.event_name.as_deref())
                            })
                            .map(contract_event_row),
                    );
                    let Some(next) = page.next_fingerprint else {
                        break;
                    };
                    fingerprint = Some(next);
                }
            }
        }
        Ok((rows, calls))
    }
}

fn contract_event_row(event: TronContractEvent) -> Value {
    json!({
        "contract_address": event.contract_address,
        "event_name": event.event_name,
        "event_signature": event.event_signature,
        "indexed_fields": event.indexed_fields,
        "non_indexed_fields": event.non_indexed_fields,
        "transaction_id": event.transaction_id,
        "block_number": event.block_number,
        "block_hash": event.block_hash,
        "transaction_index": event.transaction_index,
        "event_index": event.event_index,
        "confirmed": event.confirmed,
        "source": {
            "provider": "trongrid_contract_events",
            "raw": event.raw,
        },
    })
}

fn should_fallback_from_contract_events(kind: DatalensErrorKind) -> bool {
    matches!(
        kind,
        DatalensErrorKind::AuthenticationFailed
            | DatalensErrorKind::Unauthorized
            | DatalensErrorKind::RateLimited
            | DatalensErrorKind::ProviderFailure
            | DatalensErrorKind::ProviderLimit
            | DatalensErrorKind::ProviderTimeout
            | DatalensErrorKind::UnsupportedDataset
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NormalizedTronEventFilter {
    contract_addresses: Vec<String>,
    event_names: Vec<String>,
}

impl NormalizedTronEventFilter {
    fn matches(&self, contract_address: &str, event_name: Option<&str>) -> bool {
        let normalized = normalize_tron_contract_address(contract_address)
            .unwrap_or_else(|_| contract_address.to_owned());
        if !self.contract_addresses.is_empty() && !self.contract_addresses.contains(&normalized) {
            return false;
        }
        self.event_names.is_empty()
            || event_name.is_some_and(|name| self.event_names.iter().any(|value| value == name))
    }
}

impl TryFrom<TronEventFilter> for NormalizedTronEventFilter {
    type Error = DatalensError;

    fn try_from(filter: TronEventFilter) -> Result<Self, Self::Error> {
        let mut contract_addresses = filter
            .contract_addresses
            .iter()
            .map(|address| normalize_tron_contract_address(address))
            .collect::<Result<Vec<_>, _>>()?;
        contract_addresses.sort();
        contract_addresses.dedup();
        if contract_addresses.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "Tron event selector requires at least one contract address",
            ));
        }
        let mut event_names = filter
            .event_names
            .iter()
            .map(|name| normalize_event_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        event_names.sort();
        event_names.dedup();
        Ok(Self {
            contract_addresses,
            event_names,
        })
    }
}

fn selector_event_filter(selector: &DatasetSelector) -> NormalizedTronEventFilter {
    let DatasetSelector::Other {
        kind,
        canonical_key,
        ..
    } = selector
    else {
        return NormalizedTronEventFilter {
            contract_addresses: Vec::new(),
            event_names: Vec::new(),
        };
    };
    if kind.as_str() != TRON_EVENTS_KIND {
        return NormalizedTronEventFilter {
            contract_addresses: Vec::new(),
            event_names: Vec::new(),
        };
    }
    let Some(rest) = canonical_key.strip_prefix("contracts/") else {
        return NormalizedTronEventFilter {
            contract_addresses: Vec::new(),
            event_names: Vec::new(),
        };
    };
    let Some((contracts, events)) = rest.split_once("/events/") else {
        return NormalizedTronEventFilter {
            contract_addresses: Vec::new(),
            event_names: Vec::new(),
        };
    };
    NormalizedTronEventFilter {
        contract_addresses: contracts.split('+').map(str::to_owned).collect(),
        event_names: if events == "all" {
            Vec::new()
        } else {
            events.split('+').map(str::to_owned).collect()
        },
    }
}

fn normalize_event_name(name: &str) -> Result<String, DatalensError> {
    let name = name.trim();
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron event name must contain only letters, numbers, or underscores",
        ));
    }
    Ok(name.to_owned())
}

fn event_name_from_signature(signature: Option<&str>) -> Option<String> {
    match signature {
        Some("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") => {
            Some("Transfer".to_owned())
        }
        _ => None,
    }
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(len * 2);
    for byte in bytes.iter().take(len) {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn contract_calls(transaction: &Value) -> Value {
    let calls = transaction
        .get("raw_data")
        .and_then(|raw_data| raw_data.get("contract"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Value::Array(calls)
}

fn string_field(value: &Value, name: &str, message: &str) -> Result<String, DatalensError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| DatalensError::new(DatalensErrorKind::ProviderFailure, message))
}
