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

mod provider;
pub use provider::{TronFixtureProviderRpc, TronHttpProvider};

const TRON_ALL_KIND: &str = "tron_all";
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

pub trait TronProvider: Clone + Send + Sync + 'static {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError>;

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError>;

    fn get_transaction_info_by_id(&self, tx_id: &str) -> Result<Option<Value>, DatalensError>;

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
            .with_dataset_capability(capability(DatasetKey::tron_events()))
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
            (event_rows(&infos), info_calls)
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

fn event_rows(infos: &[TronTransactionInfo]) -> Vec<Value> {
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
            rows.push(json!({
                "contract_address": log.get("address").cloned().unwrap_or(Value::Null),
                "event_signature": topics.first().cloned().unwrap_or(Value::Null),
                "event_name": Value::Null,
                "indexed_fields": Value::Array(topics),
                "non_indexed_fields": log.get("data").cloned().unwrap_or(Value::Null),
                "transaction_id": info.transaction.tx_id,
                "block_number": info.transaction.block_number,
                "block_hash": info.transaction.block_hash,
                "transaction_index": info.transaction.transaction_index,
                "event_index": index,
                "source": {
                    "raw": log,
                },
            }));
        }
    }
    rows
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
