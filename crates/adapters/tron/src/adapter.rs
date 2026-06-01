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
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use crate::provider::{TronFixtureProviderRpc, TronHttpProvider};
use crate::rows::{
    NormalizedTronEventFilter, TronTransactionInfo, TronTransactionRef, block_rows, event_rows,
    hex_prefix, should_fallback_from_contract_events, transaction_info_rows, transaction_refs,
    transaction_rows, validate_block_scan_event_filter,
};

const TRON_ALL_KIND: &str = "tron_all";
pub(crate) const TRON_EVENTS_KIND: &str = "tron_events";
const FINALIZED: TronFinality = TronFinality::Finalized;
const LATEST: TronFinality = TronFinality::Latest;
const DEFAULT_MAX_CONTRACT_EVENT_PAGES: usize = 100;

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
/// TronGrid contract event filter before normalization. Selectors require at
/// least one contract address so optimized event queries and block-scan fallback
/// remain bounded by explicit caller intent.
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
    pub(crate) provider: P,
    max_block_range_len: u64,
    pub(crate) max_contract_event_pages: usize,
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
            max_contract_event_pages: DEFAULT_MAX_CONTRACT_EVENT_PAGES,
            block_cache: Arc::new(Mutex::new(HashMap::new())),
            transaction_info_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_max_block_range_len(mut self, max_block_range_len: u64) -> Self {
        self.max_block_range_len = max_block_range_len.max(1);
        self
    }

    pub fn with_max_contract_event_pages(mut self, max_contract_event_pages: usize) -> Self {
        self.max_contract_event_pages = max_contract_event_pages.max(1);
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
        let mut missing_blocks = Vec::new();
        for number in range.start()..=range.end() {
            if let Some(block) = self.provider.get_block_by_number(number, FINALIZED)? {
                blocks.push(block);
            } else {
                missing_blocks.push(number);
            }
        }
        if !missing_blocks.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "provider returned no finalized Tron block for requested height(s): {}",
                    missing_blocks
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ));
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
            // Prefer TronGrid contract events when available; fallback is only
            // used for errors that can be safely recovered by finalized block
            // scanning without weakening authorization or rate-limit signals.
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
                Err(error) if should_fallback_from_contract_events(&error) => {}
                Err(error) => return Err(error),
            }
        }

        if request.dataset_key == DatasetKey::tron_events()
            && matches!(request.selector.kind(), SelectorKind::Other(kind) if kind.as_str() == TRON_EVENTS_KIND)
        {
            // Block-scan fallback can filter event names only when the name maps
            // to a known topic; otherwise it would silently broaden results.
            validate_block_scan_event_filter(&request.selector)?;
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
    // The canonical key is human-readable for task dedupe, while the digest
    // keeps durable object paths short and stable.
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
