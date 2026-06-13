use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, AdapterKey, CanonicalBlock, CanonicalBlockRequest, ChainAdapter,
    ChainFetchRequest, ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector,
    FinalityKind, HeightRangeKind, ProviderDiagnostics, ReorgSignal, SelectorKind, SourceMetadata,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId, QueryRows, QueryStrategy,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use crate::provider::{TronFixtureProviderRpc, TronGridContractEventsConfig, TronHttpProvider};
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
const DEFAULT_BLOCK_CACHE_MAX_ENTRIES: usize = 16;
const DEFAULT_TRANSACTION_INFO_CACHE_MAX_ENTRIES: usize = 1024;

type BlockCache = Arc<Mutex<BoundedCache<(u64, u64), Vec<TronBlock>>>>;
type TransactionInfoCache = Arc<Mutex<BoundedCache<String, Value>>>;

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
    pub start_timestamp: Option<u64>,
    pub end_timestamp: Option<u64>,
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
    events_query_strategy: QueryStrategy,
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
            events_query_strategy: QueryStrategy::ProviderFilter,
            max_contract_event_pages: DEFAULT_MAX_CONTRACT_EVENT_PAGES,
            block_cache: Arc::new(Mutex::new(BoundedCache::new(
                DEFAULT_BLOCK_CACHE_MAX_ENTRIES,
            ))),
            transaction_info_cache: Arc::new(Mutex::new(BoundedCache::new(
                DEFAULT_TRANSACTION_INFO_CACHE_MAX_ENTRIES,
            ))),
        }
    }

    pub fn with_max_block_range_len(mut self, max_block_range_len: u64) -> Self {
        self.max_block_range_len = max_block_range_len.max(1);
        self
    }

    pub fn with_events_query_strategy(mut self, query_strategy: QueryStrategy) -> Self {
        self.events_query_strategy = query_strategy;
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
            .put(cache_key, blocks.clone());
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
                    .put(transaction.tx_id.clone(), raw.clone());
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
        warnings: Vec<String>,
    ) -> (SourceMetadata, ProviderDiagnostics) {
        (
            SourceMetadata {
                provider: self.provider.provider_name().to_owned(),
                request_id: request.context.request_id.clone(),
            },
            ProviderDiagnostics {
                calls,
                rows_scanned: 0,
                warnings,
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
            && self.events_query_strategy == QueryStrategy::ProviderFilter
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
                    let (source_metadata, provider_diagnostics) =
                        self.metadata(&request, calls, Vec::new());
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

        let mut warnings = Vec::new();
        if request.dataset_key == DatasetKey::tron_events()
            && self.events_query_strategy == QueryStrategy::BlockRange
        {
            warnings.push("tron block_range event query strategy used".to_owned());
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
        let provider_calls = provider_calls + extra_calls;
        let provider_calls = if rows.is_empty() && provider_calls == 0 {
            1
        } else {
            provider_calls
        };
        let rows = QueryRows::AdapterJson {
            dataset_key: request.dataset_key.clone(),
            rows,
        };
        let (source_metadata, provider_diagnostics) =
            self.metadata(&request, provider_calls, warnings);
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

#[derive(Clone, Debug)]
struct BoundedCache<K, V> {
    max_entries: usize,
    entries: HashMap<K, V>,
    lru: VecDeque<K>,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let value = self.entries.get(key)?.clone();
        self.touch(key);
        Some(value)
    }

    fn put(&mut self, key: K, value: V) {
        self.entries.insert(key.clone(), value);
        self.touch(&key);
        while self.entries.len() > self.max_entries {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn touch(&mut self, key: &K) {
        self.lru.retain(|existing| existing != key);
        self.lru.push_back(key.clone());
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
        return base58check_decode_address(address);
    }
    Err(DatalensError::new(
        DatalensErrorKind::InvalidInput,
        "Tron contract address must be 20-byte hex, 41-prefixed hex, or base58",
    ))
}

fn base58check_decode_address(address: &str) -> Result<String, DatalensError> {
    let decoded = base58_decode(address).ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron base58 address contains an invalid digit",
        )
    })?;
    if decoded.len() != 25 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron base58 address has an invalid length",
        ));
    }
    let (payload, checksum) = decoded.split_at(21);
    let first = Sha256::digest(payload);
    let second = Sha256::digest(first);
    if checksum != &second[..4] {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron base58 address checksum is invalid",
        ));
    }
    if payload.first() != Some(&0x41) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron base58 address must use mainnet 0x41 prefix",
        ));
    }
    Ok(payload.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn base58_decode(value: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    let mut bytes = vec![0_u8];
    for character in value.bytes() {
        let mut carry = ALPHABET.iter().position(|byte| *byte == character)? as u32;
        for byte in &mut bytes {
            let next = u32::from(*byte) * 58 + carry;
            *byte = (next % 256) as u8;
            carry = next / 256;
        }
        while carry > 0 {
            bytes.push((carry % 256) as u8);
            carry /= 256;
        }
    }
    let leading_zeroes = value.bytes().take_while(|byte| *byte == b'1').count();
    let mut decoded = vec![0_u8; leading_zeroes];
    decoded.extend(bytes.iter().rev());
    Some(decoded)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_cache_evicts_least_recently_used_range() {
        let mut cache = BoundedCache::new(2);
        cache.put(
            (10, 10),
            vec![TronBlock {
                number: 10,
                hash: "block-10".to_owned(),
                parent_hash: "block-9".to_owned(),
                timestamp: 1_700_000_010,
                witness_address: None,
                transaction_count: 0,
                raw: serde_json::json!({ "number": 10 }),
            }],
        );
        cache.put(
            (11, 11),
            vec![TronBlock {
                number: 11,
                hash: "block-11".to_owned(),
                parent_hash: "block-10".to_owned(),
                timestamp: 1_700_000_011,
                witness_address: None,
                transaction_count: 0,
                raw: serde_json::json!({ "number": 11 }),
            }],
        );

        assert!(cache.get(&(10, 10)).is_some());

        cache.put(
            (12, 12),
            vec![TronBlock {
                number: 12,
                hash: "block-12".to_owned(),
                parent_hash: "block-11".to_owned(),
                timestamp: 1_700_000_012,
                witness_address: None,
                transaction_count: 0,
                raw: serde_json::json!({ "number": 12 }),
            }],
        );

        assert!(cache.get(&(10, 10)).is_some());
        assert!(cache.get(&(11, 11)).is_none());
        assert!(cache.get(&(12, 12)).is_some());
    }

    #[test]
    fn transaction_info_cache_evicts_least_recently_used_transaction() {
        let mut cache = BoundedCache::new(2);
        cache.put("tx-10".to_owned(), serde_json::json!({ "id": "tx-10" }));
        cache.put("tx-11".to_owned(), serde_json::json!({ "id": "tx-11" }));

        assert!(cache.get(&"tx-10".to_owned()).is_some());

        cache.put("tx-12".to_owned(), serde_json::json!({ "id": "tx-12" }));

        assert!(cache.get(&"tx-10".to_owned()).is_some());
        assert!(cache.get(&"tx-11".to_owned()).is_none());
        assert!(cache.get(&"tx-12".to_owned()).is_some());
    }
}
