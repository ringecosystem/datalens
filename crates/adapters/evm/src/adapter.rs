use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use datalens_chain::{
    AdapterCapabilities, CanonicalBlock, CanonicalBlockRequest, ChainAdapter, ChainFetchRequest,
    ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector, FinalityKind,
    HeightRangeKind, ReorgSignal, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, EvmLogFilter, EvmReceipt, EvmTransaction, LedgerRange, LogFilter, LogRecord,
    QueryRows, QueryStrategy, TopicFilter, redact_urls_in_text,
};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::provider_payload::{
    LagFinalityPolicy, chain_profile, classify_transport_error, evm_log_filter, hex_u64_field,
    is_finality_tag_unsupported, string_field, zero_lag_error,
};
pub use crate::provider_payload::{
    classify_provider_error, height_from_latest_lag, parse_log_record, parse_receipt,
    parse_transaction,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmAdapterMetadata {
    pub provider_kind: &'static str,
}

impl Default for EvmAdapterMetadata {
    fn default() -> Self {
        Self {
            provider_kind: "unconfigured",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EvmAdapter {
    metadata: EvmAdapterMetadata,
}

impl EvmAdapter {
    pub fn new(metadata: EvmAdapterMetadata) -> Self {
        Self { metadata }
    }

    pub fn metadata(&self) -> &EvmAdapterMetadata {
        &self.metadata
    }
}

impl ChainAdapter for EvmAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(ChainIdentity::expect_new(
            ChainFamily::Evm,
            "evm-unconfigured",
        ))
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "EVM adapter has no configured provider",
        ))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "EVM adapter has no configured provider",
        ))
    }

    fn fetch(&self, _request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "EVM adapter has no configured provider",
        ))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// EVM finality discovery strategy. Durable cache writes use the resulting
/// safe/finalized boundary; latest block height is never enough to authorize
/// manifest coverage.
pub enum EvmFinalityPolicy {
    #[default]
    Auto,
    Lag {
        safe_lag_blocks: Option<u64>,
        finalized_lag_blocks: Option<u64>,
    },
    RpcTags {
        safe_tag: String,
        finalized_tag: String,
    },
}

const DEFAULT_BLOCK_HEADER_CACHE_MAX_ENTRIES: usize = 50_000;
const DEFAULT_BLOCK_HEADER_FETCH_CONCURRENCY: usize = 8;
const DEFAULT_BLOCK_HEADER_BATCH_SIZE: usize = 20;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvmBlockHeaderFetchMode {
    #[default]
    Concurrent,
    Batch,
}

impl EvmBlockHeaderFetchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Concurrent => "concurrent",
            Self::Batch => "batch",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmBlockHeaderMetadataConfig {
    pub cache_max_entries: usize,
    pub fetch_concurrency: usize,
    pub fetch_mode: EvmBlockHeaderFetchMode,
    pub batch_size: usize,
}

impl Default for EvmBlockHeaderMetadataConfig {
    fn default() -> Self {
        Self {
            cache_max_entries: DEFAULT_BLOCK_HEADER_CACHE_MAX_ENTRIES,
            fetch_concurrency: DEFAULT_BLOCK_HEADER_FETCH_CONCURRENCY,
            fetch_mode: EvmBlockHeaderFetchMode::Concurrent,
            batch_size: DEFAULT_BLOCK_HEADER_BATCH_SIZE,
        }
    }
}

impl EvmBlockHeaderMetadataConfig {
    pub fn with_cache_max_entries(mut self, cache_max_entries: usize) -> Self {
        self.cache_max_entries = cache_max_entries.max(1);
        self
    }

    pub fn with_fetch_concurrency(mut self, fetch_concurrency: usize) -> Self {
        self.fetch_concurrency = fetch_concurrency.max(1);
        self
    }

    pub fn with_fetch_mode(mut self, fetch_mode: EvmBlockHeaderFetchMode) -> Self {
        self.fetch_mode = fetch_mode;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BlockHeaderCacheKey {
    chain_key: String,
    number: u64,
    hash: String,
}

#[derive(Clone, Debug, Default)]
struct BlockHeaderCache {
    headers: BTreeMap<BlockHeaderCacheKey, BlockHeader>,
    insertion_order: VecDeque<BlockHeaderCacheKey>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LogBlockHeaderDiagnostics {
    cache_hits: usize,
    cache_misses: usize,
    provider_calls: usize,
    fetch_duration_ms: u128,
    fetch_mode: EvmBlockHeaderFetchMode,
    fetch_concurrency: usize,
    batch_size: usize,
}

impl LogBlockHeaderDiagnostics {
    fn warning(&self) -> String {
        format!(
            "evm log header metadata header_cache_hits={} header_cache_misses={} header_provider_calls={} header_fetch_duration_ms={} header_fetch_mode={} header_fetch_concurrency={} header_batch_size={}",
            self.cache_hits,
            self.cache_misses,
            self.provider_calls,
            self.fetch_duration_ms,
            self.fetch_mode.as_str(),
            self.fetch_concurrency,
            self.batch_size
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogBlockHeaderRequest {
    number: u64,
    hash: String,
}

#[derive(Clone)]
pub struct EvmRpcClient {
    rpc_urls: Vec<String>,
    client: Client,
    chain: ChainIdentity,
    finality_policy: EvmFinalityPolicy,
    max_block_batch_blocks: u64,
    max_get_logs_range_blocks: u64,
    max_block_scan_range_blocks: u64,
    max_addresses_per_query: usize,
    logs_query_strategy: QueryStrategy,
    block_header_metadata: EvmBlockHeaderMetadataConfig,
    block_header_cache: Arc<Mutex<BlockHeaderCache>>,
}

impl EvmRpcClient {
    pub fn new(rpc_urls: Vec<String>) -> Self {
        Self {
            rpc_urls,
            client: Client::new(),
            chain: ChainIdentity::expect_new(ChainFamily::Evm, "evm-unconfigured"),
            finality_policy: EvmFinalityPolicy::Auto,
            max_block_batch_blocks: u64::MAX,
            max_get_logs_range_blocks: u64::MAX,
            max_block_scan_range_blocks: u64::MAX,
            max_addresses_per_query: usize::MAX,
            logs_query_strategy: QueryStrategy::ProviderFilter,
            block_header_metadata: EvmBlockHeaderMetadataConfig::default(),
            block_header_cache: Arc::new(Mutex::new(BlockHeaderCache::default())),
        }
    }

    pub fn with_chain(
        rpc_urls: Vec<String>,
        chain: ChainIdentity,
        finality_policy: EvmFinalityPolicy,
        max_block_batch_blocks: u64,
        max_get_logs_range_blocks: u64,
        max_block_scan_range_blocks: u64,
        max_addresses_per_query: usize,
    ) -> Self {
        Self {
            rpc_urls,
            client: Client::new(),
            chain,
            finality_policy,
            max_block_batch_blocks,
            max_get_logs_range_blocks,
            max_block_scan_range_blocks,
            max_addresses_per_query,
            logs_query_strategy: QueryStrategy::ProviderFilter,
            block_header_metadata: EvmBlockHeaderMetadataConfig::default(),
            block_header_cache: Arc::new(Mutex::new(BlockHeaderCache::default())),
        }
    }

    pub fn with_logs_query_strategy(mut self, query_strategy: QueryStrategy) -> Self {
        self.logs_query_strategy = query_strategy;
        self
    }

    pub fn with_block_header_metadata_config(
        mut self,
        config: EvmBlockHeaderMetadataConfig,
    ) -> Self {
        self.block_header_metadata = config;
        self.block_header_cache = Arc::new(Mutex::new(BlockHeaderCache::default()));
        self
    }

    pub fn block_header_metadata_config(&self) -> &EvmBlockHeaderMetadataConfig {
        &self.block_header_metadata
    }

    pub fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        log::info!(
            "fetching EVM blocks range={}-{}",
            range.from_block,
            range.to_block
        );
        let mut blocks = Vec::new();
        for number in range.from_block..=range.to_block {
            let result = self.call(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )?;
            let Some(block) = result else {
                log::warn!("provider returned null block for {number}");
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("provider returned null block for {number}"),
                ));
            };
            blocks.push(BlockHeader {
                number,
                hash: string_field(&block, "hash")?,
                parent_hash: string_field(&block, "parentHash")?,
                timestamp: hex_u64_field(&block, "timestamp")?,
            });
        }
        Ok(blocks)
    }

    pub fn fetch_transactions(
        &self,
        range: BlockRange,
    ) -> Result<Vec<EvmTransaction>, DatalensError> {
        let mut rows = Vec::new();
        for block in self.fetch_full_blocks(range)? {
            let block_number = hex_u64_field(&block, "number")?;
            let block_hash = string_field(&block, "hash")?;
            let transactions = block
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "missing full block transactions",
                    )
                })?;
            for transaction in transactions {
                rows.push(parse_transaction(transaction, block_number, &block_hash)?);
            }
        }
        rows.sort_by_key(|row| (row.block_number, row.transaction_index));
        Ok(rows)
    }

    pub fn fetch_receipts(&self, range: BlockRange) -> Result<Vec<EvmReceipt>, DatalensError> {
        let mut rows = Vec::new();
        for transaction in self.fetch_transactions(range)? {
            let result = self.call("eth_getTransactionReceipt", json!([transaction.hash]))?;
            let Some(receipt) = result else {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "provider returned null receipt",
                ));
            };
            rows.push(parse_receipt(&receipt)?);
        }
        rows.sort_by_key(|row| (row.block_number, row.transaction_index));
        Ok(rows)
    }

    pub fn fetch_logs(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        let filter = EvmLogFilter::try_from(filter)?;
        self.fetch_evm_logs(range, &filter)
    }

    pub fn fetch_evm_logs(
        &self,
        range: BlockRange,
        filter: &EvmLogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        self.fetch_evm_logs_with_request_id(range, filter, None)
            .map(|(logs, _diagnostics)| logs)
    }

    fn fetch_evm_logs_with_request_id(
        &self,
        range: BlockRange,
        filter: &EvmLogFilter,
        request_id: Option<&str>,
    ) -> Result<(Vec<LogRecord>, LogBlockHeaderDiagnostics), DatalensError> {
        match request_id {
            Some(request_id) => log::info!(
                "fetching EVM logs request_id={} range={}-{} addresses={} topic_slots={}",
                request_id,
                range.from_block,
                range.to_block,
                filter.addresses().len(),
                filter.topics().len()
            ),
            None => log::info!(
                "fetching EVM logs range={}-{} addresses={} topic_slots={}",
                range.from_block,
                range.to_block,
                filter.addresses().len(),
                filter.topics().len()
            ),
        }
        let result = self.call("eth_getLogs", json!([evm_log_filter(range, filter)]))?;
        let logs = result
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| {
                log::warn!("provider returned invalid eth_getLogs result");
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "invalid eth_getLogs result",
                )
            })?;

        let logs = logs
            .into_iter()
            .map(|log| parse_log_record(&log))
            .collect::<Result<Vec<_>, _>>()?;
        let (mut logs, diagnostics) = self.enrich_logs_with_block_metadata(logs)?;
        log::info!("{}", diagnostics.warning());
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        Ok((logs, diagnostics))
    }

    fn fetch_evm_logs_from_receipts_with_request_id(
        &self,
        range: BlockRange,
        filter: &EvmLogFilter,
        request_id: Option<&str>,
    ) -> Result<(Vec<LogRecord>, usize), DatalensError> {
        match request_id {
            Some(request_id) => log::info!(
                "fetching EVM logs with block scan request_id={} range={}-{} addresses={} topic_slots={}",
                request_id,
                range.from_block,
                range.to_block,
                filter.addresses().len(),
                filter.topics().len()
            ),
            None => log::info!(
                "fetching EVM logs with block scan range={}-{} addresses={} topic_slots={}",
                range.from_block,
                range.to_block,
                filter.addresses().len(),
                filter.topics().len()
            ),
        }
        let mut logs = Vec::new();
        let mut provider_calls = 0;
        for block in self.fetch_full_blocks(range)? {
            provider_calls += 1;
            let block_number = hex_u64_field(&block, "number")?;
            let block_hash = string_field(&block, "hash")?;
            let parent_hash = string_field(&block, "parentHash")?;
            let block_timestamp = hex_u64_field(&block, "timestamp")?;
            let transactions = block
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "missing full block transactions",
                    )
                })?;
            for transaction in transactions {
                let transaction = parse_transaction(transaction, block_number, &block_hash)?;
                let result = self.call("eth_getTransactionReceipt", json!([transaction.hash]))?;
                provider_calls += 1;
                let Some(receipt) = result else {
                    return Err(DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "provider returned null receipt",
                    ));
                };
                let receipt_logs =
                    receipt
                        .get("logs")
                        .and_then(Value::as_array)
                        .ok_or_else(|| {
                            DatalensError::new(
                                DatalensErrorKind::ProviderFailure,
                                "missing receipt logs",
                            )
                        })?;
                for log in receipt_logs {
                    let log = parse_log_record(log)?
                        .with_block_metadata(Some(parent_hash.clone()), Some(block_timestamp));
                    if log_matches_filter(&log, filter) {
                        logs.push(log);
                    }
                }
            }
        }
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        Ok((logs, provider_calls))
    }

    fn finality_tag_height(
        &self,
        tag: &str,
        finality: FinalityKind,
    ) -> Result<ChainHeight, DatalensError> {
        let result = self.call("eth_getBlockByNumber", json!([tag, false]))?;
        let Some(block) = result else {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("provider returned no block for finality tag {tag}"),
            ));
        };
        let height = hex_u64_field(&block, "number")?;
        Ok(ChainHeight::block(height).with_finality(finality))
    }

    fn call(&self, method: &str, params: Value) -> Result<Option<Value>, DatalensError> {
        let url = self.rpc_urls.first().ok_or_else(|| {
            DatalensError::new(DatalensErrorKind::InvalidInput, "chain has no rpc_urls")
        })?;
        log::debug!("sending EVM provider request method={method}");
        let response = self
            .client
            .post(url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .map_err(|error| {
                let error = classify_transport_error(error, url);
                log::warn!(
                    "provider transport failed method={method} kind={:?}",
                    error.kind
                );
                error
            })?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            log::warn!("failed to decode provider response method={method}: {error}");
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "decode JSON-RPC response: {}",
                    redact_urls_in_text(&error.to_string())
                ),
            )
        })?;
        if !status.is_success() {
            let error = classify_provider_error(status.as_u16() as i64, &body.to_string());
            log::warn!(
                "provider returned HTTP error method={method} status={} kind={:?}",
                status.as_u16(),
                error.kind
            );
            return Err(error);
        }
        if let Some(error) = body.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("provider error");
            let error = classify_provider_error(code, message);
            log::warn!(
                "provider returned JSON-RPC error method={method} code={code} kind={:?}",
                error.kind
            );
            return Err(error);
        }
        Ok(body.get("result").cloned())
    }

    fn batch_call(&self, method: &str, requests: Vec<Value>) -> Result<Vec<Value>, DatalensError> {
        let url = self.rpc_urls.first().ok_or_else(|| {
            DatalensError::new(DatalensErrorKind::InvalidInput, "chain has no rpc_urls")
        })?;
        log::debug!(
            "sending EVM provider batch request method={method} requests={}",
            requests.len()
        );
        let response = self
            .client
            .post(url)
            .json(&requests)
            .send()
            .map_err(|error| {
                let error = classify_transport_error(error, url);
                log::warn!(
                    "provider batch transport failed method={method} kind={:?}",
                    error.kind
                );
                error
            })?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            log::warn!("failed to decode provider batch response method={method}: {error}");
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "decode JSON-RPC batch response: {}",
                    redact_urls_in_text(&error.to_string())
                ),
            )
        })?;
        if !status.is_success() {
            let error = classify_provider_error(status.as_u16() as i64, &body.to_string());
            log::warn!(
                "provider returned HTTP batch error method={method} status={} kind={:?}",
                status.as_u16(),
                error.kind
            );
            return Err(error);
        }
        body.as_array().cloned().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "invalid JSON-RPC batch response",
            )
        })
    }
}

impl ChainAdapter for EvmRpcClient {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone())
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Blocks)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(self.max_block_batch_blocks)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Transactions)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(self.max_block_batch_blocks)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Receipts)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(self.max_block_batch_blocks)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::All)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(match self.logs_query_strategy {
                        QueryStrategy::ProviderFilter => self.max_get_logs_range_blocks,
                        QueryStrategy::BlockRange => self.max_block_scan_range_blocks,
                    })
                    .with_max_addresses_per_query(self.max_addresses_per_query)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        let result = self.call("eth_blockNumber", json!([]))?;
        let value = result
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "invalid eth_blockNumber result",
                )
            })?;
        let height = u64::from_str_radix(value.trim_start_matches("0x"), 16).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("invalid eth_blockNumber result: {error}"),
            )
        })?;
        Ok(ChainHeight::block(height))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        match &self.finality_policy {
            EvmFinalityPolicy::Auto => self
                .rpc_finality_height("finalized", "safe")
                .or_else(|error| {
                    // Some EVM providers do not implement safe/finalized tags;
                    // configured lag is the durable boundary fallback.
                    if is_finality_tag_unsupported(&error) {
                        let profile = chain_profile(self.chain.network_id()).ok_or_else(|| {
                            DatalensError::new(
                                DatalensErrorKind::InvalidInput,
                                "unable to determine EVM finality: RPC finality tags are unsupported and chain profile has no lag fallback; configure [chains.<name>.finality] mode = \"lag\"",
                            )
                        })?;
                        self.lag_finality_height(&profile)
                    } else {
                        Err(error)
                    }
                }),
            EvmFinalityPolicy::Lag {
                safe_lag_blocks,
                finalized_lag_blocks,
            } => self.lag_finality_height(&LagFinalityPolicy {
                safe_lag_blocks: *safe_lag_blocks,
                finalized_lag_blocks: *finalized_lag_blocks,
            }),
            EvmFinalityPolicy::RpcTags {
                safe_tag,
                finalized_tag,
            } => self.rpc_finality_height(finalized_tag, safe_tag),
        }
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        match &self.finality_policy {
            EvmFinalityPolicy::Auto => {
                self.finality_tag_height("finalized", FinalityKind::Finalized)
            }
            EvmFinalityPolicy::Lag {
                finalized_lag_blocks,
                ..
            } => self.lag_finality_height(&LagFinalityPolicy {
                safe_lag_blocks: None,
                finalized_lag_blocks: *finalized_lag_blocks,
            }),
            EvmFinalityPolicy::RpcTags { finalized_tag, .. } => {
                self.finality_tag_height(finalized_tag, FinalityKind::Finalized)
            }
        }
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        if request.chain != self.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "request chain is not supported by adapter",
            ));
        }
        if request.range_kind != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "only block canonical lookup is supported",
            ));
        }
        let block = self.fetch_block_by_tag(&format!("0x{:x}", request.height))?;
        Ok(CanonicalBlock {
            chain: request.chain,
            height: block.number,
            hash: block.hash,
            parent_hash: block.parent_hash,
            finality: FinalityKind::Latest,
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
                "only block reorg signals are supported",
            ));
        }
        let block = self.fetch_block_by_tag(&format!("0x{height:x}"))?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn latest_reorg_signal(&self) -> Result<ReorgSignal, DatalensError> {
        let block = self.fetch_block_by_tag("latest")?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        if request.chain != self.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "request chain is not supported by adapter",
            ));
        }
        let range = request.range.block_range().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "only block ranges are supported",
            )
        })?;
        let capability = self
            .capabilities()
            .dataset(&request.dataset_key)
            .cloned()
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by EVM adapter",
                )
            })?;
        if let Some(max_range_len) = capability.max_range_len()
            && range.len() > u128::from(max_range_len)
        {
            // The adapter reports provider limits instead of splitting here so
            // planners, indexers, and warmup can account for the split policy.
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderLimit,
                if request.dataset_key == DatasetKey::evm_logs()
                    && self.logs_query_strategy == QueryStrategy::BlockRange
                {
                    "request range exceeds EVM block scan range limit"
                } else {
                    "request range exceeds EVM provider range limit"
                },
            ));
        }
        if let DatasetSelector::EvmLogs(filter) = &request.selector {
            // Selector limits are checked against normalized filters so storage
            // fingerprints and provider requests agree about the same selector.
            if let Some(max_addresses) = capability.max_addresses_per_query()
                && filter.addresses().len() > max_addresses
            {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderLimit,
                    "request address count exceeds EVM provider limit",
                ));
            }
            if let Some(max_topics) = capability.max_topics_per_query()
                && filter.topics().len() > max_topics
            {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderLimit,
                    "request topic count exceeds EVM provider limit",
                ));
            }
        }
        let mut warnings = Vec::new();
        let request_id = request.context.request_id.clone();
        let (rows, provider_calls) = match (&request.dataset_key, &request.selector) {
            (dataset, DatasetSelector::All) if *dataset == DatasetKey::evm_blocks() => (
                QueryRows::EvmBlocks(self.fetch_blocks(range)?),
                range.len().min(usize::MAX as u128) as usize,
            ),
            (dataset, DatasetSelector::All) if *dataset == DatasetKey::evm_transactions() => (
                QueryRows::EvmTransactions(self.fetch_transactions(range)?),
                range.len().min(usize::MAX as u128) as usize,
            ),
            (dataset, DatasetSelector::All) if *dataset == DatasetKey::evm_receipts() => {
                let receipts = self.fetch_receipts(range)?;
                let provider_calls = range.len().min(usize::MAX as u128) as usize + receipts.len();
                (QueryRows::EvmReceipts(receipts), provider_calls)
            }
            (dataset, DatasetSelector::All) if *dataset == DatasetKey::evm_logs() => {
                let filter = EvmLogFilter::try_from(LogFilter {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                })?;
                match self.logs_query_strategy {
                    QueryStrategy::ProviderFilter => {
                        let (mut logs, diagnostics) = self.fetch_evm_logs_with_request_id(
                            range,
                            &filter,
                            request_id.as_deref(),
                        )?;
                        let provider_calls = 1 + diagnostics.provider_calls;
                        warnings.push(diagnostics.warning());
                        logs.retain(|log| range.contains(log.block_number));
                        logs.sort_by_key(|log| {
                            (log.block_number, log.transaction_index, log.log_index)
                        });
                        (QueryRows::EvmLogs(logs), provider_calls)
                    }
                    QueryStrategy::BlockRange => {
                        warnings.push("evm block_range log query strategy used".to_owned());
                        let (logs, calls) = self.fetch_evm_logs_from_receipts_with_request_id(
                            range,
                            &filter,
                            request_id.as_deref(),
                        )?;
                        (QueryRows::EvmLogs(logs), calls)
                    }
                }
            }
            (dataset, DatasetSelector::EvmLogs(filter)) if *dataset == DatasetKey::evm_logs() => {
                match self.logs_query_strategy {
                    QueryStrategy::ProviderFilter => {
                        let (mut logs, diagnostics) = self.fetch_evm_logs_with_request_id(
                            range,
                            filter,
                            request_id.as_deref(),
                        )?;
                        let provider_calls = 1 + diagnostics.provider_calls;
                        warnings.push(diagnostics.warning());
                        logs.retain(|log| range.contains(log.block_number));
                        logs.sort_by_key(|log| {
                            (log.block_number, log.transaction_index, log.log_index)
                        });
                        (QueryRows::EvmLogs(logs), provider_calls)
                    }
                    QueryStrategy::BlockRange => {
                        warnings.push("evm block_range log query strategy used".to_owned());
                        let (logs, calls) = self.fetch_evm_logs_from_receipts_with_request_id(
                            range,
                            filter,
                            request_id.as_deref(),
                        )?;
                        (QueryRows::EvmLogs(logs), calls)
                    }
                }
            }
            (dataset, _) if *dataset == DatasetKey::evm_blocks() => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "blocks require all selector",
                ));
            }
            (dataset, _) if *dataset == DatasetKey::evm_transactions() => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "transactions require all selector",
                ));
            }
            (dataset, _) if *dataset == DatasetKey::evm_receipts() => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "receipts require all selector",
                ));
            }
            (dataset, _) if *dataset == DatasetKey::evm_logs() => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "logs require evm logs selector",
                ));
            }
            _ => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by EVM adapter",
                ));
            }
        };
        if let Some(request_id) = &request_id {
            log::info!(
                "evm adapter fetch request_id={} chain={} dataset={} range={}-{} provider_calls={} rows={}",
                request_id,
                request.chain.configured_name(),
                request.dataset_key.as_str(),
                request.range.start(),
                request.range.end(),
                provider_calls,
                rows.row_count()
            );
        } else {
            log::info!(
                "evm adapter fetch chain={} dataset={} range={}-{} provider_calls={} rows={}",
                request.chain.configured_name(),
                request.dataset_key.as_str(),
                request.range.start(),
                request.range.end(),
                provider_calls,
                rows.row_count()
            );
        }
        Ok(ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            LedgerRange::from_block_range(range),
            request.selector,
            rows,
        )?
        .with_source_metadata(datalens_chain::SourceMetadata {
            provider: "evm-rpc".to_owned(),
            request_id,
        })
        .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
            calls: provider_calls,
            rows_scanned: 0,
            warnings,
        }))
    }
}

fn log_matches_filter(log: &LogRecord, filter: &EvmLogFilter) -> bool {
    if !filter.addresses().is_empty() && !filter.addresses().contains(&log.address) {
        return false;
    }
    for (slot, topic_filter) in filter.topics().iter().enumerate() {
        match topic_filter {
            TopicFilter::Wildcard => {}
            TopicFilter::AnyOf(values) => {
                let Some(topic) = log.topics.get(slot) else {
                    return false;
                };
                if !values.contains(topic) {
                    return false;
                }
            }
        }
    }
    true
}

impl EvmRpcClient {
    fn fetch_full_blocks(&self, range: BlockRange) -> Result<Vec<Value>, DatalensError> {
        let mut blocks = Vec::new();
        for number in range.from_block..=range.to_block {
            let result = self.call(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), true]),
            )?;
            let Some(block) = result else {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("provider returned null block for {number}"),
                ));
            };
            blocks.push(block);
        }
        Ok(blocks)
    }

    fn enrich_logs_with_block_metadata(
        &self,
        logs: Vec<LogRecord>,
    ) -> Result<(Vec<LogRecord>, LogBlockHeaderDiagnostics), DatalensError> {
        let started_at = Instant::now();
        let mut diagnostics = LogBlockHeaderDiagnostics {
            fetch_mode: self.block_header_metadata.fetch_mode,
            fetch_concurrency: self.block_header_metadata.fetch_concurrency,
            batch_size: self.block_header_metadata.batch_size,
            ..LogBlockHeaderDiagnostics::default()
        };
        if logs.is_empty() {
            return Ok((logs, diagnostics));
        }

        let mut headers = BTreeMap::new();
        let mut missing = BTreeMap::new();
        {
            let cache = self.block_header_cache.lock().expect("block header cache");
            for log in &logs {
                let key = self.block_header_cache_key(log.block_number, &log.block_hash);
                if headers.contains_key(&key) || missing.contains_key(&key) {
                    continue;
                }
                if let Some(header) = cache.headers.get(&key) {
                    diagnostics.cache_hits += 1;
                    headers.insert(key, header.clone());
                } else {
                    diagnostics.cache_misses += 1;
                    missing.insert(
                        key,
                        LogBlockHeaderRequest {
                            number: log.block_number,
                            hash: log.block_hash.clone(),
                        },
                    );
                }
            }
        }

        let missing = missing.into_values().collect::<Vec<_>>();
        if !missing.is_empty() {
            let (fetched, provider_calls) = match self.block_header_metadata.fetch_mode {
                EvmBlockHeaderFetchMode::Concurrent => {
                    self.fetch_log_block_headers_concurrently(missing)?
                }
                EvmBlockHeaderFetchMode::Batch => {
                    self.fetch_log_block_headers_in_batches(missing)?
                }
            };
            diagnostics.provider_calls = provider_calls;
            self.insert_block_headers_into_cache(&fetched);
            headers.extend(fetched);
        }
        diagnostics.fetch_duration_ms = started_at.elapsed().as_millis();

        let logs = logs
            .into_iter()
            .map(|log| {
                let key = self.block_header_cache_key(log.block_number, &log.block_hash);
                let header = headers.get(&key).expect("header inserted for log block");
                self.validate_log_block_header(&log, header)?;
                Ok(log
                    .with_block_metadata(Some(header.parent_hash.clone()), Some(header.timestamp)))
            })
            .collect::<Result<Vec<_>, DatalensError>>()?;
        Ok((logs, diagnostics))
    }

    fn block_header_cache_key(&self, number: u64, hash: &str) -> BlockHeaderCacheKey {
        BlockHeaderCacheKey {
            chain_key: self.chain.key_prefix(),
            number,
            hash: hash.to_owned(),
        }
    }

    fn insert_block_headers_into_cache(
        &self,
        headers: &BTreeMap<BlockHeaderCacheKey, BlockHeader>,
    ) {
        if headers.is_empty() {
            return;
        }
        let mut cache = self.block_header_cache.lock().expect("block header cache");
        for (key, header) in headers {
            if cache.headers.contains_key(key) {
                cache.headers.insert(key.clone(), header.clone());
                continue;
            }
            cache.insertion_order.push_back(key.clone());
            cache.headers.insert(key.clone(), header.clone());
        }
        while cache.headers.len() > self.block_header_metadata.cache_max_entries {
            let Some(key) = cache.insertion_order.pop_front() else {
                break;
            };
            cache.headers.remove(&key);
        }
    }

    fn fetch_log_block_headers_concurrently(
        &self,
        missing: Vec<LogBlockHeaderRequest>,
    ) -> Result<(BTreeMap<BlockHeaderCacheKey, BlockHeader>, usize), DatalensError> {
        let provider_calls = missing.len();
        let concurrency = self
            .block_header_metadata
            .fetch_concurrency
            .min(missing.len())
            .max(1);
        let work = Arc::new(Mutex::new(VecDeque::from(missing)));
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let client = self.clone();
            let work = Arc::clone(&work);
            handles.push(thread::spawn(move || {
                let mut headers = BTreeMap::new();
                loop {
                    let request = work.lock().expect("header work").pop_front();
                    let Some(request) = request else {
                        break;
                    };
                    let header = client.fetch_log_block_header(&request)?;
                    headers.insert(
                        client.block_header_cache_key(request.number, &request.hash),
                        header,
                    );
                }
                Ok::<_, DatalensError>(headers)
            }));
        }

        let mut headers = BTreeMap::new();
        for handle in handles {
            let fetched = handle.join().map_err(|_| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    "EVM block header worker panicked",
                )
            })??;
            headers.extend(fetched);
        }
        Ok((headers, provider_calls))
    }

    fn fetch_log_block_headers_in_batches(
        &self,
        missing: Vec<LogBlockHeaderRequest>,
    ) -> Result<(BTreeMap<BlockHeaderCacheKey, BlockHeader>, usize), DatalensError> {
        let mut headers = BTreeMap::new();
        let mut provider_calls = 0;
        for chunk in missing.chunks(self.block_header_metadata.batch_size) {
            provider_calls += 1;
            let fetched = self.fetch_log_block_header_batch(chunk)?;
            headers.extend(fetched);
        }
        Ok((headers, provider_calls))
    }

    fn fetch_log_block_header(
        &self,
        request: &LogBlockHeaderRequest,
    ) -> Result<BlockHeader, DatalensError> {
        let header = self
            .fetch_block_by_tag(&format!("0x{:x}", request.number))
            .map_err(|error| {
                DatalensError::new(
                    error.kind,
                    format!(
                        "failed to fetch block header for log block {}: {}",
                        request.number, error.message
                    ),
                )
            })?;
        self.validate_log_block_header_request(request, &header)?;
        Ok(header)
    }

    fn fetch_log_block_header_batch(
        &self,
        requests: &[LogBlockHeaderRequest],
    ) -> Result<BTreeMap<BlockHeaderCacheKey, BlockHeader>, DatalensError> {
        let rpc_requests = requests
            .iter()
            .enumerate()
            .map(|(index, request)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": index + 1,
                    "method": "eth_getBlockByNumber",
                    "params": [format!("0x{:x}", request.number), false],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.batch_call("eth_getBlockByNumber", rpc_requests)?;
        let request_by_id = requests
            .iter()
            .enumerate()
            .map(|(index, request)| ((index + 1) as u64, request))
            .collect::<BTreeMap<_, _>>();
        let mut response_by_id = BTreeMap::new();
        for response in responses {
            let id = response.get("id").and_then(Value::as_u64).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "batch response missing numeric id",
                )
            })?;
            response_by_id.insert(id, response);
        }

        let mut headers = BTreeMap::new();
        for (id, request) in request_by_id {
            let response = response_by_id.get(&id).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("missing batch response for log block {}", request.number),
                )
            })?;
            if let Some(error) = response.get("error") {
                let code = error
                    .get("code")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("provider error");
                let error = classify_provider_error(code, message);
                return Err(DatalensError::new(
                    error.kind,
                    format!(
                        "failed to fetch block header for log block {}: {}",
                        request.number, error.message
                    ),
                ));
            }
            let Some(block) = response.get("result") else {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "batch response missing result for log block {}",
                        request.number
                    ),
                ));
            };
            if block.is_null() {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    format!(
                        "provider returned no block for log block {}",
                        request.number
                    ),
                ));
            }
            let header = Self::parse_block_header(block)?;
            self.validate_log_block_header_request(request, &header)?;
            headers.insert(
                self.block_header_cache_key(request.number, &request.hash),
                header,
            );
        }
        Ok(headers)
    }

    fn validate_log_block_header_request(
        &self,
        request: &LogBlockHeaderRequest,
        header: &BlockHeader,
    ) -> Result<(), DatalensError> {
        if header.number != request.number {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "fetched block number {} does not match log block {}",
                    header.number, request.number
                ),
            ));
        }
        if header.hash != request.hash {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "log block hash {} does not match fetched block hash {} for block {}",
                    request.hash, header.hash, request.number
                ),
            ));
        }
        Ok(())
    }

    fn validate_log_block_header(
        &self,
        log: &LogRecord,
        header: &BlockHeader,
    ) -> Result<(), DatalensError> {
        self.validate_log_block_header_request(
            &LogBlockHeaderRequest {
                number: log.block_number,
                hash: log.block_hash.clone(),
            },
            header,
        )
    }

    fn fetch_block_by_tag(&self, tag: &str) -> Result<BlockHeader, DatalensError> {
        let result = self.call("eth_getBlockByNumber", json!([tag, false]))?;
        let Some(block) = result else {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("provider returned no block for tag {tag}"),
            ));
        };
        Self::parse_block_header(&block)
    }

    fn parse_block_header(block: &Value) -> Result<BlockHeader, DatalensError> {
        Ok(BlockHeader {
            number: hex_u64_field(block, "number")?,
            hash: string_field(block, "hash")?,
            parent_hash: string_field(block, "parentHash")?,
            timestamp: hex_u64_field(block, "timestamp")?,
        })
    }

    fn rpc_finality_height(
        &self,
        finalized_tag: &str,
        safe_tag: &str,
    ) -> Result<ChainHeight, DatalensError> {
        match self.finality_tag_height(finalized_tag, FinalityKind::Finalized) {
            Ok(height) => Ok(height),
            Err(error) if is_finality_tag_unsupported(&error) => {
                self.finality_tag_height(safe_tag, FinalityKind::Safe)
            }
            Err(error) => Err(error),
        }
    }

    fn lag_finality_height(
        &self,
        policy: &LagFinalityPolicy,
    ) -> Result<ChainHeight, DatalensError> {
        let latest = self.latest_height()?.value;
        if let Some(lag) = policy.finalized_lag_blocks {
            if lag == 0 {
                return Err(zero_lag_error());
            }
            return Ok(ChainHeight::block(height_from_latest_lag(latest, lag))
                .with_finality(FinalityKind::Finalized));
        }
        if let Some(lag) = policy.safe_lag_blocks {
            if lag == 0 {
                return Err(zero_lag_error());
            }
            return Ok(ChainHeight::block(height_from_latest_lag(latest, lag))
                .with_finality(FinalityKind::Safe));
        }
        Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "lag finality policy must define safe_lag_blocks or finalized_lag_blocks",
        ))
    }
}
