use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use datalens_chain::{
    AdapterCapabilities, CanonicalBlock, CanonicalBlockRequest, ChainAdapter, ChainFetchRequest,
    ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector, FinalityKind,
    FinalityLevel, HeightRangeKind, ReorgSignal, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, EvmBlockHeader, EvmLogFilter, EvmReceipt, EvmTransaction, LedgerRange, LogFilter,
    LogRecord, QueryRows, QueryStrategy, TopicFilter, redact_urls_in_text,
};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::provider_payload::{
    LagFinalityPolicy, chain_profile, classify_transport_error, evm_log_filter, hex_u64_field,
    is_block_receipts_unsupported, is_finality_tag_unsupported, string_field, zero_lag_error,
};
pub use crate::provider_payload::{
    classify_provider_error, height_from_latest_lag, parse_log_record, parse_receipt,
    parse_transaction,
};
use crate::{EvmBlockHeaderFetch, EvmBlockHeaderFetcher};
use crate::{EvmBlockHeaderResolveRequest, EvmBlockHeaderResolver, EvmLogBloom};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmLogReliabilityConfig {
    pub enabled: bool,
}

impl Default for EvmLogReliabilityConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl EvmLogReliabilityConfig {
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvmBlockHeaderFetchMode {
    Concurrent,
    #[default]
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
            fetch_mode: EvmBlockHeaderFetchMode::Batch,
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
    fallback_mode: Option<EvmBlockHeaderFetchMode>,
    fallback_reason: Option<String>,
    fetch_concurrency: usize,
    batch_size: usize,
    reliability_checked: bool,
    reliability_header_blocks: usize,
    reliability_suspicious_blocks: usize,
    reliability_secondary_calls: usize,
    reliability_receipt_fallback_calls: usize,
    reliability_receipt_recovered_blocks: usize,
    reliability_receipt_unresolved_blocks: usize,
    reliability_unrecovered_blocks: usize,
    reliability_recovery_available: bool,
}

impl LogBlockHeaderDiagnostics {
    fn warning(&self) -> String {
        let mut warning = format!(
            "evm log header metadata header_cache_hits={} header_cache_misses={} header_provider_calls={} header_fetch_duration_ms={} header_fetch_mode={} header_fetch_concurrency={} header_batch_size={}",
            self.cache_hits,
            self.cache_misses,
            self.provider_calls,
            self.fetch_duration_ms,
            self.fetch_mode.as_str(),
            self.fetch_concurrency,
            self.batch_size
        );
        if let Some(fallback_mode) = self.fallback_mode {
            warning.push_str(&format!(" header_fallback_mode={}", fallback_mode.as_str()));
        }
        if let Some(fallback_reason) = &self.fallback_reason {
            warning.push_str(&format!(" header_fallback_reason={fallback_reason}"));
        }
        if self.reliability_checked {
            warning.push_str(&format!(
                " reliability_header_blocks={} reliability_suspicious_blocks={} reliability_secondary_calls={} reliability_receipt_fallback_calls={} reliability_receipt_recovered_blocks={} reliability_receipt_unresolved_blocks={} reliability_unrecovered_blocks={} reliability_recovery_available={}",
                self.reliability_header_blocks,
                self.reliability_suspicious_blocks,
                self.reliability_secondary_calls,
                self.reliability_receipt_fallback_calls,
                self.reliability_receipt_recovered_blocks,
                self.reliability_receipt_unresolved_blocks,
                self.reliability_unrecovered_blocks,
                self.reliability_recovery_available
            ));
        }
        warning
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogBlockHeaderRequest {
    number: u64,
    hash: String,
}

#[derive(Clone, Debug, Default)]
struct LogBlockHeaderBatchFetch {
    headers: BTreeMap<BlockHeaderCacheKey, BlockHeader>,
    missing: Vec<LogBlockHeaderRequest>,
    fallback_reason: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct LogBlockHeaderFetch {
    headers: BTreeMap<BlockHeaderCacheKey, BlockHeader>,
    provider_calls: usize,
    fallback_reason: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LogReliabilityDiagnostics {
    header_blocks: usize,
    suspicious_blocks: usize,
    secondary_provider_calls: usize,
    receipt_fallback_calls: usize,
    receipt_recovered_blocks: usize,
    receipt_unresolved_blocks: usize,
    unrecovered_blocks: usize,
    recovery_available: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReceiptFallbackFetch {
    logs: Vec<LogRecord>,
    provider_calls: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReceiptFallbackError {
    error: DatalensError,
    provider_calls: usize,
    unresolved: bool,
}

impl ReceiptFallbackError {
    fn fatal(error: DatalensError, provider_calls: usize) -> Self {
        Self {
            error,
            provider_calls,
            unresolved: false,
        }
    }

    fn unresolved(error: DatalensError, provider_calls: usize) -> Self {
        Self {
            error,
            provider_calls,
            unresolved: true,
        }
    }
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
    log_reliability: EvmLogReliabilityConfig,
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
            log_reliability: EvmLogReliabilityConfig::default(),
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
            log_reliability: EvmLogReliabilityConfig::default(),
            block_header_cache: Arc::new(Mutex::new(BlockHeaderCache::default())),
        }
    }

    pub fn with_logs_query_strategy(mut self, query_strategy: QueryStrategy) -> Self {
        self.logs_query_strategy = query_strategy;
        self
    }

    pub fn with_log_reliability_config(mut self, config: EvmLogReliabilityConfig) -> Self {
        self.log_reliability = config;
        self
    }

    pub fn log_reliability_config(&self) -> &EvmLogReliabilityConfig {
        &self.log_reliability
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

    pub fn primary_provider_url(&self) -> Option<&str> {
        self.rpc_urls.first().map(String::as_str)
    }

    pub fn secondary_provider_urls(&self) -> &[String] {
        if self.rpc_urls.len() > 1 {
            &self.rpc_urls[1..]
        } else {
            &[]
        }
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

    pub fn fetch_evm_block_headers(
        &self,
        range: BlockRange,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        log::info!(
            "fetching EVM block headers range={}-{} mode={} batch_size={}",
            range.from_block,
            range.to_block,
            self.block_header_metadata.fetch_mode.as_str(),
            self.block_header_metadata.batch_size
        );
        match self.block_header_metadata.fetch_mode {
            EvmBlockHeaderFetchMode::Batch => self.fetch_evm_block_headers_in_batches(range),
            EvmBlockHeaderFetchMode::Concurrent => self.fetch_evm_block_headers_concurrently(range),
        }
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
        let logs = self.fetch_primary_logs(range, filter)?;
        let (logs, reliability_diagnostics) =
            self.verify_and_merge_logs_with_secondary(range, filter, logs)?;
        let (mut logs, mut diagnostics) = self.enrich_logs_with_block_metadata(logs)?;
        if let Some(reliability_diagnostics) = reliability_diagnostics {
            diagnostics.reliability_checked = true;
            diagnostics.reliability_header_blocks = reliability_diagnostics.header_blocks;
            diagnostics.reliability_suspicious_blocks = reliability_diagnostics.suspicious_blocks;
            diagnostics.reliability_secondary_calls =
                reliability_diagnostics.secondary_provider_calls;
            diagnostics.reliability_receipt_fallback_calls =
                reliability_diagnostics.receipt_fallback_calls;
            diagnostics.reliability_receipt_recovered_blocks =
                reliability_diagnostics.receipt_recovered_blocks;
            diagnostics.reliability_receipt_unresolved_blocks =
                reliability_diagnostics.receipt_unresolved_blocks;
            diagnostics.reliability_unrecovered_blocks = reliability_diagnostics.unrecovered_blocks;
            diagnostics.reliability_recovery_available = reliability_diagnostics.recovery_available;
        }
        log::info!("{}", diagnostics.warning());
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        Ok((logs, diagnostics))
    }

    fn fetch_primary_logs(
        &self,
        range: BlockRange,
        filter: &EvmLogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        self.fetch_logs_from_url(self.primary_rpc_url_result()?, range, filter)
    }

    fn fetch_logs_from_url(
        &self,
        url: &str,
        range: BlockRange,
        filter: &EvmLogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        let result = self.call_url(url, "eth_getLogs", json!([evm_log_filter(range, filter)]))?;
        let logs = result
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| {
                log::warn!("provider returned invalid eth_getLogs result");
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "invalid eth_getLogs result",
                )
            })?;

        logs.into_iter()
            .map(|log| parse_log_record(&log))
            .collect::<Result<Vec<_>, _>>()
    }

    fn verify_and_merge_logs_with_secondary(
        &self,
        range: BlockRange,
        filter: &EvmLogFilter,
        primary_logs: Vec<LogRecord>,
    ) -> Result<(Vec<LogRecord>, Option<LogReliabilityDiagnostics>), DatalensError> {
        if !self.log_reliability.enabled {
            return Ok((primary_logs, None));
        }

        let resolver = EvmBlockHeaderResolver::without_store(self.clone());
        let headers = resolver.resolve(EvmBlockHeaderResolveRequest {
            chain: self.chain.clone(),
            range,
            finality_level: FinalityLevel::Safe,
        })?;
        self.insert_evm_block_headers_into_cache(&headers);

        let mut suspicious_blocks = Vec::new();
        for header in &headers {
            if !range.contains(header.block_number) {
                continue;
            }
            if header_bloom_may_match_filter(header, filter)? {
                suspicious_blocks.push(header.block_number);
            }
        }

        let mut diagnostics = LogReliabilityDiagnostics {
            header_blocks: headers.len(),
            suspicious_blocks: suspicious_blocks.len(),
            secondary_provider_calls: 0,
            receipt_fallback_calls: 0,
            receipt_recovered_blocks: 0,
            receipt_unresolved_blocks: 0,
            unrecovered_blocks: 0,
            recovery_available: !self.secondary_provider_urls().is_empty(),
        };
        if suspicious_blocks.is_empty() {
            return Ok((primary_logs, Some(diagnostics)));
        }

        let header_by_number = headers
            .iter()
            .map(|header| (header.block_number, header.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut merged = primary_logs;
        if self.secondary_provider_urls().is_empty() {
            log::warn!(
                "EVM log reliability found {} bloom-positive block(s) but no secondary recovery provider is configured; trying receipt fallback",
                suspicious_blocks.len()
            );
        } else {
            for suspicious_range in adjacent_block_ranges(&suspicious_blocks) {
                let mut last_error = None;
                let mut range_logs = None;
                for url in self.secondary_provider_urls() {
                    diagnostics.secondary_provider_calls += 1;
                    match self.fetch_logs_from_url(url, suspicious_range, filter) {
                        Ok(logs) => {
                            range_logs = Some(logs);
                            break;
                        }
                        Err(error) => {
                            last_error = Some(error);
                        }
                    }
                }
                let Some(mut logs) = range_logs else {
                    let error = last_error.unwrap_or_else(|| {
                        DatalensError::new(
                            DatalensErrorKind::ProviderFailure,
                            "EVM log reliability secondary provider failed",
                        )
                    });
                    return Err(DatalensError::new(
                        error.kind,
                        format!(
                            "EVM log reliability secondary eth_getLogs failed for range {}-{}: {}",
                            suspicious_range.from_block, suspicious_range.to_block, error.message
                        ),
                    ));
                };
                logs.retain(|log| suspicious_range.contains(log.block_number));
                merged.append(&mut logs);
            }
        }

        dedupe_and_sort_logs(&mut merged);
        let unresolved_blocks = suspicious_blocks
            .iter()
            .copied()
            .filter(|block| {
                !merged
                    .iter()
                    .any(|log| log.block_number == *block && log_matches_filter(log, filter))
            })
            .collect::<Vec<_>>();
        for block_number in unresolved_blocks {
            let Some(header) = header_by_number.get(&block_number) else {
                diagnostics.receipt_unresolved_blocks += 1;
                continue;
            };
            match self.fetch_logs_from_receipt_fallback(header, filter) {
                Ok(mut fallback) => {
                    diagnostics.receipt_fallback_calls += fallback.provider_calls;
                    fallback.logs.retain(|log| log.block_number == block_number);
                    if fallback.logs.is_empty() {
                        diagnostics.receipt_unresolved_blocks += 1;
                        log::warn!(
                            "EVM log reliability receipt fallback found no matching rows block={block_number}"
                        );
                    } else {
                        diagnostics.receipt_recovered_blocks += 1;
                        log::warn!(
                            "EVM log reliability receipt fallback recovered {} row(s) block={block_number}",
                            fallback.logs.len()
                        );
                        merged.append(&mut fallback.logs);
                    }
                }
                Err(error) if error.unresolved => {
                    diagnostics.receipt_fallback_calls += error.provider_calls;
                    diagnostics.receipt_unresolved_blocks += 1;
                    log::warn!(
                        "EVM log reliability receipt fallback failed block={block_number} kind={:?}: {}",
                        error.error.kind,
                        error.error.message
                    );
                }
                Err(error) => {
                    return Err(DatalensError::new(
                        error.error.kind,
                        format!(
                            "EVM log reliability receipt fallback failed block={block_number} provider_calls={}: {}",
                            error.provider_calls, error.error.message
                        ),
                    ));
                }
            }
        }
        diagnostics.unrecovered_blocks = diagnostics.receipt_unresolved_blocks;
        diagnostics.recovery_available =
            diagnostics.recovery_available || diagnostics.receipt_fallback_calls > 0;
        dedupe_and_sort_logs(&mut merged);
        Ok((merged, Some(diagnostics)))
    }

    fn fetch_logs_from_receipt_fallback(
        &self,
        header: &EvmBlockHeader,
        filter: &EvmLogFilter,
    ) -> Result<ReceiptFallbackFetch, ReceiptFallbackError> {
        log::warn!(
            "EVM log reliability trying receipt fallback block={}",
            header.block_number
        );
        match self.call(
            "eth_getBlockReceipts",
            json!([format!("0x{:x}", header.block_number)]),
        ) {
            Ok(Some(value)) => {
                let receipts = value.as_array().ok_or_else(|| {
                    ReceiptFallbackError::fatal(
                        DatalensError::new(
                            DatalensErrorKind::ProviderFailure,
                            "invalid eth_getBlockReceipts result",
                        ),
                        1,
                    )
                })?;
                Ok(ReceiptFallbackFetch {
                    logs: receipt_logs_from_receipts(receipts, header, filter)
                        .map_err(|error| ReceiptFallbackError::fatal(error, 1))?,
                    provider_calls: 1,
                })
            }
            Ok(None) => {
                log::warn!(
                    "EVM log reliability eth_getBlockReceipts returned null block={}; falling back to transaction receipts",
                    header.block_number
                );
                self.fetch_logs_from_transaction_receipts(header, filter, 1)
            }
            Err(error) if is_block_receipts_unsupported(&error) => {
                log::warn!(
                    "EVM log reliability eth_getBlockReceipts unavailable block={} kind={:?}: {}; falling back to transaction receipts",
                    header.block_number,
                    error.kind,
                    error.message
                );
                self.fetch_logs_from_transaction_receipts(header, filter, 1)
            }
            Err(error) => Err(ReceiptFallbackError::fatal(error, 1)),
        }
    }

    fn fetch_logs_from_transaction_receipts(
        &self,
        header: &EvmBlockHeader,
        filter: &EvmLogFilter,
        provider_calls: usize,
    ) -> Result<ReceiptFallbackFetch, ReceiptFallbackError> {
        let mut provider_calls = provider_calls;
        provider_calls += 1;
        let result = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", header.block_number), false]),
            )
            .map_err(|error| {
                if is_block_receipts_unsupported(&error) {
                    ReceiptFallbackError::unresolved(error, provider_calls)
                } else {
                    ReceiptFallbackError::fatal(error, provider_calls)
                }
            })?;
        let Some(block) = result else {
            return Err(ReceiptFallbackError::fatal(
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("provider returned null block for {}", header.block_number),
                ),
                provider_calls,
            ));
        };
        let block_number = hex_u64_field(&block, "number")
            .map_err(|error| ReceiptFallbackError::fatal(error, provider_calls))?;
        if block_number != header.block_number {
            return Err(ReceiptFallbackError::fatal(
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "receipt fallback block number {} does not match requested block {}",
                        block_number, header.block_number
                    ),
                ),
                provider_calls,
            ));
        }
        let block_hash = string_field(&block, "hash")
            .map_err(|error| ReceiptFallbackError::fatal(error, provider_calls))?;
        if block_hash != header.block_hash {
            return Err(ReceiptFallbackError::fatal(
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "receipt fallback block hash {} does not match header hash {}",
                        block_hash, header.block_hash
                    ),
                ),
                provider_calls,
            ));
        }
        let transactions = block
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ReceiptFallbackError::fatal(
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "missing block transaction hashes",
                    ),
                    provider_calls,
                )
            })?;
        let mut logs = Vec::new();
        for transaction in transactions {
            let transaction_hash = match transaction {
                Value::String(hash) => hash.clone(),
                Value::Object(_) => string_field(transaction, "hash")
                    .map_err(|error| ReceiptFallbackError::fatal(error, provider_calls))?,
                _ => {
                    return Err(ReceiptFallbackError::fatal(
                        DatalensError::new(
                            DatalensErrorKind::ProviderFailure,
                            "invalid block transaction hash",
                        ),
                        provider_calls,
                    ));
                }
            };
            provider_calls += 1;
            let result = self
                .call("eth_getTransactionReceipt", json!([transaction_hash]))
                .map_err(|error| {
                    if is_block_receipts_unsupported(&error) {
                        ReceiptFallbackError::unresolved(error, provider_calls)
                    } else {
                        ReceiptFallbackError::fatal(error, provider_calls)
                    }
                })?;
            let Some(receipt) = result else {
                return Err(ReceiptFallbackError::fatal(
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "provider returned null receipt",
                    ),
                    provider_calls,
                ));
            };
            let receipt_logs = receipt
                .get("logs")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    ReceiptFallbackError::fatal(
                        DatalensError::new(
                            DatalensErrorKind::ProviderFailure,
                            "missing receipt logs",
                        ),
                        provider_calls,
                    )
                })?;
            logs.append(
                &mut receipt_logs_from_values(receipt_logs, header, filter)
                    .map_err(|error| ReceiptFallbackError::fatal(error, provider_calls))?,
            );
        }
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        Ok(ReceiptFallbackFetch {
            logs,
            provider_calls,
        })
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
        self.post_rpc(self.primary_rpc_url_result()?, method, params)
    }

    fn call_url(
        &self,
        url: &str,
        method: &str,
        params: Value,
    ) -> Result<Option<Value>, DatalensError> {
        self.post_rpc(url, method, params)
    }

    fn post_rpc(
        &self,
        url: &str,
        method: &str,
        params: Value,
    ) -> Result<Option<Value>, DatalensError> {
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
        let url = self.primary_rpc_url_result()?;
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

    fn primary_rpc_url_result(&self) -> Result<&str, DatalensError> {
        self.rpc_urls.first().map(String::as_str).ok_or_else(|| {
            DatalensError::new(DatalensErrorKind::InvalidInput, "chain has no rpc_urls")
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
                        let provider_calls = 1
                            + diagnostics.provider_calls
                            + diagnostics.reliability_secondary_calls
                            + diagnostics.reliability_receipt_fallback_calls;
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
                        let provider_calls = 1
                            + diagnostics.provider_calls
                            + diagnostics.reliability_secondary_calls
                            + diagnostics.reliability_receipt_fallback_calls;
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

impl EvmBlockHeaderFetcher for EvmRpcClient {
    fn fetch_block_headers(&self, range: BlockRange) -> Result<EvmBlockHeaderFetch, DatalensError> {
        Ok(EvmBlockHeaderFetch {
            range,
            headers: self.fetch_evm_block_headers(range)?,
        })
    }
}

fn receipt_logs_from_values(
    logs: &[Value],
    header: &EvmBlockHeader,
    filter: &EvmLogFilter,
) -> Result<Vec<LogRecord>, DatalensError> {
    let mut rows = Vec::new();
    for log in logs {
        let log = parse_log_record(log)?;
        if log.block_number != header.block_number || log.block_hash != header.block_hash {
            continue;
        }
        let log = log.with_block_metadata(Some(header.parent_hash.clone()), Some(header.timestamp));
        if log_matches_filter(&log, filter) {
            rows.push(log);
        }
    }
    rows.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(rows)
}

fn receipt_logs_from_receipts(
    receipts: &[Value],
    header: &EvmBlockHeader,
    filter: &EvmLogFilter,
) -> Result<Vec<LogRecord>, DatalensError> {
    let mut rows = Vec::new();
    for receipt in receipts {
        let receipt_logs = receipt
            .get("logs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::ProviderFailure, "missing receipt logs")
            })?;
        rows.append(&mut receipt_logs_from_values(receipt_logs, header, filter)?);
    }
    rows.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(rows)
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

fn header_bloom_may_match_filter(
    header: &EvmBlockHeader,
    filter: &EvmLogFilter,
) -> Result<bool, DatalensError> {
    let bloom = EvmLogBloom::from_hex(&header.logs_bloom)?;
    if filter.addresses().is_empty()
        && filter
            .topics()
            .iter()
            .all(|topic| matches!(topic, TopicFilter::Wildcard))
    {
        return Ok(!bloom.is_empty());
    }
    bloom.may_match_filter(filter)
}

fn adjacent_block_ranges(blocks: &[u64]) -> Vec<BlockRange> {
    let mut blocks = blocks.to_vec();
    blocks.sort_unstable();
    blocks.dedup();
    let mut ranges = Vec::new();
    let mut iter = blocks.into_iter();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let mut end = start;
    for block in iter {
        if block == end.saturating_add(1) {
            end = block;
        } else {
            ranges.push(BlockRange::expect_new(start, end));
            start = block;
            end = block;
        }
    }
    ranges.push(BlockRange::expect_new(start, end));
    ranges
}

fn dedupe_and_sort_logs(logs: &mut Vec<LogRecord>) {
    logs.sort_by_key(|log| {
        (
            log.block_number,
            log.transaction_index,
            log.log_index,
            log.block_hash.clone(),
            log.transaction_hash.clone(),
        )
    });
    logs.dedup_by(|left, right| {
        left.block_number == right.block_number
            && left.transaction_index == right.transaction_index
            && left.log_index == right.log_index
            && left.block_hash == right.block_hash
            && left.transaction_hash == right.transaction_hash
    });
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

    fn fetch_evm_block_headers_in_batches(
        &self,
        range: BlockRange,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let mut headers = Vec::new();
        for chunk in range.split(self.block_header_metadata.batch_size as u64)? {
            match self.fetch_evm_block_header_batch(chunk) {
                Ok(mut chunk_headers) => headers.append(&mut chunk_headers),
                Err(error) => {
                    log::warn!(
                        "falling back to single EVM block header fetch range={}-{} kind={:?}",
                        chunk.from_block,
                        chunk.to_block,
                        error.kind
                    );
                    for number in chunk.from_block..=chunk.to_block {
                        headers.push(self.fetch_evm_block_header(number)?);
                    }
                }
            }
        }
        headers.sort_by_key(|header| header.block_number);
        headers.dedup_by_key(|header| header.block_number);
        Ok(headers)
    }

    fn fetch_evm_block_headers_concurrently(
        &self,
        range: BlockRange,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let numbers = (range.from_block..=range.to_block).collect::<VecDeque<_>>();
        let concurrency = self
            .block_header_metadata
            .fetch_concurrency
            .min(numbers.len())
            .max(1);
        let work = Arc::new(Mutex::new(numbers));
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let client = self.clone();
            let work = Arc::clone(&work);
            handles.push(thread::spawn(move || {
                let mut headers = Vec::new();
                loop {
                    let number = work.lock().expect("header work").pop_front();
                    let Some(number) = number else {
                        break;
                    };
                    headers.push(client.fetch_evm_block_header(number)?);
                }
                Ok::<_, DatalensError>(headers)
            }));
        }

        let mut headers = Vec::new();
        for handle in handles {
            let mut fetched = handle.join().map_err(|_| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    "EVM block header worker panicked",
                )
            })??;
            headers.append(&mut fetched);
        }
        headers.sort_by_key(|header| header.block_number);
        headers.dedup_by_key(|header| header.block_number);
        Ok(headers)
    }

    fn fetch_evm_block_header_batch(
        &self,
        range: BlockRange,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let rpc_requests = (range.from_block..=range.to_block)
            .enumerate()
            .map(|(index, number)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": index + 1,
                    "method": "eth_getBlockByNumber",
                    "params": [format!("0x{number:x}"), false],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.batch_call("eth_getBlockByNumber", rpc_requests)?;
        let request_by_id = (range.from_block..=range.to_block)
            .enumerate()
            .map(|(index, number)| ((index + 1) as u64, number))
            .collect::<BTreeMap<_, _>>();
        let mut response_by_id = BTreeMap::new();
        for response in responses {
            let Some(id) = response.get("id").and_then(Value::as_u64) else {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "EVM block header batch response missing numeric id",
                ));
            };
            response_by_id.insert(id, response);
        }

        let mut headers = Vec::new();
        for (id, number) in request_by_id {
            let response = response_by_id.get(&id).ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("EVM block header batch response missing block {number}"),
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
                return Err(classify_provider_error(code, message));
            }
            let Some(block) = response.get("result") else {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("EVM block header batch response missing result for {number}"),
                ));
            };
            if block.is_null() {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("provider returned null block for {number}"),
                ));
            }
            let header = Self::parse_evm_block_header(block)?;
            if header.block_number != number {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "fetched block number {} does not match requested block {}",
                        header.block_number, number
                    ),
                ));
            }
            headers.push(header);
        }
        Ok(headers)
    }

    fn fetch_evm_block_header(&self, number: u64) -> Result<EvmBlockHeader, DatalensError> {
        let result = self.call(
            "eth_getBlockByNumber",
            json!([format!("0x{number:x}"), false]),
        )?;
        let Some(block) = result else {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("provider returned null block for {number}"),
            ));
        };
        let header = Self::parse_evm_block_header(&block)?;
        if header.block_number != number {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "fetched block number {} does not match requested block {}",
                    header.block_number, number
                ),
            ));
        }
        Ok(header)
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
            let fetched = match self.block_header_metadata.fetch_mode {
                EvmBlockHeaderFetchMode::Concurrent => {
                    let (headers, provider_calls) =
                        self.fetch_log_block_headers_concurrently(missing)?;
                    LogBlockHeaderFetch {
                        headers,
                        provider_calls,
                        fallback_reason: None,
                    }
                }
                EvmBlockHeaderFetchMode::Batch => {
                    self.fetch_log_block_headers_in_batches_with_fallback(missing)?
                }
            };
            diagnostics.provider_calls = fetched.provider_calls;
            if let Some(fallback_reason) = fetched.fallback_reason {
                diagnostics.fallback_mode = Some(EvmBlockHeaderFetchMode::Concurrent);
                diagnostics.fallback_reason = Some(fallback_reason);
            }
            self.insert_block_headers_into_cache(&fetched.headers);
            headers.extend(fetched.headers);
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

    fn insert_evm_block_headers_into_cache(&self, headers: &[EvmBlockHeader]) {
        let headers = headers
            .iter()
            .map(|header| {
                (
                    self.block_header_cache_key(header.block_number, &header.block_hash),
                    BlockHeader {
                        number: header.block_number,
                        hash: header.block_hash.clone(),
                        parent_hash: header.parent_hash.clone(),
                        timestamp: header.timestamp,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.insert_block_headers_into_cache(&headers);
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

    fn fetch_log_block_headers_in_batches_with_fallback(
        &self,
        missing: Vec<LogBlockHeaderRequest>,
    ) -> Result<LogBlockHeaderFetch, DatalensError> {
        let mut headers = BTreeMap::new();
        let mut provider_calls = 0;
        let mut fallback_missing = Vec::new();
        let mut fallback_reason = None;

        for chunk in missing.chunks(self.block_header_metadata.batch_size) {
            if fallback_reason.is_some() {
                fallback_missing.extend(chunk.iter().cloned());
                continue;
            }
            provider_calls += 1;
            let fetched = self.fetch_log_block_header_batch(chunk)?;
            headers.extend(fetched.headers);
            if !fetched.missing.is_empty() {
                fallback_missing.extend(fetched.missing);
                fallback_reason = fetched.fallback_reason;
            }
        }

        if !fallback_missing.is_empty() {
            let reason = fallback_reason.unwrap_or_else(|| "incomplete_batch".to_owned());
            log::warn!(
                "falling back to concurrent EVM log header fetch missing_headers={} reason={reason}",
                fallback_missing.len()
            );
            let (fallback_headers, fallback_calls) =
                self.fetch_log_block_headers_concurrently(fallback_missing)?;
            provider_calls += fallback_calls;
            headers.extend(fallback_headers);
            return Ok(LogBlockHeaderFetch {
                headers,
                provider_calls,
                fallback_reason: Some(reason),
            });
        }

        Ok(LogBlockHeaderFetch {
            headers,
            provider_calls,
            fallback_reason: None,
        })
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
    ) -> Result<LogBlockHeaderBatchFetch, DatalensError> {
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
        let responses = match self.batch_call("eth_getBlockByNumber", rpc_requests) {
            Ok(responses) => responses,
            Err(error) => {
                return Ok(LogBlockHeaderBatchFetch {
                    headers: BTreeMap::new(),
                    missing: requests.to_vec(),
                    fallback_reason: Some(format!("{:?}", error.kind)),
                });
            }
        };
        let request_by_id = requests
            .iter()
            .enumerate()
            .map(|(index, request)| ((index + 1) as u64, request))
            .collect::<BTreeMap<_, _>>();
        let mut response_by_id = BTreeMap::new();
        for response in responses {
            let Some(id) = response.get("id").and_then(Value::as_u64) else {
                return Ok(LogBlockHeaderBatchFetch {
                    headers: BTreeMap::new(),
                    missing: requests.to_vec(),
                    fallback_reason: Some("missing_numeric_id".to_owned()),
                });
            };
            response_by_id.insert(id, response);
        }

        let mut headers = BTreeMap::new();
        let mut missing = Vec::new();
        let mut fallback_reason = None;
        for (id, request) in request_by_id {
            let Some(response) = response_by_id.get(&id) else {
                missing.push(request.clone());
                fallback_reason.get_or_insert_with(|| "missing_response".to_owned());
                continue;
            };
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
                missing.push(request.clone());
                fallback_reason.get_or_insert_with(|| format!("{:?}", error.kind));
                continue;
            }
            let Some(block) = response.get("result") else {
                missing.push(request.clone());
                fallback_reason.get_or_insert_with(|| "missing_result".to_owned());
                continue;
            };
            if block.is_null() {
                missing.push(request.clone());
                fallback_reason.get_or_insert_with(|| "null_result".to_owned());
                continue;
            }
            let header = match Self::parse_block_header(block) {
                Ok(header) => header,
                Err(error) => {
                    missing.push(request.clone());
                    fallback_reason.get_or_insert_with(|| format!("{:?}", error.kind));
                    continue;
                }
            };
            self.validate_log_block_header_request(request, &header)?;
            headers.insert(
                self.block_header_cache_key(request.number, &request.hash),
                header,
            );
        }
        Ok(LogBlockHeaderBatchFetch {
            headers,
            missing,
            fallback_reason,
        })
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

    fn parse_evm_block_header(block: &Value) -> Result<EvmBlockHeader, DatalensError> {
        Ok(EvmBlockHeader {
            block_number: hex_u64_field(block, "number")?,
            block_hash: string_field(block, "hash")?,
            parent_hash: string_field(block, "parentHash")?,
            timestamp: hex_u64_field(block, "timestamp")?,
            logs_bloom: string_field(block, "logsBloom")?,
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
