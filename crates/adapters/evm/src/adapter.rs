use datalens_chain::{
    AdapterCapabilities, CanonicalBlock, CanonicalBlockRequest, ChainAdapter, ChainFetchRequest,
    ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector, FinalityKind,
    HeightRangeKind, ReorgSignal, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, EvmLogFilter, EvmReceipt, EvmTransaction, LedgerRange, LogFilter, LogRecord,
    QueryRows, redact_urls_in_text,
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

#[derive(Clone)]
pub struct EvmRpcClient {
    rpc_urls: Vec<String>,
    client: Client,
    chain: ChainIdentity,
    finality_policy: EvmFinalityPolicy,
    max_block_batch_blocks: u64,
    max_get_logs_range_blocks: u64,
    max_addresses_per_query: usize,
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
            max_addresses_per_query: usize::MAX,
        }
    }

    pub fn with_chain(
        rpc_urls: Vec<String>,
        chain: ChainIdentity,
        finality_policy: EvmFinalityPolicy,
        max_block_batch_blocks: u64,
        max_get_logs_range_blocks: u64,
        max_addresses_per_query: usize,
    ) -> Self {
        Self {
            rpc_urls,
            client: Client::new(),
            chain,
            finality_policy,
            max_block_batch_blocks,
            max_get_logs_range_blocks,
            max_addresses_per_query,
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
        log::info!(
            "fetching EVM logs range={}-{} addresses={} topic_slots={}",
            range.from_block,
            range.to_block,
            filter.addresses().len(),
            filter.topics().len()
        );
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

        logs.into_iter().map(|log| parse_log_record(&log)).collect()
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
                    .with_max_range_len(self.max_get_logs_range_blocks)
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
                "request range exceeds EVM provider range limit",
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
                // Full durable log indexing uses eth_getLogs so query-driven log fills and
                // backfills share one provider source.
                let filter = EvmLogFilter::try_from(LogFilter {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                })?;
                let mut logs = self.fetch_evm_logs(range, &filter)?;
                logs.retain(|log| range.contains(log.block_number));
                logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
                (QueryRows::EvmLogs(logs), 1)
            }
            (dataset, DatasetSelector::EvmLogs(filter)) if *dataset == DatasetKey::evm_logs() => {
                let mut logs = self.fetch_evm_logs(range, filter)?;
                logs.retain(|log| range.contains(log.block_number));
                logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
                (QueryRows::EvmLogs(logs), 1)
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
        let request_id = request.context.request_id.clone();
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
            warnings: Vec::new(),
        }))
    }
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

    fn fetch_block_by_tag(&self, tag: &str) -> Result<BlockHeader, DatalensError> {
        let result = self.call("eth_getBlockByNumber", json!([tag, false]))?;
        let Some(block) = result else {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("provider returned no block for tag {tag}"),
            ));
        };
        Ok(BlockHeader {
            number: hex_u64_field(&block, "number")?,
            hash: string_field(&block, "hash")?,
            parent_hash: string_field(&block, "parentHash")?,
            timestamp: hex_u64_field(&block, "timestamp")?,
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
