//! EVM chain-family adapter boundary.

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, FinalityKind, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetKey, EvmLogFilter, LedgerRange, LogFilter, LogRecord, NetworkId, QueryRows, TopicFilter,
};
use reqwest::blocking::Client;
use serde_json::{Value, json};

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
                let error = classify_transport_error(error);
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
                format!("decode JSON-RPC response: {error}"),
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
                    .with_max_range_blocks(self.max_block_batch_blocks)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_blocks(self.max_get_logs_range_blocks)
                    .with_max_addresses_per_query(self.max_addresses_per_query)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(matches!(
                        self.finality_policy,
                        EvmFinalityPolicy::Auto | EvmFinalityPolicy::RpcTags { .. }
                    ))
                    .with_range_split(true),
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
        let (rows, provider_calls) = match (&request.dataset_key, &request.selector) {
            (dataset, DatasetSelector::All) if *dataset == DatasetKey::evm_blocks() => (
                QueryRows::EvmBlocks(self.fetch_blocks(range)?),
                range.len().min(usize::MAX as u128) as usize,
            ),
            (dataset, DatasetSelector::EvmLogs(filter)) if *dataset == DatasetKey::evm_logs() => {
                (QueryRows::EvmLogs(self.fetch_evm_logs(range, filter)?), 1)
            }
            (dataset, _) if *dataset == DatasetKey::evm_blocks() => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "blocks require all selector",
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
        Ok(ChainFetchResponse::new(
            request.chain,
            request.dataset_key,
            LedgerRange::from_block_range(range),
            request.selector,
            rows,
        )
        .with_provider_diagnostics(datalens_chain::ProviderDiagnostics {
            calls: provider_calls,
            rows_scanned: 0,
            warnings: Vec::new(),
        }))
    }
}

impl EvmRpcClient {
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

pub fn parse_log_record(log: &Value) -> Result<LogRecord, DatalensError> {
    LogRecord::try_new(
        hex_u64_field(log, "blockNumber")?,
        string_field(log, "blockHash")?,
        string_field(log, "transactionHash")?,
        hex_u64_field(log, "transactionIndex")?,
        hex_u64_field(log, "logIndex")?,
        string_field(log, "address")?,
        log.get("topics")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::ProviderFailure, "missing topics")
            })?
            .iter()
            .map(|topic| {
                topic.as_str().map(str::to_owned).ok_or_else(|| {
                    DatalensError::new(DatalensErrorKind::ProviderFailure, "invalid topic")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        string_field(log, "data")?,
        log.get("removed").and_then(Value::as_bool).unwrap_or(false),
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!("invalid provider log payload: {}", error.message),
        )
    })
}

fn evm_log_filter(range: BlockRange, filter: &EvmLogFilter) -> Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "fromBlock".to_owned(),
        json!(format!("0x{:x}", range.from_block)),
    );
    value.insert(
        "toBlock".to_owned(),
        json!(format!("0x{:x}", range.to_block)),
    );
    if !filter.addresses().is_empty() {
        value.insert("address".to_owned(), json!(filter.addresses()));
    }
    if !filter.topics().is_empty() {
        value.insert(
            "topics".to_owned(),
            Value::Array(
                filter
                    .topics()
                    .iter()
                    .map(|topic| match topic {
                        TopicFilter::Wildcard => Value::Null,
                        TopicFilter::AnyOf(values) => json!(values),
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(value)
}

fn classify_transport_error(error: reqwest::Error) -> DatalensError {
    if error.is_timeout() {
        DatalensError::new(
            DatalensErrorKind::ProviderTimeout,
            format!("provider timeout: {error}"),
        )
    } else {
        DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!("provider request failed: {error}"),
        )
    }
}

pub fn height_from_latest_lag(latest_height: u64, lag_blocks: u64) -> u64 {
    latest_height.saturating_sub(lag_blocks)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LagFinalityPolicy {
    safe_lag_blocks: Option<u64>,
    finalized_lag_blocks: Option<u64>,
}

fn chain_profile(network_id: Option<&NetworkId>) -> Option<LagFinalityPolicy> {
    match network_id {
        Some(NetworkId::Numeric(1)) => Some(LagFinalityPolicy {
            safe_lag_blocks: Some(64),
            finalized_lag_blocks: Some(128),
        }),
        _ => None,
    }
}

fn is_finality_tag_unsupported(error: &DatalensError) -> bool {
    matches!(
        error.kind,
        DatalensErrorKind::InvalidInput | DatalensErrorKind::UnsupportedDataset
    )
}

fn zero_lag_error() -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        "lag finality policy must not use zero lag for durable cache safety",
    )
}

pub fn classify_provider_error(code: i64, message: &str) -> DatalensError {
    let lower = message.to_ascii_lowercase();
    let kind = if code == -32602 {
        DatalensErrorKind::InvalidInput
    } else if code == -32601 || lower.contains("unsupported") || lower.contains("not supported") {
        DatalensErrorKind::UnsupportedDataset
    } else if code == 429 || lower.contains("rate") {
        DatalensErrorKind::RateLimited
    } else if lower.contains("range")
        || lower.contains("limit")
        || lower.contains("too many")
        || lower.contains("more than")
    {
        DatalensErrorKind::ProviderLimit
    } else if lower.contains("timeout") || lower.contains("timed out") {
        DatalensErrorKind::ProviderTimeout
    } else {
        DatalensErrorKind::ProviderFailure
    };
    DatalensError::new(kind, message)
}

fn string_field(value: &Value, field: &str) -> Result<String, DatalensError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("missing or invalid {field}"),
            )
        })
}

fn hex_u64_field(value: &Value, field: &str) -> Result<u64, DatalensError> {
    let text = string_field(value, field)?;
    u64::from_str_radix(text.trim_start_matches("0x"), 16).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!("invalid hex field {field}: {error}"),
        )
    })
}
