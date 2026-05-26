//! EVM chain-family adapter boundary.

use datalens_chain::{AdapterCapabilities, ChainAdapter};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind,
    LogFilter, LogRecord,
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
}

#[derive(Clone)]
pub struct EvmRpcClient {
    rpc_urls: Vec<String>,
    client: Client,
}

impl EvmRpcClient {
    pub fn new(rpc_urls: Vec<String>) -> Self {
        Self {
            rpc_urls,
            client: Client::new(),
        }
    }

    pub fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        let mut blocks = Vec::new();
        for number in range.from_block..=range.to_block {
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
        let result = self.call("eth_getLogs", json!([evm_log_filter(range, filter)]))?;
        let logs = result
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "invalid eth_getLogs result",
                )
            })?;

        logs.into_iter().map(|log| parse_log_record(&log)).collect()
    }

    fn call(&self, method: &str, params: Value) -> Result<Option<Value>, DatalensError> {
        let url = self.rpc_urls.first().ok_or_else(|| {
            DatalensError::new(DatalensErrorKind::InvalidInput, "chain has no rpc_urls")
        })?;
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
            .map_err(classify_transport_error)?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("decode JSON-RPC response: {error}"),
            )
        })?;
        if !status.is_success() {
            return Err(classify_provider_error(
                status.as_u16() as i64,
                &body.to_string(),
            ));
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
            return Err(classify_provider_error(code, message));
        }
        Ok(body.get("result").cloned())
    }
}

fn parse_log_record(log: &Value) -> Result<LogRecord, DatalensError> {
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

fn evm_log_filter(range: BlockRange, filter: &LogFilter) -> Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "fromBlock".to_owned(),
        json!(format!("0x{:x}", range.from_block)),
    );
    value.insert(
        "toBlock".to_owned(),
        json!(format!("0x{:x}", range.to_block)),
    );
    if !filter.addresses.is_empty() {
        value.insert("address".to_owned(), json!(filter.addresses));
    }
    if !filter.topics.is_empty() {
        value.insert("topics".to_owned(), json!(filter.topics));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_provider_error_maps_known_failure_modes() {
        assert_eq!(
            classify_provider_error(-32602, "invalid argument").kind,
            DatalensErrorKind::InvalidInput
        );
        assert_eq!(
            classify_provider_error(-32601, "method not found").kind,
            DatalensErrorKind::UnsupportedDataset
        );
        assert_eq!(
            classify_provider_error(-32000, "query returned more than 10000 results").kind,
            DatalensErrorKind::ProviderLimit
        );
        assert_eq!(
            classify_provider_error(-32000, "request timed out").kind,
            DatalensErrorKind::ProviderTimeout
        );
        assert_eq!(
            classify_provider_error(429, "too many requests").kind,
            DatalensErrorKind::RateLimited
        );
    }

    #[test]
    fn test_parse_log_record_canonicalizes_provider_hex_values() {
        let record = parse_log_record(&json!({
            "blockNumber": "0xa",
            "blockHash": "0xblock",
            "transactionHash": "0xtx",
            "transactionIndex": "0x0",
            "logIndex": "0x1",
            "address": "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "topics": ["0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"],
            "data": "0x",
            "removed": false
        }))
        .expect("valid log");

        assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(
            record.topics,
            vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        );
    }

    #[test]
    fn test_parse_log_record_rejects_invalid_provider_hex_values() {
        let error = parse_log_record(&json!({
            "blockNumber": "0xa",
            "blockHash": "0xblock",
            "transactionHash": "0xtx",
            "transactionIndex": "0x0",
            "logIndex": "0x1",
            "address": "0xabc",
            "topics": [],
            "data": "0x",
            "removed": false
        }))
        .expect_err("invalid address");

        assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    }
}
