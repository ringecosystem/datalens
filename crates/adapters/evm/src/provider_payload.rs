use datalens_core::{
    BlockRange, DatalensError, DatalensErrorKind, EvmLogFilter, EvmReceipt, EvmTransaction,
    LogRecord, NetworkId, TopicFilter, redact_url, redact_urls_in_text,
};
use serde_json::{Value, json};

pub fn parse_transaction(
    transaction: &Value,
    fallback_block_number: u64,
    fallback_block_hash: &str,
) -> Result<EvmTransaction, DatalensError> {
    Ok(EvmTransaction {
        hash: string_field(transaction, "hash")?,
        block_number: optional_hex_u64_field(transaction, "blockNumber")?
            .unwrap_or(fallback_block_number),
        block_hash: optional_string_field(transaction, "blockHash")?
            .unwrap_or_else(|| fallback_block_hash.to_owned()),
        transaction_index: hex_u64_field(transaction, "transactionIndex")?,
        from: string_field(transaction, "from")?,
        to: optional_string_field(transaction, "to")?,
        value: string_field(transaction, "value")?,
        input: string_field(transaction, "input")?,
        nonce: hex_u64_field(transaction, "nonce")?,
        gas: hex_u64_field(transaction, "gas")?,
        gas_price: optional_string_field(transaction, "gasPrice")?,
        max_fee_per_gas: optional_string_field(transaction, "maxFeePerGas")?,
        max_priority_fee_per_gas: optional_string_field(transaction, "maxPriorityFeePerGas")?,
        transaction_type: optional_string_field(transaction, "type")?,
    })
}

pub fn parse_receipt(receipt: &Value) -> Result<EvmReceipt, DatalensError> {
    Ok(EvmReceipt {
        transaction_hash: string_field(receipt, "transactionHash")?,
        block_number: hex_u64_field(receipt, "blockNumber")?,
        block_hash: string_field(receipt, "blockHash")?,
        transaction_index: hex_u64_field(receipt, "transactionIndex")?,
        status: optional_hex_u64_field(receipt, "status")?,
        gas_used: hex_u64_field(receipt, "gasUsed")?,
        cumulative_gas_used: hex_u64_field(receipt, "cumulativeGasUsed")?,
        effective_gas_price: optional_string_field(receipt, "effectiveGasPrice")?,
        contract_address: optional_string_field(receipt, "contractAddress")?,
        logs_bloom: optional_string_field(receipt, "logsBloom")?,
    })
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

pub(crate) fn evm_log_filter(range: BlockRange, filter: &EvmLogFilter) -> Value {
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
    let topics = filter
        .topics()
        .iter()
        .map(|topic| match topic {
            TopicFilter::Wildcard => Value::Null,
            TopicFilter::AnyOf(values) => json!(values),
        })
        .collect::<Vec<_>>();
    let topics = topics
        .into_iter()
        .rev()
        .skip_while(Value::is_null)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    if !topics.is_empty() {
        value.insert("topics".to_owned(), Value::Array(topics));
    }
    Value::Object(value)
}

pub(crate) fn classify_transport_error(error: reqwest::Error, endpoint: &str) -> DatalensError {
    let endpoint = redact_url(endpoint);
    if error.is_timeout() {
        DatalensError::new(
            DatalensErrorKind::ProviderTimeout,
            format!(
                "provider timeout endpoint={endpoint}: {}",
                redact_urls_in_text(&error.to_string())
            ),
        )
    } else {
        DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!(
                "provider request failed endpoint={endpoint}: {}",
                redact_urls_in_text(&error.to_string())
            ),
        )
    }
}

pub fn height_from_latest_lag(latest_height: u64, lag_blocks: u64) -> u64 {
    latest_height.saturating_sub(lag_blocks)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LagFinalityPolicy {
    pub(crate) safe_lag_blocks: Option<u64>,
    pub(crate) finalized_lag_blocks: Option<u64>,
}

pub(crate) fn chain_profile(network_id: Option<&NetworkId>) -> Option<LagFinalityPolicy> {
    match network_id {
        Some(NetworkId::Numeric(1)) => Some(LagFinalityPolicy {
            safe_lag_blocks: Some(64),
            finalized_lag_blocks: Some(128),
        }),
        _ => None,
    }
}

pub(crate) fn is_finality_tag_unsupported(error: &DatalensError) -> bool {
    matches!(
        error.kind,
        DatalensErrorKind::InvalidInput | DatalensErrorKind::UnsupportedDataset
    )
}

pub(crate) fn is_block_receipts_unsupported(error: &DatalensError) -> bool {
    if error.kind == DatalensErrorKind::UnsupportedDataset {
        return true;
    }
    if error.kind != DatalensErrorKind::ProviderFailure {
        return false;
    }
    let lower = error.message.to_ascii_lowercase();
    lower.contains("method unavailable")
        || lower.contains("method not available")
        || lower.contains("method is not available")
}

pub(crate) fn zero_lag_error() -> DatalensError {
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
    DatalensError::new(kind, redact_urls_in_text(message))
}

pub(crate) fn string_field(value: &Value, field: &str) -> Result<String, DatalensError> {
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

pub(crate) fn optional_string_field(
    value: &Value,
    field: &str,
) -> Result<Option<String>, DatalensError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.to_owned())),
        _ => Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!("missing or invalid {field}"),
        )),
    }
}

pub(crate) fn hex_u64_field(value: &Value, field: &str) -> Result<u64, DatalensError> {
    let text = string_field(value, field)?;
    u64::from_str_radix(text.trim_start_matches("0x"), 16).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!("invalid hex field {field}: {error}"),
        )
    })
}

pub(crate) fn optional_hex_u64_field(
    value: &Value,
    field: &str,
) -> Result<Option<u64>, DatalensError> {
    optional_string_field(value, field)?
        .map(|text| {
            u64::from_str_radix(text.trim_start_matches("0x"), 16).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("invalid hex field {field}: {error}"),
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalens_core::LogFilter;

    fn topic(value: &str) -> String {
        format!("0x{value:0>64}")
    }

    #[test]
    fn evm_log_filter_trims_trailing_wildcard_topics() {
        let first_topic = topic("1");
        let filter = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![Some(vec![first_topic.clone()]), None, None, None],
        })
        .expect("filter");

        let value = evm_log_filter(BlockRange::expect_new(10, 10), &filter);

        assert_eq!(value["topics"], json!([[first_topic]]));
    }

    #[test]
    fn evm_log_filter_preserves_inner_wildcard_topics() {
        let first_topic = topic("1");
        let third_topic = topic("3");
        let filter = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![
                Some(vec![first_topic.clone()]),
                None,
                Some(vec![third_topic.clone()]),
            ],
        })
        .expect("filter");

        let value = evm_log_filter(BlockRange::expect_new(10, 10), &filter);

        assert_eq!(value["topics"], json!([[first_topic], null, [third_topic]]));
    }

    #[test]
    fn evm_log_filter_omits_all_wildcard_topics() {
        let filter = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None, None],
        })
        .expect("filter");

        let value = evm_log_filter(BlockRange::expect_new(10, 10), &filter);

        assert!(value.get("topics").is_none());
    }
}
