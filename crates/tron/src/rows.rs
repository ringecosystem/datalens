use std::collections::HashSet;

use datalens_chain::{ChainFetchRequest, DatasetSelector};
use datalens_core::{DatalensError, DatalensErrorKind, LedgerRange};
use serde_json::{Value, json};

use crate::adapter::{
    TRON_EVENTS_KIND, TronAdapter, TronBlock, TronContractEvent, TronContractEventRequest,
    TronEventFilter, TronProvider, normalize_tron_contract_address,
};

pub(crate) fn block_rows(blocks: &[TronBlock]) -> Vec<Value> {
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
pub(crate) struct TronTransactionRef {
    pub(crate) tx_id: String,
    pub(crate) block_number: u64,
    pub(crate) block_hash: String,
    pub(crate) transaction_index: u64,
    pub(crate) raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TronTransactionInfo {
    pub(crate) transaction: TronTransactionRef,
    pub(crate) raw: Value,
}

pub(crate) fn transaction_refs(
    blocks: &[TronBlock],
) -> Result<Vec<TronTransactionRef>, DatalensError> {
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

pub(crate) fn transaction_rows(transactions: &[TronTransactionRef]) -> Vec<Value> {
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

pub(crate) fn transaction_info_rows(infos: &[TronTransactionInfo]) -> Vec<Value> {
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

pub(crate) fn event_rows(infos: &[TronTransactionInfo], selector: &DatasetSelector) -> Vec<Value> {
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
            let topic0 = topics.first().and_then(Value::as_str);
            let event_name = event_name_from_signature(topic0);
            if !filter.matches_log(&contract_address, topic0) {
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

pub(crate) fn validate_block_scan_event_filter(
    selector: &DatasetSelector,
) -> Result<(), DatalensError> {
    let filter = selector_event_filter(selector);
    if filter.event_names.is_empty() {
        return Ok(());
    }
    let unsupported = filter
        .event_names
        .iter()
        .filter(|event_name| event_topic_from_name(event_name).is_none())
        .cloned()
        .collect::<Vec<_>>();
    if unsupported.is_empty() {
        return Ok(());
    }
    Err(DatalensError::new(
        DatalensErrorKind::UnsupportedDataset,
        format!(
            "Tron block-scan fallback cannot map event names to topics: {}",
            unsupported.join(",")
        ),
    ))
}

impl<P> TronAdapter<P>
where
    P: TronProvider,
{
    pub(crate) fn fetch_contract_events(
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
                let mut pages = 0;
                let mut seen_fingerprints = HashSet::new();
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
                    pages += 1;
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
                    if !seen_fingerprints.insert(next.clone()) {
                        return Err(DatalensError::new(
                            DatalensErrorKind::ProviderLimit,
                            format!(
                                "repeated TronGrid contract event fingerprint for contract {} event {} range {}-{}",
                                contract_address,
                                event_name.as_deref().unwrap_or("all"),
                                range.start(),
                                range.end()
                            ),
                        ));
                    }
                    if pages >= self.max_contract_event_pages {
                        return Err(DatalensError::new(
                            DatalensErrorKind::ProviderLimit,
                            format!(
                                "TronGrid contract event page limit {} reached for contract {} event {} range {}-{}",
                                self.max_contract_event_pages,
                                contract_address,
                                event_name.as_deref().unwrap_or("all"),
                                range.start(),
                                range.end()
                            ),
                        ));
                    }
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

pub(crate) fn should_fallback_from_contract_events(error: &DatalensError) -> bool {
    match error.kind {
        DatalensErrorKind::AuthenticationFailed
        | DatalensErrorKind::Unauthorized
        | DatalensErrorKind::RateLimited
        | DatalensErrorKind::ProviderTimeout
        | DatalensErrorKind::UnsupportedDataset => true,
        DatalensErrorKind::ProviderFailure => {
            is_contract_event_provider_failure_fallback_safe(error)
        }
        _ => false,
    }
}

fn is_contract_event_provider_failure_fallback_safe(error: &DatalensError) -> bool {
    let message = error.message.as_str();
    message.starts_with("TronGrid contract events request failed:")
        || message.starts_with("TronGrid contract events HTTP error 5")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedTronEventFilter {
    pub(crate) contract_addresses: Vec<String>,
    pub(crate) event_names: Vec<String>,
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

    fn matches_log(&self, contract_address: &str, topic0: Option<&str>) -> bool {
        let normalized = normalize_tron_contract_address(contract_address)
            .unwrap_or_else(|_| contract_address.to_owned());
        if !self.contract_addresses.is_empty() && !self.contract_addresses.contains(&normalized) {
            return false;
        }
        if self.event_names.is_empty() {
            return true;
        }
        topic0.is_some_and(|topic| {
            self.event_names
                .iter()
                .filter_map(|event_name| event_topic_from_name(event_name))
                .any(|known_topic| topic.eq_ignore_ascii_case(known_topic))
        })
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
    KNOWN_EVENT_TOPICS
        .iter()
        .find(|event| {
            signature.is_some_and(|signature| signature.eq_ignore_ascii_case(event.topic0))
        })
        .map(|event| event.name.to_owned())
}

fn event_topic_from_name(name: &str) -> Option<&'static str> {
    KNOWN_EVENT_TOPICS
        .iter()
        .find(|event| event.name == name)
        .map(|event| event.topic0)
}

struct KnownEventTopic {
    name: &'static str,
    topic0: &'static str,
}

const KNOWN_EVENT_TOPICS: &[KnownEventTopic] = &[KnownEventTopic {
    name: "Transfer",
    topic0: "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}];

pub(crate) fn hex_prefix(bytes: &[u8], len: usize) -> String {
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
