use std::collections::{HashMap, HashSet};

use datalens_chain::{ChainFetchRequest, DatasetSelector};
use datalens_core::{DatalensError, DatalensErrorKind, LedgerRange};
use serde_json::{Value, json};

use crate::adapter::{
    TRON_EVENTS_KIND, TronAdapter, TronBlock, TronContractEvent, TronContractEventRequest,
    TronEventFilter, TronFinality, TronProvider, normalize_tron_contract_address,
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
    pub(crate) parent_hash: String,
    pub(crate) block_timestamp: u64,
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
                parent_hash: block.parent_hash.clone(),
                block_timestamp: block.timestamp,
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
                "parent_hash": transaction.parent_hash,
                "block_timestamp": transaction.block_timestamp,
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
                "parent_hash": info.transaction.parent_hash,
                "block_timestamp": info.transaction.block_timestamp,
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
                "parent_hash": info.transaction.parent_hash,
                "block_timestamp": info.transaction.block_timestamp,
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

pub(crate) fn missing_block_scan_event_filter(
    selector: &DatasetSelector,
    rows: &[Value],
) -> Option<TronEventFilter> {
    let filter = selector_event_filter(selector);
    let event_names = filter
        .event_names
        .iter()
        .filter(|event_name| event_topic_from_name(event_name).is_some())
        .filter(|event_name| {
            !rows.iter().any(|row| {
                row.get("event_name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name == event_name.as_str())
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if filter.contract_addresses.is_empty() || event_names.is_empty() {
        return None;
    }
    Some(TronEventFilter {
        contract_addresses: filter.contract_addresses,
        event_names,
    })
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
        let mut blocks = HashMap::new();
        let (start_timestamp, end_timestamp) = if range.start() == range.end() {
            (None, None)
        } else {
            let start_block = self
                .provider
                .get_block_by_number(range.start(), TronFinality::Finalized)?
                .ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        format!(
                            "provider returned no finalized Tron block metadata for range start block {}",
                            range.start()
                        ),
                    )
                })?;
            calls += 1;
            let end_block = self
                .provider
                .get_block_by_number(range.end(), TronFinality::Finalized)?
                .ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        format!(
                            "provider returned no finalized Tron block metadata for range end block {}",
                            range.end()
                        ),
                    )
                })?;
            calls += 1;
            let start_timestamp = start_block.timestamp;
            let end_timestamp = end_block.timestamp;
            blocks.insert(start_block.number, start_block);
            blocks.insert(end_block.number, end_block);
            (Some(start_timestamp), Some(end_timestamp))
        };
        for contract_address in &filter.contract_addresses {
            let event_names = match filter.event_names.as_slice() {
                [] => vec![None],
                [event_name] => vec![Some(event_name.clone())],
                _ => vec![None],
            };
            for event_name in event_names {
                let page_cap = if event_name.is_none() && filter.event_names.len() > 1 {
                    self.max_contract_event_pages
                        .saturating_mul(filter.event_names.len())
                        .saturating_mul(usize::try_from(range.len()).unwrap_or(usize::MAX))
                } else {
                    self.max_contract_event_pages
                        .saturating_mul(usize::try_from(range.len()).unwrap_or(usize::MAX))
                };
                let mut fingerprint = None;
                let mut pages = 0;
                let mut seen_fingerprints = HashSet::new();
                loop {
                    // Pagination is capped and fingerprint loops are rejected so
                    // a TronGrid query cannot run forever or duplicate coverage.
                    let page = self
                        .provider
                        .get_contract_events(TronContractEventRequest {
                            contract_address: contract_address.clone(),
                            event_name: event_name.clone(),
                            range: range.clone(),
                            start_timestamp,
                            end_timestamp,
                            only_confirmed: true,
                            limit: 200,
                            fingerprint: fingerprint.clone(),
                        })?;
                    pages += 1;
                    calls += page.provider_calls;
                    for event in page.events.into_iter().filter(|event| {
                        range.contains(event.block_number)
                            && filter.matches(&event.contract_address, event.event_name.as_deref())
                    }) {
                        let block = if let Some(block) = blocks.get(&event.block_number) {
                            block
                        } else {
                            let block = self
                                .provider
                                .get_block_by_number(event.block_number, TronFinality::Finalized)?
                                .ok_or_else(|| {
                                    DatalensError::new(
                                        DatalensErrorKind::ProviderFailure,
                                        format!(
                                            "provider returned no finalized Tron block metadata for block {}",
                                            event.block_number
                                        ),
                                    )
                                })?;
                            calls += 1;
                            blocks.insert(event.block_number, block);
                            blocks.get(&event.block_number).expect("inserted block")
                        };
                        if let Some(block_hash) = &event.block_hash
                            && !block_hash.eq_ignore_ascii_case(&block.hash)
                        {
                            return Err(DatalensError::new(
                                DatalensErrorKind::ProviderFailure,
                                format!(
                                    "TronGrid contract event block hash {} did not match finalized block hash {} for block {}",
                                    block_hash, block.hash, event.block_number
                                ),
                            ));
                        }
                        rows.push(contract_event_row(event, block));
                    }
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
                    if pages >= page_cap {
                        return Err(DatalensError::new(
                            DatalensErrorKind::ProviderLimit,
                            format!(
                                "TronGrid contract event page limit {} reached for contract {} event {} range {}-{}",
                                page_cap,
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

fn contract_event_row(event: TronContractEvent, block: &TronBlock) -> Value {
    let contract_address =
        normalize_tron_contract_address(&event.contract_address).unwrap_or(event.contract_address);
    json!({
        "contract_address": contract_address,
        "event_name": event.event_name,
        "event_signature": event.event_signature,
        "indexed_fields": event.indexed_fields,
        "non_indexed_fields": event.non_indexed_fields,
        "transaction_id": event.transaction_id,
        "block_number": event.block_number,
        "block_hash": block.hash,
        "parent_hash": block.parent_hash,
        "block_timestamp": block.timestamp,
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
    // Do not fallback on arbitrary provider failures: only errors that still
    // allow an equivalent finalized block scan should leave the optimized path.
    match error.kind {
        DatalensErrorKind::UnsupportedDataset => true,
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

const KNOWN_EVENT_TOPICS: &[KnownEventTopic] = &[
    KnownEventTopic {
        name: "Transfer",
        topic0: "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    },
    KnownEventTopic {
        name: "HashImported",
        topic0: "ea087580bb17f433441f3b6c0c0b80cae92ee74a8d7f50050388646d9ffd1431",
    },
    KnownEventTopic {
        name: "MessageAccepted",
        topic0: "cfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18",
    },
    KnownEventTopic {
        name: "MessageAssigned",
        topic0: "3832f95736b288316c84b775a004a9d17177362548ce253cba9acb4801875f4d",
    },
    KnownEventTopic {
        name: "MessageDispatched",
        topic0: "62b1dc20fd6f1518626da5b6f9897e8cd4ebadbad071bb66dc96a37c970087a8",
    },
    KnownEventTopic {
        name: "MessageRecv",
        topic0: "a931ec14fe958397dcb26e285e56292c13d77907712b51bbaa24cfc9349b789d",
    },
    KnownEventTopic {
        name: "MessageSent",
        topic0: "40195d26d027672e04e23e34282d68c3d43ea138415b24c54fcdb9c2573e5975",
    },
    KnownEventTopic {
        name: "SignatureSubmittion",
        topic0: "8b3975e4768e70d323e926e2cef0676fc9a3250437d9b8f90b52c770f0d7545f",
    },
];

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
