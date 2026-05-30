use serde_json::{Map, Value};

use super::IndexedRecord;

#[derive(Clone, Debug)]
pub(super) struct NormalizedIndexedEvent {
    pub unique_key: String,
    pub index_name: String,
    pub chain_family: String,
    pub chain_id: i64,
    pub chain_name: String,
    pub chain_identity: String,
    pub dataset: String,
    pub block_number: i64,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<i64>,
    pub event_index: Option<i64>,
    pub selector: Option<String>,
    pub topics_json: Option<String>,
    pub signature: Option<String>,
    pub event_name: Option<String>,
    pub data_payload: Option<String>,
    pub raw_payload: String,
    pub removed: Option<i64>,
    pub finality: Option<String>,
    pub position: Option<EventPosition>,
}

impl NormalizedIndexedEvent {
    pub(super) fn from_record(record: &IndexedRecord) -> Self {
        let chain_family = chain_family(record);
        let chain_identity = format!("{}:{}:{}", chain_family, record.chain, record.chain_id);
        let block_number = json_u64(&record.payload, "block_number")
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or_default();
        let transaction_index = json_u64(&record.payload, "transaction_index")
            .and_then(|value| i64::try_from(value).ok());
        let event_index =
            json_u64(&record.payload, "log_index").and_then(|value| i64::try_from(value).ok());
        let selector = json_string(&record.payload, "address")
            .or_else(|| json_string(&record.payload, "program"))
            .or_else(|| json_string(&record.payload, "account"))
            .or_else(|| json_string(&record.payload, "selector"));
        let topics_json = record
            .payload
            .get("topics")
            .map(|topics| topics.to_string());
        let signature = json_string(&record.payload, "signature").or_else(|| {
            record
                .payload
                .get("topics")
                .and_then(Value::as_array)
                .and_then(|topics| topics.first())
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        let event_name = json_string(&record.payload, "event_name");
        let block_hash = json_string(&record.payload, "block_hash");
        let transaction_hash = json_string(&record.payload, "transaction_hash");
        let position =
            EventPosition::new(&record.chain, block_number, transaction_index, event_index);
        let unique_key = unique_key(
            record,
            &chain_identity,
            block_hash.as_deref(),
            transaction_hash.as_deref(),
            event_index,
            selector.as_deref(),
        );

        Self {
            unique_key,
            index_name: record.index.clone(),
            chain_family,
            chain_id: i64::try_from(record.chain_id).unwrap_or(i64::MAX),
            chain_name: record.chain.clone(),
            chain_identity,
            dataset: record.dataset.clone(),
            block_number,
            block_hash,
            transaction_hash,
            transaction_index,
            event_index,
            selector,
            topics_json,
            signature,
            event_name,
            data_payload: json_string(&record.payload, "data"),
            raw_payload: record.payload.to_string(),
            removed: record
                .payload
                .get("removed")
                .and_then(Value::as_bool)
                .map(i64::from),
            finality: json_string(&record.payload, "finality"),
            position,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct EventPosition {
    block_number: i64,
    transaction_index: i64,
    event_index: i64,
    pub receipt_key: String,
}

impl EventPosition {
    fn new(
        chain: &str,
        block_number: i64,
        transaction_index: Option<i64>,
        event_index: Option<i64>,
    ) -> Option<Self> {
        let transaction_index = transaction_index.unwrap_or(0);
        let event_index = event_index.unwrap_or(0);
        Some(Self {
            block_number,
            transaction_index,
            event_index,
            receipt_key: format!("{chain}:{block_number}:{transaction_index}:{event_index}"),
        })
    }
}

pub(super) fn max_position(
    current: Option<EventPosition>,
    next: Option<EventPosition>,
) -> Option<EventPosition> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

pub(super) fn row_payload_with_metadata(
    mut value: Value,
    index_name: String,
    chain_name: String,
    chain_family: String,
    chain_id: i64,
    dataset: String,
    created_at: String,
) -> Value {
    let object = match &mut value {
        Value::Object(object) => object,
        _ => {
            value = Value::Object(Map::new());
            value.as_object_mut().expect("object value")
        }
    };

    object.insert("index".to_owned(), Value::String(index_name));
    object.insert("chain".to_owned(), Value::String(chain_name));
    object.insert("chain_family".to_owned(), Value::String(chain_family));
    object.insert("chain_id".to_owned(), Value::from(chain_id));
    object.insert("dataset".to_owned(), Value::String(dataset));
    object.insert("created_at".to_owned(), Value::String(created_at));
    value
}

fn unique_key(
    record: &IndexedRecord,
    chain_identity: &str,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    event_index: Option<i64>,
    selector: Option<&str>,
) -> String {
    if let Some(value) = json_string(&record.payload, "unique_key") {
        return format!("{chain_identity}:{}:{value}", record.dataset);
    }
    if let (Some(block_hash), Some(transaction_hash), Some(event_index)) =
        (block_hash, transaction_hash, event_index)
    {
        return format!(
            "{chain_identity}:{}:{block_hash}:{transaction_hash}:{event_index}",
            record.dataset
        );
    }
    format!(
        "{}:{}:{}:{}:{}:{}",
        chain_identity,
        record.dataset,
        json_u64(&record.payload, "block_number").unwrap_or_default(),
        json_u64(&record.payload, "transaction_index").unwrap_or_default(),
        event_index.unwrap_or_default(),
        selector.unwrap_or_default()
    )
}

fn chain_family(record: &IndexedRecord) -> String {
    record
        .dataset
        .split_once('.')
        .map(|(family, _)| family.to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn json_u64(payload: &Value, field: &str) -> Option<u64> {
    payload.get(field).and_then(Value::as_u64)
}

fn json_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
}
