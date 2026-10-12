use datalens_core::{LogSelector, TopicFilter};
use serde_json::{Map, Value, json};

pub(crate) fn log_filter(selector: &LogSelector, start: u64, end: u64) -> Value {
    let mut filter = Map::new();
    filter.insert("fromBlock".to_owned(), json!(format!("0x{start:x}")));
    filter.insert("toBlock".to_owned(), json!(format!("0x{end:x}")));
    if !selector.addresses.is_empty() {
        let addresses: Vec<String> = selector
            .addresses
            .iter()
            .map(|address| format!("{address:#x}"))
            .collect();
        let value = if addresses.len() == 1 {
            json!(addresses[0])
        } else {
            json!(addresses)
        };
        filter.insert("address".to_owned(), value);
    }
    let mut topics: Vec<Value> = selector.topics.iter().map(topic_value).collect();
    while topics.last().is_some_and(Value::is_null) {
        topics.pop();
    }
    if !topics.is_empty() {
        filter.insert("topics".to_owned(), Value::Array(topics));
    }
    Value::Object(filter)
}

fn topic_value(filter: &TopicFilter) -> Value {
    match filter {
        TopicFilter::Any => Value::Null,
        TopicFilter::OneOf(set) => Value::Array(
            set.iter()
                .map(|topic| json!(format!("{topic:#x}")))
                .collect(),
        ),
    }
}
