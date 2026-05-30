use serde_json::Value;

use super::StoreQuery;

#[derive(Clone, Debug, Default)]
pub(super) struct StoreQueryFilter {
    pub index: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<i64>,
    pub selector: Option<String>,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub transaction_hash: Option<String>,
    pub signature: Option<String>,
    pub event_name: Option<String>,
    pub topic0: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl StoreQueryFilter {
    pub(super) fn from_query(query: &StoreQuery) -> Self {
        let filter = query.filter.as_object();
        Self {
            index: string_filter(filter, "index"),
            chain: string_filter(filter, "chain"),
            chain_id: u64_filter(filter, "chain_id").and_then(|value| i64::try_from(value).ok()),
            selector: filter
                .and_then(|filter| filter.get("address").or_else(|| filter.get("selector")))
                .and_then(Value::as_str)
                .map(str::to_owned),
            from_block: u64_filter(filter, "from_block")
                .and_then(|value| i64::try_from(value).ok()),
            to_block: u64_filter(filter, "to_block").and_then(|value| i64::try_from(value).ok()),
            transaction_hash: string_filter(filter, "transaction_hash"),
            signature: filter
                .and_then(|filter| filter.get("signature").or_else(|| filter.get("topic")))
                .and_then(Value::as_str)
                .map(str::to_owned),
            event_name: string_filter(filter, "event_name"),
            topic0: string_filter(filter, "topic0"),
            limit: u64_filter(filter, "limit"),
            offset: u64_filter(filter, "after"),
        }
    }
}

fn string_filter(filter: Option<&serde_json::Map<String, Value>>, field: &str) -> Option<String> {
    filter
        .and_then(|filter| filter.get(field))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn u64_filter(filter: Option<&serde_json::Map<String, Value>>, field: &str) -> Option<u64> {
    filter
        .and_then(|filter| filter.get(field))
        .and_then(Value::as_u64)
}
