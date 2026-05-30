use crate::IndexerError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreQuery {
    pub dataset: String,
    pub filter: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreQueryResult {
    pub rows: Vec<serde_json::Value>,
}

pub trait QueryableStore {
    fn query(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError>;
}
