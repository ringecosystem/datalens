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

pub trait QueryableStore: Send + Sync {
    fn query(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError>;

    fn query_decoded_events(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        let _ = query;
        Err(IndexerError::Runner(
            "decoded event queries are not supported by this output store".to_owned(),
        ))
    }
}
