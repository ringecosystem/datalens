#![allow(dead_code)]

use datalens_core::{BlockRange, ChainId, ChainName};
use datalens_store::Store;
use sqlx::PgPool;

pub async fn store(pool: &PgPool) -> Store {
    let store = Store::new(pool.clone());
    store
        .ensure_chain(
            ChainId::new(1).unwrap(),
            &ChainName::new("ethereum").unwrap(),
        )
        .await
        .unwrap();
    store
}

pub fn chain() -> ChainId {
    ChainId::new(1).unwrap()
}

pub fn range(start: u64, end: u64) -> BlockRange {
    BlockRange::new(start, end).unwrap()
}
