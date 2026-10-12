#[path = "common/mod.rs"]
mod common;

use datalens_core::{Address, B256, BlockRow, Bytes, Finality, LogRow, LogSelector, TopicFilter};
use datalens_store::StoreError;
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

const CANON: B256 = B256::repeat_byte(0x11);
const FORK: B256 = B256::repeat_byte(0x99);

#[sqlx::test]
async fn a_durable_write_raises_an_unsafe_log_and_a_new_hash_is_a_reorg(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let a = Address::repeat_byte(0x0a);

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_logs(&mut tx, chain, &[log(5, 0, a, CANON, Finality::Unsafe)])
        .await
        .unwrap();
    store
        .insert_logs(&mut tx, chain, &[log(5, 0, a, CANON, Finality::Safe)])
        .await
        .unwrap();
    store
        .insert_logs(&mut tx, chain, &[log(5, 0, a, CANON, Finality::Unsafe)])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        read(&store, &any()).await,
        vec![log(5, 0, a, CANON, Finality::Safe)]
    );

    let mut tx = pool.begin().await.unwrap();
    let err = store
        .insert_logs(&mut tx, chain, &[log(5, 1, a, FORK, Finality::Safe)])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 5 }), "{err:?}");
    tx.rollback().await.unwrap();
    assert_eq!(
        read(&store, &any()).await,
        vec![log(5, 0, a, CANON, Finality::Safe)]
    );
}

#[sqlx::test]
async fn a_log_hash_that_disagrees_with_the_stored_block_is_a_reorg(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_blocks(&mut tx, chain, &[block(7, CANON, FORK, Finality::Unsafe)])
        .await
        .unwrap();
    let err = store
        .insert_logs(
            &mut tx,
            chain,
            &[log(7, 0, Address::ZERO, FORK, Finality::Unsafe)],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 7 }), "{err:?}");
}

#[sqlx::test]
async fn a_durable_write_raises_an_unsafe_block_and_a_new_hash_is_a_reorg(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_blocks(&mut tx, chain, &[block(9, CANON, FORK, Finality::Unsafe)])
        .await
        .unwrap();
    store
        .insert_blocks(
            &mut tx,
            chain,
            &[block(9, CANON, FORK, Finality::Finalized)],
        )
        .await
        .unwrap();
    let err = store
        .insert_blocks(&mut tx, chain, &[block(9, FORK, FORK, Finality::Safe)])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 9 }), "{err:?}");
    tx.rollback().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_blocks(&mut tx, chain, &[block(9, CANON, FORK, Finality::Unsafe)])
        .await
        .unwrap();
    store
        .insert_blocks(&mut tx, chain, &[block(9, CANON, FORK, Finality::Safe)])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let (rows, _) = store
        .read_blocks(chain, range(9, 9), None, 10)
        .await
        .unwrap();
    assert_eq!(rows, vec![block(9, CANON, FORK, Finality::Safe)]);
}

#[sqlx::test]
async fn promotion_raises_only_the_selector_rows_and_refuses_a_broken_hash(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let a = Address::repeat_byte(0x0a);
    let b = Address::repeat_byte(0x0b);
    let h3 = B256::repeat_byte(0x33);
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_logs(
            &mut tx,
            chain,
            &[
                log(1, 0, a, CANON, Finality::Unsafe),
                log(2, 0, b, CANON, Finality::Unsafe),
                log(3, 0, a, h3, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = store
        .promote_logs(&mut tx, chain, &only(a), range(1, 3), Finality::Safe, FORK)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 3 }), "{err:?}");
    tx.rollback().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let promoted = store
        .promote_logs(&mut tx, chain, &only(a), range(1, 3), Finality::Safe, h3)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(promoted, 2);
    assert_eq!(
        read(&store, &any()).await,
        vec![
            log(1, 0, a, CANON, Finality::Safe),
            log(2, 0, b, CANON, Finality::Unsafe),
            log(3, 0, a, h3, Finality::Safe),
        ]
    );

    let mut tx = pool.begin().await.unwrap();
    let err = store
        .insert_blocks(&mut tx, chain, &[block(2, FORK, CANON, Finality::Unsafe)])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 2 }), "{err:?}");
    tx.rollback().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "insert into evm_blocks (chain_id, number, hash, parent_hash, timestamp, finality, header)
         values (1, 2, $1, $2, 0, 0, '{}')",
    )
    .bind(FORK.as_slice())
    .bind(CANON.as_slice())
    .execute(&mut *tx)
    .await
    .unwrap();
    let err = store
        .promote_logs(&mut tx, chain, &only(b), range(1, 3), Finality::Safe, h3)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 2 }), "{err:?}");
}

#[sqlx::test]
async fn block_promotion_checks_parent_links_and_the_end_hash(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let h = |byte| B256::repeat_byte(byte);
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_blocks(
            &mut tx,
            chain,
            &[
                block(20, h(0x20), h(0x19), Finality::Safe),
                block(21, h(0x21), h(0x20), Finality::Unsafe),
                block(22, h(0x22), h(0x21), Finality::Unsafe),
                block(23, h(0x23), h(0xee), Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = store
        .promote_blocks(&mut tx, chain, range(21, 22), Finality::Safe, h(0x99))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 22 }), "{err:?}");
    let err = store
        .promote_blocks(&mut tx, chain, range(21, 23), Finality::Safe, h(0x23))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Reorg { block: 23 }), "{err:?}");
    let promoted = store
        .promote_blocks(&mut tx, chain, range(20, 22), Finality::Safe, h(0x22))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(promoted, 2);
    let (rows, _) = store
        .read_blocks(chain, range(20, 23), None, 10)
        .await
        .unwrap();
    let finality: Vec<Finality> = rows.iter().map(|row| row.finality).collect();
    assert_eq!(
        finality,
        vec![
            Finality::Safe,
            Finality::Safe,
            Finality::Safe,
            Finality::Unsafe
        ]
    );
}

async fn read(store: &datalens_store::Store, selector: &LogSelector) -> Vec<LogRow> {
    let (logs, cursor) = store
        .read_logs(chain(), selector, range(0, 100), None, 50)
        .await
        .unwrap();
    assert_eq!(cursor, None);
    logs
}

fn any() -> LogSelector {
    LogSelector {
        addresses: Default::default(),
        topics: Default::default(),
    }
}

fn only(address: Address) -> LogSelector {
    LogSelector {
        addresses: [address].into_iter().collect(),
        topics: [
            TopicFilter::Any,
            TopicFilter::Any,
            TopicFilter::Any,
            TopicFilter::Any,
        ],
    }
}

fn log(block: u64, index: u32, address: Address, hash: B256, finality: Finality) -> LogRow {
    LogRow {
        block_number: block,
        log_index: index,
        block_hash: hash,
        transaction_index: 0,
        transaction_hash: B256::repeat_byte(0x22),
        address,
        topics: Vec::new(),
        data: Bytes::from_static(&[0xab]),
        finality,
    }
}

fn block(number: u64, hash: B256, parent_hash: B256, finality: Finality) -> BlockRow {
    BlockRow {
        number,
        hash,
        parent_hash,
        timestamp: 1_700_000_000 + number,
        finality,
        header: json!({}),
    }
}
