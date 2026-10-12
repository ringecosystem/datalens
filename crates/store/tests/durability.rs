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

#[sqlx::test]
async fn reorg_deletes_only_unsafe_senders_at_or_above_the_fork(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let sender = Address::repeat_byte(0x33);
    let below = B256::repeat_byte(0x21);
    let unsafe_at_fork = B256::repeat_byte(0x22);
    let safe_at_fork = B256::repeat_byte(0x23);

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_senders(
            &mut tx,
            chain,
            &[
                (below, 4, sender, Finality::Unsafe),
                (unsafe_at_fork, 10, sender, Finality::Unsafe),
                (safe_at_fork, 10, sender, Finality::Safe),
            ],
        )
        .await
        .unwrap();
    store
        .delete_unsafe_senders_from(&mut tx, chain, 10)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        store
            .missing_hashes(chain, &[below, unsafe_at_fork, safe_at_fork])
            .await
            .unwrap(),
        vec![unsafe_at_fork]
    );
    assert_eq!(
        store.senders(chain, &[below, safe_at_fork]).await.unwrap(),
        [(below, sender), (safe_at_fork, sender)]
            .into_iter()
            .collect()
    );
}

#[sqlx::test]
async fn a_stronger_insert_or_promote_raises_sender_finality(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let sender = Address::repeat_byte(0x33);
    let other = Address::repeat_byte(0x44);
    let raised = B256::repeat_byte(0x31);
    let conflict = B256::repeat_byte(0x32);
    let neighbor = B256::repeat_byte(0x33);
    let outside = B256::repeat_byte(0x34);

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_senders(
            &mut tx,
            chain,
            &[
                (raised, 8, sender, Finality::Unsafe),
                (conflict, 8, sender, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    store
        .insert_senders(
            &mut tx,
            chain,
            &[
                (raised, 8, sender, Finality::Safe),
                (conflict, 40, other, Finality::Finalized),
            ],
        )
        .await
        .unwrap();
    store
        .insert_senders(&mut tx, chain, &[(raised, 8, sender, Finality::Unsafe)])
        .await
        .unwrap();
    store
        .delete_unsafe_senders_from(&mut tx, chain, 8)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store
            .missing_hashes(chain, &[raised, conflict])
            .await
            .unwrap(),
        vec![conflict]
    );
    assert_eq!(
        store.senders(chain, &[raised]).await.unwrap(),
        [(raised, sender)].into_iter().collect()
    );

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_senders(
            &mut tx,
            chain,
            &[
                (neighbor, 9, sender, Finality::Unsafe),
                (outside, 30, sender, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    let err = store
        .promote_senders(&mut tx, chain, range(8, 9), Finality::Unsafe)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidInput(_)), "{err:?}");
    let promoted = store
        .promote_senders(&mut tx, chain, range(8, 9), Finality::Finalized)
        .await
        .unwrap();
    let again = store
        .promote_senders(&mut tx, chain, range(8, 9), Finality::Finalized)
        .await
        .unwrap();
    store
        .delete_unsafe_senders_from(&mut tx, chain, 0)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(promoted, 2);
    assert_eq!(again, 0);
    assert_eq!(
        store
            .missing_hashes(chain, &[raised, neighbor, outside])
            .await
            .unwrap(),
        vec![outside]
    );
    assert_eq!(
        store.senders(chain, &[raised, neighbor]).await.unwrap(),
        [(raised, sender), (neighbor, sender)].into_iter().collect()
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
