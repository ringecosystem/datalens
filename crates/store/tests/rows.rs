#[path = "common/mod.rs"]
mod common;

use datalens_core::{Address, B256, BlockRow, Bytes, Finality, LogRow, LogSelector, TopicFilter};
use datalens_store::{BlockCursor, LogCursor};
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

#[sqlx::test]
async fn logs_are_idempotent_filtered_paged_promoted_and_reorged(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let a = Address::repeat_byte(0x0a);
    let b = Address::repeat_byte(0x0b);
    let t1 = B256::repeat_byte(0x01);
    let t2 = B256::repeat_byte(0x02);
    let logs = vec![
        log(2, 0, a, Some(t2), Finality::Unsafe),
        log(1, 1, b, Some(t1), Finality::Unsafe),
        log(1, 0, a, Some(t1), Finality::Unsafe),
        log(3, 0, a, Some(t1), Finality::Safe),
        log(4, 0, b, Some(t2), Finality::Finalized),
    ];
    let mut tx = pool.begin().await.unwrap();
    store.insert_logs(&mut tx, chain, &logs).await.unwrap();
    store.insert_logs(&mut tx, chain, &logs).await.unwrap();
    tx.commit().await.unwrap();

    let all = read_all(&store, chain, &any_selector()).await;
    assert_eq!(
        all,
        vec![
            log(1, 0, a, Some(t1), Finality::Unsafe),
            log(1, 1, b, Some(t1), Finality::Unsafe),
            log(2, 0, a, Some(t2), Finality::Unsafe),
            log(3, 0, a, Some(t1), Finality::Safe),
            log(4, 0, b, Some(t2), Finality::Finalized),
        ]
    );

    let only_a = read_all(&store, chain, &selector(&[a], TopicFilter::Any)).await;
    assert_eq!(
        only_a,
        vec![
            log(1, 0, a, Some(t1), Finality::Unsafe),
            log(2, 0, a, Some(t2), Finality::Unsafe),
            log(3, 0, a, Some(t1), Finality::Safe),
        ]
    );
    let only_t1 = read_all(
        &store,
        chain,
        &selector(&[], TopicFilter::one_of([t1]).unwrap()),
    )
    .await;
    assert_eq!(
        only_t1,
        vec![
            log(1, 0, a, Some(t1), Finality::Unsafe),
            log(1, 1, b, Some(t1), Finality::Unsafe),
            log(3, 0, a, Some(t1), Finality::Safe),
        ]
    );

    let (page, cursor) = store
        .read_logs(chain, &any_selector(), range(1, 4), None, 2)
        .await
        .unwrap();
    assert_eq!(
        page,
        vec![
            log(1, 0, a, Some(t1), Finality::Unsafe),
            log(1, 1, b, Some(t1), Finality::Unsafe),
        ]
    );
    assert_eq!(
        cursor,
        Some(LogCursor {
            block_number: 1,
            log_index: 1,
        })
    );
    let (page, cursor) = store
        .read_logs(chain, &any_selector(), range(1, 4), cursor, 2)
        .await
        .unwrap();
    assert_eq!(
        page,
        vec![
            log(2, 0, a, Some(t2), Finality::Unsafe),
            log(3, 0, a, Some(t1), Finality::Safe),
        ]
    );
    assert_eq!(
        cursor,
        Some(LogCursor {
            block_number: 3,
            log_index: 0,
        })
    );
    let (page, cursor) = store
        .read_logs(chain, &any_selector(), range(1, 4), cursor, 2)
        .await
        .unwrap();
    assert_eq!(page, vec![log(4, 0, b, Some(t2), Finality::Finalized)]);
    assert_eq!(cursor, None);

    let mut tx = pool.begin().await.unwrap();
    store
        .delete_unsafe_logs_from(&mut tx, chain, 2)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        read_all(&store, chain, &any_selector()).await,
        vec![
            log(1, 0, a, Some(t1), Finality::Unsafe),
            log(1, 1, b, Some(t1), Finality::Unsafe),
            log(3, 0, a, Some(t1), Finality::Safe),
            log(4, 0, b, Some(t2), Finality::Finalized),
        ]
    );

    let mut tx = pool.begin().await.unwrap();
    store
        .promote_logs(
            &mut tx,
            chain,
            &any_selector(),
            range(1, 3),
            Finality::Safe,
            B256::repeat_byte(0x11),
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        read_all(&store, chain, &any_selector()).await,
        vec![
            log(1, 0, a, Some(t1), Finality::Safe),
            log(1, 1, b, Some(t1), Finality::Safe),
            log(3, 0, a, Some(t1), Finality::Safe),
            log(4, 0, b, Some(t2), Finality::Finalized),
        ]
    );
}

#[sqlx::test]
async fn blocks_round_trip_hash_promote_and_delete_unsafe(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let parent = B256::repeat_byte(0x10);
    let hash = B256::repeat_byte(0x11);
    let later = B256::repeat_byte(0x12);
    let rows = vec![
        block(10, hash, parent, Finality::Unsafe),
        block(11, later, hash, Finality::Safe),
    ];
    let mut tx = pool.begin().await.unwrap();
    store.insert_blocks(&mut tx, chain, &rows).await.unwrap();
    store.insert_blocks(&mut tx, chain, &rows).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(store.hash_at(chain, 10).await.unwrap(), Some(hash));
    assert_eq!(store.hash_at(chain, 12).await.unwrap(), None);
    let (page, cursor) = store
        .read_blocks(chain, range(10, 11), None, 1)
        .await
        .unwrap();
    assert_eq!(page, vec![block(10, hash, parent, Finality::Unsafe)]);
    assert_eq!(cursor, Some(BlockCursor { number: 10 }));

    let mut tx = pool.begin().await.unwrap();
    store
        .promote_blocks(&mut tx, chain, range(10, 10), Finality::Finalized, hash)
        .await
        .unwrap();
    store
        .delete_unsafe_blocks_from(&mut tx, chain, 10)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let (page, cursor) = store
        .read_blocks(chain, range(10, 11), None, 10)
        .await
        .unwrap();
    assert_eq!(
        page,
        vec![
            block(10, hash, parent, Finality::Finalized),
            block(11, later, hash, Finality::Safe),
        ]
    );
    assert_eq!(cursor, None);
}

#[sqlx::test]
async fn senders_fill_missing_hashes_and_drop_from_a_block(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let h1 = B256::repeat_byte(0x21);
    let h2 = B256::repeat_byte(0x22);
    let sender = Address::repeat_byte(0x33);
    assert_eq!(
        store.missing_hashes(chain, &[h1, h2, h1]).await.unwrap(),
        vec![h1, h2]
    );

    let mut tx = pool.begin().await.unwrap();
    store
        .insert_senders(
            &mut tx,
            chain,
            &[
                (h1, 5, sender, Finality::Unsafe),
                (h2, 10, sender, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    store
        .insert_senders(&mut tx, chain, &[(h1, 5, sender, Finality::Unsafe)])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.missing_hashes(chain, &[h1, h2]).await.unwrap(),
        Vec::<B256>::new()
    );
    assert_eq!(
        store.senders(chain, &[h2, h1]).await.unwrap(),
        [(h1, sender), (h2, sender)].into_iter().collect()
    );

    let mut tx = pool.begin().await.unwrap();
    store
        .delete_unsafe_senders_from(&mut tx, chain, 10)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.missing_hashes(chain, &[h1, h2]).await.unwrap(),
        vec![h2]
    );
    assert_eq!(
        store.senders(chain, &[h1, h2]).await.unwrap(),
        [(h1, sender)].into_iter().collect()
    );
}

async fn read_all(
    store: &datalens_store::Store,
    chain: datalens_core::ChainId,
    selector: &LogSelector,
) -> Vec<LogRow> {
    let (logs, cursor) = store
        .read_logs(chain, selector, range(1, 4), None, 20)
        .await
        .unwrap();
    assert_eq!(cursor, None);
    logs
}

fn any_selector() -> LogSelector {
    selector(&[], TopicFilter::Any)
}

fn selector(addresses: &[Address], topic0: TopicFilter) -> LogSelector {
    let mut topics = [
        TopicFilter::Any,
        TopicFilter::Any,
        TopicFilter::Any,
        TopicFilter::Any,
    ];
    topics[0] = topic0;
    LogSelector {
        addresses: addresses.iter().copied().collect(),
        topics,
    }
}

fn log(
    block: u64,
    index: u32,
    address: Address,
    topic0: Option<B256>,
    finality: Finality,
) -> LogRow {
    LogRow {
        block_number: block,
        log_index: index,
        block_hash: B256::repeat_byte(0x11),
        transaction_index: 0,
        transaction_hash: B256::repeat_byte(0x22),
        address,
        topics: topic0.into_iter().collect(),
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
        header: json!({"gas": number}),
    }
}
