#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use datalens_core::{
    Address, B256, BlockRow, Bytes, Dataset, Finality, LogRow, LogSelector, Selector,
};
use datalens_store::{SelectorId, SubscriptionSource};
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

const CANON: B256 = B256::repeat_byte(0x11);
const FORK: B256 = B256::repeat_byte(0x99);

#[sqlx::test]
async fn unsafe_hashes_lists_each_unsafe_height_and_hash_once(pool: PgPool) {
    let store = store(&pool).await;
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_logs(
            &mut tx,
            chain(),
            &[
                log(5, 0, CANON, Finality::Unsafe),
                log(5, 1, CANON, Finality::Unsafe),
                log(6, 0, CANON, Finality::Safe),
                log(8, 0, CANON, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    store
        .insert_blocks(&mut tx, chain(), &[block(7, FORK, Finality::Unsafe)])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        store.unsafe_hashes(chain(), range(0, 7)).await.unwrap(),
        vec![(5, CANON), (7, FORK)]
    );
}

#[sqlx::test]
async fn delete_unsafe_through_keeps_durable_rows_and_higher_unsafe_rows(pool: PgPool) {
    let store = store(&pool).await;
    let sender = Address::repeat_byte(0x0b);
    let (low, high, durable) = (
        B256::repeat_byte(0x01),
        B256::repeat_byte(0x02),
        B256::repeat_byte(0x03),
    );
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_logs(
            &mut tx,
            chain(),
            &[
                log(5, 0, CANON, Finality::Unsafe),
                log(6, 0, CANON, Finality::Safe),
                log(9, 0, CANON, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    store
        .insert_blocks(&mut tx, chain(), &[block(5, CANON, Finality::Unsafe)])
        .await
        .unwrap();
    store
        .insert_senders(
            &mut tx,
            chain(),
            &[
                (low, 5, sender, Finality::Unsafe),
                (durable, 6, sender, Finality::Safe),
                (high, 9, sender, Finality::Unsafe),
            ],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(store.delete_unsafe_through(chain(), 7).await.unwrap(), 3);
    let (logs, _) = store
        .read_logs(chain(), &LogSelector::default(), range(0, 10), None, 10)
        .await
        .unwrap();
    let blocks: Vec<u64> = logs.iter().map(|log| log.block_number).collect();
    assert_eq!(blocks, vec![6, 9]);
    assert_eq!(store.hash_at(chain(), 5).await.unwrap(), None);
    let mut kept: Vec<B256> = store
        .senders(chain(), &[low, durable, high])
        .await
        .unwrap()
        .into_keys()
        .collect();
    kept.sort();
    assert_eq!(kept, vec![high, durable]);
}

#[sqlx::test]
async fn delete_unwatched_follow_drops_progress_without_a_live_subscription(pool: PgPool) {
    let store = store(&pool).await;
    let watched = logs_selector(&store, 0x0a).await;
    let lapsed = logs_selector(&store, 0x0b).await;
    for id in [watched, lapsed] {
        store
            .follow_get_or_init(chain(), Dataset::EvmLogs, id, 100, &B256::ZERO)
            .await
            .unwrap();
    }
    store
        .touch_subscription(
            chain(),
            Dataset::EvmLogs,
            watched,
            "degov",
            SubscriptionSource::Auto,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    store
        .touch_subscription(
            chain(),
            Dataset::EvmLogs,
            lapsed,
            "degov",
            SubscriptionSource::Auto,
            Duration::ZERO,
        )
        .await
        .unwrap();

    assert_eq!(store.delete_unwatched_follow(chain()).await.unwrap(), 1);
    let left: Vec<SelectorId> = store
        .list_follow(chain())
        .await
        .unwrap()
        .into_iter()
        .map(|progress| progress.selector_id)
        .collect();
    assert_eq!(left, vec![watched]);
}

#[sqlx::test]
async fn application_subscriptions_lists_only_that_applications_live_rows(pool: PgPool) {
    let store = store(&pool).await;
    let a = logs_selector(&store, 0x0a).await;
    let b = logs_selector(&store, 0x0b).await;
    for (id, application, ttl) in [
        (a, "degov", Duration::from_secs(60)),
        (b, "degov", Duration::ZERO),
        (b, "ormp", Duration::from_secs(60)),
    ] {
        store
            .touch_subscription(
                chain(),
                Dataset::EvmLogs,
                id,
                application,
                SubscriptionSource::Explicit,
                ttl,
            )
            .await
            .unwrap();
    }
    let live: Vec<(SelectorId, String)> = store
        .application_subscriptions("degov")
        .await
        .unwrap()
        .into_iter()
        .map(|sub| (sub.selector_id, sub.application_id))
        .collect();
    assert_eq!(live, vec![(a, "degov".to_owned())]);
}

async fn logs_selector(store: &datalens_store::Store, byte: u8) -> SelectorId {
    let selector = LogSelector {
        addresses: [Address::repeat_byte(byte)].into(),
        ..LogSelector::default()
    };
    store
        .upsert_selector(chain(), &Selector::Logs(selector))
        .await
        .unwrap()
}

fn log(block: u64, index: u32, hash: B256, finality: Finality) -> LogRow {
    LogRow {
        block_number: block,
        log_index: index,
        block_hash: hash,
        transaction_index: 0,
        transaction_hash: B256::repeat_byte(0x22),
        address: Address::repeat_byte(0x0a),
        topics: Vec::new(),
        data: Bytes::from_static(&[0xab]),
        finality,
    }
}

fn block(number: u64, hash: B256, finality: Finality) -> BlockRow {
    BlockRow {
        number,
        hash,
        parent_hash: B256::ZERO,
        timestamp: 1_700_000_000 + number,
        finality,
        header: json!({}),
    }
}
