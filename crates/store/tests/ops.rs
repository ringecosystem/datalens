#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use datalens_core::{B256, Dataset, Heads, Selector};
use datalens_store::{HeadKind, SubscriptionSource, UsageDelta, UsageTotals};
use sqlx::PgPool;

use common::{chain, store};

#[sqlx::test]
async fn heads_round_trip_and_wait_wakes_on_update(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let heads = Heads {
        latest: 10,
        latest_hash: B256::repeat_byte(0x44),
        safe: 8,
        finalized: 6,
    };
    store.upsert_heads(chain, &heads).await.unwrap();
    assert_eq!(store.heads(chain).await.unwrap(), Some(heads));
    assert_eq!(store.all_heads().await.unwrap(), vec![(chain, heads)]);
    assert_eq!(
        store
            .wait_head_change(chain, 7, HeadKind::Safe, Duration::from_secs(2))
            .await
            .unwrap(),
        heads
    );

    let waiting = store.clone();
    let handle = tokio::spawn(async move {
        waiting
            .wait_head_change(chain, 10, HeadKind::Latest, Duration::from_secs(5))
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let next = Heads {
        latest: 11,
        latest_hash: B256::repeat_byte(0x45),
        ..heads
    };
    store.upsert_heads(chain, &next).await.unwrap();
    assert_eq!(handle.await.unwrap().unwrap(), next);
}

#[sqlx::test]
async fn subscriptions_touch_list_remove_and_expire(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();
    store
        .touch_subscription(
            chain,
            Dataset::EvmBlocks,
            selector,
            "degov",
            SubscriptionSource::Auto,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
    let active = store.active_subscriptions(chain).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].application_id, "degov");
    assert_eq!(active[0].source, SubscriptionSource::Auto);
    assert_eq!(active[0].selector_id, selector);

    assert_eq!(
        store
            .expire_subscriptions(Utc::now() - ChronoDuration::days(1))
            .await
            .unwrap(),
        0
    );
    assert_eq!(store.active_subscriptions(chain).await.unwrap().len(), 1);
    assert_eq!(
        store
            .expire_subscriptions(Utc::now() + ChronoDuration::hours(2))
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.active_subscriptions(chain).await.unwrap(), Vec::new());

    store
        .touch_subscription(
            chain,
            Dataset::EvmBlocks,
            selector,
            "degov",
            SubscriptionSource::Explicit,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
    store
        .remove_subscription(chain, Dataset::EvmBlocks, selector, "degov")
        .await
        .unwrap();
    assert_eq!(store.active_subscriptions(chain).await.unwrap(), Vec::new());
}

#[sqlx::test]
async fn follow_inits_advances_and_rewinds(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();
    let hash = B256::repeat_byte(0x09);
    let row = store
        .follow_get_or_init(chain, Dataset::EvmBlocks, selector, 100, &hash)
        .await
        .unwrap();
    assert_eq!(row.from_block, 100);
    assert_eq!(row.head_block, 99);
    assert_eq!(row.promoted_to, 99);
    assert_eq!(row.head_hash, hash);

    let again = store
        .follow_get_or_init(
            chain,
            Dataset::EvmBlocks,
            selector,
            500,
            &B256::repeat_byte(0x01),
        )
        .await
        .unwrap();
    assert_eq!(again.from_block, 100);
    assert_eq!(again.head_hash, hash);

    let mut tx = pool.begin().await.unwrap();
    let row = store
        .advance_follow_head(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            110,
            &B256::repeat_byte(0x02),
        )
        .await
        .unwrap();
    assert_eq!(row.head_block, 110);
    assert_eq!(row.head_hash, B256::repeat_byte(0x02));
    let row = store
        .advance_follow_promoted(&mut tx, chain, Dataset::EvmBlocks, selector, 105)
        .await
        .unwrap();
    assert_eq!(row.promoted_to, 105);
    let rewound = store
        .rewind_follow(&mut tx, chain, 108, &B256::repeat_byte(0x07))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rewound.len(), 1);
    assert_eq!(rewound[0].head_block, 107);
    assert_eq!(rewound[0].promoted_to, 105);
    assert_eq!(rewound[0].head_hash, B256::repeat_byte(0x07));

    let listed = store.list_follow(chain).await.unwrap();
    assert_eq!(listed, rewound);

    let genesis = store
        .upsert_selector(
            chain,
            &Selector::Logs(datalens_core::LogSelector::default()),
        )
        .await
        .unwrap();
    let row = store
        .follow_get_or_init(chain, Dataset::EvmLogs, genesis, 0, &B256::ZERO)
        .await
        .unwrap();
    assert_eq!(row.head_block, -1);
    assert_eq!(row.promoted_to, -1);
}

#[sqlx::test]
async fn usage_increments_accumulate(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let day = NaiveDate::from_ymd_opt(2026, 10, 12).unwrap();
    let delta = |queries, rows| UsageDelta {
        day,
        application_id: "degov".to_owned(),
        chain_id: chain,
        dataset: Dataset::EvmLogs,
        queries,
        rows_returned: rows,
        blocks_served: 4,
        blocks_filled: 1,
        provider_calls: 2,
    };
    store.add_usage(&[delta(1, 3), delta(4, 5)]).await.unwrap();
    store.add_usage(&[delta(2, 7)]).await.unwrap();
    assert_eq!(
        store
            .usage(day, "degov", chain, Dataset::EvmLogs)
            .await
            .unwrap(),
        UsageTotals {
            queries: 7,
            rows_returned: 15,
            blocks_served: 12,
            blocks_filled: 3,
            provider_calls: 6,
        }
    );
    assert_eq!(
        store
            .usage(day, "ormp", chain, Dataset::EvmLogs)
            .await
            .unwrap(),
        UsageTotals::default()
    );
}
