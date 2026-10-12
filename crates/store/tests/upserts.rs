#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use datalens_core::{Address, B256, Dataset, Heads, LogSelector, Selector};
use datalens_store::{SelectorId, Store, SubscriptionSource};
use sqlx::PgPool;

use common::{chain, store};

#[sqlx::test]
async fn safe_and_finalized_never_move_back(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let heads = Heads {
        latest: 10,
        latest_hash: B256::repeat_byte(0x44),
        safe: 8,
        finalized: 6,
    };
    store.upsert_heads(chain, &heads).await.unwrap();
    store
        .upsert_heads(
            chain,
            &Heads {
                latest: 9,
                latest_hash: B256::repeat_byte(0x43),
                safe: 7,
                finalized: 5,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.heads(chain).await.unwrap(),
        Some(Heads {
            latest: 9,
            latest_hash: B256::repeat_byte(0x43),
            safe: 8,
            finalized: 6,
        })
    );
}

#[sqlx::test]
async fn an_auto_touch_keeps_an_explicit_subscription_and_its_expiry(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    touch(&store, selector, SubscriptionSource::Explicit, 3600).await;
    let before = expiry(&store).await;
    touch(&store, selector, SubscriptionSource::Auto, 60).await;
    let active = store.active_subscriptions(chain()).await.unwrap();
    assert_eq!(active[0].source, SubscriptionSource::Explicit);
    assert_eq!(active[0].expires_at, before);

    touch(&store, selector, SubscriptionSource::Explicit, 7200).await;
    let extended = expiry(&store).await;
    assert!(extended - before > chrono::Duration::seconds(3500));
}

#[sqlx::test]
async fn auto_renewals_write_at_most_once_a_minute(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    touch(&store, selector, SubscriptionSource::Auto, 900).await;
    let first = expiry(&store).await;
    touch(&store, selector, SubscriptionSource::Auto, 1800).await;
    assert_eq!(expiry(&store).await, first);

    sqlx::query("update subscriptions set last_query_at = now() - interval '2 minutes'")
        .execute(&pool)
        .await
        .unwrap();
    touch(&store, selector, SubscriptionSource::Auto, 1800).await;
    assert!(expiry(&store).await - first > chrono::Duration::seconds(800));
}

#[sqlx::test]
async fn repeated_upserts_return_one_id_without_burning_the_sequence(pool: PgPool) {
    let store = store(&pool).await;
    let first = blocks_selector(&store).await;
    for _ in 0..3 {
        assert_eq!(blocks_selector(&store).await, first);
    }
    let next = store
        .upsert_selector(
            chain(),
            &Selector::Logs(LogSelector {
                addresses: [Address::repeat_byte(0x0a)].into_iter().collect(),
                topics: Default::default(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(next.get(), first.get() + 1);
}

async fn blocks_selector(store: &Store) -> SelectorId {
    store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap()
}

async fn touch(store: &Store, selector: SelectorId, source: SubscriptionSource, secs: u64) {
    store
        .touch_subscription(
            chain(),
            Dataset::EvmBlocks,
            selector,
            "degov",
            source,
            Duration::from_secs(secs),
        )
        .await
        .unwrap();
}

async fn expiry(store: &Store) -> DateTime<Utc> {
    let active = store.active_subscriptions(chain()).await.unwrap();
    assert_eq!(active.len(), 1);
    active[0].expires_at
}
