#[path = "common/mod.rs"]
mod common;

use datalens_core::{B256, ChainId, Dataset, LogSelector, Selector};
use datalens_store::{SelectorId, Store, StoreError};
use sqlx::PgPool;

use common::{chain, store};

const PARENT: B256 = B256::repeat_byte(0x07);

async fn followed(store: &Store, pool: &PgPool, head: u64, promoted: u64) -> SelectorId {
    let selector = store
        .upsert_selector(chain(), &Selector::Logs(LogSelector::default()))
        .await
        .unwrap();
    store
        .follow_get_or_init(chain(), Dataset::EvmLogs, selector, 100, &B256::ZERO)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    store
        .advance_follow_head(
            &mut tx,
            chain(),
            Dataset::EvmLogs,
            selector,
            head,
            &B256::repeat_byte(0x02),
        )
        .await
        .unwrap();
    store
        .advance_follow_promoted(&mut tx, chain(), Dataset::EvmLogs, selector, promoted)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    selector
}

#[sqlx::test]
async fn a_rewind_at_or_below_promoted_keeps_promotion_and_parks_the_head_there(pool: PgPool) {
    let store = store(&pool).await;
    let selector = followed(&store, &pool, 120, 110).await;

    let mut tx = pool.begin().await.unwrap();
    let rewound = store
        .rewind_follow(&mut tx, chain(), 105, &PARENT)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rewound.len(), 1);
    assert_eq!(rewound[0].selector_id, selector);
    assert_eq!(rewound[0].head_block, 110);
    assert_eq!(rewound[0].head_hash, B256::ZERO);
    assert_eq!(rewound[0].promoted_to, 110);
    assert_eq!(rewound[0].finalized_to, 99);
}

#[sqlx::test]
async fn a_new_follow_row_starts_finalized_below_its_first_block(pool: PgPool) {
    let store = store(&pool).await;
    let selector = store
        .upsert_selector(chain(), &Selector::Logs(LogSelector::default()))
        .await
        .unwrap();
    assert_eq!(
        store
            .follow_progress(chain(), Dataset::EvmLogs, selector)
            .await
            .unwrap(),
        None
    );
    let row = store
        .follow_get_or_init(chain(), Dataset::EvmLogs, selector, 100, &B256::ZERO)
        .await
        .unwrap();
    assert_eq!(row.finalized_to, 99);
    assert_eq!(
        store
            .follow_progress(chain(), Dataset::EvmLogs, selector)
            .await
            .unwrap(),
        Some(row)
    );
}

#[sqlx::test]
async fn finalized_to_moves_forward_and_never_passes_promoted(pool: PgPool) {
    let store = store(&pool).await;
    let selector = followed(&store, &pool, 120, 110).await;
    let mut tx = pool.begin().await.unwrap();
    let row = store
        .advance_follow_finalized(&mut tx, chain(), Dataset::EvmLogs, selector, 105)
        .await
        .unwrap();
    assert_eq!(row.finalized_to, 105);
    let past = store
        .advance_follow_finalized(&mut tx, chain(), Dataset::EvmLogs, selector, 111)
        .await
        .unwrap_err();
    assert!(matches!(past, StoreError::InvalidInput(_)), "{past:?}");
    let back = store
        .advance_follow_finalized(&mut tx, chain(), Dataset::EvmLogs, selector, 104)
        .await
        .unwrap_err();
    assert!(matches!(back, StoreError::InvalidInput(_)), "{back:?}");
}

#[sqlx::test]
async fn a_halt_persists_keeps_its_first_reason_and_resume_clears_it(pool: PgPool) {
    let store = store(&pool).await;
    let other = ChainId::new(2).unwrap();
    store
        .ensure_chain(other, &datalens_core::ChainName::new("base").unwrap())
        .await
        .unwrap();
    assert_eq!(store.follow_halt(chain()).await.unwrap(), None);

    store.halt_follow(chain(), "fork below safe").await.unwrap();
    store.halt_follow(chain(), "second").await.unwrap();
    let reopened = Store::new(pool.clone());
    assert_eq!(
        reopened.follow_halt(chain()).await.unwrap().as_deref(),
        Some("fork below safe")
    );
    assert_eq!(reopened.follow_halt(other).await.unwrap(), None);

    assert_eq!(
        reopened.resume_follow(chain()).await.unwrap().as_deref(),
        Some("fork below safe")
    );
    assert_eq!(reopened.follow_halt(chain()).await.unwrap(), None);
    assert_eq!(reopened.resume_follow(chain()).await.unwrap(), None);
}
