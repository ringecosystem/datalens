#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use datalens_core::{Address, BlockRange, Dataset, Finality, LogSelector, Selector};
use datalens_store::{SelectorId, Store, StoreError};
use sqlx::PgPool;

use common::{chain, range, store};

#[sqlx::test]
async fn concurrent_adds_to_one_selector_merge_into_one_row(pool: PgPool) {
    let store = store(&pool).await;
    let selector = logs_selector(&store, 0x0a).await;
    let tasks: Vec<_> = (0..10u64)
        .map(|i| {
            let store = store.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut tx = pool.begin().await.unwrap();
                add(&store, &mut tx, &[selector], range(i * 10, i * 10 + 9)).await?;
                tokio::time::sleep(Duration::from_millis(20)).await;
                tx.commit().await.map_err(StoreError::from)
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let rows: Vec<String> = sqlx::query_scalar(
        "select blocks::text from coverage where selector_id = $1 order by blocks",
    )
    .bind(selector.get())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows, vec!["[0,100)".to_owned()]);
}

#[sqlx::test]
async fn batches_sharing_selectors_in_opposite_order_queue_instead_of_deadlocking(pool: PgPool) {
    let store = store(&pool).await;
    let a = logs_selector(&store, 0x0a).await;
    let b = logs_selector(&store, 0x0b).await;
    let c = logs_selector(&store, 0x0c).await;

    let mut first = pool.begin().await.unwrap();
    add(&store, &mut first, &[c, b, a, b], range(0, 9))
        .await
        .unwrap();
    let racing = store.clone();
    let racing_pool = pool.clone();
    let second = tokio::spawn(async move {
        let mut tx = racing_pool.begin().await.unwrap();
        add(&racing, &mut tx, &[a, b, c], range(10, 19)).await?;
        tx.commit().await.map_err(StoreError::from)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!second.is_finished(), "second batch waits on the row locks");
    first.commit().await.unwrap();
    second.await.unwrap().unwrap();

    let loops: Vec<_> = [vec![a, b, c], vec![c, b, a]]
        .into_iter()
        .enumerate()
        .map(|(lane, order)| {
            let store = store.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                for i in 0..25u64 {
                    let start = 1000 + (i * 2 + lane as u64) * 10;
                    let mut tx = pool.begin().await.unwrap();
                    add(&store, &mut tx, &order, range(start, start + 9)).await?;
                    tx.commit().await?;
                }
                Ok::<_, StoreError>(())
            })
        })
        .collect();
    for task in loops {
        task.await.unwrap().unwrap();
    }
    for selector in [a, b, c] {
        assert_eq!(
            store
                .covered(chain(), Dataset::EvmLogs, &[selector], range(0, 1499))
                .await
                .unwrap(),
            vec![range(0, 19), range(1000, 1499)]
        );
    }
}

#[sqlx::test]
async fn coverage_many_checks_every_selector_before_writing(pool: PgPool) {
    let store = store(&pool).await;
    let logs = logs_selector(&store, 0x0a).await;
    let blocks = store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let err = add(&store, &mut tx, &[logs, blocks], range(0, 9))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::InvalidInput(_)), "{err:?}");
    tx.rollback().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let ghost = logs_selector(&store, 0x0f).await;
    sqlx::query("delete from selectors where id = $1")
        .bind(ghost.get())
        .execute(&pool)
        .await
        .unwrap();
    let err = add(&store, &mut tx, &[logs, ghost], range(0, 9))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)), "{err:?}");
}

#[sqlx::test]
async fn missing_without_rows_or_selectors_is_the_whole_range(pool: PgPool) {
    let store = store(&pool).await;
    let selector = logs_selector(&store, 0x0a).await;
    for selectors in [vec![selector], Vec::new()] {
        assert_eq!(
            store
                .missing(chain(), Dataset::EvmLogs, &selectors, range(5, 9))
                .await
                .unwrap(),
            vec![range(5, 9)]
        );
        assert_eq!(
            store
                .covered(chain(), Dataset::EvmLogs, &selectors, range(5, 9))
                .await
                .unwrap(),
            Vec::<BlockRange>::new()
        );
    }
}

async fn logs_selector(store: &Store, byte: u8) -> SelectorId {
    let selector = Selector::Logs(LogSelector {
        addresses: [Address::repeat_byte(byte)].into_iter().collect(),
        topics: Default::default(),
    });
    store.upsert_selector(chain(), &selector).await.unwrap()
}

async fn add(
    store: &Store,
    tx: &mut datalens_store::PgTx<'_>,
    selectors: &[SelectorId],
    blocks: BlockRange,
) -> Result<(), StoreError> {
    store
        .add_coverage_many(
            tx,
            chain(),
            Dataset::EvmLogs,
            selectors,
            blocks,
            Finality::Safe,
        )
        .await
}
