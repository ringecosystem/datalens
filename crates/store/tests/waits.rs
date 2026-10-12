#[path = "common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use datalens_core::{B256, Dataset, Heads, Selector};
use datalens_store::{HeadKind, JobId, JobPurpose, JobStatus, Store, StoreError};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use common::{chain, range, store};

const LEASE: Duration = Duration::from_secs(30);

#[sqlx::test]
async fn five_head_waiters_share_a_two_connection_pool_and_wake_on_one_update(
    _: PgPoolOptions,
    options: PgConnectOptions,
) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(options)
        .await
        .unwrap();
    let store = store(&pool).await;
    let chain = chain();
    let heads = Heads {
        latest: 10,
        latest_hash: B256::repeat_byte(0x44),
        safe: 8,
        finalized: 6,
    };
    store.upsert_heads(chain, &heads).await.unwrap();

    let waiters: Vec<_> = (0..5)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .wait_head_change(chain, 10, HeadKind::Latest, Duration::from_secs(5))
                    .await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let next = Heads {
        latest: 11,
        latest_hash: B256::repeat_byte(0x45),
        ..heads
    };
    let updated = Instant::now();
    store.upsert_heads(chain, &next).await.unwrap();
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap().unwrap(), next);
    }
    assert!(
        updated.elapsed() < Duration::from_millis(600),
        "woke by notify, not by the 1 s recheck: {:?}",
        updated.elapsed()
    );
    pool.close().await;
}

#[sqlx::test]
async fn wait_done_rereads_status_when_no_notification_arrives(pool: PgPool) {
    let store = store(&pool).await;
    let id = pending_job(&store, 0).await;
    let waiting = store.clone();
    let handle =
        tokio::spawn(async move { waiting.wait_done(&[id], Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let marked = Instant::now();
    sqlx::query("update fill_jobs set status = 'done' where id = $1")
        .bind(id.get())
        .execute(&pool)
        .await
        .unwrap();
    let states = handle.await.unwrap().unwrap();
    assert_eq!(states[0].status, JobStatus::Done);
    assert!(marked.elapsed() < Duration::from_millis(1500));
}

#[sqlx::test]
async fn wait_done_wakes_on_a_hard_failure_but_not_on_a_retry(pool: PgPool) {
    let store = store(&pool).await;
    let hard = pending_job(&store, 0).await;
    let soft = pending_job(&store, 10).await;
    let claimed = store.claim("w1", LEASE, 10, None).await.unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![hard, soft]
    );

    let waiting = store.clone();
    let handle =
        tokio::spawn(async move { waiting.wait_done(&[hard], Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let failed = Instant::now();
    let error = json!({"kind": "invalid_input"});
    store
        .fail_job(hard, &error, false, Duration::ZERO)
        .await
        .unwrap();
    let states = handle.await.unwrap().unwrap();
    assert!(failed.elapsed() < Duration::from_millis(600));
    assert_eq!(states[0].status, JobStatus::Failed);
    assert_eq!(states[0].last_error, Some(error));

    let waiting = store.clone();
    let handle = tokio::spawn(async move {
        waiting
            .wait_done(&[soft], Duration::from_millis(1500))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    store
        .fail_job(
            soft,
            &json!({"kind": "rate_limited"}),
            true,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let err = handle.await.unwrap().unwrap_err();
    assert!(matches!(err, StoreError::Timeout(_)), "{err:?}");
    assert_eq!(
        store.job_status(&[soft]).await.unwrap()[0].status,
        JobStatus::Pending
    );
}

#[sqlx::test]
async fn waits_time_out_with_a_typed_error(pool: PgPool) {
    let store = store(&pool).await;
    let id = pending_job(&store, 0).await;
    let started = Instant::now();
    let err = store
        .wait_done(&[id], Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Timeout(_)), "{err:?}");
    let err = store
        .wait_head_change(chain(), 0, HeadKind::Safe, Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Timeout(_)), "{err:?}");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(600) && elapsed < Duration::from_millis(1500),
        "{elapsed:?}"
    );
}

async fn pending_job(store: &Store, start: u64) -> JobId {
    let selector = store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap();
    store
        .enqueue(
            chain(),
            Dataset::EvmBlocks,
            selector,
            range(start, start + 9),
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap()
}
