#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use datalens_core::{Address, ChainId, Dataset, LogSelector, Selector};
use datalens_store::{JobPurpose, JobStatus};
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

fn logs_at(byte: u8) -> Selector {
    Selector::Logs(LogSelector {
        addresses: [Address::repeat_byte(byte)].into_iter().collect(),
        topics: Default::default(),
    })
}

#[sqlx::test]
async fn claim_is_exclusive_until_the_lease_expires_and_done_frees_the_key(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();
    let blocks = range(0, 99);
    let id = store
        .enqueue(
            chain,
            Dataset::EvmBlocks,
            selector,
            blocks,
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();
    let again = store
        .enqueue(
            chain,
            Dataset::EvmBlocks,
            selector,
            blocks,
            JobPurpose::Backfill,
            "other",
        )
        .await
        .unwrap();
    assert_eq!(again, id);

    let other = ChainId::new(46).unwrap();
    assert_eq!(
        store
            .claim("nobody", Duration::from_secs(30), 10, Some(other))
            .await
            .unwrap(),
        Vec::new()
    );

    let first = store.clone();
    let second = store.clone();
    let (left, right) = tokio::join!(
        first.claim("w1", Duration::from_secs(30), 10, Some(chain)),
        second.claim("w2", Duration::from_secs(30), 10, Some(chain)),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.len() + right.len(), 1);
    let job = left.into_iter().chain(right).next().unwrap();
    assert_eq!(job.id, id);
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.attempts, 1);
    assert_eq!(job.application_id, "degov");
    assert_eq!(job.purpose, JobPurpose::Query);

    sqlx::query("update fill_jobs set lease_until = now() - interval '1 second' where id = $1")
        .bind(id.get())
        .execute(&pool)
        .await
        .unwrap();
    let reclaimed = store
        .claim("w3", Duration::from_secs(30), 10, None)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, id);
    assert_eq!(reclaimed[0].attempts, 2);
    assert_eq!(reclaimed[0].worker_id.as_deref(), Some("w3"));

    let mut tx = pool.begin().await.unwrap();
    store.complete_jobs(&mut tx, &[id]).await.unwrap();
    tx.commit().await.unwrap();
    let fresh = store
        .enqueue(
            chain,
            Dataset::EvmBlocks,
            selector,
            blocks,
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();
    assert_ne!(fresh, id);
}

#[sqlx::test]
async fn wait_done_sees_a_job_finished_before_listen_and_wakes_when_it_completes(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();
    let done_id = store
        .enqueue(
            chain,
            Dataset::EvmBlocks,
            selector,
            range(0, 9),
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    store.complete_jobs(&mut tx, &[done_id]).await.unwrap();
    tx.commit().await.unwrap();
    let states = store
        .wait_done(&[done_id], Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].id, done_id);
    assert_eq!(states[0].status, JobStatus::Done);

    let pending = store
        .enqueue(
            chain,
            Dataset::EvmBlocks,
            selector,
            range(10, 19),
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();
    let waiting = store.clone();
    let handle =
        tokio::spawn(async move { waiting.wait_done(&[pending], Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut tx = pool.begin().await.unwrap();
    store.complete_jobs(&mut tx, &[pending]).await.unwrap();
    tx.commit().await.unwrap();
    let states = handle.await.unwrap().unwrap();
    assert_eq!(states[0].status, JobStatus::Done);
    assert_eq!(store.prune_done(Duration::ZERO).await.unwrap(), 2);
}

#[sqlx::test]
async fn siblings_share_a_chunk_and_a_hard_failure_is_visible(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let blocks = range(100, 109);
    let first_selector = logs_at(0x0a);
    let second_selector = logs_at(0x0b);
    let first = store.upsert_selector(chain, &first_selector).await.unwrap();
    let second = store
        .upsert_selector(chain, &second_selector)
        .await
        .unwrap();
    let left = store
        .enqueue(
            chain,
            Dataset::EvmLogs,
            first,
            blocks,
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();
    let right = store
        .enqueue(
            chain,
            Dataset::EvmLogs,
            second,
            blocks,
            JobPurpose::Backfill,
            "ormp",
        )
        .await
        .unwrap();
    let claimed = store
        .claim("w1", Duration::from_secs(30), 1, Some(chain))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let siblings = store.claim_batch_siblings(&claimed[0]).await.unwrap();
    assert_eq!(siblings.len(), 1);
    let ids = [claimed[0].id, siblings[0].id];
    assert!(ids.contains(&left));
    assert!(ids.contains(&right));
    assert_eq!(
        store
            .claim("w2", Duration::from_secs(30), 10, Some(chain))
            .await
            .unwrap(),
        Vec::new()
    );

    let error = json!({"reason": "bad selector"});
    store
        .fail_job(claimed[0].id, &error, false, Duration::ZERO)
        .await
        .unwrap();
    let states = store.job_status(&[claimed[0].id]).await.unwrap();
    assert_eq!(states[0].status, JobStatus::Failed);
    assert_eq!(states[0].last_error, Some(error));

    store
        .fail_job(
            siblings[0].id,
            &json!({"reason": "later"}),
            true,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .claim("w3", Duration::from_secs(30), 10, Some(chain))
            .await
            .unwrap(),
        Vec::new()
    );
}
