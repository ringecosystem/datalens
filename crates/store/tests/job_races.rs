#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use datalens_core::{Address, BlockRange, Dataset, Finality, LogSelector, Selector};
use datalens_store::{JobId, JobPurpose, JobStatus, SelectorId, Store};
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

const LEASE: Duration = Duration::from_secs(30);

#[sqlx::test]
async fn a_stale_worker_cannot_fail_complete_or_extend_a_reclaimed_job(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    let id = enqueue(&store, selector, range(0, 9), JobPurpose::Query).await;
    store.claim("w1", LEASE, 10, None).await.unwrap();
    expire_lease(&pool, id).await;
    let reclaimed = store.claim("w2", LEASE, 10, None).await.unwrap();
    assert_eq!(reclaimed[0].id, id);

    let error = json!({"kind": "timeout"});
    assert!(
        !store
            .fail_job(id, "w1", &error, true, Duration::ZERO)
            .await
            .unwrap()
    );
    assert!(
        !store
            .fail_job(id, "w1", &error, false, Duration::ZERO)
            .await
            .unwrap()
    );
    assert_eq!(
        store.extend_lease("w1", &[id], LEASE).await.unwrap(),
        Vec::<JobId>::new()
    );
    let (status, worker): (String, Option<String>) =
        sqlx::query_as("select status, worker_id from fill_jobs where id = $1")
            .bind(id.get())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), worker.as_deref()),
        ("running", Some("w2"))
    );

    assert_eq!(
        store
            .extend_lease("w2", &[id], Duration::from_secs(600))
            .await
            .unwrap(),
        vec![id]
    );
    let lease_left: f64 = sqlx::query_scalar(
        "select extract(epoch from lease_until - now())::float8 from fill_jobs where id = $1",
    )
    .bind(id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(lease_left > 590.0, "{lease_left}");

    let mut tx = pool.begin().await.unwrap();
    assert_eq!(
        store.complete_jobs(&mut tx, "w1", &[id]).await.unwrap(),
        Vec::new()
    );
    assert_eq!(
        store.complete_jobs(&mut tx, "w2", &[id]).await.unwrap(),
        vec![id]
    );
    tx.commit().await.unwrap();
}

#[sqlx::test]
async fn enqueue_racing_a_completion_returns_a_finished_job(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    let blocks = range(0, 99);
    let first = enqueue(&store, selector, blocks, JobPurpose::Query).await;
    store.claim("w1", LEASE, 10, None).await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    store
        .add_coverage(
            &mut tx,
            chain(),
            Dataset::EvmBlocks,
            selector,
            blocks,
            Finality::Safe,
        )
        .await
        .unwrap();
    store.complete_jobs(&mut tx, "w1", &[first]).await.unwrap();
    let racing = store.clone();
    let handle =
        tokio::spawn(async move { enqueue(&racing, selector, blocks, JobPurpose::Query).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!handle.is_finished(), "insert waits on the completing tx");
    tx.commit().await.unwrap();

    let id = handle.await.unwrap();
    assert_eq!(
        store.job_status(&[id]).await.unwrap()[0].status,
        JobStatus::Done
    );
    assert_eq!(
        store.claim("w2", LEASE, 10, None).await.unwrap(),
        Vec::new()
    );
}

#[sqlx::test]
async fn enqueue_of_a_covered_chunk_is_done_without_a_fetch(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    let mut tx = pool.begin().await.unwrap();
    for (blocks, finality) in [
        (range(0, 49), Finality::Finalized),
        (range(50, 99), Finality::Safe),
    ] {
        store
            .add_coverage(
                &mut tx,
                chain(),
                Dataset::EvmBlocks,
                selector,
                blocks,
                finality,
            )
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let covered = enqueue(&store, selector, range(0, 99), JobPurpose::Query).await;
    let partly = enqueue(&store, selector, range(0, 199), JobPurpose::Query).await;
    let states = store.job_status(&[covered, partly]).await.unwrap();
    assert_eq!(states[0].status, JobStatus::Done);
    assert_eq!(states[1].status, JobStatus::Pending);
    let claimed = store.claim("w1", LEASE, 10, None).await.unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![partly]
    );
}

#[sqlx::test]
async fn siblings_stop_at_the_limit_and_skip_other_chunks_and_backoff(pool: PgPool) {
    let store = store(&pool).await;
    let chunk = range(100, 109);
    let mut ids = Vec::new();
    for byte in 1..=4 {
        let selector = logs_selector(&store, byte).await;
        ids.push(enqueue_logs(&store, selector, chunk).await);
    }
    let other_chunk = enqueue_logs(&store, logs_selector(&store, 2).await, range(110, 119)).await;
    sqlx::query("update fill_jobs set next_run_at = now() + interval '1 hour' where id = $1")
        .bind(ids[3].get())
        .execute(&pool)
        .await
        .unwrap();

    let primary = store.claim("w1", LEASE, 1, None).await.unwrap();
    assert_eq!(primary[0].id, ids[0]);
    let first = store.claim_batch_siblings(&primary[0], 1).await.unwrap();
    assert_eq!(first.iter().map(|job| job.id).collect::<Vec<_>>(), [ids[1]]);
    let rest = store.claim_batch_siblings(&primary[0], 10).await.unwrap();
    assert_eq!(rest.iter().map(|job| job.id).collect::<Vec<_>>(), [ids[2]]);
    assert_eq!(rest[0].lease_until, primary[0].lease_until);

    let states = store.job_status(&[ids[3], other_chunk]).await.unwrap();
    assert_eq!(states[0].status, JobStatus::Pending);
    assert_eq!(states[1].status, JobStatus::Pending);
}

#[sqlx::test]
async fn query_jobs_are_claimed_before_an_older_backfill(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    let backfill = enqueue(&store, selector, range(0, 9), JobPurpose::Backfill).await;
    let query = enqueue(&store, selector, range(10, 19), JobPurpose::Query).await;
    let claimed = store.claim("w1", LEASE, 1, None).await.unwrap();
    assert_eq!(claimed[0].id, query);
    let claimed = store.claim("w1", LEASE, 1, None).await.unwrap();
    assert_eq!(claimed[0].id, backfill);
}

#[sqlx::test]
async fn failed_jobs_are_pruned_separately_from_done(pool: PgPool) {
    let store = store(&pool).await;
    let selector = blocks_selector(&store).await;
    let id = enqueue(&store, selector, range(0, 9), JobPurpose::Query).await;
    store.claim("w1", LEASE, 1, None).await.unwrap();
    assert!(
        store
            .fail_job(id, "w1", &json!({}), false, Duration::ZERO)
            .await
            .unwrap()
    );
    assert_eq!(store.prune_done(Duration::ZERO).await.unwrap(), 0);
    assert_eq!(
        store.prune_failed(Duration::from_secs(3600)).await.unwrap(),
        0
    );
    assert_eq!(store.prune_failed(Duration::ZERO).await.unwrap(), 1);
}

async fn blocks_selector(store: &Store) -> SelectorId {
    store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap()
}

async fn logs_selector(store: &Store, byte: u8) -> SelectorId {
    let selector = Selector::Logs(LogSelector {
        addresses: [Address::repeat_byte(byte)].into_iter().collect(),
        topics: Default::default(),
    });
    store.upsert_selector(chain(), &selector).await.unwrap()
}

async fn enqueue(
    store: &Store,
    selector: SelectorId,
    blocks: BlockRange,
    purpose: JobPurpose,
) -> JobId {
    store
        .enqueue(
            chain(),
            Dataset::EvmBlocks,
            selector,
            blocks,
            purpose,
            "degov",
        )
        .await
        .unwrap()
}

async fn enqueue_logs(store: &Store, selector: SelectorId, blocks: BlockRange) -> JobId {
    store
        .enqueue(
            chain(),
            Dataset::EvmLogs,
            selector,
            blocks,
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap()
}

async fn expire_lease(pool: &PgPool, id: JobId) {
    sqlx::query("update fill_jobs set lease_until = now() - interval '1 second' where id = $1")
        .bind(id.get())
        .execute(pool)
        .await
        .unwrap();
}
