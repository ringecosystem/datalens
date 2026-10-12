#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::time::Duration;

use datalens_core::{
    Address, B256, BlockRow, Dataset, Finality, LogSelector, Selector, TopicFilter,
};
use datalens_store::{JobPurpose, JobStatus};
use serde_json::json;
use sqlx::PgPool;

use common::{chain, range, store};

#[sqlx::test]
async fn selectors_by_id_returns_input_order_and_skips_unknown(pool: PgPool) {
    let store = store(&pool).await;
    let logs = Selector::Logs(LogSelector {
        addresses: [Address::repeat_byte(0xaa)].into_iter().collect(),
        topics: [
            TopicFilter::one_of([B256::repeat_byte(0x11)]).unwrap(),
            TopicFilter::Any,
            TopicFilter::Any,
            TopicFilter::Any,
        ],
    });
    let logs_id = store.upsert_selector(chain(), &logs).await.unwrap();
    let blocks_id = store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap();
    let loaded = store.selectors_by_id(&[blocks_id, logs_id]).await.unwrap();
    assert_eq!(
        loaded,
        vec![(blocks_id, Selector::Blocks), (logs_id, logs.clone())]
    );
}

#[sqlx::test]
async fn block_timestamps_and_coverage_spans(pool: PgPool) {
    let store = store(&pool).await;
    let block = |number: u64| BlockRow {
        number,
        hash: B256::repeat_byte(number as u8),
        parent_hash: B256::repeat_byte(number as u8 - 1),
        timestamp: 1_000 + number,
        finality: Finality::Safe,
        header: json!({}),
    };
    let blocks_id = store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    store
        .insert_blocks(&mut tx, chain(), &[block(5), block(7)])
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain(),
            Dataset::EvmBlocks,
            blocks_id,
            range(0, 9),
            Finality::Finalized,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain(),
            Dataset::EvmBlocks,
            blocks_id,
            range(20, 29),
            Finality::Safe,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        store.block_timestamps(chain(), &[5, 6, 7]).await.unwrap(),
        HashMap::from([(5, 1_005), (7, 1_007)])
    );
    assert_eq!(
        store
            .coverage_spans(chain(), Dataset::EvmBlocks, blocks_id)
            .await
            .unwrap(),
        vec![
            (range(0, 9), Finality::Finalized),
            (range(20, 29), Finality::Safe)
        ]
    );
}

#[sqlx::test]
async fn list_jobs_and_counts_report_status_and_last_error(pool: PgPool) {
    let store = store(&pool).await;
    let blocks_id = store
        .upsert_selector(chain(), &Selector::Blocks)
        .await
        .unwrap();
    let first = store
        .enqueue(
            chain(),
            Dataset::EvmBlocks,
            blocks_id,
            range(0, 9),
            JobPurpose::Query,
            "ormp",
        )
        .await
        .unwrap();
    store
        .enqueue(
            chain(),
            Dataset::EvmBlocks,
            blocks_id,
            range(10, 19),
            JobPurpose::Backfill,
            "ormp",
        )
        .await
        .unwrap();
    store
        .claim("w", Duration::from_secs(30), 1, None)
        .await
        .unwrap();
    store
        .fail_job(first, "w", &json!({"kind": "fatal"}), false, Duration::ZERO)
        .await
        .unwrap();

    let failed = store.list_jobs(Some(JobStatus::Failed), 10).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0.id, first);
    assert_eq!(failed[0].0.blocks, range(0, 9));
    assert_eq!(failed[0].1, Some(json!({"kind": "fatal"})));
    assert_eq!(store.list_jobs(None, 10).await.unwrap().len(), 2);
    assert_eq!(
        store.job_counts().await.unwrap(),
        vec![
            (JobStatus::Pending, 1),
            (JobStatus::Running, 0),
            (JobStatus::Done, 0),
            (JobStatus::Failed, 1)
        ]
    );
}
