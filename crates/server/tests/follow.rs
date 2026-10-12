mod common;

use std::time::{Duration, Instant};

use datalens_core::wire::{
    Anchor, FollowInfo, FollowResumeResponse, IncompleteReason, Segment, SegmentSource,
    WatchRequest,
};
use datalens_core::{B256, ChainId, Dataset, Finality, LogSelector, QueryFinality};
use datalens_store::Store;
use sqlx::PgPool;

use common::{
    LATEST, METRICS_TOKEN, SAFE, boot_mock, ethereum, logs_request, mock_chain, range, selector,
};

fn watch(selector: Option<LogSelector>) -> WatchRequest {
    WatchRequest {
        chain: ethereum(),
        dataset: Dataset::EvmLogs,
        selector,
    }
}

fn hash(number: u64) -> B256 {
    B256::left_padding_from(&number.to_be_bytes())
}

#[sqlx::test(migrations = false)]
async fn watch_follows_a_selector_within_quota_and_unwatch_releases_it(pool: PgPool) {
    let rpc = mock_chain().await;
    let booted = boot_mock(pool, &rpc).await;
    let degov = booted.degov();

    let watched = degov.watch(&watch(Some(selector(&[0xa1])))).await.unwrap();
    assert_eq!(watched.ttl_ms, 900_000);
    assert_eq!(watched.follow.from_block, SAFE + 1);

    let deadline = Instant::now() + Duration::from_secs(10);
    let follow = loop {
        let coverage = degov
            .coverage(
                &ethereum(),
                Dataset::EvmLogs,
                Some(&selector(&[0xa1])),
                None,
            )
            .await
            .unwrap();
        if let Some(follow) = coverage.follow
            && follow.head_block.is_some()
        {
            break follow;
        }
        assert!(Instant::now() < deadline, "follow never fetched");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        follow,
        FollowInfo {
            from_block: SAFE + 1,
            head_block: Some(LATEST),
            head_hash: Some(hash(LATEST)),
            promoted_to: None,
        }
    );
    let metrics = booted.degov().metrics(METRICS_TOKEN).await.unwrap();
    assert!(
        metrics.contains("datalens_subscriptions{chain=\"ethereum\",dataset=\"evm.logs\"} 1"),
        "{metrics}"
    );

    let over = degov
        .watch(&watch(Some(selector(&[0xa2]))))
        .await
        .unwrap_err();
    assert_eq!((over.status(), over.kind()), (Some(403), Some("forbidden")));

    degov
        .unwatch(&watch(Some(selector(&[0xa1]))))
        .await
        .unwrap();
    degov.watch(&watch(Some(selector(&[0xa2])))).await.unwrap();

    let unfiltered = degov.watch(&watch(None)).await.unwrap_err();
    assert_eq!(unfiltered.status(), Some(403));
    let narrow = booted.client("narrow", "narrow-token");
    let elsewhere = narrow
        .watch(&watch(Some(selector(&[0xa1]))))
        .await
        .unwrap_err();
    assert_eq!(elsewhere.status(), Some(403));
}

#[sqlx::test(migrations = false)]
async fn latest_only_over_http_returns_the_hot_segment_anchored_at_the_follow_head(pool: PgPool) {
    let rpc = mock_chain().await;
    let booted = boot_mock(pool, &rpc).await;
    let degov = booted.degov();
    let mut request = logs_request(ethereum(), selector(&[0xa1]), range(SAFE + 1, LATEST));
    request.finality = QueryFinality::LatestOnly;
    request.wait_ms = Some(0);

    let deadline = Instant::now() + Duration::from_secs(10);
    let response = loop {
        let response = degov.query(&request).await.unwrap();
        if response.served.is_some() {
            break response;
        }
        assert_eq!(
            response.incomplete_reason,
            Some(IncompleteReason::NotFollowedYet)
        );
        assert!(Instant::now() < deadline, "never served");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(response.served, Some(range(SAFE + 1, LATEST)));
    assert_eq!(response.next_start, None);
    assert_eq!(
        response.segments,
        vec![Segment {
            start: SAFE + 1,
            end: LATEST,
            source: SegmentSource::Hot,
            finality: Finality::Unsafe,
            anchor: Some(Anchor {
                number: LATEST,
                hash: hash(LATEST),
            }),
        }]
    );
    assert_eq!(response.rows, Vec::<serde_json::Value>::new());
}

#[sqlx::test(migrations = false)]
async fn heads_wait_answers_at_once_when_passed_and_with_current_heads_on_timeout(pool: PgPool) {
    let rpc = mock_chain().await;
    let booted = boot_mock(pool, &rpc).await;
    let degov = booted.degov();

    let passed = degov
        .wait_heads(&ethereum(), LATEST - 1, "latest", 5_000)
        .await
        .unwrap();
    assert_eq!(passed.latest, LATEST);

    let started = Instant::now();
    let unchanged = degov
        .wait_heads(&ethereum(), SAFE, "safe", 300)
        .await
        .unwrap();
    let waited = started.elapsed();
    assert_eq!((unchanged.latest, unchanged.safe), (LATEST, SAFE));
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    assert!(waited < Duration::from_secs(3), "{waited:?}");

    let bad = degov
        .wait_heads(&ethereum(), 0, "pending", 100)
        .await
        .unwrap_err();
    assert_eq!(bad.status(), Some(400));
    let narrow = booted.client("narrow", "narrow-token");
    let forbidden = narrow
        .wait_heads(&ethereum(), 0, "latest", 100)
        .await
        .unwrap_err();
    assert_eq!(forbidden.status(), Some(403));
}

#[sqlx::test(migrations = false)]
async fn an_admin_resumes_a_halted_follow_and_others_may_not(pool: PgPool) {
    let rpc = mock_chain().await;
    let store = Store::new(pool.clone());
    let booted = boot_mock(pool, &rpc).await;
    let degov = booted.degov();
    let chain = ChainId::new(1).unwrap();

    assert_eq!(
        degov.resume_follow(&ethereum()).await.unwrap(),
        FollowResumeResponse {
            chain: ethereum(),
            halt_reason: None,
        }
    );
    store
        .halt_follow(chain, "fork is at or below the safe head")
        .await
        .unwrap();
    let narrow = booted.client("narrow", "narrow-token");
    let forbidden = narrow.resume_follow(&ethereum()).await.unwrap_err();
    assert_eq!(forbidden.status(), Some(403));
    assert!(store.follow_halt(chain).await.unwrap().is_some());

    let resumed = degov.resume_follow(&ethereum()).await.unwrap();
    assert_eq!(
        resumed.halt_reason.as_deref(),
        Some("fork is at or below the safe head")
    );
    assert_eq!(store.follow_halt(chain).await.unwrap(), None);
}
