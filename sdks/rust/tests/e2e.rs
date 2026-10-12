mod common;

use std::time::Duration;

use datalens_sdk::{
    Anchor, ApiErrorKind, Dataset, Error, Finality, FinalizedCursor, FollowEvent, FollowRequest,
    HeadKind, IncompleteReason, Observation, PageRequest, ProvisionalCursor, QueryFinality,
    QueryRequest, QueryResponse, Segment, SegmentSource,
};
use futures::StreamExt;
use sqlx::PgPool;

use common::{MockChain, SAFE_LAG, START, Server, blocks_of, ethereum, range, usdc};

const SAFE: u64 = START - SAFE_LAG;

fn logs(blocks: datalens_sdk::BlockRange, finality: QueryFinality) -> QueryRequest {
    QueryRequest {
        chain: ethereum(),
        dataset: Dataset::EvmLogs,
        selector: Some(usdc()),
        range: blocks,
        finality,
        fields: None,
        page: None,
        wait_ms: Some(5_000),
    }
}

#[sqlx::test(migrations = false)]
async fn query_pages_and_query_all_merges_every_page(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let client = server.client();
    let mut request = logs(range(1_900, 1_949), QueryFinality::DurableOnly);
    request.page = Some(PageRequest {
        limit: Some(20),
        cursor: None,
    });

    let first = client.query(&request).await.unwrap();
    assert_eq!(blocks_of(&first.rows), (1_900..1_920).collect::<Vec<_>>());
    assert!(first.page.next_cursor.is_some());

    let all = client.query_all(&request).await.unwrap();
    assert_eq!(blocks_of(&all.rows), (1_900..=1_949).collect::<Vec<_>>());
    assert_eq!(all.served, Some(range(1_900, 1_949)));
    assert_eq!(all.next_start, None);
    assert_eq!(all.incomplete_reason, None);
    assert_eq!(all.page.next_cursor, None);
    assert_eq!(
        all.segments,
        vec![Segment {
            start: 1_900,
            end: 1_949,
            source: SegmentSource::Durable,
            finality: Finality::Finalized,
            anchor: None,
        }]
    );
    assert_eq!(
        all.rows[0]["block_hash"].as_str().unwrap(),
        format!("{:#x}", chain.hash(1_900))
    );
}

#[sqlx::test(migrations = false)]
async fn errors_carry_the_stable_kind_status_and_retryability(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;

    let unauthorized = server
        .client_as(common::APPLICATION, "wrong")
        .heads(&ethereum())
        .await
        .unwrap_err();
    assert!(
        matches!(
            &unauthorized,
            Error::Api {
                kind: ApiErrorKind::Unauthorized,
                status: 401,
                ..
            }
        ),
        "{unauthorized:?}"
    );
    assert!(!unauthorized.is_retryable());

    let above_safe = server
        .client()
        .query(&logs(range(SAFE, SAFE + 1), QueryFinality::DurableOnly))
        .await
        .unwrap_err();
    assert_eq!(above_safe.kind(), Some(&ApiErrorKind::InvalidInput));
    assert_eq!(above_safe.status(), Some(400));
    assert!(!above_safe.is_retryable());

    let too_wide = server
        .client()
        .query(&logs(range(0, 5_000), QueryFinality::DurableOnly))
        .await
        .unwrap_err();
    assert_eq!(too_wide.kind(), Some(&ApiErrorKind::InvalidInput));
    assert_eq!(too_wide.status(), Some(400));
    assert!(!too_wide.is_retryable());
}

#[sqlx::test(migrations = false)]
async fn follow_ends_on_a_chunk_wider_than_the_range_quota(pool: PgPool) {
    let chain = MockChain::spawn(10_000).await;
    let server = Server::boot(pool, &chain).await;
    let mut request = FollowRequest::logs(ethereum(), usdc(), 0);
    request.chunk_blocks = 5_001;
    let events = tokio::time::timeout(
        Duration::from_secs(30),
        server.client().follow(request).collect::<Vec<_>>(),
    )
    .await
    .expect("follow kept retrying");
    assert!(
        matches!(
            events.as_slice(),
            [Err(Error::Api {
                kind: ApiErrorKind::InvalidInput,
                status: 400,
                ..
            })]
        ),
        "{events:?}"
    );
}

#[sqlx::test(migrations = false)]
async fn heads_and_wait_heads_report_the_mock_chain(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let client = server.client();

    let heads = client.heads(&ethereum()).await.unwrap();
    assert_eq!((heads.latest, heads.safe), (START, SAFE));
    assert_eq!(client.all_heads().await.unwrap(), vec![heads]);

    let unchanged = client
        .wait_heads(
            &ethereum(),
            START,
            HeadKind::Latest,
            Duration::from_millis(200),
        )
        .await
        .unwrap();
    assert_eq!(unchanged.latest, START);

    chain.advance(5);
    let passed = client
        .wait_heads(
            &ethereum(),
            START,
            HeadKind::Latest,
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(passed.latest, START + 5);
    assert_eq!(passed.latest_hash, chain.hash(START + 5));
}

/// Rows of the stream up to block `until`, and how often it reported being caught up.
async fn follow_until(
    stream: &mut futures::stream::BoxStream<'static, datalens_sdk::Result<FollowEvent>>,
    until: u64,
    mut on_caught_up: impl FnMut(),
) -> (Vec<QueryResponse>, usize) {
    let mut batches = Vec::new();
    let mut caught_up = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let event = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("follow stalled")
            .expect("stream ended")
            .expect("follow failed");
        match event {
            FollowEvent::Batch(response) => {
                let done = response.served.is_some_and(|served| served.end() >= until);
                batches.push(response);
                if done {
                    return (batches, caught_up);
                }
            }
            FollowEvent::CaughtUp { .. } => {
                caught_up += 1;
                on_caught_up();
            }
            FollowEvent::Stalled { .. } => {}
        }
    }
}

#[sqlx::test(migrations = false)]
async fn follow_reads_history_in_chunks_then_keeps_up_with_the_safe_head(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let mut request = FollowRequest::logs(ethereum(), usdc(), 1_900);
    request.chunk_blocks = 40;
    request.heads_wait = Duration::from_secs(5);
    request.poll_interval = Duration::from_millis(100);
    let mut stream = server.client().follow(request);

    let (history, caught_up) = follow_until(&mut stream, SAFE, || {}).await;
    assert_eq!(caught_up, 0);
    let served: Vec<_> = history.iter().filter_map(|batch| batch.served).collect();
    assert_eq!(
        served,
        vec![range(1_900, 1_939), range(1_940, 1_979), range(1_980, SAFE)]
    );

    let mut cursor = FinalizedCursor::new(1_900);
    let mut rows = Vec::new();
    for batch in &history {
        cursor.advance(batch);
        rows.extend(blocks_of(&batch.rows));
    }
    assert_eq!(cursor.next(), SAFE + 1);

    let (live, caught_up) = follow_until(&mut stream, SAFE + 30, || chain.advance(30)).await;
    assert_eq!(caught_up, 1);
    for batch in &live {
        assert!(
            batch
                .segments
                .iter()
                .all(|s| s.source == SegmentSource::Durable),
            "{:?}",
            batch.segments
        );
        cursor.advance(batch);
        rows.extend(blocks_of(&batch.rows));
    }
    assert_eq!(cursor.next(), SAFE + 31);
    assert_eq!(rows, (1_900..=SAFE + 30).collect::<Vec<_>>());
}

#[sqlx::test(migrations = false)]
async fn follow_stops_after_end(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let mut request = FollowRequest::logs(ethereum(), usdc(), 1_000);
    request.end = Some(1_009);
    let events: Vec<_> = server
        .client()
        .follow(request)
        .map(Result::unwrap)
        .collect()
        .await;
    let [FollowEvent::Batch(batch)] = events.as_slice() else {
        panic!("{events:?}");
    };
    assert_eq!(batch.served, Some(range(1_000, 1_009)));
    assert_eq!(blocks_of(&batch.rows), (1_000..=1_009).collect::<Vec<_>>());
}

/// Queries the hot tail until the response satisfies `done`.
async fn tail_until(
    server: &Server,
    request: &QueryRequest,
    done: impl Fn(&QueryResponse) -> bool,
) -> QueryResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = server.client().query(request).await.unwrap();
        if done(&response) {
            return response;
        }
        assert!(tokio::time::Instant::now() < deadline, "{response:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[sqlx::test(migrations = false)]
async fn provisional_cursor_reports_a_rollback_from_the_fork_after_a_reorg(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let mut tail = logs(range(SAFE + 1, START), QueryFinality::LatestOnly);
    tail.wait_ms = Some(0);
    let mut cursor = ProvisionalCursor::new(SAFE + 1);

    let before = tail_until(&server, &tail, |response| response.next_start.is_none()).await;
    assert_eq!(
        before.segments,
        vec![Segment {
            start: SAFE + 1,
            end: START,
            source: SegmentSource::Hot,
            finality: Finality::Unsafe,
            anchor: Some(Anchor {
                number: START,
                hash: chain.hash(START),
            }),
        }]
    );
    assert_eq!(cursor.observe(&before), Observation::Consistent);
    assert_eq!(cursor.next(), START + 1);

    let fork = START - 3;
    chain.reorg_from(fork, 7);
    let observation = loop {
        let response = tail_until(&server, &tail, |response| response.served.is_some()).await;
        match cursor.observe(&response) {
            Observation::Consistent => tokio::time::sleep(Duration::from_millis(50)).await,
            rollback => break rollback,
        }
    };
    assert_eq!(observation, Observation::Rollback { from: fork });
    assert_eq!(cursor.next(), fork);

    let mut refetch = tail.clone();
    refetch.range = range(fork, START);
    let after = tail_until(&server, &refetch, |response| {
        response.next_start.is_none()
            && response.segments[0].anchor.map(|anchor| anchor.hash) == Some(chain.hash(START))
    })
    .await;
    assert_eq!(cursor.observe(&after), Observation::Consistent);
    assert_eq!(blocks_of(&after.rows), (fork..=START).collect::<Vec<_>>());
    assert_eq!(
        after.rows[0]["block_hash"].as_str().unwrap(),
        format!("{:#x}", chain.hash(fork))
    );
}

#[sqlx::test(migrations = false)]
async fn follow_latest_only_waits_for_the_first_follow_tick(pool: PgPool) {
    let chain = MockChain::spawn(START).await;
    let server = Server::boot(pool, &chain).await;
    let mut request = FollowRequest::logs(ethereum(), usdc(), 0);
    request.finality = QueryFinality::LatestOnly;
    request.wait_ms = Some(0);
    request.poll_interval = Duration::from_millis(50);
    let mut stream = server.client().follow(request);

    let mut stalls = Vec::new();
    let batch = loop {
        match stream.next().await.unwrap().unwrap() {
            FollowEvent::Batch(batch) => break batch,
            FollowEvent::Stalled {
                next_start, reason, ..
            } => stalls.push((next_start, reason)),
            other => panic!("{other:?}"),
        }
    };
    assert!(
        stalls
            .iter()
            .all(|stall| *stall == (SAFE + 1, IncompleteReason::NotFollowedYet)),
        "{stalls:?}"
    );
    assert_eq!(batch.served, Some(range(SAFE + 1, START)));
    assert_eq!(
        blocks_of(&batch.rows),
        (SAFE + 1..=START).collect::<Vec<_>>()
    );
}
