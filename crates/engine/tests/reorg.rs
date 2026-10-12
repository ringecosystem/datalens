#[path = "common/mod.rs"]
mod common;

use datalens_core::wire::{
    Anchor, FollowInfo, IncompleteReason, LogRecord, Segment, SegmentSource, WatchRequest,
};
use datalens_core::{ChainName, Dataset, Finality, LogRow, LogSelector, QueryFinality};
use datalens_engine::QueryEngine;
use prometheus::Registry;
use serde_json::Value;
use sqlx::PgPool;

use common::follow_chain::{
    FollowChain, any_transfer, follow_chain, follow_engine, follow_settings, metrics_text, request,
    usdc,
};
use common::{caller, chain_id, range};

const START: u64 = 10_000;

fn records(rows: Vec<LogRow>, finality: Finality) -> Vec<Value> {
    rows.into_iter()
        .map(|row| {
            let mut record = LogRecord::from(row);
            record.finality = finality;
            serde_json::to_value(record).unwrap()
        })
        .collect()
}

fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

async fn setup(pool: &PgPool) -> (FollowChain, QueryEngine, Registry) {
    let mock = FollowChain::spawn(START).await;
    let registry = Registry::new();
    let engine = follow_engine(
        pool,
        vec![follow_chain("ethereum", 1, mock.rpc.url())],
        follow_settings(),
        &registry,
    )
    .await;
    (mock, engine, registry)
}

#[sqlx::test(migrations = false)]
async fn a_reorg_deletes_unsafe_rows_from_the_fork_and_follow_refetches_them(pool: PgPool) {
    let (mock, engine, registry) = setup(&pool).await;
    let ask = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::LatestOnly,
    );
    engine.query(&caller(), ask.clone()).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();
    let old_anchor = mock.hash(START);

    mock.reorg_from(9_997, 7);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 1);
    let response = engine.query(&caller(), ask.clone()).await.unwrap();
    assert_eq!(response.served, Some(range(9_993, 9_996)));
    assert_eq!(response.next_start, Some(9_997));
    assert_eq!(response.incomplete_reason, Some(IncompleteReason::Pending));
    assert_eq!(
        response.segments,
        vec![Segment {
            start: 9_993,
            end: 9_996,
            source: SegmentSource::Hot,
            finality: Finality::Unsafe,
            anchor: Some(Anchor {
                number: 9_996,
                hash: mock.hash(9_996),
            }),
        }]
    );
    assert_eq!(
        engine
            .store()
            .unsafe_hashes(chain_id(), range(9_993, START))
            .await
            .unwrap()
            .into_iter()
            .map(|(number, _)| number)
            .collect::<Vec<_>>(),
        vec![9_993, 9_994, 9_995, 9_996]
    );
    let text = metrics_text(&registry);
    assert!(
        text.contains("datalens_reorgs_total{chain=\"ethereum\"} 1"),
        "{text}"
    );
    assert!(!text.contains("datalens_finality_violations_total{chain=\"ethereum\"} 1"));

    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 2);
    let response = engine.query(&caller(), ask).await.unwrap();
    assert_eq!(response.served, Some(range(9_993, START)));
    let anchor = response.segments[0].anchor.unwrap();
    assert_eq!(anchor.number, START);
    assert_eq!(anchor.hash, mock.hash(START));
    assert_ne!(anchor.hash, old_anchor);
    assert_eq!(
        response.rows,
        records(mock.logs(&usdc(), range(9_993, START)), Finality::Unsafe)
    );
}

#[sqlx::test(migrations = false)]
async fn a_fork_below_safe_counts_a_violation_halts_follow_and_keeps_durable_rows(pool: PgPool) {
    let (mock, engine, registry) = setup(&pool).await;
    let ask = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::LatestOnly,
    );
    engine.query(&caller(), ask).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();
    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    let promoted = records(mock.logs(&usdc(), range(9_993, START)), Finality::Safe);

    mock.reorg_from(9_998, 9);
    engine.tick_once(&ethereum()).await.unwrap();
    let text = metrics_text(&registry);
    assert!(
        text.contains("datalens_reorgs_total{chain=\"ethereum\"} 1"),
        "{text}"
    );
    assert!(text.contains("datalens_finality_violations_total{chain=\"ethereum\"} 1"));
    assert!(text.contains("datalens_follow_halted{chain=\"ethereum\"} 1"));
    assert!(
        engine
            .store()
            .unsafe_hashes(chain_id(), range(9_993, 10_008))
            .await
            .unwrap()
            .is_empty()
    );

    let durable = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::DurableOnly,
    );
    let response = engine.query(&caller(), durable).await.unwrap();
    assert_eq!(response.served, Some(range(9_993, START)));
    assert_eq!(response.rows, promoted);

    let get_logs = mock.count("eth_getLogs");
    mock.advance(2);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), get_logs);
}

const CHAINS: [&str; 8] = [
    "ethereum", "base", "arbitrum", "optimism", "polygon", "bsc", "gnosis", "linea",
];

fn watch(chain: &str, dataset: Dataset, selector: Option<LogSelector>) -> WatchRequest {
    WatchRequest {
        chain: ChainName::new(chain).unwrap(),
        dataset,
        selector,
    }
}

/// Two logs groups that cannot batch plus `evm.blocks`: per chain per tick one head call,
/// one reorg check, one `eth_getLogs` per group, and one header batch.
#[sqlx::test(migrations = false)]
async fn eight_chains_in_steady_state_cost_two_plus_groups_plus_one_header_batch(pool: PgPool) {
    let mut mocks = Vec::new();
    let mut chains = Vec::new();
    for (index, name) in CHAINS.iter().enumerate() {
        let mock = FollowChain::spawn(START).await;
        chains.push(follow_chain(name, index as i32 + 1, mock.rpc.url()));
        mocks.push(mock);
    }
    let registry = Registry::new();
    let engine = follow_engine(&pool, chains, follow_settings(), &registry).await;
    for name in CHAINS {
        let watched = engine
            .watch(&caller(), watch(name, Dataset::EvmLogs, Some(usdc())))
            .await
            .unwrap();
        assert_eq!(watched.ttl_ms, 900_000);
        assert_eq!(
            watched.follow,
            FollowInfo {
                from_block: 9_993,
                head_block: None,
                head_hash: None,
                promoted_to: None,
            }
        );
        engine
            .watch(
                &caller(),
                watch(name, Dataset::EvmLogs, Some(any_transfer())),
            )
            .await
            .unwrap();
        engine
            .watch(&caller(), watch(name, Dataset::EvmBlocks, None))
            .await
            .unwrap();
    }

    for (name, mock) in CHAINS.iter().zip(&mocks) {
        let chain = ChainName::new(*name).unwrap();
        engine.tick_once(&chain).await.unwrap();
        mock.advance(2);
        engine.tick_once(&chain).await.unwrap();
    }
    for _ in 0..3 {
        for (name, mock) in CHAINS.iter().zip(&mocks) {
            mock.advance(2);
            let before = mock.requests();
            engine
                .tick_once(&ChainName::new(*name).unwrap())
                .await
                .unwrap();
            assert_eq!(mock.requests() - before, 5, "{name}");
        }
    }

    let text = metrics_text(&registry);
    for name in CHAINS {
        assert!(
            text.contains(&format!("datalens_follow_lag_blocks{{chain=\"{name}\"}} 0")),
            "{text}"
        );
    }
    let mock = &mocks[0];
    let response = engine
        .query(
            &caller(),
            request(
                "ethereum",
                usdc(),
                range(9_993, mock.latest()),
                QueryFinality::SafeToLatest,
            ),
        )
        .await
        .unwrap();
    assert_eq!(response.served, Some(range(9_993, mock.latest())));
    assert_eq!(
        response.segments.last().unwrap().anchor,
        Some(Anchor {
            number: mock.latest(),
            hash: mock.hash(mock.latest()),
        })
    );
}
