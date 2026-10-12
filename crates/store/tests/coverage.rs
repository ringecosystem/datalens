#[path = "common/mod.rs"]
mod common;

use datalens_core::{Address, B256, Dataset, Finality, LogSelector, Selector, TopicFilter};
use datalens_store::{SelectorId, longest_covered_prefix};
use sqlx::PgPool;

use common::{chain, range, store};

#[sqlx::test]
async fn adjacent_and_overlapping_ranges_merge_and_missing_returns_the_gap(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(0, 9),
            Finality::Safe,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(10, 19),
            Finality::Safe,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(5, 12),
            Finality::Safe,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(40, 49),
            Finality::Finalized,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        coverage_rows(&pool, selector).await,
        vec![("[0,20)".to_owned(), 2), ("[40,50)".to_owned(), 3),]
    );
    assert_eq!(
        store
            .missing(chain, Dataset::EvmBlocks, &[selector], range(0, 49))
            .await
            .unwrap(),
        vec![range(20, 39)]
    );
    assert_eq!(
        store
            .covered(chain, Dataset::EvmBlocks, &[selector], range(0, 49))
            .await
            .unwrap(),
        vec![range(0, 19), range(40, 49)]
    );
}

#[sqlx::test]
async fn overlap_keeps_the_stronger_finality_on_its_own_row(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let selector = store
        .upsert_selector(chain, &Selector::Blocks)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(0, 19),
            Finality::Safe,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(10, 29),
            Finality::Finalized,
        )
        .await
        .unwrap();
    store
        .add_coverage(
            &mut tx,
            chain,
            Dataset::EvmBlocks,
            selector,
            range(0, 9),
            Finality::Safe,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        coverage_rows(&pool, selector).await,
        vec![("[0,10)".to_owned(), 2), ("[10,30)".to_owned(), 3)]
    );
}

#[sqlx::test]
async fn containment_uses_superset_selectors_and_address_intersection(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let a = Address::repeat_byte(0x0a);
    let b = Address::repeat_byte(0x0b);
    let t1 = B256::repeat_byte(0x01);
    let t2 = B256::repeat_byte(0x02);
    let t3 = B256::repeat_byte(0x03);

    let topic_wide = logs(&[a], TopicFilter::one_of([t1, t2]).unwrap());
    let address_wild = logs(&[], TopicFilter::one_of([t1]).unwrap());
    let only_a = logs(&[a], TopicFilter::Any);
    let only_b = logs(&[b], TopicFilter::Any);

    let topic_wide_id = store.upsert_selector(chain, &topic_wide).await.unwrap();
    let address_wild_id = store.upsert_selector(chain, &address_wild).await.unwrap();
    let only_a_id = store.upsert_selector(chain, &only_a).await.unwrap();
    let only_b_id = store.upsert_selector(chain, &only_b).await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    for (id, blocks, finality) in [
        (topic_wide_id, range(200, 249), Finality::Safe),
        (address_wild_id, range(300, 349), Finality::Finalized),
        (only_a_id, range(0, 99), Finality::Safe),
        (only_b_id, range(50, 80), Finality::Safe),
    ] {
        store
            .add_coverage(&mut tx, chain, Dataset::EvmLogs, id, blocks, finality)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let narrow_topic = log_selector(&[a], TopicFilter::one_of([t1]).unwrap());
    assert_eq!(
        store
            .covered_for_query(chain, &narrow_topic, range(200, 249))
            .await
            .unwrap(),
        vec![range(200, 249)]
    );

    let both_topics = log_selector(&[a], TopicFilter::one_of([t1, t3]).unwrap());
    assert_eq!(
        store
            .covered_for_query(chain, &both_topics, range(200, 249))
            .await
            .unwrap(),
        Vec::new()
    );

    let wildcard_hit = log_selector(&[a], TopicFilter::one_of([t1]).unwrap());
    assert_eq!(
        store
            .covered_for_query(chain, &wildcard_hit, range(300, 349))
            .await
            .unwrap(),
        vec![range(300, 349)]
    );

    let multi = log_selector(&[a, b], TopicFilter::Any);
    let intersection = store
        .covered_for_query(chain, &multi, range(0, 99))
        .await
        .unwrap();
    assert_eq!(intersection, vec![range(50, 80)]);
    assert_eq!(
        longest_covered_prefix(&intersection, range(50, 99)),
        Some(range(50, 80))
    );
    assert_eq!(longest_covered_prefix(&intersection, range(0, 99)), None);
}

async fn coverage_rows(pool: &PgPool, selector: SelectorId) -> Vec<(String, i16)> {
    sqlx::query_as(
        "select blocks::text, finality from coverage where selector_id = $1 order by blocks",
    )
    .bind(selector.get())
    .fetch_all(pool)
    .await
    .unwrap()
}

fn logs(addresses: &[Address], topic0: TopicFilter) -> Selector {
    Selector::Logs(log_selector(addresses, topic0))
}

fn log_selector(addresses: &[Address], topic0: TopicFilter) -> LogSelector {
    let mut topics = [
        TopicFilter::Any,
        TopicFilter::Any,
        TopicFilter::Any,
        TopicFilter::Any,
    ];
    topics[0] = topic0;
    LogSelector {
        addresses: addresses.iter().copied().collect(),
        topics,
    }
}
