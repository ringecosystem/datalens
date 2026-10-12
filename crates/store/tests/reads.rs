#[path = "common/mod.rs"]
mod common;

use datalens_core::{
    Address, B256, Bytes, Dataset, Finality, LogRow, LogSelector, Selector, TopicFilter,
};
use datalens_store::LogCursor;
use sqlx::PgPool;

use common::{chain, range, store};

#[sqlx::test]
async fn rewind_moves_only_follow_rows_at_or_above_the_fork(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let start = B256::repeat_byte(0x01);
    let mut tx = pool.begin().await.unwrap();
    let mut selectors = Vec::new();
    for (byte, head, promoted) in [(0x0a, 120, 100), (0x0b, 105, 100), (0x0c, 109, 109)] {
        let selector = store
            .upsert_selector(
                chain,
                &Selector::Logs(LogSelector {
                    addresses: [Address::repeat_byte(byte)].into_iter().collect(),
                    topics: Default::default(),
                }),
            )
            .await
            .unwrap();
        store
            .follow_get_or_init(chain, Dataset::EvmLogs, selector, 100, &start)
            .await
            .unwrap();
        store
            .advance_follow_head(
                &mut tx,
                chain,
                Dataset::EvmLogs,
                selector,
                head,
                &B256::repeat_byte(byte),
            )
            .await
            .unwrap();
        store
            .advance_follow_promoted(&mut tx, chain, Dataset::EvmLogs, selector, promoted)
            .await
            .unwrap();
        selectors.push(selector);
    }
    let canonical = B256::repeat_byte(0x77);
    let rewound = store
        .rewind_follow(&mut tx, chain, 110, &canonical)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        rewound
            .iter()
            .map(|row| (row.selector_id, row.head_block, row.head_hash))
            .collect::<Vec<_>>(),
        vec![(selectors[0], 109, canonical)]
    );
    let listed: Vec<_> = store
        .list_follow(chain)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.selector_id,
                row.head_block,
                row.head_hash,
                row.promoted_to,
            )
        })
        .collect();
    assert_eq!(
        listed,
        vec![
            (selectors[0], 109, canonical, 100),
            (selectors[1], 105, B256::repeat_byte(0x0b), 100),
            (selectors[2], 109, B256::repeat_byte(0x0c), 109),
        ]
    );
}

#[sqlx::test]
async fn read_logs_filters_several_addresses_and_later_topic_positions(pool: PgPool) {
    let store = store(&pool).await;
    let chain = chain();
    let a = Address::repeat_byte(0x0a);
    let b = Address::repeat_byte(0x0b);
    let c = Address::repeat_byte(0x0c);
    let t = |byte| B256::repeat_byte(byte);
    let logs = vec![
        log(1, 0, a, &[t(1), t(2), t(3), t(4)]),
        log(1, 1, b, &[t(1), t(2), t(3), t(5)]),
        log(2, 0, c, &[t(1), t(2), t(3), t(4)]),
        log(2, 1, a, &[t(1), t(9), t(3), t(4)]),
        log(3, 0, b, &[t(1), t(2), t(8), t(4)]),
        log(3, 1, a, &[t(1), t(2)]),
        log(4, 0, b, &[t(1), t(2), t(3), t(6)]),
    ];
    let mut tx = pool.begin().await.unwrap();
    store.insert_logs(&mut tx, chain, &logs).await.unwrap();
    tx.commit().await.unwrap();

    let selector = LogSelector {
        addresses: [a, b].into_iter().collect(),
        topics: [
            TopicFilter::Any,
            TopicFilter::one_of([t(2)]).unwrap(),
            TopicFilter::one_of([t(3)]).unwrap(),
            TopicFilter::one_of([t(4), t(5), t(6)]).unwrap(),
        ],
    };
    let (page, cursor) = store
        .read_logs(chain, &selector, range(1, 4), None, 2)
        .await
        .unwrap();
    assert_eq!(page, vec![logs[0].clone(), logs[1].clone()]);
    assert_eq!(
        cursor,
        Some(LogCursor {
            block_number: 1,
            log_index: 1,
        })
    );
    let (page, cursor) = store
        .read_logs(chain, &selector, range(1, 4), cursor, 2)
        .await
        .unwrap();
    assert_eq!(page, vec![logs[6].clone()]);
    assert_eq!(cursor, None);
}

fn log(block: u64, index: u32, address: Address, topics: &[B256]) -> LogRow {
    LogRow {
        block_number: block,
        log_index: index,
        block_hash: B256::repeat_byte(0x11),
        transaction_index: index,
        transaction_hash: B256::repeat_byte(0x22),
        address,
        topics: topics.to_vec(),
        data: Bytes::from_static(&[0xcd]),
        finality: Finality::Safe,
    }
}
