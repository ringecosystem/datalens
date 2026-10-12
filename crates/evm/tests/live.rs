use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use datalens_core::{BlockRange, ChainName, Finality, LogSelector};
use datalens_evm::{EvmClient, HeadMode};
use datalens_provider::{Gateway, ProviderConfig, ProviderMetrics, Purpose, TokenWait};
use prometheus::Registry;

#[tokio::test]
#[ignore]
async fn publicnode_heads_and_ten_block_logs() {
    let registry = Registry::new();
    let metrics = ProviderMetrics::register(&registry).unwrap();
    let gateway = Gateway::new(
        ChainName::new("ethereum").unwrap(),
        ProviderConfig {
            url: "https://ethereum-rpc.publicnode.com".to_owned(),
            rps: NonZeroU32::new(5).unwrap(),
            burst: NonZeroU32::new(5).unwrap(),
            daily_calls: None,
            max_in_flight: NonZeroU32::new(2).unwrap(),
            min_range_blocks: NonZeroU64::new(10).unwrap(),
            max_get_logs_range_blocks: NonZeroU64::new(10).unwrap(),
        },
        metrics,
    )
    .unwrap();
    let evm = EvmClient::new(gateway, NonZeroUsize::new(1).unwrap(), 20).unwrap();
    let heads = evm
        .heads(HeadMode::Tags, Purpose::Head, None, TokenWait::UntilToken)
        .await
        .unwrap();
    assert!(heads.latest > 1_000_000);
    assert!(heads.finalized <= heads.safe);
    assert!(heads.safe <= heads.latest);
    let end = heads.latest - 500;
    let range = BlockRange::new(end - 9, end).unwrap();
    let selector = LogSelector {
        addresses: BTreeSet::from(["0x0000000000000000000000000000000000000001"
            .parse()
            .unwrap()]),
        topics: Default::default(),
    };
    let page = evm
        .get_logs(
            &selector,
            range,
            Purpose::Follow,
            None,
            TokenWait::UntilToken,
            Finality::Unsafe,
        )
        .await
        .unwrap();
    assert_eq!(page.fetched_to, Some(range.end()));
    assert!(page.rows.iter().all(|row| range.contains(row.block_number)));
    assert!(page.rows.len() < 100);
}
