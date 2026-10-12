#![allow(dead_code)]

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use datalens_core::ChainName;
use datalens_evm::EvmClient;
use datalens_provider::{Gateway, ProviderConfig, ProviderMetrics};
use prometheus::Registry;

pub fn client(
    url: &str,
    max_addresses: usize,
    batch: usize,
    max_range: u64,
    min_range: u64,
) -> EvmClient {
    let registry = Registry::new();
    let metrics = ProviderMetrics::register(&registry).unwrap();
    let gateway = Gateway::new(
        ChainName::new("arbitrum").unwrap(),
        ProviderConfig {
            url: url.to_owned(),
            rps: NonZeroU32::new(50).unwrap(),
            burst: NonZeroU32::new(50).unwrap(),
            daily_calls: None,
            max_in_flight: NonZeroU32::new(4).unwrap(),
            min_range_blocks: NonZeroU64::new(min_range).unwrap(),
            max_get_logs_range_blocks: NonZeroU64::new(max_range).unwrap(),
        },
        metrics,
    )
    .unwrap();
    EvmClient::new(gateway, NonZeroUsize::new(max_addresses).unwrap(), batch)
}

pub fn hex_u64(text: &str) -> u64 {
    u64::from_str_radix(text.trim_start_matches("0x"), 16).unwrap()
}

pub fn block_span(call: &serde_json::Value) -> u64 {
    let filter = &call["params"][0];
    let start = hex_u64(filter["fromBlock"].as_str().unwrap());
    let end = hex_u64(filter["toBlock"].as_str().unwrap());
    end - start + 1
}
