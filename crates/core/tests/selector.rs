use std::collections::BTreeSet;

use alloy_primitives::{address, b256};
use datalens_core::{
    Address, B256, Bytes, Dataset, Finality, LogRow, LogSelector, Selector, TopicFilter,
};

const A: Address = address!("0x000000000000000000000000000000000000000a");
const B: Address = address!("0x000000000000000000000000000000000000000b");
const C: Address = address!("0x000000000000000000000000000000000000000c");
const USDC: Address = address!("0xaf88d065e77c8cC2239327C5EDb3A432268e5831");
const T1: B256 = b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
const T2: B256 = b256!("0x0000000000000000000000000000000000000000000000000000000000000002");
const T3: B256 = b256!("0x0000000000000000000000000000000000000000000000000000000000000003");

fn any() -> TopicFilter {
    TopicFilter::Any
}

fn one_of(topics: &[B256]) -> TopicFilter {
    TopicFilter::one_of(topics.iter().copied()).unwrap()
}

fn sel(addresses: &[Address], topics: [TopicFilter; 4]) -> LogSelector {
    LogSelector {
        addresses: addresses.iter().copied().collect(),
        topics,
    }
}

fn log(address: Address, topics: &[B256]) -> LogRow {
    LogRow {
        block_number: 10,
        log_index: 0,
        block_hash: B256::ZERO,
        transaction_index: 0,
        transaction_hash: B256::ZERO,
        address,
        topics: topics.to_vec(),
        data: Bytes::new(),
        finality: Finality::Finalized,
    }
}

#[test]
fn empty_topic_filter_is_rejected() {
    assert_eq!(TopicFilter::one_of([]).unwrap_err().kind(), "invalid_input");
    assert!(serde_json::from_str::<TopicFilter>("[]").is_err());
}

#[test]
fn canonical_key_is_sorted_lowercase_hex() {
    let s = sel(&[USDC, A], [one_of(&[T2, T1]), any(), one_of(&[T3]), any()]);
    assert_eq!(
        s.canonical_key(),
        "addresses=0x000000000000000000000000000000000000000a,0xaf88d065e77c8cc2239327c5edb3a432268e5831;\
         topics=0x0000000000000000000000000000000000000000000000000000000000000001,\
         0x0000000000000000000000000000000000000000000000000000000000000002|*|\
         0x0000000000000000000000000000000000000000000000000000000000000003|*"
    );
    assert_eq!(
        s.fingerprint(),
        "a895b18979537e972255cea58bc5ca57aa912076a3c03e26405764fbdd73bd68"
    );
}

#[test]
fn wildcard_selector_and_blocks_keys() {
    let wildcard = LogSelector::default();
    assert_eq!(wildcard.canonical_key(), "addresses=*;topics=*|*|*|*");
    assert_eq!(
        wildcard.fingerprint(),
        "59a8e456c456e3064fa2ecbd156dbfef76752026bef80f6747299ce6f8758022"
    );
    assert_eq!(Selector::Blocks.canonical_key(), "all");
    assert_eq!(
        Selector::Blocks.fingerprint(),
        "5ef5ef0364b6939c4ca61f34b393f7b368d1be8619647aaf83d5b395919ab629"
    );
    assert_eq!(Selector::Blocks.dataset(), Dataset::EvmBlocks);
    assert_eq!(Selector::Logs(wildcard).dataset(), Dataset::EvmLogs);
}

#[test]
fn serde_pads_missing_topics_and_trims_trailing_wildcards() {
    let parsed: LogSelector = serde_json::from_str(&format!(
        r#"{{"addresses":["{USDC}"],"topics":[["{T1}"],null]}}"#
    ))
    .unwrap();
    assert_eq!(parsed, sel(&[USDC], [one_of(&[T1]), any(), any(), any()]));
    assert_eq!(
        serde_json::to_string(&parsed).unwrap(),
        r#"{"addresses":["0xaf88d065e77c8cc2239327c5edb3a432268e5831"],"topics":[["0x0000000000000000000000000000000000000000000000000000000000000001"]]}"#
    );
    assert_eq!(
        serde_json::from_str::<LogSelector>("{}").unwrap(),
        LogSelector::default()
    );
}

#[test]
fn serde_rejects_five_topics_empty_topic_list_and_unknown_fields() {
    assert!(
        serde_json::from_str::<LogSelector>(r#"{"topics":[null,null,null,null,null]}"#).is_err()
    );
    assert!(serde_json::from_str::<LogSelector>(r#"{"topics":[[]]}"#).is_err());
    assert!(serde_json::from_str::<LogSelector>(r#"{"address":[]}"#).is_err());
}

#[test]
fn covers_address_rules() {
    let t = || [one_of(&[T1]), any(), any(), any()];
    let any_address = sel(&[], t());
    let ab = sel(&[A, B], t());
    let a = sel(&[A], t());
    let ac = sel(&[A, C], t());

    assert!(any_address.covers(&ab));
    assert!(any_address.covers(&any_address));
    assert!(ab.covers(&a));
    assert!(ab.covers(&ab));
    assert!(!a.covers(&ab));
    assert!(!ab.covers(&ac));
    assert!(!ab.covers(&any_address));
}

#[test]
fn covers_topic_rules() {
    let wide = sel(&[A], [one_of(&[T1, T2]), any(), any(), any()]);
    let narrow = sel(&[A], [one_of(&[T1]), one_of(&[T3]), any(), any()]);
    let other_topic = sel(&[A], [one_of(&[T3]), any(), any(), any()]);
    let topic0_any = sel(&[A], [any(), any(), any(), any()]);

    assert!(wide.covers(&narrow));
    assert!(!narrow.covers(&wide));
    assert!(!wide.covers(&other_topic));
    assert!(topic0_any.covers(&wide));
    assert!(!wide.covers(&topic0_any));
}

#[test]
fn matches_address_and_topic_positions() {
    let s = sel(&[A, B], [one_of(&[T1]), any(), one_of(&[T3]), any()]);
    assert!(s.matches(&log(A, &[T1, T2, T3])));
    assert!(s.matches(&log(B, &[T1, T1, T3, T2])));
    assert!(!s.matches(&log(C, &[T1, T2, T3])));
    assert!(!s.matches(&log(A, &[T2, T2, T3])));
    assert!(!s.matches(&log(A, &[T1, T2])));

    let any_address = sel(&[], [any(), any(), any(), any()]);
    assert!(any_address.matches(&log(C, &[])));
    assert!(!sel(&[], [one_of(&[T1]), any(), any(), any()]).matches(&log(C, &[])));
}

#[test]
fn split_by_address() {
    let t = [one_of(&[T1]), any(), any(), any()];
    assert_eq!(
        sel(&[B, A], t.clone()).split_by_address(),
        vec![sel(&[A], t.clone()), sel(&[B], t.clone())]
    );
    assert_eq!(sel(&[], t.clone()).split_by_address(), vec![sel(&[], t)]);
}

#[test]
fn batch_unions_addresses_and_topic0() {
    let members = [
        sel(&[A], [one_of(&[T1]), one_of(&[T3]), any(), any()]),
        sel(&[B, A], [one_of(&[T2]), one_of(&[T3]), any(), any()]),
    ];
    let batched = LogSelector::batch(&members, 32).unwrap();
    assert_eq!(
        batched,
        sel(&[A, B], [one_of(&[T1, T2]), one_of(&[T3]), any(), any()])
    );
    assert!(members.iter().all(|member| batched.covers(member)));
}

#[test]
fn batch_topic0_any_when_one_member_leaves_it_open() {
    let members = [
        sel(&[A], [one_of(&[T1]), any(), any(), any()]),
        sel(&[B], [any(), any(), any(), any()]),
    ];
    assert_eq!(
        LogSelector::batch(&members, 32).unwrap(),
        sel(&[A, B], [any(), any(), any(), any()])
    );
}

#[test]
fn batch_differing_later_positions_become_any() {
    let members = [
        sel(&[A], [one_of(&[T1]), one_of(&[T2]), one_of(&[T3]), any()]),
        sel(&[B], [one_of(&[T1]), one_of(&[T3]), one_of(&[T3]), any()]),
    ];
    assert_eq!(
        LogSelector::batch(&members, 32).unwrap(),
        sel(&[A, B], [one_of(&[T1]), any(), one_of(&[T3]), any()])
    );
}

#[test]
fn batch_mixed_any_address_does_not_merge() {
    let members = [
        sel(&[A], [one_of(&[T1]), any(), any(), any()]),
        sel(&[], [one_of(&[T2]), any(), any(), any()]),
    ];
    assert_eq!(LogSelector::batch(&members, 32), None);
    assert_eq!(LogSelector::batch(&members, 1), None);
}

#[test]
fn batch_of_only_any_address_members_unions_topic0() {
    let members = [
        sel(&[], [one_of(&[T1]), one_of(&[T3]), any(), any()]),
        sel(&[], [one_of(&[T2]), one_of(&[T3]), any(), any()]),
    ];
    assert_eq!(
        LogSelector::batch(&members, 1).unwrap(),
        sel(&[], [one_of(&[T1, T2]), one_of(&[T3]), any(), any()])
    );
}

#[test]
fn batch_respects_max_addresses_and_empty_input() {
    let t = || [one_of(&[T1]), any(), any(), any()];
    let members = [sel(&[A, B], t()), sel(&[B, C], t())];
    assert_eq!(
        LogSelector::batch(&members, 3).unwrap().addresses,
        BTreeSet::from([A, B, C])
    );
    assert_eq!(LogSelector::batch(&members, 2), None);
    assert_eq!(LogSelector::batch(&[], 32), None);
}

#[test]
fn batch_of_one_is_identity() {
    let only = sel(&[A, B], [one_of(&[T1]), any(), one_of(&[T2]), any()]);
    assert_eq!(
        LogSelector::batch(std::slice::from_ref(&only), 1),
        Some(only)
    );
    let any_address = sel(&[], [one_of(&[T1]), any(), any(), any()]);
    assert_eq!(
        LogSelector::batch(std::slice::from_ref(&any_address), 1),
        Some(any_address)
    );
}
