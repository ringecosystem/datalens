use datalens_core::{EvmLogFilter, LogFilter};
use datalens_evm::{EvmLogBloom, EvmLogBloomInput};

const ENS_TOKEN: &str = "0xC18360217D8F7Ab5e7c516566761ea12ce7f9d72";
const ENS_GOVERNOR: &str = "0x323A76393544d5ecca80cd6ef2a560c6a395b7E3";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const VOTE_CAST_TOPIC: &str = "0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4";
const UNRELATED_ADDRESS: &str = "0x000000000000000000000000000000000000dead";
const UNRELATED_TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn test_log_bloom_contains_known_ens_vote_cast_address_and_topic() {
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(ENS_GOVERNOR),
        EvmLogBloomInput::Topic(VOTE_CAST_TOPIC),
    ])
    .expect("bloom fixture");

    assert!(bloom.may_contain_address(ENS_GOVERNOR).expect("address"));
    assert!(bloom.may_contain_topic(VOTE_CAST_TOPIC).expect("topic"));
    assert!(
        bloom
            .may_match_filter(&evm_filter(
                vec![ENS_GOVERNOR],
                vec![Some(vec![VOTE_CAST_TOPIC])]
            ))
            .expect("filter")
    );
}

#[test]
fn test_log_bloom_contains_known_ens_transfer_address_and_topic() {
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(ENS_TOKEN),
        EvmLogBloomInput::Topic(TRANSFER_TOPIC),
    ])
    .expect("bloom fixture");

    assert!(bloom.may_contain_address(ENS_TOKEN).expect("address"));
    assert!(bloom.may_contain_topic(TRANSFER_TOPIC).expect("topic"));
    assert!(
        bloom
            .may_match_filter(&evm_filter(
                vec![ENS_TOKEN],
                vec![Some(vec![TRANSFER_TOPIC])]
            ))
            .expect("filter")
    );
}

#[test]
fn test_log_bloom_rejects_unrelated_address_and_topic() {
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(ENS_TOKEN),
        EvmLogBloomInput::Topic(TRANSFER_TOPIC),
    ])
    .expect("bloom fixture");

    assert!(
        !bloom
            .may_contain_address(UNRELATED_ADDRESS)
            .expect("address")
    );
    assert!(!bloom.may_contain_topic(UNRELATED_TOPIC).expect("topic"));
    assert!(
        !bloom
            .may_match_filter(&evm_filter(
                vec![UNRELATED_ADDRESS],
                vec![Some(vec![UNRELATED_TOPIC])]
            ))
            .expect("filter")
    );
}

#[test]
fn test_log_bloom_filter_wildcards_do_not_make_match_impossible() {
    let bloom = EvmLogBloom::from_inputs([
        EvmLogBloomInput::Address(ENS_TOKEN),
        EvmLogBloomInput::Topic(TRANSFER_TOPIC),
    ])
    .expect("bloom fixture");

    assert!(
        bloom
            .may_match_filter(&evm_filter(vec![ENS_TOKEN], vec![None]))
            .expect("filter")
    );
    assert!(
        bloom
            .may_match_filter(&evm_filter(Vec::new(), vec![Some(vec![TRANSFER_TOPIC])]))
            .expect("filter")
    );
}

fn evm_filter(addresses: Vec<&str>, topics: Vec<Option<Vec<&str>>>) -> EvmLogFilter {
    EvmLogFilter::try_from(LogFilter {
        addresses: addresses.into_iter().map(str::to_owned).collect(),
        topics: topics
            .into_iter()
            .map(|slot| slot.map(|values| values.into_iter().map(str::to_owned).collect()))
            .collect(),
    })
    .expect("filter")
}
