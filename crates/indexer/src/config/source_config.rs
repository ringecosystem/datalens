use serde::Deserialize;

use super::{IndexDataset, parse_dataset, required_non_empty, required_u64};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceConfig {
    Evm(EvmSourceConfig),
    Solana(SolanaSourceConfig),
    Tron(TronSourceConfig),
}

impl SourceConfig {
    pub fn chain(&self) -> &str {
        match self {
            Self::Evm(source) => &source.chain,
            Self::Solana(source) => &source.chain,
            Self::Tron(source) => &source.chain,
        }
    }

    pub fn from_block(&self) -> u64 {
        match self {
            Self::Evm(source) => source.from_block,
            Self::Solana(source) => source.from_slot,
            Self::Tron(source) => source.from_block,
        }
    }

    pub fn to_block(&self) -> Option<u64> {
        match self {
            Self::Evm(source) => source.to_block,
            Self::Solana(source) => source.to_slot,
            Self::Tron(source) => source.to_block,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmSourceConfig {
    pub chain: String,
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub addresses: Vec<String>,
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaSourceConfig {
    pub chain: String,
    pub network_id: Option<String>,
    pub dataset: IndexDataset,
    pub from_slot: u64,
    pub to_slot: Option<u64>,
    pub selector: SolanaSelectorConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SolanaSelectorConfig {
    All,
    Address(String),
    Program(String),
    Signature(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronSourceConfig {
    pub chain: String,
    pub chain_id: u64,
    pub dataset: IndexDataset,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub contracts: Vec<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawSourceConfig {
    chain: Option<String>,
    family: Option<String>,
    chain_id: Option<u64>,
    network_id: Option<String>,
    dataset: Option<String>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    from_slot: Option<u64>,
    to_slot: Option<u64>,
    selector: Option<RawSelectorConfig>,
    #[serde(default)]
    addresses: Vec<String>,
    #[serde(default)]
    topics: Vec<String>,
    #[serde(default)]
    contracts: Vec<String>,
    #[serde(default)]
    events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSelectorConfig {
    kind: Option<String>,
    value: Option<String>,
}

pub(super) fn parse_sources(
    raw_sources: Vec<RawSourceConfig>,
    errors: &mut Vec<String>,
) -> Vec<SourceConfig> {
    if raw_sources.is_empty() {
        errors.push("sources: at least one source is required".to_owned());
        return Vec::new();
    }

    raw_sources
        .into_iter()
        .enumerate()
        .filter_map(|(index, raw)| parse_source(index, raw, errors))
        .collect()
}

fn parse_source(
    index: usize,
    raw: RawSourceConfig,
    errors: &mut Vec<String>,
) -> Option<SourceConfig> {
    let prefix = format!("sources[{index}]");
    let chain = required_non_empty(&format!("{prefix}.chain"), raw.chain.clone(), errors);
    let family = required_non_empty(&format!("{prefix}.family"), raw.family.clone(), errors);

    match family.as_deref() {
        Some("evm") => parse_evm_source(prefix, chain, raw, errors),
        Some("solana") => parse_solana_source(prefix, chain, raw, errors),
        Some("tron") => parse_tron_source(prefix, chain, raw, errors),
        Some(value) => {
            errors.push(format!(
                "{prefix}.family: unsupported family {value}; supported values are evm, solana, and tron"
            ));
            None
        }
        None => None,
    }
}

fn parse_evm_source(
    prefix: String,
    chain: Option<String>,
    raw: RawSourceConfig,
    errors: &mut Vec<String>,
) -> Option<SourceConfig> {
    let chain_id = required_u64(&format!("{prefix}.chain_id"), raw.chain_id, errors);
    let from_block = required_u64(&format!("{prefix}.from_block"), raw.from_block, errors);
    validate_block_range(&prefix, from_block, raw.to_block, errors);
    validate_hex_values(
        &format!("{prefix}.addresses"),
        &raw.addresses,
        HexKind::Address,
        errors,
    );
    validate_hex_values(
        &format!("{prefix}.topics"),
        &raw.topics,
        HexKind::Topic,
        errors,
    );
    Some(SourceConfig::Evm(EvmSourceConfig {
        chain: chain?,
        chain_id: chain_id?,
        from_block: from_block?,
        to_block: raw.to_block,
        addresses: raw.addresses,
        topics: raw.topics,
    }))
}

fn parse_solana_source(
    prefix: String,
    chain: Option<String>,
    raw: RawSourceConfig,
    errors: &mut Vec<String>,
) -> Option<SourceConfig> {
    let dataset = raw
        .dataset
        .as_deref()
        .map(|value| parse_dataset(&format!("{prefix}.dataset"), value, errors))
        .unwrap_or(Some(IndexDataset::SolanaTransactions));
    if !matches!(
        dataset,
        Some(
            IndexDataset::SolanaTransactions
                | IndexDataset::SolanaInstructions
                | IndexDataset::SolanaAccountUpdates
        )
    ) {
        errors.push(format!(
            "{prefix}.dataset: solana sources support solana.transactions, solana.instructions, and solana.account_updates"
        ));
    }
    let from_slot = required_u64(&format!("{prefix}.from_slot"), raw.from_slot, errors);
    if let (Some(from_slot), Some(to_slot)) = (from_slot, raw.to_slot)
        && from_slot > to_slot
    {
        errors.push(format!(
            "{prefix}.to_slot: must be greater than or equal to from_slot"
        ));
    }
    let selector = parse_solana_selector(&prefix, raw.selector, errors);
    let network_id = raw
        .network_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());

    Some(SourceConfig::Solana(SolanaSourceConfig {
        chain: chain?,
        network_id,
        dataset: dataset?,
        from_slot: from_slot?,
        to_slot: raw.to_slot,
        selector: selector?,
    }))
}

fn parse_tron_source(
    prefix: String,
    chain: Option<String>,
    raw: RawSourceConfig,
    errors: &mut Vec<String>,
) -> Option<SourceConfig> {
    let dataset = raw
        .dataset
        .as_deref()
        .map(|value| parse_dataset(&format!("{prefix}.dataset"), value, errors))
        .unwrap_or(Some(IndexDataset::TronEvents));
    if !matches!(dataset, Some(IndexDataset::TronEvents)) {
        errors.push(format!(
            "{prefix}.dataset: tron sources support tron.events"
        ));
    }
    let chain_id = required_u64(&format!("{prefix}.chain_id"), raw.chain_id, errors);
    let from_block = required_u64(&format!("{prefix}.from_block"), raw.from_block, errors);
    validate_block_range(&prefix, from_block, raw.to_block, errors);
    if raw.contracts.is_empty() {
        errors.push(format!(
            "{prefix}.contracts: at least one contract is required"
        ));
    }
    if let Err(error) =
        datalens_client::QuerySelector::tron_event(datalens_client::TronEventSelector {
            contract_addresses: raw.contracts.clone(),
            event_names: raw.events.clone(),
        })
    {
        errors.push(format!("{prefix}.selector: {error}"));
    }

    Some(SourceConfig::Tron(TronSourceConfig {
        chain: chain?,
        chain_id: chain_id?,
        dataset: dataset?,
        from_block: from_block?,
        to_block: raw.to_block,
        contracts: raw.contracts,
        events: raw.events,
    }))
}

fn validate_block_range(
    prefix: &str,
    from_block: Option<u64>,
    to_block: Option<u64>,
    errors: &mut Vec<String>,
) {
    if let (Some(from_block), Some(to_block)) = (from_block, to_block)
        && from_block > to_block
    {
        errors.push(format!(
            "{prefix}.to_block: must be greater than or equal to from_block"
        ));
    }
}

fn parse_solana_selector(
    prefix: &str,
    raw: Option<RawSelectorConfig>,
    errors: &mut Vec<String>,
) -> Option<SolanaSelectorConfig> {
    let Some(raw) = raw else {
        return Some(SolanaSelectorConfig::All);
    };
    let kind = required_non_empty(&format!("{prefix}.selector.kind"), raw.kind, errors);
    let value = raw.value.map(|value| value.trim().to_owned());
    match kind.as_deref() {
        Some("all") => Some(SolanaSelectorConfig::All),
        Some("address") => {
            validate_solana_selector_value(prefix, value, errors).map(SolanaSelectorConfig::Address)
        }
        Some("program") => {
            validate_solana_selector_value(prefix, value, errors).map(SolanaSelectorConfig::Program)
        }
        Some("signature") => validate_solana_selector_value(prefix, value, errors)
            .map(SolanaSelectorConfig::Signature),
        Some(value) => {
            errors.push(format!(
                "{prefix}.selector.kind: unsupported solana selector {value}; supported values are all, address, program, and signature"
            ));
            None
        }
        None => None,
    }
}

fn validate_solana_selector_value(
    prefix: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    let value = required_non_empty(&format!("{prefix}.selector.value"), value, errors)?;
    if let Err(error) = datalens_client::QuerySelector::solana_address(&value) {
        errors.push(format!("{prefix}.selector.value: {error}"));
        return None;
    }
    Some(value)
}

enum HexKind {
    Address,
    Topic,
}

fn validate_hex_values(field: &str, values: &[String], kind: HexKind, errors: &mut Vec<String>) {
    let expected_len = match kind {
        HexKind::Address => 42,
        HexKind::Topic => 66,
    };
    for (index, value) in values.iter().enumerate() {
        if value.len() != expected_len
            || !value.starts_with("0x")
            || !value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            errors.push(format!(
                "{field}[{index}]: must be a 0x-prefixed {}-byte hex value",
                (expected_len - 2) / 2
            ));
        }
    }
}
