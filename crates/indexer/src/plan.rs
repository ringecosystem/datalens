use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;

use crate::{
    DatalensIndexConfig, DecodeEventInputConfig, IndexerError, SolanaSelectorConfig, SourceConfig,
    evm_decode::{DecodeScope, planned_events_from_abi_json},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexPlan {
    application: String,
    index: String,
    #[serde(skip_serializing)]
    decode_events: Vec<PlannedDecodeEvent>,
    tasks: Vec<PlannedIndexTask>,
}

impl IndexPlan {
    pub fn empty(application: impl Into<String>) -> Self {
        Self {
            application: application.into(),
            index: String::new(),
            decode_events: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn application(&self) -> &str {
        &self.application
    }

    pub fn index(&self) -> &str {
        &self.index
    }

    pub fn tasks(&self) -> &[PlannedIndexTask] {
        &self.tasks
    }

    pub fn decode_events(&self) -> &[PlannedDecodeEvent] {
        &self.decode_events
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlannedIndexTask {
    pub label: String,
    #[serde(skip_serializing)]
    pub index: String,
    pub source_identity: String,
    pub chain: String,
    pub family: String,
    pub chain_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_id: Option<String>,
    pub dataset: String,
    pub range: PlannedRange,
    pub selector: PlannedSelector,
    pub finality: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlannedRange {
    pub kind: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlannedSelector {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_count: Option<usize>,
    #[serde(skip_serializing_if = "is_zero")]
    pub address_count: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub topic_count: usize,
    #[serde(skip_serializing)]
    pub addresses: Vec<String>,
    #[serde(skip_serializing)]
    pub topics: Vec<String>,
    #[serde(skip_serializing)]
    pub values: Vec<String>,
    #[serde(skip_serializing)]
    pub canonical_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlannedDecodeEvent {
    pub name: String,
    pub signature: String,
    pub topic0: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,
    pub inputs: Vec<PlannedDecodeEventInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlannedDecodeEventInput {
    pub name: String,
    pub kind: String,
    pub indexed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexPlanBuilder;

impl IndexPlanBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&self, config: &DatalensIndexConfig) -> Result<IndexPlan, IndexerError> {
        let mut sources = config
            .sources
            .iter()
            .enumerate()
            .map(|(source_index, source)| PlannedSource::new(source_index, source))
            .collect::<Vec<_>>();
        sources.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));

        let mut tasks = Vec::new();
        for (planned_source_index, source) in sources.iter().enumerate() {
            tasks.extend(plan_source(config, planned_source_index, source)?);
        }

        Ok(IndexPlan {
            application: config.client.application.clone(),
            index: config.index.name.clone(),
            decode_events: planned_decode_events(config)?,
            tasks,
        })
    }
}

fn planned_decode_events(
    config: &DatalensIndexConfig,
) -> Result<Vec<PlannedDecodeEvent>, IndexerError> {
    if !config.decode.enabled {
        return Ok(Vec::new());
    }
    let mut events = config
        .decode
        .events
        .iter()
        .map(|event| PlannedDecodeEvent {
            name: event.name.clone(),
            signature: event.signature.clone(),
            topic0: event.topic0.clone(),
            chain: event.chain.clone(),
            index: event.index.clone(),
            dataset: event.dataset.clone(),
            contract: event.contract.clone(),
            inputs: planned_decode_event_inputs(&event.inputs),
        })
        .collect::<Vec<_>>();
    for abi in &config.decode.abis {
        let json = match (&abi.path, &abi.json) {
            (Some(path), None) => fs::read_to_string(path).map_err(|error| {
                IndexerError::Config(format!("decode.abis path {}: {error}", path.display()))
            })?,
            (None, Some(json)) => json.clone(),
            _ => continue,
        };
        events.extend(planned_events_from_abi_json(
            DecodeScope {
                chain: abi.chain.clone(),
                index: abi.index.clone(),
                dataset: abi.dataset.clone(),
            },
            &json,
        )?);
    }
    Ok(events)
}

fn planned_decode_event_inputs(inputs: &[DecodeEventInputConfig]) -> Vec<PlannedDecodeEventInput> {
    inputs
        .iter()
        .map(|input| PlannedDecodeEventInput {
            name: input.name.clone(),
            kind: input.kind.clone(),
            indexed: input.indexed,
        })
        .collect()
}

struct PlannedSource<'a> {
    original_index: usize,
    source: &'a SourceConfig,
}

impl<'a> PlannedSource<'a> {
    fn new(original_index: usize, source: &'a SourceConfig) -> Self {
        Self {
            original_index,
            source,
        }
    }

    fn sort_key(&self) -> (&str, &str, u64, u64, Option<u64>, usize) {
        match self.source {
            SourceConfig::Evm(source) => (
                "evm",
                source.chain.as_str(),
                source.chain_id,
                source.from_block,
                source.to_block,
                self.original_index,
            ),
            SourceConfig::Solana(source) => (
                "solana",
                source.chain.as_str(),
                0,
                source.from_slot,
                source.to_slot,
                self.original_index,
            ),
            SourceConfig::Tron(source) => (
                "tron",
                source.chain.as_str(),
                source.chain_id,
                source.from_block,
                source.to_block,
                self.original_index,
            ),
        }
    }
}

fn plan_source(
    config: &DatalensIndexConfig,
    planned_source_index: usize,
    source: &PlannedSource<'_>,
) -> Result<Vec<PlannedIndexTask>, IndexerError> {
    match source.source {
        SourceConfig::Evm(evm_source) => {
            let to_block = evm_source.to_block.ok_or_else(|| {
                IndexerError::Plan(format!(
                    "sources[{}].to_block is required for index plan",
                    source.original_index
                ))
            })?;
            let source_identity = format!(
                "evm:{}:{}:{planned_source_index:03}",
                evm_source.chain, evm_source.chain_id
            );
            Ok(
                chunk_ranges(evm_source.from_block, to_block, config.index.chunk_blocks)
                    .into_iter()
                    .enumerate()
                    .map(|(chunk_index, range)| PlannedIndexTask {
                        label: format!(
                            "{}.{planned_source_index:03}.{chunk_index:06}",
                            config.index.name
                        ),
                        index: config.index.name.clone(),
                        source_identity: source_identity.clone(),
                        chain: evm_source.chain.clone(),
                        family: "evm".to_owned(),
                        chain_id: evm_source.chain_id,
                        network_id: None,
                        dataset: config.index.dataset.as_str().to_owned(),
                        range,
                        selector: PlannedSelector {
                            kind: "evm_logs".to_owned(),
                            fingerprint: None,
                            value_count: None,
                            address_count: evm_source.addresses.len(),
                            topic_count: evm_source.topics.len(),
                            addresses: evm_source.addresses.clone(),
                            topics: evm_source.topics.clone(),
                            values: Vec::new(),
                            canonical_key: None,
                        },
                        finality: "durable".to_owned(),
                    })
                    .collect(),
            )
        }
        SourceConfig::Solana(solana_source) => {
            let to_slot = solana_source.to_slot.ok_or_else(|| {
                IndexerError::Plan(format!(
                    "sources[{}].to_slot is required for index plan",
                    source.original_index
                ))
            })?;
            let network_id = solana_source.network_id.clone();
            let network_key = network_id.as_deref().unwrap_or("none");
            let source_identity = format!(
                "solana:{}:{network_key}:{planned_source_index:03}",
                solana_source.chain
            );
            let selector = solana_selector_plan(&solana_source.selector);
            Ok(
                chunk_ranges(solana_source.from_slot, to_slot, config.index.chunk_blocks)
                    .into_iter()
                    .enumerate()
                    .map(|(chunk_index, mut range)| {
                        range.kind = "slot".to_owned();
                        PlannedIndexTask {
                            label: format!(
                                "{}.{planned_source_index:03}.{chunk_index:06}",
                                config.index.name
                            ),
                            index: config.index.name.clone(),
                            source_identity: source_identity.clone(),
                            chain: solana_source.chain.clone(),
                            family: "solana".to_owned(),
                            chain_id: 0,
                            network_id: network_id.clone(),
                            dataset: solana_source.dataset.as_str().to_owned(),
                            range,
                            selector: selector.clone(),
                            finality: "durable".to_owned(),
                        }
                    })
                    .collect(),
            )
        }
        SourceConfig::Tron(tron_source) => {
            let to_block = tron_source.to_block.ok_or_else(|| {
                IndexerError::Plan(format!(
                    "sources[{}].to_block is required for index plan",
                    source.original_index
                ))
            })?;
            let source_identity = format!(
                "tron:{}:{}:{planned_source_index:03}",
                tron_source.chain, tron_source.chain_id
            );
            let selector = tron_selector_plan(&tron_source.contracts, &tron_source.events);
            Ok(
                chunk_ranges(tron_source.from_block, to_block, config.index.chunk_blocks)
                    .into_iter()
                    .enumerate()
                    .map(|(chunk_index, range)| PlannedIndexTask {
                        label: format!(
                            "{}.{planned_source_index:03}.{chunk_index:06}",
                            config.index.name
                        ),
                        index: config.index.name.clone(),
                        source_identity: source_identity.clone(),
                        chain: tron_source.chain.clone(),
                        family: "tron".to_owned(),
                        chain_id: tron_source.chain_id,
                        network_id: None,
                        dataset: tron_source.dataset.as_str().to_owned(),
                        range,
                        selector: selector.clone(),
                        finality: "durable".to_owned(),
                    })
                    .collect(),
            )
        }
    }
}

fn solana_selector_plan(selector: &SolanaSelectorConfig) -> PlannedSelector {
    let (kind, value, canonical_key) = match selector {
        SolanaSelectorConfig::All => ("solana_all", None, "all".to_owned()),
        SolanaSelectorConfig::Address(value) => (
            "solana_address",
            Some(value.clone()),
            format!("address/{value}"),
        ),
        SolanaSelectorConfig::Program(value) => (
            "solana_program",
            Some(value.clone()),
            format!("program/{value}"),
        ),
        SolanaSelectorConfig::Signature(value) => (
            "solana_signature",
            Some(value.clone()),
            format!("signature/{value}"),
        ),
    };
    let values = value.into_iter().collect::<Vec<_>>();
    PlannedSelector {
        kind: kind.to_owned(),
        fingerprint: Some(format!(
            "{}/{}",
            kind.replace('_', "-"),
            digest_prefix(&canonical_key, 8)
        )),
        value_count: Some(values.len()),
        address_count: 0,
        topic_count: 0,
        addresses: Vec::new(),
        topics: Vec::new(),
        values,
        canonical_key: Some(canonical_key),
    }
}

fn tron_selector_plan(contracts: &[String], events: &[String]) -> PlannedSelector {
    let canonical_key = tron_canonical_key(contracts, events);
    let mut values = contracts.to_vec();
    values.extend(events.iter().cloned());
    PlannedSelector {
        kind: "tron_events".to_owned(),
        fingerprint: Some(format!("tron-events/{}", digest_prefix(&canonical_key, 12))),
        value_count: Some(values.len()),
        address_count: 0,
        topic_count: 0,
        addresses: contracts.to_vec(),
        topics: events.to_vec(),
        values,
        canonical_key: Some(canonical_key),
    }
}

fn tron_canonical_key(contracts: &[String], events: &[String]) -> String {
    let mut contracts = contracts
        .iter()
        .map(|address| {
            let address = address.trim();
            let hex = address.strip_prefix("0x").unwrap_or(address);
            if hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                format!("41{}", hex.to_ascii_lowercase())
            } else {
                hex.to_ascii_lowercase()
            }
        })
        .collect::<Vec<_>>();
    contracts.sort();
    contracts.dedup();
    let mut events = events.to_vec();
    events.sort();
    events.dedup();
    format!(
        "contracts/{}/events/{}",
        contracts.join("+"),
        if events.is_empty() {
            "all".to_owned()
        } else {
            events.join("+")
        }
    )
}

fn digest_prefix(value: &str, len: usize) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .take(len)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

fn chunk_ranges(from: u64, to: u64, chunk_blocks: u64) -> Vec<PlannedRange> {
    let mut ranges = Vec::new();
    let mut start = from;
    while start <= to {
        let end = start.saturating_add(chunk_blocks.saturating_sub(1)).min(to);
        ranges.push(PlannedRange {
            kind: "block".to_owned(),
            start,
            end,
        });
        if end == u64::MAX {
            break;
        }
        start = end + 1;
    }
    ranges
}
