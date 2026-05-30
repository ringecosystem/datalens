use serde::Serialize;

use crate::{DatalensIndexConfig, IndexerError, SourceConfig};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexPlan {
    application: String,
    index: String,
    tasks: Vec<PlannedIndexTask>,
}

impl IndexPlan {
    pub fn empty(application: impl Into<String>) -> Self {
        Self {
            application: application.into(),
            index: String::new(),
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
    pub address_count: usize,
    pub topic_count: usize,
    #[serde(skip_serializing)]
    pub addresses: Vec<String>,
    #[serde(skip_serializing)]
    pub topics: Vec<String>,
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
            tasks,
        })
    }
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
                        dataset: config.index.dataset.as_str().to_owned(),
                        range,
                        selector: PlannedSelector {
                            kind: "evm_logs".to_owned(),
                            address_count: evm_source.addresses.len(),
                            topic_count: evm_source.topics.len(),
                            addresses: evm_source.addresses.clone(),
                            topics: evm_source.topics.clone(),
                        },
                        finality: "durable".to_owned(),
                    })
                    .collect(),
            )
        }
    }
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
