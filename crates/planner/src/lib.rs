//! Query planning boundary for native datalens requests.

use datalens_chain::{
    AdapterCapabilities, ChainHeight, DatasetSelector, HeightRangeKind, SelectorKind,
    validate_durable_range,
};
use datalens_core::{
    ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, DatasetId, DatasetKey,
    LedgerRange, TimeRange,
};
use datalens_storage::missing_ranges;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

impl PlanRequest {
    pub fn new(chain: ChainIdentity, dataset: DatasetId, range: TimeRange) -> Self {
        Self {
            chain,
            dataset,
            range,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanStatus {
    Covered,
    Partial,
    Missing,
}

impl PlanStatus {
    pub fn coverage_level(&self) -> CoverageLevel {
        match self {
            Self::Covered => CoverageLevel::Covered,
            Self::Partial => CoverageLevel::Partial,
            Self::Missing => CoverageLevel::Missing,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanOutput {
    pub status: PlanStatus,
    pub required_datasets: Vec<DatasetId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativePlannerConfig {
    pub max_query_range_len: u64,
    pub default_chunk_range_len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryInput {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub ledger_range: LedgerRange,
    pub selector: DatasetSelector,
    pub response_shape: ResponseShape,
    pub field_selection: FieldSelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResponseShape {
    LegacyEvmBlocks,
    LegacyEvmLogs,
    NativeRows,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldSelection {
    All,
    Include(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RangeSplitStrategy {
    MaxLedgerSpan {
        max_len: u64,
        supports_adapter_split: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCapabilityRequirement {
    pub dataset_key: DatasetKey,
    pub selector: SelectorKind,
    pub range_kind: HeightRangeKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalityPolicy {
    DurableCache { boundary: ChainHeight },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryPlanStatus {
    FullHit,
    PartialHit,
    Miss,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageSummary {
    pub status: QueryPlanStatus,
    pub hit_ranges: Vec<LedgerRange>,
    pub missing_ranges: Vec<LedgerRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableReadSegment {
    pub range: LedgerRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableFetchTask {
    pub range: LedgerRange,
    pub cache_write: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryPlan {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub ledger_range: LedgerRange,
    pub selector: DatasetSelector,
    pub response_shape: ResponseShape,
    pub field_selection: FieldSelection,
    pub range_split: RangeSplitStrategy,
    pub capability_requirements: Vec<AdapterCapabilityRequirement>,
    pub finality_policy: FinalityPolicy,
    pub coverage: CoverageSummary,
    pub read_segments: Vec<DurableReadSegment>,
    pub fetch_tasks: Vec<DurableFetchTask>,
}

impl NativeQueryPlan {
    pub fn split_ranges(
        &self,
        ranges: Vec<LedgerRange>,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        match self.range_split {
            RangeSplitStrategy::MaxLedgerSpan { max_len, .. } => ranges
                .into_iter()
                .map(|range| range.split(max_len))
                .collect::<Result<Vec<_>, _>>()
                .map(|ranges| ranges.into_iter().flatten().collect()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativePlanner {
    config: NativePlannerConfig,
}

impl NativePlanner {
    pub fn new(config: NativePlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(
        &self,
        input: NativeQueryInput,
        capabilities: &AdapterCapabilities,
        durable_boundary: ChainHeight,
    ) -> Result<NativeQueryPlan, DatalensError> {
        self.plan_with_coverage(input, capabilities, durable_boundary, Vec::new())
    }

    pub fn plan_with_coverage(
        &self,
        input: NativeQueryInput,
        capabilities: &AdapterCapabilities,
        durable_boundary: ChainHeight,
        covered_ranges: Vec<LedgerRange>,
    ) -> Result<NativeQueryPlan, DatalensError> {
        if input.ledger_range.len() > u128::from(self.config.max_query_range_len) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "query range exceeds planner.max_query_range_blocks",
            ));
        }
        validate_durable_range(&input.ledger_range, &durable_boundary)?;

        if capabilities.chain() != &input.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "chain is not supported by adapter",
            ));
        }
        let dataset_capability = capabilities.dataset(&input.dataset_key).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "dataset is not supported by adapter",
            )
        })?;
        let selector_kind = input.selector.kind();
        if !dataset_capability.supports_selector(selector_kind.clone()) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "selector is not supported by adapter",
            ));
        }
        let range_kind = input.ledger_range.kind();
        if !dataset_capability.ranges().contains(&range_kind) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "ledger range kind is not supported by adapter",
            ));
        }
        if !dataset_capability.supports_safe_height()
            && !dataset_capability.supports_finalized_height()
        {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "adapter dataset does not expose safe or finalized height for durable cache",
            ));
        }
        validate_selector_limits(&input.selector, dataset_capability)?;

        let max_len = dataset_capability
            .max_range_len()
            .unwrap_or(u64::MAX)
            .min(self.config.default_chunk_range_len)
            .max(1);

        let hit_ranges = covered_ranges
            .into_iter()
            .filter_map(|range| range.intersection(&input.ledger_range))
            .collect::<Vec<_>>();
        let miss_ranges = missing_ranges(input.ledger_range.clone(), &hit_ranges);
        let status = match (hit_ranges.is_empty(), miss_ranges.is_empty()) {
            (_, true) => QueryPlanStatus::FullHit,
            (true, false) => QueryPlanStatus::Miss,
            (false, false) => QueryPlanStatus::PartialHit,
        };
        let read_segments = hit_ranges
            .iter()
            .cloned()
            .map(|range| DurableReadSegment { range })
            .collect();
        let fetch_tasks = miss_ranges
            .iter()
            .map(|range| range.split(max_len))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .map(|range| DurableFetchTask {
                range,
                cache_write: true,
            })
            .collect();

        Ok(NativeQueryPlan {
            chain: input.chain,
            dataset_key: input.dataset_key.clone(),
            ledger_range: input.ledger_range,
            selector: input.selector,
            response_shape: input.response_shape,
            field_selection: input.field_selection,
            range_split: RangeSplitStrategy::MaxLedgerSpan {
                max_len,
                supports_adapter_split: dataset_capability.supports_range_split(),
            },
            capability_requirements: vec![AdapterCapabilityRequirement {
                dataset_key: input.dataset_key,
                selector: selector_kind,
                range_kind,
            }],
            finality_policy: FinalityPolicy::DurableCache {
                boundary: durable_boundary,
            },
            coverage: CoverageSummary {
                status,
                hit_ranges,
                missing_ranges: miss_ranges,
            },
            read_segments,
            fetch_tasks,
        })
    }
}

fn validate_selector_limits(
    selector: &DatasetSelector,
    capability: &datalens_chain::DatasetCapability,
) -> Result<(), DatalensError> {
    if let DatasetSelector::EvmLogs(filter) = selector {
        if let Some(max_addresses) = capability.max_addresses_per_query()
            && filter.addresses().len() > max_addresses
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "too many log addresses",
            ));
        }
        if let Some(max_topics) = capability.max_topics_per_query()
            && filter.topics().len() > max_topics
        {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "too many log topic slots",
            ));
        }
    }
    Ok(())
}
