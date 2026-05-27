//! Query planning boundary for native datalens requests.

use datalens_chain::{
    AdapterCapabilities, ChainHeight, DatasetSelector, HeightRangeKind, SelectorKind,
    validate_durable_range,
};
use datalens_core::{
    ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, Dataset, DatasetId, DatasetKey,
    LedgerRange, QueryRequest, TimeRange,
};

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

impl NativeQueryInput {
    pub fn from_evm_query(request: QueryRequest) -> Result<Self, DatalensError> {
        let selector = match request.dataset {
            Dataset::Blocks => DatasetSelector::all(),
            Dataset::Logs => {
                let filter = request.filter.ok_or_else(|| {
                    DatalensError::new(DatalensErrorKind::InvalidInput, "logs require filter")
                })?;
                DatasetSelector::try_evm_logs(filter)?
            }
        };
        let response_shape = match request.dataset {
            Dataset::Blocks => ResponseShape::LegacyEvmBlocks,
            Dataset::Logs => ResponseShape::LegacyEvmLogs,
        };

        Ok(Self {
            chain: request.chain,
            dataset_key: DatasetKey::from(request.dataset),
            ledger_range: LedgerRange::from_block_range(request.range),
            selector,
            response_shape,
            field_selection: FieldSelection::All,
        })
    }
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
            .max_range_blocks()
            .unwrap_or(u64::MAX)
            .min(self.config.default_chunk_range_len)
            .max(1);

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
