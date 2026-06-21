use datalens_chain::{
    AdapterCapabilities, ChainHeight, DatasetSelector, HeightRangeKind, SelectorKind,
    validate_durable_range,
};
use datalens_core::{
    ChainIdentity, CoverageLevel, DatalensError, DatalensErrorKind, DatasetId, DatasetKey,
    LedgerRange, QueryDataFinality, QueryFinalityRequirement, QuerySegmentMetadata,
    QuerySegmentSource, TimeRange, missing_ranges,
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
    pub field_selection: FieldSelection,
    pub finality: QueryFinalityRequirement,
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
    DurableCache {
        boundary: ChainHeight,
    },
    HotReadThrough {
        latest: ChainHeight,
    },
    MixedReadThrough {
        durable_boundary: ChainHeight,
        latest: ChainHeight,
    },
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
    pub durable_hit_ranges: Vec<LedgerRange>,
    pub hot_hit_ranges: Vec<LedgerRange>,
    pub provider_fill_ranges: Vec<LedgerRange>,
    pub promotion_pending_ranges: Vec<LedgerRange>,
    pub segments: Vec<QuerySegmentMetadata>,
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
    pub field_selection: FieldSelection,
    pub requested_finality: QueryFinalityRequirement,
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
        self.plan_with_live_coverage(
            input,
            capabilities,
            durable_boundary.clone(),
            durable_boundary,
            covered_ranges,
        )
    }

    pub fn plan_with_live_coverage(
        &self,
        input: NativeQueryInput,
        capabilities: &AdapterCapabilities,
        durable_boundary: ChainHeight,
        latest: ChainHeight,
        covered_ranges: Vec<LedgerRange>,
    ) -> Result<NativeQueryPlan, DatalensError> {
        if input.ledger_range.len() > u128::from(self.config.max_query_range_len) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "query range exceeds planner.max_query_range_blocks",
            ));
        }
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

        if matches!(input.finality, QueryFinalityRequirement::LatestOnly) {
            validate_hot_range(&input.ledger_range, &latest)?;
            let ledger_range = input.ledger_range.clone();
            let fetch_tasks = input
                .ledger_range
                .split(max_len)?
                .into_iter()
                .map(|range| DurableFetchTask {
                    range,
                    cache_write: false,
                })
                .collect::<Vec<_>>();

            return Ok(NativeQueryPlan {
                chain: input.chain,
                dataset_key: input.dataset_key.clone(),
                ledger_range: input.ledger_range.clone(),
                selector: input.selector,
                field_selection: input.field_selection,
                requested_finality: input.finality,
                range_split: RangeSplitStrategy::MaxLedgerSpan {
                    max_len,
                    supports_adapter_split: dataset_capability.supports_range_split(),
                },
                capability_requirements: vec![AdapterCapabilityRequirement {
                    dataset_key: input.dataset_key,
                    selector: selector_kind,
                    range_kind,
                }],
                finality_policy: FinalityPolicy::HotReadThrough { latest },
                coverage: CoverageSummary {
                    status: QueryPlanStatus::Miss,
                    hit_ranges: Vec::new(),
                    missing_ranges: vec![ledger_range],
                    durable_hit_ranges: Vec::new(),
                    hot_hit_ranges: Vec::new(),
                    provider_fill_ranges: Vec::new(),
                    promotion_pending_ranges: Vec::new(),
                    segments: fetch_tasks
                        .iter()
                        .cloned()
                        .map(|task| {
                            QuerySegmentMetadata::new(
                                task.range,
                                QuerySegmentSource::Provider,
                                QueryDataFinality::Latest,
                            )
                        })
                        .collect(),
                },
                read_segments: Vec::new(),
                fetch_tasks,
            });
        }

        if matches!(input.finality, QueryFinalityRequirement::SafeToLatest) {
            validate_hot_range(&input.ledger_range, &latest)?;
            validate_durable_boundary(&input.ledger_range, &durable_boundary)?;

            let durable_range = capped_range(
                &input.ledger_range,
                input.ledger_range.start(),
                durable_boundary.value,
            )?;
            let hot_range = if input.ledger_range.end() > durable_boundary.value {
                capped_range(
                    &input.ledger_range,
                    input
                        .ledger_range
                        .start()
                        .max(durable_boundary.value.saturating_add(1)),
                    input.ledger_range.end(),
                )?
            } else {
                None
            };

            let hit_ranges = durable_range
                .as_ref()
                .map(|durable_range| {
                    covered_ranges
                        .into_iter()
                        .filter_map(|range| range.intersection(durable_range))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let durable_miss_ranges = durable_range
                .clone()
                .map(|durable_range| missing_ranges(durable_range, &hit_ranges))
                .unwrap_or_default();
            let hot_ranges = hot_range.into_iter().collect::<Vec<_>>();
            let miss_ranges = durable_miss_ranges
                .iter()
                .cloned()
                .chain(hot_ranges.iter().cloned())
                .collect::<Vec<_>>();
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
            let durable_fetch_tasks = durable_miss_ranges
                .iter()
                .map(|range| range.split(max_len))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .map(|range| DurableFetchTask {
                    range,
                    cache_write: true,
                });
            let hot_fetch_tasks = hot_ranges
                .iter()
                .map(|range| range.split(max_len))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .map(|range| DurableFetchTask {
                    range,
                    cache_write: false,
                });
            let fetch_tasks = durable_fetch_tasks
                .chain(hot_fetch_tasks)
                .collect::<Vec<_>>();

            let durable_finality = durable_query_finality(&durable_boundary);
            let mut segments = hit_ranges
                .iter()
                .cloned()
                .map(|range| {
                    QuerySegmentMetadata::new(range, QuerySegmentSource::Durable, durable_finality)
                })
                .collect::<Vec<_>>();
            segments.extend(fetch_tasks.iter().cloned().map(|task| {
                QuerySegmentMetadata::new(
                    task.range,
                    QuerySegmentSource::Provider,
                    if task.cache_write {
                        durable_finality
                    } else {
                        QueryDataFinality::Latest
                    },
                )
            }));

            return Ok(NativeQueryPlan {
                chain: input.chain,
                dataset_key: input.dataset_key.clone(),
                ledger_range: input.ledger_range,
                selector: input.selector,
                field_selection: input.field_selection,
                requested_finality: input.finality,
                range_split: RangeSplitStrategy::MaxLedgerSpan {
                    max_len,
                    supports_adapter_split: dataset_capability.supports_range_split(),
                },
                capability_requirements: vec![AdapterCapabilityRequirement {
                    dataset_key: input.dataset_key,
                    selector: selector_kind,
                    range_kind,
                }],
                finality_policy: FinalityPolicy::MixedReadThrough {
                    durable_boundary,
                    latest,
                },
                coverage: CoverageSummary {
                    status,
                    hit_ranges: hit_ranges.clone(),
                    missing_ranges: miss_ranges,
                    durable_hit_ranges: hit_ranges,
                    hot_hit_ranges: Vec::new(),
                    provider_fill_ranges: Vec::new(),
                    promotion_pending_ranges: Vec::new(),
                    segments,
                },
                read_segments,
                fetch_tasks,
            });
        }

        validate_durable_range(&input.ledger_range, &durable_boundary)?;

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
        let fetch_tasks: Vec<DurableFetchTask> = miss_ranges
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

        let finality = durable_query_finality(&durable_boundary);
        let mut segments = hit_ranges
            .iter()
            .cloned()
            .map(|range| QuerySegmentMetadata::new(range, QuerySegmentSource::Durable, finality))
            .collect::<Vec<_>>();
        segments.extend(fetch_tasks.iter().cloned().map(|task| {
            QuerySegmentMetadata::new(task.range, QuerySegmentSource::Provider, finality)
        }));

        Ok(NativeQueryPlan {
            chain: input.chain,
            dataset_key: input.dataset_key.clone(),
            ledger_range: input.ledger_range,
            selector: input.selector,
            field_selection: input.field_selection,
            requested_finality: input.finality,
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
                hit_ranges: hit_ranges.clone(),
                missing_ranges: miss_ranges.clone(),
                durable_hit_ranges: hit_ranges,
                hot_hit_ranges: Vec::new(),
                provider_fill_ranges: Vec::new(),
                promotion_pending_ranges: Vec::new(),
                segments,
            },
            read_segments,
            fetch_tasks,
        })
    }
}

fn capped_range(
    range: &LedgerRange,
    start: u64,
    end: u64,
) -> Result<Option<LedgerRange>, DatalensError> {
    if start > range.end() || end < range.start() || start > end {
        return Ok(None);
    }
    LedgerRange::try_new(range.kind(), start.max(range.start()), end.min(range.end())).map(Some)
}

fn validate_durable_boundary(
    range: &LedgerRange,
    durable_boundary: &ChainHeight,
) -> Result<(), DatalensError> {
    if range.kind() != durable_boundary.range_kind {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "range kind does not match adapter safe/finalized height kind",
        ));
    }
    durable_boundary.validate_durable_cache_safe()
}

fn validate_hot_range(
    range: &LedgerRange,
    latest_height: &ChainHeight,
) -> Result<(), DatalensError> {
    if range.kind() != latest_height.range_kind {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "range kind does not match adapter latest height kind",
        ));
    }
    if range.end() > latest_height.value {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!(
                "range exceeds adapter latest height: requested end {}, latest height {}",
                range.end(),
                latest_height.value
            ),
        ));
    }
    Ok(())
}

fn durable_query_finality(boundary: &ChainHeight) -> QueryDataFinality {
    match boundary.finality {
        datalens_chain::FinalityLevel::Finalized => QueryDataFinality::Finalized,
        _ => QueryDataFinality::Safe,
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

#[cfg(test)]
mod tests {
    use super::*;
    use datalens_chain::{DatasetCapability, FinalityLevel};
    use datalens_core::{ChainFamily, LogFilter, NetworkId};

    fn test_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
    }

    fn evm_logs_capabilities(chain: &ChainIdentity) -> AdapterCapabilities {
        AdapterCapabilities::new(chain.clone()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_logs())
                .with_selector(SelectorKind::EvmLogs)
                .with_range(HeightRangeKind::Block)
                .with_safe_height(true)
                .with_max_range_len(100),
        )
    }

    fn evm_logs_input(chain: &ChainIdentity, range: LedgerRange) -> NativeQueryInput {
        NativeQueryInput {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_logs(),
            ledger_range: range,
            selector: DatasetSelector::try_evm_logs(LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .expect("valid selector"),
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        }
    }

    #[test]
    fn test_plan_with_coverage_reports_full_hit_only_when_coverage_spans_query() {
        let chain = test_chain();
        let planner = NativePlanner::new(NativePlannerConfig {
            max_query_range_len: 100,
            default_chunk_range_len: 10,
        });
        let capabilities = evm_logs_capabilities(&chain);
        let boundary = ChainHeight::block(20).with_finality(FinalityLevel::Safe);
        let query_range = LedgerRange::blocks(10, 12).expect("valid range");

        let full_hit = planner
            .plan_with_coverage(
                evm_logs_input(&chain, query_range.clone()),
                &capabilities,
                boundary.clone(),
                vec![query_range.clone()],
            )
            .expect("full hit plan");
        assert_eq!(full_hit.coverage.status, QueryPlanStatus::FullHit);
        assert_eq!(full_hit.read_segments.len(), 1);
        assert!(full_hit.fetch_tasks.is_empty());

        let partial_hit = planner
            .plan_with_coverage(
                evm_logs_input(&chain, query_range),
                &capabilities,
                boundary,
                vec![LedgerRange::blocks(10, 11).expect("valid range")],
            )
            .expect("partial hit plan");
        assert_eq!(partial_hit.coverage.status, QueryPlanStatus::PartialHit);
        assert_eq!(
            partial_hit.coverage.missing_ranges,
            vec![LedgerRange::blocks(12, 12).expect("valid range")]
        );
    }
}
