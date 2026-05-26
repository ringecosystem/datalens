//! Chain-neutral adapter boundary for datalens chain sources.

use datalens_core::{
    BlockRange, ChainIdentity, DatalensError, Dataset, DatasetId, EvmLogFilter, LogFilter,
    QueryRows, ResultEnvelope, TimeRange,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    chain: ChainIdentity,
    datasets: Vec<DatasetId>,
    dataset_capabilities: Vec<DatasetCapability>,
}

impl AdapterCapabilities {
    pub fn new(chain: ChainIdentity) -> Self {
        Self {
            chain,
            datasets: Vec::new(),
            dataset_capabilities: Vec::new(),
        }
    }

    pub fn with_dataset(mut self, dataset: DatasetId) -> Self {
        self.datasets.push(dataset);
        self
    }

    pub fn with_dataset_capability(mut self, capability: DatasetCapability) -> Self {
        self.datasets
            .push(DatasetId::expect_new(capability.dataset.as_str()));
        self.dataset_capabilities.push(capability);
        self
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn datasets(&self) -> &[DatasetId] {
        &self.datasets
    }

    pub fn dataset(&self, dataset: Dataset) -> Option<&DatasetCapability> {
        self.dataset_capabilities
            .iter()
            .find(|capability| capability.dataset == dataset)
    }

    pub fn dataset_capabilities(&self) -> &[DatasetCapability] {
        &self.dataset_capabilities
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetCapability {
    dataset: Dataset,
    selectors: Vec<SelectorKind>,
    ranges: Vec<HeightRangeKind>,
    max_range_blocks: Option<u64>,
    max_batch_rows: Option<usize>,
    supports_empty_coverage: bool,
}

impl DatasetCapability {
    pub fn new(dataset: Dataset) -> Self {
        Self {
            dataset,
            selectors: Vec::new(),
            ranges: Vec::new(),
            max_range_blocks: None,
            max_batch_rows: None,
            supports_empty_coverage: false,
        }
    }

    pub fn with_selector(mut self, selector: SelectorKind) -> Self {
        self.selectors.push(selector);
        self
    }

    pub fn with_range(mut self, range: HeightRangeKind) -> Self {
        self.ranges.push(range);
        self
    }

    pub fn with_max_range_blocks(mut self, max_range_blocks: u64) -> Self {
        self.max_range_blocks = Some(max_range_blocks);
        self
    }

    pub fn with_max_batch_rows(mut self, max_batch_rows: usize) -> Self {
        self.max_batch_rows = Some(max_batch_rows);
        self
    }

    pub fn with_empty_coverage(mut self, supports_empty_coverage: bool) -> Self {
        self.supports_empty_coverage = supports_empty_coverage;
        self
    }

    pub fn dataset(&self) -> Dataset {
        self.dataset
    }

    pub fn selectors(&self) -> &[SelectorKind] {
        &self.selectors
    }

    pub fn supports_selector(&self, selector: SelectorKind) -> bool {
        self.selectors.contains(&selector)
    }

    pub fn ranges(&self) -> &[HeightRangeKind] {
        &self.ranges
    }

    pub fn max_range_blocks(&self) -> Option<u64> {
        self.max_range_blocks
    }

    pub fn max_batch_rows(&self) -> Option<usize> {
        self.max_batch_rows
    }

    pub fn supports_empty_coverage(&self) -> bool {
        self.supports_empty_coverage
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectorKind {
    All,
    EvmLogs,
    Other(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeightRangeKind {
    Block,
    Other(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatasetSelector {
    All,
    EvmLogs(EvmLogFilter),
    Other {
        kind: &'static str,
        fingerprint: String,
        canonical_key: String,
    },
}

impl DatasetSelector {
    pub fn all() -> Self {
        Self::All
    }

    pub fn try_evm_logs(filter: LogFilter) -> Result<Self, DatalensError> {
        Ok(Self::EvmLogs(EvmLogFilter::try_from(filter)?))
    }

    pub fn kind(&self) -> SelectorKind {
        match self {
            Self::All => SelectorKind::All,
            Self::EvmLogs(_) => SelectorKind::EvmLogs,
            Self::Other { kind, .. } => SelectorKind::Other(kind),
        }
    }

    pub fn fingerprint(&self) -> String {
        match self {
            Self::All => "all".to_owned(),
            Self::EvmLogs(filter) => format!("evm-logs/{}", filter.compact_key()),
            Self::Other { fingerprint, .. } => fingerprint.clone(),
        }
    }

    pub fn canonical_key(&self) -> String {
        match self {
            Self::All => "all".to_owned(),
            Self::EvmLogs(filter) => format!("evm-logs/{}", filter.canonical_key()),
            Self::Other { canonical_key, .. } => canonical_key.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainFetchRequest {
    pub chain: ChainIdentity,
    pub dataset: Dataset,
    pub range: HeightRange,
    pub selector: DatasetSelector,
    pub limit: Option<FetchLimit>,
    pub context: FetchContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchLimit {
    pub max_rows: Option<usize>,
}

impl FetchLimit {
    pub fn max_rows(max_rows: usize) -> Self {
        Self {
            max_rows: Some(max_rows),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FetchContext {
    pub request_id: Option<String>,
    pub cache_write: bool,
}

impl ChainFetchRequest {
    pub fn new(
        chain: ChainIdentity,
        dataset: Dataset,
        range: HeightRange,
        selector: DatasetSelector,
    ) -> Self {
        Self {
            chain,
            dataset,
            range,
            selector,
            limit: None,
            context: FetchContext::default(),
        }
    }

    pub fn with_limit(mut self, limit: FetchLimit) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_context(mut self, context: FetchContext) -> Self {
        self.context = context;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeightRange {
    Block(BlockRange),
    Other {
        kind: &'static str,
        start: u64,
        end: u64,
    },
}

impl HeightRange {
    pub fn blocks(range: BlockRange) -> Self {
        Self::Block(range)
    }

    pub fn kind(&self) -> HeightRangeKind {
        match self {
            Self::Block(_) => HeightRangeKind::Block,
            Self::Other { kind, .. } => HeightRangeKind::Other(kind),
        }
    }

    pub fn block_range(&self) -> Option<BlockRange> {
        match self {
            Self::Block(range) => Some(*range),
            Self::Other { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityKind {
    Latest,
    Safe,
    Finalized,
    Other(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainHeight {
    pub range_kind: HeightRangeKind,
    pub value: u64,
    pub finality: FinalityKind,
}

impl ChainHeight {
    pub fn block(value: u64) -> Self {
        Self {
            range_kind: HeightRangeKind::Block,
            value,
            finality: FinalityKind::Latest,
        }
    }

    pub fn with_finality(mut self, finality: FinalityKind) -> Self {
        self.finality = finality;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainFetchResponse {
    pub chain: ChainIdentity,
    pub dataset: Dataset,
    pub range: HeightRange,
    pub rows: QueryRows,
    pub coverage_selector: DatasetSelector,
    pub source_metadata: SourceMetadata,
    pub provider_diagnostics: ProviderDiagnostics,
}

impl ChainFetchResponse {
    pub fn new(
        chain: ChainIdentity,
        dataset: Dataset,
        range: HeightRange,
        coverage_selector: DatasetSelector,
        rows: QueryRows,
    ) -> Self {
        Self {
            chain,
            dataset,
            range,
            rows,
            coverage_selector,
            source_metadata: SourceMetadata::default(),
            provider_diagnostics: ProviderDiagnostics::default(),
        }
    }

    pub fn empty(
        chain: ChainIdentity,
        dataset: Dataset,
        range: HeightRange,
        coverage_selector: DatasetSelector,
    ) -> Self {
        let rows = match dataset {
            Dataset::Blocks => QueryRows::Blocks(Vec::new()),
            Dataset::Logs => QueryRows::Logs(Vec::new()),
        };
        Self::new(chain, dataset, range, coverage_selector, rows)
    }

    pub fn with_source_metadata(mut self, source_metadata: SourceMetadata) -> Self {
        self.source_metadata = source_metadata;
        self
    }

    pub fn with_provider_diagnostics(mut self, provider_diagnostics: ProviderDiagnostics) -> Self {
        self.provider_diagnostics = provider_diagnostics;
        self
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceMetadata {
    pub provider: String,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderDiagnostics {
    pub calls: usize,
    pub rows_scanned: usize,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResponse<T> {
    pub envelope: ResultEnvelope<T>,
}

pub trait ChainAdapter: Clone + Send + Sync + 'static {
    fn capabilities(&self) -> AdapterCapabilities;

    fn latest_height(&self) -> Result<ChainHeight, DatalensError>;

    fn safe_height(&self) -> Result<ChainHeight, DatalensError>;

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError>;
}

#[cfg(test)]
mod tests {
    use datalens_core::{
        BlockRange, ChainFamily, ChainIdentity, DatalensError, Dataset, LogFilter, NetworkId,
        QueryRows,
    };

    use super::*;

    #[derive(Clone)]
    struct EmptyAdapter {
        chain: ChainIdentity,
    }

    impl ChainAdapter for EmptyAdapter {
        fn capabilities(&self) -> AdapterCapabilities {
            AdapterCapabilities::new(self.chain.clone()).with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::EvmLogs)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_blocks(2)
                    .with_max_batch_rows(100)
                    .with_empty_coverage(true),
            )
        }

        fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(12))
        }

        fn safe_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(10).with_finality(FinalityKind::Safe))
        }

        fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
            Ok(ChainFetchResponse::empty(
                request.chain,
                request.dataset,
                request.range,
                request.selector,
            )
            .with_source_metadata(SourceMetadata {
                provider: "mock".to_owned(),
                request_id: Some("req-1".to_owned()),
            })
            .with_provider_diagnostics(ProviderDiagnostics {
                calls: 1,
                rows_scanned: 0,
                warnings: Vec::new(),
            }))
        }
    }

    #[test]
    fn test_dataset_selector_fingerprint_is_stable_and_storage_safe() {
        let first = DatasetSelector::try_evm_logs(LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![None],
        })
        .expect("valid selector");
        let second = DatasetSelector::try_evm_logs(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None],
        })
        .expect("valid selector");

        assert_eq!(first.kind(), SelectorKind::EvmLogs);
        assert_eq!(first.fingerprint(), second.fingerprint());
        assert!(first.fingerprint().starts_with("evm-logs/addr-topic-"));
        assert!(!first.fingerprint().contains("0xaaaaaaaa"));
        assert_ne!(first.canonical_key(), first.fingerprint());
    }

    #[test]
    fn test_fetch_request_response_and_capabilities_cover_query_cache_contract() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .expect("valid chain");
        let adapter = EmptyAdapter {
            chain: chain.clone(),
        };
        let capabilities = adapter.capabilities();

        assert_eq!(capabilities.chain(), &chain);
        let logs = capabilities
            .dataset(Dataset::Logs)
            .expect("logs capability");
        assert!(logs.supports_selector(SelectorKind::EvmLogs));
        assert_eq!(logs.max_range_blocks(), Some(2));
        assert!(logs.supports_empty_coverage());

        let request = ChainFetchRequest::new(
            chain.clone(),
            Dataset::Logs,
            HeightRange::blocks(BlockRange::expect_new(10, 11)),
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .expect("valid selector"),
        )
        .with_limit(FetchLimit::max_rows(100))
        .with_context(FetchContext {
            request_id: Some("query-1".to_owned()),
            cache_write: true,
        });
        let response = adapter.fetch(request).expect("fetch response");

        assert_eq!(adapter.latest_height().unwrap(), ChainHeight::block(12));
        assert_eq!(
            adapter.safe_height().unwrap(),
            ChainHeight::block(10).with_finality(FinalityKind::Safe)
        );
        assert_eq!(response.dataset, Dataset::Logs);
        assert_eq!(
            response.range,
            HeightRange::blocks(BlockRange::expect_new(10, 11))
        );
        assert_eq!(response.rows, QueryRows::Logs(Vec::new()));
        assert_eq!(response.coverage_selector.kind(), SelectorKind::EvmLogs);
        assert_eq!(response.source_metadata.provider, "mock");
        assert_eq!(response.provider_diagnostics.calls, 1);
    }
}
