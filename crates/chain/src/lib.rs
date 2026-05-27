//! Chain-neutral adapter boundary for datalens chain sources.

use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    EvmLogFilter, LedgerRange, LedgerRangeKind, LogFilter, QueryRows,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    chain: ChainIdentity,
    datasets: Vec<DatasetKey>,
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

    pub fn with_dataset(mut self, dataset: DatasetKey) -> Self {
        self.datasets.push(dataset);
        self
    }

    pub fn with_dataset_capability(mut self, capability: DatasetCapability) -> Self {
        self.datasets.push(capability.dataset.clone());
        self.dataset_capabilities.push(capability);
        self
    }

    pub fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub fn datasets(&self) -> &[DatasetKey] {
        &self.datasets
    }

    pub fn dataset(&self, dataset: &DatasetKey) -> Option<&DatasetCapability> {
        self.dataset_capabilities
            .iter()
            .find(|capability| &capability.dataset == dataset)
    }

    pub fn dataset_capabilities(&self) -> &[DatasetCapability] {
        &self.dataset_capabilities
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetCapability {
    dataset: DatasetKey,
    selectors: Vec<SelectorKind>,
    ranges: Vec<HeightRangeKind>,
    max_range_blocks: Option<u64>,
    max_addresses_per_query: Option<usize>,
    max_topics_per_query: Option<usize>,
    supports_empty_coverage: bool,
    supports_safe_height: bool,
    supports_finalized_height: bool,
    supports_provider_native_finality_tags: bool,
    supports_range_split: bool,
}

impl DatasetCapability {
    pub fn new(dataset: impl Into<DatasetKey>) -> Self {
        Self {
            dataset: dataset.into(),
            selectors: Vec::new(),
            ranges: Vec::new(),
            max_range_blocks: None,
            max_addresses_per_query: None,
            max_topics_per_query: None,
            supports_empty_coverage: false,
            supports_safe_height: false,
            supports_finalized_height: false,
            supports_provider_native_finality_tags: false,
            supports_range_split: false,
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

    pub fn with_max_addresses_per_query(mut self, max_addresses_per_query: usize) -> Self {
        self.max_addresses_per_query = Some(max_addresses_per_query);
        self
    }

    pub fn with_max_topics_per_query(mut self, max_topics_per_query: usize) -> Self {
        self.max_topics_per_query = Some(max_topics_per_query);
        self
    }

    pub fn with_empty_coverage(mut self, supports_empty_coverage: bool) -> Self {
        self.supports_empty_coverage = supports_empty_coverage;
        self
    }

    pub fn with_safe_height(mut self, supports_safe_height: bool) -> Self {
        self.supports_safe_height = supports_safe_height;
        self
    }

    pub fn with_finalized_height(mut self, supports_finalized_height: bool) -> Self {
        self.supports_finalized_height = supports_finalized_height;
        self
    }

    pub fn with_provider_native_finality_tags(
        mut self,
        supports_provider_native_finality_tags: bool,
    ) -> Self {
        self.supports_provider_native_finality_tags = supports_provider_native_finality_tags;
        self
    }

    pub fn with_range_split(mut self, supports_range_split: bool) -> Self {
        self.supports_range_split = supports_range_split;
        self
    }

    pub fn dataset(&self) -> &DatasetKey {
        &self.dataset
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

    pub fn max_addresses_per_query(&self) -> Option<usize> {
        self.max_addresses_per_query
    }

    pub fn max_topics_per_query(&self) -> Option<usize> {
        self.max_topics_per_query
    }

    pub fn supports_empty_coverage(&self) -> bool {
        self.supports_empty_coverage
    }

    pub fn supports_safe_height(&self) -> bool {
        self.supports_safe_height
    }

    pub fn supports_finalized_height(&self) -> bool {
        self.supports_finalized_height
    }

    pub fn supports_provider_native_finality_tags(&self) -> bool {
        self.supports_provider_native_finality_tags
    }

    pub fn supports_range_split(&self) -> bool {
        self.supports_range_split
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterKey(String);

impl AdapterKey {
    pub fn try_new(value: impl Into<String>) -> Result<Self, DatalensError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "adapter key must not be empty",
            ));
        }
        if value.contains('/') || value.contains('\\') {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "adapter key must not contain path separators",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectorKind {
    All,
    EvmLogs,
    Other(AdapterKey),
}

pub type HeightRangeKind = LedgerRangeKind;
pub type HeightRange = LedgerRange;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatasetSelector {
    All,
    EvmLogs(EvmLogFilter),
    Other {
        kind: AdapterKey,
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

    pub fn try_other(
        kind: AdapterKey,
        fingerprint: impl Into<String>,
        canonical_key: impl Into<String>,
    ) -> Result<Self, DatalensError> {
        let fingerprint = validate_storage_key("selector fingerprint", fingerprint.into())?;
        let canonical_key = validate_storage_key("selector canonical key", canonical_key.into())?;
        Ok(Self::Other {
            kind,
            fingerprint,
            canonical_key,
        })
    }

    pub fn kind(&self) -> SelectorKind {
        match self {
            Self::All => SelectorKind::All,
            Self::EvmLogs(_) => SelectorKind::EvmLogs,
            Self::Other { kind, .. } => SelectorKind::Other(kind.clone()),
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
    pub dataset_key: DatasetKey,
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
        dataset_key: DatasetKey,
        range: HeightRange,
        selector: DatasetSelector,
    ) -> Self {
        Self {
            chain,
            dataset_key,
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
pub enum FinalityLevel {
    Latest,
    Safe,
    Finalized,
    ChainSpecific(&'static str),
}

impl FinalityLevel {
    pub fn is_durable_writable(&self) -> bool {
        matches!(self, Self::Safe | Self::Finalized)
    }
}

pub type FinalityKind = FinalityLevel;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainHeight {
    pub range_kind: HeightRangeKind,
    pub value: u64,
    pub finality: FinalityLevel,
}

impl ChainHeight {
    pub fn block(value: u64) -> Self {
        Self {
            range_kind: HeightRangeKind::Block,
            value,
            finality: FinalityLevel::Latest,
        }
    }

    pub fn with_finality(mut self, finality: FinalityLevel) -> Self {
        self.finality = finality;
        self
    }

    pub fn is_durable_cache_safe(&self) -> bool {
        self.finality.is_durable_writable()
    }

    pub fn validate_durable_cache_safe(&self) -> Result<(), DatalensError> {
        self.validate_durable_writable()
    }

    pub fn validate_durable_writable(&self) -> Result<(), DatalensError> {
        if !self.finality.is_durable_writable() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "adapter durable boundary is not safe or finalized",
            ));
        }
        Ok(())
    }
}

fn validate_storage_key(kind: &str, value: String) -> Result<String, DatalensError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must not be empty"),
        ));
    }
    if value.contains('\\')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must be a relative storage key"),
        ));
    }
    Ok(value.to_owned())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainFetchResponse {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: HeightRange,
    pub rows: DatasetRows,
    pub coverage_selector: DatasetSelector,
    pub source_metadata: SourceMetadata,
    pub provider_diagnostics: ProviderDiagnostics,
}

impl ChainFetchResponse {
    pub fn new(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
        rows: QueryRows,
    ) -> Self {
        let rows = DatasetRows::new(dataset_key.clone(), rows).expect("matching dataset rows");
        Self {
            chain,
            dataset_key,
            range,
            rows,
            coverage_selector,
            source_metadata: SourceMetadata::default(),
            provider_diagnostics: ProviderDiagnostics::default(),
        }
    }

    pub fn empty(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
    ) -> Self {
        let rows = match dataset_key.legacy_dataset() {
            Some(Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
            Some(Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
            None => QueryRows::OtherJson(Vec::new()),
        };
        Self::new(chain, dataset_key, range, coverage_selector, rows)
    }

    pub fn with_source_metadata(mut self, source_metadata: SourceMetadata) -> Self {
        self.source_metadata = source_metadata;
        self
    }

    pub fn with_provider_diagnostics(mut self, provider_diagnostics: ProviderDiagnostics) -> Self {
        self.provider_diagnostics = provider_diagnostics;
        self
    }

    pub fn validate_for_request(&self, request: &ChainFetchRequest) -> Result<(), DatalensError> {
        if self.chain != request.chain
            || self.dataset_key != request.dataset_key
            || self.range != request.range
            || self.coverage_selector != request.selector
            || self.rows.dataset_key() != &request.dataset_key
        {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "chain adapter response does not match fetch request",
            ));
        }
        if self.rows.row_count() == 0 && self.provider_diagnostics.calls == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "empty chain adapter response must include provider diagnostics confirming a provider query",
            ));
        }
        Ok(())
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

pub fn validate_durable_range(
    range: &HeightRange,
    cache_safe_height: &ChainHeight,
) -> Result<(), DatalensError> {
    cache_safe_height.validate_durable_writable()?;
    if range.kind() != cache_safe_height.range_kind {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "range kind does not match adapter cache-safe height kind",
        ));
    }
    if range.end() > cache_safe_height.value {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!(
                "range exceeds adapter safe/finalized height: requested end {}, safe/finalized height {}",
                range.end(),
                cache_safe_height.value
            ),
        ));
    }
    Ok(())
}

pub trait ChainAdapter: Clone + Send + Sync + 'static {
    fn capabilities(&self) -> AdapterCapabilities;

    fn latest_height(&self) -> Result<ChainHeight, DatalensError>;

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError>;

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not expose finalized height",
        ))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError>;
}

#[cfg(test)]
mod tests {
    use datalens_core::{
        ChainFamily, ChainIdentity, DatalensError, Dataset, DatasetKey, DatasetRows, LedgerRange,
        LedgerRangeKind, LogFilter, NetworkId, QueryRows,
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
                    .with_max_addresses_per_query(100)
                    .with_max_topics_per_query(4)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_provider_native_finality_tags(true)
                    .with_range_split(true),
            )
        }

        fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(12))
        }

        fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(10).with_finality(FinalityKind::Safe))
        }

        fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
            Ok(ChainFetchResponse::empty(
                request.chain,
                request.dataset_key,
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
            .dataset(&DatasetKey::evm_logs())
            .expect("logs capability");
        assert!(logs.supports_selector(SelectorKind::EvmLogs));
        assert_eq!(logs.max_range_blocks(), Some(2));
        assert_eq!(logs.max_topics_per_query(), Some(4));
        assert!(logs.supports_empty_coverage());
        assert!(logs.supports_safe_height());
        assert!(logs.supports_finalized_height());
        assert!(logs.supports_provider_native_finality_tags());
        assert!(logs.supports_range_split());

        let request = ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::blocks(10, 11).expect("valid range"),
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
        let response = adapter.fetch(request.clone()).expect("fetch response");

        assert_eq!(adapter.latest_height().unwrap(), ChainHeight::block(12));
        assert_eq!(
            adapter.cache_safe_height().unwrap(),
            ChainHeight::block(10).with_finality(FinalityKind::Safe)
        );
        response
            .validate_for_request(&request)
            .expect("response matches request");
        assert_eq!(response.dataset_key, DatasetKey::evm_logs());
        assert_eq!(
            response.range,
            LedgerRange::blocks(10, 11).expect("valid range")
        );
        assert_eq!(
            response.rows,
            DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new())).unwrap()
        );
        assert_eq!(response.coverage_selector.kind(), SelectorKind::EvmLogs);
        assert_eq!(response.source_metadata.provider, "mock");
        assert_eq!(response.provider_diagnostics.calls, 1);
    }

    #[test]
    fn test_fetch_response_validate_for_request_rejects_contract_mismatch() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .expect("valid chain");
        let selector = DatasetSelector::all();
        let request = ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_blocks(),
            LedgerRange::blocks(1, 2).expect("valid range"),
            selector.clone(),
        );
        let response = ChainFetchResponse::new(
            chain,
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 2).expect("valid range"),
            selector,
            QueryRows::EvmLogs(Vec::new()),
        );

        let error = response
            .validate_for_request(&request)
            .expect_err("dataset mismatch rejected");

        assert_eq!(error.kind, DatalensErrorKind::Internal);
    }

    #[test]
    fn test_fetch_response_validate_for_request_rejects_unconfirmed_empty_response() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .expect("valid chain");
        let request = ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 2).expect("valid range"),
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .expect("valid selector"),
        );
        let response = ChainFetchResponse::empty(
            chain,
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 2).expect("valid range"),
            request.selector.clone(),
        );

        let error = response
            .validate_for_request(&request)
            .expect_err("unconfirmed empty response rejected");

        assert_eq!(error.kind, DatalensErrorKind::Internal);
    }

    #[test]
    fn test_durable_range_requires_safe_or_finalized_matching_height_kind() {
        let range = LedgerRange::blocks(1, 10).expect("valid range");
        assert!(
            validate_durable_range(
                &range,
                &ChainHeight::block(10).with_finality(FinalityKind::Safe),
            )
            .is_ok()
        );
        assert!(
            validate_durable_range(
                &range,
                &ChainHeight::block(10).with_finality(FinalityKind::Finalized),
            )
            .is_ok()
        );

        let latest_error = validate_durable_range(&range, &ChainHeight::block(10))
            .expect_err("latest is not durable");
        assert_eq!(latest_error.kind, DatalensErrorKind::InvalidInput);

        let too_high_error = validate_durable_range(
            &range,
            &ChainHeight::block(9).with_finality(FinalityKind::Safe),
        )
        .expect_err("range above safe height is rejected");
        assert_eq!(too_high_error.kind, DatalensErrorKind::InvalidInput);

        let other_height = ChainHeight {
            range_kind: LedgerRangeKind::Slot,
            value: 10,
            finality: FinalityLevel::Safe,
        };
        let kind_error =
            validate_durable_range(&range, &other_height).expect_err("kind mismatch rejected");
        assert_eq!(kind_error.kind, DatalensErrorKind::InvalidInput);
    }

    #[test]
    fn test_other_finality_cannot_authorize_durable_cache_write() {
        let height =
            ChainHeight::block(10).with_finality(FinalityLevel::ChainSpecific("checkpoint"));

        assert!(!height.finality.is_durable_writable());
        assert!(height.validate_durable_writable().is_err());
    }

    #[test]
    fn test_other_selector_and_range_kinds_are_owned_stable_and_storage_safe() {
        let first = AdapterKey::try_new("solana-accounts").expect("valid key");
        let second = AdapterKey::try_new("solana-accounts").expect("valid key");

        assert_eq!(
            SelectorKind::Other(first.clone()),
            SelectorKind::Other(second.clone())
        );
        assert_eq!(
            HeightRangeKind::Other(first.as_str().to_owned()),
            HeightRangeKind::Other(second.as_str().to_owned())
        );
        assert_eq!(first.as_str(), "solana-accounts");
        assert!(AdapterKey::try_new("").is_err());
        assert!(AdapterKey::try_new("bad/key").is_err());
        assert!(AdapterKey::try_new("bad\\key").is_err());

        let selector = DatasetSelector::try_other(
            first.clone(),
            "accounts-fingerprint",
            "accounts/canonical-key",
        )
        .expect("valid selector");
        let range = HeightRange::try_new(HeightRangeKind::Other(first.as_str().to_owned()), 1, 2)
            .expect("valid range");

        assert_eq!(selector.kind(), SelectorKind::Other(second.clone()));
        assert_eq!(
            range.kind(),
            HeightRangeKind::Other(second.as_str().to_owned())
        );
        assert_eq!(selector.fingerprint(), "accounts-fingerprint");
        assert_eq!(selector.canonical_key(), "accounts/canonical-key");
        assert!(
            DatasetSelector::try_other(
                AdapterKey::try_new("bad-selector").expect("valid key"),
                "bad\\fingerprint",
                "canonical",
            )
            .is_err()
        );
        assert!(
            HeightRange::try_new(HeightRangeKind::Other("bad-range".to_owned()), 2, 1,).is_err()
        );
    }

    #[test]
    fn test_other_selector_rejects_dot_path_segments() {
        let kind = AdapterKey::try_new("other-selector").expect("valid key");

        for key in ["../x", "a/../b", ".", "a/./b"] {
            assert!(
                DatasetSelector::try_other(kind.clone(), key, "accounts/fingerprint").is_err(),
                "fingerprint {key:?} should be rejected"
            );
            assert!(
                DatasetSelector::try_other(kind.clone(), "accounts/fingerprint", key).is_err(),
                "canonical key {key:?} should be rejected"
            );
        }
    }
}
