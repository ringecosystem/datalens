use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    EvmLogFilter, LedgerRange, LedgerRangeKind, LogFilter, QueryRows,
};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Adapter-declared contract for one chain. Capabilities describe what the
/// query planner may ask for; they do not grant application access and they do
/// not imply that durable storage already contains coverage.
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
/// Dataset-level provider limits and selector support advertised to planners,
/// indexers, and warmup. Range and selector caps are provider limits, not
/// durable storage policy; callers must split requests before invoking fetch.
pub struct DatasetCapability {
    dataset: DatasetKey,
    selectors: Vec<SelectorKind>,
    ranges: Vec<HeightRangeKind>,
    max_range_len: Option<u64>,
    max_addresses_per_query: Option<usize>,
    max_topics_per_query: Option<usize>,
    supports_empty_coverage: bool,
    supports_safe_height: bool,
    supports_finalized_height: bool,
    supports_provider_native_finality_tags: bool,
    supports_range_split: bool,
    supports_reorg_signals: bool,
    supports_canonical_block_lookup: bool,
    supports_latest_reorg_signal: bool,
}

impl DatasetCapability {
    pub fn new(dataset: impl Into<DatasetKey>) -> Self {
        Self {
            dataset: dataset.into(),
            selectors: Vec::new(),
            ranges: Vec::new(),
            max_range_len: None,
            max_addresses_per_query: None,
            max_topics_per_query: None,
            supports_empty_coverage: false,
            supports_safe_height: false,
            supports_finalized_height: false,
            supports_provider_native_finality_tags: false,
            supports_range_split: false,
            supports_reorg_signals: false,
            supports_canonical_block_lookup: false,
            supports_latest_reorg_signal: false,
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

    pub fn with_max_range_len(mut self, max_range_len: u64) -> Self {
        self.max_range_len = Some(max_range_len);
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

    pub fn with_reorg_signals(mut self, supports_reorg_signals: bool) -> Self {
        self.supports_reorg_signals = supports_reorg_signals;
        self.supports_canonical_block_lookup = supports_reorg_signals;
        self.supports_latest_reorg_signal = supports_reorg_signals;
        self
    }

    pub fn with_canonical_block_lookup(mut self, supports_canonical_block_lookup: bool) -> Self {
        self.supports_canonical_block_lookup = supports_canonical_block_lookup;
        self
    }

    pub fn with_latest_reorg_signal(mut self, supports_latest_reorg_signal: bool) -> Self {
        self.supports_latest_reorg_signal = supports_latest_reorg_signal;
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

    pub fn max_range_len(&self) -> Option<u64> {
        self.max_range_len
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

    pub fn supports_reorg_signals(&self) -> bool {
        self.supports_reorg_signals
    }

    pub fn supports_canonical_block_lookup(&self) -> bool {
        self.supports_canonical_block_lookup
    }

    pub fn supports_latest_reorg_signal(&self) -> bool {
        self.supports_latest_reorg_signal
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
/// Selector whose fingerprint is safe for storage paths and whose canonical key
/// is stable across semantically equivalent filters. Adapters provide
/// chain-specific selector kinds while the planner treats all selectors through
/// this common contract.
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
/// Query-driven provider fetch request. The adapter owns provider IO only; it
/// must not apply application authorization, durable storage writes, or
/// business-specific indexing rules.
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
/// Per-fetch execution context supplied by higher layers. `cache_write` tells
/// adapters whether the caller intends durable persistence, but adapters still
/// must return data only for the requested range and selector.
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
/// Chain height with the finality level that made it observable. Only safe and
/// finalized heights may authorize durable cache writes or manifest coverage.
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReorgSignal {
    pub range_kind: HeightRangeKind,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: Option<u64>,
}

impl ReorgSignal {
    pub fn block(
        height: u64,
        hash: impl Into<String>,
        parent_hash: impl Into<String>,
        timestamp: Option<u64>,
    ) -> Self {
        Self {
            range_kind: HeightRangeKind::Block,
            height,
            hash: hash.into(),
            parent_hash: parent_hash.into(),
            timestamp,
        }
    }

    pub fn slot(
        height: u64,
        hash: impl Into<String>,
        parent_hash: impl Into<String>,
        timestamp: Option<u64>,
    ) -> Self {
        Self {
            range_kind: HeightRangeKind::Slot,
            height,
            hash: hash.into(),
            parent_hash: parent_hash.into(),
            timestamp,
        }
    }
}

impl ChainHeight {
    pub fn block(value: u64) -> Self {
        Self {
            range_kind: HeightRangeKind::Block,
            value,
            finality: FinalityLevel::Latest,
        }
    }

    pub fn slot(value: u64) -> Self {
        Self {
            range_kind: HeightRangeKind::Slot,
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
/// Provider response for an exact fetch request. Coverage selector and row
/// dataset must match the request so durable cache keys cannot be polluted by a
/// broader or differently normalized provider result.
pub struct ChainFetchResponse {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: HeightRange,
    pub rows: DatasetRows,
    pub coverage_selector: DatasetSelector,
    pub source_metadata: SourceMetadata,
    pub provider_diagnostics: ProviderDiagnostics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBlockRequest {
    pub chain: ChainIdentity,
    pub range_kind: HeightRangeKind,
    pub height: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBlock {
    pub chain: ChainIdentity,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    pub finality: FinalityLevel,
}

impl ChainFetchResponse {
    pub fn try_new(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
        rows: QueryRows,
    ) -> Result<Self, DatalensError> {
        let rows = DatasetRows::new(dataset_key.clone(), rows)?;
        Ok(Self {
            chain,
            dataset_key,
            range,
            rows,
            coverage_selector,
            source_metadata: SourceMetadata::default(),
            provider_diagnostics: ProviderDiagnostics::default(),
        })
    }

    pub fn try_empty(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
    ) -> Result<Self, DatalensError> {
        let rows = match dataset_key.evm_dataset() {
            Some(Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
            Some(Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
            Some(Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
            Some(Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
            None => QueryRows::AdapterJson {
                dataset_key: dataset_key.clone(),
                rows: Vec::new(),
            },
        };
        Self::try_new(chain, dataset_key, range, coverage_selector, rows)
    }

    pub fn expect_new(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
        rows: QueryRows,
    ) -> Self {
        Self::try_new(chain, dataset_key, range, coverage_selector, rows)
            .expect("matching dataset rows")
    }

    pub fn expect_empty(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        range: HeightRange,
        coverage_selector: DatasetSelector,
    ) -> Self {
        Self::try_empty(chain, dataset_key, range, coverage_selector)
            .expect("matching empty dataset rows")
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
    // Durable storage authority comes from the adapter's safe/finalized boundary,
    // not from a request's desired range or a provider's latest height.
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
    /// Return planner-visible capabilities for this adapter's configured chain.
    /// EVM, Solana, and Tron all expose the same query contract even though
    /// their selectors and height domains are chain-specific.
    fn capabilities(&self) -> AdapterCapabilities;

    /// Return the provider's latest observable height. Latest data is suitable
    /// for hot read-through, but by itself cannot authorize durable writes.
    fn latest_height(&self) -> Result<ChainHeight, DatalensError>;

    /// Return the highest height that may be written to durable cache. The
    /// returned finality must be safe or finalized and must match the range kind
    /// accepted by this adapter.
    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError>;

    /// Return an explicitly finalized height when the provider exposes one.
    /// Adapters that only expose a cache-safe boundary leave this unsupported.
    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not expose finalized height",
        ))
    }

    fn canonical_block(
        &self,
        _request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not expose canonical block lookup",
        ))
    }

    fn reorg_signal(
        &self,
        _range_kind: HeightRangeKind,
        _height: u64,
    ) -> Result<ReorgSignal, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not expose reorg signals",
        ))
    }

    fn latest_reorg_signal(&self) -> Result<ReorgSignal, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "adapter does not expose latest reorg signal",
        ))
    }

    /// Fetch rows for exactly the requested dataset, selector, and range. The
    /// response must include diagnostics even for empty coverage so callers can
    /// distinguish a real provider query from an accidental empty value.
    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError>;
}
