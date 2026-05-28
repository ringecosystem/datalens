//! Prometheus-compatible metrics recording for datalens.

use datalens_core::{ChainIdentity, DatalensErrorKind, Dataset, DatasetKey};
use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};

const APPLICATION: &str = "unknown";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationIdentity {
    value: String,
}

impl ApplicationIdentity {
    pub fn named(value: impl Into<String>) -> Self {
        let value = value.into();
        let value = if value.trim().is_empty() {
            APPLICATION.to_owned()
        } else {
            value
        };
        Self { value }
    }

    pub fn unknown() -> Self {
        Self {
            value: APPLICATION.to_owned(),
        }
    }

    pub fn from_optional(value: Option<&str>) -> Self {
        value.map(Self::named).unwrap_or_else(Self::unknown)
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsLabels {
    application: ApplicationIdentity,
    chain: String,
    chain_kind: String,
    dataset: String,
}

impl MetricsLabels {
    pub fn new(application: ApplicationIdentity, chain: ChainIdentity, dataset: Dataset) -> Self {
        Self::from_dataset_key(application, chain, DatasetKey::from(dataset))
    }

    pub fn from_dataset_key(
        application: ApplicationIdentity,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
    ) -> Self {
        Self {
            application,
            chain: chain.configured_name().to_owned(),
            chain_kind: chain.family_ref().key().to_owned(),
            dataset: dataset_key
                .legacy_dataset()
                .map(|dataset| dataset.as_str().to_owned())
                .unwrap_or_else(|| dataset_key.as_str().to_owned()),
        }
    }

    pub fn label_values(&self) -> [&str; 4] {
        [
            self.application.as_str(),
            &self.chain,
            &self.chain_kind,
            &self.dataset,
        ]
    }

    fn query_label_values<'a>(&'a self, outcome: &'a str) -> [&'a str; 5] {
        [
            self.application.as_str(),
            &self.chain,
            &self.chain_kind,
            &self.dataset,
            outcome,
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorLabels {
    chain: String,
    chain_kind: String,
    dataset: String,
    error_kind: String,
}

impl ErrorLabels {
    pub fn from_labels(labels: &MetricsLabels, error_kind: DatalensErrorKind) -> Self {
        Self {
            chain: labels.chain.clone(),
            chain_kind: labels.chain_kind.clone(),
            dataset: labels.dataset.clone(),
            error_kind: error_kind_label(&error_kind).to_owned(),
        }
    }

    fn label_values(&self) -> [&str; 4] {
        [
            &self.chain,
            &self.chain_kind,
            &self.dataset,
            &self.error_kind,
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    Hit,
    Miss,
    PartialHit,
    Filled,
    Empty,
    Error,
}

impl QueryOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::PartialHit => "partial_hit",
            Self::Filled => "filled",
            Self::Empty => "empty",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheCoverageOutcome {
    Hit,
    Miss,
    PartialHit,
    Empty,
    Error,
}

impl CacheCoverageOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::PartialHit => "partial_hit",
            Self::Empty => "empty",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FillOutcome {
    Filled,
    Empty,
    Error,
}

impl FillOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::Empty => "empty",
            Self::Error => "error",
        }
    }
}

#[derive(Clone)]
pub struct MetricsRecorder {
    registry: Registry,
    query_total: CounterVec,
    query_duration_seconds: HistogramVec,
    cache_coverage_total: CounterVec,
    fill_total: CounterVec,
    fill_duration_seconds: HistogramVec,
    provider_error_total: CounterVec,
    storage_error_total: CounterVec,
    latest_requested_block: GaugeVec,
    latest_filled_block: GaugeVec,
}

impl MetricsRecorder {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let query_total = CounterVec::new(
            Opts::new("datalens_query_total", "Datalens queries by outcome."),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_query_duration_seconds",
                "Datalens query duration in seconds.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;
        let cache_coverage_total = CounterVec::new(
            Opts::new(
                "datalens_cache_coverage_total",
                "Datalens cache coverage checks by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let fill_total = CounterVec::new(
            Opts::new(
                "datalens_fill_total",
                "Datalens fill operations by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let fill_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_fill_duration_seconds",
                "Datalens fill duration in seconds.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;
        let provider_error_total = CounterVec::new(
            Opts::new(
                "datalens_provider_error_total",
                "Datalens provider errors by stable error kind.",
            ),
            &["chain", "chain_kind", "dataset", "error_kind"],
        )?;
        let storage_error_total = CounterVec::new(
            Opts::new(
                "datalens_storage_error_total",
                "Datalens storage errors by stable error kind.",
            ),
            &["chain", "chain_kind", "dataset", "error_kind"],
        )?;
        let latest_requested_block = GaugeVec::new(
            Opts::new(
                "datalens_application_chain_latest_requested_block",
                "Latest block requested by application, chain, and dataset.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;
        let latest_filled_block = GaugeVec::new(
            Opts::new(
                "datalens_application_chain_latest_filled_block",
                "Latest block filled by application, chain, and dataset.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;

        registry.register(Box::new(query_total.clone()))?;
        registry.register(Box::new(query_duration_seconds.clone()))?;
        registry.register(Box::new(cache_coverage_total.clone()))?;
        registry.register(Box::new(fill_total.clone()))?;
        registry.register(Box::new(fill_duration_seconds.clone()))?;
        registry.register(Box::new(provider_error_total.clone()))?;
        registry.register(Box::new(storage_error_total.clone()))?;
        registry.register(Box::new(latest_requested_block.clone()))?;
        registry.register(Box::new(latest_filled_block.clone()))?;

        Ok(Self {
            registry,
            query_total,
            query_duration_seconds,
            cache_coverage_total,
            fill_total,
            fill_duration_seconds,
            provider_error_total,
            storage_error_total,
            latest_requested_block,
            latest_filled_block,
        })
    }

    pub fn record_query(&self, labels: &MetricsLabels, outcome: QueryOutcome) {
        self.query_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn observe_query_duration(&self, labels: &MetricsLabels, seconds: f64) {
        self.query_duration_seconds
            .with_label_values(&labels.label_values())
            .observe(seconds);
    }

    pub fn record_cache_coverage(&self, labels: &MetricsLabels, outcome: CacheCoverageOutcome) {
        self.cache_coverage_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn record_fill(&self, labels: &MetricsLabels, outcome: FillOutcome) {
        self.fill_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn observe_fill_duration(&self, labels: &MetricsLabels, seconds: f64) {
        self.fill_duration_seconds
            .with_label_values(&labels.label_values())
            .observe(seconds);
    }

    pub fn record_provider_error(&self, labels: &ErrorLabels) {
        self.provider_error_total
            .with_label_values(&labels.label_values())
            .inc();
    }

    pub fn record_storage_error(&self, labels: &ErrorLabels) {
        self.storage_error_total
            .with_label_values(&labels.label_values())
            .inc();
    }

    pub fn set_latest_requested_block(&self, labels: &MetricsLabels, block: u64) {
        self.latest_requested_block
            .with_label_values(&labels.label_values())
            .set(block as f64);
    }

    pub fn set_latest_filled_block(&self, labels: &MetricsLabels, block: u64) {
        self.latest_filled_block
            .with_label_values(&labels.label_values())
            .set(block as f64);
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let families = self.registry.gather();
        let mut output = String::new();
        TextEncoder::new().encode_utf8(&families, &mut output)?;
        Ok(output)
    }
}

fn error_kind_label(kind: &DatalensErrorKind) -> &'static str {
    match kind {
        DatalensErrorKind::InvalidInput => "invalid_input",
        DatalensErrorKind::InvalidRequest => "invalid_request",
        DatalensErrorKind::UnsupportedDataset => "unsupported_dataset",
        DatalensErrorKind::ProviderFailure => "provider_failure",
        DatalensErrorKind::ProviderLimit => "provider_limit",
        DatalensErrorKind::ProviderTimeout => "provider_timeout",
        DatalensErrorKind::RateLimited => "rate_limited",
        DatalensErrorKind::StorageReadFailure => "storage_read_failure",
        DatalensErrorKind::StorageWriteFailure => "storage_write_failure",
        DatalensErrorKind::ManifestUpdateFailure => "manifest_update_failure",
        DatalensErrorKind::Internal => "internal",
    }
}
