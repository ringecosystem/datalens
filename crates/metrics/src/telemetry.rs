use datalens_core::{ChainIdentity, DatalensErrorKind, DatasetKey};
use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};

const APPLICATION: &str = "unknown";
const COMPACTION_PAUSE_REASONS: [&str; 3] =
    ["query_latency", "write_latency", "object_store_error"];

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionBacklogLabels {
    chain: String,
    chain_kind: String,
    dataset: String,
    selector_kind: String,
    selector: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageDeltaBacklogLabels {
    chain: String,
    chain_kind: String,
    dataset: String,
    scope_kind: String,
    scope: String,
    bucket_start: String,
    bucket_end: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactionTickMetrics<'a> {
    pub status: &'a str,
    pub pause_reason: &'a str,
    pub input_objects: usize,
    pub output_objects: usize,
    pub deleted_source_objects: usize,
    pub deleted_manifest_segments: usize,
    pub duration_seconds: f64,
}

impl CompactionBacklogLabels {
    pub fn new(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        selector_kind: impl Into<String>,
        selector: impl Into<String>,
    ) -> Self {
        Self {
            chain: chain.configured_name().to_owned(),
            chain_kind: chain.family_ref().key().to_owned(),
            dataset: dataset_key.as_str().to_owned(),
            selector_kind: selector_kind.into(),
            selector: selector.into(),
        }
    }

    fn label_values(&self) -> [&str; 5] {
        [
            &self.chain,
            &self.chain_kind,
            &self.dataset,
            &self.selector_kind,
            &self.selector,
        ]
    }
}

impl CoverageDeltaBacklogLabels {
    pub fn new(
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        scope_kind: impl Into<String>,
        scope: impl Into<String>,
        bucket_start: u64,
        bucket_end: u64,
    ) -> Self {
        Self {
            chain: chain.configured_name().to_owned(),
            chain_kind: chain.family_ref().key().to_owned(),
            dataset: dataset_key.as_str().to_owned(),
            scope_kind: scope_kind.into(),
            scope: scope.into(),
            bucket_start: bucket_start.to_string(),
            bucket_end: bucket_end.to_string(),
        }
    }

    fn label_values(&self) -> [&str; 7] {
        [
            &self.chain,
            &self.chain_kind,
            &self.dataset,
            &self.scope_kind,
            &self.scope,
            &self.bucket_start,
            &self.bucket_end,
        ]
    }
}

impl MetricsLabels {
    pub fn new(
        application: ApplicationIdentity,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
    ) -> Self {
        Self {
            application,
            chain: chain.configured_name().to_owned(),
            chain_kind: chain.family_ref().key().to_owned(),
            dataset: dataset_key.as_str().to_owned(),
        }
    }

    pub fn from_dataset_key(
        application: ApplicationIdentity,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
    ) -> Self {
        Self::new(application, chain, dataset_key)
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
    HotHit,
    Miss,
    HotMiss,
    PartialHit,
    Mixed,
    Filled,
    Empty,
    ReorgRollback,
    PromotionCompleted,
    PromotionSkipped,
    Error,
}

impl QueryOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::HotHit => "hot_hit",
            Self::Miss => "miss",
            Self::HotMiss => "hot_miss",
            Self::PartialHit => "partial_hit",
            Self::Mixed => "mixed",
            Self::Filled => "filled",
            Self::Empty => "empty",
            Self::ReorgRollback => "reorg_rollback",
            Self::PromotionCompleted => "promotion_completed",
            Self::PromotionSkipped => "promotion_skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheCoverageOutcome {
    Hit,
    HotHit,
    Miss,
    HotMiss,
    PartialHit,
    Mixed,
    Empty,
    Error,
}

impl CacheCoverageOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::HotHit => "hot_hit",
            Self::Miss => "miss",
            Self::HotMiss => "hot_miss",
            Self::PartialHit => "partial_hit",
            Self::Mixed => "mixed",
            Self::Empty => "empty",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FillOutcome {
    Filled,
    LiveFetch,
    Empty,
    ReorgRollback,
    PromotionWritten,
    PromotionSkipped,
    Error,
}

impl FillOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::LiveFetch => "live_fetch",
            Self::Empty => "empty",
            Self::ReorgRollback => "reorg_rollback",
            Self::PromotionWritten => "promotion_written",
            Self::PromotionSkipped => "promotion_skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableWriteOutcome {
    NotAttempted,
    Staged,
    Flushed,
    EmptyCoverageRecorded,
    Skipped,
    StorageError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableIntentOutcome {
    Submitted,
    AlreadyPending,
    AlreadyCompleted,
    Completed,
    RetryableFailed,
    TerminalFailed,
    Error,
}

impl DurableIntentOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::AlreadyPending => "already_pending",
            Self::AlreadyCompleted => "already_completed",
            Self::Completed => "completed",
            Self::RetryableFailed => "retryable_failed",
            Self::TerminalFailed => "terminal_failed",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableIntentClaimOutcome {
    Claimed,
    Empty,
    ListError,
    MarkRunningError,
    SkippedIneligible,
}

impl DurableIntentClaimOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Empty => "empty",
            Self::ListError => "list_error",
            Self::MarkRunningError => "mark_running_error",
            Self::SkippedIneligible => "skipped_ineligible",
        }
    }
}

impl DurableWriteOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::Staged => "staged",
            Self::Flushed => "flushed",
            Self::EmptyCoverageRecorded => "empty_coverage_recorded",
            Self::Skipped => "skipped",
            Self::StorageError => "storage_error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HotReorgOutcome {
    Detected,
    RollbackApplied,
    StaleEntry,
    RefetchSucceeded,
    RefetchFailed,
    Unsupported,
}

impl HotReorgOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Detected => "detected",
            Self::RollbackApplied => "rollback_applied",
            Self::StaleEntry => "stale_entry",
            Self::RefetchSucceeded => "refetch_succeeded",
            Self::RefetchFailed => "refetch_failed",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HotPromotionOutcome {
    Attempted,
    Promoted,
    Skipped,
    Failed,
}

impl HotPromotionOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Attempted => "attempted",
            Self::Promoted => "promoted",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmupTaskOutcome {
    Submitted,
    Running,
    Completed,
    Paused,
    Cancelled,
    Failed,
}

impl WarmupTaskOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmupFetchOutcome {
    Fetched,
    Empty,
    Skipped,
    Error,
}

impl WarmupFetchOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Fetched => "fetched",
            Self::Empty => "empty",
            Self::Skipped => "skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmupWriteOutcome {
    Written,
    EmptyCoverageRecorded,
    Skipped,
    Error,
}

impl WarmupWriteOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::EmptyCoverageRecorded => "empty_coverage_recorded",
            Self::Skipped => "skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMetadataEnqueueOutcome {
    Enqueued,
    Coalesced,
    CoalesceFull,
    Dropped,
    Closed,
}

impl QueryMetadataEnqueueOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Enqueued => "enqueued",
            Self::Coalesced => "coalesced",
            Self::CoalesceFull => "coalesce_full",
            Self::Dropped => "dropped",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMetadataWriteOutcome {
    Completed,
    Failed,
}

impl QueryMetadataWriteOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
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
    durable_write_total: CounterVec,
    durable_intent_total: CounterVec,
    durable_intent_duration_seconds: HistogramVec,
    durable_intent_pending_total: GaugeVec,
    durable_intent_oldest_pending_age_seconds: GaugeVec,
    durable_intent_claim_total: CounterVec,
    durable_intent_claim_duration_seconds: HistogramVec,
    hot_reorg_total: CounterVec,
    hot_promotion_total: CounterVec,
    fill_duration_seconds: HistogramVec,
    provider_error_total: CounterVec,
    storage_error_total: CounterVec,
    latest_requested_block: GaugeVec,
    latest_filled_block: GaugeVec,
    warmup_task_total: CounterVec,
    warmup_fetch_total: CounterVec,
    warmup_write_total: CounterVec,
    warmup_rows_total: CounterVec,
    warmup_provider_error_total: CounterVec,
    warmup_current_height: GaugeVec,
    query_metadata_enqueue_total: CounterVec,
    query_metadata_write_total: CounterVec,
    query_metadata_write_duration_seconds: HistogramVec,
    indexer_graphql_query_total: CounterVec,
    indexer_graphql_query_duration_seconds: HistogramVec,
    indexer_graphql_auth_failure_total: CounterVec,
    indexer_graphql_rate_limited_total: CounterVec,
    compaction_small_objects: GaugeVec,
    compaction_manifest_segments: GaugeVec,
    compaction_candidate_backlog: GaugeVec,
    compaction_input_objects_total: CounterVec,
    compaction_output_objects_total: CounterVec,
    compaction_deleted_source_objects_total: CounterVec,
    compaction_deleted_manifest_segments_total: CounterVec,
    compaction_tick_duration_seconds: HistogramVec,
    compaction_paused: GaugeVec,
    storage_coverage_delta_backlog: GaugeVec,
    storage_coverage_delta_bytes: GaugeVec,
    storage_coverage_snapshot_age_ms: GaugeVec,
    storage_coverage_compactions_total: CounterVec,
    storage_cleanup_failures_total: CounterVec,
    storage_lock_renew_failures_total: CounterVec,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexerGraphqlMetricLabels {
    pub application: String,
    pub index: String,
    pub chain: String,
    pub dataset: String,
    pub output: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexerGraphqlQueryOutcome {
    Success,
    Error,
}

impl IndexerGraphqlQueryOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
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
        let durable_write_total = CounterVec::new(
            Opts::new(
                "datalens_durable_write_total",
                "Datalens durable write operations by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let durable_intent_total = CounterVec::new(
            Opts::new(
                "datalens_durable_intent_total",
                "Datalens durable promotion intents by source and outcome.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "source",
                "outcome",
            ],
        )?;
        let durable_intent_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_durable_intent_duration_seconds",
                "Datalens durable promotion intent processing duration in seconds.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "source",
                "outcome",
            ],
        )?;
        let durable_intent_pending_total = GaugeVec::new(
            Opts::new(
                "datalens_durable_intent_pending_total",
                "Datalens durable promotion intents currently eligible in the pending index.",
            ),
            &["chain", "chain_kind", "source"],
        )?;
        let durable_intent_oldest_pending_age_seconds = GaugeVec::new(
            Opts::new(
                "datalens_durable_intent_oldest_pending_age_seconds",
                "Age in seconds of the oldest currently eligible durable promotion intent in the pending index.",
            ),
            &["chain", "chain_kind", "source"],
        )?;
        let durable_intent_claim_total = CounterVec::new(
            Opts::new(
                "datalens_durable_intent_claim_total",
                "Datalens durable promotion intent claim attempts by outcome.",
            ),
            &["chain", "chain_kind", "source", "outcome"],
        )?;
        let durable_intent_claim_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_durable_intent_claim_duration_seconds",
                "Datalens durable promotion intent pending-index list and claim duration in seconds.",
            ),
            &["chain", "chain_kind", "source", "outcome"],
        )?;
        let hot_reorg_total = CounterVec::new(
            Opts::new(
                "datalens_hot_reorg_total",
                "Datalens hot cache reorg events by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let hot_promotion_total = CounterVec::new(
            Opts::new(
                "datalens_hot_promotion_total",
                "Datalens hot cache promotion events by outcome.",
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
        let warmup_task_total = CounterVec::new(
            Opts::new(
                "datalens_warmup_task_total",
                "Datalens warmup tasks by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let warmup_fetch_total = CounterVec::new(
            Opts::new(
                "datalens_warmup_fetch_total",
                "Datalens warmup fetches by selector kind and outcome.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "selector_kind",
                "outcome",
            ],
        )?;
        let warmup_write_total = CounterVec::new(
            Opts::new(
                "datalens_warmup_write_total",
                "Datalens warmup durable writes by outcome.",
            ),
            &["application", "chain", "chain_kind", "dataset", "outcome"],
        )?;
        let warmup_rows_total = CounterVec::new(
            Opts::new(
                "datalens_warmup_rows_total",
                "Datalens warmup rows fetched.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;
        let warmup_provider_error_total = CounterVec::new(
            Opts::new(
                "datalens_warmup_provider_error_total",
                "Datalens warmup provider errors by selector kind and stable error kind.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "selector_kind",
                "error_kind",
            ],
        )?;
        let warmup_current_height = GaugeVec::new(
            Opts::new(
                "datalens_warmup_current_height",
                "Current warmup height by application, chain, and dataset.",
            ),
            &["application", "chain", "chain_kind", "dataset"],
        )?;
        let query_metadata_enqueue_total = CounterVec::new(
            Opts::new(
                "datalens_query_metadata_enqueue_total",
                "Datalens query metadata enqueue attempts by metadata kind and outcome.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "metadata_kind",
                "outcome",
            ],
        )?;
        let query_metadata_write_total = CounterVec::new(
            Opts::new(
                "datalens_query_metadata_write_total",
                "Datalens query metadata background writes by metadata kind and outcome.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "metadata_kind",
                "outcome",
            ],
        )?;
        let query_metadata_write_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_query_metadata_write_duration_seconds",
                "Datalens query metadata background write duration in seconds.",
            ),
            &[
                "application",
                "chain",
                "chain_kind",
                "dataset",
                "metadata_kind",
            ],
        )?;
        let indexer_graphql_query_total = CounterVec::new(
            Opts::new(
                "datalens_indexer_graphql_query_total",
                "Datalens indexer GraphQL queries by outcome.",
            ),
            &[
                "application",
                "chain",
                "dataset",
                "index",
                "outcome",
                "output",
            ],
        )?;
        let indexer_graphql_query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_indexer_graphql_query_duration_seconds",
                "Datalens indexer GraphQL query duration in seconds.",
            ),
            &["application", "chain", "dataset", "index", "output"],
        )?;
        let indexer_graphql_auth_failure_total = CounterVec::new(
            Opts::new(
                "datalens_indexer_graphql_auth_failure_total",
                "Datalens indexer GraphQL metrics authentication failures.",
            ),
            &["application", "chain", "dataset", "index", "output"],
        )?;
        let indexer_graphql_rate_limited_total = CounterVec::new(
            Opts::new(
                "datalens_indexer_graphql_rate_limited_total",
                "Datalens indexer GraphQL rate limited requests.",
            ),
            &["application", "chain", "dataset", "index", "output"],
        )?;
        let compaction_small_objects = GaugeVec::new(
            Opts::new(
                "datalens_compaction_small_objects",
                "Small source objects remaining in the compaction backlog.",
            ),
            &[
                "chain",
                "chain_kind",
                "dataset",
                "selector_kind",
                "selector",
            ],
        )?;
        let compaction_manifest_segments = GaugeVec::new(
            Opts::new(
                "datalens_compaction_manifest_segments",
                "Manifest segments represented by the remaining compaction backlog.",
            ),
            &[
                "chain",
                "chain_kind",
                "dataset",
                "selector_kind",
                "selector",
            ],
        )?;
        let compaction_candidate_backlog = GaugeVec::new(
            Opts::new(
                "datalens_compaction_candidate_backlog",
                "Compaction candidate groups remaining after the latest tick.",
            ),
            &[
                "chain",
                "chain_kind",
                "dataset",
                "selector_kind",
                "selector",
            ],
        )?;
        let compaction_input_objects_total = CounterVec::new(
            Opts::new(
                "datalens_compaction_input_objects_total",
                "Source objects read by compaction ticks.",
            ),
            &["chain", "chain_kind", "status", "pause_reason"],
        )?;
        let compaction_output_objects_total = CounterVec::new(
            Opts::new(
                "datalens_compaction_output_objects_total",
                "Compacted output objects written by compaction ticks.",
            ),
            &["chain", "chain_kind", "status", "pause_reason"],
        )?;
        let compaction_deleted_source_objects_total = CounterVec::new(
            Opts::new(
                "datalens_compaction_deleted_source_objects_total",
                "Source objects deleted after successful compaction.",
            ),
            &["chain", "chain_kind", "status", "pause_reason"],
        )?;
        let compaction_deleted_manifest_segments_total = CounterVec::new(
            Opts::new(
                "datalens_compaction_deleted_manifest_segments_total",
                "Manifest segments superseded by successful compaction.",
            ),
            &["chain", "chain_kind", "status", "pause_reason"],
        )?;
        let compaction_tick_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "datalens_compaction_tick_duration_seconds",
                "Storage compaction tick duration in seconds.",
            ),
            &["chain", "chain_kind", "status", "pause_reason"],
        )?;
        let compaction_paused = GaugeVec::new(
            Opts::new(
                "datalens_compaction_paused",
                "Whether compaction is currently paused for a backpressure reason.",
            ),
            &["chain", "chain_kind", "reason"],
        )?;
        let storage_coverage_delta_backlog = GaugeVec::new(
            Opts::new(
                "datalens_storage_coverage_delta_backlog",
                "Coverage-index-v2 delta objects currently backlogged by bucket.",
            ),
            &[
                "chain",
                "chain_kind",
                "dataset",
                "scope_kind",
                "scope",
                "bucket_start",
                "bucket_end",
            ],
        )?;
        let storage_coverage_delta_bytes = GaugeVec::new(
            Opts::new(
                "datalens_storage_coverage_delta_bytes",
                "Coverage-index-v2 delta bytes currently backlogged by bucket.",
            ),
            &[
                "chain",
                "chain_kind",
                "dataset",
                "scope_kind",
                "scope",
                "bucket_start",
                "bucket_end",
            ],
        )?;
        let storage_coverage_snapshot_age_ms = GaugeVec::new(
            Opts::new(
                "datalens_storage_coverage_snapshot_age_ms",
                "Maximum age of coverage-index-v2 snapshots in milliseconds.",
            ),
            &["chain", "chain_kind"],
        )?;
        let storage_coverage_compactions_total = CounterVec::new(
            Opts::new(
                "datalens_storage_coverage_compactions_total",
                "Coverage-index-v2 bucket compactions by status.",
            ),
            &["chain", "chain_kind", "status"],
        )?;
        let storage_cleanup_failures_total = CounterVec::new(
            Opts::new(
                "datalens_storage_cleanup_failures_total",
                "Storage cleanup failures by cleanup kind.",
            ),
            &["chain", "chain_kind", "kind"],
        )?;
        let storage_lock_renew_failures_total = CounterVec::new(
            Opts::new(
                "datalens_storage_lock_renew_failures_total",
                "Storage lock renewal failures.",
            ),
            &["chain", "chain_kind"],
        )?;

        registry.register(Box::new(query_total.clone()))?;
        registry.register(Box::new(query_duration_seconds.clone()))?;
        registry.register(Box::new(cache_coverage_total.clone()))?;
        registry.register(Box::new(fill_total.clone()))?;
        registry.register(Box::new(durable_write_total.clone()))?;
        registry.register(Box::new(durable_intent_total.clone()))?;
        registry.register(Box::new(durable_intent_duration_seconds.clone()))?;
        registry.register(Box::new(durable_intent_pending_total.clone()))?;
        registry.register(Box::new(durable_intent_oldest_pending_age_seconds.clone()))?;
        registry.register(Box::new(durable_intent_claim_total.clone()))?;
        registry.register(Box::new(durable_intent_claim_duration_seconds.clone()))?;
        registry.register(Box::new(hot_reorg_total.clone()))?;
        registry.register(Box::new(hot_promotion_total.clone()))?;
        registry.register(Box::new(fill_duration_seconds.clone()))?;
        registry.register(Box::new(provider_error_total.clone()))?;
        registry.register(Box::new(storage_error_total.clone()))?;
        registry.register(Box::new(latest_requested_block.clone()))?;
        registry.register(Box::new(latest_filled_block.clone()))?;
        registry.register(Box::new(warmup_task_total.clone()))?;
        registry.register(Box::new(warmup_fetch_total.clone()))?;
        registry.register(Box::new(warmup_write_total.clone()))?;
        registry.register(Box::new(warmup_rows_total.clone()))?;
        registry.register(Box::new(warmup_provider_error_total.clone()))?;
        registry.register(Box::new(warmup_current_height.clone()))?;
        registry.register(Box::new(query_metadata_enqueue_total.clone()))?;
        registry.register(Box::new(query_metadata_write_total.clone()))?;
        registry.register(Box::new(query_metadata_write_duration_seconds.clone()))?;
        registry.register(Box::new(indexer_graphql_query_total.clone()))?;
        registry.register(Box::new(indexer_graphql_query_duration_seconds.clone()))?;
        registry.register(Box::new(indexer_graphql_auth_failure_total.clone()))?;
        registry.register(Box::new(indexer_graphql_rate_limited_total.clone()))?;
        registry.register(Box::new(compaction_small_objects.clone()))?;
        registry.register(Box::new(compaction_manifest_segments.clone()))?;
        registry.register(Box::new(compaction_candidate_backlog.clone()))?;
        registry.register(Box::new(compaction_input_objects_total.clone()))?;
        registry.register(Box::new(compaction_output_objects_total.clone()))?;
        registry.register(Box::new(compaction_deleted_source_objects_total.clone()))?;
        registry.register(Box::new(compaction_deleted_manifest_segments_total.clone()))?;
        registry.register(Box::new(compaction_tick_duration_seconds.clone()))?;
        registry.register(Box::new(compaction_paused.clone()))?;
        registry.register(Box::new(storage_coverage_delta_backlog.clone()))?;
        registry.register(Box::new(storage_coverage_delta_bytes.clone()))?;
        registry.register(Box::new(storage_coverage_snapshot_age_ms.clone()))?;
        registry.register(Box::new(storage_coverage_compactions_total.clone()))?;
        registry.register(Box::new(storage_cleanup_failures_total.clone()))?;
        registry.register(Box::new(storage_lock_renew_failures_total.clone()))?;

        Ok(Self {
            registry,
            query_total,
            query_duration_seconds,
            cache_coverage_total,
            fill_total,
            durable_write_total,
            durable_intent_total,
            durable_intent_duration_seconds,
            durable_intent_pending_total,
            durable_intent_oldest_pending_age_seconds,
            durable_intent_claim_total,
            durable_intent_claim_duration_seconds,
            hot_reorg_total,
            hot_promotion_total,
            fill_duration_seconds,
            provider_error_total,
            storage_error_total,
            latest_requested_block,
            latest_filled_block,
            warmup_task_total,
            warmup_fetch_total,
            warmup_write_total,
            warmup_rows_total,
            warmup_provider_error_total,
            warmup_current_height,
            query_metadata_enqueue_total,
            query_metadata_write_total,
            query_metadata_write_duration_seconds,
            indexer_graphql_query_total,
            indexer_graphql_query_duration_seconds,
            indexer_graphql_auth_failure_total,
            indexer_graphql_rate_limited_total,
            compaction_small_objects,
            compaction_manifest_segments,
            compaction_candidate_backlog,
            compaction_input_objects_total,
            compaction_output_objects_total,
            compaction_deleted_source_objects_total,
            compaction_deleted_manifest_segments_total,
            compaction_tick_duration_seconds,
            compaction_paused,
            storage_coverage_delta_backlog,
            storage_coverage_delta_bytes,
            storage_coverage_snapshot_age_ms,
            storage_coverage_compactions_total,
            storage_cleanup_failures_total,
            storage_lock_renew_failures_total,
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

    pub fn record_durable_write(&self, labels: &MetricsLabels, outcome: DurableWriteOutcome) {
        self.durable_write_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn record_durable_intent(
        &self,
        labels: &MetricsLabels,
        source: &str,
        outcome: DurableIntentOutcome,
    ) {
        self.durable_intent_total
            .with_label_values(&durable_intent_label_values(
                labels,
                source,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn observe_durable_intent_duration(
        &self,
        labels: &MetricsLabels,
        source: &str,
        outcome: DurableIntentOutcome,
        seconds: f64,
    ) {
        self.durable_intent_duration_seconds
            .with_label_values(&durable_intent_label_values(
                labels,
                source,
                outcome.as_str(),
            ))
            .observe(seconds);
    }

    pub fn set_durable_intent_backlog_for_scope(
        &self,
        chain: &ChainIdentity,
        source: &str,
        pending_total: usize,
        oldest_pending_age_seconds: u64,
    ) {
        self.durable_intent_pending_total
            .with_label_values(&durable_intent_scope_label_values(chain, source))
            .set(pending_total as f64);
        self.durable_intent_oldest_pending_age_seconds
            .with_label_values(&durable_intent_scope_label_values(chain, source))
            .set(oldest_pending_age_seconds as f64);
    }

    pub fn record_durable_intent_claim(
        &self,
        chain: &ChainIdentity,
        source: &str,
        outcome: DurableIntentClaimOutcome,
    ) {
        self.durable_intent_claim_total
            .with_label_values(&durable_intent_claim_label_values(
                chain,
                source,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn observe_durable_intent_claim_duration(
        &self,
        chain: &ChainIdentity,
        source: &str,
        outcome: DurableIntentClaimOutcome,
        seconds: f64,
    ) {
        self.durable_intent_claim_duration_seconds
            .with_label_values(&durable_intent_claim_label_values(
                chain,
                source,
                outcome.as_str(),
            ))
            .observe(seconds);
    }

    pub fn record_hot_reorg(&self, labels: &MetricsLabels, outcome: HotReorgOutcome, count: u64) {
        self.hot_reorg_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc_by(count as f64);
    }

    pub fn record_hot_promotion(
        &self,
        labels: &MetricsLabels,
        outcome: HotPromotionOutcome,
        count: u64,
    ) {
        self.hot_promotion_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc_by(count as f64);
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

    pub fn record_warmup_task(&self, labels: &MetricsLabels, outcome: WarmupTaskOutcome) {
        self.warmup_task_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn record_warmup_fetch(
        &self,
        labels: &MetricsLabels,
        selector_kind: &str,
        outcome: WarmupFetchOutcome,
    ) {
        self.warmup_fetch_total
            .with_label_values(&warmup_selector_label_values(
                labels,
                selector_kind,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn record_warmup_write(&self, labels: &MetricsLabels, outcome: WarmupWriteOutcome) {
        self.warmup_write_total
            .with_label_values(&labels.query_label_values(outcome.as_str()))
            .inc();
    }

    pub fn record_warmup_rows(&self, labels: &MetricsLabels, rows: u64) {
        self.warmup_rows_total
            .with_label_values(&labels.label_values())
            .inc_by(rows as f64);
    }

    pub fn record_warmup_provider_error(
        &self,
        labels: &MetricsLabels,
        selector_kind: &str,
        error_kind: DatalensErrorKind,
    ) {
        self.warmup_provider_error_total
            .with_label_values(&warmup_error_label_values(
                labels,
                selector_kind,
                error_kind_label(&error_kind),
            ))
            .inc();
    }

    pub fn set_warmup_current_height(&self, labels: &MetricsLabels, height: u64) {
        self.warmup_current_height
            .with_label_values(&labels.label_values())
            .set(height as f64);
    }

    pub fn record_query_metadata_enqueue(
        &self,
        labels: &MetricsLabels,
        metadata_kind: &str,
        outcome: QueryMetadataEnqueueOutcome,
    ) {
        self.query_metadata_enqueue_total
            .with_label_values(&query_metadata_outcome_label_values(
                labels,
                metadata_kind,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn record_query_metadata_write(
        &self,
        labels: &MetricsLabels,
        metadata_kind: &str,
        outcome: QueryMetadataWriteOutcome,
    ) {
        self.query_metadata_write_total
            .with_label_values(&query_metadata_outcome_label_values(
                labels,
                metadata_kind,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn observe_query_metadata_write_duration(
        &self,
        labels: &MetricsLabels,
        metadata_kind: &str,
        seconds: f64,
    ) {
        self.query_metadata_write_duration_seconds
            .with_label_values(&query_metadata_duration_label_values(labels, metadata_kind))
            .observe(seconds);
    }

    pub fn record_indexer_graphql_query(
        &self,
        labels: &IndexerGraphqlMetricLabels,
        outcome: IndexerGraphqlQueryOutcome,
    ) {
        self.indexer_graphql_query_total
            .with_label_values(&indexer_graphql_query_label_values(
                labels,
                outcome.as_str(),
            ))
            .inc();
    }

    pub fn observe_indexer_graphql_query_duration(
        &self,
        labels: &IndexerGraphqlMetricLabels,
        seconds: f64,
    ) {
        self.indexer_graphql_query_duration_seconds
            .with_label_values(&indexer_graphql_label_values(labels))
            .observe(seconds);
    }

    pub fn record_indexer_graphql_auth_failure(&self, labels: &IndexerGraphqlMetricLabels) {
        self.indexer_graphql_auth_failure_total
            .with_label_values(&indexer_graphql_label_values(labels))
            .inc();
    }

    pub fn record_indexer_graphql_rate_limited(&self, labels: &IndexerGraphqlMetricLabels) {
        self.indexer_graphql_rate_limited_total
            .with_label_values(&indexer_graphql_label_values(labels))
            .inc();
    }

    pub fn set_compaction_backlog(
        &self,
        labels: &CompactionBacklogLabels,
        small_objects: usize,
        manifest_segments: usize,
        candidate_backlog: usize,
    ) {
        self.compaction_small_objects
            .with_label_values(&labels.label_values())
            .set(small_objects as f64);
        self.compaction_manifest_segments
            .with_label_values(&labels.label_values())
            .set(manifest_segments as f64);
        self.compaction_candidate_backlog
            .with_label_values(&labels.label_values())
            .set(candidate_backlog as f64);
    }

    pub fn set_storage_coverage_delta_backlog(
        &self,
        labels: &CoverageDeltaBacklogLabels,
        object_count: usize,
        bytes: u64,
    ) {
        self.storage_coverage_delta_backlog
            .with_label_values(&labels.label_values())
            .set(object_count as f64);
        self.storage_coverage_delta_bytes
            .with_label_values(&labels.label_values())
            .set(bytes as f64);
    }

    pub fn set_storage_coverage_snapshot_age_ms(&self, chain: &ChainIdentity, age_ms: u64) {
        self.storage_coverage_snapshot_age_ms
            .with_label_values(&chain_label_values(chain))
            .set(age_ms as f64);
    }

    pub fn record_storage_coverage_compaction(
        &self,
        chain: &ChainIdentity,
        status: &str,
        count: usize,
    ) {
        self.storage_coverage_compactions_total
            .with_label_values(&storage_status_label_values(chain, status))
            .inc_by(count as f64);
    }

    pub fn record_storage_cleanup_failures(&self, chain: &ChainIdentity, kind: &str, count: usize) {
        self.storage_cleanup_failures_total
            .with_label_values(&storage_kind_label_values(chain, kind))
            .inc_by(count as f64);
    }

    pub fn record_storage_lock_renew_failure(&self, chain: &ChainIdentity) {
        self.storage_lock_renew_failures_total
            .with_label_values(&chain_label_values(chain))
            .inc();
    }

    pub fn record_compaction_tick(&self, chain: &ChainIdentity, tick: CompactionTickMetrics<'_>) {
        let labels = compaction_tick_label_values(chain, tick.status, tick.pause_reason);
        self.compaction_input_objects_total
            .with_label_values(&labels)
            .inc_by(tick.input_objects as f64);
        self.compaction_output_objects_total
            .with_label_values(&labels)
            .inc_by(tick.output_objects as f64);
        self.compaction_deleted_source_objects_total
            .with_label_values(&labels)
            .inc_by(tick.deleted_source_objects as f64);
        self.compaction_deleted_manifest_segments_total
            .with_label_values(&labels)
            .inc_by(tick.deleted_manifest_segments as f64);
        self.compaction_tick_duration_seconds
            .with_label_values(&labels)
            .observe(tick.duration_seconds);
        for reason in COMPACTION_PAUSE_REASONS {
            if reason != tick.pause_reason {
                self.compaction_paused
                    .with_label_values(&compaction_paused_label_values(chain, reason))
                    .set(0.0);
            }
        }
        self.compaction_paused
            .with_label_values(&compaction_paused_label_values(chain, tick.pause_reason))
            .set((tick.pause_reason != "none") as u8 as f64);
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let families = self.registry.gather();
        let mut output = String::new();
        TextEncoder::new().encode_utf8(&families, &mut output)?;
        Ok(output)
    }
}

fn warmup_selector_label_values<'a>(
    labels: &'a MetricsLabels,
    selector_kind: &'a str,
    outcome: &'a str,
) -> [&'a str; 6] {
    [
        labels.application.as_str(),
        &labels.chain,
        &labels.chain_kind,
        &labels.dataset,
        selector_kind,
        outcome,
    ]
}

fn query_metadata_outcome_label_values<'a>(
    labels: &'a MetricsLabels,
    metadata_kind: &'a str,
    outcome: &'a str,
) -> [&'a str; 6] {
    [
        labels.application.as_str(),
        &labels.chain,
        &labels.chain_kind,
        &labels.dataset,
        metadata_kind,
        outcome,
    ]
}

fn query_metadata_duration_label_values<'a>(
    labels: &'a MetricsLabels,
    metadata_kind: &'a str,
) -> [&'a str; 5] {
    [
        labels.application.as_str(),
        &labels.chain,
        &labels.chain_kind,
        &labels.dataset,
        metadata_kind,
    ]
}

fn durable_intent_label_values<'a>(
    labels: &'a MetricsLabels,
    source: &'a str,
    outcome: &'a str,
) -> [&'a str; 6] {
    [
        labels.application.as_str(),
        &labels.chain,
        &labels.chain_kind,
        &labels.dataset,
        source,
        outcome,
    ]
}

fn durable_intent_scope_label_values<'a>(
    chain: &'a ChainIdentity,
    source: &'a str,
) -> [&'a str; 3] {
    [chain.configured_name(), chain.family_ref().key(), source]
}

fn durable_intent_claim_label_values<'a>(
    chain: &'a ChainIdentity,
    source: &'a str,
    outcome: &'a str,
) -> [&'a str; 4] {
    [
        chain.configured_name(),
        chain.family_ref().key(),
        source,
        outcome,
    ]
}

fn warmup_error_label_values<'a>(
    labels: &'a MetricsLabels,
    selector_kind: &'a str,
    error_kind: &'a str,
) -> [&'a str; 6] {
    [
        labels.application.as_str(),
        &labels.chain,
        &labels.chain_kind,
        &labels.dataset,
        selector_kind,
        error_kind,
    ]
}

fn indexer_graphql_label_values(labels: &IndexerGraphqlMetricLabels) -> [&str; 5] {
    [
        labels.application.as_str(),
        labels.chain.as_str(),
        labels.dataset.as_str(),
        labels.index.as_str(),
        labels.output.as_str(),
    ]
}

fn compaction_tick_label_values<'a>(
    chain: &'a ChainIdentity,
    status: &'a str,
    pause_reason: &'a str,
) -> [&'a str; 4] {
    [
        chain.configured_name(),
        chain.family_ref().key(),
        status,
        pause_reason,
    ]
}

fn compaction_paused_label_values<'a>(chain: &'a ChainIdentity, reason: &'a str) -> [&'a str; 3] {
    [chain.configured_name(), chain.family_ref().key(), reason]
}

fn chain_label_values(chain: &ChainIdentity) -> [&str; 2] {
    [chain.configured_name(), chain.family_ref().key()]
}

fn storage_status_label_values<'a>(chain: &'a ChainIdentity, status: &'a str) -> [&'a str; 3] {
    [chain.configured_name(), chain.family_ref().key(), status]
}

fn storage_kind_label_values<'a>(chain: &'a ChainIdentity, kind: &'a str) -> [&'a str; 3] {
    [chain.configured_name(), chain.family_ref().key(), kind]
}

fn indexer_graphql_query_label_values<'a>(
    labels: &'a IndexerGraphqlMetricLabels,
    outcome: &'a str,
) -> [&'a str; 6] {
    [
        labels.application.as_str(),
        labels.chain.as_str(),
        labels.dataset.as_str(),
        labels.index.as_str(),
        outcome,
        labels.output.as_str(),
    ]
}

fn error_kind_label(kind: &DatalensErrorKind) -> &'static str {
    match kind {
        DatalensErrorKind::AuthenticationFailed => "authentication_failed",
        DatalensErrorKind::InvalidInput => "invalid_input",
        DatalensErrorKind::InvalidRequest => "invalid_request",
        DatalensErrorKind::Unauthorized => "unauthorized",
        DatalensErrorKind::UnsupportedDataset => "unsupported_dataset",
        DatalensErrorKind::UnsupportedHotQuery => "unsupported_hot_query",
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
