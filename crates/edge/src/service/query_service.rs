use std::{sync::Arc, time::Instant};

use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainHeight, DatasetCapability, SelectorKind,
};
use datalens_core::{DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor, generate_query_id};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_planner::{NativePlannerConfig, NativeQueryInput};
use datalens_storage::{
    DurablePromotionIntentRepository, QueryWatermarkRepository, StorageRepository,
    UsageLedgerRepository,
};
use datalens_warmup::{
    WarmupEnsureOutcome, WarmupRegistry, WarmupRunResult, WarmupSubmitOutcome, WarmupSubmitRequest,
    WarmupTask, WarmupTaskFilter, WarmupTaskId, WarmupTaskPool,
};
use datalens_writer::{DurableWriteResult, DurableWriter, DurableWriterConfig};

use crate::{
    chain_family,
    config::{ChainConfig, MetricsConfig, PlannerConfig, WriterConfig},
    contract::discovery::{ChainDiscovery, DatasetDiscovery},
};

#[derive(Clone)]
/// Edge-facing service wrapper around the native query executor. REST and
/// GraphQL both enter through this service so route validation, application
/// attribution, and the native query contract stay identical across APIs.
pub struct QueryService<S> {
    executor: NativeQueryExecutor<Arc<dyn StorageRepository>, S>,
    chain_name: String,
    chain: ChainConfig,
    capabilities: AdapterCapabilities,
    metrics: Option<MetricsRecorder>,
    warmup: Option<Arc<dyn RegisteredWarmupService>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// API-safe cache summary derived from planner coverage. Segment metadata is
/// range-local because one response may combine durable cache, hot cache, and
/// provider-filled data with different finality.
pub struct NativeCacheSummary {
    pub hit_ranges: Vec<LedgerRange>,
    pub missing_ranges: Vec<LedgerRange>,
    pub durable_hit_ranges: Vec<LedgerRange>,
    pub hot_hit_ranges: Vec<LedgerRange>,
    pub provider_fill_ranges: Vec<LedgerRange>,
    pub promotion_pending_ranges: Vec<LedgerRange>,
    pub segments: Vec<datalens_core::QuerySegmentMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryResponse {
    pub chain: datalens_core::ChainIdentity,
    pub dataset_key: DatasetKey,
    pub ledger_range: LedgerRange,
    pub cache: NativeCacheSummary,
    pub rows: DatasetRows,
}

impl<S> QueryService<S>
where
    S: ChainAdapter,
{
    pub fn new(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain: ChainConfig,
    ) -> Self {
        Self::new_named(storage, source, planner, writer, "ethereum", chain)
    }

    pub fn new_named(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain_name: impl Into<String>,
        chain: ChainConfig,
    ) -> Self {
        Self::new_with_metrics_config(
            storage,
            source,
            planner,
            writer,
            chain_name,
            chain,
            MetricsConfig::default(),
        )
        .expect("default metrics recorder initializes")
    }

    pub fn new_with_metrics_config(
        storage: impl StorageRepository + 'static,
        source: S,
        planner: PlannerConfig,
        writer: WriterConfig,
        chain_name: impl Into<String>,
        chain: ChainConfig,
        metrics_config: MetricsConfig,
    ) -> Result<Self, DatalensError> {
        let storage: Arc<dyn StorageRepository> = Arc::new(storage);
        let recorder = if metrics_config.enabled {
            Some(MetricsRecorder::new().map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("initialize metrics recorder: {error}"),
                )
            })?)
        } else {
            None
        };
        let capabilities = source.capabilities();
        let mut executor = NativeQueryExecutor::new(
            storage,
            source,
            NativeQueryExecutionConfig {
                planner: NativePlannerConfig {
                    max_query_range_len: planner.max_query_range_blocks,
                    default_chunk_range_len: planner.default_chunk_range_blocks,
                },
                writer: DurableWriterConfig {
                    target_object_bytes: writer.target_object_bytes,
                    min_object_rows: writer.min_object_rows,
                    record_empty_coverage: writer.record_empty_coverage,
                    staging: datalens_writer::WriteStagingConfig {
                        enabled: writer.staging.enabled,
                        min_rows: writer.staging.min_rows,
                        target_object_bytes: writer.staging.target_object_bytes,
                        max_staged_ranges: writer.staging.max_staged_ranges,
                        max_staged_rows: writer.staging.max_staged_rows,
                        max_staged_age_ms: writer.staging.max_staged_age_ms,
                        flush_on_shutdown: writer.staging.flush_on_shutdown,
                        max_staged_bytes: writer.staging.max_staged_bytes,
                    },
                },
            },
        );
        if let Some(recorder) = recorder.clone() {
            executor = executor.with_metrics(
                recorder,
                ApplicationIdentity::named(metrics_config.default_application),
            );
        }

        Ok(Self {
            executor,
            chain_name: chain_name.into(),
            chain,
            capabilities,
            metrics: recorder,
            warmup: None,
        })
    }

    pub fn with_metrics(mut self, metrics: MetricsRecorder) -> Self {
        self.executor = self
            .executor
            .with_metrics(metrics.clone(), ApplicationIdentity::unknown());
        self.metrics = Some(metrics);
        self
    }

    pub fn metrics_recorder(&self) -> Option<MetricsRecorder> {
        self.metrics.clone()
    }

    pub fn durable_writer(&self) -> DurableWriter<Arc<dyn StorageRepository>> {
        self.executor.durable_writer()
    }

    pub fn with_usage_ledger(
        mut self,
        repository: impl UsageLedgerRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.executor = self.executor.with_usage_ledger(repository, application);
        self
    }

    pub fn with_query_watermarks(
        mut self,
        repository: impl QueryWatermarkRepository + 'static,
        application: ApplicationIdentity,
    ) -> Self {
        self.executor = self.executor.with_query_watermarks(repository, application);
        self
    }

    pub fn with_durable_intents(
        mut self,
        repository: impl DurablePromotionIntentRepository + 'static,
    ) -> Self {
        self.executor = self.executor.with_durable_intents(repository);
        self
    }

    pub fn with_warmup_pool<P>(mut self, pool: P) -> Self
    where
        P: RegisteredWarmupService + 'static,
    {
        self.warmup = Some(Arc::new(pool));
        self
    }

    pub fn chain_name(&self) -> &str {
        &self.chain_name
    }

    pub fn chain_kind(&self) -> &str {
        &self.chain.kind
    }

    pub fn chain_id(&self) -> u64 {
        self.chain.chain_id
    }

    pub fn chain_identity(&self) -> datalens_core::ChainIdentity {
        self.capabilities.chain().clone()
    }

    pub fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        self.executor.latest_height()
    }

    pub fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        self.executor.cache_safe_height()
    }

    pub fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        self.executor.finalized_height()
    }

    pub fn query_native(
        &self,
        native_input: NativeQueryInput,
    ) -> Result<NativeQueryResponse, DatalensError> {
        self.query_native_with_application(native_input, None)
    }

    pub fn query_native_with_application(
        &self,
        native_input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError> {
        let query_id = generate_query_id();
        self.query_native_with_application_and_query_id(native_input, application, query_id)
    }

    pub fn query_native_with_application_and_query_id(
        &self,
        native_input: NativeQueryInput,
        application: Option<ApplicationIdentity>,
        query_id: String,
    ) -> Result<NativeQueryResponse, DatalensError> {
        if let Err(error) = self.validate_native_query_route(&native_input) {
            log::warn!(
                "native query validation failed query_id={} chain={} dataset={} range={}-{} kind={:?} message={}",
                query_id,
                native_input.chain.configured_name(),
                native_input.dataset_key.as_str(),
                native_input.ledger_range.start(),
                native_input.ledger_range.end(),
                error.kind,
                error.message
            );
            return Err(error);
        }
        let start = Instant::now();
        log::info!(
            "native query start query_id={} chain={} dataset={} range={}-{}",
            query_id,
            native_input.chain.configured_name(),
            native_input.dataset_key.as_str(),
            native_input.ledger_range.start(),
            native_input.ledger_range.end()
        );
        let result = self.executor.execute_with_application_and_query_id(
            native_input,
            application,
            query_id.clone(),
        );
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                log::warn!(
                    "native query failed query_id={} kind={:?} message={} duration_ms={}",
                    query_id,
                    error.kind,
                    error.message,
                    start.elapsed().as_millis()
                );
                return Err(error);
            }
        };
        log::info!(
            "native query completed query_id={} chain={} dataset={} range={}-{} rows={} duration_ms={}",
            query_id,
            result.chain.configured_name(),
            result.dataset_key.as_str(),
            result.ledger_range.start(),
            result.ledger_range.end(),
            result.rows.row_count(),
            start.elapsed().as_millis()
        );
        Ok(NativeQueryResponse {
            chain: result.chain,
            dataset_key: result.dataset_key,
            ledger_range: result.ledger_range,
            cache: NativeCacheSummary {
                hit_ranges: result.cache.hit_ranges,
                missing_ranges: result.cache.missing_ranges,
                durable_hit_ranges: result.cache.durable_hit_ranges,
                hot_hit_ranges: result.cache.hot_hit_ranges,
                provider_fill_ranges: result.cache.provider_fill_ranges,
                promotion_pending_ranges: result.cache.promotion_pending_ranges,
                segments: result.cache.segments,
            },
            rows: result.rows,
        })
    }

    pub fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        self.metrics.as_ref().map(|recorder| {
            recorder.encode().map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode metrics: {error}"),
                )
            })
        })
    }

    pub fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        self.executor.flush_staged_writes_for_shutdown()
    }

    pub fn wait_for_durable_promotions(&self) -> Result<(), DatalensError> {
        self.executor.wait_for_durable_promotions()
    }

    pub fn discovery(&self) -> Result<ChainDiscovery, DatalensError> {
        Ok(ChainDiscovery {
            identity: self.capabilities.chain().clone(),
            datasets: self
                .capabilities
                .dataset_capabilities()
                .iter()
                .filter(|capability| self.dataset_discovery_enabled(capability.dataset()))
                .map(dataset_discovery)
                .collect(),
        })
    }

    fn dataset_discovery_enabled(&self, dataset_key: &DatasetKey) -> bool {
        if self.chain.kind != "evm" {
            return true;
        }
        if dataset_key == &DatasetKey::evm_blocks() {
            return self.chain.datasets.blocks.enabled;
        }
        if dataset_key == &DatasetKey::evm_logs() {
            return self.chain.datasets.logs.enabled;
        }
        true
    }

    fn validate_native_query_route(&self, input: &NativeQueryInput) -> Result<(), DatalensError> {
        // Route policy is the edge boundary: adapters validate provider support,
        // while the service rejects chains or disabled datasets before execution.
        if input.chain.configured_name() != self.chain_name {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "chain is not configured",
            ));
        }
        let chain_family = chain_family(&self.chain.kind)?;
        if input.chain.family_ref() != &chain_family || input.dataset_key.family() != &chain_family
        {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "query chain and dataset must match the configured chain kind",
            ));
        }
        if self.chain.kind == "evm" {
            if input.dataset_key == DatasetKey::evm_blocks() && !self.chain.datasets.blocks.enabled
            {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "blocks dataset is disabled",
                ));
            }
            if input.dataset_key == DatasetKey::evm_logs() && !self.chain.datasets.logs.enabled {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "logs dataset is disabled",
                ));
            }
        }
        Ok(())
    }
}

fn dataset_discovery(capability: &DatasetCapability) -> DatasetDiscovery {
    DatasetDiscovery {
        dataset_key: capability.dataset().as_str().to_owned(),
        range_kinds: capability.ranges().to_vec(),
        selectors: capability
            .selectors()
            .iter()
            .map(selector_kind_name)
            .collect(),
        enabled: true,
    }
}

fn selector_kind_name(selector: &SelectorKind) -> String {
    match selector {
        SelectorKind::All => "all".to_owned(),
        SelectorKind::EvmLogs => "evm_logs".to_owned(),
        SelectorKind::Other(kind) => kind.as_str().to_owned(),
    }
}

pub(crate) trait RegisteredQueryService: Send + Sync {
    fn chain_name(&self) -> &str;

    fn chain_kind(&self) -> &str;

    fn chain_id(&self) -> u64;

    fn chain_identity(&self) -> datalens_core::ChainIdentity;

    fn latest_height(&self) -> Result<ChainHeight, DatalensError>;

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError>;

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError>;

    fn query_native(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
        query_id: String,
    ) -> Result<NativeQueryResponse, DatalensError>;

    fn metrics_text(&self) -> Option<Result<String, DatalensError>>;

    fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError>;

    fn wait_for_durable_promotions(&self) -> Result<(), DatalensError>;

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError>;

    fn warmup(&self) -> Option<Arc<dyn RegisteredWarmupService>>;
}

pub trait RegisteredWarmupService: Send + Sync {
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError>;
    fn ensure(&self, request: WarmupSubmitRequest) -> Result<WarmupEnsureOutcome, DatalensError>;
    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError>;
    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError>;
    fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError>;
    fn run_available_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError>;
}

impl<A, S, R> RegisteredWarmupService for WarmupTaskPool<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: WarmupRegistry,
{
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError> {
        WarmupTaskPool::submit(self, request)
    }

    fn ensure(&self, request: WarmupSubmitRequest) -> Result<WarmupEnsureOutcome, DatalensError> {
        WarmupTaskPool::ensure(self, request)
    }

    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        WarmupTaskPool::get(self, task_id)
    }

    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError> {
        WarmupTaskPool::list(self, filter)
    }

    fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::pause(self, task_id)
    }

    fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::cancel(self, task_id)
    }

    fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        WarmupTaskPool::retry_failed(self, task_id)
    }

    fn run_available_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError> {
        WarmupTaskPool::run_available_once(self)
    }
}

impl<S> RegisteredQueryService for QueryService<S>
where
    S: ChainAdapter + 'static,
{
    fn chain_name(&self) -> &str {
        QueryService::chain_name(self)
    }

    fn chain_kind(&self) -> &str {
        QueryService::chain_kind(self)
    }

    fn chain_id(&self) -> u64 {
        QueryService::chain_id(self)
    }

    fn chain_identity(&self) -> datalens_core::ChainIdentity {
        QueryService::chain_identity(self)
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        QueryService::latest_height(self)
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        QueryService::cache_safe_height(self)
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        QueryService::finalized_height(self)
    }

    fn query_native(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
        query_id: String,
    ) -> Result<NativeQueryResponse, DatalensError> {
        QueryService::query_native_with_application_and_query_id(
            self,
            request,
            application,
            query_id,
        )
    }

    fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        QueryService::metrics_text(self)
    }

    fn flush_staged_writes_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        QueryService::flush_staged_writes_for_shutdown(self)
    }

    fn wait_for_durable_promotions(&self) -> Result<(), DatalensError> {
        QueryService::wait_for_durable_promotions(self)
    }

    fn discovery(&self) -> Result<ChainDiscovery, DatalensError> {
        QueryService::discovery(self)
    }

    fn warmup(&self) -> Option<Arc<dyn RegisteredWarmupService>> {
        self.warmup.clone()
    }
}
