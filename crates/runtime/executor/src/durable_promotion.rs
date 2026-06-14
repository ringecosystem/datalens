use std::{
    collections::{BTreeSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc, Condvar, Mutex, Once,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FetchContext, FinalityLevel,
};
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRangeKind};
use datalens_metrics::{
    ApplicationIdentity, DurableIntentClaimOutcome as MetricsDurableIntentClaimOutcome,
    DurableIntentOutcome as MetricsDurableIntentOutcome,
    DurableWriteOutcome as MetricsDurableWriteOutcome, ErrorLabels, MetricsLabels, MetricsRecorder,
};
use datalens_storage::{
    DurablePromotionIntent, DurablePromotionIntentBacklog, DurablePromotionIntentRepository,
    DurablePromotionIntentSource, StorageRepository,
};
use datalens_writer::{DurableWriteRequest, DurableWriteSegment, DurableWriter};

use crate::helpers::{
    dataset_capability_max_range_len, is_storage_error, metrics_durable_write_outcome,
    parse_provider_limit_hint, split_fetch_request_by_max_len, split_provider_limit_range,
};
use crate::provider_range::{ProviderRangeController, ProviderRangeKey};

const DEFAULT_PROMOTION_WORKERS: usize = 4;
const DEFAULT_PROMOTION_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_INTENT_WORKERS: usize = 2;
const DEFAULT_INTENT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_INTENT_CLAIM_BATCH_SIZE: usize = 16;
const DEFAULT_INTENT_BACKLOG_METRIC_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS: u64 = 60;
const DEFAULT_INTENT_RETRY_MAX_DELAY_SECONDS: u64 = 3600;
const DEFAULT_INTENT_RETRY_MAX_JITTER_SECONDS: u64 = 60;
const DEFAULT_INTENT_STALE_RUNNING_SECONDS: u64 = 300;
const DEFAULT_INTENT_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurablePromotionIntentWorkerConfig {
    pub worker_threads: usize,
    pub claim_batch_size: usize,
    pub terminal_retention_seconds: Option<u64>,
    pub cleanup_max_scan: usize,
    pub cleanup_max_deletes: usize,
    pub cleanup_interval: Duration,
}

impl Default for DurablePromotionIntentWorkerConfig {
    fn default() -> Self {
        Self {
            worker_threads: DEFAULT_INTENT_WORKERS,
            claim_batch_size: DEFAULT_INTENT_CLAIM_BATCH_SIZE,
            terminal_retention_seconds: None,
            cleanup_max_scan: 1024,
            cleanup_max_deletes: 256,
            cleanup_interval: DEFAULT_INTENT_CLEANUP_INTERVAL,
        }
    }
}

#[derive(Clone)]
pub(crate) struct DurablePromotionQueue<R> {
    sender: SyncSender<DurablePromotionWork>,
    state: Arc<(Mutex<DurablePromotionState>, Condvar)>,
    _storage: std::marker::PhantomData<R>,
}

#[derive(Clone)]
pub(crate) struct DurablePromotionIntentWorker<R, A> {
    _storage: std::marker::PhantomData<R>,
    _adapter: std::marker::PhantomData<A>,
}

impl<R, A> DurablePromotionIntentWorker<R, A>
where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    pub(crate) fn start_with_startup_maintenance_once(
        repository: Arc<dyn DurablePromotionIntentRepository>,
        writer: DurableWriter<R>,
        adapter: A,
        metrics: Option<Arc<MetricsRecorder>>,
        provider_ranges: ProviderRangeController,
        startup_maintenance_once: Arc<Once>,
        config: DurablePromotionIntentWorkerConfig,
    ) -> Result<Self, DatalensError> {
        let claim_lock = Arc::new(Mutex::new(()));
        let shared = IntentWorkerShared {
            repository,
            metrics,
            provider_ranges,
            claim_lock,
            startup_maintenance_once,
            claim_batch_size: config.claim_batch_size,
            terminal_retention_seconds: config.terminal_retention_seconds,
            cleanup_max_scan: config.cleanup_max_scan,
            cleanup_max_deletes: config.cleanup_max_deletes,
            cleanup_interval: config.cleanup_interval,
        };
        for worker_index in 0..config.worker_threads {
            spawn_intent_worker(
                worker_index,
                writer.clone(),
                adapter.clone(),
                shared.clone(),
            )?;
        }
        Ok(Self {
            _storage: std::marker::PhantomData,
            _adapter: std::marker::PhantomData,
        })
    }
}

#[derive(Clone)]
struct IntentWorkerShared {
    repository: Arc<dyn DurablePromotionIntentRepository>,
    metrics: Option<Arc<MetricsRecorder>>,
    provider_ranges: ProviderRangeController,
    claim_lock: Arc<Mutex<()>>,
    startup_maintenance_once: Arc<Once>,
    claim_batch_size: usize,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
    cleanup_interval: Duration,
}

#[derive(Debug, Default)]
struct DurablePromotionState {
    in_flight: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct DurablePromotionMetrics {
    pub(crate) recorder: Arc<MetricsRecorder>,
    pub(crate) labels: MetricsLabels,
}

pub(crate) struct DurablePromotionRequest {
    pub(crate) query_id: String,
    pub(crate) chain: ChainIdentity,
    pub(crate) dataset_key: DatasetKey,
    pub(crate) selector: DatasetSelector,
    pub(crate) finality_level: FinalityLevel,
    pub(crate) segments: Vec<DurableWriteSegment>,
    pub(crate) metrics: Option<DurablePromotionMetrics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PromotionEnqueueOutcome {
    Queued,
    AlreadyInFlight,
    Rejected,
}

struct DurablePromotionWork {
    key: String,
    request: DurablePromotionRequest,
}

impl<R> DurablePromotionQueue<R>
where
    R: StorageRepository + Clone + 'static,
{
    pub(crate) fn new(writer: DurableWriter<R>) -> Result<Self, DatalensError> {
        let (sender, receiver) = mpsc::sync_channel(DEFAULT_PROMOTION_QUEUE_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        let state = Arc::new((Mutex::new(DurablePromotionState::default()), Condvar::new()));
        for worker_index in 0..DEFAULT_PROMOTION_WORKERS {
            spawn_worker(
                worker_index,
                writer.clone(),
                receiver.clone(),
                state.clone(),
            )?;
        }
        Ok(Self {
            sender,
            state,
            _storage: std::marker::PhantomData,
        })
    }

    pub(crate) fn enqueue(
        &self,
        request: DurablePromotionRequest,
    ) -> Result<PromotionEnqueueOutcome, DatalensError> {
        if request.segments.is_empty() {
            return Ok(PromotionEnqueueOutcome::Rejected);
        }
        let key = promotion_key(&request);
        {
            let (lock, _) = self.state.as_ref();
            let mut state = lock
                .lock()
                .map_err(|_| DatalensError::internal("durable promotion lock poisoned"))?;
            if state.in_flight.contains(&key) {
                return Ok(PromotionEnqueueOutcome::AlreadyInFlight);
            }
            state.in_flight.insert(key.clone());
        }

        let work = DurablePromotionWork {
            key: key.clone(),
            request,
        };
        match self.sender.try_send(work) {
            Ok(()) => Ok(PromotionEnqueueOutcome::Queued),
            Err(TrySendError::Full(work)) => {
                log::warn!(
                    "durable promotion queue full query_id={} dataset={} key={}",
                    work.request.query_id,
                    work.request.dataset_key.as_str(),
                    work.key
                );
                self.finish_key(&work.key)?;
                Ok(PromotionEnqueueOutcome::Rejected)
            }
            Err(TrySendError::Disconnected(work)) => {
                log::error!(
                    "durable promotion queue stopped query_id={} dataset={} key={}",
                    work.request.query_id,
                    work.request.dataset_key.as_str(),
                    work.key
                );
                self.finish_key(&work.key)?;
                Ok(PromotionEnqueueOutcome::Rejected)
            }
        }
    }

    pub(crate) fn wait_for_idle(&self) -> Result<(), DatalensError> {
        let (lock, condvar) = self.state.as_ref();
        let mut state = lock
            .lock()
            .map_err(|_| DatalensError::internal("durable promotion lock poisoned"))?;
        while !state.in_flight.is_empty() {
            state = condvar
                .wait(state)
                .map_err(|_| DatalensError::internal("durable promotion lock poisoned"))?;
        }
        Ok(())
    }

    fn finish_key(&self, key: &str) -> Result<(), DatalensError> {
        let (lock, condvar) = self.state.as_ref();
        let mut state = lock
            .lock()
            .map_err(|_| DatalensError::internal("durable promotion lock poisoned"))?;
        state.in_flight.remove(key);
        if state.in_flight.is_empty() {
            condvar.notify_all();
        }
        Ok(())
    }
}

fn spawn_worker<R>(
    worker_index: usize,
    writer: DurableWriter<R>,
    receiver: Arc<Mutex<Receiver<DurablePromotionWork>>>,
    state: Arc<(Mutex<DurablePromotionState>, Condvar)>,
) -> Result<(), DatalensError>
where
    R: StorageRepository + Clone + 'static,
{
    thread::Builder::new()
        .name(format!("datalens-durable-promotion-{worker_index}"))
        .spawn(move || {
            loop {
                let work = {
                    let receiver = match receiver.lock() {
                        Ok(receiver) => receiver,
                        Err(_) => return,
                    };
                    receiver.recv()
                };
                let Ok(work) = work else {
                    return;
                };
                run_promotion_work(&writer, &work);
                let (lock, condvar) = state.as_ref();
                let mut state = match lock.lock() {
                    Ok(state) => state,
                    Err(_) => return,
                };
                state.in_flight.remove(&work.key);
                if state.in_flight.is_empty() {
                    condvar.notify_all();
                }
            }
        })
        .map(|_| ())
        .map_err(|error| {
            DatalensError::new(
                datalens_core::DatalensErrorKind::Internal,
                format!("start durable promotion worker: {error}"),
            )
        })
}

fn run_promotion_work<R>(writer: &DurableWriter<R>, work: &DurablePromotionWork)
where
    R: StorageRepository + Clone + 'static,
{
    let request = &work.request;
    let ranges = request
        .segments
        .iter()
        .map(|segment| {
            format!(
                "{}:{}-{}",
                range_kind_key(segment.range.kind()),
                segment.range.start(),
                segment.range.end()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    log::info!(
        "durable promotion started query_id={} chain_key={} dataset={} finality={} ranges={} key={}",
        request.query_id,
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        finality_key(request.finality_level),
        ranges,
        work.key
    );
    let started = Instant::now();
    let result = writer.write(DurableWriteRequest {
        chain: request.chain.clone(),
        dataset_key: request.dataset_key.clone(),
        selector: request.selector.clone(),
        finality_level: request.finality_level,
        segments: request.segments.clone(),
    });
    match result {
        Ok(mut result) => {
            if !result.staged_ranges.is_empty() {
                let staged_ranges = result.staged_ranges.clone();
                match writer.flush_ranges(
                    &request.chain,
                    &request.dataset_key,
                    &request.selector,
                    &staged_ranges,
                ) {
                    Ok(flush_result) => {
                        result.data_objects.extend(flush_result.data_objects);
                        result.empty_coverages.extend(flush_result.empty_coverages);
                        result.skipped_ranges.extend(flush_result.skipped_ranges);
                        result.staged_ranges.clear();
                        result.flush_reason = flush_result.flush_reason.or(result.flush_reason);
                    }
                    Err(error) => {
                        log::error!(
                            "durable promotion flush failed query_id={} dataset={} key={} kind={:?} message={} duration_ms={}",
                            request.query_id,
                            request.dataset_key.as_str(),
                            work.key,
                            error.kind,
                            error.message,
                            started.elapsed().as_millis()
                        );
                        if let Some(metrics) = request.metrics.as_ref() {
                            metrics.recorder.record_durable_write(
                                &metrics.labels,
                                MetricsDurableWriteOutcome::StorageError,
                            );
                            if is_storage_error(&error.kind) {
                                metrics
                                    .recorder
                                    .record_storage_error(&ErrorLabels::from_labels(
                                        &metrics.labels,
                                        error.kind,
                                    ));
                            }
                        }
                        return;
                    }
                }
            }
            log::info!(
                "durable promotion completed query_id={} dataset={} key={} data_objects={} empty_coverages={} staged_ranges={} skipped_ranges={} duration_ms={}",
                request.query_id,
                request.dataset_key.as_str(),
                work.key,
                result.data_objects.len(),
                result.empty_coverages.len(),
                result.staged_ranges.len(),
                result.skipped_ranges.len(),
                started.elapsed().as_millis()
            );
            if let Some(metrics) = request.metrics.as_ref() {
                metrics
                    .recorder
                    .record_durable_write(&metrics.labels, metrics_durable_write_outcome(&result));
            }
        }
        Err(error) => {
            log::error!(
                "durable promotion failed query_id={} dataset={} key={} kind={:?} message={} duration_ms={}",
                request.query_id,
                request.dataset_key.as_str(),
                work.key,
                error.kind,
                error.message,
                started.elapsed().as_millis()
            );
            if let Some(metrics) = request.metrics.as_ref() {
                metrics.recorder.record_durable_write(
                    &metrics.labels,
                    MetricsDurableWriteOutcome::StorageError,
                );
                if is_storage_error(&error.kind) {
                    metrics
                        .recorder
                        .record_storage_error(&ErrorLabels::from_labels(
                            &metrics.labels,
                            error.kind,
                        ));
                }
            }
        }
    }
}

fn spawn_intent_worker<R, A>(
    worker_index: usize,
    writer: DurableWriter<R>,
    adapter: A,
    shared: IntentWorkerShared,
) -> Result<(), DatalensError>
where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    thread::Builder::new()
        .name(format!("datalens-durable-intent-{worker_index}"))
        .spawn(move || {
            let worker_chain = adapter.capabilities().chain().clone();
            spawn_startup_maintenance_once(
                shared.repository.clone(),
                shared.startup_maintenance_once.as_ref(),
                shared.terminal_retention_seconds,
                shared.cleanup_max_scan,
                shared.cleanup_max_deletes,
                shared.cleanup_interval,
            );
            let mut last_backlog_metric_sample: Option<Instant> = None;
            loop {
                let now = unix_seconds_now();
                let record_backlog_metrics = last_backlog_metric_sample.is_none_or(|sampled| {
                    sampled.elapsed() >= DEFAULT_INTENT_BACKLOG_METRIC_INTERVAL
                });
                let intents = {
                    let _claim = match shared.claim_lock.lock() {
                        Ok(claim) => claim,
                        Err(_) => return,
                    };
                    claim_pending_intents(
                        shared.repository.as_ref(),
                        &worker_chain,
                        shared.metrics.as_ref(),
                        now,
                        worker_index,
                        record_backlog_metrics,
                        shared.claim_batch_size,
                    )
                };
                if record_backlog_metrics {
                    last_backlog_metric_sample = Some(Instant::now());
                }
                if intents.is_empty() {
                    thread::sleep(DEFAULT_INTENT_POLL_INTERVAL);
                    continue;
                }
                run_intent_batch_work(
                    shared.repository.as_ref(),
                    &writer,
                    &adapter,
                    &intents,
                    shared.metrics.as_ref(),
                    &shared.provider_ranges,
                );
            }
        })
        .map(|_| ())
        .map_err(|error| {
            DatalensError::new(
                datalens_core::DatalensErrorKind::Internal,
                format!("start durable intent worker: {error}"),
            )
        })
}

fn run_intent_batch_work<R, A>(
    repository: &dyn DurablePromotionIntentRepository,
    writer: &DurableWriter<R>,
    adapter: &A,
    intents: &[DurablePromotionIntent],
    metrics: Option<&Arc<MetricsRecorder>>,
    provider_ranges: &ProviderRangeController,
) where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    if intents.len() <= 1 {
        if let Some(intent) = intents.first() {
            run_intent_work(
                repository,
                writer,
                adapter,
                intent,
                metrics,
                provider_ranges,
            );
        }
        return;
    }
    let mut batch = intents[0].clone();
    batch.intent_id = intents
        .iter()
        .map(|intent| intent.intent_id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    batch.ranges = batch_ranges(intents);
    let started = Instant::now();
    log::info!(
        "durable intent promotion batch started count={} chain_key={} dataset={} selector_fingerprint={} ranges={}",
        intents.len(),
        batch.chain.key_prefix(),
        batch.dataset_key.as_str(),
        batch.selector_fingerprint,
        intent_ranges_label(&batch.ranges)
    );
    let result = promote_intent(writer, adapter, &batch, provider_ranges);
    for intent in intents {
        match &result {
            Ok(()) => {
                if let Err(error) = repository.mark_completed(&intent.intent_id, unix_seconds_now())
                {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                        started,
                    );
                    log::error!(
                        "durable intent batch completion mark failed intent_id={} kind={:?} message={}",
                        intent.intent_id,
                        error.kind,
                        error.message
                    );
                } else {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Completed,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Completed,
                        started,
                    );
                }
            }
            Err(error) => mark_intent_failure(repository, intent, error, metrics, started),
        }
    }
    match result {
        Ok(()) => log::info!(
            "durable intent promotion batch completed count={} duration_ms={}",
            intents.len(),
            started.elapsed().as_millis()
        ),
        Err(error) => log::error!(
            "durable intent promotion batch failed count={} kind={:?} message={} duration_ms={}",
            intents.len(),
            error.kind,
            error.message,
            started.elapsed().as_millis()
        ),
    }
}

fn spawn_startup_maintenance_once(
    repository: Arc<dyn DurablePromotionIntentRepository>,
    startup_maintenance_once: &Once,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
    cleanup_interval: Duration,
) {
    startup_maintenance_once.call_once(|| {
        if let Err(error) = thread::Builder::new()
            .name("datalens-durable-intent-maintenance".to_owned())
            .spawn(move || {
                run_startup_maintenance(
                    repository.as_ref(),
                    terminal_retention_seconds,
                    cleanup_max_scan,
                    cleanup_max_deletes,
                );
                run_terminal_cleanup_periodically(
                    repository.as_ref(),
                    terminal_retention_seconds,
                    cleanup_max_scan,
                    cleanup_max_deletes,
                    cleanup_interval,
                );
            })
        {
            log::error!("durable intent startup maintenance spawn failed error={error}");
        }
    });
}

fn run_intent_work<R, A>(
    repository: &dyn DurablePromotionIntentRepository,
    writer: &DurableWriter<R>,
    adapter: &A,
    intent: &DurablePromotionIntent,
    metrics: Option<&Arc<MetricsRecorder>>,
    provider_ranges: &ProviderRangeController,
) where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    let started = Instant::now();
    log::info!(
        "durable intent promotion started intent_id={} source={:?} chain_key={} dataset={} selector_fingerprint={} ranges={} attempt={}",
        intent.intent_id,
        intent.source,
        intent.chain.key_prefix(),
        intent.dataset_key.as_str(),
        intent.selector_fingerprint,
        intent_ranges_label(&intent.ranges),
        intent.attempt_count + 1
    );
    let result = promote_intent(writer, adapter, intent, provider_ranges);
    match result {
        Ok(()) => match repository.mark_completed(&intent.intent_id, unix_seconds_now()) {
            Ok(_) => {
                record_intent_worker_metric(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::Completed,
                );
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::Completed,
                    started,
                );
                log::info!(
                    "durable intent promotion completed intent_id={} duration_ms={}",
                    intent.intent_id,
                    started.elapsed().as_millis()
                );
            }
            Err(error) => {
                record_intent_worker_metric(metrics, intent, MetricsDurableIntentOutcome::Error);
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::Error,
                    started,
                );
                log::error!(
                    "durable intent completion mark failed intent_id={} kind={:?} message={} duration_ms={}",
                    intent.intent_id,
                    error.kind,
                    error.message,
                    started.elapsed().as_millis()
                );
            }
        },
        Err(error) => {
            let now = unix_seconds_now();
            if error.kind.is_retryable() {
                let retry_delay = intent_retry_delay_seconds(intent);
                let next_retry = now.saturating_add(retry_delay);
                let mark_result = repository.mark_retryable_failure(
                    &intent.intent_id,
                    &error.message,
                    now,
                    next_retry,
                );
                if let Err(mark_error) = mark_result {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                        started,
                    );
                    log::error!(
                        "durable intent retry mark failed intent_id={} kind={:?} message={} original_kind={:?} original_message={}",
                        intent.intent_id,
                        mark_error.kind,
                        mark_error.message,
                        error.kind,
                        error.message
                    );
                } else {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::RetryableFailed,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::RetryableFailed,
                        started,
                    );
                }
                log::error!(
                    "durable intent promotion failed intent_id={} kind={:?} message={} retry_delay_seconds={} retry_at={} duration_ms={}",
                    intent.intent_id,
                    error.kind,
                    error.message,
                    retry_delay,
                    next_retry,
                    started.elapsed().as_millis()
                );
            } else {
                let mark_result =
                    repository.mark_terminal_failure(&intent.intent_id, &error.message, now);
                if let Err(mark_error) = mark_result {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::Error,
                        started,
                    );
                    log::error!(
                        "durable intent terminal mark failed intent_id={} kind={:?} message={} original_kind={:?} original_message={}",
                        intent.intent_id,
                        mark_error.kind,
                        mark_error.message,
                        error.kind,
                        error.message
                    );
                } else {
                    record_intent_worker_metric(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::TerminalFailed,
                    );
                    observe_intent_worker_duration(
                        metrics,
                        intent,
                        MetricsDurableIntentOutcome::TerminalFailed,
                        started,
                    );
                }
                log::error!(
                    "durable intent promotion terminal failure intent_id={} kind={:?} message={} duration_ms={}",
                    intent.intent_id,
                    error.kind,
                    error.message,
                    started.elapsed().as_millis()
                );
            }
        }
    }
}

fn record_intent_worker_metric(
    metrics: Option<&Arc<MetricsRecorder>>,
    intent: &DurablePromotionIntent,
    outcome: MetricsDurableIntentOutcome,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(intent.application.clone()),
        intent.chain.clone(),
        intent.dataset_key.clone(),
    );
    metrics.record_durable_intent(&labels, intent_source_label(intent.source), outcome);
}

fn observe_intent_worker_duration(
    metrics: Option<&Arc<MetricsRecorder>>,
    intent: &DurablePromotionIntent,
    outcome: MetricsDurableIntentOutcome,
    started: Instant,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named(intent.application.clone()),
        intent.chain.clone(),
        intent.dataset_key.clone(),
    );
    metrics.observe_durable_intent_duration(
        &labels,
        intent_source_label(intent.source),
        outcome,
        started.elapsed().as_secs_f64(),
    );
}

fn mark_intent_failure(
    repository: &dyn DurablePromotionIntentRepository,
    intent: &DurablePromotionIntent,
    error: &DatalensError,
    metrics: Option<&Arc<MetricsRecorder>>,
    started: Instant,
) {
    let now = unix_seconds_now();
    if error.kind.is_retryable() {
        let retry_delay = intent_retry_delay_seconds(intent);
        let next_retry = now.saturating_add(retry_delay);
        match repository.mark_retryable_failure(&intent.intent_id, &error.message, now, next_retry)
        {
            Ok(_) => {
                record_intent_worker_metric(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::RetryableFailed,
                );
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::RetryableFailed,
                    started,
                );
            }
            Err(mark_error) => {
                record_intent_worker_metric(metrics, intent, MetricsDurableIntentOutcome::Error);
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::Error,
                    started,
                );
                log::error!(
                    "durable intent retry mark failed intent_id={} kind={:?} message={} original_kind={:?} original_message={}",
                    intent.intent_id,
                    mark_error.kind,
                    mark_error.message,
                    error.kind,
                    error.message
                );
            }
        }
    } else {
        match repository.mark_terminal_failure(&intent.intent_id, &error.message, now) {
            Ok(_) => {
                record_intent_worker_metric(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::TerminalFailed,
                );
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::TerminalFailed,
                    started,
                );
            }
            Err(mark_error) => {
                record_intent_worker_metric(metrics, intent, MetricsDurableIntentOutcome::Error);
                observe_intent_worker_duration(
                    metrics,
                    intent,
                    MetricsDurableIntentOutcome::Error,
                    started,
                );
                log::error!(
                    "durable intent terminal mark failed intent_id={} kind={:?} message={} original_kind={:?} original_message={}",
                    intent.intent_id,
                    mark_error.kind,
                    mark_error.message,
                    error.kind,
                    error.message
                );
            }
        }
    }
}

fn record_intent_backlog_metric(
    metrics: Option<&Arc<MetricsRecorder>>,
    backlog: &[DurablePromotionIntentBacklog],
) {
    let Some(metrics) = metrics else {
        return;
    };
    for scope in backlog {
        metrics.set_durable_intent_backlog_for_scope(
            &scope.chain,
            intent_source_label(scope.source),
            scope.pending_total,
            scope.oldest_pending_age_seconds,
        );
    }
}

fn record_intent_claim_metric(
    metrics: Option<&Arc<MetricsRecorder>>,
    chain: &ChainIdentity,
    source: &str,
    outcome: MetricsDurableIntentClaimOutcome,
    started: Instant,
) {
    let Some(metrics) = metrics else {
        return;
    };
    metrics.record_durable_intent_claim(chain, source, outcome);
    metrics.observe_durable_intent_claim_duration(
        chain,
        source,
        outcome,
        started.elapsed().as_secs_f64(),
    );
}

fn observe_intent_claim_duration_metric(
    metrics: Option<&Arc<MetricsRecorder>>,
    chain: &ChainIdentity,
    source: &str,
    outcome: MetricsDurableIntentClaimOutcome,
    started: Instant,
) {
    let Some(metrics) = metrics else {
        return;
    };
    metrics.observe_durable_intent_claim_duration(
        chain,
        source,
        outcome,
        started.elapsed().as_secs_f64(),
    );
}

fn intent_source_label(source: DurablePromotionIntentSource) -> &'static str {
    match source {
        DurablePromotionIntentSource::Query => "query",
        DurablePromotionIntentSource::Warmup => "warmup",
    }
}

fn intent_retry_delay_seconds(intent: &DurablePromotionIntent) -> u64 {
    let shift = intent.attempt_count.min(10);
    let exponential = DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS
        .saturating_mul(1_u64 << shift)
        .min(DEFAULT_INTENT_RETRY_MAX_DELAY_SECONDS);
    exponential.saturating_add(intent_retry_jitter_seconds(intent, exponential))
}

fn intent_retry_jitter_seconds(intent: &DurablePromotionIntent, delay_seconds: u64) -> u64 {
    let jitter_cap = DEFAULT_INTENT_RETRY_MAX_JITTER_SECONDS.min(delay_seconds / 5);
    if jitter_cap == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    intent.intent_id.hash(&mut hasher);
    intent.attempt_count.hash(&mut hasher);
    hasher.finish() % (jitter_cap + 1)
}

fn promote_intent<R, A>(
    writer: &DurableWriter<R>,
    adapter: &A,
    intent: &DurablePromotionIntent,
    provider_ranges: &ProviderRangeController,
) -> Result<(), DatalensError>
where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    let finality_level = finality_from_intent(&intent.finality)?;
    if intent
        .ranges
        .iter()
        .all(|range| durable_covered(writer, intent, range).unwrap_or(false))
    {
        return Ok(());
    }

    let mut segments = Vec::new();
    for range in &intent.ranges {
        if durable_covered(writer, intent, range)? {
            continue;
        }
        let request = ChainFetchRequest::new(
            intent.chain.clone(),
            intent.dataset_key.clone(),
            range.clone(),
            intent.selector.clone(),
        )
        .with_context(FetchContext {
            request_id: Some(intent.intent_id.clone()),
            cache_write: true,
        });
        for (request, response) in
            fetch_intent_with_provider_limit_splits(adapter, request, intent, provider_ranges)?
        {
            response.validate_for_request(&request)?;
            segments.push(DurableWriteSegment {
                range: request.range,
                rows: response.rows,
            });
        }
    }
    if segments.is_empty() {
        return Ok(());
    }
    let mut result = writer.write(DurableWriteRequest {
        chain: intent.chain.clone(),
        dataset_key: intent.dataset_key.clone(),
        selector: intent.selector.clone(),
        finality_level,
        segments,
    })?;
    if !result.staged_ranges.is_empty() {
        let staged_ranges = result.staged_ranges.clone();
        let flush_result = writer.flush_ranges(
            &intent.chain,
            &intent.dataset_key,
            &intent.selector,
            &staged_ranges,
        )?;
        result.data_objects.extend(flush_result.data_objects);
        result.empty_coverages.extend(flush_result.empty_coverages);
        result.skipped_ranges.extend(flush_result.skipped_ranges);
        result.staged_ranges.clear();
    }
    for range in &intent.ranges {
        if !durable_covered(writer, intent, range)? {
            return Err(DatalensError::new(
                datalens_core::DatalensErrorKind::StorageWriteFailure,
                format!(
                    "durable coverage not visible after promotion {}:{}-{}",
                    range_kind_key(range.kind()),
                    range.start(),
                    range.end()
                ),
            ));
        }
    }
    Ok(())
}

fn durable_covered<R>(
    writer: &DurableWriter<R>,
    intent: &DurablePromotionIntent,
    range: &datalens_core::LedgerRange,
) -> Result<bool, DatalensError>
where
    R: StorageRepository + Clone + 'static,
{
    let covered = writer.storage().covered_ranges(
        &intent.chain,
        &intent.dataset_key,
        &intent.selector,
        range.clone(),
    )?;
    Ok(covered
        .iter()
        .any(|covered_range| covered_range.intersection(range) == Some(range.clone())))
}

fn claim_pending_intents(
    repository: &dyn DurablePromotionIntentRepository,
    chain: &ChainIdentity,
    metrics: Option<&Arc<MetricsRecorder>>,
    now: u64,
    worker_index: usize,
    record_backlog_metrics: bool,
    claim_batch_size: usize,
) -> Vec<DurablePromotionIntent> {
    let Some(first) = claim_pending_intent(
        repository,
        chain,
        metrics,
        now,
        worker_index,
        record_backlog_metrics,
        claim_batch_size,
    ) else {
        return Vec::new();
    };
    let pending = match repository.list_pending_for_chain_and_source(
        chain,
        first.source,
        now,
        claim_batch_size,
    ) {
        Ok(pending) => pending,
        Err(error) => {
            log::warn!(
                "durable intent batch list failed worker={} kind={:?} message={}",
                worker_index,
                error.kind,
                error.message
            );
            return vec![first];
        }
    };
    let mut claimed = vec![first.clone()];
    for candidate in pending {
        if candidate.intent_id == first.intent_id {
            continue;
        }
        if !intent_batch_compatible(&first, &candidate) {
            continue;
        }
        if !ranges_adjacent_to_batch(&claimed, &candidate) {
            continue;
        }
        let source = intent_source_label(candidate.source);
        let claim_started = Instant::now();
        match repository.mark_running(&candidate.intent_id, unix_seconds_now()) {
            Ok(Some(intent)) => {
                record_intent_claim_metric(
                    metrics,
                    chain,
                    source,
                    MetricsDurableIntentClaimOutcome::Claimed,
                    claim_started,
                );
                claimed.push(intent);
            }
            Ok(None) => record_intent_claim_metric(
                metrics,
                chain,
                source,
                MetricsDurableIntentClaimOutcome::SkippedIneligible,
                claim_started,
            ),
            Err(error) => {
                record_intent_claim_metric(
                    metrics,
                    chain,
                    source,
                    MetricsDurableIntentClaimOutcome::MarkRunningError,
                    claim_started,
                );
                log::error!(
                    "durable intent batch claim failed worker={} intent_id={} kind={:?} message={}",
                    worker_index,
                    candidate.intent_id,
                    error.kind,
                    error.message
                );
            }
        }
    }
    claimed
}

fn claim_pending_intent(
    repository: &dyn DurablePromotionIntentRepository,
    chain: &ChainIdentity,
    metrics: Option<&Arc<MetricsRecorder>>,
    now: u64,
    worker_index: usize,
    record_backlog_metrics: bool,
    claim_batch_size: usize,
) -> Option<DurablePromotionIntent> {
    if record_backlog_metrics {
        match repository.pending_backlog_for_chain(chain, now) {
            Ok(backlog) => record_intent_backlog_metric(metrics, &backlog),
            Err(error) => {
                log::warn!(
                    "durable intent backlog metric failed worker={} kind={:?} message={}",
                    worker_index,
                    error.kind,
                    error.message
                );
            }
        }
    }
    for source in intent_claim_source_order(worker_index) {
        let source_label = intent_source_label(source);
        let list_started = Instant::now();
        let pending = match repository.list_pending_for_chain_and_source(
            chain,
            source,
            now,
            claim_batch_size,
        ) {
            Ok(pending) => {
                let outcome = if pending.is_empty() {
                    MetricsDurableIntentClaimOutcome::Empty
                } else {
                    MetricsDurableIntentClaimOutcome::Claimed
                };
                if pending.is_empty() {
                    record_intent_claim_metric(metrics, chain, source_label, outcome, list_started);
                } else {
                    observe_intent_claim_duration_metric(
                        metrics,
                        chain,
                        source_label,
                        outcome,
                        list_started,
                    );
                }
                pending
            }
            Err(error) => {
                record_intent_claim_metric(
                    metrics,
                    chain,
                    source_label,
                    MetricsDurableIntentClaimOutcome::ListError,
                    list_started,
                );
                log::error!(
                    "durable intent list failed worker={} source={} kind={:?} message={}",
                    worker_index,
                    source_label,
                    error.kind,
                    error.message
                );
                continue;
            }
        };
        for intent in pending {
            let source = intent_source_label(intent.source);
            let claim_started = Instant::now();
            match repository.mark_running(&intent.intent_id, unix_seconds_now()) {
                Ok(Some(intent)) => {
                    record_intent_claim_metric(
                        metrics,
                        chain,
                        source,
                        MetricsDurableIntentClaimOutcome::Claimed,
                        claim_started,
                    );
                    return Some(intent);
                }
                Ok(None) => {
                    record_intent_claim_metric(
                        metrics,
                        chain,
                        source,
                        MetricsDurableIntentClaimOutcome::SkippedIneligible,
                        claim_started,
                    );
                }
                Err(error) => {
                    record_intent_claim_metric(
                        metrics,
                        chain,
                        source,
                        MetricsDurableIntentClaimOutcome::MarkRunningError,
                        claim_started,
                    );
                    log::error!(
                        "durable intent claim failed worker={} intent_id={} kind={:?} message={}",
                        worker_index,
                        intent.intent_id,
                        error.kind,
                        error.message
                    );
                }
            }
        }
    }
    None
}

fn intent_claim_source_order(worker_index: usize) -> [DurablePromotionIntentSource; 2] {
    if worker_index.is_multiple_of(2) {
        [
            DurablePromotionIntentSource::Warmup,
            DurablePromotionIntentSource::Query,
        ]
    } else {
        [
            DurablePromotionIntentSource::Query,
            DurablePromotionIntentSource::Warmup,
        ]
    }
}

fn intent_batch_compatible(
    first: &DurablePromotionIntent,
    candidate: &DurablePromotionIntent,
) -> bool {
    first.chain == candidate.chain
        && first.dataset_key == candidate.dataset_key
        && first.selector_fingerprint == candidate.selector_fingerprint
        && first.selector_canonical_key == candidate.selector_canonical_key
        && first.finality == candidate.finality
        && first
            .ranges
            .first()
            .zip(candidate.ranges.first())
            .is_some_and(|(left, right)| left.kind() == right.kind())
}

fn ranges_adjacent_to_batch(
    claimed: &[DurablePromotionIntent],
    candidate: &DurablePromotionIntent,
) -> bool {
    let mut ranges = batch_ranges(claimed);
    let candidate_ranges = normalized_intent_ranges(candidate);
    ranges.extend(candidate_ranges.clone());
    ranges.sort_by_key(|range| (range.start(), range.end()));
    for pair in ranges.windows(2) {
        if pair[1].start() > pair[0].end().saturating_add(1) {
            return false;
        }
    }
    !candidate_ranges.is_empty()
}

fn batch_ranges(intents: &[DurablePromotionIntent]) -> Vec<datalens_core::LedgerRange> {
    let mut ranges = intents
        .iter()
        .flat_map(normalized_intent_ranges)
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| (range.start(), range.end()));
    let mut merged: Vec<datalens_core::LedgerRange> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && last.kind() == range.kind()
            && range.start() <= last.end().saturating_add(1)
        {
            let end = last.end().max(range.end());
            *last = datalens_core::LedgerRange::try_new(last.kind(), last.start(), end)
                .expect("merged intent range remains valid");
            continue;
        }
        merged.push(range);
    }
    merged
}

fn normalized_intent_ranges(intent: &DurablePromotionIntent) -> Vec<datalens_core::LedgerRange> {
    let mut ranges = intent.ranges.clone();
    ranges.sort_by_key(|range| (range.start(), range.end()));
    ranges
}

fn fetch_intent_with_provider_limit_splits<A>(
    adapter: &A,
    fetch_request: ChainFetchRequest,
    intent: &DurablePromotionIntent,
    provider_ranges: &ProviderRangeController,
) -> Result<Vec<(ChainFetchRequest, datalens_chain::ChainFetchResponse)>, DatalensError>
where
    A: ChainAdapter,
{
    let capability_max_len =
        dataset_capability_max_range_len(&adapter.capabilities(), &fetch_request.dataset_key);
    let range_key = ProviderRangeKey::from_request(&fetch_request);
    let effective_max_len = provider_ranges.effective_limit(&range_key, capability_max_len);
    let initial_requests = split_fetch_request_by_max_len(&fetch_request, effective_max_len)?;
    if initial_requests.len() > 1 {
        log::info!(
            "durable intent provider fetch pre-split intent_id={} dataset={} range={}-{} target_max_len={:?} chunks={}",
            intent.intent_id,
            fetch_request.dataset_key.as_str(),
            fetch_request.range.start(),
            fetch_request.range.end(),
            effective_max_len,
            initial_requests.len()
        );
    }
    let mut responses = Vec::new();
    let mut queue = VecDeque::from(initial_requests);
    while let Some(fetch_request) = queue.pop_front() {
        let fetch_start = Instant::now();
        match adapter.fetch(fetch_request.clone()) {
            Ok(response) => {
                log::info!(
                    "durable intent provider fetch completed intent_id={} dataset={} range={}-{} duration_ms={}",
                    intent.intent_id,
                    fetch_request.dataset_key.as_str(),
                    fetch_request.range.start(),
                    fetch_request.range.end(),
                    fetch_start.elapsed().as_millis()
                );
                provider_ranges.record_success(
                    &range_key,
                    capability_max_len,
                    fetch_request.range.len(),
                );
                responses.push((fetch_request, response));
            }
            Err(error)
                if error.kind == DatalensErrorKind::ProviderLimit
                    && fetch_request.range.len() > 1 =>
            {
                let hint_max_len = parse_provider_limit_hint(&error.message);
                let split_target = provider_ranges.record_provider_limit(
                    &range_key,
                    capability_max_len,
                    fetch_request.range.len(),
                    hint_max_len,
                );
                let split_ranges = split_provider_limit_range(&fetch_request.range, split_target)?;
                log::warn!(
                    "durable intent provider limit split intent_id={} dataset={} range={}-{} target_max_len={:?} configured_max_len={:?} hint_max_len={:?} chunks={} duration_ms={}",
                    intent.intent_id,
                    fetch_request.dataset_key.as_str(),
                    fetch_request.range.start(),
                    fetch_request.range.end(),
                    split_target,
                    capability_max_len,
                    hint_max_len,
                    split_ranges.len(),
                    fetch_start.elapsed().as_millis()
                );
                for range in split_ranges.into_iter().rev() {
                    queue.push_front(ChainFetchRequest {
                        range,
                        ..fetch_request.clone()
                    });
                }
            }
            Err(error) => {
                log::warn!(
                    "durable intent provider fetch failed intent_id={} dataset={} range={}-{} kind={:?} duration_ms={}",
                    intent.intent_id,
                    fetch_request.dataset_key.as_str(),
                    fetch_request.range.start(),
                    fetch_request.range.end(),
                    error.kind,
                    fetch_start.elapsed().as_millis()
                );
                return Err(error);
            }
        }
    }
    Ok(responses)
}

fn promotion_key(request: &DurablePromotionRequest) -> String {
    let ranges = request
        .segments
        .iter()
        .map(|segment| {
            format!(
                "{}:{}-{}",
                range_kind_key(segment.range.kind()),
                segment.range.start(),
                segment.range.end()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{}|{}|{}|{}|{}|{}",
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        request.selector.fingerprint(),
        request.selector.canonical_key(),
        finality_key(request.finality_level),
        ranges,
    )
}

fn finality_key(finality: FinalityLevel) -> &'static str {
    match finality {
        FinalityLevel::Latest => "latest",
        FinalityLevel::Safe => "safe",
        FinalityLevel::Finalized => "finalized",
        FinalityLevel::ChainSpecific(value) => value,
    }
}

fn finality_from_intent(finality: &str) -> Result<FinalityLevel, DatalensError> {
    match finality {
        "safe" => Ok(FinalityLevel::Safe),
        "finalized" => Ok(FinalityLevel::Finalized),
        value => Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("durable intent finality {value} is not supported"),
        )),
    }
}

fn intent_ranges_label(ranges: &[datalens_core::LedgerRange]) -> String {
    ranges
        .iter()
        .map(|range| {
            format!(
                "{}:{}-{}",
                range_kind_key(range.kind()),
                range.start(),
                range.end()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn reset_stale_intents(
    repository: &dyn DurablePromotionIntentRepository,
) -> Result<(), DatalensError> {
    let now = unix_seconds_now();
    let stale_before = now.saturating_sub(DEFAULT_INTENT_STALE_RUNNING_SECONDS);
    let reset = repository.reset_stale_running(stale_before, now)?;
    if !reset.is_empty() {
        log::warn!("durable intent stale running reset count={}", reset.len());
    }
    Ok(())
}

fn run_startup_maintenance(
    repository: &dyn DurablePromotionIntentRepository,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
) {
    let started = Instant::now();
    log::info!("durable intent startup maintenance started");
    match reset_stale_intents(repository) {
        Ok(()) => {
            log::info!("durable intent stale running reset completed");
        }
        Err(error) => {
            log::error!(
                "durable intent stale running reset failed kind={:?} message={}",
                error.kind,
                error.message
            );
        }
    }
    match repository.rebuild_pending_indexes(unix_seconds_now()) {
        Ok(rebuilt) => {
            log::info!(
                "durable intent pending index rebuild completed rebuilt={} duration_ms={}",
                rebuilt,
                started.elapsed().as_millis()
            );
        }
        Err(error) => {
            log::error!(
                "durable intent pending index rebuild failed kind={:?} message={} duration_ms={}",
                error.kind,
                error.message,
                started.elapsed().as_millis()
            );
        }
    }
    run_terminal_cleanup(
        repository,
        terminal_retention_seconds,
        cleanup_max_scan,
        cleanup_max_deletes,
    );
    log::info!(
        "durable intent startup maintenance finished duration_ms={}",
        started.elapsed().as_millis()
    );
}

pub fn spawn_terminal_cleanup_once(
    repository: Arc<dyn DurablePromotionIntentRepository>,
    startup_maintenance_once: Arc<Once>,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
    cleanup_interval: Duration,
) {
    if terminal_retention_seconds.is_none() {
        return;
    }
    startup_maintenance_once.call_once(|| {
        if let Err(error) = thread::Builder::new()
            .name("datalens-durable-intent-cleanup".to_owned())
            .spawn(move || {
                run_terminal_cleanup(
                    repository.as_ref(),
                    terminal_retention_seconds,
                    cleanup_max_scan,
                    cleanup_max_deletes,
                );
                run_terminal_cleanup_periodically(
                    repository.as_ref(),
                    terminal_retention_seconds,
                    cleanup_max_scan,
                    cleanup_max_deletes,
                    cleanup_interval,
                );
            })
        {
            log::error!("durable intent terminal cleanup spawn failed error={error}");
        }
    });
}

fn run_terminal_cleanup_periodically(
    repository: &dyn DurablePromotionIntentRepository,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
    cleanup_interval: Duration,
) {
    if terminal_retention_seconds.is_none() {
        return;
    }
    let cleanup_interval = if cleanup_interval.is_zero() {
        DEFAULT_INTENT_CLEANUP_INTERVAL
    } else {
        cleanup_interval
    };
    loop {
        thread::sleep(cleanup_interval);
        run_terminal_cleanup(
            repository,
            terminal_retention_seconds,
            cleanup_max_scan,
            cleanup_max_deletes,
        );
    }
}

fn run_terminal_cleanup(
    repository: &dyn DurablePromotionIntentRepository,
    terminal_retention_seconds: Option<u64>,
    cleanup_max_scan: usize,
    cleanup_max_deletes: usize,
) {
    let Some(retention_seconds) = terminal_retention_seconds else {
        return;
    };
    let started = Instant::now();
    let cutoff = unix_seconds_now().saturating_sub(retention_seconds);
    match repository.cleanup_terminal(cutoff, cleanup_max_scan, cleanup_max_deletes) {
        Ok(cleanup) => {
            log::info!(
                "durable intent terminal cleanup completed scanned={} deleted={} stale_pending_indexes_deleted={} cutoff_unix_seconds={} duration_ms={}",
                cleanup.scanned,
                cleanup.deleted,
                cleanup.stale_pending_indexes_deleted,
                cutoff,
                started.elapsed().as_millis()
            );
        }
        Err(error) => {
            log::error!(
                "durable intent terminal cleanup failed kind={:?} message={} cutoff_unix_seconds={} duration_ms={}",
                error.kind,
                error.message,
                cutoff,
                started.elapsed().as_millis()
            );
        }
    }
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalens_chain::{
        AdapterCapabilities, ChainFetchResponse, ChainHeight, DatasetCapability, HeightRangeKind,
        SelectorKind,
    };
    use datalens_core::{BlockRange, ChainFamily, Dataset, LedgerRange, NetworkId, QueryRows};
    use datalens_storage::{CreateDurablePromotionIntent, DurablePromotionIntentCreateOutcome};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FailingListIntentRepository {
        last_limit: AtomicUsize,
    }

    struct PendingListIntentRepository {
        intent: DurablePromotionIntent,
        last_limit: AtomicUsize,
    }

    struct SampleIntentRepository {
        intents: Mutex<Vec<DurablePromotionIntent>>,
        marked_running: Mutex<Vec<String>>,
        marked_terminal: Mutex<Vec<String>>,
        cleanup_calls: Mutex<Vec<(u64, usize, usize)>>,
        last_limit: AtomicUsize,
    }

    #[derive(Clone)]
    struct LimitHintAdapter {
        calls: Arc<Mutex<Vec<BlockRange>>>,
    }

    impl LimitHintAdapter {
        fn calls(&self) -> Vec<BlockRange> {
            self.calls.lock().expect("adapter calls lock").clone()
        }

        fn clear_calls(&self) {
            self.calls.lock().expect("adapter calls lock").clear();
        }
    }

    impl ChainAdapter for LimitHintAdapter {
        fn capabilities(&self) -> AdapterCapabilities {
            AdapterCapabilities::new(ethereum_chain()).with_dataset_capability(
                DatasetCapability::new(Dataset::Logs)
                    .with_selector(SelectorKind::All)
                    .with_range(HeightRangeKind::Block)
                    .with_max_range_len(5_000)
                    .with_empty_coverage(true)
                    .with_safe_height(true)
                    .with_finalized_height(true)
                    .with_range_split(true),
            )
        }

        fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(5_000))
        }

        fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
            Ok(ChainHeight::block(5_000).with_finality(datalens_chain::FinalityKind::Safe))
        }

        fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
            let range = request.range.block_range().expect("expected block range");
            self.calls.lock().expect("adapter calls lock").push(range);
            if request.range.len() > 1_000 {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderLimit,
                    "query block range exceeds server limit, narrow your filter: 1000",
                ));
            }
            ChainFetchResponse::try_new(
                request.chain,
                request.dataset_key,
                request.range,
                request.selector,
                QueryRows::EvmLogs(Vec::new()),
            )
        }
    }

    impl DurablePromotionIntentRepository for FailingListIntentRepository {
        fn create_or_get(
            &self,
            _request: CreateDurablePromotionIntent,
        ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
            Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "unused create",
            ))
        }

        fn get(&self, _intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn list_pending(
            &self,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "intent list failed",
            ))
        }

        fn list_pending_for_chain(
            &self,
            _chain: &ChainIdentity,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "intent list failed",
            ))
        }

        fn mark_running(
            &self,
            _intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_completed(
            &self,
            _intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_retryable_failure(
            &self,
            _intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
            _next_retry_at_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_terminal_failure(
            &self,
            _intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn reset_stale_running(
            &self,
            _stale_before_unix_seconds: u64,
            _now_unix_seconds: u64,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            Ok(Vec::new())
        }
    }

    impl DurablePromotionIntentRepository for PendingListIntentRepository {
        fn create_or_get(
            &self,
            _request: CreateDurablePromotionIntent,
        ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
            Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "unused create",
            ))
        }

        fn get(&self, _intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn list_pending(
            &self,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            Ok(vec![self.intent.clone()])
        }

        fn list_pending_for_chain(
            &self,
            chain: &ChainIdentity,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            if &self.intent.chain == chain {
                Ok(vec![self.intent.clone()])
            } else {
                Ok(Vec::new())
            }
        }

        fn mark_running(
            &self,
            _intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_completed(
            &self,
            _intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_retryable_failure(
            &self,
            _intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
            _next_retry_at_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_terminal_failure(
            &self,
            _intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn reset_stale_running(
            &self,
            _stale_before_unix_seconds: u64,
            _now_unix_seconds: u64,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            Ok(Vec::new())
        }
    }

    impl DurablePromotionIntentRepository for SampleIntentRepository {
        fn create_or_get(
            &self,
            _request: CreateDurablePromotionIntent,
        ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
            Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "unused create",
            ))
        }

        fn get(&self, _intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn list_pending(
            &self,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            let intents = self.intents.lock().expect("sample intent repository lock");
            Ok(intents.iter().take(limit).cloned().collect())
        }

        fn list_pending_for_chain(
            &self,
            chain: &ChainIdentity,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            let intents = self.intents.lock().expect("sample intent repository lock");
            Ok(intents
                .iter()
                .filter(|intent| &intent.chain == chain)
                .take(limit)
                .cloned()
                .collect())
        }

        fn list_pending_for_chain_and_source(
            &self,
            chain: &ChainIdentity,
            source: DurablePromotionIntentSource,
            _now_unix_seconds: u64,
            limit: usize,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            self.last_limit.store(limit, Ordering::SeqCst);
            let intents = self.intents.lock().expect("sample intent repository lock");
            Ok(intents
                .iter()
                .filter(|intent| &intent.chain == chain && intent.source == source)
                .take(limit)
                .cloned()
                .collect())
        }

        fn mark_running(
            &self,
            intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            self.marked_running
                .lock()
                .expect("marked running lock")
                .push(intent_id.to_owned());
            let mut intents = self.intents.lock().expect("sample intent repository lock");
            let Some(intent) = intents
                .iter_mut()
                .find(|intent| intent.intent_id == intent_id)
            else {
                return Ok(None);
            };
            intent.status = datalens_storage::DurablePromotionIntentStatus::Running;
            Ok(Some(intent.clone()))
        }

        fn mark_completed(
            &self,
            _intent_id: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_retryable_failure(
            &self,
            _intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
            _next_retry_at_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            Ok(None)
        }

        fn mark_terminal_failure(
            &self,
            intent_id: &str,
            _error: &str,
            _now_unix_seconds: u64,
        ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
            self.marked_terminal
                .lock()
                .expect("marked terminal lock")
                .push(intent_id.to_owned());
            Ok(None)
        }

        fn reset_stale_running(
            &self,
            _stale_before_unix_seconds: u64,
            _now_unix_seconds: u64,
        ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
            Ok(Vec::new())
        }

        fn cleanup_terminal(
            &self,
            retention_cutoff_unix_seconds: u64,
            max_scan: usize,
            max_deletes: usize,
        ) -> Result<datalens_storage::DurablePromotionIntentCleanup, DatalensError> {
            self.cleanup_calls
                .lock()
                .expect("cleanup calls lock")
                .push((retention_cutoff_unix_seconds, max_scan, max_deletes));
            Ok(datalens_storage::DurablePromotionIntentCleanup::default())
        }
    }

    #[test]
    fn test_terminal_cleanup_skips_when_retention_disabled() {
        let repository = sample_repository(Vec::new());

        run_terminal_cleanup(&repository, None, 17, 5);

        assert!(
            repository
                .cleanup_calls
                .lock()
                .expect("cleanup calls lock")
                .is_empty()
        );
    }

    #[test]
    fn test_terminal_cleanup_uses_retention_cutoff_and_bounds() {
        let repository = sample_repository(Vec::new());
        let before = unix_seconds_now();

        run_terminal_cleanup(&repository, Some(60), 17, 5);

        let after = unix_seconds_now();
        let cleanup_calls = repository.cleanup_calls.lock().expect("cleanup calls lock");
        assert_eq!(cleanup_calls.len(), 1);
        let (cutoff, max_scan, max_deletes) = cleanup_calls[0];
        assert!(cutoff >= before.saturating_sub(60));
        assert!(cutoff <= after.saturating_sub(60));
        assert_eq!(max_scan, 17);
        assert_eq!(max_deletes, 5);
    }

    #[test]
    fn test_terminal_cleanup_once_runs_periodically() {
        let repository = Arc::new(sample_repository(Vec::new()));
        let worker_repository: Arc<dyn DurablePromotionIntentRepository> = repository.clone();
        spawn_terminal_cleanup_once(
            worker_repository,
            Arc::new(Once::new()),
            Some(60),
            17,
            5,
            Duration::from_millis(50),
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if repository
                .cleanup_calls
                .lock()
                .expect("cleanup calls lock")
                .len()
                >= 2
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "periodic terminal cleanup did not run twice"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn test_claim_intent_uses_bounded_pending_list_and_preserves_backlog_on_list_failure() {
        let repository = FailingListIntentRepository::default();
        let metrics = Arc::new(MetricsRecorder::new().expect("metrics recorder"));
        metrics.set_durable_intent_backlog_for_scope(&ethereum_chain(), "query", 7, 30);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum_chain(),
            Some(&metrics),
            123,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert!(claimed.is_none());
        assert_eq!(
            repository.last_limit.load(Ordering::SeqCst),
            DEFAULT_INTENT_CLAIM_BATCH_SIZE
        );
        let output = metrics.encode().expect("prometheus text");
        assert!(output.contains(
            r#"datalens_durable_intent_pending_total{chain="ethereum",chain_kind="evm",source="query"} 7"#
        ));
        assert!(output.contains(
            r#"datalens_durable_intent_oldest_pending_age_seconds{chain="ethereum",chain_kind="evm",source="query"} 30"#
        ));
        assert!(output.contains(
            r#"datalens_durable_intent_claim_total{chain="ethereum",chain_kind="evm",outcome="list_error",source="warmup"} 1"#
        ));
        assert!(output.contains(
            r#"datalens_durable_intent_claim_total{chain="ethereum",chain_kind="evm",outcome="list_error",source="query"} 1"#
        ));
    }

    #[test]
    fn test_claim_intent_updates_bounded_positive_backlog_sample() {
        let repository = PendingListIntentRepository {
            intent: test_intent_with_created_at(100),
            last_limit: AtomicUsize::new(0),
        };
        let metrics = Arc::new(MetricsRecorder::new().expect("metrics recorder"));

        let claimed = claim_pending_intent(
            &repository,
            &ethereum_chain(),
            Some(&metrics),
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert!(claimed.is_none());
        assert_eq!(
            repository.last_limit.load(Ordering::SeqCst),
            DEFAULT_INTENT_CLAIM_BATCH_SIZE
        );
        let output = metrics.encode().expect("prometheus text");
        assert!(output.contains("datalens_durable_intent_claim_duration_seconds"));
        assert!(output.contains(
            r#"datalens_durable_intent_claim_total{chain="ethereum",chain_kind="evm",outcome="skipped_ineligible",source="query"} 1"#
        ));
    }

    #[test]
    fn test_claim_intent_uses_configured_claim_batch_size() {
        let repository = PendingListIntentRepository {
            intent: test_intent_with_created_at(100),
            last_limit: AtomicUsize::new(0),
        };

        let claimed = claim_pending_intent(&repository, &ethereum_chain(), None, 130, 0, true, 64);

        assert!(claimed.is_none());
        assert_eq!(repository.last_limit.load(Ordering::SeqCst), 64);
    }

    #[test]
    fn test_claim_intent_skips_pending_intents_for_other_chain() {
        let ethereum = ethereum_chain();
        let lisk = lisk_chain();
        let repository = sample_repository(vec![test_intent_with_chain("intent-lisk", lisk, 100)]);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum,
            None,
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert!(claimed.is_none());
        assert_eq!(
            repository.last_limit.load(Ordering::SeqCst),
            DEFAULT_INTENT_CLAIM_BATCH_SIZE
        );
        assert!(
            repository
                .marked_running
                .lock()
                .expect("marked running lock")
                .is_empty()
        );
    }

    #[test]
    fn test_claim_intent_does_not_mark_wrong_chain_intent_terminal() {
        let ethereum = ethereum_chain();
        let lisk = lisk_chain();
        let repository = sample_repository(vec![test_intent_with_chain("intent-lisk", lisk, 100)]);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum,
            None,
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert!(claimed.is_none());
        assert!(
            repository
                .marked_terminal
                .lock()
                .expect("marked terminal lock")
                .is_empty()
        );
    }

    #[test]
    fn test_claim_intent_claims_matching_chain_after_unrelated_sample_entry() {
        let ethereum = ethereum_chain();
        let lisk = lisk_chain();
        let repository = sample_repository(vec![
            test_intent_with_chain("intent-lisk", lisk, 100),
            test_intent_with_chain("intent-ethereum", ethereum.clone(), 105),
        ]);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum,
            None,
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert_eq!(
            claimed.map(|intent| intent.intent_id),
            Some("intent-ethereum".to_owned())
        );
        assert_eq!(
            repository
                .marked_running
                .lock()
                .expect("marked running lock")
                .as_slice(),
            ["intent-ethereum"]
        );
    }

    #[test]
    fn test_claim_intent_claims_matching_chain_beyond_first_global_batch() {
        let ethereum = ethereum_chain();
        let lisk = lisk_chain();
        let mut intents = Vec::new();
        for index in 0..DEFAULT_INTENT_CLAIM_BATCH_SIZE {
            intents.push(test_intent_with_chain(
                &format!("intent-lisk-{index}"),
                lisk.clone(),
                100 + index as u64,
            ));
        }
        intents.push(test_intent_with_chain(
            "intent-ethereum",
            ethereum.clone(),
            100 + DEFAULT_INTENT_CLAIM_BATCH_SIZE as u64,
        ));
        let repository = sample_repository(intents);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum,
            None,
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert_eq!(
            claimed.map(|intent| intent.intent_id),
            Some("intent-ethereum".to_owned())
        );
    }

    #[test]
    fn test_claim_intent_claims_warmup_when_older_query_intents_fill_chain_batch() {
        let ethereum = ethereum_chain();
        let mut intents = Vec::new();
        for index in 0..DEFAULT_INTENT_CLAIM_BATCH_SIZE {
            intents.push(test_intent_with_chain(
                &format!("intent-query-{index}"),
                ethereum.clone(),
                100 + index as u64,
            ));
        }
        let mut warmup = test_intent_with_chain(
            "intent-warmup",
            ethereum.clone(),
            100 + DEFAULT_INTENT_CLAIM_BATCH_SIZE as u64,
        );
        warmup.source = DurablePromotionIntentSource::Warmup;
        intents.push(warmup);
        let repository = sample_repository(intents);

        let claimed = claim_pending_intent(
            &repository,
            &ethereum,
            None,
            130,
            0,
            true,
            DEFAULT_INTENT_CLAIM_BATCH_SIZE,
        );

        assert_eq!(
            claimed.map(|intent| intent.intent_id),
            Some("intent-warmup".to_owned())
        );
        assert_eq!(
            repository
                .marked_running
                .lock()
                .expect("marked running lock")
                .as_slice(),
            ["intent-warmup"]
        );
    }

    #[test]
    fn test_intent_retry_delay_uses_exponential_backoff_with_bounded_jitter() {
        let mut intent = test_intent_with_created_at(100);
        intent.status = datalens_storage::DurablePromotionIntentStatus::FailedRetryable;

        let first = intent_retry_delay_seconds(&intent);
        assert!(first >= DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS);
        assert!(
            first
                <= DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS
                    + DEFAULT_INTENT_RETRY_MAX_JITTER_SECONDS
                        .min(DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS / 5)
        );

        intent.attempt_count = 3;
        let later = intent_retry_delay_seconds(&intent);
        assert!(later >= DEFAULT_INTENT_RETRY_BASE_DELAY_SECONDS * 8);
        assert!(
            later
                <= DEFAULT_INTENT_RETRY_MAX_DELAY_SECONDS + DEFAULT_INTENT_RETRY_MAX_JITTER_SECONDS
        );

        intent.attempt_count = 32;
        let capped = intent_retry_delay_seconds(&intent);
        assert!(capped >= DEFAULT_INTENT_RETRY_MAX_DELAY_SECONDS);
        assert!(
            capped
                <= DEFAULT_INTENT_RETRY_MAX_DELAY_SECONDS + DEFAULT_INTENT_RETRY_MAX_JITTER_SECONDS
        );
    }

    #[test]
    fn test_batch_ranges_merges_only_adjacent_compatible_intents() {
        let first = test_intent_with_range("intent-10", 10, 10);
        let second = test_intent_with_range("intent-11", 11, 12);
        let gap = test_intent_with_range("intent-14", 14, 14);

        assert!(intent_batch_compatible(&first, &second));
        assert!(ranges_adjacent_to_batch(
            std::slice::from_ref(&first),
            &second
        ));
        assert!(!ranges_adjacent_to_batch(
            &[first.clone(), second.clone()],
            &gap
        ));
        assert_eq!(
            batch_ranges(&[first, second]),
            vec![datalens_core::LedgerRange::blocks(10, 12).expect("range")]
        );
    }

    #[test]
    fn test_fetch_intent_uses_provider_limit_hint_instead_of_repeated_halving() {
        let adapter = LimitHintAdapter {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let provider_ranges = ProviderRangeController::default();
        let intent = test_intent_with_chain("intent-provider-limit-hint", ethereum_chain(), 100);
        let request = ChainFetchRequest::new(
            ethereum_chain(),
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 5_000).expect("valid range"),
            DatasetSelector::all(),
        );

        fetch_intent_with_provider_limit_splits(&adapter, request, &intent, &provider_ranges)
            .expect("provider hint split succeeds");

        assert_eq!(
            adapter.calls(),
            vec![
                BlockRange::expect_new(1, 5_000),
                BlockRange::expect_new(1, 1_000),
                BlockRange::expect_new(1_001, 2_000),
                BlockRange::expect_new(2_001, 3_000),
                BlockRange::expect_new(3_001, 4_000),
                BlockRange::expect_new(4_001, 5_000),
            ]
        );
        assert!(
            !adapter.calls().iter().any(|range| {
                range.from_block == 1 && (range.to_block == 2_500 || range.to_block == 1_250)
            }),
            "provider limit hint should avoid 2500/1250 retry ranges"
        );
    }

    #[test]
    fn test_fetch_intent_reuses_provider_limit_hint_on_repeated_fetch() {
        let adapter = LimitHintAdapter {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let provider_ranges = ProviderRangeController::default();
        let intent =
            test_intent_with_chain("intent-provider-limit-hint-reuse", ethereum_chain(), 100);
        let request = ChainFetchRequest::new(
            ethereum_chain(),
            DatasetKey::evm_logs(),
            LedgerRange::blocks(1, 5_000).expect("valid range"),
            DatasetSelector::all(),
        );

        fetch_intent_with_provider_limit_splits(
            &adapter,
            request.clone(),
            &intent,
            &provider_ranges,
        )
        .expect("first provider hint split succeeds");
        adapter.clear_calls();
        fetch_intent_with_provider_limit_splits(&adapter, request, &intent, &provider_ranges)
            .expect("second provider hint split succeeds");

        assert_eq!(
            adapter.calls(),
            vec![
                BlockRange::expect_new(1, 1_000),
                BlockRange::expect_new(1_001, 2_000),
                BlockRange::expect_new(2_001, 3_000),
                BlockRange::expect_new(3_001, 4_000),
                BlockRange::expect_new(4_001, 5_000),
            ]
        );
    }

    fn sample_repository(intents: Vec<DurablePromotionIntent>) -> SampleIntentRepository {
        SampleIntentRepository {
            intents: Mutex::new(intents),
            marked_running: Mutex::new(Vec::new()),
            marked_terminal: Mutex::new(Vec::new()),
            cleanup_calls: Mutex::new(Vec::new()),
            last_limit: AtomicUsize::new(0),
        }
    }

    fn test_intent_with_created_at(created_at_unix_seconds: u64) -> DurablePromotionIntent {
        test_intent_with_chain("intent-a", ethereum_chain(), created_at_unix_seconds)
    }

    fn test_intent_with_chain(
        intent_id: &str,
        chain: ChainIdentity,
        created_at_unix_seconds: u64,
    ) -> DurablePromotionIntent {
        DurablePromotionIntent {
            intent_id: intent_id.to_owned(),
            dedupe_key: format!("dedupe-{intent_id}"),
            source: DurablePromotionIntentSource::Query,
            application: "app".to_owned(),
            chain,
            dataset_key: DatasetKey::evm_logs(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![],
            status: datalens_storage::DurablePromotionIntentStatus::Pending,
            attempt_count: 0,
            next_retry_at_unix_seconds: None,
            created_at_unix_seconds,
            updated_at_unix_seconds: created_at_unix_seconds,
            last_error: None,
            request_id: None,
            task_id: None,
        }
    }

    fn test_intent_with_range(intent_id: &str, start: u64, end: u64) -> DurablePromotionIntent {
        let mut intent = test_intent_with_chain(intent_id, ethereum_chain(), start);
        intent.ranges = vec![datalens_core::LedgerRange::blocks(start, end).expect("range")];
        intent
    }

    fn ethereum_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
    }

    fn lisk_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "lisk", NetworkId::numeric(1135))
    }
}
