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

use crate::helpers::{is_storage_error, metrics_durable_write_outcome};

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
        startup_maintenance_once: Arc<Once>,
    ) -> Result<Self, DatalensError> {
        let claim_lock = Arc::new(Mutex::new(()));
        for worker_index in 0..DEFAULT_INTENT_WORKERS {
            spawn_intent_worker(
                worker_index,
                repository.clone(),
                writer.clone(),
                adapter.clone(),
                metrics.clone(),
                claim_lock.clone(),
                startup_maintenance_once.clone(),
            )?;
        }
        Ok(Self {
            _storage: std::marker::PhantomData,
            _adapter: std::marker::PhantomData,
        })
    }
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
    repository: Arc<dyn DurablePromotionIntentRepository>,
    writer: DurableWriter<R>,
    adapter: A,
    metrics: Option<Arc<MetricsRecorder>>,
    claim_lock: Arc<Mutex<()>>,
    startup_maintenance_once: Arc<Once>,
) -> Result<(), DatalensError>
where
    R: StorageRepository + Clone + 'static,
    A: ChainAdapter,
{
    thread::Builder::new()
        .name(format!("datalens-durable-intent-{worker_index}"))
        .spawn(move || {
            let worker_chain = adapter.capabilities().chain().clone();
            spawn_startup_maintenance_once(repository.clone(), startup_maintenance_once.as_ref());
            let mut last_backlog_metric_sample: Option<Instant> = None;
            loop {
                let now = unix_seconds_now();
                let record_backlog_metrics = last_backlog_metric_sample.is_none_or(|sampled| {
                    sampled.elapsed() >= DEFAULT_INTENT_BACKLOG_METRIC_INTERVAL
                });
                let intent = {
                    let _claim = match claim_lock.lock() {
                        Ok(claim) => claim,
                        Err(_) => return,
                    };
                    claim_pending_intent(
                        repository.as_ref(),
                        &worker_chain,
                        metrics.as_ref(),
                        now,
                        worker_index,
                        record_backlog_metrics,
                    )
                };
                if record_backlog_metrics {
                    last_backlog_metric_sample = Some(Instant::now());
                }
                let Some(intent) = intent else {
                    thread::sleep(DEFAULT_INTENT_POLL_INTERVAL);
                    continue;
                };
                run_intent_work(
                    repository.as_ref(),
                    &writer,
                    &adapter,
                    &intent,
                    metrics.as_ref(),
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

fn spawn_startup_maintenance_once(
    repository: Arc<dyn DurablePromotionIntentRepository>,
    startup_maintenance_once: &Once,
) {
    startup_maintenance_once.call_once(|| {
        if let Err(error) = thread::Builder::new()
            .name("datalens-durable-intent-maintenance".to_owned())
            .spawn(move || run_startup_maintenance(repository.as_ref()))
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
    let result = promote_intent(writer, adapter, intent);
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
            fetch_intent_with_provider_limit_splits(adapter, request, intent)?
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

fn claim_pending_intent(
    repository: &dyn DurablePromotionIntentRepository,
    chain: &ChainIdentity,
    metrics: Option<&Arc<MetricsRecorder>>,
    now: u64,
    worker_index: usize,
    record_backlog_metrics: bool,
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
    let list_started = Instant::now();
    let pending =
        match repository.list_pending_for_chain(chain, now, DEFAULT_INTENT_CLAIM_BATCH_SIZE) {
            Ok(pending) => {
                let outcome = if pending.is_empty() {
                    MetricsDurableIntentClaimOutcome::Empty
                } else {
                    MetricsDurableIntentClaimOutcome::Claimed
                };
                if pending.is_empty() {
                    record_intent_claim_metric(metrics, chain, "all", outcome, list_started);
                } else {
                    observe_intent_claim_duration_metric(
                        metrics,
                        chain,
                        "all",
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
                    "all",
                    MetricsDurableIntentClaimOutcome::ListError,
                    list_started,
                );
                log::error!(
                    "durable intent list failed worker={} kind={:?} message={}",
                    worker_index,
                    error.kind,
                    error.message
                );
                return None;
            }
        };
    match pending.into_iter().next() {
        Some(intent) => {
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
                    Some(intent)
                }
                Ok(None) => {
                    record_intent_claim_metric(
                        metrics,
                        chain,
                        source,
                        MetricsDurableIntentClaimOutcome::SkippedIneligible,
                        claim_started,
                    );
                    None
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
                    None
                }
            }
        }
        None => None,
    }
}

fn fetch_intent_with_provider_limit_splits<A>(
    adapter: &A,
    fetch_request: ChainFetchRequest,
    intent: &DurablePromotionIntent,
) -> Result<Vec<(ChainFetchRequest, datalens_chain::ChainFetchResponse)>, DatalensError>
where
    A: ChainAdapter,
{
    let mut responses = Vec::new();
    let mut queue = VecDeque::from([fetch_request]);
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
                responses.push((fetch_request, response));
            }
            Err(error)
                if error.kind == DatalensErrorKind::ProviderLimit
                    && fetch_request.range.len() > 1 =>
            {
                log::warn!(
                    "durable intent provider limit split intent_id={} dataset={} range={}-{} duration_ms={}",
                    intent.intent_id,
                    fetch_request.dataset_key.as_str(),
                    fetch_request.range.start(),
                    fetch_request.range.end(),
                    fetch_start.elapsed().as_millis()
                );
                for range in crate::helpers::split_provider_limit_range(&fetch_request.range)?
                    .into_iter()
                    .rev()
                {
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

fn run_startup_maintenance(repository: &dyn DurablePromotionIntentRepository) {
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
    log::info!(
        "durable intent startup maintenance finished duration_ms={}",
        started.elapsed().as_millis()
    );
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
    use datalens_core::{ChainFamily, NetworkId};
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
        last_limit: AtomicUsize,
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
    }

    #[test]
    fn test_claim_intent_uses_bounded_pending_list_and_preserves_backlog_on_list_failure() {
        let repository = FailingListIntentRepository::default();
        let metrics = Arc::new(MetricsRecorder::new().expect("metrics recorder"));
        metrics.set_durable_intent_backlog_for_scope(&ethereum_chain(), "query", 7, 30);

        let claimed =
            claim_pending_intent(&repository, &ethereum_chain(), Some(&metrics), 123, 0, true);

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
            r#"datalens_durable_intent_claim_total{chain="ethereum",chain_kind="evm",outcome="list_error",source="all"} 1"#
        ));
    }

    #[test]
    fn test_claim_intent_updates_bounded_positive_backlog_sample() {
        let repository = PendingListIntentRepository {
            intent: test_intent_with_created_at(100),
            last_limit: AtomicUsize::new(0),
        };
        let metrics = Arc::new(MetricsRecorder::new().expect("metrics recorder"));

        let claimed =
            claim_pending_intent(&repository, &ethereum_chain(), Some(&metrics), 130, 0, true);

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
    fn test_claim_intent_skips_pending_intents_for_other_chain() {
        let ethereum = ethereum_chain();
        let lisk = lisk_chain();
        let repository = sample_repository(vec![test_intent_with_chain("intent-lisk", lisk, 100)]);

        let claimed = claim_pending_intent(&repository, &ethereum, None, 130, 0, true);

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

        let claimed = claim_pending_intent(&repository, &ethereum, None, 130, 0, true);

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

        let claimed = claim_pending_intent(&repository, &ethereum, None, 130, 0, true);

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

        let claimed = claim_pending_intent(&repository, &ethereum, None, 130, 0, true);

        assert_eq!(
            claimed.map(|intent| intent.intent_id),
            Some("intent-ethereum".to_owned())
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

    fn sample_repository(intents: Vec<DurablePromotionIntent>) -> SampleIntentRepository {
        SampleIntentRepository {
            intents: Mutex::new(intents),
            marked_running: Mutex::new(Vec::new()),
            marked_terminal: Mutex::new(Vec::new()),
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

    fn ethereum_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
    }

    fn lisk_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "lisk", NetworkId::numeric(1135))
    }
}
