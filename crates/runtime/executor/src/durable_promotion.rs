use std::{
    collections::BTreeSet,
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
    time::Instant,
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{ChainIdentity, DatalensError, DatasetKey, LedgerRangeKind};
use datalens_metrics::{
    DurableWriteOutcome as MetricsDurableWriteOutcome, ErrorLabels, MetricsLabels, MetricsRecorder,
};
use datalens_storage::StorageRepository;
use datalens_writer::{DurableWriteRequest, DurableWriteSegment, DurableWriter};

use crate::helpers::{is_storage_error, metrics_durable_write_outcome};

const DEFAULT_PROMOTION_WORKERS: usize = 4;
const DEFAULT_PROMOTION_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone)]
pub(crate) struct DurablePromotionQueue<R> {
    sender: SyncSender<DurablePromotionWork>,
    state: Arc<(Mutex<DurablePromotionState>, Condvar)>,
    _storage: std::marker::PhantomData<R>,
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

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}
