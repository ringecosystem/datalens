use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatasetKey, DatasetRows, LedgerRange,
    NetworkId, QueryRows,
};
use datalens_storage::{
    DurableStorage, LocalObjectStore, MaintenanceCompactionConfig, ObjectListPage, ObjectMetadata,
    ObjectStore, StorageWriteRequest,
};

#[test]
fn test_compaction_performance_regression_harness_produces_comparable_baseline() {
    let config = HarnessConfig::ci_smoke();
    let report = run_compaction_performance_harness(config, MaintenanceCompactionConfig::default())
        .expect("run compaction performance harness");

    assert_eq!(report.before_compaction.read_rows.error_count, 0);
    assert_eq!(report.after_compaction.read_rows.error_count, 0);
    assert_eq!(report.during_compaction.total_error_count(), 0);
    assert!(report.before_compaction.read_rows.p95_micros > 0);
    assert!(report.after_compaction.read_rows.p95_micros > 0);
    assert!(report.after_compaction.object_store_ops.total() > 0);
    assert!(report.after_compaction.object_store_ops.total() <= report.config.object_op_budget);
    assert!(report.after_compaction.read_rows.p99_micros <= report.config.p99_micros_budget);
    assert_bounded_regression(
        "read_rows p99",
        report.before_compaction.read_rows.p99_micros,
        report.after_compaction.read_rows.p99_micros,
        10,
    );
    assert_bounded_regression(
        "covered_ranges p99",
        report.before_compaction.covered_ranges.p99_micros,
        report.after_compaction.covered_ranges.p99_micros,
        10,
    );
    assert_bounded_regression(
        "write_rows p99",
        report.before_compaction.write_rows.p99_micros,
        report.after_compaction.write_rows.p99_micros,
        10,
    );

    println!("{}", report.to_smoke_summary_json().replace('\n', " "));
}

#[derive(Clone, Copy, Debug)]
struct HarnessConfig {
    small_object_count: u64,
    read_iterations: usize,
    covered_iterations: usize,
    write_iterations: usize,
    p99_micros_budget: u64,
    object_op_budget: usize,
}

impl HarnessConfig {
    fn ci_smoke() -> Self {
        Self {
            small_object_count: env_u64("DATALENS_COMPACTION_HARNESS_OBJECTS", 48),
            read_iterations: env_usize("DATALENS_COMPACTION_HARNESS_READS", 24),
            covered_iterations: env_usize("DATALENS_COMPACTION_HARNESS_COVERED", 24),
            write_iterations: env_usize("DATALENS_COMPACTION_HARNESS_WRITES", 8),
            p99_micros_budget: env_u64("DATALENS_COMPACTION_HARNESS_P99_MICROS_BUDGET", 1_500_000),
            object_op_budget: env_usize("DATALENS_COMPACTION_HARNESS_OBJECT_OP_BUDGET", 3_500),
        }
    }
}

#[derive(Clone, Debug)]
struct HarnessReport {
    config: HarnessConfig,
    before_compaction: WorkloadMetrics,
    after_compaction: WorkloadMetrics,
    during_compaction: ConcurrentCompactionMetrics,
    compaction: CompactionRunMetrics,
}

impl HarnessReport {
    fn to_smoke_summary_json(&self) -> String {
        format!(
            r#"{{"small_object_count":{},"before":{},"after":{},"during":{},"compaction":{{"compacted_objects":{},"compacted_rows":{},"deleted_source_objects":{},"source_delete_failures":{},"object_store_ops":{}}}}}"#,
            self.config.small_object_count,
            self.before_compaction.to_json(),
            self.after_compaction.to_json(),
            self.during_compaction.to_json(),
            self.compaction.compacted_objects,
            self.compaction.compacted_rows,
            self.compaction.deleted_source_objects,
            self.compaction.source_delete_failures,
            self.compaction.object_store_ops.total(),
        )
    }
}

#[derive(Clone, Debug)]
struct WorkloadMetrics {
    read_rows: LatencyMetrics,
    covered_ranges: LatencyMetrics,
    write_rows: LatencyMetrics,
    object_store_ops: ObjectStoreOps,
}

impl WorkloadMetrics {
    fn to_json(&self) -> String {
        format!(
            r#"{{"read_rows":{},"covered_ranges":{},"write_rows":{},"object_store_ops":{}}}"#,
            self.read_rows.to_json(),
            self.covered_ranges.to_json(),
            self.write_rows.to_json(),
            self.object_store_ops.total(),
        )
    }
}

#[derive(Clone, Debug, Default)]
struct ConcurrentCompactionMetrics {
    phases: BTreeMap<&'static str, WorkloadMetrics>,
}

impl ConcurrentCompactionMetrics {
    fn total_error_count(&self) -> usize {
        self.phases
            .values()
            .map(|metrics| {
                metrics.read_rows.error_count
                    + metrics.covered_ranges.error_count
                    + metrics.write_rows.error_count
            })
            .sum()
    }

    fn to_json(&self) -> String {
        let phases = self
            .phases
            .iter()
            .map(|(phase, metrics)| format!(r#""{phase}":{}"#, metrics.to_json()))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"total_error_count":{},"phases":{{{phases}}}}}"#,
            self.total_error_count()
        )
    }
}

#[derive(Clone, Debug)]
struct CompactionRunMetrics {
    compacted_objects: usize,
    compacted_rows: usize,
    deleted_source_objects: usize,
    source_delete_failures: usize,
    object_store_ops: ObjectStoreOps,
}

#[derive(Clone, Debug, Default)]
struct LatencyMetrics {
    samples: usize,
    error_count: usize,
    p95_micros: u64,
    p99_micros: u64,
}

impl LatencyMetrics {
    fn to_json(&self) -> String {
        format!(
            r#"{{"samples":{},"errors":{},"p95_micros":{},"p99_micros":{}}}"#,
            self.samples, self.error_count, self.p95_micros, self.p99_micros
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ObjectStoreOps {
    get: usize,
    put: usize,
    exists: usize,
    list: usize,
    list_page: usize,
    delete: usize,
}

impl ObjectStoreOps {
    fn total(self) -> usize {
        self.get + self.put + self.exists + self.list + self.list_page + self.delete
    }
}

#[derive(Clone, Debug, Default)]
struct ObjectStoreOpCounters {
    get: Arc<AtomicUsize>,
    put: Arc<AtomicUsize>,
    exists: Arc<AtomicUsize>,
    list: Arc<AtomicUsize>,
    list_page: Arc<AtomicUsize>,
    delete: Arc<AtomicUsize>,
}

impl ObjectStoreOpCounters {
    fn snapshot(&self) -> ObjectStoreOps {
        ObjectStoreOps {
            get: self.get.load(Ordering::SeqCst),
            put: self.put.load(Ordering::SeqCst),
            exists: self.exists.load(Ordering::SeqCst),
            list: self.list.load(Ordering::SeqCst),
            list_page: self.list_page.load(Ordering::SeqCst),
            delete: self.delete.load(Ordering::SeqCst),
        }
    }

    fn delta_since(&self, before: ObjectStoreOps) -> ObjectStoreOps {
        let after = self.snapshot();
        ObjectStoreOps {
            get: after.get.saturating_sub(before.get),
            put: after.put.saturating_sub(before.put),
            exists: after.exists.saturating_sub(before.exists),
            list: after.list.saturating_sub(before.list),
            list_page: after.list_page.saturating_sub(before.list_page),
            delete: after.delete.saturating_sub(before.delete),
        }
    }
}

#[derive(Clone, Debug)]
struct InstrumentedObjectStore {
    inner: LocalObjectStore,
    counters: ObjectStoreOpCounters,
    pauses: Arc<PauseController>,
    pause_chain_prefix: String,
}

impl InstrumentedObjectStore {
    fn new(root: PathBuf, pause_chain: &ChainIdentity) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            counters: ObjectStoreOpCounters::default(),
            pauses: Arc::new(PauseController::default()),
            pause_chain_prefix: format!("chains/{}/", pause_chain.key_prefix()),
        }
    }

    fn counters(&self) -> ObjectStoreOpCounters {
        self.counters.clone()
    }

    fn pause_controller(&self) -> Arc<PauseController> {
        self.pauses.clone()
    }

    fn should_pause_key(&self, key: &str) -> bool {
        key.starts_with(&self.pause_chain_prefix)
    }
}

impl ObjectStore for InstrumentedObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.counters.get.fetch_add(1, Ordering::SeqCst);
        if self.should_pause_key(key) && key.contains("/datasets/") && !key.contains("/compacted/")
        {
            self.pauses
                .pause_if_enabled(CompactionPhase::ReadingSources);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.counters.put.fetch_add(1, Ordering::SeqCst);
        if self.should_pause_key(key) && key.contains("/datasets/") && key.contains("/compacted/") {
            self.pauses
                .pause_if_enabled(CompactionPhase::WritingCompactedObject);
        }
        if self.should_pause_key(key) && key.contains("/manifest-segments/") {
            self.pauses
                .pause_if_enabled(CompactionPhase::PublishingReplacement);
        }
        if self.should_pause_key(key) && key.contains("/metadata/compaction-superseded-sources/") {
            self.pauses
                .pause_if_enabled(CompactionPhase::RecordingSupersededSources);
        }
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.counters.exists.fetch_add(1, Ordering::SeqCst);
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.counters.list.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.counters.list_page.fetch_add(1, Ordering::SeqCst);
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.counters.delete.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CompactionPhase {
    ReadingSources,
    WritingCompactedObject,
    PublishingReplacement,
    RecordingSupersededSources,
}

impl CompactionPhase {
    fn name(self) -> &'static str {
        match self {
            Self::ReadingSources => "reading_sources",
            Self::WritingCompactedObject => "writing_compacted_object",
            Self::PublishingReplacement => "publishing_replacement",
            Self::RecordingSupersededSources => "recording_superseded_sources",
        }
    }
}

#[derive(Debug, Default)]
struct PauseController {
    state: Mutex<PauseState>,
    ready: Condvar,
    released: Condvar,
}

#[derive(Debug, Default)]
struct PauseState {
    enabled: bool,
    active: Option<CompactionPhase>,
    release_generation: u64,
    paused_counts: BTreeMap<CompactionPhase, usize>,
}

impl PauseController {
    fn enable(&self) {
        self.state.lock().expect("pause state").enabled = true;
    }

    fn pause_if_enabled(&self, phase: CompactionPhase) {
        let mut state = self.state.lock().expect("pause state");
        if !state.enabled || state.paused_counts.contains_key(&phase) {
            return;
        }
        state.paused_counts.insert(phase, 1);
        state.active = Some(phase);
        let generation = state.release_generation;
        self.ready.notify_all();
        while state.release_generation == generation {
            state = self.released.wait(state).expect("pause release");
        }
        state.active = None;
        self.ready.notify_all();
    }

    fn wait_for_phase(&self, phase: CompactionPhase) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().expect("pause state");
        while state.active != Some(phase) {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for {}", phase.name());
            let wait_for = deadline.saturating_duration_since(now);
            let (next_state, _timeout) = self
                .ready
                .wait_timeout(state, wait_for)
                .expect("wait phase");
            state = next_state;
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("pause state");
        state.release_generation += 1;
        self.released.notify_all();
    }
}

fn run_compaction_performance_harness(
    config: HarnessConfig,
    mut compaction_config: MaintenanceCompactionConfig,
) -> Result<HarnessReport, DatalensError> {
    compaction_config.min_object_bytes = u64::MAX;
    compaction_config.max_merge_ranges = config.small_object_count as usize;
    compaction_config.max_candidates_per_tick = 1;
    compaction_config.max_tick_duration_ms = 30_000;
    compaction_config.max_manifest_entries_per_tick = 50_000;
    compaction_config.delete_source_objects = true;

    let root = temp_storage_root("compaction-performance");
    let chain = test_chain();
    let store = InstrumentedObjectStore::new(root, &chain);
    let counters = store.counters();
    let storage = DurableStorage::from_object_store(store.clone());
    let selector = DatasetSelector::all();
    build_small_object_dataset(&storage, &chain, &selector, config.small_object_count)?;

    let write_chain = write_chain();
    let before_compaction = measure_workload(
        &storage,
        &counters,
        &chain,
        &write_chain,
        &selector,
        &config,
        10_000,
    )?;

    let compaction_started = counters.snapshot();
    let report = storage.compact_small_objects(compaction_config)?;
    let compaction = CompactionRunMetrics {
        compacted_objects: report.compacted_objects,
        compacted_rows: report.compacted_rows,
        deleted_source_objects: report.deleted_source_objects,
        source_delete_failures: report.source_delete_failures,
        object_store_ops: counters.delta_since(compaction_started),
    };
    let after_compaction = measure_workload(
        &storage,
        &counters,
        &chain,
        &write_chain,
        &selector,
        &config,
        20_000,
    )?;

    let during_compaction = run_concurrent_phase_harness(config, compaction_config)?;

    Ok(HarnessReport {
        config,
        before_compaction,
        after_compaction,
        during_compaction,
        compaction,
    })
}

fn run_concurrent_phase_harness(
    config: HarnessConfig,
    compaction_config: MaintenanceCompactionConfig,
) -> Result<ConcurrentCompactionMetrics, DatalensError> {
    let root = temp_storage_root("compaction-performance-concurrent");
    let chain = test_chain();
    let store = InstrumentedObjectStore::new(root, &chain);
    let counters = store.counters();
    let pauses = store.pause_controller();
    let storage = DurableStorage::from_object_store(store);
    let write_chain = write_chain();
    let selector = DatasetSelector::all();
    build_small_object_dataset(&storage, &chain, &selector, config.small_object_count)?;

    pauses.enable();
    let compactor = storage.clone();
    let handle = thread::spawn(move || compactor.compact_small_objects(compaction_config));

    let mut phases = BTreeMap::new();
    for phase in [
        CompactionPhase::ReadingSources,
        CompactionPhase::WritingCompactedObject,
        CompactionPhase::PublishingReplacement,
        CompactionPhase::RecordingSupersededSources,
    ] {
        pauses.wait_for_phase(phase);
        let metrics = measure_workload(
            &storage,
            &counters,
            &chain,
            &write_chain,
            &selector,
            &config,
            phase_write_base(phase),
        )?;
        phases.insert(phase.name(), metrics);
        pauses.release();
    }

    let report = handle.join().expect("compaction thread")?;
    assert_eq!(report.source_delete_failures, 0);
    Ok(ConcurrentCompactionMetrics { phases })
}

fn build_small_object_dataset(
    storage: &DurableStorage<InstrumentedObjectStore>,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    count: u64,
) -> Result<(), DatalensError> {
    for block in 1..=count {
        let rows = block_rows(block);
        storage.write_rows(StorageWriteRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: LedgerRange::blocks(block, block)?,
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })?;
    }
    Ok(())
}

fn measure_workload(
    storage: &DurableStorage<InstrumentedObjectStore>,
    counters: &ObjectStoreOpCounters,
    read_chain: &ChainIdentity,
    write_chain: &ChainIdentity,
    selector: &DatasetSelector,
    config: &HarnessConfig,
    write_base: u64,
) -> Result<WorkloadMetrics, DatalensError> {
    let before = counters.snapshot();
    let query_range = LedgerRange::blocks(1, config.small_object_count)?;
    let read_rows = measure_latency(config.read_iterations, || {
        storage
            .read_rows(
                read_chain,
                &DatasetKey::evm_blocks(),
                selector,
                query_range.clone(),
            )
            .map(|rows| {
                assert_eq!(rows.row_count(), config.small_object_count as usize);
            })
    });
    let covered_ranges = measure_latency(config.covered_iterations, || {
        storage
            .covered_ranges(
                read_chain,
                &DatasetKey::evm_blocks(),
                selector,
                query_range.clone(),
            )
            .map(|ranges| assert!(!ranges.is_empty()))
    });
    let write_rows = measure_latency(config.write_iterations, || {
        let offset = NEXT_WRITE_OFFSET.fetch_add(1, Ordering::SeqCst);
        let block = write_base + offset;
        let rows = block_rows(block);
        storage.write_rows(StorageWriteRequest {
            chain: write_chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: LedgerRange::blocks(block, block)?,
            rows: &rows,
            finality_level: FinalityLevel::Safe,
            record_empty_coverage: true,
        })?;
        Ok(())
    });
    Ok(WorkloadMetrics {
        read_rows,
        covered_ranges,
        write_rows,
        object_store_ops: counters.delta_since(before),
    })
}

static NEXT_WRITE_OFFSET: AtomicU64 = AtomicU64::new(1);

fn measure_latency(
    iterations: usize,
    mut operation: impl FnMut() -> Result<(), DatalensError>,
) -> LatencyMetrics {
    let mut durations = Vec::with_capacity(iterations);
    let mut error_count = 0;
    for _ in 0..iterations {
        let started = Instant::now();
        match operation() {
            Ok(()) => durations.push(started.elapsed().as_micros().max(1) as u64),
            Err(_error) => {
                error_count += 1;
                durations.push(started.elapsed().as_micros().max(1) as u64);
            }
        }
    }
    durations.sort_unstable();
    LatencyMetrics {
        samples: durations.len(),
        error_count,
        p95_micros: percentile(&durations, 95),
        p99_micros: percentile(&durations, 99),
    }
}

fn percentile(sorted_values: &[u64], percentile: usize) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let index = ((sorted_values.len() * percentile).div_ceil(100)).saturating_sub(1);
    sorted_values[index.min(sorted_values.len() - 1)]
}

fn assert_bounded_regression(name: &str, before: u64, after: u64, max_multiplier: u64) {
    assert!(
        after <= before.saturating_mul(max_multiplier).max(1),
        "{name} regressed from {before}us to {after}us"
    );
}

fn phase_write_base(phase: CompactionPhase) -> u64 {
    match phase {
        CompactionPhase::ReadingSources => 30_000,
        CompactionPhase::WritingCompactedObject => 40_000,
        CompactionPhase::PublishingReplacement => 50_000,
        CompactionPhase::RecordingSupersededSources => 60_000,
    }
}

fn block_rows(number: u64) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![BlockHeader {
            number,
            hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
            timestamp: number.saturating_mul(12),
        }]),
    )
    .expect("block rows")
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
}

fn write_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(ChainFamily::Evm, "lisk", NetworkId::numeric(1135))
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-storage-{name}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
