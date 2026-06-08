use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows, LedgerRange,
    QueryRows,
};
use datalens_storage::{StorageDataObject, StorageRepository, StorageWriteRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Durable writer policy for object sizing and optional staging. Staging is a
/// write-side buffer only; manifest coverage is created by `write_direct` after
/// rows are flushed to durable storage.
pub struct DurableWriterConfig {
    pub target_object_bytes: u64,
    pub min_object_rows: usize,
    pub record_empty_coverage: bool,
    pub staging: WriteStagingConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Thresholds for holding small non-empty writes until they can form larger
/// durable objects. Empty coverage is never staged because it is metadata-only
/// authority and should become visible immediately when enabled.
pub struct WriteStagingConfig {
    pub enabled: bool,
    pub min_rows: Option<usize>,
    pub target_object_bytes: Option<u64>,
    pub max_staged_ranges: Option<usize>,
    pub max_staged_rows: Option<usize>,
    pub max_staged_age_ms: Option<u64>,
    pub flush_on_shutdown: bool,
    pub max_staged_bytes: Option<u64>,
}

impl Default for WriteStagingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_rows: None,
            target_object_bytes: None,
            max_staged_ranges: None,
            max_staged_rows: None,
            max_staged_age_ms: None,
            flush_on_shutdown: true,
            max_staged_bytes: None,
        }
    }
}

impl DurableWriterConfig {
    fn target_object_bytes(&self) -> u64 {
        self.target_object_bytes.max(1)
    }

    fn min_object_rows(&self) -> usize {
        self.min_object_rows.max(1)
    }

    fn staging_min_rows(&self) -> usize {
        self.staging
            .min_rows
            .unwrap_or(self.min_object_rows())
            .max(1)
    }

    fn staging_target_object_bytes(&self) -> u64 {
        self.staging
            .target_object_bytes
            .unwrap_or(self.target_object_bytes())
            .max(1)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWriteRequest {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub finality_level: FinalityLevel,
    pub segments: Vec<DurableWriteSegment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWriteSegment {
    pub range: LedgerRange,
    pub rows: DatasetRows,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DurableWriteResult {
    pub data_objects: Vec<DurableDataObject>,
    pub empty_coverages: Vec<LedgerRange>,
    pub skipped_ranges: Vec<LedgerRange>,
    pub staged_ranges: Vec<LedgerRange>,
    pub flush_reason: Option<WriteFlushReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFlushReason {
    RowsThreshold,
    BytesThreshold,
    RangeThreshold,
    AgeThreshold,
    Manual,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableDataObject {
    pub range: LedgerRange,
    pub object_key: String,
    pub row_count: usize,
    pub object_size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
    pub written_at_unix_seconds: u64,
}

#[derive(Clone)]
/// Converts safe/finalized fetched segments into durable storage coverage.
/// The writer may stage non-empty rows, but callers should treat returned
/// `staged_ranges` as not yet visible in manifest coverage.
pub struct DurableWriter<R> {
    storage: R,
    config: DurableWriterConfig,
    staged: Arc<Mutex<Vec<StagedWrite>>>,
}

impl<R> DurableWriter<R>
where
    R: StorageRepository,
{
    pub fn new(storage: R, config: DurableWriterConfig) -> Self {
        Self {
            storage,
            config,
            staged: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn write(&self, request: DurableWriteRequest) -> Result<DurableWriteResult, DatalensError> {
        if self.config.staging.enabled {
            return self.ingest(request);
        }
        self.write_direct(request)
    }

    pub fn flush(&self) -> Result<DurableWriteResult, DatalensError> {
        self.flush_with_reason(WriteFlushReason::Manual)
    }

    pub fn flush_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        ranges: &[LedgerRange],
    ) -> Result<DurableWriteResult, DatalensError> {
        if !self.config.staging.enabled || ranges.is_empty() {
            return Ok(DurableWriteResult::default());
        }
        self.flush_matching_with_reason(WriteFlushReason::Manual, |write| {
            write.chain == *chain
                && write.dataset_key == *dataset_key
                && write.selector == *selector
                && ranges.contains(&write.segment.range)
        })
    }

    pub fn flush_for_shutdown(&self) -> Result<DurableWriteResult, DatalensError> {
        if !self.config.staging.enabled || !self.config.staging.flush_on_shutdown {
            return Ok(DurableWriteResult::default());
        }
        self.flush_with_reason(WriteFlushReason::Shutdown)
    }

    pub fn flush_ranges_for_shutdown(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        ranges: &[LedgerRange],
    ) -> Result<DurableWriteResult, DatalensError> {
        if !self.config.staging.enabled || !self.config.staging.flush_on_shutdown {
            return Ok(DurableWriteResult::default());
        }
        if ranges.is_empty() {
            return Ok(DurableWriteResult::default());
        }
        self.flush_matching_with_reason(WriteFlushReason::Shutdown, |write| {
            write.chain == *chain
                && write.dataset_key == *dataset_key
                && write.selector == *selector
                && ranges.contains(&write.segment.range)
        })
    }

    pub fn staged_covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        let staged = self
            .staged
            .lock()
            .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
        let mut ranges = staged
            .iter()
            .filter(|write| write.finality_level.is_durable_writable())
            .filter(|write| write.matches_coverage(chain, dataset_key, selector, &range))
            .filter_map(|write| write.segment.range.intersection(&range))
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.start());
        Ok(ranges)
    }

    pub fn read_staged_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Option<DatasetRows>, DatalensError> {
        let staged = self
            .staged
            .lock()
            .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
        let mut rows = empty_query_rows(dataset_key.clone());
        for write in staged
            .iter()
            .filter(|write| write.finality_level.is_durable_writable())
            .filter(|write| write.matches_coverage(chain, dataset_key, selector, &range))
        {
            let filtered = filter_rows(write.segment.rows.clone(), range.clone());
            rows.try_append(filtered.into_rows())?;
        }
        rows.sort();
        let rows = DatasetRows::new(dataset_key.clone(), rows)?;
        if rows.row_count() == 0 {
            return Ok(None);
        }
        Ok(Some(rows))
    }

    fn flush_with_reason(
        &self,
        reason: WriteFlushReason,
    ) -> Result<DurableWriteResult, DatalensError> {
        let staged = {
            let mut staged = self
                .staged
                .lock()
                .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
            std::mem::take(&mut *staged)
        };
        if staged.is_empty() {
            return Ok(DurableWriteResult::default());
        }
        match self.flush_staged(staged.clone(), reason) {
            Ok(result) => Ok(result),
            Err(error) => {
                let mut current = self
                    .staged
                    .lock()
                    .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
                let mut retained = staged;
                retained.append(&mut *current);
                *current = retained;
                Err(error)
            }
        }
    }

    fn flush_matching_with_reason(
        &self,
        reason: WriteFlushReason,
        mut should_flush: impl FnMut(&StagedWrite) -> bool,
    ) -> Result<DurableWriteResult, DatalensError> {
        let staged = {
            let mut staged = self
                .staged
                .lock()
                .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
            let mut selected = Vec::new();
            let mut retained = Vec::new();
            for write in std::mem::take(&mut *staged) {
                if should_flush(&write) {
                    selected.push(write);
                } else {
                    retained.push(write);
                }
            }
            *staged = retained;
            selected
        };
        if staged.is_empty() {
            return Ok(DurableWriteResult::default());
        }
        match self.flush_staged(staged.clone(), reason) {
            Ok(result) => Ok(result),
            Err(error) => {
                let mut current = self
                    .staged
                    .lock()
                    .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
                let mut retained = staged;
                retained.append(&mut *current);
                *current = retained;
                Err(error)
            }
        }
    }

    fn ingest(&self, request: DurableWriteRequest) -> Result<DurableWriteResult, DatalensError> {
        let mut result = DurableWriteResult::default();
        let DurableWriteRequest {
            chain,
            dataset_key,
            selector,
            finality_level,
            segments,
        } = request;
        let mut direct_segments = Vec::new();
        let mut staged_segments = Vec::new();
        let mut staged_ranges = Vec::new();

        for segment in segments {
            if segment.rows.row_count() == 0 || rows_must_be_durable_immediately(&segment.rows) {
                // Empty coverage and adapter JSON rows must be visible through
                // manifest coverage immediately; staged JSON rows would be
                // lost across a fresh query executor before shutdown flush.
                direct_segments.push(segment);
            } else {
                staged_ranges.push(segment.range.clone());
                staged_segments.push(StagedWrite {
                    chain: chain.clone(),
                    dataset_key: dataset_key.clone(),
                    selector: selector.clone(),
                    finality_level,
                    staged_at: Instant::now(),
                    segment,
                });
            }
        }

        if !direct_segments.is_empty() {
            result.extend(self.write_direct(DurableWriteRequest {
                chain: chain.clone(),
                dataset_key: dataset_key.clone(),
                selector: selector.clone(),
                finality_level,
                segments: direct_segments,
            })?);
        }

        let (staged, reason) = {
            let mut staged = self
                .staged
                .lock()
                .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
            staged.extend(staged_segments);
            let Some(reason) = staged_ready(&self.config, &staged)? else {
                result.staged_ranges.extend(staged_ranges);
                return Ok(result);
            };
            (std::mem::take(&mut *staged), reason)
        };
        match self.flush_staged(staged.clone(), reason) {
            Ok(flush_result) => result.extend(flush_result),
            Err(error) => {
                // Put failed flush work back in front of newer staged writes so
                // retries preserve the original durable write ordering.
                let mut current = self
                    .staged
                    .lock()
                    .map_err(|_| DatalensError::internal("durable writer staging lock poisoned"))?;
                let mut retained = staged;
                retained.append(&mut *current);
                *current = retained;
                return Err(error);
            }
        }
        Ok(result)
    }

    fn flush_staged(
        &self,
        staged: Vec<StagedWrite>,
        reason: WriteFlushReason,
    ) -> Result<DurableWriteResult, DatalensError> {
        let mut result = DurableWriteResult::default();
        let mut remaining = staged;

        while !remaining.is_empty() {
            let first = remaining.remove(0);
            let mut segments = vec![first.segment];
            let mut index = 0;
            // A flush may contain many applications or selectors; only merge
            // segments that would share the same manifest key namespace.
            while index < remaining.len() {
                if remaining[index].chain == first.chain
                    && remaining[index].dataset_key == first.dataset_key
                    && remaining[index].selector == first.selector
                    && remaining[index].finality_level == first.finality_level
                {
                    segments.push(remaining.remove(index).segment);
                } else {
                    index += 1;
                }
            }
            result.extend(self.write_direct(DurableWriteRequest {
                chain: first.chain,
                dataset_key: first.dataset_key,
                selector: first.selector,
                finality_level: first.finality_level,
                segments,
            })?);
        }

        if !result.data_objects.is_empty()
            || !result.empty_coverages.is_empty()
            || !result.skipped_ranges.is_empty()
        {
            result.flush_reason = Some(reason);
        }
        Ok(result)
    }

    fn write_direct(
        &self,
        request: DurableWriteRequest,
    ) -> Result<DurableWriteResult, DatalensError> {
        let DurableWriteRequest {
            chain,
            dataset_key,
            selector,
            finality_level,
            segments,
        } = request;

        let mut result = DurableWriteResult::default();
        let mut pending: Option<DurableWriteSegment> = None;
        let context = WriteContext {
            storage: &self.storage,
            config: &self.config,
            chain: &chain,
            dataset_key: &dataset_key,
            selector: &selector,
            finality_level,
        };

        for segment in segments {
            if segment.rows.dataset_key() != &dataset_key {
                return Err(DatalensError::new(
                    DatalensErrorKind::Internal,
                    "fetched segment dataset key does not match durable write request",
                ));
            }

            if segment.rows.row_count() == 0 {
                flush_pending(&context, &mut pending, &mut result)?;
                write_segment(&context, segment, &mut result)?;
                continue;
            }

            let Some(current) = pending.take() else {
                pending = Some(segment);
                continue;
            };

            if should_merge(&self.config, &current, &segment)? {
                pending = Some(merge_segments(current, segment)?);
            } else {
                write_segment(&context, current, &mut result)?;
                pending = Some(segment);
            }
        }

        flush_pending(&context, &mut pending, &mut result)?;

        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StagedWrite {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector: DatasetSelector,
    finality_level: FinalityLevel,
    staged_at: Instant,
    segment: DurableWriteSegment,
}

impl StagedWrite {
    fn matches_coverage(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: &LedgerRange,
    ) -> bool {
        self.chain == *chain
            && self.dataset_key == *dataset_key
            && self.selector == *selector
            && self.segment.range.kind() == range.kind()
    }
}

impl DurableWriteResult {
    fn extend(&mut self, other: Self) {
        self.data_objects.extend(other.data_objects);
        self.empty_coverages.extend(other.empty_coverages);
        self.skipped_ranges.extend(other.skipped_ranges);
        self.staged_ranges.extend(other.staged_ranges);
        if other.flush_reason.is_some() {
            self.flush_reason = other.flush_reason;
        }
    }
}

fn staged_ready(
    config: &DurableWriterConfig,
    staged: &[StagedWrite],
) -> Result<Option<WriteFlushReason>, DatalensError> {
    let row_count = staged
        .iter()
        .map(|write| write.segment.rows.row_count())
        .sum::<usize>();
    if row_count >= config.staging_min_rows()
        || config
            .staging
            .max_staged_rows
            .is_some_and(|max_rows| row_count >= max_rows.max(1))
    {
        return Ok(Some(WriteFlushReason::RowsThreshold));
    }
    let mut staged_bytes = 0_u64;
    for write in staged {
        let object_bytes = estimated_object_bytes(&write.segment.rows)?;
        staged_bytes = staged_bytes.saturating_add(object_bytes);
        if object_bytes >= config.staging_target_object_bytes() {
            return Ok(Some(WriteFlushReason::BytesThreshold));
        }
    }
    if config
        .staging
        .max_staged_bytes
        .is_some_and(|max_bytes| staged_bytes >= max_bytes.max(1))
    {
        return Ok(Some(WriteFlushReason::BytesThreshold));
    }
    if config
        .staging
        .max_staged_ranges
        .is_some_and(|max_ranges| staged.len() >= max_ranges.max(1))
    {
        return Ok(Some(WriteFlushReason::RangeThreshold));
    }
    if let Some(max_age_ms) = config.staging.max_staged_age_ms {
        let max_age = Duration::from_millis(max_age_ms);
        if staged
            .iter()
            .any(|write| write.staged_at.elapsed() >= max_age)
        {
            return Ok(Some(WriteFlushReason::AgeThreshold));
        }
    }
    Ok(None)
}

struct WriteContext<'a, R> {
    storage: &'a R,
    config: &'a DurableWriterConfig,
    chain: &'a ChainIdentity,
    dataset_key: &'a DatasetKey,
    selector: &'a DatasetSelector,
    finality_level: FinalityLevel,
}

fn flush_pending<R>(
    context: &WriteContext<'_, R>,
    pending: &mut Option<DurableWriteSegment>,
    result: &mut DurableWriteResult,
) -> Result<(), DatalensError>
where
    R: StorageRepository,
{
    if let Some(segment) = pending.take() {
        write_segment(context, segment, result)?;
    }
    Ok(())
}

fn write_segment<R>(
    context: &WriteContext<'_, R>,
    segment: DurableWriteSegment,
    result: &mut DurableWriteResult,
) -> Result<(), DatalensError>
where
    R: StorageRepository,
{
    let outcome = context.storage.write_rows(StorageWriteRequest {
        chain: context.chain,
        dataset_key: context.dataset_key.clone(),
        selector: context.selector,
        range: segment.range.clone(),
        rows: &segment.rows,
        finality_level: context.finality_level,
        record_empty_coverage: context.config.record_empty_coverage,
    })?;

    if let Some(data_object) = outcome.data_object {
        result
            .data_objects
            .push(data_object_result(outcome.range, data_object));
    } else if outcome.recorded_empty_coverage {
        result.empty_coverages.push(outcome.range);
    } else {
        result.skipped_ranges.push(outcome.range);
    }
    Ok(())
}

fn data_object_result(range: LedgerRange, data_object: StorageDataObject) -> DurableDataObject {
    DurableDataObject {
        range,
        object_key: data_object.object_key,
        row_count: data_object.row_count,
        object_size_bytes: data_object.object_size_bytes,
        checksum: data_object.checksum,
        checksum_algorithm: data_object.checksum_algorithm,
        written_at_unix_seconds: data_object.written_at_unix_seconds,
    }
}

fn should_merge(
    config: &DurableWriterConfig,
    current: &DurableWriteSegment,
    next: &DurableWriteSegment,
) -> Result<bool, DatalensError> {
    if current.range.kind() != next.range.kind()
        || current.range.end().checked_add(1) != Some(next.range.start())
        || current.rows.dataset_key() != next.rows.dataset_key()
        || current.rows.row_count() == 0
        || next.rows.row_count() == 0
    {
        return Ok(false);
    }
    if estimated_object_bytes(&current.rows)? >= config.target_object_bytes() {
        return Ok(false);
    }
    if current.rows.row_count() >= config.min_object_rows()
        && estimated_object_bytes_after_merge(current, next)? > config.target_object_bytes()
    {
        return Ok(false);
    }
    Ok(true)
}

fn merge_segments(
    current: DurableWriteSegment,
    next: DurableWriteSegment,
) -> Result<DurableWriteSegment, DatalensError> {
    let dataset_key = current.rows.dataset_key().clone();
    let mut rows = current.rows.into_rows();
    rows.try_append(next.rows.into_rows())?;
    rows.sort();
    Ok(DurableWriteSegment {
        range: LedgerRange::try_new(
            current.range.kind(),
            current.range.start(),
            next.range.end(),
        )?,
        rows: DatasetRows::new(dataset_key, rows)?,
    })
}

fn estimated_object_bytes(rows: &DatasetRows) -> Result<u64, DatalensError> {
    match rows.rows() {
        QueryRows::EvmBlocks(_)
        | QueryRows::EvmTransactions(_)
        | QueryRows::EvmReceipts(_)
        | QueryRows::EvmLogs(_)
        | QueryRows::AdapterJson { .. } => serde_json::to_vec(rows)
            .map(|bytes| bytes.len() as u64)
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("estimate durable object bytes: {error}"),
                )
            }),
    }
}

fn rows_must_be_durable_immediately(rows: &DatasetRows) -> bool {
    matches!(rows.rows(), QueryRows::AdapterJson { .. })
}

fn estimated_object_bytes_after_merge(
    current: &DurableWriteSegment,
    next: &DurableWriteSegment,
) -> Result<u64, DatalensError> {
    let dataset_key = current.rows.dataset_key().clone();
    let mut rows = current.rows.clone().into_rows();
    rows.try_append(next.rows.clone().into_rows())?;
    rows.sort();
    estimated_object_bytes(&DatasetRows::new(dataset_key, rows)?)
}

fn empty_query_rows(dataset_key: DatasetKey) -> QueryRows {
    match dataset_key.evm_dataset() {
        Some(Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
        Some(Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
        Some(Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key,
            rows: Vec::new(),
        },
    }
}

fn filter_rows(rows: DatasetRows, range: LedgerRange) -> DatasetRows {
    let dataset_key = rows.dataset_key().clone();
    let Some(block_range) = range.block_range() else {
        return rows;
    };
    let rows = match rows.into_rows() {
        QueryRows::EvmBlocks(rows) => QueryRows::EvmBlocks(
            rows.into_iter()
                .filter(|row| block_range.contains(row.number))
                .collect(),
        ),
        QueryRows::EvmTransactions(rows) => QueryRows::EvmTransactions(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::EvmReceipts(rows) => QueryRows::EvmReceipts(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::EvmLogs(rows) => QueryRows::EvmLogs(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::AdapterJson { dataset_key, rows } => {
            QueryRows::AdapterJson { dataset_key, rows }
        }
    };
    DatasetRows::new(dataset_key, rows).expect("filtered rows keep dataset key")
}
