//! Writer boundary for normalized chunk persistence.

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetId, DatasetKey, DatasetRows,
    LedgerRange, QueryRows, TimeRange,
};
use datalens_storage::{StorageDataObject, StorageRepository, StorageWriteRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    pub chain: ChainIdentity,
    pub dataset: DatasetId,
    pub range: TimeRange,
}

impl WriteRequest {
    pub fn new(chain: ChainIdentity, dataset: DatasetId, range: TimeRange) -> Self {
        Self {
            chain,
            dataset,
            range,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteStatus {
    Persisted,
    Deferred,
}

impl WriteStatus {
    pub fn error_kind(&self) -> DatalensErrorKind {
        match self {
            Self::Persisted => DatalensErrorKind::Internal,
            Self::Deferred => DatalensErrorKind::UnsupportedDataset,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWriterConfig {
    pub target_object_bytes: u64,
    pub min_object_rows: usize,
    pub record_empty_coverage: bool,
}

impl DurableWriterConfig {
    fn target_object_bytes(&self) -> u64 {
        self.target_object_bytes.max(1)
    }

    fn min_object_rows(&self) -> usize {
        self.min_object_rows.max(1)
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
pub struct DurableWriter<R> {
    storage: R,
    config: DurableWriterConfig,
}

impl<R> DurableWriter<R>
where
    R: StorageRepository,
{
    pub fn new(storage: R, config: DurableWriterConfig) -> Self {
        Self { storage, config }
    }

    pub fn write(&self, request: DurableWriteRequest) -> Result<DurableWriteResult, DatalensError> {
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
    if current.rows.row_count() >= config.min_object_rows() {
        return Ok(false);
    }
    if estimated_object_bytes(&current.rows)? >= config.target_object_bytes() {
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
        QueryRows::EvmBlocks(_) | QueryRows::EvmLogs(_) | QueryRows::AdapterJson { .. } => {
            serde_json::to_vec(rows)
                .map(|bytes| bytes.len() as u64)
                .map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::Internal,
                        format!("estimate durable object bytes: {error}"),
                    )
                })
        }
    }
}
