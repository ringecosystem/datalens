use std::{
    collections::BTreeSet,
    sync::{Arc, Condvar, Mutex},
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows,
    EvmBlockHeader, LedgerRange, QueryRows,
};
use datalens_storage::StorageRepository;
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};

const DEFAULT_EVM_BLOCK_HEADER_CHUNK_SIZE_BLOCKS: u64 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmBlockHeaderResolveRequest {
    pub chain: ChainIdentity,
    pub range: BlockRange,
    pub finality_level: FinalityLevel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmBlockHeaderFetch {
    pub range: BlockRange,
    pub headers: Vec<EvmBlockHeader>,
}

pub trait EvmBlockHeaderFetcher {
    fn fetch_block_headers(&self, range: BlockRange) -> Result<EvmBlockHeaderFetch, DatalensError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvmBlockHeaderChunkPolicy {
    chunk_size_blocks: u64,
}

impl Default for EvmBlockHeaderChunkPolicy {
    fn default() -> Self {
        Self {
            chunk_size_blocks: DEFAULT_EVM_BLOCK_HEADER_CHUNK_SIZE_BLOCKS,
        }
    }
}

impl EvmBlockHeaderChunkPolicy {
    pub fn new(chunk_size_blocks: u64) -> Self {
        Self {
            chunk_size_blocks: chunk_size_blocks.max(1),
        }
    }

    pub fn chunk_size_blocks(&self) -> u64 {
        self.chunk_size_blocks
    }

    fn aligned_ranges(&self, range: BlockRange) -> Vec<BlockRange> {
        let mut ranges = Vec::new();
        let mut chunk_start = (range.from_block / self.chunk_size_blocks) * self.chunk_size_blocks;
        loop {
            let chunk_end = chunk_start.saturating_add(self.chunk_size_blocks - 1);
            ranges.push(BlockRange::expect_new(chunk_start, chunk_end));
            if chunk_end >= range.to_block || chunk_end == u64::MAX {
                break;
            }
            chunk_start = chunk_end + 1;
        }
        ranges
    }
}

pub trait EvmBlockHeaderStore {
    fn read_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError>;

    fn persist_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
        headers: Vec<EvmBlockHeader>,
    ) -> Result<(), DatalensError>;

    fn synchronize_chunk<T>(
        &self,
        _chain: &ChainIdentity,
        _range: BlockRange,
        _finality_level: FinalityLevel,
        operation: impl FnOnce() -> Result<T, DatalensError>,
    ) -> Result<T, DatalensError> {
        operation()
    }
}

#[derive(Clone, Debug)]
pub struct EvmBlockHeaderResolver<F, S = NoEvmBlockHeaderStore> {
    fetcher: F,
    store: Option<S>,
    chunk_policy: EvmBlockHeaderChunkPolicy,
}

impl<F> EvmBlockHeaderResolver<F, NoEvmBlockHeaderStore> {
    pub fn without_store(fetcher: F) -> Self {
        Self {
            fetcher,
            store: None,
            chunk_policy: EvmBlockHeaderChunkPolicy::default(),
        }
    }
}

impl<F, S> EvmBlockHeaderResolver<F, S> {
    pub fn with_store(fetcher: F, store: S) -> Self {
        Self {
            fetcher,
            store: Some(store),
            chunk_policy: EvmBlockHeaderChunkPolicy::default(),
        }
    }

    pub fn with_chunk_policy(mut self, chunk_policy: EvmBlockHeaderChunkPolicy) -> Self {
        self.chunk_policy = chunk_policy;
        self
    }
}

impl<F, S> EvmBlockHeaderResolver<F, S>
where
    F: EvmBlockHeaderFetcher,
    S: EvmBlockHeaderStore,
{
    pub fn resolve(
        &self,
        request: EvmBlockHeaderResolveRequest,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let mut headers = match &self.store {
            Some(store) => {
                store.read_headers(&request.chain, request.range, request.finality_level)?
            }
            None => Vec::new(),
        };
        retain_range(&mut headers, request.range);

        if self.store.is_none() || !request.finality_level.is_durable_writable() {
            for missing in missing_evm_block_header_ranges(request.range, &headers) {
                let fetched = self.fetcher.fetch_block_headers(missing)?;
                let mut fetched_headers = fetched.headers;
                retain_range(&mut fetched_headers, missing);
                fetched_headers.sort_by_key(|header| header.block_number);
                fetched_headers.dedup_by_key(|header| header.block_number);
                headers.extend(fetched_headers);
            }
            return complete_resolved_headers(request.range, headers);
        }

        let store = self.store.as_ref().expect("store checked");
        for missing in missing_evm_block_header_ranges(request.range, &headers) {
            for chunk in self.chunk_policy.aligned_ranges(missing) {
                let mut chunk_headers = store.synchronize_chunk(
                    &request.chain,
                    chunk,
                    request.finality_level,
                    || {
                        let mut chunk_headers =
                            store.read_headers(&request.chain, chunk, request.finality_level)?;
                        normalize_headers(&mut chunk_headers);
                        if missing_evm_block_header_ranges(chunk, &chunk_headers).is_empty() {
                            return Ok(chunk_headers);
                        }

                        let fetched = match self.fetcher.fetch_block_headers(chunk) {
                            Ok(fetched) => fetched,
                            Err(error) if error.kind == DatalensErrorKind::ProviderFailure => {
                                let fallback_range =
                                    intersect_block_ranges(chunk, missing).expect("chunk overlaps missing range");
                                log::warn!(
                                    "EVM block header aligned chunk fetch failed; falling back to requested range chain_key={} requested_range={}-{} chunk_range={}-{} fallback_range={}-{} kind={:?} message={}",
                                    request.chain.key_prefix(),
                                    request.range.from_block,
                                    request.range.to_block,
                                    chunk.from_block,
                                    chunk.to_block,
                                    fallback_range.from_block,
                                    fallback_range.to_block,
                                    error.kind,
                                    error.message
                                );
                                return self.fetcher.fetch_block_headers(fallback_range).map(|fetched| {
                                    let mut fetched_headers = fetched.headers;
                                    retain_range(&mut fetched_headers, fallback_range);
                                    normalize_headers(&mut fetched_headers);
                                    fetched_headers
                                });
                            }
                            Err(error) => return Err(error),
                        };
                        let mut fetched_headers = fetched.headers;
                        retain_range(&mut fetched_headers, chunk);
                        normalize_headers(&mut fetched_headers);
                        if fetched_headers.is_empty()
                            || !missing_evm_block_header_ranges(chunk, &fetched_headers).is_empty()
                        {
                            return Ok(fetched_headers);
                        }

                        let mut refreshed_headers =
                            store.read_headers(&request.chain, chunk, request.finality_level)?;
                        normalize_headers(&mut refreshed_headers);
                        if missing_evm_block_header_ranges(chunk, &refreshed_headers).is_empty() {
                            return Ok(refreshed_headers);
                        }

                        store.persist_headers(
                            &request.chain,
                            chunk,
                            request.finality_level,
                            fetched_headers.clone(),
                        )?;
                        Ok(fetched_headers)
                    },
                )?;
                retain_range(&mut chunk_headers, request.range);
                headers.extend(chunk_headers);
            }
        }

        complete_resolved_headers(request.range, headers)
    }
}

fn complete_resolved_headers(
    range: BlockRange,
    mut headers: Vec<EvmBlockHeader>,
) -> Result<Vec<EvmBlockHeader>, DatalensError> {
    retain_range(&mut headers, range);
    normalize_headers(&mut headers);
    let missing = missing_evm_block_header_ranges(range, &headers);
    if !missing.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            format!(
                "failed to resolve EVM block headers for {} missing ranges",
                missing.len()
            ),
        ));
    }
    Ok(headers)
}

fn normalize_headers(headers: &mut Vec<EvmBlockHeader>) {
    headers.sort_by_key(|header| header.block_number);
    headers.dedup_by_key(|header| header.block_number);
}

fn intersect_block_ranges(left: BlockRange, right: BlockRange) -> Option<BlockRange> {
    let from_block = left.from_block.max(right.from_block);
    let to_block = left.to_block.min(right.to_block);
    (from_block <= to_block).then(|| BlockRange::expect_new(from_block, to_block))
}

#[derive(Clone, Debug, Default)]
pub struct NoEvmBlockHeaderStore;

impl EvmBlockHeaderStore for NoEvmBlockHeaderStore {
    fn read_headers(
        &self,
        _chain: &ChainIdentity,
        _range: BlockRange,
        _finality_level: FinalityLevel,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        Ok(Vec::new())
    }

    fn persist_headers(
        &self,
        _chain: &ChainIdentity,
        _range: BlockRange,
        _finality_level: FinalityLevel,
        _headers: Vec<EvmBlockHeader>,
    ) -> Result<(), DatalensError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct DurableEvmBlockHeaderStore<R> {
    storage: R,
    writer: DurableWriter<R>,
    chunk_coordinator: Arc<BlockHeaderChunkCoordinator>,
}

impl<R> DurableEvmBlockHeaderStore<R>
where
    R: StorageRepository + Clone,
{
    pub fn new(storage: R, writer_config: DurableWriterConfig) -> Self {
        Self {
            writer: DurableWriter::new(storage.clone(), writer_config),
            storage,
            chunk_coordinator: Arc::default(),
        }
    }

    pub fn from_writer(writer: DurableWriter<R>) -> Self {
        Self {
            storage: writer.storage(),
            writer,
            chunk_coordinator: Arc::default(),
        }
    }

    pub fn storage(&self) -> R {
        self.storage.clone()
    }
}

impl<R> EvmBlockHeaderStore for DurableEvmBlockHeaderStore<R>
where
    R: StorageRepository + Clone,
{
    fn read_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let rows = self.storage.read_rows_for_finality(
            chain,
            &DatasetKey::evm_block_headers(),
            &DatasetSelector::All,
            LedgerRange::from_block_range(range),
            finality_level,
        )?;
        match rows.into_rows() {
            QueryRows::EvmBlockHeaders(mut headers) => {
                retain_range(&mut headers, range);
                headers.sort_by_key(|header| header.block_number);
                headers.dedup_by_key(|header| header.block_number);
                Ok(headers)
            }
            _ => Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "storage returned non-header rows for evm.block_headers",
            )),
        }
    }

    fn persist_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
        headers: Vec<EvmBlockHeader>,
    ) -> Result<(), DatalensError> {
        if headers.is_empty() {
            return Ok(());
        }
        let dataset_key = DatasetKey::evm_block_headers();
        let rows = DatasetRows::new(dataset_key.clone(), QueryRows::EvmBlockHeaders(headers))?;
        self.writer.write(DurableWriteRequest {
            chain: chain.clone(),
            dataset_key,
            selector: DatasetSelector::All,
            finality_level,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::from_block_range(range),
                rows,
            }],
        })?;
        Ok(())
    }

    fn synchronize_chunk<T>(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
        operation: impl FnOnce() -> Result<T, DatalensError>,
    ) -> Result<T, DatalensError> {
        let _guard = self.chunk_coordinator.acquire(format!(
            "{}|{:?}|{}-{}",
            chain.key_prefix(),
            finality_level,
            range.from_block,
            range.to_block
        ));
        operation()
    }
}

#[derive(Debug, Default)]
struct BlockHeaderChunkCoordinator {
    in_flight: Mutex<BTreeSet<String>>,
    ready: Condvar,
}

impl BlockHeaderChunkCoordinator {
    fn acquire(&self, key: String) -> BlockHeaderChunkGuard<'_> {
        let mut in_flight = self
            .in_flight
            .lock()
            .expect("block header chunk coordinator");
        while in_flight.contains(&key) {
            in_flight = self
                .ready
                .wait(in_flight)
                .expect("block header chunk coordinator");
        }
        in_flight.insert(key.clone());
        BlockHeaderChunkGuard {
            coordinator: self,
            key,
        }
    }
}

struct BlockHeaderChunkGuard<'a> {
    coordinator: &'a BlockHeaderChunkCoordinator,
    key: String,
}

impl Drop for BlockHeaderChunkGuard<'_> {
    fn drop(&mut self) {
        let mut in_flight = self
            .coordinator
            .in_flight
            .lock()
            .expect("block header chunk coordinator");
        in_flight.remove(&self.key);
        self.coordinator.ready.notify_all();
    }
}

pub fn missing_evm_block_header_ranges(
    range: BlockRange,
    headers: &[EvmBlockHeader],
) -> Vec<BlockRange> {
    let present = headers
        .iter()
        .filter(|header| range.contains(header.block_number))
        .map(|header| header.block_number)
        .collect::<BTreeSet<_>>();
    let mut missing = Vec::new();
    let mut current_start = None;
    for block_number in range.from_block..=range.to_block {
        if present.contains(&block_number) {
            if let Some(start) = current_start.take() {
                missing.push(BlockRange::expect_new(start, block_number - 1));
            }
        } else if current_start.is_none() {
            current_start = Some(block_number);
        }
    }
    if let Some(start) = current_start {
        missing.push(BlockRange::expect_new(start, range.to_block));
    }
    missing
}

fn retain_range(headers: &mut Vec<EvmBlockHeader>, range: BlockRange) {
    headers.retain(|header| range.contains(header.block_number));
}
