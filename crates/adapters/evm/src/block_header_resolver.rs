use std::collections::BTreeSet;

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows,
    EvmBlockHeader, LedgerRange, QueryRows,
};
use datalens_storage::StorageRepository;
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig,
};

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

pub trait EvmBlockHeaderStore {
    fn read_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError>;

    fn persist_headers(
        &self,
        chain: &ChainIdentity,
        range: BlockRange,
        finality_level: FinalityLevel,
        headers: Vec<EvmBlockHeader>,
    ) -> Result<(), DatalensError>;
}

#[derive(Clone, Debug)]
pub struct EvmBlockHeaderResolver<F, S = NoEvmBlockHeaderStore> {
    fetcher: F,
    store: Option<S>,
}

impl<F> EvmBlockHeaderResolver<F, NoEvmBlockHeaderStore> {
    pub fn without_store(fetcher: F) -> Self {
        Self {
            fetcher,
            store: None,
        }
    }
}

impl<F, S> EvmBlockHeaderResolver<F, S> {
    pub fn with_store(fetcher: F, store: S) -> Self {
        Self {
            fetcher,
            store: Some(store),
        }
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
            Some(store) => store.read_headers(&request.chain, request.range)?,
            None => Vec::new(),
        };
        retain_range(&mut headers, request.range);

        for missing in missing_evm_block_header_ranges(request.range, &headers) {
            let fetched = self.fetcher.fetch_block_headers(missing)?;
            let mut fetched_headers = fetched.headers;
            retain_range(&mut fetched_headers, missing);
            fetched_headers.sort_by_key(|header| header.block_number);
            fetched_headers.dedup_by_key(|header| header.block_number);
            if let Some(store) = &self.store
                && !fetched_headers.is_empty()
            {
                store.persist_headers(
                    &request.chain,
                    missing,
                    request.finality_level,
                    fetched_headers.clone(),
                )?;
            }
            headers.extend(fetched_headers);
        }

        headers.sort_by_key(|header| header.block_number);
        headers.dedup_by_key(|header| header.block_number);
        let missing = missing_evm_block_header_ranges(request.range, &headers);
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
}

#[derive(Clone, Debug, Default)]
pub struct NoEvmBlockHeaderStore;

impl EvmBlockHeaderStore for NoEvmBlockHeaderStore {
    fn read_headers(
        &self,
        _chain: &ChainIdentity,
        _range: BlockRange,
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
}

impl<R> DurableEvmBlockHeaderStore<R>
where
    R: StorageRepository + Clone,
{
    pub fn new(storage: R, writer_config: DurableWriterConfig) -> Self {
        Self {
            writer: DurableWriter::new(storage.clone(), writer_config),
            storage,
        }
    }

    pub fn from_writer(writer: DurableWriter<R>) -> Self {
        Self {
            storage: writer.storage(),
            writer,
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
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        let rows = self.storage.read_rows(
            chain,
            &DatasetKey::evm_block_headers(),
            &DatasetSelector::All,
            LedgerRange::from_block_range(range),
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
