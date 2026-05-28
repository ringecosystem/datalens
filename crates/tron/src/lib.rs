//! Tron chain-family adapter boundary.

use datalens_chain::{
    AdapterCapabilities, AdapterKey, CanonicalBlock, CanonicalBlockRequest, ChainAdapter,
    ChainFetchRequest, ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector,
    FinalityKind, HeightRangeKind, ProviderDiagnostics, ReorgSignal, SelectorKind, SourceMetadata,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId, QueryRows,
};
use serde_json::{Value, json};

mod provider;
pub use provider::{TronFixtureProviderRpc, TronHttpProvider};

const TRON_ALL_KIND: &str = "tron_all";
const FINALIZED: TronFinality = TronFinality::Finalized;
const LATEST: TronFinality = TronFinality::Latest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TronFinality {
    Latest,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TronBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub witness_address: Option<String>,
    pub transaction_count: usize,
    pub raw: Value,
}

pub trait TronProvider: Clone + Send + Sync + 'static {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError>;

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError>;

    fn provider_name(&self) -> &'static str;
}

#[derive(Clone)]
pub struct TronAdapter<P> {
    chain: ChainIdentity,
    provider: P,
    max_block_range_len: u64,
}

impl<P> TronAdapter<P>
where
    P: TronProvider,
{
    pub fn with_provider(chain: ChainIdentity, provider: P) -> Self {
        Self {
            chain,
            provider,
            max_block_range_len: 64,
        }
    }

    pub fn with_max_block_range_len(mut self, max_block_range_len: u64) -> Self {
        self.max_block_range_len = max_block_range_len.max(1);
        self
    }

    fn block_range(&self, request: &ChainFetchRequest) -> Result<LedgerRange, DatalensError> {
        if request.chain != self.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "request chain is not supported by Tron adapter",
            ));
        }
        if request.range.kind() != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron adapter only supports block ranges",
            ));
        }
        if request.range.len() > u128::from(self.max_block_range_len) {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderLimit,
                "request range exceeds Tron provider block range limit",
            ));
        }
        Ok(request.range.clone())
    }

    fn fetch_blocks_for_range(
        &self,
        range: &LedgerRange,
    ) -> Result<(Vec<TronBlock>, usize), DatalensError> {
        let mut blocks = Vec::new();
        for number in range.start()..=range.end() {
            if let Some(block) = self.provider.get_block_by_number(number, FINALIZED)? {
                blocks.push(block);
            }
        }
        blocks.sort_by_key(|block| block.number);
        let provider_calls = range.len().min(u128::from(usize::MAX as u64)) as usize;
        Ok((blocks, provider_calls))
    }

    fn metadata(
        &self,
        request: &ChainFetchRequest,
        calls: usize,
    ) -> (SourceMetadata, ProviderDiagnostics) {
        (
            SourceMetadata {
                provider: self.provider.provider_name().to_owned(),
                request_id: request.context.request_id.clone(),
            },
            ProviderDiagnostics {
                calls,
                rows_scanned: 0,
                warnings: Vec::new(),
            },
        )
    }
}

impl TronAdapter<TronFixtureProviderRpc> {
    pub fn with_fixture_defaults() -> Self {
        Self::with_provider(default_tron_chain(), TronFixtureProviderRpc)
            .with_max_block_range_len(3)
    }

    pub fn with_provider_limits(
        provider: TronFixtureProviderRpc,
        max_block_range_len: u64,
    ) -> Self {
        Self::with_provider(default_tron_chain(), provider)
            .with_max_block_range_len(max_block_range_len)
    }
}

impl<P> ChainAdapter for TronAdapter<P>
where
    P: TronProvider,
{
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone()).with_dataset_capability(
            DatasetCapability::new(DatasetKey::tron_blocks())
                .with_selector(SelectorKind::Other(adapter_key(TRON_ALL_KIND)))
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(self.max_block_range_len)
                .with_empty_coverage(true)
                .with_finalized_height(true)
                .with_range_split(true)
                .with_reorg_signals(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(
            ChainHeight::block(self.provider.latest_block(LATEST)?.number)
                .with_finality(FinalityKind::Latest),
        )
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        self.finalized_height()
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(
            ChainHeight::block(self.provider.latest_block(FINALIZED)?.number)
                .with_finality(FinalityKind::Finalized),
        )
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        if request.chain != self.chain || request.range_kind != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron canonical lookup only supports blocks for this chain",
            ));
        }
        let block = self
            .provider
            .get_block_by_number(request.height, FINALIZED)?
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "provider returned no finalized Tron block",
                )
            })?;
        Ok(CanonicalBlock {
            chain: request.chain,
            height: block.number,
            hash: block.hash,
            parent_hash: block.parent_hash,
            finality: FinalityKind::Finalized,
        })
    }

    fn reorg_signal(
        &self,
        range_kind: HeightRangeKind,
        height: u64,
    ) -> Result<ReorgSignal, DatalensError> {
        if range_kind != HeightRangeKind::Block {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Tron reorg signals only support blocks",
            ));
        }
        let block = self
            .provider
            .get_block_by_number(height, FINALIZED)?
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "provider returned no finalized Tron block",
                )
            })?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn latest_reorg_signal(&self) -> Result<ReorgSignal, DatalensError> {
        let block = self.provider.latest_block(LATEST)?;
        Ok(ReorgSignal::block(
            block.number,
            block.hash,
            block.parent_hash,
            Some(block.timestamp),
        ))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = self.block_range(&request)?;
        let capability = self
            .capabilities()
            .dataset(&request.dataset_key)
            .cloned()
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by Tron adapter",
                )
            })?;
        if !capability.supports_selector(request.selector.kind()) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "selector is not supported by Tron adapter",
            ));
        }
        if request.dataset_key != DatasetKey::tron_blocks() {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "dataset is not supported by Tron adapter",
            ));
        }

        let (blocks, provider_calls) = self.fetch_blocks_for_range(&range)?;
        let rows = QueryRows::AdapterJson {
            dataset_key: request.dataset_key.clone(),
            rows: block_rows(&blocks),
        };
        let (source_metadata, provider_diagnostics) = self.metadata(&request, provider_calls);
        Ok(ChainFetchResponse::try_new(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
            rows,
        )?
        .with_source_metadata(source_metadata)
        .with_provider_diagnostics(provider_diagnostics))
    }
}

pub fn tron_all_selector() -> Result<DatasetSelector, DatalensError> {
    DatasetSelector::try_other(adapter_key(TRON_ALL_KIND), "tron-all/all", "all")
}

fn adapter_key(value: &str) -> AdapterKey {
    AdapterKey::try_new(value).expect("valid Tron adapter key")
}

fn default_tron_chain() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("tron".to_owned()),
        "tron-mainnet",
        Some(NetworkId::textual("mainnet").expect("valid network id")),
    )
    .expect("valid Tron chain")
}

fn block_rows(blocks: &[TronBlock]) -> Vec<Value> {
    blocks
        .iter()
        .map(|block| {
            json!({
                "number": block.number,
                "range_kind": "block",
                "hash": block.hash,
                "parent_hash": block.parent_hash,
                "timestamp": block.timestamp,
                "witness_address": block.witness_address,
                "transaction_count": block.transaction_count,
                "finality": "finalized",
                "reorg": {
                    "hash": block.hash,
                    "parent_hash": block.parent_hash,
                },
                "source": {
                    "provider_block_id": block.hash,
                    "raw": block.raw,
                }
            })
        })
        .collect()
}
