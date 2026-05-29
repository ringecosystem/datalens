//! Solana chain-family adapter boundary.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, AdapterKey, CanonicalBlock, CanonicalBlockRequest, ChainAdapter,
    ChainFetchRequest, ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector,
    FinalityKind, HeightRangeKind, ProviderDiagnostics, ReorgSignal, SelectorKind, SourceMetadata,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange,
    NetworkId, QueryRows,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

mod provider;
mod rows;
pub use provider::{SolanaFixtureRpc, SolanaHttpRpc};
use rows::{
    account_update_rows, block_from_transaction, block_rows, can_fallback_selector_fetch,
    instruction_rows, selector_value, slot_rows, transaction_rows,
};

const SOLANA_ALL_KIND: &str = "solana_all";
const SOLANA_ADDRESS_KIND: &str = "solana_address";
const SOLANA_PROGRAM_KIND: &str = "solana_program";
const SOLANA_SIGNATURE_KIND: &str = "solana_signature";
const FINALIZED: SolanaCommitment = SolanaCommitment::Finalized;
const LATEST: SolanaCommitment = SolanaCommitment::Processed;
const MAX_SIGNATURE_DISCOVERY_PAGES: usize = 8;
type FinalizedBlockCache = Arc<Mutex<HashMap<(u64, u64), Vec<SolanaBlock>>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SolanaCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl SolanaCommitment {
    fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaBlock {
    pub slot: u64,
    pub block_height: Option<u64>,
    pub blockhash: String,
    pub previous_blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<u64>,
    pub transactions: Vec<SolanaTransaction>,
    pub raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaTransaction {
    pub signature: String,
    pub fee: u64,
    pub err: Option<Value>,
    pub account_keys: Vec<String>,
    pub loaded_addresses: Vec<String>,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub pre_token_balances: Vec<SolanaTokenBalance>,
    pub post_token_balances: Vec<SolanaTokenBalance>,
    pub instructions: Vec<SolanaInstruction>,
    pub inner_instructions: Vec<SolanaInnerInstructionGroup>,
    pub raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaTransactionWithSlot {
    pub slot: u64,
    pub block_time: Option<u64>,
    pub blockhash: String,
    pub transaction: SolanaTransaction,
    pub raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaSignatureInfo {
    pub signature: String,
    pub slot: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaTokenBalance {
    pub account_index: usize,
    pub mint: String,
    pub owner: Option<String>,
    pub program_id: Option<String>,
    pub amount: String,
    pub decimals: Option<u64>,
    pub ui_amount_string: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: Option<String>,
    pub parsed: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaInnerInstructionGroup {
    pub index: usize,
    pub instructions: Vec<SolanaInstruction>,
}

pub trait SolanaRpc: Clone + Send + Sync + 'static {
    fn get_slot(&self, commitment: SolanaCommitment) -> Result<u64, DatalensError>;

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        commitment: SolanaCommitment,
    ) -> Result<Vec<u64>, DatalensError>;

    fn get_block(
        &self,
        slot: u64,
        commitment: SolanaCommitment,
    ) -> Result<Option<SolanaBlock>, DatalensError>;

    fn get_signatures_for_address(
        &self,
        _address: &str,
        _before: Option<&str>,
        _until: Option<&str>,
        _limit: usize,
        _commitment: SolanaCommitment,
    ) -> Result<Vec<SolanaSignatureInfo>, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "Solana provider does not support getSignaturesForAddress",
        ))
    }

    fn get_transaction(
        &self,
        _signature: &str,
        _commitment: SolanaCommitment,
    ) -> Result<Option<SolanaTransactionWithSlot>, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "Solana provider does not support getTransaction",
        ))
    }

    fn provider_name(&self) -> &'static str;
}

#[derive(Clone)]
pub struct SolanaAdapter<P> {
    chain: ChainIdentity,
    provider: P,
    max_slot_range_len: u64,
    finalized_blocks: FinalizedBlockCache,
}

impl<P> SolanaAdapter<P>
where
    P: SolanaRpc,
{
    pub fn with_provider(chain: ChainIdentity, provider: P) -> Self {
        Self {
            chain,
            provider,
            max_slot_range_len: 64,
            finalized_blocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_max_slot_range_len(mut self, max_slot_range_len: u64) -> Self {
        self.max_slot_range_len = max_slot_range_len.max(1);
        self
    }

    fn slot_range(&self, request: &ChainFetchRequest) -> Result<LedgerRange, DatalensError> {
        if request.chain != self.chain {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "request chain is not supported by Solana adapter",
            ));
        }
        if request.range.kind() != HeightRangeKind::Slot {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Solana adapter only supports slot ranges",
            ));
        }
        if request.range.len() > u128::from(self.max_slot_range_len) {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderLimit,
                "request range exceeds Solana provider slot range limit",
            ));
        }
        Ok(request.range.clone())
    }

    fn fetch_blocks_for_range(
        &self,
        range: &LedgerRange,
    ) -> Result<(Vec<SolanaBlock>, usize), DatalensError> {
        let cache_key = (range.start(), range.end());
        if let Some(blocks) = self
            .finalized_blocks
            .lock()
            .expect("Solana finalized block cache")
            .get(&cache_key)
            .cloned()
        {
            return Ok((blocks, 1));
        }

        let slots = self.provider.get_blocks_with_limit(
            range.start(),
            range.len().min(u128::from(u64::MAX)) as u64,
            FINALIZED,
        )?;
        let mut blocks = Vec::new();
        let mut provider_calls = 1;
        for slot in slots.into_iter().filter(|slot| range.contains(*slot)) {
            provider_calls += 1;
            if let Some(block) = self.provider.get_block(slot, FINALIZED)? {
                blocks.push(block);
            }
        }
        blocks.sort_by_key(|block| block.slot);
        self.finalized_blocks
            .lock()
            .expect("Solana finalized block cache")
            .insert(cache_key, blocks.clone());
        Ok((blocks, provider_calls))
    }

    fn fetch_blocks_for_selector(
        &self,
        range: &LedgerRange,
        selector: &DatasetSelector,
    ) -> Result<Option<(Vec<SolanaBlock>, usize)>, DatalensError> {
        if let Some(signature) = selector_value(selector, SOLANA_SIGNATURE_KIND, "signature/") {
            let Some(transaction) = self.provider.get_transaction(signature, FINALIZED)? else {
                return Ok(Some((Vec::new(), 1)));
            };
            if !range.contains(transaction.slot) {
                return Ok(Some((Vec::new(), 1)));
            }
            return Ok(Some((vec![block_from_transaction(transaction)], 1)));
        }

        let address = selector_value(selector, SOLANA_ADDRESS_KIND, "address/")
            .or_else(|| selector_value(selector, SOLANA_PROGRAM_KIND, "program/"));
        let Some(address) = address else {
            return Ok(None);
        };

        let mut before = None;
        let mut signatures = Vec::new();
        let mut provider_calls = 0;
        let mut signature_pages = 0;
        loop {
            if signature_pages >= MAX_SIGNATURE_DISCOVERY_PAGES {
                return Err(DatalensError::new(
                    DatalensErrorKind::ProviderLimit,
                    "Solana signature discovery page limit reached",
                ));
            }
            let previous_before = before.clone();
            provider_calls += 1;
            signature_pages += 1;
            let page = self.provider.get_signatures_for_address(
                address,
                before.as_deref(),
                None,
                1_000,
                FINALIZED,
            )?;
            if page.is_empty() {
                break;
            }
            let reached_before_range = page.iter().any(|entry| entry.slot < range.start());
            before = page.last().map(|entry| entry.signature.clone());
            signatures.extend(
                page.into_iter()
                    .filter(|entry| range.contains(entry.slot))
                    .map(|entry| entry.signature),
            );
            if reached_before_range || before.is_none() || before == previous_before {
                break;
            }
        }

        signatures.sort();
        signatures.dedup();
        let mut blocks = Vec::new();
        for signature in signatures {
            provider_calls += 1;
            if let Some(transaction) = self.provider.get_transaction(&signature, FINALIZED)?
                && range.contains(transaction.slot)
            {
                blocks.push(block_from_transaction(transaction));
            }
        }
        blocks.sort_by_key(|block| block.slot);
        Ok(Some((blocks, provider_calls)))
    }

    fn metadata(
        &self,
        request: &ChainFetchRequest,
        calls: usize,
        warnings: Vec<String>,
    ) -> (SourceMetadata, ProviderDiagnostics) {
        (
            SourceMetadata {
                provider: self.provider.provider_name().to_owned(),
                request_id: request.context.request_id.clone(),
            },
            ProviderDiagnostics {
                calls,
                rows_scanned: 0,
                warnings,
            },
        )
    }
}

impl SolanaAdapter<SolanaFixtureRpc> {
    pub fn with_fixture_defaults() -> Self {
        Self::with_provider(default_solana_chain(), SolanaFixtureRpc).with_max_slot_range_len(3)
    }

    pub fn with_provider_limits(provider: SolanaFixtureRpc, max_slot_range_len: u64) -> Self {
        Self::with_provider(default_solana_chain(), provider)
            .with_max_slot_range_len(max_slot_range_len)
    }
}

impl<P> ChainAdapter for SolanaAdapter<P>
where
    P: SolanaRpc,
{
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone())
            .with_dataset_capability(solana_all_slot_capability(
                DatasetKey::solana_slots(),
                self.max_slot_range_len,
            ))
            .with_dataset_capability(solana_all_slot_capability(
                DatasetKey::solana_blocks(),
                self.max_slot_range_len,
            ))
            .with_dataset_capability(
                DatasetCapability::new(DatasetKey::solana_transactions())
                    .with_selector(SelectorKind::All)
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_ADDRESS_KIND)))
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_PROGRAM_KIND)))
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_SIGNATURE_KIND)))
                    .with_range(HeightRangeKind::Slot)
                    .with_max_range_len(self.max_slot_range_len)
                    .with_empty_coverage(true)
                    .with_finalized_height(true)
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(DatasetKey::solana_instructions())
                    .with_selector(SelectorKind::All)
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_PROGRAM_KIND)))
                    .with_range(HeightRangeKind::Slot)
                    .with_max_range_len(self.max_slot_range_len)
                    .with_empty_coverage(true)
                    .with_finalized_height(true)
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(DatasetKey::solana_account_updates())
                    .with_selector(SelectorKind::All)
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_ALL_KIND)))
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_ADDRESS_KIND)))
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_PROGRAM_KIND)))
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_SIGNATURE_KIND)))
                    .with_range(HeightRangeKind::Slot)
                    .with_max_range_len(self.max_slot_range_len)
                    .with_empty_coverage(true)
                    .with_finalized_height(true)
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::slot(self.provider.get_slot(LATEST)?).with_finality(FinalityKind::Latest))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        self.finalized_height()
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::slot(self.provider.get_slot(FINALIZED)?)
            .with_finality(FinalityKind::Finalized))
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        if request.chain != self.chain || request.range_kind != HeightRangeKind::Slot {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Solana canonical lookup only supports slots for this chain",
            ));
        }
        let block = self
            .provider
            .get_block(request.height, FINALIZED)?
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "provider returned no finalized block for slot",
                )
            })?;
        Ok(CanonicalBlock {
            chain: request.chain,
            height: block.slot,
            hash: block.blockhash,
            parent_hash: block.previous_blockhash,
            finality: FinalityKind::Finalized,
        })
    }

    fn reorg_signal(
        &self,
        range_kind: HeightRangeKind,
        height: u64,
    ) -> Result<ReorgSignal, DatalensError> {
        if range_kind != HeightRangeKind::Slot {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "Solana reorg signals only support slots",
            ));
        }
        let block = self.provider.get_block(height, FINALIZED)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "provider returned no finalized block for slot",
            )
        })?;
        Ok(ReorgSignal::slot(
            block.slot,
            block.blockhash,
            block.previous_blockhash,
            block.block_time,
        ))
    }

    fn latest_reorg_signal(&self) -> Result<ReorgSignal, DatalensError> {
        let latest = self.latest_height()?.value;
        let block = self.provider.get_block(latest, LATEST)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "provider returned no latest block for slot",
            )
        })?;
        Ok(ReorgSignal::slot(
            block.slot,
            block.blockhash,
            block.previous_blockhash,
            block.block_time,
        ))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        let range = self.slot_range(&request)?;
        let capability = self
            .capabilities()
            .dataset(&request.dataset_key)
            .cloned()
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by Solana adapter",
                )
            })?;
        if !capability.supports_selector(request.selector.kind()) {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "selector is not supported by Solana adapter",
            ));
        }

        let mut warnings = Vec::new();
        let (blocks, provider_calls) = if request.dataset_key == DatasetKey::solana_slots()
            || request.dataset_key == DatasetKey::solana_blocks()
        {
            self.fetch_blocks_for_range(&range)?
        } else {
            match self.fetch_blocks_for_selector(&range, &request.selector) {
                Ok(Some(result)) => result,
                Ok(None) => self.fetch_blocks_for_range(&range)?,
                Err(error) if can_fallback_selector_fetch(&error) => {
                    warnings.push(
                        "solana optimized selector fetch failed; fell back to slot scan".to_owned(),
                    );
                    self.fetch_blocks_for_range(&range)?
                }
                Err(error) => return Err(error),
            }
        };
        let rows = match &request.dataset_key {
            dataset if *dataset == DatasetKey::solana_slots() => slot_rows(&blocks),
            dataset if *dataset == DatasetKey::solana_blocks() => block_rows(&blocks),
            dataset if *dataset == DatasetKey::solana_transactions() => {
                transaction_rows(&blocks, &request.selector)
            }
            dataset if *dataset == DatasetKey::solana_instructions() => {
                instruction_rows(&blocks, &request.selector)
            }
            dataset if *dataset == DatasetKey::solana_account_updates() => {
                account_update_rows(&blocks, &request.selector)
            }
            _ => {
                return Err(DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    "dataset is not supported by Solana adapter",
                ));
            }
        };
        let rows = QueryRows::AdapterJson {
            dataset_key: request.dataset_key.clone(),
            rows,
        };
        let (source_metadata, provider_diagnostics) =
            self.metadata(&request, provider_calls, warnings);
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

fn solana_all_slot_capability(
    dataset_key: DatasetKey,
    max_slot_range_len: u64,
) -> DatasetCapability {
    DatasetCapability::new(dataset_key)
        .with_selector(SelectorKind::Other(adapter_key(SOLANA_ALL_KIND)))
        .with_selector(SelectorKind::All)
        .with_range(HeightRangeKind::Slot)
        .with_max_range_len(max_slot_range_len)
        .with_empty_coverage(true)
        .with_finalized_height(true)
        .with_range_split(true)
        .with_reorg_signals(true)
}

pub fn solana_all_selector() -> Result<DatasetSelector, DatalensError> {
    DatasetSelector::try_other(adapter_key(SOLANA_ALL_KIND), "solana-all/all", "all")
}

pub fn solana_address_selector(address: &str) -> Result<DatasetSelector, DatalensError> {
    let address = normalize_solana_key("address", address)?;
    DatasetSelector::try_other(
        adapter_key(SOLANA_ADDRESS_KIND),
        format!("solana-address/{}", digest_prefix(&address)),
        format!("address/{address}"),
    )
}

pub fn solana_program_selector(program_id: &str) -> Result<DatasetSelector, DatalensError> {
    let program_id = normalize_solana_key("program id", program_id)?;
    DatasetSelector::try_other(
        adapter_key(SOLANA_PROGRAM_KIND),
        format!("solana-program/{}", digest_prefix(&program_id)),
        format!("program/{program_id}"),
    )
}

pub fn solana_signature_selector(signature: &str) -> Result<DatasetSelector, DatalensError> {
    let signature = normalize_solana_key("signature", signature)?;
    DatasetSelector::try_other(
        adapter_key(SOLANA_SIGNATURE_KIND),
        format!("solana-signature/{}", digest_prefix(&signature)),
        format!("signature/{signature}"),
    )
}

fn adapter_key(value: &str) -> AdapterKey {
    AdapterKey::try_new(value).expect("valid Solana adapter key")
}

fn normalize_solana_key(kind: &str, value: &str) -> Result<String, DatalensError> {
    let value = value.trim();
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("Solana {kind} must be a non-empty base58-like key"),
        ));
    }
    Ok(value.to_owned())
}

fn digest_prefix(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn default_solana_chain() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(NetworkId::textual("mainnet-beta").expect("valid network id")),
    )
    .expect("valid Solana chain")
}
