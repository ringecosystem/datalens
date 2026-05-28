//! Solana chain-family adapter boundary.

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
use sha2::{Digest, Sha256};

mod provider;
pub use provider::{SolanaFixtureRpc, SolanaHttpRpc};

const SOLANA_ALL_KIND: &str = "solana_all";
const SOLANA_ADDRESS_KIND: &str = "solana_address";
const SOLANA_PROGRAM_KIND: &str = "solana_program";
const SOLANA_SIGNATURE_KIND: &str = "solana_signature";
const FINALIZED: SolanaCommitment = SolanaCommitment::Finalized;
const LATEST: SolanaCommitment = SolanaCommitment::Processed;

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaTransaction {
    pub signature: String,
    pub fee: u64,
    pub err: Option<Value>,
    pub account_keys: Vec<String>,
    pub loaded_addresses: Vec<String>,
    pub instructions: Vec<SolanaInstruction>,
    pub inner_instructions: Vec<SolanaInnerInstructionGroup>,
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

    fn provider_name(&self) -> &'static str;
}

#[derive(Clone)]
pub struct SolanaAdapter<P> {
    chain: ChainIdentity,
    provider: P,
    max_slot_range_len: u64,
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
        let slots = self.provider.get_blocks_with_limit(
            range.start(),
            range.len().min(u128::from(u64::MAX)) as u64,
            FINALIZED,
        )?;
        let mut blocks = Vec::new();
        for slot in slots.into_iter().filter(|slot| range.contains(*slot)) {
            if let Some(block) = self.provider.get_block(slot, FINALIZED)? {
                blocks.push(block);
            }
        }
        blocks.sort_by_key(|block| block.slot);
        let provider_calls = blocks.len() + 1;
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
            .with_dataset_capability(
                DatasetCapability::new(DatasetKey::solana_slots())
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_ALL_KIND)))
                    .with_range(HeightRangeKind::Slot)
                    .with_max_range_len(self.max_slot_range_len)
                    .with_empty_coverage(true)
                    .with_finalized_height(true)
                    .with_range_split(true)
                    .with_reorg_signals(true),
            )
            .with_dataset_capability(
                DatasetCapability::new(DatasetKey::solana_transactions())
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
                    .with_selector(SelectorKind::Other(adapter_key(SOLANA_PROGRAM_KIND)))
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

        let (blocks, provider_calls) = self.fetch_blocks_for_range(&range)?;
        let rows = match &request.dataset_key {
            dataset if *dataset == DatasetKey::solana_slots() => slot_rows(&blocks),
            dataset if *dataset == DatasetKey::solana_transactions() => {
                transaction_rows(&blocks, &request.selector)
            }
            dataset if *dataset == DatasetKey::solana_instructions() => {
                instruction_rows(&blocks, &request.selector)
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

fn slot_rows(blocks: &[SolanaBlock]) -> Vec<Value> {
    blocks
        .iter()
        .map(|block| {
            json!({
                "slot": block.slot,
                "range_kind": "slot",
                "block_height": block.block_height,
                "blockhash": block.blockhash,
                "previous_blockhash": block.previous_blockhash,
                "parent_slot": block.parent_slot,
                "block_time": block.block_time,
                "transaction_count": block.transactions.len(),
                "commitment": FINALIZED.as_str(),
                "reorg": {
                    "hash": block.blockhash,
                    "parent_hash": block.previous_blockhash,
                    "parent_slot": block.parent_slot,
                }
            })
        })
        .collect()
}

fn transaction_rows(blocks: &[SolanaBlock], selector: &DatasetSelector) -> Vec<Value> {
    let mut rows = Vec::new();
    for block in blocks {
        for transaction in &block.transactions {
            if !transaction_matches(transaction, selector) {
                continue;
            }
            rows.push(json!({
                "slot": block.slot,
                "range_kind": "slot",
                "signature": transaction.signature,
                "blockhash": block.blockhash,
                "err": transaction.err,
                "status": if transaction.err.is_some() { "error" } else { "ok" },
                "fee": transaction.fee,
                "account_keys": transaction.account_keys,
                "loaded_addresses": transaction.loaded_addresses,
                "selector_kind": selector_kind_name(selector),
                "commitment": FINALIZED.as_str(),
                "raw": transaction.raw,
            }));
        }
    }
    rows
}

fn instruction_rows(blocks: &[SolanaBlock], selector: &DatasetSelector) -> Vec<Value> {
    let Some(program_id) = selector_value(selector, SOLANA_PROGRAM_KIND, "program/") else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for block in blocks {
        for transaction in &block.transactions {
            for (index, instruction) in transaction.instructions.iter().enumerate() {
                if instruction.program_id == program_id {
                    rows.push(instruction_row(
                        block,
                        transaction,
                        instruction,
                        index.to_string(),
                    ));
                }
            }
            for group in &transaction.inner_instructions {
                for (inner_index, instruction) in group.instructions.iter().enumerate() {
                    if instruction.program_id == program_id {
                        rows.push(instruction_row(
                            block,
                            transaction,
                            instruction,
                            format!("{}/{}", group.index, inner_index),
                        ));
                    }
                }
            }
        }
    }
    rows
}

fn instruction_row(
    block: &SolanaBlock,
    transaction: &SolanaTransaction,
    instruction: &SolanaInstruction,
    path: String,
) -> Value {
    json!({
        "slot": block.slot,
        "range_kind": "slot",
        "signature": transaction.signature,
        "instruction_path": path,
        "program_id": instruction.program_id,
        "accounts": instruction.accounts,
        "data": instruction.data,
        "parsed": instruction.parsed,
        "blockhash": block.blockhash,
        "commitment": FINALIZED.as_str(),
    })
}

fn transaction_matches(transaction: &SolanaTransaction, selector: &DatasetSelector) -> bool {
    match selector {
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == SOLANA_PROGRAM_KIND => canonical_key
            .strip_prefix("program/")
            .is_some_and(|program_id| transaction_has_program(transaction, program_id)),
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == SOLANA_ADDRESS_KIND => canonical_key
            .strip_prefix("address/")
            .is_some_and(|address| transaction.account_keys.iter().any(|key| key == address)),
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == SOLANA_SIGNATURE_KIND => canonical_key
            .strip_prefix("signature/")
            .is_some_and(|signature| transaction.signature == signature),
        _ => false,
    }
}

fn transaction_has_program(transaction: &SolanaTransaction, program_id: &str) -> bool {
    transaction
        .instructions
        .iter()
        .chain(
            transaction
                .inner_instructions
                .iter()
                .flat_map(|group| group.instructions.iter()),
        )
        .any(|instruction| instruction.program_id == program_id)
}

fn selector_value<'a>(
    selector: &'a DatasetSelector,
    expected_kind: &str,
    prefix: &str,
) -> Option<&'a str> {
    match selector {
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == expected_kind => canonical_key.strip_prefix(prefix),
        _ => None,
    }
}

fn selector_kind_name(selector: &DatasetSelector) -> &'static str {
    match selector {
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_ADDRESS_KIND => {
            SOLANA_ADDRESS_KIND
        }
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_PROGRAM_KIND => {
            SOLANA_PROGRAM_KIND
        }
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_SIGNATURE_KIND => {
            SOLANA_SIGNATURE_KIND
        }
        _ => "unsupported",
    }
}
