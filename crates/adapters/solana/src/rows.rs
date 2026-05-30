use datalens_chain::DatasetSelector;
use datalens_core::{DatalensError, DatalensErrorKind};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

use crate::adapter::{
    FINALIZED, SOLANA_ADDRESS_KIND, SOLANA_ALL_KIND, SOLANA_PROGRAM_KIND, SOLANA_SIGNATURE_KIND,
    SolanaBlock, SolanaInstruction, SolanaTokenBalance, SolanaTransaction,
    SolanaTransactionWithSlot,
};

pub(crate) fn slot_rows(blocks: &[SolanaBlock]) -> Vec<Value> {
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

pub(crate) fn block_rows(blocks: &[SolanaBlock]) -> Vec<Value> {
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
                "finality": "finalized",
                "raw": block.raw,
            })
        })
        .collect()
}

pub(crate) fn block_from_transaction(transaction: SolanaTransactionWithSlot) -> SolanaBlock {
    SolanaBlock {
        slot: transaction.slot,
        block_height: None,
        blockhash: transaction.blockhash,
        previous_blockhash: String::new(),
        parent_slot: transaction.slot.saturating_sub(1),
        block_time: transaction.block_time,
        transactions: vec![transaction.transaction],
        raw: transaction.raw,
    }
}

pub(crate) fn can_fallback_selector_fetch(error: &DatalensError) -> bool {
    matches!(
        error.kind,
        DatalensErrorKind::UnsupportedDataset
            | DatalensErrorKind::ProviderLimit
            | DatalensErrorKind::RateLimited
    )
}

pub(crate) fn transaction_rows(blocks: &[SolanaBlock], selector: &DatasetSelector) -> Vec<Value> {
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

pub(crate) fn instruction_rows(blocks: &[SolanaBlock], selector: &DatasetSelector) -> Vec<Value> {
    let program_id = selector_value(selector, SOLANA_PROGRAM_KIND, "program/");
    let mut rows = Vec::new();
    for block in blocks {
        for transaction in &block.transactions {
            for (index, instruction) in transaction.instructions.iter().enumerate() {
                if program_id.is_none_or(|program_id| instruction.program_id == program_id) {
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
                    if program_id.is_none_or(|program_id| instruction.program_id == program_id) {
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

pub(crate) fn account_update_rows(
    blocks: &[SolanaBlock],
    selector: &DatasetSelector,
) -> Vec<Value> {
    let mut rows = Vec::new();
    for block in blocks {
        for (transaction_index, transaction) in block.transactions.iter().enumerate() {
            if !account_update_transaction_matches(transaction, selector) {
                continue;
            }
            for row in lamport_update_rows(block, transaction, transaction_index, selector) {
                rows.push(row);
            }
            for row in token_update_rows(block, transaction, transaction_index, selector) {
                rows.push(row);
            }
        }
    }
    rows
}

fn lamport_update_rows(
    block: &SolanaBlock,
    transaction: &SolanaTransaction,
    transaction_index: usize,
    selector: &DatasetSelector,
) -> Vec<Value> {
    let account_keys = transaction_account_keys(transaction);
    let max_len = transaction
        .pre_balances
        .len()
        .min(transaction.post_balances.len())
        .min(account_keys.len());
    let mut rows = Vec::new();
    for (account_index, account) in account_keys.iter().enumerate().take(max_len) {
        let before = transaction.pre_balances[account_index];
        let after = transaction.post_balances[account_index];
        if before == after {
            continue;
        }
        if !account_update_account_matches(account, selector) {
            continue;
        }
        let delta = i64::try_from(i128::from(after) - i128::from(before))
            .unwrap_or(if after >= before { i64::MAX } else { i64::MIN });
        rows.push(json!({
            "slot": block.slot,
            "range_kind": "slot",
            "signature": transaction.signature,
            "transaction_index": transaction_index,
            "account_index": account_index,
            "account": account,
            "update_kind": "lamports",
            "lamports_before": before,
            "lamports_after": after,
            "lamports_delta": delta,
            "blockhash": block.blockhash,
            "source": "getBlock.transaction.meta",
            "selector_kind": selector_kind_name(selector),
            "commitment": FINALIZED.as_str(),
        }));
    }
    rows
}

fn token_update_rows(
    block: &SolanaBlock,
    transaction: &SolanaTransaction,
    transaction_index: usize,
    selector: &DatasetSelector,
) -> Vec<Value> {
    let account_keys = transaction_account_keys(transaction);
    let before = token_balances_by_key(&transaction.pre_token_balances);
    let after = token_balances_by_key(&transaction.post_token_balances);
    let keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for key in keys {
        let before_balance = before.get(&key);
        let after_balance = after.get(&key);
        let before_amount = before_balance
            .map(|balance| balance.amount.as_str())
            .unwrap_or("0");
        let after_amount = after_balance
            .map(|balance| balance.amount.as_str())
            .unwrap_or("0");
        if before_amount == after_amount {
            continue;
        }
        let balance = after_balance
            .or(before_balance)
            .expect("token balance exists for key");
        let account = account_keys
            .get(balance.account_index)
            .cloned()
            .unwrap_or_default();
        if !account_update_account_matches(&account, selector) {
            continue;
        }
        rows.push(json!({
            "slot": block.slot,
            "range_kind": "slot",
            "signature": transaction.signature,
            "transaction_index": transaction_index,
            "account_index": balance.account_index,
            "account": account,
            "update_kind": "spl_token",
            "mint": balance.mint,
            "owner": balance.owner,
            "program_id": balance.program_id,
            "token_amount_before": before_amount,
            "token_amount_after": after_amount,
            "token_decimals": balance.decimals,
            "token_ui_amount_before": before_balance.and_then(|balance| balance.ui_amount_string.clone()),
            "token_ui_amount_after": after_balance.and_then(|balance| balance.ui_amount_string.clone()),
            "blockhash": block.blockhash,
            "source": "getBlock.transaction.meta",
            "selector_kind": selector_kind_name(selector),
            "commitment": FINALIZED.as_str(),
            "raw_before": before_balance.map(|balance| balance.raw.clone()),
            "raw_after": after_balance.map(|balance| balance.raw.clone()),
        }));
    }
    rows
}

fn token_balances_by_key(
    balances: &[SolanaTokenBalance],
) -> BTreeMap<(usize, String), &SolanaTokenBalance> {
    balances
        .iter()
        .map(|balance| ((balance.account_index, balance.mint.clone()), balance))
        .collect()
}

fn transaction_account_keys(transaction: &SolanaTransaction) -> Vec<String> {
    transaction
        .account_keys
        .iter()
        .chain(transaction.loaded_addresses.iter())
        .cloned()
        .collect()
}

fn account_update_transaction_matches(
    transaction: &SolanaTransaction,
    selector: &DatasetSelector,
) -> bool {
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
        } if kind.as_str() == SOLANA_SIGNATURE_KIND => canonical_key
            .strip_prefix("signature/")
            .is_some_and(|signature| transaction.signature == signature),
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_ADDRESS_KIND => true,
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_ALL_KIND => true,
        DatasetSelector::All => true,
        _ => false,
    }
}

fn account_update_account_matches(account: &str, selector: &DatasetSelector) -> bool {
    match selector {
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == SOLANA_ADDRESS_KIND => canonical_key
            .strip_prefix("address/")
            .is_some_and(|address| account == address),
        _ => true,
    }
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
            .is_some_and(|address| {
                transaction_account_keys(transaction)
                    .iter()
                    .any(|key| key == address)
            }),
        DatasetSelector::Other {
            kind,
            canonical_key,
            ..
        } if kind.as_str() == SOLANA_SIGNATURE_KIND => canonical_key
            .strip_prefix("signature/")
            .is_some_and(|signature| transaction.signature == signature),
        DatasetSelector::All => true,
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

pub(crate) fn selector_value<'a>(
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
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_ALL_KIND => SOLANA_ALL_KIND,
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_ADDRESS_KIND => {
            SOLANA_ADDRESS_KIND
        }
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_PROGRAM_KIND => {
            SOLANA_PROGRAM_KIND
        }
        DatasetSelector::Other { kind, .. } if kind.as_str() == SOLANA_SIGNATURE_KIND => {
            SOLANA_SIGNATURE_KIND
        }
        DatasetSelector::All => "all",
        _ => "unsupported",
    }
}
