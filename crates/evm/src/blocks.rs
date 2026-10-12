use std::collections::{HashMap, HashSet, VecDeque};

use datalens_core::{Address, B256, BlockRange, BlockRow, Finality};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use serde_json::{Value, json};

use crate::client::{BlockRef, EvmClient};
use crate::decode::{parse_block, parse_tx};

/// Sender of a mined transaction, with the block the provider reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxSender {
    pub address: Address,
    pub block_number: u64,
    pub block_hash: B256,
}

/// Resolved senders, plus hashes that still failed after one retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderLookup {
    pub senders: HashMap<B256, TxSender>,
    pub failed: Vec<B256>,
}

impl EvmClient {
    pub async fn block_headers(
        &self,
        range: BlockRange,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
        finality: Finality,
    ) -> Result<Vec<BlockRow>, ProviderError> {
        let numbers: Vec<u64> = (range.start()..=range.end()).collect();
        self.batch_resilient(
            &numbers,
            purpose,
            application,
            wait,
            &|number| {
                (
                    "eth_getBlockByNumber".to_owned(),
                    json!([format!("0x{number:x}"), false]),
                )
            },
            &|number, value| {
                let row = parse_block(&value, finality)?;
                if row.number != *number {
                    return Err(ProviderError::fatal(
                        "block number does not match the request",
                    ));
                }
                Ok(row)
            },
        )
        .await
    }

    pub async fn block_by_number(
        &self,
        block: BlockRef,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
        finality: Finality,
    ) -> Result<Option<BlockRow>, ProviderError> {
        let value = self
            .gateway
            .call(
                "eth_getBlockByNumber",
                json!([block.param(), false]),
                purpose,
                application,
                wait,
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }
        let row = parse_block(&value, finality)?;
        if let BlockRef::Number(expected) = block
            && row.number != expected
        {
            return Err(ProviderError::fatal(
                "block number does not match the request",
            ));
        }
        Ok(Some(row))
    }

    pub async fn transaction_senders(
        &self,
        hashes: &[B256],
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
    ) -> Result<SenderLookup, ProviderError> {
        let unique = deduped(hashes);
        let first = self
            .sender_batch(&unique, purpose, application, wait)
            .await?;
        let (mut senders, mut failed) = fold_senders(first);
        if !failed.is_empty()
            && let Ok(again) = self.sender_batch(&failed, purpose, application, wait).await
        {
            let (retried, still_failed) = fold_senders(again);
            senders.extend(retried);
            failed = still_failed;
        }
        Ok(SenderLookup { senders, failed })
    }

    async fn sender_batch(
        &self,
        hashes: &[B256],
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
    ) -> Result<Vec<(B256, Result<Value, ProviderError>)>, ProviderError> {
        let results = self
            .batch_chunks(hashes, purpose, application, wait, &|hash| {
                (
                    "eth_getTransactionByHash".to_owned(),
                    json!([format!("{hash:#x}")]),
                )
            })
            .await?;
        Ok(hashes.iter().copied().zip(results).collect())
    }

    async fn batch_resilient<T, R>(
        &self,
        items: &[T],
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
        build: &impl Fn(&T) -> (String, Value),
        parse: &impl Fn(&T, Value) -> Result<R, ProviderError>,
    ) -> Result<Vec<R>, ProviderError> {
        let results = self
            .batch_chunks(items, purpose, application, wait, build)
            .await?;
        let mut out = Vec::with_capacity(items.len());
        for (item, result) in items.iter().zip(results) {
            out.push(parse(item, result?)?);
        }
        Ok(out)
    }

    /// Halves only when the provider rejects the batch as too large.
    async fn batch_chunks<T>(
        &self,
        items: &[T],
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
        build: &impl Fn(&T) -> (String, Value),
    ) -> Result<Vec<Result<Value, ProviderError>>, ProviderError> {
        let mut out = Vec::with_capacity(items.len());
        let mut queue: VecDeque<&[T]> = items.chunks(self.rpc_batch).collect();
        while let Some(chunk) = queue.pop_front() {
            let requests: Vec<(String, Value)> = chunk.iter().map(build).collect();
            match self
                .gateway
                .batch(requests, purpose, application, wait)
                .await
            {
                Ok(results) => {
                    if results.len() != chunk.len() {
                        return Err(ProviderError::fatal("batch result length mismatch"));
                    }
                    out.extend(results);
                }
                Err(ProviderError::BatchTooLarge) if chunk.len() > 1 => {
                    let mid = chunk.len() / 2;
                    queue.push_front(&chunk[mid..]);
                    queue.push_front(&chunk[..mid]);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(out)
    }
}

fn deduped(hashes: &[B256]) -> Vec<B256> {
    let mut seen = HashSet::with_capacity(hashes.len());
    let mut unique = Vec::with_capacity(hashes.len());
    for hash in hashes {
        if seen.insert(*hash) {
            unique.push(*hash);
        }
    }
    unique
}

fn fold_senders(
    results: Vec<(B256, Result<Value, ProviderError>)>,
) -> (HashMap<B256, TxSender>, Vec<B256>) {
    let mut senders = HashMap::new();
    let mut failed = Vec::new();
    for (hash, result) in results {
        match result {
            Ok(value) => match parse_tx(&value) {
                Ok(Some(parsed)) => {
                    senders.insert(
                        hash,
                        TxSender {
                            address: parsed.address,
                            block_number: parsed.block_number,
                            block_hash: parsed.block_hash,
                        },
                    );
                }
                Ok(None) => {}
                Err(_) => failed.push(hash),
            },
            Err(_) => failed.push(hash),
        }
    }
    (senders, failed)
}
