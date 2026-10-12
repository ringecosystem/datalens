use std::collections::{HashMap, VecDeque};

use datalens_core::{Address, B256, BlockRange, BlockRow, Finality};
use datalens_provider::{ProviderError, Purpose};
use serde_json::{Value, json};

use crate::client::{BlockRef, EvmClient};
use crate::decode::{parse_block, parse_sender};

impl EvmClient {
    pub async fn block_headers(
        &self,
        range: BlockRange,
        purpose: Purpose,
        application: Option<&str>,
        finality: Finality,
    ) -> Result<Vec<BlockRow>, ProviderError> {
        let numbers: Vec<u64> = (range.start()..=range.end()).collect();
        self.batch_resilient(
            &numbers,
            purpose,
            application,
            &|number| {
                (
                    "eth_getBlockByNumber".to_owned(),
                    json!([format!("0x{number:x}"), false]),
                )
            },
            &|_, value| parse_block(&value, finality),
        )
        .await
    }

    pub async fn block_by_number(
        &self,
        block: BlockRef,
        purpose: Purpose,
        application: Option<&str>,
        finality: Finality,
    ) -> Result<Option<BlockRow>, ProviderError> {
        let value = self
            .gateway
            .call(
                "eth_getBlockByNumber",
                json!([block.param(), false]),
                purpose,
                application,
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }
        Ok(Some(parse_block(&value, finality)?))
    }

    pub async fn transaction_senders(
        &self,
        hashes: &[B256],
        purpose: Purpose,
        application: Option<&str>,
    ) -> Result<HashMap<B256, Address>, ProviderError> {
        let pairs = self
            .batch_resilient(
                hashes,
                purpose,
                application,
                &|hash| {
                    (
                        "eth_getTransactionByHash".to_owned(),
                        json!([format!("{hash:#x}")]),
                    )
                },
                &|hash, value| Ok((*hash, parse_sender(&value)?)),
            )
            .await?;
        Ok(pairs
            .into_iter()
            .filter_map(|(hash, sender)| sender.map(|address| (hash, address)))
            .collect())
    }

    /// A JSON-RPC batch that comes back HTTP 5xx is split in half and tried again.
    /// Halving stops at one call. The old client fell through to one request per block.
    async fn batch_resilient<T, R>(
        &self,
        items: &[T],
        purpose: Purpose,
        application: Option<&str>,
        build: &impl Fn(&T) -> (String, Value),
        parse: &impl Fn(&T, Value) -> Result<R, ProviderError>,
    ) -> Result<Vec<R>, ProviderError> {
        let mut out = Vec::with_capacity(items.len());
        let mut queue: VecDeque<&[T]> = items.chunks(self.rpc_batch).collect();
        while let Some(chunk) = queue.pop_front() {
            let requests: Vec<(String, Value)> = chunk.iter().map(build).collect();
            match self.gateway.batch(requests, purpose, application).await {
                Ok(results) => {
                    if results.len() != chunk.len() {
                        return Err(ProviderError::fatal("batch result length mismatch"));
                    }
                    for (item, result) in chunk.iter().zip(results) {
                        out.push(parse(item, result?)?);
                    }
                }
                Err(ProviderError::Transient { status: Some(code) })
                    if (500..600).contains(&code) && chunk.len() > 1 =>
                {
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
