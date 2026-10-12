use std::collections::{BTreeSet, HashMap};

use datalens_core::wire::{BlockRecord, LogRecord, QueryRequest};
use datalens_core::{
    Address, B256, BlockRange, Dataset, Error, Finality, Heads, LogSelector, Selector,
};
use datalens_provider::{Purpose, TokenWait};
use datalens_store::{BlockCursor, LogCursor, StoreError};
use serde_json::Value;

use crate::chain::Chain;
use crate::engine::{QueryEngine, provider_error};
use crate::plan::{Cursor, row_finality};

const LOG_FIELDS: [&str; 9] = [
    "block_number",
    "block_hash",
    "transaction_hash",
    "transaction_index",
    "log_index",
    "address",
    "topics",
    "data",
    "finality",
];

/// Fields that cost a lookup, so they are returned only when `fields` lists them.
const LOG_ENRICHMENTS: [&str; 2] = ["block_timestamp", "transaction_from"];

/// What a query reads: its dataset, selector, and output fields.
pub(crate) enum Target {
    Logs {
        selector: LogSelector,
        keep: Option<BTreeSet<String>>,
        block_timestamp: bool,
        transaction_from: bool,
    },
    Blocks {
        keep: Option<BTreeSet<String>>,
    },
}

impl Target {
    pub(crate) fn parse(request: &QueryRequest) -> Result<Self, Error> {
        Self::new(
            request.dataset,
            request.selector.clone(),
            request.fields.as_deref(),
        )
    }

    pub(crate) fn new(
        dataset: Dataset,
        selector: Option<LogSelector>,
        fields: Option<&[String]>,
    ) -> Result<Self, Error> {
        let keep: Option<BTreeSet<String>> = fields.map(|fields| fields.iter().cloned().collect());
        match dataset {
            Dataset::EvmLogs => {
                if let Some(unknown) = keep.iter().flatten().find(|field| {
                    !LOG_FIELDS.contains(&field.as_str())
                        && !LOG_ENRICHMENTS.contains(&field.as_str())
                }) {
                    return Err(Error::InvalidInput(format!(
                        "unknown evm.logs field {unknown:?}"
                    )));
                }
                let listed = |name: &str| keep.as_ref().is_some_and(|keep| keep.contains(name));
                Ok(Self::Logs {
                    selector: selector.unwrap_or_default(),
                    block_timestamp: listed("block_timestamp"),
                    transaction_from: listed("transaction_from"),
                    keep,
                })
            }
            Dataset::EvmBlocks => {
                if selector.is_some() {
                    return Err(Error::InvalidInput(
                        "evm.blocks takes no selector".to_owned(),
                    ));
                }
                Ok(Self::Blocks { keep })
            }
        }
    }

    pub(crate) fn selector(&self) -> Selector {
        match self {
            Self::Logs { selector, .. } => Selector::Logs(selector.clone()),
            Self::Blocks { .. } => Selector::Blocks,
        }
    }
}

/// One page inside `served`, the covered prefix, starting after `cursor`.
pub(crate) struct Page {
    pub(crate) served: BlockRange,
    pub(crate) cursor: Option<Cursor>,
    pub(crate) limit: u32,
}

impl QueryEngine {
    pub(crate) async fn read_rows(
        &self,
        application: &str,
        chain: &Chain,
        target: &Target,
        page: Page,
        heads: &Heads,
    ) -> Result<(Vec<Value>, Option<String>), Error> {
        let store = &self.inner.store;
        let Page {
            served,
            cursor,
            limit,
        } = page;
        match target {
            Target::Logs {
                selector,
                keep,
                block_timestamp,
                transaction_from,
            } => {
                let after = match cursor {
                    None => None,
                    Some(Cursor::Log {
                        block_number,
                        log_index,
                    }) => Some(LogCursor {
                        block_number,
                        log_index,
                    }),
                    Some(Cursor::Block { .. }) => return Err(wrong_cursor()),
                };
                let (rows, next) = store
                    .read_logs(chain.id(), selector, served, after, limit)
                    .await?;
                let mut records: Vec<LogRecord> = rows.into_iter().map(LogRecord::from).collect();
                if *transaction_from {
                    self.attach_senders(application, chain, heads, &mut records)
                        .await?;
                }
                if *block_timestamp {
                    self.attach_timestamps(application, chain, heads, &mut records)
                        .await?;
                }
                let rows = records
                    .into_iter()
                    .map(|record| project(&record, keep.as_ref()))
                    .collect::<Result<_, _>>()?;
                let next = next.map(|cursor| {
                    Cursor::Log {
                        block_number: cursor.block_number,
                        log_index: cursor.log_index,
                    }
                    .encode()
                });
                Ok((rows, next))
            }
            Target::Blocks { keep } => {
                let after = match cursor {
                    None => None,
                    Some(Cursor::Block { number }) => Some(BlockCursor { number }),
                    Some(Cursor::Log { .. }) => return Err(wrong_cursor()),
                };
                let (rows, next) = store.read_blocks(chain.id(), served, after, limit).await?;
                let rows = rows
                    .into_iter()
                    .map(|row| project(&BlockRecord::from(row), keep.as_ref()))
                    .collect::<Result<_, _>>()?;
                let next = next.map(|cursor| {
                    Cursor::Block {
                        number: cursor.number,
                    }
                    .encode()
                });
                Ok((rows, next))
            }
        }
    }

    /// Senders come from `evm_tx_senders`. Only hashes never seen before reach the provider.
    async fn attach_senders(
        &self,
        application: &str,
        chain: &Chain,
        heads: &Heads,
        records: &mut [LogRecord],
    ) -> Result<(), Error> {
        let store = &self.inner.store;
        let hashes: Vec<B256> = records
            .iter()
            .map(|record| record.transaction_hash)
            .collect();
        let missing = store.missing_hashes(chain.id(), &hashes).await?;
        if !missing.is_empty() {
            let found = chain
                .evm
                .transaction_senders(
                    &missing,
                    Purpose::QueryFill,
                    Some(application),
                    TokenWait::For(self.inner.settings.token_wait),
                )
                .await
                .map_err(provider_error)?;
            let blocks: HashMap<B256, (u64, B256)> = records
                .iter()
                .map(|record| {
                    (
                        record.transaction_hash,
                        (record.block_number, record.block_hash),
                    )
                })
                .collect();
            // A sender the provider places in another block than the durable log is from a
            // fork, so it is not stored. Like a failed or missing lookup, it fails the query
            // instead of reading as a field nobody asked for. Resolved senders are stored first,
            // so the next query asks only for the unresolved hashes.
            let rows: Vec<(B256, u64, Address, Finality)> = found
                .senders
                .into_iter()
                .filter(|(hash, sender)| {
                    blocks.get(hash) == Some(&(sender.block_number, sender.block_hash))
                })
                .map(|(hash, sender)| {
                    let finality = row_finality(sender.block_number, heads);
                    (hash, sender.block_number, sender.address, finality)
                })
                .collect();
            let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
            store.insert_senders(&mut tx, chain.id(), &rows).await?;
            tx.commit().await.map_err(StoreError::from)?;
            if rows.len() < missing.len() {
                return Err(Error::Internal(format!(
                    "no canonical sender for {} of {} transactions",
                    missing.len() - rows.len(),
                    missing.len()
                )));
            }
        }
        let senders = store.senders(chain.id(), &hashes).await?;
        for record in records {
            record.transaction_from = senders.get(&record.transaction_hash).copied();
        }
        Ok(())
    }

    /// Timestamps come from `evm_blocks`. Missing headers are fetched once and stored.
    async fn attach_timestamps(
        &self,
        application: &str,
        chain: &Chain,
        heads: &Heads,
        records: &mut [LogRecord],
    ) -> Result<(), Error> {
        let store = &self.inner.store;
        let numbers: Vec<u64> = records
            .iter()
            .map(|record| record.block_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut stamps = store.block_timestamps(chain.id(), &numbers).await?;
        let missing: Vec<u64> = numbers
            .into_iter()
            .filter(|number| !stamps.contains_key(number))
            .collect();
        if !missing.is_empty() {
            let hashes: HashMap<u64, B256> = records
                .iter()
                .map(|record| (record.block_number, record.block_hash))
                .collect();
            let mut fetched = Vec::new();
            for run in finality_runs(&missing, heads) {
                fetched.extend(
                    chain
                        .evm
                        .block_headers(
                            run,
                            Purpose::QueryFill,
                            Some(application),
                            TokenWait::For(self.inner.settings.token_wait),
                            row_finality(run.end(), heads),
                        )
                        .await
                        .map_err(provider_error)?,
                );
            }
            // A header off the log's fork carries the wrong timestamp, and storing it would
            // contradict the log.
            fetched.retain(|block| hashes.get(&block.number) == Some(&block.hash));
            let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
            match store.insert_blocks(&mut tx, chain.id(), &fetched).await {
                Ok(_) => tx.commit().await.map_err(StoreError::from)?,
                Err(StoreError::Reorg { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            stamps.extend(fetched.iter().map(|block| (block.number, block.timestamp)));
        }
        for record in records {
            record.block_timestamp = stamps.get(&record.block_number).copied();
        }
        Ok(())
    }
}

fn project(
    record: &impl serde::Serialize,
    keep: Option<&BTreeSet<String>>,
) -> Result<Value, Error> {
    let mut value =
        serde_json::to_value(record).map_err(|error| Error::Internal(error.to_string()))?;
    if let (Value::Object(map), Some(keep)) = (&mut value, keep) {
        map.retain(|key, _| keep.contains(key));
    }
    Ok(value)
}

/// Consecutive runs of `sorted` that each hold one finality.
fn finality_runs(sorted: &[u64], heads: &Heads) -> Vec<BlockRange> {
    let mut runs: Vec<BlockRange> = Vec::new();
    for &number in sorted {
        match runs.last_mut() {
            Some(last)
                if last.end() + 1 == number
                    && row_finality(last.end(), heads) == row_finality(number, heads) =>
            {
                *last = BlockRange::new(last.start(), number).expect("run grows upward");
            }
            _ => runs.extend(BlockRange::new(number, number).ok()),
        }
    }
    runs
}

fn wrong_cursor() -> Error {
    Error::InvalidInput("page cursor belongs to another dataset".to_owned())
}
