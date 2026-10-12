use std::collections::{BTreeMap, BTreeSet, HashMap};

use datalens_core::{
    B256, BlockRange, Dataset, Error, Finality, Heads, LogRow, LogSelector, Selector,
};
use datalens_provider::{Purpose, TokenWait};
use datalens_store::{FollowProgress, PgTx, SelectorId, StoreError};

use crate::chain::Chain;
use crate::engine::{QueryEngine, lock, provider_error};
use crate::reorg::{Canon, HeadCheck};
use crate::usage::UsageCounts;

/// Applications subscribed to each followed selector.
type Watchers = BTreeMap<(Dataset, SelectorId), Vec<String>>;

#[derive(PartialEq, Eq)]
enum Step {
    Done,
    /// A fork or stale data stopped this tick. The next tick starts from the rewound heads.
    Reorged,
}

impl QueryEngine {
    /// Reorg check, then one fetch per group into unsafe rows, then promotion up to safe.
    /// Provider calls per tick: one reorg check, one `eth_getLogs` per group, and one header
    /// batch when `evm.blocks` is followed. None depend on how many clients poll.
    pub(crate) async fn follow_tick(&self, chain: &Chain, heads: &Heads) -> Result<(), Error> {
        let store = &self.inner.store;
        let name = chain.name().as_str();
        let mut watchers = Watchers::new();
        for sub in store.active_subscriptions(chain.id()).await? {
            watchers
                .entry((sub.dataset, sub.selector_id))
                .or_default()
                .push(sub.application_id);
        }
        for dataset in [Dataset::EvmLogs, Dataset::EvmBlocks] {
            let count = watchers
                .iter()
                .filter(|((watched, _), _)| *watched == dataset)
                .map(|(_, apps)| apps.len())
                .sum();
            self.inner
                .metrics
                .set_subscriptions(name, dataset.as_str(), count);
        }
        if watchers.is_empty() || lock(&chain.follow).halted {
            self.inner.metrics.set_follow_lag(name, 0);
            return Ok(());
        }

        let mut progress: Vec<FollowProgress> = store
            .list_follow(chain.id())
            .await?
            .into_iter()
            .filter(|row| watchers.contains_key(&(row.dataset, row.selector_id)))
            .collect();
        for &(dataset, selector_id) in watchers.keys() {
            if !progress
                .iter()
                .any(|row| row.dataset == dataset && row.selector_id == selector_id)
            {
                progress.push(
                    store
                        .follow_get_or_init(
                            chain.id(),
                            dataset,
                            selector_id,
                            heads.safe + 1,
                            &B256::ZERO,
                        )
                        .await?,
                );
            }
        }

        let mut canon = Canon::new(heads);
        if let HeadCheck::Reorged = self
            .check_heads(chain, heads, &progress, &mut canon)
            .await?
        {
            return Ok(());
        }
        let ids: Vec<SelectorId> = progress.iter().map(|row| row.selector_id).collect();
        let selectors: HashMap<SelectorId, Selector> =
            store.selectors_by_id(&ids).await?.into_iter().collect();

        let mut hashes = HashMap::from([(heads.latest, heads.latest_hash)]);
        for row in progress
            .iter()
            .filter(|row| row.dataset == Dataset::EvmBlocks)
        {
            let apps = &watchers[&(row.dataset, row.selector_id)];
            if self
                .follow_blocks(chain, heads, row, apps, &mut hashes, &mut canon)
                .await?
                == Step::Reorged
            {
                return Ok(());
            }
        }
        let logs: Vec<(FollowProgress, LogSelector)> = progress
            .iter()
            .filter_map(|row| match selectors.get(&row.selector_id) {
                Some(Selector::Logs(selector)) => Some((row.clone(), selector.clone())),
                _ => None,
            })
            .collect();
        if self
            .follow_logs(chain, heads, logs, &watchers, &hashes, &mut canon)
            .await?
            == Step::Reorged
        {
            return Ok(());
        }

        // Only heads verified at the start of this tick bound promotion. Rows fetched just
        // now are checked by the next tick's reorg check first.
        self.finalize(chain, heads, &progress, &selectors, &mut canon)
            .await?;

        let lowest = store
            .list_follow(chain.id())
            .await?
            .iter()
            .filter(|row| watchers.contains_key(&(row.dataset, row.selector_id)))
            .map(|row| row.head_block)
            .min()
            .unwrap_or(-1);
        let lag = heads
            .latest
            .saturating_sub(u64::try_from(lowest).unwrap_or(0));
        self.inner.metrics.set_follow_lag(name, lag);
        Ok(())
    }

    async fn follow_blocks(
        &self,
        chain: &Chain,
        heads: &Heads,
        row: &FollowProgress,
        apps: &[String],
        hashes: &mut HashMap<u64, B256>,
        canon: &mut Canon,
    ) -> Result<Step, Error> {
        let store = &self.inner.store;
        let Some(span) = next_span(
            row.head_block,
            heads.latest,
            chain.settings.header_chunk_blocks.get(),
        ) else {
            return Ok(Step::Done);
        };
        let headers = chain
            .evm
            .block_headers(
                span,
                Purpose::Follow,
                None,
                TokenWait::UntilToken,
                Finality::Unsafe,
            )
            .await
            .map_err(provider_error)?;
        let mut parent = (row.head_hash != B256::ZERO).then_some(row.head_hash);
        for header in &headers {
            if parent.is_some_and(|parent| parent != header.parent_hash) {
                self.roll_back(chain, heads, header.number.saturating_sub(1), canon)
                    .await?;
                return Ok(Step::Reorged);
            }
            parent = Some(header.hash);
        }
        let Some(last) = headers.last() else {
            return Ok(Step::Done);
        };
        if span.end() == heads.latest && last.hash != heads.latest_hash {
            return Ok(Step::Reorged);
        }
        let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
        if let Err(error) = store.insert_blocks(&mut tx, chain.id(), &headers).await {
            drop(tx);
            return self.after_conflict(chain, heads, error, canon).await;
        }
        store
            .advance_follow_head(
                &mut tx,
                chain.id(),
                Dataset::EvmBlocks,
                row.selector_id,
                span.end(),
                &last.hash,
            )
            .await?;
        tx.commit().await.map_err(StoreError::from)?;
        hashes.extend(headers.iter().map(|header| (header.number, header.hash)));
        self.record_follow_calls(chain, Dataset::EvmBlocks, apps.iter(), 1);
        Ok(Step::Done)
    }

    /// Groups selectors whose heads sit within one fetch window and that batch into one
    /// `eth_getLogs`. Each group fetches from its lowest head to `latest`, capped at the
    /// adaptive range, so heads converge and later ticks share one span.
    async fn follow_logs(
        &self,
        chain: &Chain,
        heads: &Heads,
        mut pending: Vec<(FollowProgress, LogSelector)>,
        watchers: &Watchers,
        hashes: &HashMap<u64, B256>,
        canon: &mut Canon,
    ) -> Result<Step, Error> {
        let cap = chain.evm.gateway().range_blocks(Dataset::EvmLogs);
        let max = chain.settings.max_addresses_per_query.get();
        pending.sort_by_key(|(row, _)| row.head_block);
        while let Some((first, _)) = pending.first() {
            let Some(span) = next_span(first.head_block, heads.latest, cap) else {
                break;
            };
            let mut members: Vec<(FollowProgress, LogSelector)> = Vec::new();
            let mut rest = Vec::new();
            for (row, selector) in pending.drain(..) {
                let fits = row.head_block < span.end() as i64 && {
                    let mut trial: Vec<LogSelector> =
                        members.iter().map(|(_, s)| s.clone()).collect();
                    trial.push(selector.clone());
                    LogSelector::batch(&trial, max).is_some()
                };
                if fits {
                    members.push((row, selector));
                } else {
                    rest.push((row, selector));
                }
            }
            if self
                .follow_group(chain, heads, &members, span, watchers, hashes, canon)
                .await?
                == Step::Reorged
            {
                return Ok(Step::Reorged);
            }
            pending = rest;
        }
        Ok(Step::Done)
    }

    #[allow(clippy::too_many_arguments)]
    async fn follow_group(
        &self,
        chain: &Chain,
        heads: &Heads,
        members: &[(FollowProgress, LogSelector)],
        span: BlockRange,
        watchers: &Watchers,
        hashes: &HashMap<u64, B256>,
        canon: &mut Canon,
    ) -> Result<Step, Error> {
        let store = &self.inner.store;
        let selectors: Vec<LogSelector> = members.iter().map(|(_, s)| s.clone()).collect();
        let query = LogSelector::batch(&selectors, chain.settings.max_addresses_per_query.get())
            .ok_or_else(|| Error::Internal("follow group does not batch".to_owned()))?;
        let page = chain
            .evm
            .get_logs(
                &query,
                span,
                Purpose::Follow,
                None,
                TokenWait::UntilToken,
                Finality::Unsafe,
            )
            .await
            .map_err(|failure| provider_error(failure.error))?;
        let mut calls = 1;
        let rows: Vec<LogRow> = page
            .rows
            .into_iter()
            .filter(|row| selectors.iter().any(|selector| selector.matches(row)))
            .collect();

        let end = span.end();
        let end_hash = match hashes.get(&end) {
            Some(hash) => *hash,
            None => match store.hash_at(chain.id(), end).await? {
                Some(hash) => hash,
                None => {
                    calls += 1;
                    self.canonical(chain, canon, end)
                        .await?
                        .ok_or_else(|| Error::Internal(format!("no canonical block {end}")))?
                }
            },
        };
        if rows
            .iter()
            .any(|row| row.block_number == end && row.block_hash != end_hash)
        {
            return Ok(Step::Reorged);
        }

        let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
        if let Err(error) = store.insert_logs(&mut tx, chain.id(), &rows).await {
            drop(tx);
            return self.after_conflict(chain, heads, error, canon).await;
        }
        for (row, _) in members {
            store
                .advance_follow_head(
                    &mut tx,
                    chain.id(),
                    Dataset::EvmLogs,
                    row.selector_id,
                    end,
                    &end_hash,
                )
                .await?;
        }
        tx.commit().await.map_err(StoreError::from)?;
        let apps: BTreeSet<&String> = members
            .iter()
            .flat_map(|(row, _)| watchers[&(Dataset::EvmLogs, row.selector_id)].iter())
            .collect();
        self.record_follow_calls(chain, Dataset::EvmLogs, apps.into_iter(), calls);
        Ok(Step::Done)
    }

    /// A stored row at another hash than an incoming one: roll back if the stored side is
    /// off the canonical chain, otherwise drop the stale fetch. Either way the tick stops.
    async fn after_conflict(
        &self,
        chain: &Chain,
        heads: &Heads,
        error: StoreError,
        canon: &mut Canon,
    ) -> Result<Step, Error> {
        match error {
            StoreError::Reorg { block } => {
                self.roll_back(chain, heads, block, canon).await?;
                Ok(Step::Reorged)
            }
            other => Err(other.into()),
        }
    }

    /// Promotes `(promoted_to, min(safe, head_block)]` of each verified follow row in one
    /// transaction per row: row finality, senders, durable coverage, and `promoted_to`.
    async fn finalize(
        &self,
        chain: &Chain,
        heads: &Heads,
        progress: &[FollowProgress],
        selectors: &HashMap<SelectorId, Selector>,
        canon: &mut Canon,
    ) -> Result<(), Error> {
        let store = &self.inner.store;
        for row in progress {
            let Ok(head) = u64::try_from(row.head_block) else {
                continue;
            };
            let end = heads.safe.min(head);
            let Ok(start) = u64::try_from(row.promoted_to + 1) else {
                continue;
            };
            let Ok(segment) = BlockRange::new(start, end) else {
                continue;
            };
            let Some(selector) = selectors.get(&row.selector_id) else {
                continue;
            };
            // The head hash was verified this tick and commits to every block below it, so
            // a stored hash at a lower segment end is canonical too.
            let end_hash = if end == head {
                row.head_hash
            } else {
                match self.stored_hash(chain, selector, end).await? {
                    Some(hash) => hash,
                    None => continue,
                }
            };
            let finality = if end <= heads.finalized {
                Finality::Finalized
            } else {
                Finality::Safe
            };
            let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
            match self
                .promote(&mut tx, chain, row, selector, segment, finality, end_hash)
                .await
            {
                Ok(()) => tx.commit().await.map_err(StoreError::from)?,
                Err(StoreError::Reorg { block }) => {
                    drop(tx);
                    self.roll_back(chain, heads, block, canon).await?;
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn promote(
        &self,
        tx: &mut PgTx<'_>,
        chain: &Chain,
        row: &FollowProgress,
        selector: &Selector,
        segment: BlockRange,
        finality: Finality,
        end_hash: B256,
    ) -> Result<(), StoreError> {
        let store = &self.inner.store;
        match selector {
            Selector::Logs(logs) => {
                store
                    .promote_logs(tx, chain.id(), logs, segment, finality, end_hash)
                    .await?;
            }
            Selector::Blocks => {
                store
                    .promote_blocks(tx, chain.id(), segment, finality, end_hash)
                    .await?;
            }
        }
        store
            .promote_senders(tx, chain.id(), segment, finality)
            .await?;
        store
            .add_coverage(
                tx,
                chain.id(),
                row.dataset,
                row.selector_id,
                segment,
                finality,
            )
            .await?;
        store
            .advance_follow_promoted(tx, chain.id(), row.dataset, row.selector_id, segment.end())
            .await?;
        Ok(())
    }

    /// The hash stored at `number`. For logs with no row there, any hash passes promotion's
    /// end check, so zero stands in.
    async fn stored_hash(
        &self,
        chain: &Chain,
        selector: &Selector,
        number: u64,
    ) -> Result<Option<B256>, Error> {
        Ok(match selector {
            Selector::Blocks => self.inner.store.hash_at(chain.id(), number).await?,
            Selector::Logs(_) => Some(
                self.stored_hash_at(chain, number)
                    .await?
                    .unwrap_or(B256::ZERO),
            ),
        })
    }

    /// Splits follow calls evenly across the applications that asked for them.
    fn record_follow_calls<'a>(
        &self,
        chain: &Chain,
        dataset: Dataset,
        apps: impl Iterator<Item = &'a String>,
        calls: i64,
    ) {
        let apps: Vec<&String> = apps.collect();
        let count = apps.len() as i64;
        for (index, app) in apps.into_iter().enumerate() {
            let share = calls / count + i64::from((index as i64) < calls % count);
            if share > 0 {
                self.inner.usage.record(
                    app,
                    chain.id(),
                    dataset,
                    UsageCounts {
                        provider_calls: share,
                        ..UsageCounts::default()
                    },
                );
            }
        }
    }
}

/// `[head + 1, latest]`, at most `cap` blocks. `None` when the head is at `latest`.
fn next_span(head: i64, latest: u64, cap: u64) -> Option<BlockRange> {
    let start = u64::try_from(head + 1).ok()?;
    let end = latest.min(start.saturating_add(cap.max(1) - 1));
    BlockRange::new(start, end).ok()
}
