use std::collections::{BTreeMap, BTreeSet, HashMap};

use datalens_core::{B256, BlockRange, Error, Finality, Heads, LogSelector};
use datalens_evm::BlockRef;
use datalens_provider::{Purpose, TokenWait};
use datalens_store::{FollowProgress, StoreError};
use tracing::{error, warn};

use crate::chain::Chain;
use crate::engine::{QueryEngine, lock, provider_error};

/// Canonical hashes learned during one tick. The head call already gave `latest`.
pub(crate) struct Canon {
    latest: u64,
    known: HashMap<u64, B256>,
}

impl Canon {
    pub(crate) fn new(heads: &Heads) -> Self {
        Self {
            latest: heads.latest,
            known: HashMap::from([(heads.latest, heads.latest_hash)]),
        }
    }
}

pub(crate) enum HeadCheck {
    /// Every unpromoted follow head is canonical, so rows below them may be promoted.
    Verified,
    /// Unsafe rows were rolled back, or the fetched data was stale. The next tick refetches.
    Reorged,
}

/// Stored hashes by height. Two hashes at one height mean the stored rows disagree.
type Points = BTreeMap<u64, BTreeSet<B256>>;

impl QueryEngine {
    /// One `eth_getBlockByNumber` per distinct unpromoted follow head, usually exactly one
    /// because every group fetches to the same `latest`.
    pub(crate) async fn check_heads(
        &self,
        chain: &Chain,
        heads: &Heads,
        progress: &[FollowProgress],
        canon: &mut Canon,
    ) -> Result<HeadCheck, Error> {
        let mut points = Points::new();
        for row in progress
            .iter()
            .filter(|row| row.head_block > row.promoted_to)
        {
            add_head(&mut points, row);
        }
        for (number, hashes) in &points {
            let canonical = self.canonical(chain, canon, *number).await?;
            if hashes.iter().any(|hash| Some(*hash) != canonical) {
                self.roll_back(chain, heads, *number, canon).await?;
                return Ok(HeadCheck::Reorged);
            }
        }
        Ok(HeadCheck::Verified)
    }

    /// `None` above `latest`, where no canonical block exists yet.
    pub(crate) async fn canonical(
        &self,
        chain: &Chain,
        canon: &mut Canon,
        number: u64,
    ) -> Result<Option<B256>, Error> {
        if number > canon.latest {
            return Ok(None);
        }
        if let Some(hash) = canon.known.get(&number) {
            return Ok(Some(*hash));
        }
        let block = chain
            .evm
            .block_by_number(
                BlockRef::Number(number),
                Purpose::Reorg,
                None,
                TokenWait::UntilToken,
                Finality::Unsafe,
            )
            .await
            .map_err(provider_error)?
            .ok_or_else(|| Error::Internal(format!("provider has no block {number} yet")))?;
        canon.known.insert(number, block.hash);
        Ok(Some(block.hash))
    }

    /// Finds the lowest stored height off the canonical chain at or above the window floor,
    /// deletes unsafe rows from there, and rewinds follow progress. Returns `false` when every
    /// stored hash is canonical, which means the conflicting fetch was the stale side.
    pub(crate) async fn roll_back(
        &self,
        chain: &Chain,
        heads: &Heads,
        detected: u64,
        canon: &mut Canon,
    ) -> Result<bool, Error> {
        let store = &self.inner.store;
        let progress = store.list_follow(chain.id()).await?;
        let top = progress
            .iter()
            .filter_map(|row| u64::try_from(row.head_block).ok())
            .chain([detected])
            .max()
            .unwrap_or(detected);
        let floor = top.saturating_sub(chain.settings.reorg_window_blocks.get());
        let points = self.points(chain, &progress, floor, top).await?;
        let heights: Vec<u64> = points.keys().copied().collect();

        let (mut low, mut high) = (0, heights.len());
        while low < high {
            let mid = (low + high) / 2;
            let canonical = self.canonical(chain, canon, heights[mid]).await?;
            if points[&heights[mid]]
                .iter()
                .any(|hash| Some(*hash) != canonical)
            {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        let Some(&first_bad) = heights.get(low) else {
            return Ok(false);
        };

        let unpromoted_from = progress
            .iter()
            .filter_map(|row| u64::try_from(row.promoted_to + 1).ok())
            .min()
            .unwrap_or(floor);
        if low == 0
            && unpromoted_from < floor
            && !self
                .points(chain, &progress, unpromoted_from, floor - 1)
                .await?
                .is_empty()
        {
            self.halt(chain, "fork is deeper than reorg_window_blocks");
            return Ok(true);
        }

        let fork = first_bad.min(canon.latest + 1);
        let parent = match fork.checked_sub(1) {
            None => B256::ZERO,
            Some(parent) => self
                .canonical(chain, canon, parent)
                .await?
                .ok_or_else(|| Error::Internal(format!("no canonical block {parent}")))?,
        };
        // Every searched point may be bad when the fork starts under them, for example under
        // promoted rows. A stored parent off the canonical chain shows that.
        let deeper = match fork.checked_sub(1) {
            Some(below) if low == 0 => self
                .stored_hash_at(chain, below)
                .await?
                .is_some_and(|stored| stored != parent),
            _ => false,
        };
        let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
        store
            .delete_unsafe_logs_from(&mut tx, chain.id(), fork)
            .await?;
        store
            .delete_unsafe_blocks_from(&mut tx, chain.id(), fork)
            .await?;
        store
            .delete_unsafe_senders_from(&mut tx, chain.id(), fork)
            .await?;
        store
            .rewind_follow(&mut tx, chain.id(), fork, &parent)
            .await?;
        tx.commit().await.map_err(StoreError::from)?;

        let name = chain.name().as_str();
        self.inner.metrics.count_reorg(name);
        warn!(
            chain = name,
            fork, detected, "rolled back unsafe rows after a fork"
        );
        if fork <= heads.safe || (deeper && fork - 1 <= heads.safe) {
            self.inner.metrics.count_finality_violation(name);
            self.halt(chain, "fork is at or below the safe head");
        } else if deeper {
            self.halt(chain, "fork is deeper than reorg_window_blocks");
        }
        Ok(true)
    }

    /// The hash any stored block or log row holds at `number`.
    pub(crate) async fn stored_hash_at(
        &self,
        chain: &Chain,
        number: u64,
    ) -> Result<Option<B256>, Error> {
        let store = &self.inner.store;
        if let Some(hash) = store.hash_at(chain.id(), number).await? {
            return Ok(Some(hash));
        }
        let (rows, _) = store
            .read_logs(
                chain.id(),
                &LogSelector::default(),
                BlockRange::new(number, number)?,
                None,
                1,
            )
            .await?;
        Ok(rows.first().map(|row| row.block_hash))
    }

    /// Unsafe row hashes plus known follow head hashes in `[from, to]`.
    async fn points(
        &self,
        chain: &Chain,
        progress: &[FollowProgress],
        from: u64,
        to: u64,
    ) -> Result<Points, Error> {
        let Ok(range) = BlockRange::new(from, to) else {
            return Ok(Points::new());
        };
        let mut points = Points::new();
        for (number, hash) in self.inner.store.unsafe_hashes(chain.id(), range).await? {
            points.entry(number).or_default().insert(hash);
        }
        for row in progress {
            if u64::try_from(row.head_block).is_ok_and(|head| range.contains(head)) {
                add_head(&mut points, row);
            }
        }
        Ok(points)
    }

    fn halt(&self, chain: &Chain, reason: &str) {
        lock(&chain.follow).halted = true;
        self.inner
            .metrics
            .set_follow_halted(chain.name().as_str(), true);
        error!(chain = %chain.name(), reason, "follow stopped until restart");
    }
}

/// A follow head with an unknown hash (fresh progress) proves nothing and is skipped.
fn add_head(points: &mut Points, row: &FollowProgress) {
    if let Ok(head) = u64::try_from(row.head_block)
        && row.head_hash != B256::ZERO
    {
        points.entry(head).or_default().insert(row.head_hash);
    }
}
