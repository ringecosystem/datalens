use datalens_core::{B256, ChainId, Dataset};

use crate::codec::{b256_from_db, chain_from_db, dataset_from_db, i64_to_u64, u64_to_i64};
use crate::ids::SelectorId;
use crate::{PgTx, Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowProgress {
    pub chain_id: ChainId,
    pub dataset: Dataset,
    pub selector_id: SelectorId,
    pub from_block: u64,
    /// Highest block fully fetched. `-1` means the next pull starts at block 0.
    pub head_block: i64,
    pub head_hash: B256,
    pub promoted_to: i64,
}

impl Store {
    pub async fn follow_get_or_init(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        from_block: u64,
        head_hash: &B256,
    ) -> Result<FollowProgress, StoreError> {
        let from_block = u64_to_i64(from_block, "from block")?;
        let head_block = from_block - 1;
        sqlx::query(
            "insert into follow_progress (
                chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to
             )
             values ($1, $2, $3, $4, $5, $6, $5)
             on conflict (chain_id, dataset, selector_id) do nothing",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(from_block)
        .bind(head_block)
        .bind(head_hash.as_slice())
        .execute(&self.pool)
        .await?;
        self.follow_row(chain, dataset, selector_id).await
    }

    pub async fn advance_follow_head(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        head_block: u64,
        head_hash: &B256,
    ) -> Result<FollowProgress, StoreError> {
        let head_block = u64_to_i64(head_block, "head block")?;
        let updated: Option<FollowRow> = sqlx::query_as(
            "update follow_progress
             set head_block = $4, head_hash = $5
             where chain_id = $1 and dataset = $2 and selector_id = $3 and head_block <= $4
             returning chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(head_block)
        .bind(head_hash.as_slice())
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(row) = updated {
            return follow_from_row(row);
        }
        let current = self.follow_row_tx(tx, chain, dataset, selector_id).await?;
        if current.head_block > head_block {
            Err(StoreError::invalid("follow head moves only forward"))
        } else {
            Ok(current)
        }
    }

    pub async fn advance_follow_promoted(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        promoted_to: u64,
    ) -> Result<FollowProgress, StoreError> {
        let promoted_to = u64_to_i64(promoted_to, "promoted block")?;
        let current = self.follow_row_tx(tx, chain, dataset, selector_id).await?;
        if promoted_to < current.promoted_to {
            return Err(StoreError::invalid("promoted_to moves only forward"));
        }
        if promoted_to > current.head_block {
            return Err(StoreError::invalid(
                "promoted_to cannot pass the followed head",
            ));
        }
        let updated: Option<FollowRow> = sqlx::query_as(
            "update follow_progress
             set promoted_to = $4
             where chain_id = $1 and dataset = $2 and selector_id = $3
             returning chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(promoted_to)
        .fetch_optional(&mut **tx)
        .await?;
        follow_from_row(updated.ok_or_else(|| StoreError::not_found("follow progress not found"))?)
    }

    /// Rewinds every progress row on `chain` that still reaches `from_block`.
    /// `head_hash` is the canonical hash of `from_block - 1`. The next pull starts there.
    pub async fn rewind_follow(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        from_block: u64,
        head_hash: &B256,
    ) -> Result<Vec<FollowProgress>, StoreError> {
        let fork = u64_to_i64(from_block, "from block")?;
        let head = fork - 1;
        let rows: Vec<FollowRow> = sqlx::query_as(
            "update follow_progress
             set promoted_to = least(promoted_to, $2),
                 head_hash = case when head_block >= $3 then $4 else head_hash end,
                 head_block = least(head_block, $2)
             where chain_id = $1 and (head_block >= $3 or promoted_to >= $3)
             returning chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to",
        )
        .bind(chain.get())
        .bind(head)
        .bind(fork)
        .bind(head_hash.as_slice())
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter().map(follow_from_row).collect()
    }

    /// Deletes progress rows on `chain` whose selector has no unexpired subscription, so a
    /// later subscription starts again at safe + 1 instead of resuming a stale head.
    pub async fn delete_unwatched_follow(&self, chain: ChainId) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "delete from follow_progress f
             where f.chain_id = $1
               and not exists (
                 select 1 from subscriptions s
                 where s.chain_id = f.chain_id and s.dataset = f.dataset
                   and s.selector_id = f.selector_id and s.expires_at > now()
               )",
        )
        .bind(chain.get())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn list_follow(&self, chain: ChainId) -> Result<Vec<FollowProgress>, StoreError> {
        let rows: Vec<FollowRow> = sqlx::query_as(
            "select chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to
             from follow_progress
             where chain_id = $1
             order by dataset, selector_id",
        )
        .bind(chain.get())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(follow_from_row).collect()
    }

    async fn follow_row(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
    ) -> Result<FollowProgress, StoreError> {
        let row: Option<FollowRow> = sqlx::query_as(
            "select chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to
             from follow_progress
             where chain_id = $1 and dataset = $2 and selector_id = $3",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .fetch_optional(&self.pool)
        .await?;
        follow_from_row(row.ok_or_else(|| StoreError::not_found("follow progress not found"))?)
    }

    async fn follow_row_tx(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
    ) -> Result<FollowProgress, StoreError> {
        let row: Option<FollowRow> = sqlx::query_as(
            "select chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to
             from follow_progress
             where chain_id = $1 and dataset = $2 and selector_id = $3
             for update",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .fetch_optional(&mut **tx)
        .await?;
        follow_from_row(row.ok_or_else(|| StoreError::not_found("follow progress not found"))?)
    }
}

type FollowRow = (i32, String, i64, i64, i64, Vec<u8>, i64);

fn follow_from_row(row: FollowRow) -> Result<FollowProgress, StoreError> {
    let (chain_id, dataset, selector_id, from_block, head_block, head_hash, promoted_to) = row;
    Ok(FollowProgress {
        chain_id: chain_from_db(chain_id)?,
        dataset: dataset_from_db(&dataset)?,
        selector_id: SelectorId::from_raw(selector_id),
        from_block: i64_to_u64(from_block, "from block")?,
        head_block,
        head_hash: b256_from_db(&head_hash)?,
        promoted_to,
    })
}
