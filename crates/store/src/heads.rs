use std::time::Duration;

use datalens_core::{ChainId, Heads};

use crate::codec::{b256_from_db, chain_from_db, i64_to_u64, u64_to_i64};
use crate::notify::HEADS;
use crate::{Store, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeadKind {
    Latest,
    Safe,
    Finalized,
}

impl HeadKind {
    fn value(self, heads: &Heads) -> u64 {
        match self {
            Self::Latest => heads.latest,
            Self::Safe => heads.safe,
            Self::Finalized => heads.finalized,
        }
    }
}

impl Store {
    pub async fn upsert_heads(&self, chain: ChainId, heads: &Heads) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let changed: Option<i32> = sqlx::query_scalar(
            "insert into chain_heads (chain_id, latest, latest_hash, safe, finalized, updated_at)
             values ($1, $2, $3, $4, $5, now())
             on conflict (chain_id) do update set
                latest = excluded.latest,
                latest_hash = excluded.latest_hash,
                safe = excluded.safe,
                finalized = excluded.finalized,
                updated_at = now()
             where chain_heads.latest is distinct from excluded.latest
                or chain_heads.latest_hash is distinct from excluded.latest_hash
                or chain_heads.safe is distinct from excluded.safe
                or chain_heads.finalized is distinct from excluded.finalized
             returning chain_id",
        )
        .bind(chain.get())
        .bind(u64_to_i64(heads.latest, "latest")?)
        .bind(heads.latest_hash.as_slice())
        .bind(u64_to_i64(heads.safe, "safe")?)
        .bind(u64_to_i64(heads.finalized, "finalized")?)
        .fetch_optional(&mut *tx)
        .await?;
        if changed.is_some() {
            sqlx::query("select pg_notify($1, $2)")
                .bind(HEADS)
                .bind(chain.get().to_string())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn heads(&self, chain: ChainId) -> Result<Option<Heads>, StoreError> {
        let row: Option<(i64, Vec<u8>, i64, i64)> = sqlx::query_as(
            "select latest, latest_hash, safe, finalized from chain_heads where chain_id = $1",
        )
        .bind(chain.get())
        .fetch_optional(&self.pool)
        .await?;
        row.map(heads_from_row).transpose()
    }

    pub async fn all_heads(&self) -> Result<Vec<(ChainId, Heads)>, StoreError> {
        let rows: Vec<(i32, i64, Vec<u8>, i64, i64)> = sqlx::query_as(
            "select chain_id, latest, latest_hash, safe, finalized
             from chain_heads order by chain_id",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (chain_id, latest, hash, safe, finalized) in rows {
            out.push((
                chain_from_db(chain_id)?,
                heads_from_row((latest, hash, safe, finalized))?,
            ));
        }
        Ok(out)
    }

    /// Returns when `kind` is strictly above `after`. Rereads on each `heads` notify for this
    /// chain and at least once a second.
    pub async fn wait_head_change(
        &self,
        chain: ChainId,
        after: u64,
        kind: HeadKind,
        timeout: Duration,
    ) -> Result<Heads, StoreError> {
        let chain_key = chain.get().to_string();
        let store = self;
        let heads = self
            .wait_until(
                timeout,
                |channel, payload| channel == HEADS && payload == chain_key,
                move || async move {
                    Ok(store
                        .heads(chain)
                        .await?
                        .filter(|heads| kind.value(heads) > after))
                },
            )
            .await?;
        heads.ok_or_else(|| StoreError::Timeout(format!("chain {chain} head")))
    }
}

fn heads_from_row(
    (latest, hash, safe, finalized): (i64, Vec<u8>, i64, i64),
) -> Result<Heads, StoreError> {
    Ok(Heads {
        latest: i64_to_u64(latest, "latest")?,
        latest_hash: b256_from_db(&hash)?,
        safe: i64_to_u64(safe, "safe")?,
        finalized: i64_to_u64(finalized, "finalized")?,
    })
}
