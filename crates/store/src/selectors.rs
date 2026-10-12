use datalens_core::{ChainId, LogSelector, Selector};

use crate::codec::{addresses_from_db, addresses_to_db, topics_from_db, topics_to_db};
use crate::ids::SelectorId;
use crate::{Store, StoreError};

impl Store {
    /// Looks the selector up first, so the hot path is one round trip and does not consume a
    /// sequence value. `last_used_at` moves at most once a minute to avoid rewriting hot rows.
    pub async fn upsert_selector(
        &self,
        chain: ChainId,
        selector: &Selector,
    ) -> Result<SelectorId, StoreError> {
        let (addresses, topics) = match selector {
            Selector::Logs(logs) => (
                addresses_to_db(&logs.addresses),
                topics_to_db(&logs.topics)?,
            ),
            Selector::Blocks => (Vec::new(), serde_json::json!([])),
        };
        let found: Option<i64> = sqlx::query_scalar(
            "with hit as (
                select id from selectors
                where chain_id = $1 and dataset = $2 and fingerprint = $3
             ),
             touched as (
                update selectors set last_used_at = now()
                where id = (select id from hit) and last_used_at < now() - interval '60 seconds'
             )
             select id from hit",
        )
        .bind(chain.get())
        .bind(selector.dataset().as_str())
        .bind(selector.fingerprint())
        .fetch_optional(&self.pool)
        .await?;
        if let Some(id) = found {
            return Ok(SelectorId::from_raw(id));
        }
        let inserted: Option<i64> = sqlx::query_scalar(
            "insert into selectors (chain_id, dataset, fingerprint, canonical_key, addresses, topics)
             values ($1, $2, $3, $4, $5, $6)
             on conflict (chain_id, dataset, fingerprint) do nothing
             returning id",
        )
        .bind(chain.get())
        .bind(selector.dataset().as_str())
        .bind(selector.fingerprint())
        .bind(selector.canonical_key())
        .bind(&addresses)
        .bind(&topics)
        .fetch_optional(&self.pool)
        .await?;
        let id = match inserted {
            Some(id) => id,
            None => {
                sqlx::query_scalar(
                    "select id from selectors
                     where chain_id = $1 and dataset = $2 and fingerprint = $3",
                )
                .bind(chain.get())
                .bind(selector.dataset().as_str())
                .bind(selector.fingerprint())
                .fetch_one(&self.pool)
                .await?
            }
        };
        Ok(SelectorId::from_raw(id))
    }

    /// GIN prefilter, then [`LogSelector::covers`] for topic position and address subset.
    pub async fn candidates_covering(
        &self,
        chain: ChainId,
        query: &LogSelector,
    ) -> Result<Vec<(SelectorId, LogSelector)>, StoreError> {
        let rows: Vec<(i64, Vec<Vec<u8>>, serde_json::Value)> = sqlx::query_as(
            "select id, addresses, topics
             from selectors
             where chain_id = $1
               and dataset = 'evm.logs'
               and (addresses = '{}' or addresses @> $2)
             order by id",
        )
        .bind(chain.get())
        .bind(addresses_to_db(&query.addresses))
        .fetch_all(&self.pool)
        .await?;
        let mut candidates = Vec::new();
        for (id, addresses, topics) in rows {
            let selector = LogSelector {
                addresses: addresses_from_db(addresses)?,
                topics: topics_from_db(topics)?,
            };
            if selector.covers(query) {
                candidates.push((SelectorId::from_raw(id), selector));
            }
        }
        Ok(candidates)
    }
}
