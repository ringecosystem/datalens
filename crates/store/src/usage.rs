use std::collections::BTreeMap;

use chrono::NaiveDate;
use datalens_core::{ChainId, Dataset};

use crate::{Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageDelta {
    pub day: NaiveDate,
    pub application_id: String,
    pub chain_id: ChainId,
    pub dataset: Dataset,
    pub queries: i64,
    pub rows_returned: i64,
    pub blocks_served: i64,
    pub blocks_filled: i64,
    pub provider_calls: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UsageTotals {
    pub queries: i64,
    pub rows_returned: i64,
    pub blocks_served: i64,
    pub blocks_filled: i64,
    pub provider_calls: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsageKey {
    day: NaiveDate,
    application_id: String,
    chain_id: i32,
    dataset: Dataset,
}

impl Store {
    /// Sums deltas that share a key, then upserts. One statement must not touch a key twice.
    pub async fn add_usage(&self, deltas: &[UsageDelta]) -> Result<(), StoreError> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut folded: BTreeMap<UsageKey, [i64; 5]> = BTreeMap::new();
        for delta in deltas {
            let key = UsageKey {
                day: delta.day,
                application_id: delta.application_id.clone(),
                chain_id: delta.chain_id.get(),
                dataset: delta.dataset,
            };
            let totals = folded.entry(key).or_default();
            totals[0] += delta.queries;
            totals[1] += delta.rows_returned;
            totals[2] += delta.blocks_served;
            totals[3] += delta.blocks_filled;
            totals[4] += delta.provider_calls;
        }
        let mut days = Vec::with_capacity(folded.len());
        let mut applications = Vec::with_capacity(folded.len());
        let mut chains = Vec::with_capacity(folded.len());
        let mut datasets = Vec::with_capacity(folded.len());
        let mut queries = Vec::with_capacity(folded.len());
        let mut rows_returned = Vec::with_capacity(folded.len());
        let mut blocks_served = Vec::with_capacity(folded.len());
        let mut blocks_filled = Vec::with_capacity(folded.len());
        let mut provider_calls = Vec::with_capacity(folded.len());
        for (key, totals) in folded {
            days.push(key.day);
            applications.push(key.application_id);
            chains.push(key.chain_id);
            datasets.push(key.dataset.as_str().to_owned());
            queries.push(totals[0]);
            rows_returned.push(totals[1]);
            blocks_served.push(totals[2]);
            blocks_filled.push(totals[3]);
            provider_calls.push(totals[4]);
        }
        sqlx::query(
            "insert into usage_daily (
                day, application_id, chain_id, dataset, queries, rows_returned,
                blocks_served, blocks_filled, provider_calls
             )
             select * from unnest(
                $1::date[], $2::text[], $3::int[], $4::text[],
                $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[], $9::bigint[]
             )
             on conflict (day, application_id, chain_id, dataset) do update set
                queries = usage_daily.queries + excluded.queries,
                rows_returned = usage_daily.rows_returned + excluded.rows_returned,
                blocks_served = usage_daily.blocks_served + excluded.blocks_served,
                blocks_filled = usage_daily.blocks_filled + excluded.blocks_filled,
                provider_calls = usage_daily.provider_calls + excluded.provider_calls",
        )
        .bind(&days)
        .bind(&applications)
        .bind(&chains)
        .bind(&datasets)
        .bind(&queries)
        .bind(&rows_returned)
        .bind(&blocks_served)
        .bind(&blocks_filled)
        .bind(&provider_calls)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn usage(
        &self,
        day: NaiveDate,
        application_id: &str,
        chain: ChainId,
        dataset: Dataset,
    ) -> Result<UsageTotals, StoreError> {
        let row: Option<(i64, i64, i64, i64, i64)> = sqlx::query_as(
            "select queries, rows_returned, blocks_served, blocks_filled, provider_calls
             from usage_daily
             where day = $1 and application_id = $2 and chain_id = $3 and dataset = $4",
        )
        .bind(day)
        .bind(application_id)
        .bind(chain.get())
        .bind(dataset.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some((queries, rows_returned, blocks_served, blocks_filled, provider_calls)) => {
                UsageTotals {
                    queries,
                    rows_returned,
                    blocks_served,
                    blocks_filled,
                    provider_calls,
                }
            }
            None => UsageTotals::default(),
        })
    }
}
