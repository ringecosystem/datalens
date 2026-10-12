use std::collections::BTreeSet;

use datalens_core::{BlockRange, ChainId, Dataset, Finality, LogSelector};
use sqlx::postgres::types::PgRange;

use crate::codec::{block_range_from_pg, finality_from_db};
use crate::ids::SelectorId;
use crate::{PgTx, Store, StoreError};

#[derive(Clone, Copy)]
struct Span {
    start: i64,
    end: i64,
    finality: Finality,
}

impl Store {
    /// Merges the range into this selector's coverage.
    ///
    /// The exclusion constraint forbids overlapping ranges of any finality, so each finality
    /// keeps its own non-overlapping rows. An overlap takes the stronger finality. That lets
    /// promotion upgrade `safe` to `finalized` and refuses to downgrade. The selector row lock
    /// is the serialization point: two adds of one selector must read the same merged timeline.
    pub async fn add_coverage(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        blocks: BlockRange,
        finality: Finality,
    ) -> Result<(), StoreError> {
        if finality < Finality::Safe {
            return Err(StoreError::invalid(
                "coverage records durable finality only",
            ));
        }
        let selector: Option<(i32, String)> =
            sqlx::query_as("select chain_id, dataset from selectors where id = $1 for update")
                .bind(selector_id.get())
                .fetch_optional(&mut **tx)
                .await?;
        let Some((selector_chain, selector_dataset)) = selector else {
            return Err(StoreError::not_found(format!(
                "selector {selector_id} not found"
            )));
        };
        if selector_chain != chain.get() || selector_dataset != dataset.as_str() {
            return Err(StoreError::invalid(
                "coverage chain and dataset must match the selector",
            ));
        }

        let rows: Vec<(PgRange<i64>, i16)> = sqlx::query_as(
            "select blocks, finality from coverage
             where chain_id = $1 and dataset = $2 and selector_id = $3
             order by blocks",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .fetch_all(&mut **tx)
        .await?;
        let mut existing = Vec::with_capacity(rows.len());
        for (blocks, finality) in rows {
            let range = block_range_from_pg(blocks)?;
            existing.push(Span {
                start: range.start() as i64,
                end: range.end() as i64 + 1,
                finality: finality_from_db(finality)?,
            });
        }
        let merged = merge_spans(
            &existing,
            blocks.start() as i64,
            blocks.end() as i64 + 1,
            finality,
        );

        sqlx::query(
            "delete from coverage
             where chain_id = $1 and dataset = $2 and selector_id = $3",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .execute(&mut **tx)
        .await?;
        for span in merged {
            sqlx::query(
                "insert into coverage (chain_id, dataset, selector_id, blocks, finality)
                 values ($1, $2, $3, $4, $5)",
            )
            .bind(chain.get())
            .bind(dataset.as_str())
            .bind(selector_id.get())
            .bind(PgRange::from(span.start..span.end))
            .bind(span.finality.as_i16())
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }

    pub async fn covered(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selectors: &[SelectorId],
        blocks: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        self.range_op(chain, dataset, selectors, blocks, RangeOp::Covered)
            .await
    }

    pub async fn missing(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selectors: &[SelectorId],
        blocks: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        self.range_op(chain, dataset, selectors, blocks, RangeOp::Missing)
            .await
    }

    /// Union of covering candidates. A multi-address query is the intersection of its
    /// per-address parts, because `{A,B}` is covered only where both `{A}` and `{B}` are.
    pub async fn covered_for_query(
        &self,
        chain: ChainId,
        query: &LogSelector,
        blocks: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        let mut acc: Option<Vec<BlockRange>> = None;
        for part in query.split_by_address() {
            let ids: Vec<SelectorId> = self
                .candidates_covering(chain, &part)
                .await?
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            let covered = self.covered(chain, Dataset::EvmLogs, &ids, blocks).await?;
            acc = Some(match acc {
                None => covered,
                Some(previous) => intersect_ranges(&previous, &covered),
            });
        }
        Ok(acc.unwrap_or_default())
    }

    async fn range_op(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selectors: &[SelectorId],
        blocks: BlockRange,
        op: RangeOp,
    ) -> Result<Vec<BlockRange>, StoreError> {
        let ids: Vec<i64> = selectors.iter().map(|id| id.get()).collect();
        let start = blocks.start() as i64;
        let end = blocks.end() as i64 + 1;
        let sql = match op {
            RangeOp::Covered => {
                "select unnest(coalesce(
                    range_agg(blocks) * int8multirange(int8range($3, $4)),
                    '{}'::int8multirange
                 )) as blocks
                 from coverage
                 where chain_id = $1 and dataset = $2 and selector_id = any($5)
                   and blocks && int8range($3, $4)"
            }
            RangeOp::Missing => {
                "select unnest(
                    int8multirange(int8range($3, $4))
                    - coalesce(range_agg(blocks), '{}'::int8multirange)
                 ) as blocks
                 from coverage
                 where chain_id = $1 and dataset = $2 and selector_id = any($5)
                   and blocks && int8range($3, $4)"
            }
        };
        let rows: Vec<(PgRange<i64>,)> = sqlx::query_as(sql)
            .bind(chain.get())
            .bind(dataset.as_str())
            .bind(start)
            .bind(end)
            .bind(&ids)
            .fetch_all(&self.pool)
            .await?;
        let mut ranges = Vec::with_capacity(rows.len());
        for (range,) in rows {
            ranges.push(block_range_from_pg(range)?);
        }
        ranges.sort_by_key(|range| range.start());
        Ok(ranges)
    }
}

#[derive(Clone, Copy)]
enum RangeOp {
    Covered,
    Missing,
}

/// Longest covered prefix of `query` that starts at `query.start`.
/// `covered` is the coalesced result of [`Store::covered`] or [`Store::covered_for_query`].
pub fn longest_covered_prefix(covered: &[BlockRange], query: BlockRange) -> Option<BlockRange> {
    let hit = covered.iter().find(|range| range.contains(query.start()))?;
    let end = hit.end().min(query.end());
    BlockRange::new(query.start(), end).ok()
}

fn merge_spans(existing: &[Span], start: i64, end: i64, finality: Finality) -> Vec<Span> {
    let mut bounds = BTreeSet::from([start, end]);
    for span in existing {
        bounds.insert(span.start);
        bounds.insert(span.end);
    }
    let bounds: Vec<i64> = bounds.into_iter().collect();
    let mut out: Vec<Span> = Vec::new();
    for pair in bounds.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a >= b {
            continue;
        }
        let old = existing
            .iter()
            .find(|span| span.start <= a && a < span.end)
            .map(|span| span.finality);
        let in_new = start <= a && a < end;
        let Some(fin) = (match (old, in_new) {
            (None, false) => None,
            (Some(fin), false) => Some(fin),
            (None, true) => Some(finality),
            (Some(fin), true) => Some(fin.max(finality)),
        }) else {
            continue;
        };
        if let Some(last) = out.last_mut()
            && last.end == a
            && last.finality == fin
        {
            last.end = b;
            continue;
        }
        out.push(Span {
            start: a,
            end: b,
            finality: fin,
        });
    }
    out
}

fn intersect_ranges(left: &[BlockRange], right: &[BlockRange]) -> Vec<BlockRange> {
    let mut out = Vec::new();
    for a in left {
        for b in right {
            if let Some(hit) = a.intersect(b) {
                out.push(hit);
            }
        }
    }
    out
}
