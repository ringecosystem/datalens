use datalens_core::{B256, BlockRange, Bytes, ChainId, Finality, LogRow, LogSelector, TopicFilter};
use sqlx::{Postgres, QueryBuilder};

use crate::blocks::hash_conflict;
use crate::codec::{
    address_from_db, b256_from_db, finality_from_db, i32_to_u32, i64_to_u64, topic_columns,
    topics_from_columns, u32_to_i32, u64_to_i64,
};
use crate::{PgTx, Store, StoreError};

/// Keyset cursor for `(block_number, log_index)` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogCursor {
    pub block_number: u64,
    pub log_index: u32,
}

impl Store {
    pub async fn insert_logs(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        logs: &[LogRow],
    ) -> Result<(), StoreError> {
        if logs.is_empty() {
            return Ok(());
        }
        let mut block_number = Vec::with_capacity(logs.len());
        let mut log_index = Vec::with_capacity(logs.len());
        let mut block_hash = Vec::with_capacity(logs.len());
        let mut transaction_index = Vec::with_capacity(logs.len());
        let mut transaction_hash = Vec::with_capacity(logs.len());
        let mut address = Vec::with_capacity(logs.len());
        let mut topic0 = Vec::with_capacity(logs.len());
        let mut topic1 = Vec::with_capacity(logs.len());
        let mut topic2 = Vec::with_capacity(logs.len());
        let mut topic3 = Vec::with_capacity(logs.len());
        let mut data = Vec::with_capacity(logs.len());
        let mut finality = Vec::with_capacity(logs.len());
        for log in logs {
            let topics = topic_columns(&log.topics)?;
            block_number.push(u64_to_i64(log.block_number, "block number")?);
            log_index.push(u32_to_i32(log.log_index, "log index")?);
            block_hash.push(log.block_hash.to_vec());
            transaction_index.push(u32_to_i32(log.transaction_index, "transaction index")?);
            transaction_hash.push(log.transaction_hash.to_vec());
            address.push(log.address.to_vec());
            topic0.push(topics[0].clone());
            topic1.push(topics[1].clone());
            topic2.push(topics[2].clone());
            topic3.push(topics[3].clone());
            data.push(log.data.to_vec());
            finality.push(log.finality.as_i16());
        }
        if let Some(block) = hash_conflict(tx, chain, &block_number, &block_hash).await? {
            return Err(StoreError::Reorg { block });
        }
        sqlx::query(
            "insert into evm_logs (
                chain_id, block_number, log_index, block_hash, transaction_index,
                transaction_hash, address, topic0, topic1, topic2, topic3, data, finality
             )
             select $1, block_number, log_index, block_hash, transaction_index,
                    transaction_hash, address, topic0, topic1, topic2, topic3, data, finality
             from unnest(
                $2::bigint[], $3::int[], $4::bytea[], $5::int[], $6::bytea[], $7::bytea[],
                $8::bytea[], $9::bytea[], $10::bytea[], $11::bytea[], $12::bytea[], $13::smallint[]
             ) as row(
                block_number, log_index, block_hash, transaction_index, transaction_hash,
                address, topic0, topic1, topic2, topic3, data, finality
             )
             on conflict (chain_id, block_number, log_index) do update
             set finality = excluded.finality
             where evm_logs.finality < excluded.finality
               and evm_logs.block_hash = excluded.block_hash",
        )
        .bind(chain.get())
        .bind(&block_number)
        .bind(&log_index)
        .bind(&block_hash)
        .bind(&transaction_index)
        .bind(&transaction_hash)
        .bind(&address)
        .bind(&topic0)
        .bind(&topic1)
        .bind(&topic2)
        .bind(&topic3)
        .bind(&data)
        .bind(&finality)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn read_logs(
        &self,
        chain: ChainId,
        selector: &LogSelector,
        blocks: BlockRange,
        page: Option<LogCursor>,
        limit: u32,
    ) -> Result<(Vec<LogRow>, Option<LogCursor>), StoreError> {
        if limit == 0 {
            return Err(StoreError::invalid("log page limit must be positive"));
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "select block_number, log_index, block_hash, transaction_index, transaction_hash,
                    address, topic0, topic1, topic2, topic3, data, finality
             from evm_logs
             where chain_id = ",
        );
        query
            .push_bind(chain.get())
            .push(" and block_number between ")
            .push_bind(u64_to_i64(blocks.start(), "block start")?)
            .push(" and ")
            .push_bind(u64_to_i64(blocks.end(), "block end")?);
        push_selector_filter(&mut query, "", selector);
        if let Some(cursor) = page {
            query
                .push(" and (block_number, log_index) > (")
                .push_bind(u64_to_i64(cursor.block_number, "cursor block")?)
                .push(", ")
                .push_bind(u32_to_i32(cursor.log_index, "cursor log index")?)
                .push(")");
        }
        query
            .push(" order by block_number, log_index limit ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let mut logs = Vec::with_capacity(rows.len());
        for row in &rows {
            logs.push(log_from_row(row)?);
        }
        let next = (logs.len() > limit as usize).then(|| {
            logs.truncate(limit as usize);
            let last = logs.last().expect("a full page has a last row");
            LogCursor {
                block_number: last.block_number,
                log_index: last.log_index,
            }
        });
        Ok((logs, next))
    }

    /// Raises weaker rows matching `selector` in the range to `finality` and returns how many
    /// changed. `safe` can become `finalized`.
    ///
    /// Rows of other selectors stay as they are, because only this selector's follow group
    /// vouched for them. `end_hash` is the canonical hash of `blocks.end` the caller verified.
    /// A matching row at `blocks.end` with another hash, or one that disagrees with the stored
    /// block at its height, is [`StoreError::Reorg`] and nothing is promoted.
    pub async fn promote_logs(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        selector: &LogSelector,
        blocks: BlockRange,
        finality: Finality,
        end_hash: B256,
    ) -> Result<u64, StoreError> {
        if finality == Finality::Unsafe {
            return Err(StoreError::invalid("promote target must be durable"));
        }
        let start = u64_to_i64(blocks.start(), "block start")?;
        let end = u64_to_i64(blocks.end(), "block end")?;

        let mut check = QueryBuilder::<Postgres>::new(
            "select min(l.block_number) from evm_logs l
             left join evm_blocks b on b.chain_id = l.chain_id and b.number = l.block_number
             where l.chain_id = ",
        );
        check
            .push_bind(chain.get())
            .push(" and l.block_number between ")
            .push_bind(start)
            .push(" and ")
            .push_bind(end)
            .push(" and (b.hash <> l.block_hash or (l.block_number = ")
            .push_bind(end)
            .push(" and l.block_hash <> ")
            .push_bind(end_hash.to_vec())
            .push("))");
        push_selector_filter(&mut check, "l.", selector);
        let broken: Option<i64> = check.build_query_scalar().fetch_one(&mut **tx).await?;
        if let Some(block) = broken {
            return Err(StoreError::Reorg {
                block: i64_to_u64(block, "block number")?,
            });
        }

        let mut update = QueryBuilder::<Postgres>::new("update evm_logs set finality = ");
        update
            .push_bind(finality.as_i16())
            .push(" where chain_id = ")
            .push_bind(chain.get())
            .push(" and block_number between ")
            .push_bind(start)
            .push(" and ")
            .push_bind(end)
            .push(" and finality < ")
            .push_bind(finality.as_i16());
        push_selector_filter(&mut update, "", selector);
        let result = update.build().execute(&mut **tx).await?;
        Ok(result.rows_affected())
    }

    pub async fn delete_unsafe_logs_from(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        from_block: u64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "delete from evm_logs
             where chain_id = $1 and block_number >= $2 and finality = $3",
        )
        .bind(chain.get())
        .bind(u64_to_i64(from_block, "from block")?)
        .bind(Finality::Unsafe.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// Appends `and ...` predicates for the selector's constrained positions only. Separate SQL per
/// shape lets the planner use the address index, which an `$flag or col = any(..)` guard defeats
/// once sqlx's cached statement switches to a generic plan.
fn push_selector_filter(qb: &mut QueryBuilder<Postgres>, alias: &str, selector: &LogSelector) {
    let addresses: Vec<Vec<u8>> = selector.addresses.iter().map(|a| a.to_vec()).collect();
    match addresses.as_slice() {
        [] => {}
        [one] => {
            qb.push(format_args!(" and {alias}address = "))
                .push_bind(one.clone());
        }
        _ => {
            qb.push(format_args!(" and {alias}address = any("))
                .push_bind(addresses)
                .push(")");
        }
    }
    for (position, filter) in selector.topics.iter().enumerate() {
        if let TopicFilter::OneOf(set) = filter {
            let topics: Vec<Vec<u8>> = set.iter().map(|topic| topic.to_vec()).collect();
            qb.push(format_args!(" and {alias}topic{position} = any("))
                .push_bind(topics)
                .push(")");
        }
    }
}

fn log_from_row(row: &sqlx::postgres::PgRow) -> Result<LogRow, StoreError> {
    use sqlx::Row;
    let topic0: Option<Vec<u8>> = row.try_get("topic0")?;
    let topic1: Option<Vec<u8>> = row.try_get("topic1")?;
    let topic2: Option<Vec<u8>> = row.try_get("topic2")?;
    let topic3: Option<Vec<u8>> = row.try_get("topic3")?;
    let data: Vec<u8> = row.try_get("data")?;
    Ok(LogRow {
        block_number: i64_to_u64(row.try_get("block_number")?, "block number")?,
        log_index: i32_to_u32(row.try_get("log_index")?, "log index")?,
        block_hash: b256_from_db(&row.try_get::<Vec<u8>, _>("block_hash")?)?,
        transaction_index: i32_to_u32(row.try_get("transaction_index")?, "transaction index")?,
        transaction_hash: b256_from_db(&row.try_get::<Vec<u8>, _>("transaction_hash")?)?,
        address: address_from_db(&row.try_get::<Vec<u8>, _>("address")?)?,
        topics: topics_from_columns([topic0, topic1, topic2, topic3])?,
        data: Bytes::from(data),
        finality: finality_from_db(row.try_get("finality")?)?,
    })
}
