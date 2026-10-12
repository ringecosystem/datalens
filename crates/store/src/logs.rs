use datalens_core::{BlockRange, Bytes, ChainId, Finality, LogRow, LogSelector};

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
             on conflict do nothing",
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
        let addresses: Vec<Vec<u8>> = selector.addresses.iter().map(|a| a.to_vec()).collect();
        let any_address = selector.addresses.is_empty();
        let topics: Vec<(bool, Vec<Vec<u8>>)> = selector
            .topics
            .iter()
            .map(|filter| match filter {
                datalens_core::TopicFilter::Any => (true, Vec::new()),
                datalens_core::TopicFilter::OneOf(set) => {
                    (false, set.iter().map(|topic| topic.to_vec()).collect())
                }
            })
            .collect();
        let cursor_block = page
            .map(|cursor| u64_to_i64(cursor.block_number, "cursor block"))
            .transpose()?;
        let cursor_index = page
            .map(|cursor| u32_to_i32(cursor.log_index, "cursor log index"))
            .transpose()?;
        let rows = sqlx::query(
            "select block_number, log_index, block_hash, transaction_index, transaction_hash,
                    address, topic0, topic1, topic2, topic3, data, finality
             from evm_logs
             where chain_id = $1
               and block_number between $2 and $3
               and ($4::bool or address = any($5))
               and ($6::bool or topic0 = any($7))
               and ($8::bool or topic1 = any($9))
               and ($10::bool or topic2 = any($11))
               and ($12::bool or topic3 = any($13))
               and (
                    $14::bigint is null
                    or (block_number, log_index) > ($14, $15)
               )
             order by block_number, log_index
             limit $16",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(any_address)
        .bind(&addresses)
        .bind(topics[0].0)
        .bind(&topics[0].1)
        .bind(topics[1].0)
        .bind(&topics[1].1)
        .bind(topics[2].0)
        .bind(&topics[2].1)
        .bind(topics[3].0)
        .bind(&topics[3].1)
        .bind(cursor_block)
        .bind(cursor_index)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
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

    /// Raises every weaker row in the range to `finality`. `safe` can become `finalized`.
    pub async fn promote_logs(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        blocks: BlockRange,
        finality: Finality,
    ) -> Result<(), StoreError> {
        if finality == Finality::Unsafe {
            return Err(StoreError::invalid("promote target must be durable"));
        }
        sqlx::query(
            "update evm_logs set finality = $4
             where chain_id = $1 and block_number between $2 and $3 and finality < $4",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(finality.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(())
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
