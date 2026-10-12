use datalens_core::{B256, BlockRange, BlockRow, ChainId, Finality};

use crate::codec::{b256_from_db, finality_from_db, i64_to_u64, u64_to_i64};
use crate::{PgTx, Store, StoreError};

/// Keyset cursor for block number order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockCursor {
    pub number: u64,
}

impl Store {
    pub async fn insert_blocks(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        blocks: &[BlockRow],
    ) -> Result<(), StoreError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let mut number = Vec::with_capacity(blocks.len());
        let mut hash = Vec::with_capacity(blocks.len());
        let mut parent_hash = Vec::with_capacity(blocks.len());
        let mut timestamp = Vec::with_capacity(blocks.len());
        let mut finality = Vec::with_capacity(blocks.len());
        let mut header = Vec::with_capacity(blocks.len());
        for block in blocks {
            number.push(u64_to_i64(block.number, "block number")?);
            hash.push(block.hash.to_vec());
            parent_hash.push(block.parent_hash.to_vec());
            timestamp.push(u64_to_i64(block.timestamp, "timestamp")?);
            finality.push(block.finality.as_i16());
            header.push(block.header.clone());
        }
        sqlx::query(
            "insert into evm_blocks (chain_id, number, hash, parent_hash, timestamp, finality, header)
             select $1, number, hash, parent_hash, timestamp, finality, header
             from unnest(
                $2::bigint[], $3::bytea[], $4::bytea[], $5::bigint[], $6::smallint[], $7::jsonb[]
             ) as row(number, hash, parent_hash, timestamp, finality, header)
             on conflict do nothing",
        )
        .bind(chain.get())
        .bind(&number)
        .bind(&hash)
        .bind(&parent_hash)
        .bind(&timestamp)
        .bind(&finality)
        .bind(&header)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn read_blocks(
        &self,
        chain: ChainId,
        blocks: BlockRange,
        page: Option<BlockCursor>,
        limit: u32,
    ) -> Result<(Vec<BlockRow>, Option<BlockCursor>), StoreError> {
        if limit == 0 {
            return Err(StoreError::invalid("block page limit must be positive"));
        }
        let cursor = page
            .map(|cursor| u64_to_i64(cursor.number, "cursor block"))
            .transpose()?;
        let rows = sqlx::query(
            "select number, hash, parent_hash, timestamp, finality, header
             from evm_blocks
             where chain_id = $1
               and number between $2 and $3
               and ($4::bigint is null or number > $4)
             order by number
             limit $5",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(block_from_row(row)?);
        }
        let next = (out.len() > limit as usize).then(|| {
            out.truncate(limit as usize);
            BlockCursor {
                number: out.last().expect("a full page has a last row").number,
            }
        });
        Ok((out, next))
    }

    pub async fn hash_at(&self, chain: ChainId, number: u64) -> Result<Option<B256>, StoreError> {
        let hash: Option<Vec<u8>> =
            sqlx::query_scalar("select hash from evm_blocks where chain_id = $1 and number = $2")
                .bind(chain.get())
                .bind(u64_to_i64(number, "block number")?)
                .fetch_optional(&self.pool)
                .await?;
        hash.as_deref().map(b256_from_db).transpose()
    }

    pub async fn promote_blocks(
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
            "update evm_blocks set finality = $4
             where chain_id = $1 and number between $2 and $3 and finality < $4",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(finality.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn delete_unsafe_blocks_from(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        from_block: u64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "delete from evm_blocks
             where chain_id = $1 and number >= $2 and finality = $3",
        )
        .bind(chain.get())
        .bind(u64_to_i64(from_block, "from block")?)
        .bind(Finality::Unsafe.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

fn block_from_row(row: &sqlx::postgres::PgRow) -> Result<BlockRow, StoreError> {
    use sqlx::Row;
    Ok(BlockRow {
        number: i64_to_u64(row.try_get("number")?, "block number")?,
        hash: b256_from_db(&row.try_get::<Vec<u8>, _>("hash")?)?,
        parent_hash: b256_from_db(&row.try_get::<Vec<u8>, _>("parent_hash")?)?,
        timestamp: i64_to_u64(row.try_get("timestamp")?, "timestamp")?,
        finality: finality_from_db(row.try_get("finality")?)?,
        header: row.try_get("header")?,
    })
}
