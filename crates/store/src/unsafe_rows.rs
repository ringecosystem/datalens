use datalens_core::{B256, BlockRange, ChainId, Finality};

use crate::codec::{b256_from_db, i64_to_u64, u64_to_i64};
use crate::{Store, StoreError};

impl Store {
    /// Distinct `(block, hash)` pairs of unsafe logs and blocks in the range, in block order.
    /// Two pairs at one height mean the stored rows disagree.
    pub async fn unsafe_hashes(
        &self,
        chain: ChainId,
        blocks: BlockRange,
    ) -> Result<Vec<(u64, B256)>, StoreError> {
        let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "select block_number, block_hash from evm_logs
             where chain_id = $1 and finality = $4 and block_number between $2 and $3
             union
             select number, hash from evm_blocks
             where chain_id = $1 and finality = $4 and number between $2 and $3
             order by 1, 2",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(Finality::Unsafe.as_i16())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(number, hash)| Ok((i64_to_u64(number, "block number")?, b256_from_db(&hash)?)))
            .collect()
    }

    /// Deletes unsafe logs, blocks, and senders at or below `through` in one transaction and
    /// returns how many rows went.
    pub async fn delete_unsafe_through(
        &self,
        chain: ChainId,
        through: u64,
    ) -> Result<u64, StoreError> {
        let through = u64_to_i64(through, "block number")?;
        let mut tx = self.pool.begin().await?;
        let mut deleted = 0;
        for sql in [
            "delete from evm_logs where chain_id = $1 and finality = $2 and block_number <= $3",
            "delete from evm_blocks where chain_id = $1 and finality = $2 and number <= $3",
            "delete from evm_tx_senders where chain_id = $1 and finality = $2 and block_number <= $3",
        ] {
            deleted += sqlx::query(sql)
                .bind(chain.get())
                .bind(Finality::Unsafe.as_i16())
                .bind(through)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        }
        tx.commit().await?;
        Ok(deleted)
    }
}
