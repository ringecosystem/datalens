use std::collections::{HashMap, HashSet};

use datalens_core::{Address, B256, BlockRange, ChainId, Finality};

use crate::codec::{address_from_db, b256_from_db, u64_to_i64};
use crate::{PgTx, Store, StoreError};

impl Store {
    /// Input order, with duplicates removed.
    pub async fn missing_hashes(
        &self,
        chain: ChainId,
        hashes: &[B256],
    ) -> Result<Vec<B256>, StoreError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let raw: Vec<Vec<u8>> = hashes.iter().map(|hash| hash.to_vec()).collect();
        let present_rows: Vec<Vec<u8>> = sqlx::query_scalar(
            "select transaction_hash from evm_tx_senders
             where chain_id = $1 and transaction_hash = any($2)",
        )
        .bind(chain.get())
        .bind(&raw)
        .fetch_all(&self.pool)
        .await?;
        let mut present = HashSet::with_capacity(present_rows.len());
        for row in present_rows {
            present.insert(b256_from_db(&row)?);
        }
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        for hash in hashes {
            if seen.insert(*hash) && !present.contains(hash) {
                missing.push(*hash);
            }
        }
        Ok(missing)
    }

    pub async fn insert_senders(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        rows: &[(B256, u64, Address, Finality)],
    ) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut hashes = Vec::with_capacity(rows.len());
        let mut blocks = Vec::with_capacity(rows.len());
        let mut senders = Vec::with_capacity(rows.len());
        let mut finality = Vec::with_capacity(rows.len());
        for (hash, block, sender, row_finality) in rows {
            hashes.push(hash.to_vec());
            blocks.push(u64_to_i64(*block, "block number")?);
            senders.push(sender.to_vec());
            finality.push(row_finality.as_i16());
        }
        sqlx::query(
            "insert into evm_tx_senders
                (chain_id, transaction_hash, block_number, sender, finality)
             select $1, hash, block_number, sender, finality
             from unnest($2::bytea[], $3::bigint[], $4::bytea[], $5::smallint[])
               as row(hash, block_number, sender, finality)
             on conflict (chain_id, transaction_hash) do update
             set finality = excluded.finality
             where evm_tx_senders.finality < excluded.finality
               and evm_tx_senders.block_number = excluded.block_number
               and evm_tx_senders.sender = excluded.sender",
        )
        .bind(chain.get())
        .bind(&hashes)
        .bind(&blocks)
        .bind(&senders)
        .bind(&finality)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn senders(
        &self,
        chain: ChainId,
        hashes: &[B256],
    ) -> Result<HashMap<B256, Address>, StoreError> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }
        let raw: Vec<Vec<u8>> = hashes.iter().map(|hash| hash.to_vec()).collect();
        let rows: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "select transaction_hash, sender from evm_tx_senders
             where chain_id = $1 and transaction_hash = any($2)",
        )
        .bind(chain.get())
        .bind(&raw)
        .fetch_all(&self.pool)
        .await?;
        let mut out = HashMap::with_capacity(rows.len());
        for (hash, sender) in rows {
            out.insert(b256_from_db(&hash)?, address_from_db(&sender)?);
        }
        Ok(out)
    }

    pub async fn delete_unsafe_senders_from(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        from_block: u64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "delete from evm_tx_senders
             where chain_id = $1 and block_number >= $2 and finality = $3",
        )
        .bind(chain.get())
        .bind(u64_to_i64(from_block, "from block")?)
        .bind(Finality::Unsafe.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Raises weaker senders in the range to `finality` and returns how many changed.
    ///
    /// A sender row has no block hash. The caller runs this in the same transaction as the
    /// log or block promotion that already verified the range, so a reorg rolls the raise back.
    pub async fn promote_senders(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        blocks: BlockRange,
        finality: Finality,
    ) -> Result<u64, StoreError> {
        if finality == Finality::Unsafe {
            return Err(StoreError::invalid("promote target must be durable"));
        }
        let result = sqlx::query(
            "update evm_tx_senders set finality = $4
             where chain_id = $1 and block_number between $2 and $3 and finality < $4",
        )
        .bind(chain.get())
        .bind(u64_to_i64(blocks.start(), "block start")?)
        .bind(u64_to_i64(blocks.end(), "block end")?)
        .bind(finality.as_i16())
        .execute(&mut **tx)
        .await?;
        Ok(result.rows_affected())
    }
}
