use std::collections::{HashMap, HashSet};

use datalens_core::{Address, B256, ChainId};

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
        rows: &[(B256, u64, Address)],
    ) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut hashes = Vec::with_capacity(rows.len());
        let mut blocks = Vec::with_capacity(rows.len());
        let mut senders = Vec::with_capacity(rows.len());
        for (hash, block, sender) in rows {
            hashes.push(hash.to_vec());
            blocks.push(u64_to_i64(*block, "block number")?);
            senders.push(sender.to_vec());
        }
        sqlx::query(
            "insert into evm_tx_senders (chain_id, transaction_hash, block_number, sender)
             select $1, hash, block_number, sender
             from unnest($2::bytea[], $3::bigint[], $4::bytea[]) as row(hash, block_number, sender)
             on conflict do nothing",
        )
        .bind(chain.get())
        .bind(&hashes)
        .bind(&blocks)
        .bind(&senders)
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

    pub async fn delete_senders_from(
        &self,
        tx: &mut PgTx<'_>,
        chain: ChainId,
        from_block: u64,
    ) -> Result<(), StoreError> {
        sqlx::query("delete from evm_tx_senders where chain_id = $1 and block_number >= $2")
            .bind(chain.get())
            .bind(u64_to_i64(from_block, "from block")?)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}
