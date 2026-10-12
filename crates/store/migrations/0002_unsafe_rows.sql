-- Unsafe rows are a thin band near the head. Reorg rollback, retention, and fork search
-- read only that band, so these partial indexes stay small however large the tables grow.
create index evm_logs_unsafe_idx on evm_logs (chain_id, block_number) where finality = 1;
create index evm_blocks_unsafe_idx on evm_blocks (chain_id, number) where finality = 1;
create index evm_tx_senders_unsafe_idx on evm_tx_senders (chain_id, block_number) where finality = 1;
