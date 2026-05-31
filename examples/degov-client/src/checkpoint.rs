use rusqlite::Transaction;

use crate::{AppResult, db::AppDatabase};

pub fn read_checkpoint(db: &AppDatabase, consumer_name: &str) -> AppResult<Option<String>> {
    db.checkpoint(consumer_name)
}

pub fn write_checkpoint(tx: &Transaction<'_>, consumer_name: &str, cursor: &str) -> AppResult<()> {
    tx.execute(
        "INSERT INTO consumer_checkpoints (consumer_name, cursor, updated_at)
         VALUES (?1, ?2, CURRENT_TIMESTAMP)
         ON CONFLICT(consumer_name) DO UPDATE SET
            cursor = excluded.cursor,
            updated_at = CURRENT_TIMESTAMP",
        (consumer_name, cursor),
    )?;
    Ok(())
}
