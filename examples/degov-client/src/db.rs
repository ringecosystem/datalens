use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::{AppError, AppResult};

const INIT_MIGRATION: &str = include_str!("../migrations/0001_init.sql");

pub struct AppDatabase {
    connection: RefCell<Connection>,
}

impl AppDatabase {
    pub fn open(url: &str) -> AppResult<Self> {
        let connection = if url == "sqlite::memory:" || url.contains("mode=memory") {
            Connection::open_in_memory()?
        } else {
            let path = sqlite_path(url)?;
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent)?;
            }
            Connection::open(path)?
        };

        Ok(Self {
            connection: RefCell::new(connection),
        })
    }

    pub fn migrate(&self) -> AppResult<()> {
        self.connection.borrow().execute_batch(INIT_MIGRATION)?;
        Ok(())
    }

    pub fn transaction<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> AppResult<T>,
    ) -> AppResult<T> {
        let mut connection = self.connection.borrow_mut();
        let tx = connection.transaction()?;
        let result = action(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn table_exists(&self, name: &str) -> AppResult<bool> {
        let exists = self
            .connection
            .borrow()
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn index_exists(&self, name: &str) -> AppResult<bool> {
        let exists = self
            .connection
            .borrow()
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn vote_count(&self) -> AppResult<i64> {
        Ok(self
            .connection
            .borrow()
            .query_row("SELECT COUNT(*) FROM degov_votes", [], |row| row.get(0))?)
    }

    pub fn proposal_totals(&self, proposal_id: &str) -> AppResult<Option<(i64, i64, i64)>> {
        Ok(self
            .connection
            .borrow()
            .query_row(
                "SELECT for_votes, against_votes, abstain_votes
                 FROM degov_proposals
                 WHERE proposal_id = ?1",
                [proposal_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?)
    }

    pub fn checkpoint(&self, consumer_name: &str) -> AppResult<Option<String>> {
        Ok(self
            .connection
            .borrow()
            .query_row(
                "SELECT cursor FROM consumer_checkpoints WHERE consumer_name = ?1",
                [consumer_name],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn insert_checkpoint(&self, consumer_name: &str, cursor: &str) -> AppResult<()> {
        self.connection.borrow().execute(
            "INSERT INTO consumer_checkpoints (consumer_name, cursor, updated_at)
             VALUES (?1, ?2, CURRENT_TIMESTAMP)
             ON CONFLICT(consumer_name) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at = CURRENT_TIMESTAMP",
            [consumer_name, cursor],
        )?;
        Ok(())
    }
}

fn sqlite_path(url: &str) -> AppResult<PathBuf> {
    let path = url.strip_prefix("sqlite:").unwrap_or(url);
    if path.is_empty() {
        return Err(AppError::Config(
            "DEGOV_DATABASE_URL cannot be empty".to_owned(),
        ));
    }
    Ok(Path::new(path).to_path_buf())
}
