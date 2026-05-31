pub mod checkpoint;
pub mod config;
pub mod datalens;
pub mod db;
pub mod error;
pub mod handlers;
pub mod schema;

pub use datalens::fetch_vote_cast_page;
pub use error::{AppError, AppResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSummary {
    pub fetched_rows: usize,
    pub inserted_rows: usize,
    pub skipped_duplicates: usize,
    pub skipped_invalid: usize,
    pub updated_proposals: usize,
    pub checkpoint_cursor: Option<String>,
    pub has_next_page: bool,
}

pub fn run_once(
    config: &config::AppConfig,
    db: &db::AppDatabase,
    client: &datalens::DatalensDegovClient,
) -> AppResult<RunSummary> {
    let stored_cursor = checkpoint::read_checkpoint(db, &config.consumer_name)?;
    let after = config.start_cursor.clone().or(stored_cursor);
    let page = client.fetch_vote_cast_page(after, config.page_size)?;
    let has_next_page = page.has_next_page;
    let handler = handlers::vote_cast::VoteCastHandler::new(&config.consumer_name);
    let summary = handlers::vote_cast::handle_vote_cast_page(db, &handler, page)?;

    Ok(RunSummary {
        fetched_rows: summary.fetched_rows,
        inserted_rows: summary.inserted_rows,
        skipped_duplicates: summary.skipped_duplicates,
        skipped_invalid: summary.skipped_invalid,
        updated_proposals: summary.updated_proposals,
        checkpoint_cursor: summary.checkpoint_cursor,
        has_next_page,
    })
}
