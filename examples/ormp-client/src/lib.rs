pub mod checkpoint;
pub mod config;
pub mod datalens;
pub mod db;
pub mod error;
pub mod handlers;
pub mod schema;

pub use datalens::fetch_message_accepted_page;
pub use error::{AppError, AppResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSummary {
    pub fetched_rows: usize,
    pub inserted_rows: usize,
    pub skipped_duplicates: usize,
    pub skipped_invalid: usize,
    pub checkpoint_cursor: Option<String>,
    pub has_next_page: bool,
}

pub fn run_once(
    config: &config::AppConfig,
    db: &db::AppDatabase,
    client: &datalens::DatalensOrmpClient,
) -> AppResult<RunSummary> {
    let stored_cursor = checkpoint::read_checkpoint(db, &config.consumer_name)?;
    let after = config.start_cursor.clone().or(stored_cursor);
    let page = client.fetch_message_accepted_page(after, config.page_size)?;
    let has_next_page = page.has_next_page;
    let handler = handlers::message_accepted::MessageAcceptedHandler::new(&config.consumer_name);
    let summary = handlers::message_accepted::handle_message_accepted_page(db, &handler, page)?;

    Ok(RunSummary {
        fetched_rows: summary.fetched_rows,
        inserted_rows: summary.inserted_rows,
        skipped_duplicates: summary.skipped_duplicates,
        skipped_invalid: summary.skipped_invalid,
        checkpoint_cursor: summary.checkpoint_cursor,
        has_next_page,
    })
}
