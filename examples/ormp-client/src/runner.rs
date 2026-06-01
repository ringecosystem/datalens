use crate::{
    checkpoint, config, datalens, db,
    error::{AppError, AppResult},
    handlers,
};

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
    let stored_checkpoint = if config.reset_checkpoint {
        None
    } else {
        checkpoint::read_checkpoint(db, &config.consumer_name)?
    };
    let start_block = stored_checkpoint
        .as_deref()
        .map(parse_checkpoint_block)
        .transpose()?
        .unwrap_or(config.start_block);
    let target_end = config.end_block.unwrap_or_else(|| {
        start_block
            .saturating_add(i32::try_from(config.chunk_size).unwrap_or(i32::MAX))
            .saturating_sub(1)
    });
    if start_block > target_end {
        return Ok(RunSummary {
            fetched_rows: 0,
            inserted_rows: 0,
            skipped_duplicates: 0,
            skipped_invalid: 0,
            checkpoint_cursor: Some(start_block.to_string()),
            has_next_page: false,
        });
    }
    let chunk_end = start_block
        .saturating_add(i32::try_from(config.chunk_size).unwrap_or(i32::MAX))
        .saturating_sub(1)
        .min(target_end);
    let page = client.fetch_message_accepted_page(config, start_block, chunk_end)?;
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

fn parse_checkpoint_block(value: &str) -> AppResult<i32> {
    value.parse().map_err(|error| {
        AppError::Config(format!(
            "stored ORMP checkpoint must be the next block number: {error}"
        ))
    })
}
