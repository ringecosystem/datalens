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
    pub updated_proposals: usize,
    pub checkpoint_cursor: Option<String>,
    pub has_next_page: bool,
}

pub fn run_once(
    config: &config::AppConfig,
    db: &db::AppDatabase,
    client: &datalens::DatalensDegovClient,
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
            updated_proposals: 0,
            checkpoint_cursor: Some(start_block.to_string()),
            has_next_page: false,
        });
    }
    let chunk_end = start_block
        .saturating_add(i32::try_from(config.chunk_size).unwrap_or(i32::MAX))
        .saturating_sub(1)
        .min(target_end);
    let page = client.fetch_vote_cast_page(config, start_block, chunk_end)?;
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

pub fn run_until_complete(
    config: &config::AppConfig,
    db: &db::AppDatabase,
    client: &datalens::DatalensDegovClient,
) -> AppResult<RunSummary> {
    let mut page_config = config.clone();
    let mut total = RunSummary {
        fetched_rows: 0,
        inserted_rows: 0,
        skipped_duplicates: 0,
        skipped_invalid: 0,
        updated_proposals: 0,
        checkpoint_cursor: None,
        has_next_page: false,
    };

    loop {
        let page = run_once_with_retry(&page_config, db, client)?;
        total.fetched_rows += page.fetched_rows;
        total.inserted_rows += page.inserted_rows;
        total.skipped_duplicates += page.skipped_duplicates;
        total.skipped_invalid += page.skipped_invalid;
        total.updated_proposals += page.updated_proposals;
        total.checkpoint_cursor = page.checkpoint_cursor.clone();
        total.has_next_page = page.has_next_page;

        if !page.has_next_page {
            break;
        }
        page_config.reset_checkpoint = false;
    }

    Ok(total)
}

fn run_once_with_retry(
    config: &config::AppConfig,
    db: &db::AppDatabase,
    client: &datalens::DatalensDegovClient,
) -> AppResult<RunSummary> {
    let max_attempts = std::env::var("DEGOV_PAGE_RETRIES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3);
    let backoff_ms = std::env::var("DEGOV_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(250);

    let mut attempt = 1;
    loop {
        match run_once(config, db, client) {
            Ok(summary) => return Ok(summary),
            Err(error) if attempt < max_attempts && is_retryable(&error) => {
                attempt += 1;
                if backoff_ms > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_retryable(error: &AppError) -> bool {
    if matches!(error, AppError::Datalens(_) | AppError::Io(_)) {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("provider_failure")
        || message.contains("transport")
        || message.contains("timeout")
        || message.contains("temporarily unavailable")
}

fn parse_checkpoint_block(value: &str) -> AppResult<i32> {
    value.parse().map_err(|error| {
        AppError::Config(format!(
            "stored Degov checkpoint must be the next block number: {error}"
        ))
    })
}
