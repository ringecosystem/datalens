use datalens_example_degov_client::{
    config::AppConfig, datalens::DatalensDegovClient, db::AppDatabase,
};
use datalens_sdk::DatalensClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::from_env()?;
    let db = AppDatabase::open(&config.database_url)?;
    db.migrate()?;
    let sdk_client = DatalensClient::new(config.sdk_config())?;
    let client = DatalensDegovClient::new(sdk_client);
    let summary = datalens_example_degov_client::run_once(&config, &db, &client)?;

    println!(
        "fetched={} inserted_votes={} skipped_duplicates={} skipped_invalid={} updated_proposals={} checkpoint_cursor={} has_next_page={}",
        summary.fetched_rows,
        summary.inserted_rows,
        summary.skipped_duplicates,
        summary.skipped_invalid,
        summary.updated_proposals,
        summary.checkpoint_cursor.as_deref().unwrap_or("<none>"),
        summary.has_next_page
    );

    Ok(())
}
