use datalens_example_ormp_client::{
    config::AppConfig, datalens::DatalensOrmpClient, db::AppDatabase,
};
use datalens_sdk::DatalensClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::from_env()?;
    let db = AppDatabase::open(&config.database_url)?;
    db.migrate()?;

    let sdk = DatalensClient::new(config.sdk_config())?;
    let client = DatalensOrmpClient::new(sdk);
    let summary = datalens_example_ormp_client::run_once(&config, &db, &client)?;

    println!(
        "fetched={} inserted={} duplicates={} invalid={} checkpoint={} has_next_page={}",
        summary.fetched_rows,
        summary.inserted_rows,
        summary.skipped_duplicates,
        summary.skipped_invalid,
        summary.checkpoint_cursor.as_deref().unwrap_or("<none>"),
        summary.has_next_page,
    );

    Ok(())
}
