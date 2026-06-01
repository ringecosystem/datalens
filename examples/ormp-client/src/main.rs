use datalens_example_ormp_client::{
    RunSummary,
    config::{AppConfig, OrmpFixtureFile},
    datalens::DatalensOrmpClient,
    db::AppDatabase,
};
use datalens_sdk::DatalensClient;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("ORMP_FIXTURES_PATH") {
        run_fixtures(&path)?;
    } else {
        let config = AppConfig::from_env()?;
        let started_at = Instant::now();
        let summary = run_config(&config)?;
        print_summary(None, &config, &summary, started_at.elapsed().as_millis());
    }

    Ok(())
}

fn run_fixtures(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let fixtures = OrmpFixtureFile::from_path(path)?;
    for workload in &fixtures.workloads {
        let config = AppConfig::from_fixture_workload(workload)?;
        let started_at = Instant::now();
        let summary = run_config_until_complete(&config)?;
        print_summary(
            Some(&workload.name),
            &config,
            &summary,
            started_at.elapsed().as_millis(),
        );
    }
    Ok(())
}

fn run_config(config: &AppConfig) -> Result<RunSummary, Box<dyn std::error::Error>> {
    let db = AppDatabase::open(&config.database_url)?;
    db.migrate()?;
    let sdk = DatalensClient::new(config.sdk_config())?;
    let client = DatalensOrmpClient::new(sdk);
    let summary = datalens_example_ormp_client::run_once(config, &db, &client)?;

    Ok(summary)
}

fn run_config_until_complete(config: &AppConfig) -> Result<RunSummary, Box<dyn std::error::Error>> {
    let db = AppDatabase::open(&config.database_url)?;
    db.migrate()?;
    let sdk = DatalensClient::new(config.sdk_config())?;
    let client = DatalensOrmpClient::new(sdk);
    let summary = datalens_example_ormp_client::run_until_complete(config, &db, &client)?;

    Ok(summary)
}

fn print_summary(
    fixture: Option<&str>,
    config: &AppConfig,
    summary: &RunSummary,
    elapsed_ms: u128,
) {
    println!(
        "fixture={} chain={} range={}-{} elapsed_ms={} fetched={} inserted={} duplicates={} invalid={} checkpoint={} has_next_page={}",
        fixture.unwrap_or("<env>"),
        config.chain_name,
        config.start_block,
        config.end_block.unwrap_or(config.start_block),
        elapsed_ms,
        summary.fetched_rows,
        summary.inserted_rows,
        summary.skipped_duplicates,
        summary.skipped_invalid,
        summary.checkpoint_cursor.as_deref().unwrap_or("<none>"),
        summary.has_next_page,
    );
}
