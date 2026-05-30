use std::process::ExitCode;

use datalens_client::DatalensClient;
use datalens_example_ormp::{OrmpConfig, query_with_client, summarize_response};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ormp datalens example failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = OrmpConfig::from_env()?;
    let client = DatalensClient::new(config.client_config())?;
    let response = query_with_client(&client, &config)?;
    let summary = summarize_response(&response)?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}
