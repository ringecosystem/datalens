use std::process::ExitCode;

use clap::Parser;
use datalens_client::DatalensClient;
use datalens_example_ormp::{
    OrmpCli, OrmpConfig, OrmpEndpointConfig, parse_plan, query_with_client, run_plan_with_client,
    summarize_response,
};

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
    let cli = OrmpCli::parse();
    if let Some(plan_path) = cli.plan {
        let config = OrmpEndpointConfig::from_env()?;
        let client = DatalensClient::new(config.client_config())?;
        let plan = parse_plan(&std::fs::read(plan_path)?)?;
        run_plan_with_client(&client, &plan, &mut std::io::stdout())?;
    } else {
        let config = OrmpConfig::from_env()?;
        let client = DatalensClient::new(config.client_config())?;
        let response = query_with_client(&client, &config)?;
        let summary = summarize_response(&response)?;
        println!("{}", serde_json::to_string(&summary)?);
    }
    Ok(())
}
