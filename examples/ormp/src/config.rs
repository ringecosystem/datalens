use std::{collections::BTreeMap, env};

use clap::Parser;
use datalens_client::DatalensClientConfig;

use crate::{ORMP_START_BLOCK, OrmpExampleError};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3000";
const DEFAULT_APPLICATION: &str = "public";

#[derive(Clone, Debug, Parser)]
#[command(
    name = "datalens-example-ormp",
    about = "Query cached ORMP EVM logs through datalens",
    after_help = "Configuration:
  DATALENS_ENDPOINT defaults to http://127.0.0.1:3000
  DATALENS_APPLICATION defaults to public
  ORMP_FROM_BLOCK defaults to 20009590
  ORMP_TO_BLOCK is required
  DATALENS_PUBLIC_APP_TOKEN is optional for local unauthenticated smoke runs

Example:
  ORMP_TO_BLOCK=20009600 cargo run --manifest-path examples/ormp/Cargo.toml"
)]
pub struct OrmpCli {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpConfig {
    pub endpoint: String,
    pub application: String,
    pub bearer_token: Option<String>,
    pub from_block: u64,
    pub to_block: u64,
}

impl OrmpConfig {
    pub fn from_env() -> Result<Self, OrmpExampleError> {
        Self::from_map(env::vars().collect())
    }

    pub fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Self, OrmpExampleError> {
        Self::from_map(
            pairs
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        )
    }

    pub fn client_config(&self) -> DatalensClientConfig {
        DatalensClientConfig {
            endpoint: self.endpoint.clone(),
            application: Some(self.application.clone()),
            bearer_token: self.bearer_token.clone(),
        }
    }

    fn from_map(values: BTreeMap<String, String>) -> Result<Self, OrmpExampleError> {
        let from_block = read_u64_env(&values, "ORMP_FROM_BLOCK")?.unwrap_or(ORMP_START_BLOCK);
        let to_block = read_u64_env(&values, "ORMP_TO_BLOCK")?
            .ok_or(OrmpExampleError::MissingEnv("ORMP_TO_BLOCK"))?;

        Ok(Self {
            endpoint: read_string_env(&values, "DATALENS_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned()),
            application: read_string_env(&values, "DATALENS_APPLICATION")
                .unwrap_or_else(|| DEFAULT_APPLICATION.to_owned()),
            bearer_token: read_string_env(&values, "DATALENS_PUBLIC_APP_TOKEN"),
            from_block,
            to_block,
        })
    }
}

fn read_string_env(values: &BTreeMap<String, String>, name: &'static str) -> Option<String> {
    values
        .get(name)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_u64_env(
    values: &BTreeMap<String, String>,
    name: &'static str,
) -> Result<Option<u64>, OrmpExampleError> {
    read_string_env(values, name)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| OrmpExampleError::InvalidEnv {
                    name,
                    message: error.to_string(),
                })
        })
        .transpose()
}
