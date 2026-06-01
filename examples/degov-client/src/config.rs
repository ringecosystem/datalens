use std::{env, time::Duration};

use datalens_sdk::ClientConfig;

use crate::{AppError, AppResult};

pub const DEFAULT_DATALENS_ENDPOINT: &str = "http://127.0.0.1:3000";
pub const DEFAULT_DATABASE_URL: &str = "sqlite:.tmp/degov-client.sqlite";
pub const DEFAULT_CHUNK_SIZE: u32 = 100;
pub const DEFAULT_CONSUMER_NAME: &str = "degov-vote-consumer";
pub const DEFAULT_APPLICATION: &str = "degov-client";
pub const DEFAULT_CHAIN_NAME: &str = "ethereum";
pub const DEFAULT_CHAIN_ID: i32 = 1;
pub const DEFAULT_DATASET_FAMILY: &str = "evm";
pub const DEFAULT_DATASET_NAME: &str = "logs";
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
pub const DEFAULT_EVENT_TOPIC0: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const DEFAULT_START_BLOCK: i32 = 0;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppConfig {
    pub datalens_endpoint: String,
    pub token: Option<String>,
    pub application: String,
    pub database_url: String,
    pub chain_name: String,
    pub chain_id: i32,
    pub dataset_family: String,
    pub dataset_name: String,
    pub contract_address: String,
    pub event_topic0: String,
    pub event_signature: String,
    pub start_block: i32,
    pub end_block: Option<i32>,
    pub chunk_size: u32,
    pub reset_checkpoint: bool,
    pub consumer_name: String,
}

impl AppConfig {
    pub fn from_env() -> AppResult<Self> {
        let chunk_size = match env::var("DEGOV_CHUNK_SIZE").or_else(|_| env::var("DEGOV_PAGE_SIZE"))
        {
            Ok(value) => value.parse().map_err(|error| {
                AppError::Config(format!(
                    "DEGOV_CHUNK_SIZE must be a positive integer: {error}"
                ))
            })?,
            Err(_) => DEFAULT_CHUNK_SIZE,
        };
        if chunk_size == 0 {
            return Err(AppError::Config(
                "DEGOV_CHUNK_SIZE must be greater than zero".to_owned(),
            ));
        }
        let start_block = env_i32("DEGOV_START_BLOCK", DEFAULT_START_BLOCK)?;
        let end_block = optional_env_i32("DEGOV_END_BLOCK")?;
        if let Some(end_block) = end_block
            && end_block < start_block
        {
            return Err(AppError::Config(
                "DEGOV_END_BLOCK must be greater than or equal to DEGOV_START_BLOCK".to_owned(),
            ));
        }

        Ok(Self {
            datalens_endpoint: env::var("DATALENS_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_DATALENS_ENDPOINT.to_owned()),
            token: env::var("DATALENS_TOKEN").ok(),
            application: env::var("DATALENS_APPLICATION")
                .unwrap_or_else(|_| DEFAULT_APPLICATION.to_owned()),
            database_url: env::var("DEGOV_DATABASE_URL")
                .unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned()),
            chain_name: env::var("DEGOV_CHAIN_NAME")
                .unwrap_or_else(|_| DEFAULT_CHAIN_NAME.to_owned()),
            chain_id: env_i32("DEGOV_CHAIN_ID", DEFAULT_CHAIN_ID)?,
            dataset_family: env::var("DEGOV_DATASET_FAMILY")
                .unwrap_or_else(|_| DEFAULT_DATASET_FAMILY.to_owned()),
            dataset_name: env::var("DEGOV_DATASET_NAME")
                .unwrap_or_else(|_| DEFAULT_DATASET_NAME.to_owned()),
            contract_address: env::var("DEGOV_CONTRACT_ADDRESS")
                .unwrap_or_else(|_| DEFAULT_CONTRACT_ADDRESS.to_owned()),
            event_topic0: env::var("DEGOV_EVENT_TOPIC0")
                .unwrap_or_else(|_| DEFAULT_EVENT_TOPIC0.to_owned()),
            event_signature: env::var("DEGOV_EVENT_SIGNATURE")
                .unwrap_or_else(|_| crate::datalens::VOTE_CAST_SIGNATURE.to_owned()),
            start_block,
            end_block,
            chunk_size,
            reset_checkpoint: env_bool("DEGOV_RESET_CHECKPOINT"),
            consumer_name: env::var("DEGOV_CONSUMER_NAME")
                .unwrap_or_else(|_| DEFAULT_CONSUMER_NAME.to_owned()),
        })
    }

    pub fn sdk_config(&self) -> ClientConfig {
        ClientConfig {
            endpoint: format!(
                "{}/native/graphql",
                self.datalens_endpoint.trim_end_matches('/')
            ),
            bearer_token: self.token.clone(),
            application: Some(self.application.clone()),
            timeout: Some(Duration::from_secs(10)),
            user_agent: Some("datalens-degov-client-example".to_owned()),
        }
    }
}

fn env_i32(name: &str, default: i32) -> AppResult<i32> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| AppError::Config(format!("{name} must be an integer: {error}"))),
        Err(_) => Ok(default),
    }
}

fn optional_env_i32(name: &str) -> AppResult<Option<i32>> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|error| AppError::Config(format!("{name} must be an integer: {error}")))
        })
        .transpose()
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}
