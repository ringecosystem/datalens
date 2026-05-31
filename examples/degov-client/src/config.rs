use std::{env, time::Duration};

use datalens_sdk::ClientConfig;

use crate::{AppError, AppResult};

pub const DEFAULT_INDEX_GRAPHQL_URL: &str = "http://127.0.0.1:3100/graphql";
pub const DEFAULT_DATABASE_URL: &str = "sqlite:.tmp/degov-client.sqlite";
pub const DEFAULT_PAGE_SIZE: u32 = 25;
pub const DEFAULT_CONSUMER_NAME: &str = "degov-vote-consumer";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppConfig {
    pub index_graphql_url: String,
    pub token: Option<String>,
    pub database_url: String,
    pub page_size: u32,
    pub start_cursor: Option<String>,
    pub consumer_name: String,
}

impl AppConfig {
    pub fn from_env() -> AppResult<Self> {
        let page_size = match env::var("DEGOV_PAGE_SIZE") {
            Ok(value) => value.parse().map_err(|error| {
                AppError::Config(format!(
                    "DEGOV_PAGE_SIZE must be a positive integer: {error}"
                ))
            })?,
            Err(_) => DEFAULT_PAGE_SIZE,
        };
        if page_size == 0 {
            return Err(AppError::Config(
                "DEGOV_PAGE_SIZE must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            index_graphql_url: env::var("DATALENS_INDEX_GRAPHQL_URL")
                .unwrap_or_else(|_| DEFAULT_INDEX_GRAPHQL_URL.to_owned()),
            token: env::var("DATALENS_TOKEN").ok(),
            database_url: env::var("DEGOV_DATABASE_URL")
                .unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned()),
            page_size,
            start_cursor: env::var("DEGOV_START_CURSOR").ok(),
            consumer_name: env::var("DEGOV_CONSUMER_NAME")
                .unwrap_or_else(|_| DEFAULT_CONSUMER_NAME.to_owned()),
        })
    }

    pub fn sdk_config(&self) -> ClientConfig {
        ClientConfig {
            endpoint: self.index_graphql_url.clone(),
            bearer_token: self.token.clone(),
            timeout: Some(Duration::from_secs(10)),
            user_agent: Some("datalens-degov-client-example".to_owned()),
        }
    }
}
