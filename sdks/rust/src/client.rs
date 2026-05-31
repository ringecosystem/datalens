use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Error, GraphqlError, index::IndexClient, native::NativeClient};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub endpoint: String,
    pub bearer_token: Option<String>,
    pub timeout: Option<Duration>,
    pub user_agent: Option<String>,
}

#[derive(Clone)]
pub struct DatalensClient {
    endpoint: String,
    bearer_token: Option<String>,
    user_agent: Option<String>,
    http: reqwest::blocking::Client,
}

impl DatalensClient {
    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        let endpoint = config.endpoint.trim().trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(Error::InvalidConfig(
                "datalens GraphQL endpoint must not be empty".to_owned(),
            ));
        }

        let mut builder = reqwest::blocking::Client::builder();
        if let Some(timeout) = config.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(user_agent) = normalize_optional(config.user_agent) {
            builder = builder.user_agent(user_agent.clone());
            Ok(Self {
                endpoint,
                bearer_token: normalize_optional(config.bearer_token),
                user_agent: Some(user_agent),
                http: builder
                    .build()
                    .map_err(|error| Error::InvalidConfig(error.to_string()))?,
            })
        } else {
            Ok(Self {
                endpoint,
                bearer_token: normalize_optional(config.bearer_token),
                user_agent: None,
                http: builder
                    .build()
                    .map_err(|error| Error::InvalidConfig(error.to_string()))?,
            })
        }
    }

    pub fn native(&self) -> NativeClient<'_> {
        NativeClient::new(self)
    }

    pub fn index(&self) -> IndexClient<'_> {
        IndexClient::new(self)
    }

    pub(crate) fn execute<T, V>(&self, query: &'static str, variables: V) -> Result<T, Error>
    where
        T: DeserializeOwned,
        V: Serialize,
    {
        let request = GraphqlRequest { query, variables };
        let mut builder = self.http.post(&self.endpoint).json(&request);
        if let Some(token) = &self.bearer_token {
            builder = builder.bearer_auth(token);
        }
        if let Some(user_agent) = &self.user_agent {
            builder = builder.header(reqwest::header::USER_AGENT, user_agent);
        }

        let response = builder
            .send()
            .map_err(|error| Error::Transport(format!("send datalens GraphQL request: {error}")))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|error| {
            Error::Transport(format!("read datalens GraphQL response: {error}"))
        })?;

        if !(200..300).contains(&status) {
            return if status == 401 || status == 403 {
                Err(Error::Unauthorized { status, body })
            } else {
                Err(Error::HttpStatus { status, body })
            };
        }

        let response: GraphqlResponse<T> = serde_json::from_str(&body)
            .map_err(|error| Error::Decode(format!("decode datalens GraphQL response: {error}")))?;
        if !response.errors.is_empty() {
            return Err(Error::Graphql(response.errors));
        }
        response.data.ok_or_else(|| {
            Error::Decode("datalens GraphQL response did not include data".to_owned())
        })
    }
}

#[derive(Serialize)]
struct GraphqlRequest<V> {
    query: &'static str,
    variables: V,
}

#[derive(Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}
