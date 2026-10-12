//! Typed REST client over [`datalens_core::wire`].

use datalens_core::wire::{
    BackfillRequest, BackfillResponse, ChainHeads, CoverageResponse, ErrorBody, HeadsResponse,
    JobsResponse, QueryRequest, QueryResponse, WatchRequest, WatchResponse,
};
use datalens_core::{BlockRange, ChainName, Dataset, LogSelector};
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The server answered with an [`ErrorBody`]. `kind` is the stable error kind.
    #[error("http {status} {kind}: {message}")]
    Api {
        status: u16,
        kind: String,
        message: String,
    },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

impl ClientError {
    pub fn kind(&self) -> Option<&str> {
        match self {
            Self::Api { kind, .. } => Some(kind),
            Self::Http(_) => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            Self::Http(error) => error.status().map(|status| status.as_u16()),
        }
    }
}

/// One application's view of a datalens server.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    application: String,
    token: String,
}

impl Client {
    pub fn new(
        base_url: impl Into<String>,
        application: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base_url.into().trim_end_matches('/').to_owned(),
            application: application.into(),
            token: token.into(),
        }
    }

    pub async fn query(&self, request: &QueryRequest) -> Result<QueryResponse, ClientError> {
        self.send(self.post("/v1/query").json(request)).await
    }

    /// GraphQL `query(input:)` with `input` set to the REST body. Returns the `data` object.
    pub async fn graphql_query(
        &self,
        request: &QueryRequest,
        selection: &str,
    ) -> Result<Value, ClientError> {
        let body = json!({
            "query": format!("query Q($input: JSON!) {{ query(input: $input) {{ {selection} }} }}"),
            "variables": {"input": request},
        });
        let response: Value = self.send(self.post("/graphql").json(&body)).await?;
        if let Some(error) = response["errors"].get(0) {
            return Err(ClientError::Api {
                status: 200,
                kind: error["extensions"]["kind"]
                    .as_str()
                    .unwrap_or("internal")
                    .to_owned(),
                message: error["message"].as_str().unwrap_or_default().to_owned(),
            });
        }
        Ok(response["data"].clone())
    }

    pub async fn heads(&self) -> Result<HeadsResponse, ClientError> {
        self.send(self.get("/v1/heads")).await
    }

    pub async fn chain_heads(&self, chain: &ChainName) -> Result<ChainHeads, ClientError> {
        self.send(self.get(&format!("/v1/heads/{chain}"))).await
    }

    /// Waits until head `kind` (`latest`, `safe`, or `finalized`) passes `after`. On the
    /// server's timeout this returns the unchanged heads.
    pub async fn wait_heads(
        &self,
        chain: &ChainName,
        after: u64,
        kind: &str,
        timeout_ms: u64,
    ) -> Result<ChainHeads, ClientError> {
        let request = self.get(&format!("/v1/heads/{chain}/wait")).query(&[
            ("after", after.to_string()),
            ("kind", kind.to_owned()),
            ("timeout_ms", timeout_ms.to_string()),
        ]);
        self.send(request).await
    }

    pub async fn watch(&self, request: &WatchRequest) -> Result<WatchResponse, ClientError> {
        self.send(self.post("/v1/watch").json(request)).await
    }

    pub async fn unwatch(&self, request: &WatchRequest) -> Result<(), ClientError> {
        let delete = self.authed(self.http.delete(format!("{}/v1/watch", self.base)));
        checked(delete.json(request).send().await?).await?;
        Ok(())
    }

    pub async fn coverage(
        &self,
        chain: &ChainName,
        dataset: Dataset,
        selector: Option<&LogSelector>,
        range: Option<BlockRange>,
    ) -> Result<CoverageResponse, ClientError> {
        let mut params = vec![
            ("chain", chain.to_string()),
            ("dataset", dataset.as_str().to_owned()),
        ];
        if let Some(selector) = selector {
            params.push((
                "selector",
                serde_json::to_string(selector).expect("selector serializes"),
            ));
        }
        if let Some(range) = range {
            params.push(("start", range.start().to_string()));
            params.push(("end", range.end().to_string()));
        }
        self.send(self.get("/v1/admin/coverage").query(&params))
            .await
    }

    pub async fn backfill(
        &self,
        request: &BackfillRequest,
    ) -> Result<BackfillResponse, ClientError> {
        self.send(self.post("/v1/admin/backfill").json(request))
            .await
    }

    pub async fn jobs(&self, status: Option<&str>) -> Result<JobsResponse, ClientError> {
        let path = match status {
            Some(status) => format!("/v1/admin/jobs?status={status}"),
            None => "/v1/admin/jobs".to_owned(),
        };
        self.send(self.get(&path)).await
    }

    /// Prometheus text from `/metrics`, which takes the metrics token instead of an application.
    pub async fn metrics(&self, metrics_token: &str) -> Result<String, ClientError> {
        let response = self
            .http
            .get(format!("{}/metrics", self.base))
            .bearer_auth(metrics_token)
            .send()
            .await?;
        Ok(checked(response).await?.text().await?)
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.authed(self.http.get(format!("{}{path}", self.base)))
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.authed(self.http.post(format!("{}{path}", self.base)))
    }

    fn authed(&self, request: RequestBuilder) -> RequestBuilder {
        request
            .header("x-datalens-application", &self.application)
            .bearer_auth(&self.token)
    }

    async fn send<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, ClientError> {
        let response = checked(request.send().await?).await?;
        Ok(response.json().await?)
    }
}

async fn checked(response: Response) -> Result<Response, ClientError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let text = response.text().await?;
    let (kind, message) = match serde_json::from_str::<ErrorBody>(&text) {
        Ok(body) => (body.error.kind, body.error.message),
        Err(_) => ("http".to_owned(), text),
    };
    Err(ClientError::Api {
        status: status.as_u16(),
        kind,
        message,
    })
}
