use datalens_core::ChainIdentity;
use datalens_indexer::sdk::{ApplicationChainReader, ProcessorError, ProcessorFuture};
use serde_json::{Value, json};

#[derive(Default)]
pub struct MockBridgeMetadataReader {
    route_names: std::sync::Mutex<Vec<(u64, String)>>,
    requests: std::sync::Mutex<Vec<String>>,
}

impl MockBridgeMetadataReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_route_name(self, destination_chain: u64, route_name: impl Into<String>) -> Self {
        self.route_names
            .lock()
            .expect("route names lock")
            .push((destination_chain, route_name.into()));
        self
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl ApplicationChainReader for MockBridgeMetadataReader {
    fn read_json<'a>(
        &'a self,
        chain: &'a ChainIdentity,
        key: &'a str,
    ) -> ProcessorFuture<'a, Result<Value, ProcessorError>> {
        Box::pin(async move {
            self.requests
                .lock()
                .expect("requests lock")
                .push(format!("{}:{key}", chain.key_prefix()));
            let destination_chain = key
                .strip_prefix("route:")
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| ProcessorError::user("unsupported bridge metadata key"))?;
            let route_name = self
                .route_names
                .lock()
                .expect("route names lock")
                .iter()
                .find(|(candidate, _)| *candidate == destination_chain)
                .map(|(_, route_name)| route_name.clone())
                .unwrap_or_else(|| format!("route-{destination_chain}"));
            Ok(json!({ "route_name": route_name }))
        })
    }
}
