use serde::{Deserialize, Serialize};

use crate::{DatalensClient, Error};

pub struct IndexClient<'a> {
    client: &'a DatalensClient,
}

impl<'a> IndexClient<'a> {
    pub(crate) fn new(client: &'a DatalensClient) -> Self {
        Self { client }
    }

    pub fn raw_events(
        &self,
        filter: EventFilter,
        limit: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<IndexedEvent>, Error> {
        let variables = filter.into_variables().with_limit(limit).with_after(after);
        let data: EventsData = self.client.execute(RAW_EVENTS_QUERY, variables)?;
        Ok(data.events)
    }

    pub fn raw_events_connection(
        &self,
        filter: EventFilter,
        page: PageRequest,
    ) -> Result<EventConnection<IndexedEvent>, Error> {
        let variables = filter
            .into_variables()
            .with_first(page.first)
            .with_after(page.after);
        let data: RawEventsConnectionData = self
            .client
            .execute(RAW_EVENTS_CONNECTION_QUERY, variables)?;
        Ok(data.events_connection)
    }

    pub fn decoded_events(
        &self,
        filter: EventFilter,
        limit: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<DecodedEvent>, Error> {
        let variables = filter.into_variables().with_limit(limit).with_after(after);
        let data: DecodedEventsData = self.client.execute(DECODED_EVENTS_QUERY, variables)?;
        Ok(data.decoded_events)
    }

    pub fn decoded_events_connection(
        &self,
        filter: EventFilter,
        page: PageRequest,
    ) -> Result<EventConnection<DecodedEvent>, Error> {
        let variables = filter
            .into_variables()
            .with_first(page.first)
            .with_after(page.after);
        let data: DecodedEventsConnectionData = self
            .client
            .execute(DECODED_EVENTS_CONNECTION_QUERY, variables)?;
        Ok(data.decoded_events_connection)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventFilter {
    pub dataset: String,
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<i32>,
    pub address: Option<String>,
    pub event_name: Option<String>,
    pub signature: Option<String>,
    pub from_block: Option<i32>,
    pub to_block: Option<i32>,
    pub topic0: Option<String>,
}

impl EventFilter {
    pub fn new(dataset: impl Into<String>) -> Self {
        Self {
            dataset: dataset.into(),
            index_name: None,
            chain: None,
            chain_id: None,
            address: None,
            event_name: None,
            signature: None,
            from_block: None,
            to_block: None,
            topic0: None,
        }
    }

    pub fn with_index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    pub fn with_chain(mut self, chain: impl Into<String>) -> Self {
        self.chain = Some(chain.into());
        self
    }

    pub fn with_chain_id(mut self, chain_id: i32) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn with_event_name(mut self, event_name: impl Into<String>) -> Self {
        self.event_name = Some(event_name.into());
        self
    }

    pub fn with_signature(mut self, signature: impl Into<String>) -> Self {
        self.signature = Some(signature.into());
        self
    }

    pub fn with_block_range(mut self, from_block: i32, to_block: i32) -> Self {
        self.from_block = Some(from_block);
        self.to_block = Some(to_block);
        self
    }

    pub fn with_topic0(mut self, topic0: impl Into<String>) -> Self {
        self.topic0 = Some(topic0.into());
        self
    }

    fn into_variables(self) -> EventVariables {
        EventVariables {
            index_name: self.index_name,
            chain: self.chain,
            chain_id: self.chain_id,
            dataset: self.dataset,
            address: self.address,
            event_name: self.event_name,
            signature: self.signature,
            from_block: self.from_block,
            to_block: self.to_block,
            topic0: self.topic0,
            limit: None,
            first: None,
            after: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRequest {
    pub first: Option<u32>,
    pub after: Option<String>,
}

impl PageRequest {
    pub fn first(first: u32) -> Self {
        Self {
            first: Some(first),
            after: None,
        }
    }

    pub fn after(mut self, after: impl Into<String>) -> Self {
        self.after = Some(after.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<i32>,
    pub dataset: Option<String>,
    pub block_number: Option<i32>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<i32>,
    pub event_index: Option<i32>,
    pub address: Option<String>,
    pub selector: Option<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    pub topic0: Option<String>,
    pub signature: Option<String>,
    pub event_name: Option<String>,
    #[serde(default)]
    pub decoded: serde_json::Value,
    pub data: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub created_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecodedEvent {
    pub index_name: Option<String>,
    pub chain: Option<String>,
    pub chain_id: Option<i32>,
    pub dataset: Option<String>,
    pub block_number: Option<i32>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<i32>,
    pub log_index: Option<i32>,
    pub address: Option<String>,
    pub event_name: Option<String>,
    pub signature: Option<String>,
    pub topic0: Option<String>,
    #[serde(default)]
    pub decoded_args: serde_json::Value,
    pub decode_status: Option<String>,
    pub decode_error: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub created_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventConnection<T> {
    pub edges: Vec<EventEdge<T>>,
    pub nodes: Vec<T>,
    pub page_info: EventPageInfo,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct EventEdge<T> {
    pub cursor: String,
    pub node: T,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventPageInfo {
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventVariables {
    #[serde(skip_serializing_if = "Option::is_none")]
    index_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<i32>,
    dataset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from_block: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_block: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic0: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
}

impl EventVariables {
    fn with_limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    fn with_first(mut self, first: Option<u32>) -> Self {
        self.first = first;
        self
    }

    fn with_after(mut self, after: Option<String>) -> Self {
        self.after = after;
        self
    }
}

#[derive(Deserialize)]
struct EventsData {
    events: Vec<IndexedEvent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawEventsConnectionData {
    events_connection: EventConnection<IndexedEvent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodedEventsData {
    decoded_events: Vec<DecodedEvent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodedEventsConnectionData {
    decoded_events_connection: EventConnection<DecodedEvent>,
}

const RAW_EVENTS_QUERY: &str = r#"
query($indexName: String, $chain: String, $chainId: Int, $dataset: String!, $address: String, $eventName: String, $signature: String, $fromBlock: Int, $toBlock: Int, $topic0: String, $limit: Int, $after: String) {
  events(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, limit: $limit, after: $after) {
    indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex eventIndex address selector topics topic0 signature eventName decoded data payload createdAt
  }
}
"#;

const RAW_EVENTS_CONNECTION_QUERY: &str = r#"
query($indexName: String, $chain: String, $chainId: Int, $dataset: String!, $address: String, $eventName: String, $signature: String, $fromBlock: Int, $toBlock: Int, $topic0: String, $first: Int, $after: String) {
  eventsConnection(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, first: $first, after: $after) {
    edges { cursor node { indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex eventIndex address selector topics topic0 signature eventName decoded data payload createdAt } }
    nodes { indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex eventIndex address selector topics topic0 signature eventName decoded data payload createdAt }
    pageInfo { endCursor hasNextPage }
  }
}
"#;

const DECODED_EVENTS_QUERY: &str = r#"
query($indexName: String, $chain: String, $chainId: Int, $dataset: String!, $address: String, $eventName: String, $signature: String, $fromBlock: Int, $toBlock: Int, $topic0: String, $limit: Int, $after: String) {
  decodedEvents(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, limit: $limit, after: $after) {
    indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex logIndex address eventName signature topic0 decodedArgs decodeStatus decodeError payload createdAt
  }
}
"#;

const DECODED_EVENTS_CONNECTION_QUERY: &str = r#"
query($indexName: String, $chain: String, $chainId: Int, $dataset: String!, $address: String, $eventName: String, $signature: String, $fromBlock: Int, $toBlock: Int, $topic0: String, $first: Int, $after: String) {
  decodedEventsConnection(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, first: $first, after: $after) {
    edges { cursor node { indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex logIndex address eventName signature topic0 decodedArgs decodeStatus decodeError payload createdAt } }
    nodes { indexName chain chainId dataset blockNumber blockHash transactionHash transactionIndex logIndex address eventName signature topic0 decodedArgs decodeStatus decodeError payload createdAt }
    pageInfo { endCursor hasNextPage }
  }
}
"#;
