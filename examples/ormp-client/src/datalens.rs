use datalens_sdk::{
    DatalensClient,
    index::{DecodedEvent, EventFilter, PageRequest},
};

use crate::AppResult;

pub const DATASET: &str = "evm.logs";
pub const INDEX_NAME: &str = "ormp";
pub const MESSAGE_ACCEPTED: &str = "MessageAccepted";
pub const MESSAGE_ACCEPTED_SIGNATURE: &str =
    "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))";

pub struct DatalensOrmpClient {
    client: DatalensClient,
}

impl DatalensOrmpClient {
    pub fn new(client: DatalensClient) -> Self {
        Self { client }
    }

    pub fn fetch_message_accepted_page(
        &self,
        after: Option<String>,
        page_size: u32,
    ) -> AppResult<MessageAcceptedPage> {
        fetch_message_accepted_page(&self.client, after, page_size)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MessageAcceptedPage {
    pub events: Vec<MessageAcceptedEvent>,
    pub next_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MessageAcceptedEvent {
    pub cursor: String,
    pub event: DecodedEvent,
}

pub fn fetch_message_accepted_page(
    client: &DatalensClient,
    after: Option<String>,
    page_size: u32,
) -> AppResult<MessageAcceptedPage> {
    let filter = EventFilter::new(DATASET)
        .with_index_name(INDEX_NAME)
        .with_event_name(MESSAGE_ACCEPTED)
        .with_signature(MESSAGE_ACCEPTED_SIGNATURE);
    let mut page = PageRequest::first(page_size);
    if let Some(after) = after {
        page = page.after(after);
    }

    let connection = client.index().decoded_events_connection(filter, page)?;
    let events = connection
        .edges
        .into_iter()
        .map(|edge| MessageAcceptedEvent {
            cursor: edge.cursor,
            event: edge.node,
        })
        .collect();

    Ok(MessageAcceptedPage {
        events,
        next_cursor: connection.page_info.end_cursor,
        has_next_page: connection.page_info.has_next_page,
    })
}
