use datalens_sdk::{
    DatalensClient,
    index::{DecodedEvent, EventFilter, PageRequest},
};

use crate::AppResult;

pub const DATASET: &str = "evm.logs";
pub const INDEX_NAME: &str = "degov";
pub const VOTE_CAST: &str = "VoteCast";

pub struct DatalensDegovClient {
    client: DatalensClient,
}

impl DatalensDegovClient {
    pub fn new(client: DatalensClient) -> Self {
        Self { client }
    }

    pub fn fetch_vote_cast_page(
        &self,
        after: Option<String>,
        page_size: u32,
    ) -> AppResult<VoteCastPage> {
        fetch_vote_cast_page(&self.client, after, page_size)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VoteCastPage {
    pub events: Vec<VoteCastEvent>,
    pub next_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VoteCastEvent {
    pub cursor: String,
    pub event: DecodedEvent,
}

pub fn fetch_vote_cast_page(
    client: &DatalensClient,
    after: Option<String>,
    page_size: u32,
) -> AppResult<VoteCastPage> {
    let filter = EventFilter::new(DATASET)
        .with_index_name(INDEX_NAME)
        .with_event_name(VOTE_CAST);
    let mut page = PageRequest::first(page_size);
    if let Some(after) = after {
        page = page.after(after);
    }

    let connection = client.index().decoded_events_connection(filter, page)?;
    let events = connection
        .edges
        .into_iter()
        .map(|edge| VoteCastEvent {
            cursor: edge.cursor,
            event: edge.node,
        })
        .collect();

    Ok(VoteCastPage {
        events,
        next_cursor: connection.page_info.end_cursor,
        has_next_page: connection.page_info.has_next_page,
    })
}
