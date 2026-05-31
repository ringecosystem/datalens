use datalens_sdk::{
    DatalensClient, Error,
    index::{DecodedEvent, EventFilter, PageRequest},
};

pub const DATASET: &str = "evm.logs";
pub const INDEX_NAME: &str = "ormp";
pub const MESSAGE_ACCEPTED: &str = "MessageAccepted";
pub const MESSAGE_ACCEPTED_SIGNATURE: &str =
    "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpEventPage {
    pub events: Vec<OrmpMessageAccepted>,
    pub next_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpMessageAccepted {
    pub cursor: String,
    pub block_number: Option<i32>,
    pub transaction_hash: Option<String>,
    pub message_hash: Option<String>,
}

pub fn fetch_message_accepted_page(
    client: &DatalensClient,
    after: Option<String>,
    page_size: u32,
) -> Result<OrmpEventPage, Error> {
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
        .map(|edge| OrmpMessageAccepted {
            cursor: edge.cursor,
            block_number: edge.node.block_number,
            message_hash: message_hash(&edge.node),
            transaction_hash: edge.node.transaction_hash,
        })
        .collect();

    Ok(OrmpEventPage {
        events,
        next_cursor: connection.page_info.end_cursor,
        has_next_page: connection.page_info.has_next_page,
    })
}

fn message_hash(event: &DecodedEvent) -> Option<String> {
    event
        .decoded_args
        .get("messageHash")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}
