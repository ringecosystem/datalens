use std::collections::BTreeMap;

use datalens_sdk::{
    DatalensClient, Error,
    index::{DecodedEvent, EventFilter, PageRequest},
};

pub const DATASET: &str = "evm.logs";
pub const INDEX_NAME: &str = "degov";
pub const VOTE_CAST: &str = "VoteCast";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConsumptionCheckpoint {
    pub cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProposalProjection {
    for_votes: BTreeMap<String, u64>,
}

impl ProposalProjection {
    pub fn apply_vote(&mut self, vote: &VoteCast) {
        if vote.support == Some(1) {
            *self.for_votes.entry(vote.proposal_id.clone()).or_default() += vote.weight;
        }
    }

    pub fn for_votes(&self, proposal_id: &str) -> u64 {
        self.for_votes.get(proposal_id).copied().unwrap_or_default()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProposalMaterializer {
    consumed: Vec<VoteCast>,
}

impl ProposalMaterializer {
    pub fn record(&mut self, vote: VoteCast) {
        self.consumed.push(vote);
    }

    pub fn consumed(&self) -> &[VoteCast] {
        &self.consumed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoteCast {
    pub cursor: String,
    pub proposal_id: String,
    pub support: Option<u64>,
    pub weight: u64,
}

pub fn consume_vote_page(
    client: &DatalensClient,
    materializer: &mut ProposalMaterializer,
    projection: &mut ProposalProjection,
    after: Option<String>,
    page_size: u32,
) -> Result<ConsumptionCheckpoint, Error> {
    let filter = EventFilter::new(DATASET)
        .with_index_name(INDEX_NAME)
        .with_event_name(VOTE_CAST);
    let mut page = PageRequest::first(page_size);
    if let Some(after) = after {
        page = page.after(after);
    }

    let connection = client.index().decoded_events_connection(filter, page)?;
    for edge in connection.edges {
        if let Some(vote) = vote_cast(edge.cursor, &edge.node) {
            projection.apply_vote(&vote);
            materializer.record(vote);
        }
    }

    Ok(ConsumptionCheckpoint {
        cursor: connection.page_info.end_cursor,
        has_next_page: connection.page_info.has_next_page,
    })
}

fn vote_cast(cursor: String, event: &DecodedEvent) -> Option<VoteCast> {
    let proposal_id = string_arg(event, "proposalId")?;
    Some(VoteCast {
        cursor,
        proposal_id,
        support: integer_arg(event, "support"),
        weight: integer_arg(event, "weight").unwrap_or_default(),
    })
}

fn string_arg(event: &DecodedEvent, name: &str) -> Option<String> {
    let value = event.decoded_args.get(name)?;
    if let Some(value) = value.as_str() {
        return Some(value.to_owned());
    }
    value.as_u64().map(|value| value.to_string())
}

fn integer_arg(event: &DecodedEvent, name: &str) -> Option<u64> {
    let value = event.decoded_args.get(name)?;
    if let Some(value) = value.as_u64() {
        return Some(value);
    }
    value.as_str()?.parse().ok()
}
