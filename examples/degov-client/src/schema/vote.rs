#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernanceVote {
    pub vote_key: String,
    pub proposal_id: String,
    pub voter: Option<String>,
    pub support: i64,
    pub weight: i64,
    pub reason: Option<String>,
    pub transaction_hash: Option<String>,
    pub block_number: Option<i64>,
    pub event_cursor: String,
    pub raw_event_json: String,
}
