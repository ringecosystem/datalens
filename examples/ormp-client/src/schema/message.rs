#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpMessage {
    pub message_hash: String,
    pub source_chain_id: Option<i64>,
    pub target_chain_id: Option<i64>,
    pub sender: Option<String>,
    pub receiver: Option<String>,
    pub transaction_hash: Option<String>,
    pub block_number: Option<i64>,
    pub event_cursor: String,
    pub raw_event_json: String,
}
