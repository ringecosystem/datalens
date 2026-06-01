#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalProjectionDelta {
    pub proposal_id: String,
    pub support: i64,
    pub weight: String,
}
