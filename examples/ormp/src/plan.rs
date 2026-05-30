use serde::Deserialize;

use crate::RangeSummary;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpPlan {
    pub jobs: Vec<OrmpPlanJob>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OrmpPlanJob {
    pub label: String,
    pub chain: String,
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub addresses: Vec<String>,
}

impl OrmpPlanJob {
    pub fn range_summary(&self) -> RangeSummary {
        RangeSummary::Block {
            start: self.from_block,
            end: self.to_block,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PlanShape {
    Jobs { jobs: Vec<OrmpPlanJob> },
    Array(Vec<OrmpPlanJob>),
}

pub fn parse_plan(bytes: &[u8]) -> Result<OrmpPlan, serde_json::Error> {
    let shape = serde_json::from_slice(bytes)?;
    Ok(match shape {
        PlanShape::Jobs { jobs } => OrmpPlan { jobs },
        PlanShape::Array(jobs) => OrmpPlan { jobs },
    })
}
