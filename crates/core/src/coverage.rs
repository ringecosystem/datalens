use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CoverageLevel {
    Covered,
    Partial,
    Missing,
}
