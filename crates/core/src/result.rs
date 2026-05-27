use serde::{Deserialize, Serialize};

use crate::{DatasetId, TimeRange};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResultEnvelope<T> {
    dataset: DatasetId,
    range: TimeRange,
    payload: T,
}

impl<T> ResultEnvelope<T> {
    pub fn ok(dataset: DatasetId, range: TimeRange, payload: T) -> Self {
        Self {
            dataset,
            range,
            payload,
        }
    }

    pub fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    pub fn range(&self) -> &TimeRange {
        &self.range
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}
