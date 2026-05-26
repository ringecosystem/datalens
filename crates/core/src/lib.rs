//! Chain-neutral datalens vocabulary shared across workspace crates.

pub mod chain {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum ChainFamily {
        Evm,
        Other(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ChainIdentity {
        family: ChainFamily,
        id: String,
    }

    impl ChainIdentity {
        pub fn new(family: ChainFamily, id: impl Into<String>) -> Self {
            Self {
                family,
                id: id.into(),
            }
        }

        pub fn family(&self) -> ChainFamily {
            self.family.clone()
        }

        pub fn id(&self) -> &str {
            &self.id
        }
    }
}

pub mod range {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TimeRange {
        start: u64,
        end: u64,
    }

    impl TimeRange {
        pub fn blocks(start: u64, end: u64) -> Self {
            Self { start, end }
        }

        pub fn start(&self) -> u64 {
            self.start
        }

        pub fn end(&self) -> u64 {
            self.end
        }
    }
}

pub mod dataset {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetId(String);

    impl DatasetId {
        pub fn new(id: impl Into<String>) -> Self {
            Self(id.into())
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }
}

pub mod coverage {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum CoverageLevel {
        Covered,
        Partial,
        Missing,
    }
}

pub mod error {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum DatalensErrorKind {
        InvalidRequest,
        Unsupported,
        Unavailable,
        Persistence,
        Internal,
    }
}

pub mod result {
    use crate::{DatasetId, TimeRange};

    #[derive(Clone, Debug, Eq, PartialEq)]
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
}

pub use chain::{ChainFamily, ChainIdentity};
pub use coverage::CoverageLevel;
pub use dataset::DatasetId;
pub use error::DatalensErrorKind;
pub use range::TimeRange;
pub use result::ResultEnvelope;
