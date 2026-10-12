//! Client for the datalens query API.
//!
//! [`Client`] sends queries, reads and long-polls heads, and manages subscriptions.
//! [`Client::follow`] turns a start block into a stream of batches that keeps up with the
//! head. [`FinalizedCursor`] and [`ProvisionalCursor`] decide which served data is final and
//! when a reorg invalidates provisional data.
//!
//! Request and response types are the server's own, re-exported from `datalens-core`.

mod client;
mod cursor;
mod error;
mod follow;

pub use client::{Client, ClientConfig, HeadKind, RetryConfig};
pub use cursor::{FinalizedCursor, Observation, ProvisionalCursor};
pub use error::{ApiErrorKind, Error, Result};
pub use follow::{FollowEvent, FollowRequest};

/// Validation error of [`BlockRange::new`], [`ChainName::new`], and the selector builders.
pub use datalens_core::Error as InvalidValue;
pub use datalens_core::wire::{
    Anchor, BlockRecord, ChainHeads, FollowInfo, FollowResumeRequest, FollowResumeResponse,
    IncompleteReason, LogRecord, PageInfo, PageRequest, QueryRequest, QueryResponse, Segment,
    SegmentSource, WatchRequest, WatchResponse,
};
pub use datalens_core::{
    Address, B256, BlockRange, Bytes, ChainName, Dataset, Finality, LogSelector, QueryFinality,
    TopicFilter, TopicSet,
};
