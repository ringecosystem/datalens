//! Domain vocabulary shared by every datalens crate. Pure data and rules, no IO.

mod chain;
mod dataset;
mod error;
mod finality;
mod range;
mod rows;
mod selector;

pub use alloy_primitives::{Address, B256, Bytes};
pub use chain::{ChainId, ChainName};
pub use dataset::Dataset;
pub use error::Error;
pub use finality::{Finality, QueryFinality};
pub use range::BlockRange;
pub use rows::{BlockRow, Heads, LogRow};
pub use selector::{LogSelector, Selector, TopicFilter, TopicSet};
