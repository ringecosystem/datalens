//! Typed EVM JSON-RPC. Every request goes through [`datalens_provider::Gateway`].

mod blocks;
mod client;
mod decode;
mod encode;
mod heads;
mod logs;
#[cfg(feature = "mock")]
pub mod mock;

pub use blocks::{SenderLookup, TxSender};
pub use client::{BlockRef, EvmClient, HeadMode, HeaderBatchSizeError};
pub use logs::{LogFetchError, LogsPage};
