//! Typed EVM JSON-RPC. Every request goes through [`datalens_provider::Gateway`].

mod blocks;
mod client;
mod decode;
mod encode;
mod heads;
mod logs;
pub mod mock;

pub use client::{BlockRef, EvmClient, HeadMode};
