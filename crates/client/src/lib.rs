//! Internal REST client used by datalens workspace crates.

mod client;
mod selectors;

pub use client::*;
pub use selectors::TronEventSelector;
