//! Query execution boundary for durable native datalens plans.

mod durable_promotion;
mod execution;
mod helpers;
mod hot_promotion;
mod provider_singleflight;

pub use execution::*;
