mod client;
mod error;

pub mod index;
pub mod native;

pub use client::{ClientConfig, DatalensClient};
pub use error::{Error, GraphqlError};
