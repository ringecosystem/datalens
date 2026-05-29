//! Durable full-indexing runtime contract.

mod cursor;
mod index_plan;
mod runtime;

pub use cursor::FileIndexCursorStore;
pub use index_plan::*;
pub use runtime::{InMemoryIndexCursorStore, IndexCursorRepository, IndexRuntime};
