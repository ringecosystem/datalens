use std::time::Duration;

/// Why a call is spending provider budget. Lower [`Purpose::rank`] is served first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Purpose {
    Head,
    Reorg,
    Follow,
    QueryFill,
    Backfill,
}

impl Purpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Reorg => "reorg",
            Self::Follow => "follow",
            Self::QueryFill => "query_fill",
            Self::Backfill => "backfill",
        }
    }

    pub(crate) fn rank(self) -> usize {
        match self {
            Self::Head => 0,
            Self::Reorg => 1,
            Self::Follow => 2,
            Self::QueryFill => 3,
            Self::Backfill => 4,
        }
    }
}

/// How long [`crate::Gateway`] may park a call that does not yet have a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenWait {
    /// Head, reorg, and follow park until a token is free or the daily cap is hit.
    UntilToken,
    /// Query fill and backfill park until `limit`, then [`crate::ProviderError::Throttled`].
    For(Duration),
}
