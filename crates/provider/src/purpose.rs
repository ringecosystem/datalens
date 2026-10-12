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

    /// Head and reorg wait for a token. Lower priorities give up quickly so they
    /// cannot sit on the next token ahead of a head check.
    pub(crate) fn fails_fast(self) -> bool {
        !matches!(self, Self::Head | Self::Reorg)
    }
}
