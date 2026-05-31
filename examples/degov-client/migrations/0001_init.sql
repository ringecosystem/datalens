CREATE TABLE IF NOT EXISTS consumer_checkpoints (
    consumer_name TEXT PRIMARY KEY NOT NULL,
    cursor TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS degov_votes (
    vote_key TEXT PRIMARY KEY NOT NULL,
    proposal_id TEXT NOT NULL,
    voter TEXT,
    support INTEGER NOT NULL,
    weight INTEGER NOT NULL,
    reason TEXT,
    transaction_hash TEXT,
    block_number INTEGER,
    event_cursor TEXT NOT NULL UNIQUE,
    raw_event_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS degov_proposals (
    proposal_id TEXT PRIMARY KEY NOT NULL,
    for_votes INTEGER NOT NULL DEFAULT 0,
    against_votes INTEGER NOT NULL DEFAULT 0,
    abstain_votes INTEGER NOT NULL DEFAULT 0,
    vote_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_degov_votes_event_cursor
    ON degov_votes(event_cursor);

CREATE INDEX IF NOT EXISTS idx_degov_votes_proposal_id
    ON degov_votes(proposal_id);

CREATE INDEX IF NOT EXISTS idx_degov_votes_block_number
    ON degov_votes(block_number);
