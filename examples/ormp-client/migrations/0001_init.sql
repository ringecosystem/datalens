CREATE TABLE IF NOT EXISTS consumer_checkpoints (
    consumer_name TEXT PRIMARY KEY NOT NULL,
    cursor TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ormp_messages (
    message_hash TEXT PRIMARY KEY NOT NULL,
    source_chain_id INTEGER,
    target_chain_id INTEGER,
    sender TEXT,
    receiver TEXT,
    transaction_hash TEXT,
    block_number INTEGER,
    event_cursor TEXT NOT NULL UNIQUE,
    raw_event_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ormp_messages_event_cursor
    ON ormp_messages(event_cursor);

CREATE INDEX IF NOT EXISTS idx_ormp_messages_block_number
    ON ormp_messages(block_number);
