use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

use crate::{
    AppError, AppResult, checkpoint,
    datalens::{MessageAcceptedEvent, MessageAcceptedPage},
    db::AppDatabase,
    schema::message::OrmpMessage,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageAcceptedHandler {
    consumer_name: String,
}

impl MessageAcceptedHandler {
    pub fn new(consumer_name: impl Into<String>) -> Self {
        Self {
            consumer_name: consumer_name.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandlerSummary {
    pub fetched_rows: usize,
    pub inserted_rows: usize,
    pub skipped_duplicates: usize,
    pub skipped_invalid: usize,
    pub checkpoint_cursor: Option<String>,
}

pub fn handle_message_accepted_page(
    db: &AppDatabase,
    handler: &MessageAcceptedHandler,
    page: MessageAcceptedPage,
) -> AppResult<HandlerSummary> {
    let fetched_rows = page.events.len();
    let checkpoint_cursor = page
        .next_cursor
        .clone()
        .or_else(|| page.events.last().map(|event| event.cursor.clone()));

    db.transaction(|tx| {
        let mut inserted_rows = 0;
        let mut skipped_duplicates = 0;
        let mut skipped_invalid = 0;

        for event in &page.events {
            let Some(message) = to_ormp_message(event)? else {
                skipped_invalid += 1;
                eprintln!(
                    "skipping MessageAccepted event at cursor {} because messageHash is missing",
                    event.cursor
                );
                continue;
            };

            if cursor_belongs_to_other_message(tx, &message.event_cursor, &message.message_hash)? {
                return Err(AppError::Handler(format!(
                    "event cursor already belongs to another ORMP message: {}",
                    message.event_cursor
                )));
            }

            let changed = tx.execute(
                "INSERT OR IGNORE INTO ormp_messages (
                    message_hash,
                    source_chain_id,
                    target_chain_id,
                    sender,
                    receiver,
                    transaction_hash,
                    block_number,
                    event_cursor,
                    raw_event_json,
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, CURRENT_TIMESTAMP)",
                params![
                    message.message_hash,
                    message.source_chain_id,
                    message.target_chain_id,
                    message.sender,
                    message.receiver,
                    message.transaction_hash,
                    message.block_number,
                    message.event_cursor,
                    message.raw_event_json,
                ],
            )?;

            if changed == 0 {
                skipped_duplicates += 1;
            } else {
                inserted_rows += 1;
            }
        }

        if let Some(cursor) = &checkpoint_cursor {
            checkpoint::write_checkpoint(tx, &handler.consumer_name, cursor)?;
        }

        Ok(HandlerSummary {
            fetched_rows,
            inserted_rows,
            skipped_duplicates,
            skipped_invalid,
            checkpoint_cursor,
        })
    })
}

fn cursor_belongs_to_other_message(
    tx: &rusqlite::Transaction<'_>,
    cursor: &str,
    message_hash: &str,
) -> AppResult<bool> {
    let existing = tx
        .query_row(
            "SELECT message_hash FROM ormp_messages WHERE event_cursor = ?1",
            [cursor],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    Ok(existing.is_some_and(|existing| existing != message_hash))
}

fn to_ormp_message(event: &MessageAcceptedEvent) -> AppResult<Option<OrmpMessage>> {
    let args = &event.event.decoded_args;
    let Some(message_hash) = string_field(args, &["msgHash", "messageHash"]) else {
        return Ok(None);
    };

    let raw_event_json = serde_json::to_string(&json!({
        "cursor": event.cursor,
        "indexName": event.event.index_name,
        "chain": event.event.chain,
        "chainId": event.event.chain_id,
        "dataset": event.event.dataset,
        "blockNumber": event.event.block_number,
        "blockHash": event.event.block_hash,
        "transactionHash": event.event.transaction_hash,
        "transactionIndex": event.event.transaction_index,
        "logIndex": event.event.log_index,
        "address": event.event.address,
        "eventName": event.event.event_name,
        "signature": event.event.signature,
        "topic0": event.event.topic0,
        "decodedArgs": event.event.decoded_args,
        "decodeStatus": event.event.decode_status,
        "decodeError": event.event.decode_error,
        "payload": event.event.payload,
        "createdAt": event.event.created_at,
    }))?;

    Ok(Some(OrmpMessage {
        message_hash,
        source_chain_id: integer_field(args, &["sourceChainId", "source_chain_id"]),
        target_chain_id: integer_field(args, &["targetChainId", "target_chain_id"]),
        sender: string_field(args, &["sender", "from"]),
        receiver: string_field(args, &["receiver", "to"]),
        transaction_hash: event.event.transaction_hash.clone(),
        block_number: event.event.block_number.map(i64::from),
        event_cursor: event.cursor.clone(),
        raw_event_json,
    }))
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn integer_field(value: &Value, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
}
