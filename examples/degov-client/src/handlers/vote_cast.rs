use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

use crate::{
    AppError, AppResult, checkpoint,
    datalens::{VoteCastEvent, VoteCastPage},
    db::AppDatabase,
    schema::{proposal::ProposalProjectionDelta, vote::GovernanceVote},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoteCastHandler {
    consumer_name: String,
}

impl VoteCastHandler {
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
    pub updated_proposals: usize,
    pub checkpoint_cursor: Option<String>,
}

pub fn handle_vote_cast_page(
    db: &AppDatabase,
    handler: &VoteCastHandler,
    page: VoteCastPage,
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
        let mut updated_proposals = 0;

        for event in &page.events {
            let Some(vote) = to_governance_vote(event)? else {
                skipped_invalid += 1;
                eprintln!(
                    "skipping VoteCast event at cursor {} because proposalId or weight is missing",
                    event.cursor
                );
                continue;
            };

            if cursor_belongs_to_other_vote(tx, &vote.event_cursor, &vote.vote_key)? {
                return Err(AppError::Handler(format!(
                    "event cursor already belongs to another Degov vote: {}",
                    vote.event_cursor
                )));
            }

            let changed = tx.execute(
                "INSERT OR IGNORE INTO degov_votes (
                    vote_key,
                    proposal_id,
                    voter,
                    support,
                    weight,
                    reason,
                    transaction_hash,
                    block_number,
                    event_cursor,
                    raw_event_json,
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP)",
                params![
                    vote.vote_key,
                    vote.proposal_id,
                    vote.voter,
                    vote.support,
                    vote.weight,
                    vote.reason,
                    vote.transaction_hash,
                    vote.block_number,
                    vote.event_cursor,
                    vote.raw_event_json,
                ],
            )?;

            if changed == 0 {
                skipped_duplicates += 1;
            } else {
                inserted_rows += 1;
                apply_projection_delta(tx, vote.projection_delta())?;
                updated_proposals += 1;
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
            updated_proposals,
            checkpoint_cursor,
        })
    })
}

fn cursor_belongs_to_other_vote(
    tx: &rusqlite::Transaction<'_>,
    cursor: &str,
    vote_key: &str,
) -> AppResult<bool> {
    let existing = tx
        .query_row(
            "SELECT vote_key FROM degov_votes WHERE event_cursor = ?1",
            [cursor],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    Ok(existing.is_some_and(|existing| existing != vote_key))
}

fn apply_projection_delta(
    tx: &rusqlite::Transaction<'_>,
    delta: ProposalProjectionDelta,
) -> AppResult<()> {
    let current = tx
        .query_row(
            "SELECT for_votes, against_votes, abstain_votes, vote_count
             FROM degov_proposals
             WHERE proposal_id = ?1",
            [&delta.proposal_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;

    let (mut for_votes, mut against_votes, mut abstain_votes, vote_count) =
        current.unwrap_or_else(|| ("0".to_owned(), "0".to_owned(), "0".to_owned(), 0));

    match delta.support {
        1 => for_votes = add_decimal_strings(&for_votes, &delta.weight),
        0 => against_votes = add_decimal_strings(&against_votes, &delta.weight),
        2 => abstain_votes = add_decimal_strings(&abstain_votes, &delta.weight),
        _ => {}
    };

    tx.execute(
        "INSERT INTO degov_proposals (
            proposal_id,
            for_votes,
            against_votes,
            abstain_votes,
            vote_count,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP)
        ON CONFLICT(proposal_id) DO UPDATE SET
            for_votes = excluded.for_votes,
            against_votes = excluded.against_votes,
            abstain_votes = excluded.abstain_votes,
            vote_count = excluded.vote_count,
            updated_at = CURRENT_TIMESTAMP",
        params![
            delta.proposal_id,
            for_votes,
            against_votes,
            abstain_votes,
            vote_count + 1,
        ],
    )?;
    Ok(())
}

fn to_governance_vote(event: &VoteCastEvent) -> AppResult<Option<GovernanceVote>> {
    let args = &event.event.decoded_args;
    let Some(proposal_id) = string_field(args, &["proposalId", "proposal_id"]) else {
        return Ok(None);
    };
    let Some(weight) = decimal_string_field(args, &["weight"]) else {
        return Ok(None);
    };

    let vote_key = event
        .event
        .transaction_hash
        .as_ref()
        .zip(event.event.log_index)
        .map(|(transaction_hash, log_index)| format!("{transaction_hash}:{log_index}"))
        .unwrap_or_else(|| event.cursor.clone());
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

    Ok(Some(GovernanceVote {
        vote_key,
        proposal_id,
        voter: string_field(args, &["voter", "account"]),
        support: integer_field(args, &["support"]).unwrap_or_default(),
        weight,
        reason: string_field(args, &["reason"]),
        transaction_hash: event.event.transaction_hash.clone(),
        block_number: event.event.block_number.map(i64::from),
        event_cursor: event.cursor.clone(),
        raw_event_json,
    }))
}

impl GovernanceVote {
    fn projection_delta(&self) -> ProposalProjectionDelta {
        ProposalProjectionDelta {
            proposal_id: self.proposal_id.clone(),
            support: self.support,
            weight: self.weight.clone(),
        }
    }
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.as_u64().map(|value| value.to_string()))
                .or_else(|| value.as_i64().map(|value| value.to_string()))
        })
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

fn decimal_string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.as_u64().map(|value| value.to_string()))
                .or_else(|| value.as_i64().map(|value| value.to_string()))
        })
        .filter(|value| value.chars().all(|character| character.is_ascii_digit()))
        .map(|value| trim_decimal_leading_zeroes(&value))
}

fn trim_decimal_leading_zeroes(value: &str) -> String {
    let trimmed = value.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn add_decimal_strings(left: &str, right: &str) -> String {
    let mut carry = 0_u8;
    let mut output = Vec::new();
    let mut left = left.as_bytes().iter().rev();
    let mut right = right.as_bytes().iter().rev();

    loop {
        let left_digit = left.next().map(|digit| digit - b'0');
        let right_digit = right.next().map(|digit| digit - b'0');
        if left_digit.is_none() && right_digit.is_none() && carry == 0 {
            break;
        }

        let sum = left_digit.unwrap_or(0) + right_digit.unwrap_or(0) + carry;
        output.push(char::from(b'0' + (sum % 10)));
        carry = sum / 10;
    }

    output.iter().rev().collect()
}
