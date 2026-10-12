use std::collections::BTreeSet;
use std::ops::Deref;

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Dataset, Error, LogRow};

/// Non-empty set of topic values. An empty set would match nothing, which is rejected as
/// invalid input rather than modeled.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicSet(BTreeSet<B256>);

impl TopicSet {
    pub fn new(topics: impl IntoIterator<Item = B256>) -> Result<Self, Error> {
        let set: BTreeSet<B256> = topics.into_iter().collect();
        if set.is_empty() {
            Err(Error::invalid("topic filter must list at least one topic"))
        } else {
            Ok(Self(set))
        }
    }
}

impl Deref for TopicSet {
    type Target = BTreeSet<B256>;

    fn deref(&self) -> &BTreeSet<B256> {
        &self.0
    }
}

/// One topic position: `null` on the wire is `Any`, a non-empty array is `OneOf`.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "Option<Vec<B256>>", into = "Option<Vec<B256>>")]
pub enum TopicFilter {
    #[default]
    Any,
    OneOf(TopicSet),
}

impl TopicFilter {
    pub fn one_of(topics: impl IntoIterator<Item = B256>) -> Result<Self, Error> {
        TopicSet::new(topics).map(Self::OneOf)
    }

    /// True when every topic this filter accepts is also accepted by `self`.
    pub fn covers(&self, other: &TopicFilter) -> bool {
        match (self, other) {
            (Self::Any, _) => true,
            (Self::OneOf(_), Self::Any) => false,
            (Self::OneOf(wide), Self::OneOf(narrow)) => narrow.is_subset(wide),
        }
    }

    pub fn matches(&self, topic: Option<&B256>) -> bool {
        match self {
            Self::Any => true,
            Self::OneOf(set) => topic.is_some_and(|topic| set.contains(topic)),
        }
    }

    fn canonical(&self) -> String {
        match self {
            Self::Any => "*".to_owned(),
            Self::OneOf(set) => join_hex(set.iter().map(|topic| topic.as_slice())),
        }
    }
}

impl TryFrom<Option<Vec<B256>>> for TopicFilter {
    type Error = Error;

    fn try_from(value: Option<Vec<B256>>) -> Result<Self, Error> {
        match value {
            None => Ok(Self::Any),
            Some(topics) => Self::one_of(topics),
        }
    }
}

impl From<TopicFilter> for Option<Vec<B256>> {
    fn from(value: TopicFilter) -> Self {
        match value {
            TopicFilter::Any => None,
            TopicFilter::OneOf(set) => Some(set.0.into_iter().collect()),
        }
    }
}

/// `evm.logs` selector. Empty `addresses` means any address.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawLogSelector", into = "RawLogSelector")]
pub struct LogSelector {
    pub addresses: BTreeSet<Address>,
    pub topics: [TopicFilter; 4],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLogSelector {
    #[serde(default)]
    addresses: BTreeSet<Address>,
    #[serde(default)]
    topics: Vec<TopicFilter>,
}

impl TryFrom<RawLogSelector> for LogSelector {
    type Error = Error;

    fn try_from(raw: RawLogSelector) -> Result<Self, Error> {
        if raw.topics.len() > 4 {
            return Err(Error::invalid(format!(
                "selector has {} topic positions, at most 4 allowed",
                raw.topics.len()
            )));
        }
        let mut topics: [TopicFilter; 4] = Default::default();
        for (slot, filter) in topics.iter_mut().zip(raw.topics) {
            *slot = filter;
        }
        Ok(Self {
            addresses: raw.addresses,
            topics,
        })
    }
}

impl From<LogSelector> for RawLogSelector {
    fn from(selector: LogSelector) -> Self {
        let mut topics = Vec::from(selector.topics);
        while topics.last() == Some(&TopicFilter::Any) {
            topics.pop();
        }
        Self {
            addresses: selector.addresses,
            topics,
        }
    }
}

impl LogSelector {
    /// Stable textual identity: lowercase hex, sets sorted, `*` for wildcards.
    pub fn canonical_key(&self) -> String {
        let addresses = if self.addresses.is_empty() {
            "*".to_owned()
        } else {
            join_hex(self.addresses.iter().map(|address| address.as_slice()))
        };
        let topics: Vec<String> = self.topics.iter().map(TopicFilter::canonical).collect();
        format!("addresses={addresses};topics={}", topics.join("|"))
    }

    pub fn fingerprint(&self) -> String {
        sha256_hex(&self.canonical_key())
    }

    /// True when every log matched by `other` is also matched by `self`, so coverage
    /// recorded for `self` proves completeness for `other`.
    pub fn covers(&self, other: &LogSelector) -> bool {
        let addresses_cover = self.addresses.is_empty()
            || (!other.addresses.is_empty() && other.addresses.is_subset(&self.addresses));
        addresses_cover
            && self
                .topics
                .iter()
                .zip(&other.topics)
                .all(|(wide, narrow)| wide.covers(narrow))
    }

    pub fn matches(&self, log: &LogRow) -> bool {
        (self.addresses.is_empty() || self.addresses.contains(&log.address))
            && self
                .topics
                .iter()
                .enumerate()
                .all(|(position, filter)| filter.matches(log.topics.get(position)))
    }

    /// One selector per address with the same topic filters. An any-address selector
    /// cannot be split and is returned as is.
    pub fn split_by_address(&self) -> Vec<LogSelector> {
        if self.addresses.is_empty() {
            return vec![self.clone()];
        }
        self.addresses
            .iter()
            .map(|address| LogSelector {
                addresses: BTreeSet::from([*address]),
                topics: self.topics.clone(),
            })
            .collect()
    }

    /// Smallest single selector that covers every member, for one merged `eth_getLogs`.
    ///
    /// Addresses are unioned (any-address if a member is any-address). Topic0 is unioned
    /// when every member constrains it, otherwise `Any`. Positions 1..=3 keep a filter only
    /// when all members agree on it exactly. Returns `None` for no members or when the
    /// address union exceeds `max_addresses`.
    pub fn batch(members: &[LogSelector], max_addresses: usize) -> Option<LogSelector> {
        let (first, rest) = members.split_first()?;

        let addresses = if members.iter().any(|member| member.addresses.is_empty()) {
            BTreeSet::new()
        } else {
            let union: BTreeSet<Address> = members
                .iter()
                .flat_map(|member| member.addresses.iter().copied())
                .collect();
            if union.len() > max_addresses {
                return None;
            }
            union
        };

        let topic0 = members
            .iter()
            .map(|member| match &member.topics[0] {
                TopicFilter::Any => None,
                TopicFilter::OneOf(set) => Some(set.iter().copied()),
            })
            .collect::<Option<Vec<_>>>()
            .map_or(TopicFilter::Any, |sets| {
                TopicFilter::OneOf(TopicSet(sets.into_iter().flatten().collect()))
            });

        let mut topics = [topic0, TopicFilter::Any, TopicFilter::Any, TopicFilter::Any];
        for (position, slot) in topics.iter_mut().enumerate().skip(1) {
            let shared = &first.topics[position];
            if rest.iter().all(|member| &member.topics[position] == shared) {
                *slot = shared.clone();
            }
        }

        Some(LogSelector { addresses, topics })
    }
}

/// Selector of any dataset. `evm.blocks` has the single selector `all`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Selector {
    Logs(LogSelector),
    Blocks,
}

impl Selector {
    pub fn dataset(&self) -> Dataset {
        match self {
            Self::Logs(_) => Dataset::EvmLogs,
            Self::Blocks => Dataset::EvmBlocks,
        }
    }

    pub fn canonical_key(&self) -> String {
        match self {
            Self::Logs(selector) => selector.canonical_key(),
            Self::Blocks => "all".to_owned(),
        }
    }

    pub fn fingerprint(&self) -> String {
        sha256_hex(&self.canonical_key())
    }
}

fn join_hex<'a>(values: impl Iterator<Item = &'a [u8]>) -> String {
    values
        .map(|value| format!("0x{}", hex::encode(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn sha256_hex(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}
