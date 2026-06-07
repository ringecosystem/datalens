use datalens_chain::FinalityLevel;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange};
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use std::collections::BTreeMap;

use crate::{ObjectEncoding, ParquetCompression, range_kind_key, validate_object_key};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
/// Durable coverage authority for one or more chains. Query planners and
/// warmup/index runtimes use manifest entries, not cursors or usage records, to
/// decide whether a range is durably covered.
pub struct Manifest {
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub(crate) fn upsert(&mut self, entry: ManifestEntry) {
        self.entries.push(entry);
        self.normalize();
    }

    pub(crate) fn merge(&mut self, manifest: Manifest) {
        self.entries.extend(manifest.entries);
        self.normalize();
    }

    pub(crate) fn merge_filtering_shadowed_segments(
        &mut self,
        manifest: Manifest,
        base_entries: &[ManifestEntry],
    ) {
        self.entries
            .extend(manifest.entries.into_iter().filter(|entry| {
                if base_entries
                    .iter()
                    .any(|base_entry| base_entry.shadows_segment(entry))
                {
                    return false;
                }
                true
            }));
        self.normalize();
    }

    pub(crate) fn normalize(&mut self) {
        let mut entries = BTreeMap::new();
        for entry in self.entries.drain(..) {
            entries.insert(entry.logical_key(), entry);
        }
        self.entries = entries.into_values().collect();
    }

    pub(crate) fn find_logical(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector_fingerprint: &str,
        range: &LedgerRange,
        finality_level: ManifestFinalityLevel,
    ) -> Option<&ManifestEntry> {
        self.entries.iter().find(|existing| {
            existing.chain == *chain
                && existing.dataset_key == *dataset_key
                && existing.selector_fingerprint == selector_fingerprint
                && existing.range == *range
                && existing.finality_level == finality_level
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// One logical durable coverage record. Entries with no object key represent
/// provider-confirmed empty coverage; entries with an object key must carry full
/// object metadata so reads can validate stored bytes.
pub struct ManifestEntry {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub finality_level: ManifestFinalityLevel,
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_encoding: Option<ObjectEncoding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_compression: Option<ParquetCompression>,
    pub row_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub written_at_unix_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct RawManifestEntry {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    range: LedgerRange,
    selector_fingerprint: String,
    selector_canonical_key: String,
    finality_level: ManifestFinalityLevel,
    object_key: Option<String>,
    #[serde(default)]
    object_encoding: Option<ObjectEncoding>,
    #[serde(default)]
    object_compression: Option<ParquetCompression>,
    row_count: usize,
    #[serde(default)]
    object_size_bytes: Option<u64>,
    #[serde(default)]
    checksum: Option<String>,
    #[serde(default)]
    checksum_algorithm: Option<String>,
    #[serde(default)]
    written_at_unix_seconds: Option<u64>,
}

impl<'de> Deserialize<'de> for ManifestEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawManifestEntry::deserialize(deserializer)?;
        ManifestEntry::try_from_raw(raw).map_err(D::Error::custom)
    }
}

impl ManifestEntry {
    fn logical_key(&self) -> ManifestEntryLogicalKey {
        ManifestEntryLogicalKey {
            chain_key: self.chain.key_prefix(),
            dataset_key: self.dataset_key.as_str().to_owned(),
            selector_fingerprint: self.selector_fingerprint.clone(),
            range_kind: range_kind_key(self.range.kind()),
            range_start: self.range.start(),
            range_end: self.range.end(),
            finality: self.finality_level.as_str().to_owned(),
        }
    }

    fn shadows_segment(&self, segment: &ManifestEntry) -> bool {
        self.chain == segment.chain
            && self.dataset_key == segment.dataset_key
            && self.selector_fingerprint == segment.selector_fingerprint
            && self.finality_level == segment.finality_level
            && self.object_key.is_some()
            && segment.object_key.is_some()
            && self.range.kind() == segment.range.kind()
            && self.range.start() <= segment.range.start()
            && self.range.end() >= segment.range.end()
    }

    fn try_from_raw(raw: RawManifestEntry) -> Result<Self, DatalensError> {
        validate_object_key(&raw.selector_fingerprint)?;
        validate_object_key(&raw.selector_canonical_key)?;
        if let Some(object_key) = raw.object_key.as_deref() {
            validate_object_key(object_key)?;
        }
        if let (Some(object_key), Some(object_encoding)) =
            (raw.object_key.as_deref(), raw.object_encoding)
        {
            object_encoding.validate_object_key(object_key)?;
        }
        match raw.object_key {
            Some(object_key) => {
                if raw.row_count == 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage must have row_count greater than zero",
                    ));
                }
                let object_encoding =
                    required_data_object_metadata(raw.object_encoding, "object_encoding")?;
                let object_size_bytes =
                    required_data_object_metadata(raw.object_size_bytes, "object_size_bytes")?;
                let checksum = required_data_object_metadata(raw.checksum, "checksum")?;
                let checksum_algorithm =
                    required_data_object_metadata(raw.checksum_algorithm, "checksum_algorithm")?;
                let written_at_unix_seconds = required_data_object_metadata(
                    raw.written_at_unix_seconds,
                    "written_at_unix_seconds",
                )?;
                Ok(Self {
                    chain: raw.chain,
                    dataset_key: raw.dataset_key,
                    range: raw.range,
                    selector_fingerprint: raw.selector_fingerprint,
                    selector_canonical_key: raw.selector_canonical_key,
                    finality_level: raw.finality_level,
                    object_key: Some(object_key),
                    object_encoding: Some(object_encoding),
                    object_compression: raw.object_compression,
                    row_count: raw.row_count,
                    object_size_bytes: Some(object_size_bytes),
                    checksum: Some(checksum),
                    checksum_algorithm: Some(checksum_algorithm),
                    written_at_unix_seconds: Some(written_at_unix_seconds),
                })
            }
            None => {
                if raw.row_count != 0 {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "empty coverage must have row_count zero",
                    ));
                }
                Ok(Self {
                    chain: raw.chain,
                    dataset_key: raw.dataset_key,
                    range: raw.range,
                    selector_fingerprint: raw.selector_fingerprint,
                    selector_canonical_key: raw.selector_canonical_key,
                    finality_level: raw.finality_level,
                    object_key: None,
                    object_encoding: None,
                    object_compression: None,
                    row_count: raw.row_count,
                    object_size_bytes: None,
                    checksum: None,
                    checksum_algorithm: None,
                    written_at_unix_seconds: None,
                })
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ManifestEntryLogicalKey {
    chain_key: String,
    dataset_key: String,
    selector_fingerprint: String,
    range_kind: String,
    range_start: u64,
    range_end: u64,
    finality: String,
}

fn required_data_object_metadata<T>(value: Option<T>, field: &str) -> Result<T, DatalensError> {
    value.ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("data object coverage must include {field}"),
        )
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Finality levels permitted in durable manifest coverage. Latest and
/// chain-specific finality values are rejected before a manifest entry can be
/// written.
pub enum ManifestFinalityLevel {
    Safe,
    Finalized,
}

impl TryFrom<FinalityLevel> for ManifestFinalityLevel {
    type Error = DatalensError;

    fn try_from(value: FinalityLevel) -> Result<Self, Self::Error> {
        match value {
            FinalityLevel::Safe => Ok(Self::Safe),
            FinalityLevel::Finalized => Ok(Self::Finalized),
            FinalityLevel::Latest | FinalityLevel::ChainSpecific(_) => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable storage coverage requires safe or finalized finality",
            )),
        }
    }
}

impl ManifestFinalityLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }
}
