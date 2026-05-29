use datalens_chain::FinalityLevel;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange};
use serde::{Deserialize, Deserializer, Serialize, de::Error};

use crate::{ObjectEncoding, range_kind_key, validate_object_key};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub(crate) fn upsert(&mut self, entry: ManifestEntry) {
        if let Some(existing) = self.entries.iter_mut().find(|existing| {
            existing.chain == entry.chain
                && existing.dataset_key == entry.dataset_key
                && existing.selector_fingerprint == entry.selector_fingerprint
                && existing.range == entry.range
                && existing.finality_level == entry.finality_level
        }) {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.entries.sort_by_key(|entry| {
            (
                entry.dataset_key.as_str().to_owned(),
                range_kind_key(entry.range.kind()),
                entry.selector_fingerprint.clone(),
                entry.range.start(),
                entry.range.end(),
            )
        });
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
                let object_encoding = raw.object_encoding.ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage requires object_encoding",
                    )
                })?;
                let object_size_bytes = raw.object_size_bytes.ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage requires object_size_bytes",
                    )
                })?;
                let checksum = raw.checksum.ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage requires checksum",
                    )
                })?;
                let checksum_algorithm = raw.checksum_algorithm.ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage requires checksum_algorithm",
                    )
                })?;
                let written_at_unix_seconds = raw.written_at_unix_seconds.ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "data object coverage requires written_at_unix_seconds",
                    )
                })?;
                Ok(Self {
                    chain: raw.chain,
                    dataset_key: raw.dataset_key,
                    range: raw.range,
                    selector_fingerprint: raw.selector_fingerprint,
                    selector_canonical_key: raw.selector_canonical_key,
                    finality_level: raw.finality_level,
                    object_key: Some(object_key),
                    object_encoding: Some(object_encoding),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
