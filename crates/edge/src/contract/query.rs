use datalens_chain::{AdapterKey, DatasetSelector};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, LogFilter, QueryDataFinality, QueryFinalityRequirement, QuerySegmentSource,
};
use datalens_planner::NativeQueryInput;
use serde::{Deserialize, Serialize};

use crate::service::query_service::{NativeCacheSummary, NativeQueryResponse};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryApiRequest {
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub selector: QuerySelectorApi,
    pub range: QueryRangeApi,
    #[serde(default)]
    pub finality: QueryFinalityRequirement,
    #[serde(default)]
    pub fields: FieldSelectionApi,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryRangeApi {
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QuerySelectorApi {
    All,
    EvmLogs(LogFilter),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum FieldSelectionApi {
    #[default]
    All,
    Include(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryApiResponse {
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub range: QueryRangeApi,
    pub cache: QueryCacheApi,
    pub rows: DatasetRows,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryCacheApi {
    pub hit_ranges: Vec<QueryRangeApi>,
    pub missing_ranges: Vec<QueryRangeApi>,
    pub durable_hit_ranges: Vec<QueryRangeApi>,
    pub hot_hit_ranges: Vec<QueryRangeApi>,
    pub provider_fill_ranges: Vec<QueryRangeApi>,
    pub promotion_pending_ranges: Vec<QueryRangeApi>,
    pub segments: Vec<QuerySegmentApi>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuerySegmentApi {
    pub range: QueryRangeApi,
    pub source: QuerySegmentSource,
    pub finality: QueryDataFinality,
}

impl QueryApiRequest {
    pub fn into_native_input(self) -> Result<NativeQueryInput, DatalensError> {
        Ok(NativeQueryInput {
            chain: self.chain,
            dataset_key: parse_dataset_key(&self.dataset_key)?,
            ledger_range: self.range.into_ledger_range()?,
            selector: self.selector.into_selector()?,
            field_selection: self.fields.into_field_selection(),
            finality: self.finality,
        })
    }

    pub(crate) fn dataset_for_auth(&self) -> Result<String, DatalensError> {
        Ok(parse_dataset_key(&self.dataset_key)?.as_str().to_owned())
    }

    pub(crate) fn range_len(&self) -> u128 {
        self.range.len()
    }
}

impl QueryRangeApi {
    fn len(&self) -> u128 {
        let (start, end) = match *self {
            Self::Block { start, end }
            | Self::Slot { start, end }
            | Self::Height { start, end } => (start, end),
        };
        u128::from(end.saturating_sub(start)) + 1
    }

    pub(crate) fn into_ledger_range(self) -> Result<LedgerRange, DatalensError> {
        match self {
            Self::Block { start, end } => LedgerRange::blocks(start, end),
            Self::Slot { start, end } => LedgerRange::slots(start, end),
            Self::Height { start, end } => LedgerRange::heights(start, end),
        }
    }

    pub(crate) fn from_ledger_range(range: LedgerRange) -> Result<Self, DatalensError> {
        let start = range.start();
        let end = range.end();
        match range.kind() {
            LedgerRangeKind::Block => Ok(Self::Block { start, end }),
            LedgerRangeKind::Slot => Ok(Self::Slot { start, end }),
            LedgerRangeKind::Height => Ok(Self::Height { start, end }),
            LedgerRangeKind::Other(kind) => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("ledger range kind {kind} is not supported by the query API"),
            )),
        }
    }
}

impl QuerySelectorApi {
    pub(crate) fn into_selector(self) -> Result<DatasetSelector, DatalensError> {
        match self {
            Self::All => Ok(DatasetSelector::all()),
            Self::EvmLogs(filter) => DatasetSelector::try_evm_logs(filter),
            Self::Other {
                kind,
                fingerprint,
                canonical_key,
            } => DatasetSelector::try_other(AdapterKey::try_new(kind)?, fingerprint, canonical_key),
        }
    }
}

impl FieldSelectionApi {
    fn into_field_selection(self) -> datalens_planner::FieldSelection {
        match self {
            Self::All => datalens_planner::FieldSelection::All,
            Self::Include(fields) => datalens_planner::FieldSelection::Include(fields),
        }
    }
}

impl Serialize for FieldSelectionApi {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::All => serializer.serialize_str("all"),
            Self::Include(fields) => {
                use serde::ser::SerializeMap;

                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("include", fields)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for FieldSelectionApi {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value == serde_json::Value::String("all".to_owned()) {
            return Ok(Self::All);
        }
        if let Some(include) = value.get("include") {
            let fields =
                Vec::<String>::deserialize(include.clone()).map_err(serde::de::Error::custom)?;
            return Ok(Self::Include(fields));
        }
        Err(serde::de::Error::custom(
            "fields must be \"all\" or an object with include",
        ))
    }
}

impl QueryApiResponse {
    pub fn try_from_native_response(response: NativeQueryResponse) -> Result<Self, DatalensError> {
        Ok(Self {
            chain: response.chain,
            dataset_key: response.dataset_key.as_str().to_owned(),
            range: QueryRangeApi::from_ledger_range(response.ledger_range)?,
            cache: QueryCacheApi::try_from(response.cache)?,
            rows: response.rows,
        })
    }
}

impl From<NativeQueryResponse> for QueryApiResponse {
    fn from(response: NativeQueryResponse) -> Self {
        Self::try_from_native_response(response)
            .expect("native response uses query API ledger range kinds")
    }
}

impl TryFrom<NativeCacheSummary> for QueryCacheApi {
    type Error = DatalensError;

    fn try_from(cache: NativeCacheSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            hit_ranges: query_api_ranges(cache.hit_ranges)?,
            missing_ranges: query_api_ranges(cache.missing_ranges)?,
            durable_hit_ranges: query_api_ranges(cache.durable_hit_ranges)?,
            hot_hit_ranges: query_api_ranges(cache.hot_hit_ranges)?,
            provider_fill_ranges: query_api_ranges(cache.provider_fill_ranges)?,
            promotion_pending_ranges: query_api_ranges(cache.promotion_pending_ranges)?,
            segments: cache
                .segments
                .into_iter()
                .map(QuerySegmentApi::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<datalens_core::QuerySegmentMetadata> for QuerySegmentApi {
    type Error = DatalensError;

    fn try_from(segment: datalens_core::QuerySegmentMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            range: QueryRangeApi::from_ledger_range(segment.range)?,
            source: segment.source,
            finality: segment.finality,
        })
    }
}

fn query_api_ranges(ranges: Vec<LedgerRange>) -> Result<Vec<QueryRangeApi>, DatalensError> {
    ranges
        .into_iter()
        .map(QueryRangeApi::from_ledger_range)
        .collect()
}

pub(crate) fn parse_dataset_key(value: &str) -> Result<DatasetKey, DatalensError> {
    DatasetKey::parse(value)
}
