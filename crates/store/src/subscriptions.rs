use std::time::Duration;

use chrono::{DateTime, Utc};
use datalens_core::{ChainId, Dataset};

use crate::codec::{chain_from_db, dataset_from_db, duration_millis};
use crate::ids::SelectorId;
use crate::{Store, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionSource {
    Auto,
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub chain_id: ChainId,
    pub dataset: Dataset,
    pub selector_id: SelectorId,
    pub application_id: String,
    pub source: SubscriptionSource,
    pub expires_at: DateTime<Utc>,
}

impl SubscriptionSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Explicit => "explicit",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "auto" => Ok(Self::Auto),
            "explicit" => Ok(Self::Explicit),
            other => Err(StoreError::data(format!(
                "unknown subscription source {other}"
            ))),
        }
    }
}

impl Store {
    /// Creates or renews a subscription. `explicit` is never downgraded to `auto`, and the
    /// expiry only moves later. An `auto` renewal writes at most once a minute, because it runs
    /// on every follow-attached query.
    pub async fn touch_subscription(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        application_id: &str,
        source: SubscriptionSource,
        ttl: Duration,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "insert into subscriptions (
                chain_id, dataset, selector_id, application_id, source, last_query_at, expires_at
             )
             values ($1, $2, $3, $4, $5, now(), now() + ($6::bigint * interval '1 millisecond'))
             on conflict (chain_id, dataset, selector_id, application_id) do update set
                source = case
                    when subscriptions.source = 'explicit' then 'explicit'
                    else excluded.source
                end,
                last_query_at = now(),
                expires_at = greatest(subscriptions.expires_at, excluded.expires_at)
             where excluded.source = 'explicit'
                or subscriptions.last_query_at < now() - interval '60 seconds'",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(application_id)
        .bind(source.as_str())
        .bind(duration_millis(ttl)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn active_subscriptions(
        &self,
        chain: ChainId,
    ) -> Result<Vec<Subscription>, StoreError> {
        let rows: Vec<(i32, String, i64, String, String, DateTime<Utc>)> = sqlx::query_as(
            "select chain_id, dataset, selector_id, application_id, source, expires_at
             from subscriptions
             where chain_id = $1 and expires_at > now()
             order by dataset, selector_id, application_id",
        )
        .bind(chain.get())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(subscription_from_row).collect()
    }

    /// Unexpired subscriptions of one application on every chain, for its quota.
    pub async fn application_subscriptions(
        &self,
        application_id: &str,
    ) -> Result<Vec<Subscription>, StoreError> {
        let rows: Vec<(i32, String, i64, String, String, DateTime<Utc>)> = sqlx::query_as(
            "select chain_id, dataset, selector_id, application_id, source, expires_at
             from subscriptions
             where application_id = $1 and expires_at > now()
             order by chain_id, dataset, selector_id",
        )
        .bind(application_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(subscription_from_row).collect()
    }

    pub async fn remove_subscription(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        application_id: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "delete from subscriptions
             where chain_id = $1 and dataset = $2 and selector_id = $3 and application_id = $4",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(application_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn expire_subscriptions(&self, now: DateTime<Utc>) -> Result<u64, StoreError> {
        let result = sqlx::query("delete from subscriptions where expires_at <= $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

fn subscription_from_row(
    (chain_id, dataset, selector_id, application_id, source, expires_at): (
        i32,
        String,
        i64,
        String,
        String,
        DateTime<Utc>,
    ),
) -> Result<Subscription, StoreError> {
    Ok(Subscription {
        chain_id: chain_from_db(chain_id)?,
        dataset: dataset_from_db(&dataset)?,
        selector_id: SelectorId::from_raw(selector_id),
        application_id,
        source: SubscriptionSource::parse(&source)?,
        expires_at,
    })
}
