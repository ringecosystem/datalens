use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

#[derive(Clone)]
pub struct EngineMetrics {
    queries: IntCounterVec,
    query_duration: HistogramVec,
    fill_jobs: IntGaugeVec,
    fill_duration: HistogramVec,
    follow_lag: IntGaugeVec,
    reorgs: IntCounterVec,
    finality_violations: IntCounterVec,
    subscriptions: IntGaugeVec,
    follow_halted: IntGaugeVec,
}

impl EngineMetrics {
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let queries = IntCounterVec::new(
            Opts::new(
                "datalens_queries_total",
                "Queries by application, chain, dataset, and outcome",
            ),
            &["application", "chain", "dataset", "outcome"],
        )?;
        let query_duration = HistogramVec::new(
            HistogramOpts::new(
                "datalens_query_duration_seconds",
                "Query handling time, fills included",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 30.0,
            ]),
            &["chain", "dataset", "finality", "outcome"],
        )?;
        let fill_jobs = IntGaugeVec::new(
            Opts::new("datalens_fill_jobs", "Fill jobs by status"),
            &["status"],
        )?;
        let fill_duration = HistogramVec::new(
            HistogramOpts::new(
                "datalens_fill_job_duration_seconds",
                "Time from claim to commit of one fill batch",
            )
            .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["chain", "dataset", "outcome"],
        )?;
        let follow_lag = IntGaugeVec::new(
            Opts::new(
                "datalens_follow_lag_blocks",
                "Latest head minus the lowest followed head",
            ),
            &["chain"],
        )?;
        let reorgs = IntCounterVec::new(
            Opts::new("datalens_reorgs_total", "Unsafe rollbacks after a fork"),
            &["chain"],
        )?;
        let finality_violations = IntCounterVec::new(
            Opts::new(
                "datalens_finality_violations_total",
                "Forks at or below the safe head",
            ),
            &["chain"],
        )?;
        let subscriptions = IntGaugeVec::new(
            Opts::new("datalens_subscriptions", "Unexpired subscriptions"),
            &["chain", "dataset"],
        )?;
        let follow_halted = IntGaugeVec::new(
            Opts::new(
                "datalens_follow_halted",
                "1 when follow stopped after a fork it may not roll back",
            ),
            &["chain"],
        )?;
        registry.register(Box::new(queries.clone()))?;
        registry.register(Box::new(query_duration.clone()))?;
        registry.register(Box::new(fill_jobs.clone()))?;
        registry.register(Box::new(fill_duration.clone()))?;
        registry.register(Box::new(follow_lag.clone()))?;
        registry.register(Box::new(reorgs.clone()))?;
        registry.register(Box::new(finality_violations.clone()))?;
        registry.register(Box::new(subscriptions.clone()))?;
        registry.register(Box::new(follow_halted.clone()))?;
        Ok(Self {
            queries,
            query_duration,
            fill_jobs,
            fill_duration,
            follow_lag,
            reorgs,
            finality_violations,
            subscriptions,
            follow_halted,
        })
    }

    pub fn record_query(
        &self,
        application: &str,
        chain: &str,
        dataset: &str,
        finality: &str,
        outcome: &str,
        seconds: f64,
    ) {
        self.queries
            .with_label_values(&[application, chain, dataset, outcome])
            .inc();
        self.query_duration
            .with_label_values(&[chain, dataset, finality, outcome])
            .observe(seconds);
    }

    pub(crate) fn set_fill_jobs(&self, status: &str, count: i64) {
        self.fill_jobs.with_label_values(&[status]).set(count);
    }

    pub(crate) fn observe_fill(&self, chain: &str, dataset: &str, outcome: &str, seconds: f64) {
        self.fill_duration
            .with_label_values(&[chain, dataset, outcome])
            .observe(seconds);
    }

    pub(crate) fn set_follow_lag(&self, chain: &str, blocks: u64) {
        self.follow_lag
            .with_label_values(&[chain])
            .set(i64::try_from(blocks).unwrap_or(i64::MAX));
    }

    pub(crate) fn count_reorg(&self, chain: &str) {
        self.reorgs.with_label_values(&[chain]).inc();
    }

    pub(crate) fn count_finality_violation(&self, chain: &str) {
        self.finality_violations.with_label_values(&[chain]).inc();
    }

    pub(crate) fn set_subscriptions(&self, chain: &str, dataset: &str, count: usize) {
        self.subscriptions
            .with_label_values(&[chain, dataset])
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    pub(crate) fn set_follow_halted(&self, chain: &str, halted: bool) {
        self.follow_halted
            .with_label_values(&[chain])
            .set(i64::from(halted));
    }
}
