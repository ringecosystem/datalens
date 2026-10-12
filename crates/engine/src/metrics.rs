use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

#[derive(Clone)]
pub struct EngineMetrics {
    queries: IntCounterVec,
    query_duration: HistogramVec,
    fill_jobs: IntGaugeVec,
    fill_duration: HistogramVec,
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
            &["chain", "dataset"],
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
        registry.register(Box::new(queries.clone()))?;
        registry.register(Box::new(query_duration.clone()))?;
        registry.register(Box::new(fill_jobs.clone()))?;
        registry.register(Box::new(fill_duration.clone()))?;
        Ok(Self {
            queries,
            query_duration,
            fill_jobs,
            fill_duration,
        })
    }

    pub fn record_query(
        &self,
        application: &str,
        chain: &str,
        dataset: &str,
        outcome: &str,
        seconds: f64,
    ) {
        self.queries
            .with_label_values(&[application, chain, dataset, outcome])
            .inc();
        self.query_duration
            .with_label_values(&[chain, dataset])
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
}
