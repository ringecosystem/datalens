use prometheus::{GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};

/// Counters the server registers once and shares across per-chain gateways.
#[derive(Clone)]
pub struct ProviderMetrics {
    calls: IntCounterVec,
    rejections: IntCounterVec,
    duration: HistogramVec,
    tokens: GaugeVec,
}

impl ProviderMetrics {
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let calls = IntCounterVec::new(
            Opts::new(
                "datalens_provider_calls_total",
                "JSON-RPC calls by chain, method, purpose, application, and outcome",
            ),
            &["chain", "method", "purpose", "application", "outcome"],
        )?;
        let rejections = IntCounterVec::new(
            Opts::new(
                "datalens_provider_budget_rejections_total",
                "Calls that never reached the provider because the budget would not grant a token",
            ),
            &["chain", "purpose"],
        )?;
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "datalens_provider_call_duration_seconds",
                "HTTP duration of a provider call",
            ),
            &["chain", "method", "purpose"],
        )?;
        let tokens = GaugeVec::new(
            Opts::new(
                "datalens_provider_budget_tokens",
                "Remaining token-bucket balance for a chain",
            ),
            &["chain"],
        )?;
        registry.register(Box::new(calls.clone()))?;
        registry.register(Box::new(rejections.clone()))?;
        registry.register(Box::new(duration.clone()))?;
        registry.register(Box::new(tokens.clone()))?;
        Ok(Self {
            calls,
            rejections,
            duration,
            tokens,
        })
    }

    pub(crate) fn record_call(
        &self,
        chain: &str,
        method: &str,
        purpose: &str,
        application: &str,
        outcome: &str,
    ) {
        self.calls
            .with_label_values(&[chain, method, purpose, application, outcome])
            .inc();
    }

    pub(crate) fn record_rejection(&self, chain: &str, purpose: &str) {
        self.rejections.with_label_values(&[chain, purpose]).inc();
    }

    pub(crate) fn observe_duration(&self, chain: &str, method: &str, purpose: &str, seconds: f64) {
        self.duration
            .with_label_values(&[chain, method, purpose])
            .observe(seconds);
    }

    pub(crate) fn set_tokens(&self, chain: &str, tokens: f64) {
        self.tokens.with_label_values(&[chain]).set(tokens);
    }
}
