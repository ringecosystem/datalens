use std::collections::BTreeSet;

const REGISTRY_SOURCES: &[&str] = &[
    include_str!("../../engine/src/metrics.rs"),
    include_str!("../../provider/src/metrics.rs"),
];

#[test]
fn grafana_dashboard_only_names_registered_metrics() {
    let dashboard = include_str!("../../../deploy/grafana/datalens.json");
    serde_json::from_str::<serde_json::Value>(dashboard).expect("dashboard json");
    let mut missing = Vec::new();
    for name in metric_names(dashboard) {
        let base = name
            .trim_end_matches("_bucket")
            .trim_end_matches("_sum")
            .trim_end_matches("_count");
        let quoted = format!("\"{base}\"");
        if !REGISTRY_SOURCES
            .iter()
            .any(|source| source.contains(&quoted))
        {
            missing.push(base.to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "dashboard names missing from the metric registry: {missing:?}"
    );
}

fn metric_names(dashboard: &str) -> BTreeSet<&str> {
    let mut names = BTreeSet::new();
    let bytes = dashboard.as_bytes();
    let mut index = 0;
    while let Some(found) = dashboard[index..].find("datalens_") {
        let start = index + found;
        let end = bytes[start..]
            .iter()
            .position(|byte| !byte.is_ascii_lowercase() && *byte != b'_')
            .map(|offset| start + offset)
            .unwrap_or(dashboard.len());
        names.insert(&dashboard[start..end]);
        index = end;
    }
    names
}
