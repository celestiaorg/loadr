//! Store + regression-check integration tests (in-memory SQLite, no network).

use loadr_core::aggregate::{AggValues, Snapshot};
use loadr_core::summary::{MetricSummary, Summary};
use loadr_core::MetricKind;
use loadr_history::{check, Store};

fn summary(run_id: &str, ts: u64, p99: f64) -> Summary {
    Summary {
        name: Some("checkout".into()),
        run_id: run_id.into(),
        started_ms: 0,
        ended_ms: ts,
        duration_secs: 1.0,
        scenarios: vec!["s".into()],
        metrics: vec![MetricSummary {
            metric: "http_req_duration".into(),
            kind: MetricKind::Trend,
            agg: AggValues {
                med: Some(50.0),
                p95: Some(p99 * 0.8),
                p99: Some(p99),
                ..Default::default()
            },
        }],
        checks: vec![],
        thresholds: vec![],
        thresholds_passed: true,
        aborted: None,
        snapshot: Snapshot::default(),
        timeline: vec![],
        jobs: vec![],
    }
}

#[test]
fn records_lists_and_detects_a_regression() {
    let store = Store::open_memory().unwrap();

    // Six stable baseline runs, p99 ~120ms.
    for i in 0..6 {
        store
            .record(
                &summary(&format!("r{i}"), 1000 + i, 120.0 + i as f64),
                "checkout",
                None,
                None,
            )
            .unwrap();
    }
    assert_eq!(store.list(Some("checkout")).unwrap().len(), 6);

    // A run that spikes p99.
    let spike = summary("spike", 2000, 900.0);
    store.record(&spike, "checkout", None, None).unwrap();

    let rows = check(&store, &spike, "checkout", 20).unwrap();
    let p99 = rows.iter().find(|r| r.field == "p99").expect("p99 row");
    assert!(
        p99.verdict.regression,
        "900ms vs ~120ms baseline should regress (z={})",
        p99.verdict.z
    );
    assert!(
        !p99.verdict.low_confidence,
        "6 samples is enough for robust stats"
    );
}

#[test]
fn a_normal_run_is_not_flagged() {
    let store = Store::open_memory().unwrap();
    for i in 0..8 {
        store
            .record(
                &summary(&format!("r{i}"), 1000 + i, 120.0 + (i % 3) as f64),
                "checkout",
                None,
                None,
            )
            .unwrap();
    }
    let ok = summary("ok", 3000, 121.0);
    store.record(&ok, "checkout", None, None).unwrap();
    let rows = check(&store, &ok, "checkout", 20).unwrap();
    assert!(
        rows.iter().all(|r| !r.verdict.regression),
        "a normal run must not regress"
    );
}
