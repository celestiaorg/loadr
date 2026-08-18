//! End-to-end tests for native dynamic-library plugins: build the example
//! cdylibs, load them via abi_stable, and drive the core-facing adapters.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use ed25519_dalek::Verifier;
use indexmap::IndexMap;

use loadr_core::data::{DataSourcePlugin, PluginRowCtx, PluginRowResult};
use loadr_core::metrics::{now_millis, MetricKind, MetricRegistry, MetricsBus, Sample, Tags};
use loadr_core::vu::RunContext;
use loadr_core::{
    Aggregator, Output, PreparedRequest, ProtocolHandler, RequestOptions, Summary, VuContext,
};
use loadr_plugin_api::NativePlugin;

fn output_so() -> std::path::PathBuf {
    common::build_native_example("loadr-plugin-example-native-output", "native_output")
}

fn protocol_so() -> std::path::PathBuf {
    common::build_native_example("loadr-plugin-example-native-protocol", "native_protocol")
}

fn data_source_so() -> std::path::PathBuf {
    common::build_native_example(
        "loadr-plugin-example-native-data-source",
        "native_data_source",
    )
}

/// An existing `kind = "service"` plugin that does NOT implement
/// `data_source` -- used to prove a plain lifecycle-only service plugin
/// still loads cleanly under the new `Service { service, data_source }`
/// shape.
fn hmac_signer_so() -> std::path::PathBuf {
    common::build_native_example("loadr-plugin-hmac-signer", "loadr_plugin_hmac_signer")
}

/// A manifest for a `kind = "service"` native plugin, built directly (no
/// `plugin.toml` on disk) so these tests don't depend on plugin discovery.
fn service_manifest(
    entry: std::path::PathBuf,
    default_config: serde_json::Value,
) -> loadr_plugin_api::PluginManifest {
    loadr_plugin_api::PluginManifest {
        name: "test-service".to_string(),
        version: "0.1.0".to_string(),
        kind: loadr_plugin_api::PluginKind::Service,
        plugin_type: loadr_plugin_api::PluginType::Native,
        abi: None,
        entry,
        description: String::new(),
        default_config,
        schemes: Vec::new(),
        capabilities: Vec::new(),
        dir: std::env::temp_dir(),
        enabled: true,
    }
}

fn sample(metric: &str, kind: MetricKind, value: f64) -> Sample {
    Sample {
        metric: Arc::from(metric),
        kind,
        value,
        tags: Arc::new(Tags::new()),
        timestamp_ms: now_millis(),
    }
}

fn minimal_vu() -> VuContext {
    let (bus, _rx) = MetricsBus::new();
    let run = Arc::new(RunContext {
        variables: serde_json::Map::new(),
        secrets: HashMap::new(),
        env: HashMap::new(),
        data: Default::default(),
        registry: Arc::new(MetricRegistry::with_builtins()),
        base_dir: ".".into(),
        setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
    });
    VuContext::new(1, Arc::from("test"), Arc::new(Tags::new()), bus, run, true)
}

#[tokio::test]
async fn output_plugin_writes_report_file() {
    let plugin = NativePlugin::load(&output_so()).expect("load output plugin");
    assert_eq!(plugin.info().name, "file-report");
    assert_eq!(plugin.info().kind, "output");

    let dir = tempfile::tempdir().expect("tempdir");
    let report = dir.path().join("report.txt");
    let mut output = plugin
        .make_output(serde_json::json!({"path": report}))
        .expect("make output");
    assert_eq!(output.name(), "file-report");

    // Build real snapshots/summary through the aggregator.
    let mut agg = Aggregator::new();
    agg.record(&sample("http_reqs", MetricKind::Counter, 1.0));
    agg.record(&sample("http_req_duration", MetricKind::Trend, 42.0));

    output.start().await.expect("start");
    let samples = [sample("http_reqs", MetricKind::Counter, 1.0)];
    output.on_samples(&samples).await;
    let snapshot = agg.snapshot();
    assert_eq!(snapshot.series.len(), 2);
    output.on_snapshot(&snapshot).await;
    let summary = Summary::build(
        Some("native-output-test".into()),
        "run-42".into(),
        now_millis(),
        vec!["default".into()],
        &mut agg,
        Vec::new(),
        None,
        Vec::new(),
    );
    output.finish(&summary).await;

    let text = std::fs::read_to_string(&report).expect("report written");
    assert!(
        text.contains("snapshot 1: series=2"),
        "snapshot line present:\n{text}"
    );
    assert!(
        text.contains("summary: run_id=run-42"),
        "summary line present:\n{text}"
    );
    assert!(text.contains("snapshots=1"), "{text}");
}

#[tokio::test]
async fn output_plugin_rejects_bad_config() {
    let plugin = NativePlugin::load(&output_so()).expect("load output plugin");
    let mut output = plugin
        .make_output(serde_json::json!({}))
        .expect("make output");
    let err = output.start().await.expect_err("missing path must fail");
    assert!(err.to_string().contains("path"), "{err}");
}

#[tokio::test]
async fn protocol_plugin_reverses_body_with_prefix() {
    let plugin = NativePlugin::load(&protocol_so()).expect("load protocol plugin");
    assert_eq!(plugin.info().kind, "protocol");
    let handler = plugin
        .make_protocol(serde_json::Value::Null)
        .expect("make protocol");
    assert_eq!(ProtocolHandler::name(&handler), "echo-proto");

    let mut vu = minimal_vu();
    let request = PreparedRequest {
        name: "echo".into(),
        protocol: "echo-proto".into(),
        method: "SEND".into(),
        url: "echo://local".into(),
        headers: vec![("x-test".into(), "1".into())],
        body: bytes::Bytes::from_static(b"abcdef"),
        timeout: Duration::from_secs(5),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions {
            plugin: Some(serde_json::json!({"prefix": "PFX:"})),
            ..Default::default()
        },
    };
    let response = handler.execute(&mut vu, &request).await.expect("execute");
    assert_eq!(response.status, 200);
    assert_eq!(response.status_text, "OK");
    assert_eq!(&response.body[..], b"PFX:fedcba");
    assert!(response.timings.duration_ms >= 0.0);
    assert_eq!(response.header("x-echo-proto"), Some("1"));
    assert_eq!(response.extras["prefix_applied"], true);
    assert!(response.error.is_none());
    assert_eq!(response.bytes_sent, 6);
    assert_eq!(response.bytes_received, 10);
}

#[tokio::test]
async fn protocol_plugin_config_prefix_fallback() {
    let plugin = NativePlugin::load(&protocol_so()).expect("load protocol plugin");
    let handler = plugin
        .make_protocol(serde_json::json!({"prefix": "CFG:"}))
        .expect("make protocol");
    let mut vu = minimal_vu();
    let request = PreparedRequest {
        name: "echo".into(),
        protocol: "echo-proto".into(),
        method: "SEND".into(),
        url: "echo://local".into(),
        headers: Vec::new(),
        body: bytes::Bytes::from_static(b"xyz"),
        timeout: Duration::from_secs(5),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions::default(),
    };
    let response = handler.execute(&mut vu, &request).await.expect("execute");
    assert_eq!(&response.body[..], b"CFG:zyx");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_plugin_concurrent_handlers_keep_configs_isolated() {
    let plugin = NativePlugin::load(&protocol_so()).expect("load protocol plugin");
    let left_handler = Arc::new(
        plugin
            .make_protocol(serde_json::json!({"prefix": "LEFT-CONFIG:"}))
            .expect("make left protocol"),
    );
    let right_handler = Arc::new(
        plugin
            .make_protocol(serde_json::json!({"prefix": "RIGHT-CONFIG:"}))
            .expect("make right protocol"),
    );

    let left_request = PreparedRequest {
        name: "left echo".into(),
        protocol: "echo-proto".into(),
        method: "SEND".into(),
        url: "echo://left".into(),
        headers: Vec::new(),
        body: bytes::Bytes::from_static(b"left-body"),
        timeout: Duration::from_secs(5),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions::default(),
    };
    let right_request = PreparedRequest {
        name: "right echo".into(),
        protocol: "echo-proto".into(),
        method: "SEND".into(),
        url: "echo://right".into(),
        headers: Vec::new(),
        body: bytes::Bytes::from_static(b"right-body"),
        timeout: Duration::from_secs(5),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions::default(),
    };

    let start = Arc::new(tokio::sync::Barrier::new(2));
    let run_calls = |handler: Arc<loadr_plugin_api::NativeProtocolAdapter>,
                     start: Arc<tokio::sync::Barrier>,
                     mut vu: VuContext,
                     request: PreparedRequest,
                     expected: &'static [u8]| async move {
        start.wait().await;
        for iteration in 0..256 {
            let response = handler
                .execute(&mut vu, &request)
                .await
                .expect("execute protocol");
            assert_eq!(
                &response.body[..],
                expected,
                "handler config mixed on iteration {iteration}"
            );
            tokio::task::yield_now().await;
        }
    };

    let left_task = tokio::spawn(run_calls(
        left_handler,
        Arc::clone(&start),
        minimal_vu(),
        left_request,
        b"LEFT-CONFIG:ydob-tfel",
    ));
    let right_task = tokio::spawn(run_calls(
        right_handler,
        start,
        minimal_vu(),
        right_request,
        b"RIGHT-CONFIG:ydob-thgir",
    ));

    let (left_result, right_result) = tokio::join!(left_task, right_task);
    left_result.expect("left handler task");
    right_result.expect("right handler task");
}

#[test]
fn kind_mismatch_constructors_error() {
    let plugin = NativePlugin::load(&output_so()).expect("load output plugin");
    let err = plugin
        .make_protocol(serde_json::Value::Null)
        .expect_err("output plugin has no protocol");
    assert!(
        matches!(err, loadr_plugin_api::PluginError::KindMismatch { .. }),
        "{err}"
    );
    let err = plugin.make_service().expect_err("no service either");
    assert!(
        matches!(err, loadr_plugin_api::PluginError::KindMismatch { .. }),
        "{err}"
    );
}

#[test]
fn missing_library_errors_cleanly() {
    let err = NativePlugin::load(std::path::Path::new("/nonexistent/libplugin.so"))
        .expect_err("missing file");
    assert!(
        matches!(err, loadr_plugin_api::PluginError::Load { .. }),
        "{err}"
    );
}

#[test]
fn data_source_plugin_loads_via_registry_with_capability() {
    let manifest = service_manifest(data_source_so(), serde_json::json!({"seed": 7}));
    let loaded =
        loadr_plugin_api::PluginRegistry::load_with_config(&manifest, &serde_json::Value::Null)
            .expect("load");
    match loaded {
        loadr_plugin_api::LoadedPlugin::Service {
            service,
            data_source,
        } => {
            assert!(service.is_none(), "tx-signer has no service lifecycle");
            assert!(data_source.is_some(), "tx-signer provides data_source");
        }
        other => panic!("expected Service variant, got {other:?}"),
    }
}

#[test]
fn service_plugin_without_data_source_capability_still_loads() {
    let manifest = service_manifest(hmac_signer_so(), serde_json::json!({"secret": "s3cr3t"}));
    let loaded =
        loadr_plugin_api::PluginRegistry::load_with_config(&manifest, &serde_json::Value::Null)
            .expect("load");
    match loaded {
        loadr_plugin_api::LoadedPlugin::Service {
            service,
            data_source,
        } => {
            assert!(
                service.is_some(),
                "hmac-signer provides a service lifecycle"
            );
            assert!(data_source.is_none(), "hmac-signer has no data_source");
        }
        other => panic!("expected Service variant, got {other:?}"),
    }
}

fn derive_test_key(seed: u64) -> ed25519_dalek::SigningKey {
    let seed_bytes = seed.to_le_bytes();
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = seed_bytes[i % seed_bytes.len()] ^ (i as u8);
    }
    ed25519_dalek::SigningKey::from_bytes(&key)
}

#[test]
fn data_source_adapter_signs_rows_and_signature_verifies() {
    let plugin = NativePlugin::load(&data_source_so()).expect("load data source plugin");
    assert_eq!(plugin.info().kind, "service");
    let mut adapter = plugin
        .make_data_source(serde_json::json!({"seed": 42}))
        .expect("plugin provides data_source capability");

    let mut sources = IndexMap::new();
    sources.insert(
        "signed_tx".to_string(),
        serde_json::json!({"chain_id": "testnet-1"}),
    );
    adapter.init(&sources).expect("init");

    let ctx = PluginRowCtx {
        source: "signed_tx",
        vu: 3,
        iteration: 0,
        seq: 5,
        scenario: "submit",
        request: Some("submit tx"),
        ts_ms: 1_700_000_000_000,
    };
    let row = match adapter.next_row(&ctx).expect("next_row") {
        PluginRowResult::Row(row) => row,
        PluginRowResult::Exhausted => panic!("unexpected exhaustion"),
    };
    assert_eq!(row.get("nonce").map(String::as_str), Some("3:5"));

    let tx_b64 = row.get("tx_b64").expect("tx_b64 present");
    let tx = base64::engine::general_purpose::STANDARD
        .decode(tx_b64)
        .expect("valid base64");
    assert!(
        tx.len() > 64,
        "tx must be the signed message plus a 64-byte signature"
    );
    let (message, signature_bytes) = tx.split_at(tx.len() - 64);
    assert!(
        message.starts_with(b"testnet-1"),
        "payload starts with chain_id bytes"
    );

    // The design doc's acceptance criterion: an independent verifier with
    // the plugin's public key confirms the signature over the payload.
    let verifying_key = derive_test_key(42).verifying_key();
    let signature = ed25519_dalek::Signature::from_bytes(
        signature_bytes.try_into().expect("signature is 64 bytes"),
    );
    verifying_key
        .verify(message, &signature)
        .expect("signature must verify");
}

#[test]
fn data_source_limit_reports_exhausted() {
    let plugin = NativePlugin::load(&data_source_so()).expect("load data source plugin");
    let mut adapter = plugin
        .make_data_source(serde_json::json!({"seed": 1}))
        .expect("data source");
    let mut sources = IndexMap::new();
    sources.insert(
        "signed_tx".to_string(),
        serde_json::json!({"chain_id": "t", "limit": 2}),
    );
    adapter.init(&sources).expect("init");

    let ctx = |seq: u64| PluginRowCtx {
        source: "signed_tx",
        vu: 1,
        iteration: 0,
        seq,
        scenario: "s",
        request: None,
        ts_ms: 0,
    };
    assert!(matches!(
        adapter.next_row(&ctx(0)).expect("row 1"),
        PluginRowResult::Row(_)
    ));
    assert!(matches!(
        adapter.next_row(&ctx(1)).expect("row 2"),
        PluginRowResult::Row(_)
    ));
    assert!(matches!(
        adapter.next_row(&ctx(2)).expect("row 3 is exhausted"),
        PluginRowResult::Exhausted
    ));
}

#[test]
fn data_source_invalid_config_errors_at_init() {
    let plugin = NativePlugin::load(&data_source_so()).expect("load data source plugin");
    let mut adapter = plugin
        .make_data_source(serde_json::Value::Null)
        .expect("data source");
    let sources: IndexMap<String, serde_json::Value> = IndexMap::new();
    let err = adapter
        .init(&sources)
        .expect_err("missing key_hex/seed must fail");
    assert!(
        err.contains("key_hex") || err.contains("seed"),
        "error should mention the missing config: {err}"
    );
}

#[test]
fn data_source_concurrent_next_row_yields_unique_nonces() {
    let plugin = NativePlugin::load(&data_source_so()).expect("load data source plugin");
    let mut adapter = plugin
        .make_data_source(serde_json::json!({"seed": 9}))
        .expect("data source");
    let mut sources = IndexMap::new();
    sources.insert(
        "signed_tx".to_string(),
        serde_json::json!({"chain_id": "c"}),
    );
    adapter.init(&sources).expect("init");
    let adapter = Arc::new(adapter);

    let handles: Vec<_> = (0..8u64)
        .map(|vu| {
            let adapter = Arc::clone(&adapter);
            std::thread::spawn(move || {
                (0..20u64)
                    .map(|seq| {
                        let ctx = PluginRowCtx {
                            source: "signed_tx",
                            vu,
                            iteration: 0,
                            seq,
                            scenario: "s",
                            request: None,
                            ts_ms: 0,
                        };
                        match adapter.next_row(&ctx).expect("row") {
                            PluginRowResult::Row(row) => row["nonce"].clone(),
                            PluginRowResult::Exhausted => panic!("unexpected exhaustion"),
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    let mut nonces = std::collections::BTreeSet::new();
    for h in handles {
        for nonce in h.join().expect("worker thread panicked") {
            assert!(nonces.insert(nonce), "duplicate nonce across threads");
        }
    }
    assert_eq!(nonces.len(), 8 * 20);
}

/// Perf smoke, not a correctness check: hammer the adapter single-threaded
/// for ~1s and print rows/s. No assertion -- wall-clock throughput is
/// inherently noisy in CI; run with `--ignored` and eyeball the number.
#[test]
#[ignore]
fn data_source_adapter_perf_smoke() {
    let plugin = NativePlugin::load(&data_source_so()).expect("load data source plugin");
    let mut adapter = plugin
        .make_data_source(serde_json::json!({"seed": 5}))
        .expect("data source");
    let mut sources = IndexMap::new();
    sources.insert(
        "signed_tx".to_string(),
        serde_json::json!({"chain_id": "perf"}),
    );
    adapter.init(&sources).expect("init");

    let start = std::time::Instant::now();
    let mut seq = 0u64;
    let mut rows = 0u64;
    while start.elapsed() < Duration::from_secs(1) {
        let ctx = PluginRowCtx {
            source: "signed_tx",
            vu: 1,
            iteration: 0,
            seq,
            scenario: "s",
            request: None,
            ts_ms: 0,
        };
        match adapter.next_row(&ctx).expect("row") {
            PluginRowResult::Row(_) => {}
            PluginRowResult::Exhausted => break,
        }
        seq += 1;
        rows += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "data_source_adapter_perf_smoke: {rows} rows in {elapsed:.3}s = {:.0} rows/s",
        rows as f64 / elapsed
    );
}
