use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
use loadr_core::vu::RunContext;
use loadr_core::{PreparedRequest, ProtocolHandler, RequestOptions, VuContext};
use loadr_plugin_api::NativePlugin;

fn minimal_vu(id: u64) -> VuContext {
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
    VuContext::new(
        id,
        Arc::from("bench"),
        Arc::new(Tags::new()),
        bus,
        run,
        true,
    )
}

fn request() -> PreparedRequest {
    PreparedRequest {
        name: "echo".into(),
        protocol: "echo-proto".into(),
        method: "SEND".into(),
        url: "echo://local".into(),
        headers: vec![("x-bench".into(), "1".into())],
        body: Bytes::from_static(b"ping"),
        timeout: Duration::from_secs(5),
        follow_redirects: false,
        max_redirects: 0,
        options: RequestOptions::default(),
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index] as f64 / 1_000.0
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: {} PLUGIN_PATH VUS DURATION_SECONDS", args[0]);
        std::process::exit(2);
    }
    let plugin_path = &args[1];
    let vus: usize = args[2].parse().expect("VUS must be an integer");
    let duration = Duration::from_secs_f64(
        args[3]
            .parse::<f64>()
            .expect("DURATION_SECONDS must be numeric"),
    );
    assert!(vus > 0);
    assert!(!duration.is_zero());

    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build Tokio runtime");

    runtime.block_on(async {
        let plugin = NativePlugin::load(Path::new(plugin_path)).expect("load native plugin");
        let handler = Arc::new(
            plugin
                .make_protocol(serde_json::Value::Null)
                .expect("construct native protocol"),
        );

        let mut warm_vu = minimal_vu(0);
        let warm_request = request();
        for _ in 0..2_000 {
            let response = handler
                .execute(&mut warm_vu, &warm_request)
                .await
                .expect("warm-up request");
            assert_eq!(response.status, 200);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(vus + 1));
        let start_at = Instant::now() + Duration::from_millis(100);
        let deadline = start_at + duration;
        let mut handles = Vec::with_capacity(vus);
        for id in 1..=vus {
            let handler = Arc::clone(&handler);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let mut vu = minimal_vu(id as u64);
                let request = request();
                barrier.wait().await;
                tokio::time::sleep_until(tokio::time::Instant::from_std(start_at)).await;
                let mut samples = Vec::new();
                while Instant::now() < deadline {
                    let started = Instant::now();
                    let response = handler
                        .execute(&mut vu, &request)
                        .await
                        .expect("benchmark request");
                    assert_eq!(response.status, 200);
                    samples.push(started.elapsed().as_nanos() as u64);
                }
                samples
            }));
        }
        barrier.wait().await;

        let mut samples = Vec::new();
        for handle in handles {
            samples.extend(handle.await.expect("benchmark VU task"));
        }
        let elapsed = start_at.elapsed();
        samples.sort_unstable();
        let calls = samples.len();
        let total_ns: u128 = samples.iter().map(|&value| value as u128).sum();
        let average_us = total_ns as f64 / calls as f64 / 1_000.0;
        println!(
            "vus={vus} workers={workers} calls={calls} elapsed_s={:.6} rps={:.3} avg_us={average_us:.3} p50_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3}",
            elapsed.as_secs_f64(),
            calls as f64 / elapsed.as_secs_f64(),
            percentile(&samples, 0.50),
            percentile(&samples, 0.95),
            percentile(&samples, 0.99),
            samples.last().copied().unwrap_or_default() as f64 / 1_000.0,
        );
    });
}
