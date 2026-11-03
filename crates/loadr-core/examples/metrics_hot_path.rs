#[cfg(debug_assertions)]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loadr_core::{
    Engine, EngineOptions, PreparedRequest, ProtocolError, ProtocolHandler, ProtocolRegistry,
    ProtocolResponse, VuContext,
};

#[cfg(debug_assertions)]
struct CountingAllocator;

#[cfg(debug_assertions)]
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

#[cfg(debug_assertions)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[cfg(debug_assertions)]
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct EmptyGrpc;

#[async_trait]
impl ProtocolHandler for EmptyGrpc {
    fn name(&self) -> &str {
        "grpc"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        Ok(ProtocolResponse {
            status: 0,
            protocol_version: "grpc".to_string(),
            url: request.url.clone(),
            bytes_sent: 241,
            ..Default::default()
        })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let iterations = std::env::args()
        .nth(1)
        .map(|arg| arg.parse().expect("iterations must be an integer"))
        .unwrap_or(2_000_000u64);
    let vus = std::env::args()
        .nth(2)
        .map(|arg| arg.parse().expect("VUs must be an integer"))
        .unwrap_or(500u64);
    let plan = format!(
        r#"
scenarios:
  bench:
    executor: shared-iterations
    vus: {vus}
    iterations: {iterations}
    flow:
      - request:
          url: grpc://mock/Submit
          checks:
            - {{ type: status, name: status_a, equals: 0 }}
            - {{ type: status, name: status_b, equals: 0 }}
"#
    );
    let loaded = loadr_config::load_str(&plan, &loadr_config::LoadOptions::new()).expect("plan");
    let mut protocols = ProtocolRegistry::new();
    protocols.register(Arc::new(EmptyGrpc));
    let engine = Engine::new(
        loaded.plan,
        ".".into(),
        EngineOptions {
            protocols,
            snapshot_interval: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .expect("engine");

    #[cfg(debug_assertions)]
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let result = engine.run().await.expect("run");
    let elapsed = started.elapsed();
    let completed = result
        .summary
        .metrics
        .iter()
        .find(|metric| metric.metric == "iterations")
        .expect("iterations metric")
        .agg
        .sum as u64;
    println!(
        "iterations={completed} elapsed_s={:.6} ns_per_request={:.1}",
        elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / completed as f64,
    );
    #[cfg(debug_assertions)]
    println!(
        "allocations={} allocations_per_request={:.3}",
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATIONS.load(Ordering::Relaxed) as f64 / completed as f64,
    );
}
