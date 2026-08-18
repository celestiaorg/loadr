//! Serves the real embedded web UI with a run that deliberately produces a mix
//! of failures, so the failure & error breakdown panel (and its CSV / report
//! download) can be demoed and recorded without external services.
//!
//! It drives a real [`loadr_core::Engine`] through [`LocalBackend`] — exactly
//! like the CLI's standalone mode — wired to a mock HTTP handler that returns a
//! spread of statuses and transport errors, plus a couple of scripted
//! exceptions. The YAML adds a failing check.
//!
//! ```text
//! cargo run -p loadr-plugin-webui --example failure_demo -- 127.0.0.1:6471
//! ```
//!
//! Then open the printed URL (default `http://127.0.0.1:6471/`) and scroll to
//! the "Failure breakdown" panel. Note the NON-default port so it can coexist
//! with a normal UI on 6464.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use loadr_core::metrics::MetricKind;
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, ProtocolRegistry, Timings};
use loadr_core::vu::VuContext;
use loadr_core::{Engine, EngineOptions, ProtocolError, ProtocolResponse};
use loadr_plugin_webui::{AuthConfig, EngineLauncher, LocalBackend, UiBackend, WebUi, WebUiConfig};

/// Mock HTTP handler producing a realistic spread of outcomes by request name:
/// healthy 200s, 500/503 server errors, 404s, periodic transport timeouts, and
/// occasional scripted exceptions (emitted as the `vu_exceptions` counter).
#[derive(Default)]
struct FlakyHttp {
    seq: AtomicU64,
}

#[async_trait::async_trait]
impl ProtocolHandler for FlakyHttp {
    fn name(&self) -> &str {
        "http"
    }

    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let ms = 1 + n % 4;
        tokio::time::sleep(Duration::from_millis(ms)).await;

        // ~1 in 12 requests raises a (scripted) exception, grouped by message.
        if n.is_multiple_of(12) {
            let tags = ctx.sample_tags(&[
                (
                    "exception",
                    "TypeError: cannot read properties of undefined",
                ),
                ("site", "exec"),
            ]);
            let metric: Arc<str> = Arc::from("vu_exceptions");
            ctx.metrics
                .emit_value(&metric, MetricKind::Counter, 1.0, &tags);
        }

        // ~1 in 9 requests is a transport timeout (no response status).
        if n % 9 == 4 {
            return Ok(ProtocolResponse {
                protocol_version: "HTTP/1.1".to_string(),
                error: Some("read timed out after 10s".to_string()),
                url: request.url.clone(),
                timings: Timings {
                    duration_ms: ms as f64,
                    ..Default::default()
                },
                ..Default::default()
            });
        }

        // Otherwise the status is chosen by the request name (see the YAML).
        let status = match request.name.as_str() {
            "server-error" => 500,
            "unavailable" => 503,
            "not-found" => 404,
            _ => 200,
        };
        Ok(ProtocolResponse {
            status,
            status_text: "".to_string(),
            protocol_version: "HTTP/1.1".to_string(),
            timings: Timings {
                waiting_ms: ms as f64,
                duration_ms: ms as f64,
                ..Default::default()
            },
            bytes_sent: 120,
            bytes_received: 256,
            url: request.url.clone(),
            ..Default::default()
        })
    }
}

const PLAN: &str = r#"
name: failure-breakdown-demo
scenarios:
  mixed:
    executor: constant-vus
    vus: 8
    duration: 600s
    flow:
      - request: { name: ok,           method: GET, url: http://demo.local/status/200,
                   checks: [ { type: status, equals: 200 } ] }
      - request: { name: server-error, method: GET, url: http://demo.local/status/500 }
      - request: { name: not-found,    method: GET, url: http://demo.local/status/404,
                   checks: [ { type: status, name: resource exists, equals: 200 } ] }
      - request: { name: unavailable,  method: GET, url: http://demo.local/status/503 }
"#;

fn flaky_launcher() -> EngineLauncher {
    Arc::new(|plan, base_dir, run_id| {
        let mut protocols = ProtocolRegistry::new();
        protocols.register(Arc::new(FlakyHttp::default()));
        let opts = EngineOptions {
            run_id: Some(run_id),
            protocols,
            ..Default::default()
        };
        let engine = Engine::new(plan, base_dir, opts).map_err(|e| e.to_string())?;
        let handle = engine.handle();
        let task = tokio::spawn(engine.run());
        Ok((handle, task))
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("warn").init();

    let bind: std::net::SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6471".to_string())
        .parse()?;

    let dir = std::env::temp_dir().join("loadr-failure-demo");
    std::fs::create_dir_all(&dir)?;
    let backend = Arc::new(LocalBackend::new(dir, flaky_launcher())?);

    let run_id = backend
        .start_test(
            Some("failure-breakdown".to_string()),
            PLAN.to_string(),
            None,
        )
        .await
        .map_err(|e| format!("start: {e}"))?;

    let handle = WebUi::serve(WebUiConfig {
        bind,
        auth: AuthConfig::default(),
        backend,
    })
    .await?;

    println!(
        "failure-breakdown demo UI: http://{}/ (run {run_id})",
        handle.addr
    );
    println!("scroll to the 'Failure breakdown' panel; Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;
    handle.shutdown().await;
    Ok(())
}
