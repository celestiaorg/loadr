//! A `browser` load-testing protocol for loadr, driving a real headless Chrome
//! via the Chrome DevTools Protocol (CDP) with [`chromiumoxide`].
//!
//! Each [`BrowserHandler`] owns a single shared, lazily-launched [`Browser`]
//! process. Every virtual user (VU) gets its own [`Page`] (browser tab), kept
//! alive across requests so navigation reuses the same tab (warm caches, real
//! browsing session). On [`execute`](ProtocolHandler::execute) the handler
//! navigates the VU's page to the requested URL, then evaluates JavaScript in
//! the page to collect **Navigation Timing** (mapped onto [`Timings`]) and
//! **Web Vitals** (FCP, LCP, DCL, load, resource count, transferred bytes),
//! which are returned in [`ProtocolResponse::extras`].
//!
//! Transport-level failures (DNS, connection refused, navigation aborts) are
//! reported via [`ProtocolResponse::error`] with `status = 0` rather than as an
//! `Err`, so the run still records a (failed) sample.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::page::Page;
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, warn};

use loadr_config::HttpDefaults;
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{
    PreparedRequest, ProtocolHandler, ProtocolRegistry, ProtocolResponse, Timings,
};
use loadr_core::vu::VuContext;

/// Path to the Chrome executable used to launch the headless browser.
const CHROME_EXECUTABLE: &str = "/usr/bin/google-chrome";

/// Default navigation timeout when a request specifies none.
const DEFAULT_NAV_TIMEOUT: Duration = Duration::from_secs(30);

/// Extra settle time after `load` to let async Web Vitals (LCP) flush.
const VITALS_SETTLE: Duration = Duration::from_millis(150);

/// JavaScript injected before navigation to capture the Largest Contentful
/// Paint via a `PerformanceObserver` (LCP is reported asynchronously and is
/// otherwise unavailable through the timing entries).
const LCP_OBSERVER_JS: &str = r#"
(() => {
  try {
    if (window.__loadrLcp === undefined) {
      window.__loadrLcp = null;
      const obs = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length) {
          window.__loadrLcp = entries[entries.length - 1].startTime;
        }
      });
      obs.observe({ type: 'largest-contentful-paint', buffered: true });
    }
  } catch (e) { /* LCP unsupported: leave null */ }
  return true;
})()
"#;

/// JavaScript evaluated after navigation to collect Navigation Timing and Web
/// Vitals. Returns a JSON string (parsed into [`PageMetrics`]).
const METRICS_JS: &str = r#"
(() => {
  const nav = performance.getEntriesByType('navigation')[0] || {};
  const paint = performance.getEntriesByType('paint');
  const fcpEntry = paint.find(p => p.name === 'first-contentful-paint');
  const fcp = fcpEntry ? fcpEntry.startTime : null;
  const res = performance.getEntriesByType('resource');
  const transferred = res.reduce((a, r) => a + (r.transferSize || 0), 0) + (nav.transferSize || 0);
  let lcp = (typeof window.__loadrLcp === 'number') ? window.__loadrLcp : null;
  if (lcp === null) {
    try {
      const lcps = performance.getEntriesByType('largest-contentful-paint');
      if (lcps.length) lcp = lcps[lcps.length - 1].startTime;
    } catch (e) { /* unsupported */ }
  }
  return JSON.stringify({
    dns: (nav.domainLookupEnd || 0) - (nav.domainLookupStart || 0),
    connect: (nav.connectEnd || 0) - (nav.connectStart || 0),
    tls: nav.secureConnectionStart ? ((nav.connectEnd || 0) - nav.secureConnectionStart) : 0,
    request_start: nav.requestStart || 0,
    response_start: nav.responseStart || 0,
    response_end: nav.responseEnd || 0,
    ttfb: (nav.responseStart || 0) - (nav.requestStart || 0),
    receiving: (nav.responseEnd || 0) - (nav.responseStart || 0),
    duration: nav.duration || nav.loadEventEnd || 0,
    dcl: nav.domContentLoadedEventEnd || 0,
    load: nav.loadEventEnd || 0,
    fcp: fcp,
    lcp: lcp,
    resources: res.length,
    transferred: transferred,
    title: document.title || ''
  });
})()
"#;

/// Raw Navigation Timing + Web Vitals harvested from the page.
#[derive(Debug, Clone, Deserialize)]
struct PageMetrics {
    dns: f64,
    connect: f64,
    tls: f64,
    ttfb: f64,
    receiving: f64,
    duration: f64,
    dcl: f64,
    load: f64,
    fcp: Option<f64>,
    lcp: Option<f64>,
    resources: u64,
    transferred: f64,
    title: String,
}

impl PageMetrics {
    /// Map Navigation Timing phases onto loadr's [`Timings`].
    fn to_timings(&self, wall_ms: f64) -> Timings {
        let dns = non_neg(self.dns);
        let connect = non_neg(self.connect);
        let tls = non_neg(self.tls);
        // The TCP `connect` window includes the TLS handshake; subtract it so
        // the connect phase reflects only the transport handshake.
        let connect_only = non_neg(connect - tls);
        let waiting = non_neg(self.ttfb);
        let receiving = non_neg(self.receiving);
        let mut duration = non_neg(self.duration);
        if duration == 0.0 {
            duration = non_neg(self.load);
        }
        if duration == 0.0 {
            duration = wall_ms;
        }
        Timings {
            dns_ms: dns,
            connect_ms: connect_only,
            tls_ms: tls,
            sending_ms: 0.0,
            waiting_ms: waiting,
            receiving_ms: receiving,
            duration_ms: duration,
            blocked_ms: dns + connect_only + tls,
        }
    }
}

/// Coerce non-finite / negative timings to zero (Chrome reports `0` for phases
/// that did not occur, e.g. a reused connection has no DNS).
fn non_neg(v: f64) -> f64 {
    if v.is_finite() && v > 0.0 {
        v
    } else {
        0.0
    }
}

/// The `browser` protocol handler.
///
/// Holds the lazily-launched shared browser and a per-VU page table. Cheap to
/// clone-share behind an `Arc` (as the registry stores it).
pub struct BrowserHandler {
    /// The shared Chrome process, launched on first use.
    browser: OnceCell<Browser>,
    /// One open tab per VU id, reused across `execute` calls.
    pages: Mutex<HashMap<u64, Page>>,
    /// Navigation timeout fallback (from HTTP defaults) when a request gives none.
    default_timeout: Duration,
}

impl Default for BrowserHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserHandler {
    /// Create a handler with the default navigation timeout.
    pub fn new() -> Self {
        BrowserHandler {
            browser: OnceCell::new(),
            pages: Mutex::new(HashMap::new()),
            default_timeout: DEFAULT_NAV_TIMEOUT,
        }
    }

    /// Create a handler, taking the navigation timeout from HTTP defaults.
    ///
    /// Only the `timeout` is consulted; other HTTP options do not apply to the
    /// browser protocol. Kept fallible/with this signature so the CLI can build
    /// every protocol uniformly.
    pub fn from_config(config: &HttpDefaults) -> Result<Self, ProtocolError> {
        let timeout = config.timeout.as_duration();
        let default_timeout = if timeout.is_zero() {
            DEFAULT_NAV_TIMEOUT
        } else {
            timeout
        };
        Ok(BrowserHandler {
            browser: OnceCell::new(),
            pages: Mutex::new(HashMap::new()),
            default_timeout,
        })
    }

    /// Get (launching on first call) the shared headless Chrome browser.
    async fn browser(&self) -> Result<&Browser, ProtocolError> {
        self.browser.get_or_try_init(launch_browser).await
    }

    /// Get or open the page (tab) dedicated to `vu_id`, reusing it if present.
    async fn page_for_vu(&self, vu_id: u64) -> Result<Page, ProtocolError> {
        {
            let pages = self.pages.lock().await;
            if let Some(page) = pages.get(&vu_id) {
                return Ok(page.clone());
            }
        }
        let browser = self.browser().await?;
        let page = browser
            .new_page("about:blank")
            .await
            .map_err(|e| ProtocolError::Connect(format!("failed to open browser page: {e}")))?;
        let mut pages = self.pages.lock().await;
        // Another task may have raced us; keep whichever is already stored.
        let entry = pages.entry(vu_id).or_insert(page);
        Ok(entry.clone())
    }
}

/// Build a process-unique user-data directory under the system temp dir.
///
/// chromiumoxide otherwise shares a single profile directory across launches,
/// whose `SingletonLock` makes concurrent Chrome processes (multiple handlers /
/// parallel test binaries) abort. A unique dir per launch isolates them.
fn unique_user_data_dir() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "loadr-browser-{}-{}-{}",
        std::process::id(),
        seq,
        nanos
    ))
}

/// Launch the shared headless Chrome process and spawn its CDP event pump.
async fn launch_browser() -> Result<Browser, ProtocolError> {
    let config = BrowserConfig::builder()
        .chrome_executable(CHROME_EXECUTABLE)
        .user_data_dir(unique_user_data_dir())
        .arg("--no-sandbox")
        .arg("--headless=new")
        .arg("--disable-gpu")
        .arg("--disable-dev-shm-usage")
        .build()
        .map_err(ProtocolError::Connect)?;

    let (browser, mut handler) = Browser::launch(config)
        .await
        .map_err(|e| ProtocolError::Connect(format!("failed to launch chrome: {e}")))?;

    // The CDP handler stream MUST be polled for the browser to make progress.
    // Detach it onto the runtime; it ends when the browser is dropped/closed.
    tokio::spawn(async move {
        while let Some(event) = handler.next().await {
            if let Err(e) = event {
                debug!(error = %e, "chromiumoxide handler event error");
            }
        }
        debug!("chromiumoxide handler stream ended");
    });

    debug!("launched shared headless chrome");
    Ok(browser)
}

#[async_trait]
impl ProtocolHandler for BrowserHandler {
    fn name(&self) -> &str {
        "browser"
    }

    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        if request.url.trim().is_empty() {
            return Err(ProtocolError::InvalidRequest(
                "browser request requires a url".to_string(),
            ));
        }

        let timeout = if request.timeout.is_zero() {
            self.default_timeout
        } else {
            request.timeout
        };

        let page = self.page_for_vu(ctx.vu_id).await?;
        let url = request.url.clone();

        match tokio::time::timeout(timeout, navigate_and_measure(&page, &url)).await {
            Ok(Ok(outcome)) => Ok(outcome.into_response(url)),
            Ok(Err(err)) => {
                warn!(url = %url, error = %err, "browser navigation failed");
                Ok(error_response(url, &err))
            }
            Err(_elapsed) => {
                warn!(url = %url, ?timeout, "browser navigation timed out");
                Err(ProtocolError::Timeout(timeout))
            }
        }
    }
}

/// The result of one successful navigation + measurement pass.
struct NavOutcome {
    status: i64,
    status_text: String,
    headers: Vec<(String, String)>,
    metrics: PageMetrics,
    body: Bytes,
    wall_ms: f64,
}

impl NavOutcome {
    fn into_response(self, url: String) -> ProtocolResponse {
        let timings = self.metrics.to_timings(self.wall_ms);
        let bytes_received = self.metrics.transferred.max(0.0) as u64;
        let extras = serde_json::json!({
            "fcp_ms": self.metrics.fcp,
            "lcp_ms": self.metrics.lcp,
            "dcl_ms": self.metrics.dcl,
            "load_ms": self.metrics.load,
            "resources": self.metrics.resources,
            "transferred_bytes": self.metrics.transferred,
            "title": self.metrics.title,
        });
        ProtocolResponse {
            status: self.status,
            status_text: self.status_text,
            headers: self.headers,
            body: self.body,
            timings,
            bytes_sent: 0,
            bytes_received,
            protocol_version: "browser".to_string(),
            error: None,
            url,
            extras,
            grpc_protobuf_outcomes: Vec::new(),
        }
    }
}

/// Navigate the page and harvest timings + vitals. Errors here are transport
/// failures (the page exists but the navigation/eval failed).
async fn navigate_and_measure(page: &Page, url: &str) -> Result<NavOutcome, String> {
    // Arm the LCP observer before navigation so it captures the paint.
    if let Err(e) = page.evaluate(LCP_OBSERVER_JS).await {
        debug!(error = %e, "failed to arm LCP observer (continuing)");
    }

    let started = Instant::now();
    page.goto(url).await.map_err(|e| e.to_string())?;

    // The navigation response carries the real HTTP status / headers.
    let (status, status_text, headers) = match page.wait_for_navigation_response().await {
        Ok(Some(req)) => response_meta(&req),
        Ok(None) => (200, "OK".to_string(), Vec::new()),
        Err(e) => return Err(e.to_string()),
    };

    page.wait_for_navigation()
        .await
        .map_err(|e| e.to_string())?;

    // Give async vitals (LCP) a brief moment to flush.
    tokio::time::sleep(VITALS_SETTLE).await;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;

    let metrics = collect_metrics(page).await?;
    let body = page
        .content()
        .await
        .map(|html| Bytes::from(html.into_bytes()))
        .unwrap_or_default();

    Ok(NavOutcome {
        status,
        status_text,
        headers,
        metrics,
        body,
        wall_ms,
    })
}

/// Extract status, status text and headers from the navigation response.
fn response_meta(
    req: &chromiumoxide::handler::http::HttpRequest,
) -> (i64, String, Vec<(String, String)>) {
    match &req.response {
        Some(resp) => {
            let status = resp.status;
            let status_text = if resp.status_text.is_empty() {
                status_phrase(status)
            } else {
                resp.status_text.clone()
            };
            let headers = resp
                .headers
                .inner()
                .as_object()
                .map(|map| {
                    map.iter()
                        .map(|(k, v)| {
                            let val = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            (k.clone(), val)
                        })
                        .collect()
                })
                .unwrap_or_default();
            (status, status_text, headers)
        }
        None => (200, "OK".to_string(), Vec::new()),
    }
}

/// Evaluate the metrics JS and parse the result.
async fn collect_metrics(page: &Page) -> Result<PageMetrics, String> {
    let result = page.evaluate(METRICS_JS).await.map_err(|e| e.to_string())?;
    let json = result
        .into_value::<String>()
        .map_err(|e| format!("metrics evaluation returned non-string: {e}"))?;
    serde_json::from_str::<PageMetrics>(&json).map_err(|e| format!("failed to parse metrics: {e}"))
}

/// Build a failed-but-sampled response for a navigation error.
fn error_response(url: String, err: &str) -> ProtocolResponse {
    ProtocolResponse {
        status: 0,
        status_text: String::new(),
        headers: Vec::new(),
        body: Bytes::new(),
        timings: Timings::default(),
        bytes_sent: 0,
        bytes_received: 0,
        protocol_version: "browser".to_string(),
        error: Some(err.to_string()),
        url,
        extras: serde_json::json!({}),
        grpc_protobuf_outcomes: Vec::new(),
    }
}

/// Best-effort reason phrase for common HTTP status codes.
fn status_phrase(status: i64) -> String {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "",
    }
    .to_string()
}

/// Register the `browser` protocol handler in `registry`.
pub fn register(registry: &mut ProtocolRegistry) {
    registry.register(Arc::new(BrowserHandler::new()));
}

/// Construct a `browser` handler as a boxed [`ProtocolHandler`] for the CLI.
pub fn try_new_handler() -> Result<Arc<dyn ProtocolHandler>, ProtocolError> {
    Ok(Arc::new(BrowserHandler::new()))
}
