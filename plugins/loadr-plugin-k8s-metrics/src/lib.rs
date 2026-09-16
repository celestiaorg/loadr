//! `loadr-plugin-k8s-metrics` — a native **service** plugin that polls the
//! Kubernetes **`metrics.k8s.io`** aggregated API during a run and emits
//! system-metric samples aligned to loadr's run timeline, so container resource
//! usage overlays the load metrics on one chart.
//!
//! # Why a service plugin
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> bound_addr` and an idempotent `stop()`. On `start`
//! this plugin:
//!
//! 1. resolves its bearer token (config `token`, else the mounted
//!    service-account token) and the cluster CA (config `ca_cert`, else the
//!    mounted service-account CA), and builds a pure-Rust **hyper + rustls**
//!    HTTPS client — no `kubectl`, no Kubernetes client SDK, no C dependency;
//! 2. spawns a background poller that scrapes
//!    `GET /apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods` on `interval_ms`,
//!    reads each pod's summed container CPU/memory usage, and appends a point to
//!    a per-pod system-metric series
//!    (`k8s_pod_cpu_cores`, `k8s_pod_mem_bytes`, `k8s_scrapes`);
//! 3. binds a tiny local endpoint (`127.0.0.1:0` by default) that serves the
//!    accumulated series as JSON and returns its bound address.
//!
//! # Failure isolation
//!
//! Like every collector, it **never fails the load test**: a slow scrape, an
//! unreachable API server or a garbage response is counted (`failures`) and
//! leaves a gap in the series — it is not fatal. Only a bad *config* (unparseable
//! JSON, an unreadable `ca_cert`, an unbindable endpoint) fails `start()`.
//!
//! # Transport
//!
//! The metrics API is plain HTTPS/JSON, so the poller GETs it directly over the
//! project's existing hyper + hyper-rustls stack. Because the API server
//! presents the *cluster* CA in-cluster (not a public root), the client's rustls
//! root store is built from the mounted `ca_cert`; `insecure` installs a verifier
//! that accepts any certificate for off-cluster testing.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{ACCEPT, AUTHORIZATION};
use hyper::Request;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "k8s-metrics";

/// In-cluster Kubernetes API server address (used when `api_url` is unset).
const IN_CLUSTER_API: &str = "https://kubernetes.default.svc";
/// Projected service-account token mounted into every in-cluster pod.
const SA_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// Mounted service-account cluster CA bundle.
const SA_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Pod CPU usage in cores, tagged `{ namespace, pod }`.
const CPU_METRIC: &str = "k8s_pod_cpu_cores";
/// Pod working-set memory in bytes, tagged `{ namespace, pod }`.
const MEM_METRIC: &str = "k8s_pod_mem_bytes";
/// Cumulative count of successful scrapes, tagged `{ namespace }`.
const SCRAPES_METRIC: &str = "k8s_scrapes";

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build k8s-metrics plugin tokio runtime")
    })
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Kubernetes quantity parsing.
//
// `resource.Quantity` values arrive as strings with an SI (decimal) or binary
// suffix — CPU as e.g. "250m" / "123456n" / "1", memory as "128974848" /
// "132Mi" / "2Gi". Both normalize to a plain `f64` in the metric's base unit
// (CPU cores, memory bytes).
// ---------------------------------------------------------------------------

/// Parse a Kubernetes quantity string into its base-unit `f64` value, or `None`
/// if it is malformed. CPU quantities resolve to cores, memory to bytes.
fn parse_quantity(raw: &str) -> Option<f64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    // Binary (power-of-two) suffixes: Ki, Mi, Gi, Ti, Pi, Ei.
    const BINARY: &[(&str, f64)] = &[
        ("Ki", 1024.0),
        ("Mi", 1_048_576.0),
        ("Gi", 1_073_741_824.0),
        ("Ti", 1_099_511_627_776.0),
        ("Pi", 1_125_899_906_842_624.0),
        ("Ei", 1_152_921_504_606_846_976.0),
    ];
    for &(suffix, factor) in BINARY {
        if let Some(num) = s.strip_suffix(suffix) {
            return num.trim().parse::<f64>().ok().map(|n| n * factor);
        }
    }
    // Decimal (SI) single-char suffixes: n, u, m (sub-unit) and k..E (multiples).
    const DECIMAL: &[(char, f64)] = &[
        ('n', 1e-9),
        ('u', 1e-6),
        ('m', 1e-3),
        ('k', 1e3),
        ('M', 1e6),
        ('G', 1e9),
        ('T', 1e12),
        ('P', 1e15),
        ('E', 1e18),
    ];
    for &(suffix, factor) in DECIMAL {
        if s.ends_with(suffix) {
            let num = &s[..s.len() - suffix.len_utf8()];
            return num.trim().parse::<f64>().ok().map(|n| n * factor);
        }
    }
    // No suffix (plain integer/float, incl. exponent form like "1e3").
    s.parse::<f64>().ok()
}

// ---------------------------------------------------------------------------
// metrics.k8s.io request + response.
// ---------------------------------------------------------------------------

/// Percent-encode a label selector for use in the `labelSelector` query param.
fn encode_selector(selector: &str) -> String {
    let mut out = String::with_capacity(selector.len());
    for b in selector.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build the pod-metrics endpoint URL for a namespace + optional selector.
fn metrics_url(api_url: &str, namespace: &str, selector: Option<&str>) -> String {
    let base = api_url.trim_end_matches('/');
    let mut url = format!("{base}/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods");
    if let Some(sel) = selector.filter(|s| !s.is_empty()) {
        url.push_str("?labelSelector=");
        url.push_str(&encode_selector(sel));
    }
    url
}

/// One pod's summed resource usage from a metrics-API scrape.
#[derive(Debug, Clone, PartialEq)]
struct PodUsage {
    namespace: String,
    pod: String,
    cpu_cores: f64,
    mem_bytes: f64,
}

/// Parse a `PodMetricsList` body into per-pod usage. Container usages are summed
/// per pod. Returns `Err` only when the response is not a metrics list.
fn parse_pod_metrics(body: &str) -> Result<Vec<PodUsage>, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid metrics JSON: {e}"))?;
    let items = value
        .get("items")
        .and_then(|i| i.as_array())
        .ok_or_else(|| "metrics response has no `items` array".to_string())?;

    let mut out = Vec::new();
    for item in items {
        let pod = item
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();
        if pod.is_empty() {
            continue;
        }
        let namespace = item
            .pointer("/metadata/namespace")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();

        let mut cpu_cores = 0.0;
        let mut mem_bytes = 0.0;
        if let Some(containers) = item.get("containers").and_then(|c| c.as_array()) {
            for c in containers {
                if let Some(cpu) = c.pointer("/usage/cpu").and_then(|q| q.as_str()) {
                    if let Some(cores) = parse_quantity(cpu) {
                        cpu_cores += cores;
                    }
                }
                if let Some(mem) = c.pointer("/usage/memory").and_then(|q| q.as_str()) {
                    if let Some(bytes) = parse_quantity(mem) {
                        mem_bytes += bytes;
                    }
                }
            }
        }
        out.push(PodUsage {
            namespace,
            pod,
            cpu_cores,
            mem_bytes,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Accumulated series store.
// ---------------------------------------------------------------------------

/// A normalized external metric series: `(unix_ms, value)` points plus tags.
#[derive(Debug, Clone)]
struct Series {
    name: String,
    unit: String,
    tags: BTreeMap<String, String>,
    points: Vec<(i64, f64)>,
}

/// Everything collected so far, served to the controller as JSON on connect.
#[derive(Debug, Default)]
struct Store {
    /// Keyed by `name|namespace|pod` (or `name|namespace` for the scrape count).
    series: BTreeMap<String, Series>,
    /// Successful scrapes.
    scrapes: u64,
    /// Scrapes that failed (transport error, non-2xx, or unparseable body).
    failures: u64,
}

impl Store {
    /// Append a `(ts, value)` point to a per-pod gauge series, creating it (with
    /// `{ namespace, pod }` tags) on first use.
    fn record_pod_sample(
        &mut self,
        name: &str,
        unit: &str,
        ns: &str,
        pod: &str,
        ts: i64,
        value: f64,
    ) {
        let key = format!("{name}|{ns}|{pod}");
        let entry = self.series.entry(key).or_insert_with(|| {
            let mut tags = BTreeMap::new();
            tags.insert("namespace".to_string(), ns.to_string());
            tags.insert("pod".to_string(), pod.to_string());
            Series {
                name: name.to_string(),
                unit: unit.to_string(),
                tags,
                points: Vec::new(),
            }
        });
        entry.points.push((ts, value));
    }

    /// Append the running scrape count to the `k8s_scrapes` counter series.
    fn record_scrape(&mut self, ns: &str, ts: i64) {
        let key = format!("{SCRAPES_METRIC}|{ns}");
        let count = self.scrapes as f64;
        let entry = self.series.entry(key).or_insert_with(|| {
            let mut tags = BTreeMap::new();
            tags.insert("namespace".to_string(), ns.to_string());
            Series {
                name: SCRAPES_METRIC.to_string(),
                unit: "count".to_string(),
                tags,
                points: Vec::new(),
            }
        });
        entry.points.push((ts, count));
    }

    /// Serialize the accumulated series to the JSON the collector endpoint emits.
    fn to_json(&self) -> String {
        let mut series = Vec::with_capacity(self.series.len());
        for s in self.series.values() {
            let mut tags = serde_json::Map::new();
            for (k, v) in &s.tags {
                tags.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            let points: Vec<serde_json::Value> = s
                .points
                .iter()
                .map(|(t, v)| {
                    serde_json::Value::Array(vec![
                        serde_json::Value::from(*t),
                        serde_json::Value::from(*v),
                    ])
                })
                .collect();
            let mut obj = serde_json::Map::new();
            obj.insert(
                "name".to_string(),
                serde_json::Value::String(s.name.clone()),
            );
            obj.insert(
                "unit".to_string(),
                serde_json::Value::String(s.unit.clone()),
            );
            obj.insert("tags".to_string(), serde_json::Value::Object(tags));
            obj.insert("points".to_string(), serde_json::Value::Array(points));
            series.push(serde_json::Value::Object(obj));
        }
        let mut root = serde_json::Map::new();
        root.insert("series".to_string(), serde_json::Value::Array(series));
        root.insert("scrapes".to_string(), serde_json::Value::from(self.scrapes));
        root.insert(
            "failures".to_string(),
            serde_json::Value::from(self.failures),
        );
        serde_json::Value::Object(root).to_string()
    }
}

/// Fold one scrape outcome into the store: record per-pod samples and bump the
/// scrape count on success, or bump `failures` on any error. Never fails.
fn apply_scrape(store: &mut Store, namespace: &str, result: Result<String, String>, now_ms: i64) {
    let body = match result {
        Ok(b) => b,
        Err(_) => {
            store.failures += 1;
            return;
        }
    };
    let pods = match parse_pod_metrics(&body) {
        Ok(p) => p,
        Err(_) => {
            store.failures += 1;
            return;
        }
    };
    for p in pods {
        // A pod's own namespace wins; fall back to the configured one.
        let ns = if p.namespace.is_empty() {
            namespace
        } else {
            p.namespace.as_str()
        };
        store.record_pod_sample(CPU_METRIC, "cores", ns, &p.pod, now_ms, p.cpu_cores);
        store.record_pod_sample(MEM_METRIC, "bytes", ns, &p.pod, now_ms, p.mem_bytes);
    }
    store.scrapes += 1;
    store.record_scrape(namespace, now_ms);
}

// ---------------------------------------------------------------------------
// Scraper seam — a trait so the collector can be unit-tested without a socket.
// ---------------------------------------------------------------------------

/// Fetches one raw metrics-API response body. `Err` is any transport / non-2xx
/// failure (counted, never fatal).
trait Scraper: Send {
    fn scrape(&self) -> Result<String, String>;
}

/// A real HTTPS scrape of the metrics API over hyper + rustls.
struct HttpScraper {
    client: HttpClient,
    url: String,
    token: Option<String>,
    timeout_ms: u64,
}

impl HttpScraper {
    async fn get(&self) -> Result<String, String> {
        let mut builder = Request::builder()
            .method("GET")
            .uri(&self.url)
            .header(ACCEPT, "application/json");
        if let Some(token) = self.token.as_deref().filter(|t| !t.is_empty()) {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = builder
            .body(Full::new(Bytes::new()))
            .map_err(|e| format!("building request failed: {e}"))?;

        let send = self.client.request(request);
        let resp = if self.timeout_ms == 0 {
            send.await
                .map_err(|e| format!("request to {} failed: {e}", self.url))?
        } else {
            tokio::time::timeout(Duration::from_millis(self.timeout_ms), send)
                .await
                .map_err(|_| format!("request to {} timed out", self.url))?
                .map_err(|e| format!("request to {} failed: {e}", self.url))?
        };
        let status = resp.status().as_u16();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("reading body failed: {e}"))?
            .to_bytes();
        if !(200..300).contains(&status) {
            return Err(format!("metrics API returned HTTP {status}"));
        }
        String::from_utf8(bytes.to_vec()).map_err(|e| format!("non-utf8 response: {e}"))
    }
}

impl Scraper for HttpScraper {
    fn scrape(&self) -> Result<String, String> {
        runtime().block_on(self.get())
    }
}

// ---------------------------------------------------------------------------
// TLS — a rustls client config with cluster-CA roots (or no verification).
// ---------------------------------------------------------------------------

/// Certificate verifier that accepts everything (`insecure`).
#[derive(Debug)]
struct NoVerify {
    schemes: Vec<SignatureScheme>,
}

impl NoVerify {
    fn new() -> Self {
        NoVerify {
            schemes: rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

fn read_pem_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let data = std::fs::read(path).map_err(|e| format!("cannot read CA file {path}: {e}"))?;
    let mut certs = Vec::new();
    for cert in rustls_pemfile::certs(&mut data.as_slice()) {
        certs.push(cert.map_err(|e| format!("invalid certificate in {path}: {e}"))?);
    }
    if certs.is_empty() {
        return Err(format!("no certificates found in {path}"));
    }
    Ok(certs)
}

/// Build the HTTPS client: webpki roots plus any cluster `ca_cert`, or a
/// verifier that accepts anything when `insecure`.
fn build_client(ca_cert: Option<&str>, insecure: bool) -> Result<HttpClient, String> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = ca_cert {
        for cert in read_pem_certs(path)? {
            roots
                .add(cert)
                .map_err(|e| format!("cannot add CA from {path}: {e}"))?;
        }
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    if insecure {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerify::new()));
    }
    let https = HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_or_http()
        .enable_http1()
        .build();
    Ok(Client::builder(TokioExecutor::new()).build(https))
}

// ---------------------------------------------------------------------------
// Config.
// ---------------------------------------------------------------------------

/// The collector's parsed configuration.
#[derive(Debug, Clone)]
struct Config {
    api_url: String,
    namespace: String,
    selector: Option<String>,
    interval_ms: u64,
    token: Option<String>,
    ca_cert: Option<String>,
    insecure: bool,
    bind: String,
}

fn parse_config(config_json: &str) -> Result<Config, String> {
    let cfg: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let str_opt = |key: &str| -> Option<String> {
        cfg.get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };

    let api_url = str_opt("api_url")
        .unwrap_or_else(|| IN_CLUSTER_API.to_string())
        .trim_end_matches('/')
        .to_string();
    let namespace = str_opt("namespace").unwrap_or_else(|| "default".to_string());
    let selector = str_opt("selector");
    let interval_ms = cfg
        .get("interval_ms")
        .and_then(|v| v.as_u64())
        .filter(|n| *n > 0)
        .unwrap_or(5000);
    let token = str_opt("token");
    let ca_cert = str_opt("ca_cert");
    let insecure = cfg
        .get("insecure")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let bind = str_opt("bind").unwrap_or_else(|| "127.0.0.1:0".to_string());

    Ok(Config {
        api_url,
        namespace,
        selector,
        interval_ms,
        token,
        ca_cert,
        insecure,
        bind,
    })
}

/// Resolve the bearer token: an explicit value wins, else the mounted
/// service-account token (best-effort; absent is fine).
fn resolve_token(explicit: Option<&str>) -> Option<String> {
    if let Some(t) = explicit.filter(|t| !t.is_empty()) {
        return Some(t.to_string());
    }
    std::fs::read_to_string(SA_TOKEN_PATH)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Resolve the CA path: an explicit value wins, else the mounted SA CA if it
/// exists (so in-cluster defaults work without extra config).
fn resolve_ca(explicit: Option<&str>) -> Option<String> {
    if let Some(p) = explicit {
        return Some(p.to_string());
    }
    if Path::new(SA_CA_PATH).exists() {
        Some(SA_CA_PATH.to_string())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Collector core + background poller.
// ---------------------------------------------------------------------------

/// Ties a [`Scraper`] to the shared [`Store`]. `poll` performs one scrape and
/// folds the result in under the lock.
struct Collector {
    scraper: Box<dyn Scraper>,
    store: Arc<Mutex<Store>>,
    namespace: String,
}

impl Collector {
    fn poll(&self, now_ms: i64) {
        let result = self.scraper.scrape();
        let mut store = match self.store.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        apply_scrape(&mut store, &self.namespace, result, now_ms);
    }
}

/// A running collector. Handed to `stop()` for teardown.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    poller: Option<JoinHandle<()>>,
    accept: Option<JoinHandle<()>>,
    addr: String,
}

/// Spawn the poller thread: scrape immediately, then every `interval`, checking
/// the shutdown flag on a 100ms granularity so `stop()` returns promptly.
fn spawn_poller(
    collector: Collector,
    interval: Duration,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let step = Duration::from_millis(100);
        while !shutdown.load(Ordering::Relaxed) {
            collector.poll(now_unix_ms());
            let mut slept = Duration::ZERO;
            while slept < interval && !shutdown.load(Ordering::Relaxed) {
                let chunk = step.min(interval - slept);
                std::thread::sleep(chunk);
                slept += chunk;
            }
        }
    })
}

/// Spawn the accept loop: each connection is answered with the current store as
/// one JSON line, then closed.
fn spawn_accept_loop(
    listener: TcpListener,
    store: Arc<Mutex<Store>>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let store = store.clone();
                    std::thread::spawn(move || serve_snapshot(stream, store));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    })
}

/// Write the accumulated series to one connection as a single JSON line.
fn serve_snapshot(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    let json = {
        let guard = match store.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.to_json()
    };
    if stream.write_all(json.as_bytes()).is_err() {
        return;
    }
    let _ = stream.write_all(b"\n");
    let _ = stream.flush();
}

// ---------------------------------------------------------------------------
// The service plugin.
// ---------------------------------------------------------------------------

/// The service plugin instance.
#[derive(Default)]
struct K8sMetrics {
    handle: Option<ServerHandle>,
}

impl K8sMetrics {
    fn start_config(&mut self, cfg: Config) -> Result<String, String> {
        let token = resolve_token(cfg.token.as_deref());
        let ca = resolve_ca(cfg.ca_cert.as_deref());
        let client = build_client(ca.as_deref(), cfg.insecure)?;
        let url = metrics_url(&cfg.api_url, &cfg.namespace, cfg.selector.as_deref());

        // A scrape budget a little beyond the poll interval; a slow scrape is
        // counted, never fatal.
        let timeout_ms = cfg.interval_ms.saturating_add(2000).max(2000);
        let scraper: Box<dyn Scraper> = Box::new(HttpScraper {
            client,
            url,
            token,
            timeout_ms,
        });

        // Bind the local collector endpoint.
        let listener = TcpListener::bind(&cfg.bind)
            .map_err(|e| format!("cannot bind collector endpoint {}: {e}", cfg.bind))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let store = Arc::new(Mutex::new(Store::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let collector = Collector {
            scraper,
            store: store.clone(),
            namespace: cfg.namespace,
        };
        let poller = spawn_poller(
            collector,
            Duration::from_millis(cfg.interval_ms),
            shutdown.clone(),
        );
        let accept = spawn_accept_loop(listener, store, shutdown.clone());

        self.handle = Some(ServerHandle {
            shutdown,
            poller: Some(poller),
            accept: Some(accept),
            addr: addr.clone(),
        });
        Ok(addr)
    }
}

impl FfiService for K8sMetrics {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if let Some(h) = self.handle.as_ref() {
            // Already running: return the existing address rather than rebind.
            return ROk(RString::from(h.addr.clone()));
        }
        let cfg = match parse_config(config_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        match self.start_config(cfg) {
            Ok(addr) => ROk(RString::from(addr)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // Idempotent: a no-op when never started or already stopped.
        if let Some(mut handle) = self.handle.take() {
            handle.shutdown.store(true, Ordering::Relaxed);
            if let Some(join) = handle.poller.take() {
                let _ = join.join();
            }
            if let Some(join) = handle.accept.take() {
                let _ = join.join();
            }
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description":
                "Polls metrics.k8s.io for pod CPU/memory and emits system-metric samples",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(K8sMetrics::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RNone,
        make_service: RSome(make_service),
        make_data_source: RNone,
    }
}

// ---------------------------------------------------------------------------
// Tests — all offline; scrapes come from a scripted mock, never a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A scripted scraper: each `scrape()` returns the next queued result.
    struct MockScraper {
        replies: Mutex<VecDeque<Result<String, String>>>,
    }

    impl MockScraper {
        fn new(replies: Vec<Result<String, String>>) -> Self {
            MockScraper {
                replies: Mutex::new(replies.into_iter().collect()),
            }
        }
    }

    impl Scraper for MockScraper {
        fn scrape(&self) -> Result<String, String> {
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("mock: no more scripted replies".to_string()))
        }
    }

    fn sample_body() -> String {
        serde_json::json!({
            "kind": "PodMetricsList",
            "items": [
                {
                    "metadata": { "name": "api-0", "namespace": "app" },
                    "containers": [
                        { "name": "api", "usage": { "cpu": "250m", "memory": "128Mi" } },
                        { "name": "sidecar", "usage": { "cpu": "50m", "memory": "16Mi" } }
                    ]
                },
                {
                    "metadata": { "name": "api-1", "namespace": "app" },
                    "containers": [
                        { "name": "api", "usage": { "cpu": "123456789n", "memory": "1000000" } }
                    ]
                }
            ]
        })
        .to_string()
    }

    // -- quantity parsing ----------------------------------------------------

    #[test]
    fn parses_cpu_quantities() {
        assert_eq!(parse_quantity("1"), Some(1.0));
        assert_eq!(parse_quantity("250m"), Some(0.25));
        assert_eq!(parse_quantity("1500m"), Some(1.5));
        // Nanocores: 123456789n -> ~0.123456789 cores.
        let cores = parse_quantity("123456789n").unwrap();
        assert!((cores - 0.123_456_789).abs() < 1e-9, "{cores}");
    }

    #[test]
    fn parses_memory_quantities() {
        assert_eq!(parse_quantity("1000000"), Some(1_000_000.0));
        assert_eq!(parse_quantity("1Ki"), Some(1024.0));
        assert_eq!(parse_quantity("128Mi"), Some(128.0 * 1_048_576.0));
        assert_eq!(parse_quantity("2Gi"), Some(2.0 * 1_073_741_824.0));
        // Decimal SI mega vs binary mebi are distinct.
        assert_eq!(parse_quantity("1M"), Some(1e6));
    }

    #[test]
    fn rejects_malformed_quantities() {
        assert_eq!(parse_quantity(""), None);
        assert_eq!(parse_quantity("   "), None);
        assert_eq!(parse_quantity("abc"), None);
        assert_eq!(parse_quantity("Mi"), None);
    }

    // -- url building --------------------------------------------------------

    #[test]
    fn encodes_selector() {
        assert_eq!(encode_selector("app=api"), "app%3Dapi");
        assert_eq!(
            encode_selector("tier=backend,app=api"),
            "tier%3Dbackend%2Capp%3Dapi"
        );
        assert_eq!(encode_selector("plain-name_1.2~"), "plain-name_1.2~");
    }

    #[test]
    fn builds_metrics_url() {
        assert_eq!(
            metrics_url("https://kubernetes.default.svc", "app", None),
            "https://kubernetes.default.svc/apis/metrics.k8s.io/v1beta1/namespaces/app/pods"
        );
        assert_eq!(
            metrics_url("https://k8s/", "app", Some("app=api")),
            "https://k8s/apis/metrics.k8s.io/v1beta1/namespaces/app/pods?labelSelector=app%3Dapi"
        );
        // An empty selector adds no query string.
        assert_eq!(
            metrics_url("https://k8s", "ns", Some("")),
            "https://k8s/apis/metrics.k8s.io/v1beta1/namespaces/ns/pods"
        );
    }

    // -- response parsing ----------------------------------------------------

    #[test]
    fn parses_pod_metrics_and_sums_containers() {
        let pods = parse_pod_metrics(&sample_body()).unwrap();
        assert_eq!(pods.len(), 2);

        let api0 = &pods[0];
        assert_eq!(api0.pod, "api-0");
        assert_eq!(api0.namespace, "app");
        // 250m + 50m = 0.30 cores; 128Mi + 16Mi = 144Mi bytes.
        assert!((api0.cpu_cores - 0.30).abs() < 1e-9, "{}", api0.cpu_cores);
        assert_eq!(api0.mem_bytes, 144.0 * 1_048_576.0);

        let api1 = &pods[1];
        assert_eq!(api1.pod, "api-1");
        assert!(
            (api1.cpu_cores - 0.123_456_789).abs() < 1e-9,
            "{}",
            api1.cpu_cores
        );
        assert_eq!(api1.mem_bytes, 1_000_000.0);
    }

    #[test]
    fn parse_rejects_non_list_and_skips_nameless() {
        assert!(parse_pod_metrics("not json").is_err());
        assert!(parse_pod_metrics(r#"{"kind":"Status"}"#).is_err());
        // Items without a metadata name are skipped, not fatal.
        let body = r#"{"items":[{"containers":[{"usage":{"cpu":"1"}}]}]}"#;
        assert!(parse_pod_metrics(body).unwrap().is_empty());
    }

    // -- scrape folding ------------------------------------------------------

    #[test]
    fn apply_scrape_records_samples_and_counts() {
        let mut store = Store::default();
        apply_scrape(&mut store, "app", Ok(sample_body()), 1_000);
        assert_eq!(store.scrapes, 1);
        assert_eq!(store.failures, 0);

        // Two pods x (cpu + mem) = 4 pod series, plus 1 scrape-count series.
        assert_eq!(store.series.len(), 5);
        let cpu0 = store
            .series
            .get("k8s_pod_cpu_cores|app|api-0")
            .expect("cpu series");
        assert_eq!(cpu0.points.len(), 1);
        assert_eq!(cpu0.points[0].0, 1_000);
        assert!(
            (cpu0.points[0].1 - 0.30).abs() < 1e-9,
            "{}",
            cpu0.points[0].1
        );
        assert_eq!(cpu0.tags.get("pod").map(String::as_str), Some("api-0"));
        let scrapes = store.series.get("k8s_scrapes|app").expect("scrape series");
        assert_eq!(scrapes.points, vec![(1_000, 1.0)]);
    }

    #[test]
    fn apply_scrape_counts_failures_without_samples() {
        let mut store = Store::default();
        // Transport error: no samples, one failure, no scrape counted.
        apply_scrape(&mut store, "app", Err("connection refused".to_string()), 1);
        // Garbage body: parse failure, another failure counted.
        apply_scrape(&mut store, "app", Ok("garbage".to_string()), 2);
        assert_eq!(store.scrapes, 0);
        assert_eq!(store.failures, 2);
        assert!(store.series.is_empty());
    }

    #[test]
    fn collector_poll_folds_scrapes_in_order() {
        let store = Arc::new(Mutex::new(Store::default()));
        let scraper = Box::new(MockScraper::new(vec![
            Ok(sample_body()),
            Err("timeout".to_string()),
            Ok(sample_body()),
        ]));
        let collector = Collector {
            scraper,
            store: store.clone(),
            namespace: "app".to_string(),
        };
        collector.poll(10);
        collector.poll(20); // failed scrape -> gap
        collector.poll(30);

        let store = store.lock().unwrap();
        assert_eq!(store.scrapes, 2);
        assert_eq!(store.failures, 1);
        // The cpu series got a point from each successful scrape (t=10, t=30).
        let cpu0 = store.series.get("k8s_pod_cpu_cores|app|api-0").unwrap();
        assert_eq!(cpu0.points.len(), 2);
        assert_eq!(cpu0.points[0].0, 10);
        assert_eq!(cpu0.points[1].0, 30);
    }

    // -- store serialization -------------------------------------------------

    #[test]
    fn store_to_json_shape() {
        let mut store = Store::default();
        apply_scrape(&mut store, "app", Ok(sample_body()), 1_000);
        let value: serde_json::Value = serde_json::from_str(&store.to_json()).unwrap();
        assert_eq!(value["scrapes"], 1);
        assert_eq!(value["failures"], 0);
        let series = value["series"].as_array().unwrap();
        assert_eq!(series.len(), 5);
        // BTreeMap key order puts `k8s_pod_cpu_cores|app|api-0` first. Every
        // series carries a name, unit, tags object and points array.
        let cpu = &series[0];
        assert_eq!(cpu["name"], "k8s_pod_cpu_cores");
        assert_eq!(cpu["unit"], "cores");
        assert_eq!(cpu["tags"]["namespace"], "app");
        assert_eq!(cpu["tags"]["pod"], "api-0");
        assert_eq!(cpu["points"][0][0], 1_000);
    }

    // -- config --------------------------------------------------------------

    #[test]
    fn config_defaults() {
        let cfg = parse_config("{}").unwrap();
        assert_eq!(cfg.api_url, IN_CLUSTER_API);
        assert_eq!(cfg.namespace, "default");
        assert_eq!(cfg.selector, None);
        assert_eq!(cfg.interval_ms, 5000);
        assert_eq!(cfg.token, None);
        assert_eq!(cfg.ca_cert, None);
        assert!(!cfg.insecure);
        assert_eq!(cfg.bind, "127.0.0.1:0");
    }

    #[test]
    fn config_overrides() {
        let cfg = parse_config(
            r#"{
                "api_url": "https://k8s.example.com/",
                "namespace": "app",
                "selector": "app=api",
                "interval_ms": 10000,
                "token": "abc",
                "ca_cert": "/etc/ca.pem",
                "insecure": true,
                "bind": "127.0.0.1:9999"
            }"#,
        )
        .unwrap();
        // Trailing slash is trimmed.
        assert_eq!(cfg.api_url, "https://k8s.example.com");
        assert_eq!(cfg.namespace, "app");
        assert_eq!(cfg.selector.as_deref(), Some("app=api"));
        assert_eq!(cfg.interval_ms, 10000);
        assert_eq!(cfg.token.as_deref(), Some("abc"));
        assert_eq!(cfg.ca_cert.as_deref(), Some("/etc/ca.pem"));
        assert!(cfg.insecure);
        assert_eq!(cfg.bind, "127.0.0.1:9999");
    }

    #[test]
    fn config_rejects_invalid_json() {
        assert!(parse_config("not json").is_err());
    }

    #[test]
    fn resolve_token_prefers_explicit() {
        // An explicit token wins outright, with no filesystem lookup.
        assert_eq!(resolve_token(Some("tok")).as_deref(), Some("tok"));
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = K8sMetrics::default();
        svc.stop();
        svc.stop(); // second stop must not panic
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_rejects_bad_config() {
        let mut svc = K8sMetrics::default();
        let res = svc.start(RString::from("not json"));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "k8s-metrics");
    }
}
