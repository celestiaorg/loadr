//! `loadr-plugin-data-cleanup` — a native **service** plugin that keeps a
//! registry of the resources a run creates and deletes them at run end, so a
//! load test against a shared, long-lived environment does not leak data.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle: the
//! host calls `start(config_json)` once, before the run, and `stop()` once
//! afterwards (idempotently). On `start` this plugin:
//!
//! 1. parses and validates the cleanup `strategy` (`http-delete` or `sql`),
//!    failing fast on a misconfigured plan (and, for `sql`, opening the pool);
//! 2. binds a tiny local line endpoint (`127.0.0.1:0` by default) and returns
//!    its bound address.
//!
//! Every VU that opens that endpoint and writes a line **registers** the id (or
//! absolute URL) of a resource it just created — via a `js:` hook. The service
//! buffers each into an in-memory registry; nothing is deleted during the run,
//! so tracking adds no per-request network cost. When the last VU retires and
//! `stop()` runs, the service walks the registry and issues **one cleanup call
//! per resource**: an HTTP `DELETE` (the `http-delete` strategy) or a
//! parameterised SQL `DELETE` (the `sql` strategy), returning the environment to
//! the state it was in before the run.
//!
//! # Transport
//!
//! `http-delete` uses the project's existing **hyper + hyper-rustls** stack
//! (webpki roots + `ring`) — no extra HTTP client, no OpenSSL/C dependency.
//! `sql` runs a parameterised `DELETE FROM <table> WHERE <key> = $1` through
//! `sqlx`'s `any` driver (PostgreSQL or MySQL, chosen from the URL scheme); the
//! tracked id is always **bound as a query parameter**, never interpolated, so a
//! value from a response body cannot turn into SQL injection.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::Value;
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "data-cleanup";
/// Wall-clock cap for a single end-of-run DELETE.
const DELETE_TIMEOUT_MS: u64 = 30_000;

/// The single Tokio runtime driving the async hyper / sqlx work. Shared across
/// the (potentially concurrent) worker threads in `stop()`.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build data-cleanup plugin tokio runtime")
    })
}

/// Register the `any` driver's concrete backends exactly once per process.
/// `install_default_drivers` panics if called twice, so it is guarded.
fn ensure_drivers() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

// ---------------------------------------------------------------------------
// Config parsing.
// ---------------------------------------------------------------------------

/// The cleanup mechanism and its strategy-specific targets.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Strategy {
    HttpDelete {
        base: String,
        headers: Vec<(String, String)>,
    },
    Sql {
        url: String,
        table: String,
        key: String,
    },
}

/// The validated configuration handed to `start()`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    strategy: Strategy,
    bind: String,
    concurrency: usize,
    continue_on_error: bool,
}

/// True for a safe SQL identifier (table / column). Restricting these to
/// `[A-Za-z0-9_.]` keeps a hand-written `table`/`key` from opening an injection
/// hole, since they are interpolated into the statement (the *id* is always
/// bound as a parameter).
fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.'))
}

/// Pull a non-empty string field from `cfg`, or a descriptive error.
fn required_str(cfg: &Value, key: &str) -> Result<String, String> {
    match cfg.get(key).and_then(Value::as_str) {
        Some(s) if !s.trim().is_empty() => Ok(s.to_string()),
        _ => Err(format!("config requires a non-empty `{key}` string")),
    }
}

fn parse_config(config_json: &str) -> Result<Config, String> {
    let cfg: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let strategy = match required_str(&cfg, "strategy")?.as_str() {
        "http-delete" => {
            let base = required_str(&cfg, "base")?;
            let headers = parse_headers(cfg.get("headers"))?;
            Strategy::HttpDelete { base, headers }
        }
        "sql" => {
            let url = required_str(&cfg, "url")?;
            let table = required_str(&cfg, "table")?;
            let key = cfg
                .get("key")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .unwrap_or("id")
                .to_string();
            if !valid_ident(&table) {
                return Err(format!("invalid `table` identifier `{table}`"));
            }
            if !valid_ident(&key) {
                return Err(format!("invalid `key` identifier `{key}`"));
            }
            Strategy::Sql { url, table, key }
        }
        other => {
            return Err(format!(
                "unknown strategy `{other}` (use `http-delete` or `sql`)"
            ))
        }
    };

    let concurrency = cfg
        .get("concurrency")
        .and_then(Value::as_u64)
        .unwrap_or(8)
        .max(1) as usize;
    let continue_on_error = cfg
        .get("continue_on_error")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let bind = cfg
        .get("bind")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1:0")
        .to_string();

    Ok(Config {
        strategy,
        bind,
        concurrency,
        continue_on_error,
    })
}

/// Parse the optional `headers` object into a `(name, value)` list.
fn parse_headers(value: Option<&Value>) -> Result<Vec<(String, String)>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let map = value
        .as_object()
        .ok_or_else(|| "`headers` must be an object of string values".to_string())?;
    let mut out = Vec::with_capacity(map.len());
    for (name, v) in map {
        let text = v
            .as_str()
            .ok_or_else(|| format!("header `{name}` must be a string"))?;
        out.push((name.clone(), text.to_string()));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Cleanup — a `Cleaner` seam so the drain loop is unit-tested without a socket.
// ---------------------------------------------------------------------------

/// The outcome of deleting one tracked resource.
#[derive(Debug, PartialEq, Eq)]
enum DeleteOutcome {
    /// Removed (HTTP 2xx/404, or a SQL DELETE that ran).
    Deleted,
    /// Could not be removed; the string is the reason (counted, not fatal).
    Failed(String),
}

/// Deletes one tracked resource. Implementations must not panic.
trait Cleaner: Send + Sync {
    fn delete(&self, resource: &str) -> DeleteOutcome;
}

/// Aggregate teardown result, surfaced after `stop()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CleanupStats {
    deleted: u64,
    errors: u64,
}

/// Drain `resources`, deleting each through `cleaner` with up to `concurrency`
/// calls in flight. With `continue_on_error = false` the first failure stops
/// further work. Pure of any socket — the tests drive it through a mock cleaner.
fn run_cleanup(
    cleaner: &dyn Cleaner,
    resources: Vec<String>,
    concurrency: usize,
    continue_on_error: bool,
) -> CleanupStats {
    if resources.is_empty() {
        return CleanupStats::default();
    }
    let queue = Mutex::new(VecDeque::from(resources));
    let deleted = AtomicU64::new(0);
    let errors = AtomicU64::new(0);
    let abort = AtomicBool::new(false);
    let workers = concurrency.max(1);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                if abort.load(Ordering::Relaxed) {
                    break;
                }
                let next = {
                    let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                    q.pop_front()
                };
                let Some(resource) = next else {
                    break;
                };
                match cleaner.delete(&resource) {
                    DeleteOutcome::Deleted => {
                        deleted.fetch_add(1, Ordering::Relaxed);
                    }
                    DeleteOutcome::Failed(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        if !continue_on_error {
                            abort.store(true, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    CleanupStats {
        deleted: deleted.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------
// http-delete cleaner.
// ---------------------------------------------------------------------------

/// Resolve a tracked value to the URL to DELETE. An absolute `http(s)://` value
/// is deleted as-is; a bare id is appended to `base`.
fn resource_url(base: &str, resource: &str) -> String {
    if resource.starts_with("http://") || resource.starts_with("https://") {
        resource.to_string()
    } else {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            resource.trim_start_matches('/')
        )
    }
}

/// Map an HTTP DELETE status to an outcome. `2xx` and `404` (already gone) are
/// success; anything else is a counted failure.
fn http_outcome(status: u16) -> DeleteOutcome {
    match status {
        200..=299 | 404 => DeleteOutcome::Deleted,
        other => DeleteOutcome::Failed(format!("DELETE returned status {other}")),
    }
}

/// Issues HTTP `DELETE`s over hyper + hyper-rustls.
struct HttpCleaner {
    base: String,
    headers: Vec<(String, String)>,
    timeout_ms: u64,
}

impl Cleaner for HttpCleaner {
    fn delete(&self, resource: &str) -> DeleteOutcome {
        let url = resource_url(&self.base, resource);
        match runtime().block_on(http_delete(&url, &self.headers, self.timeout_ms)) {
            Ok(status) => http_outcome(status),
            Err(e) => DeleteOutcome::Failed(e),
        }
    }
}

/// DELETE `url` with the given headers, returning the HTTP status.
async fn http_delete(
    url: &str,
    headers: &[(String, String)],
    timeout_ms: u64,
) -> Result<u16, String> {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also allows plaintext endpoints.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(tls);

    let mut builder = Request::builder().method("DELETE").uri(url);
    for (k, v) in headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(Bytes::new()))
        .map_err(|e| format!("building DELETE {url} failed: {e}"))?;

    let send = client.request(request);
    let resp = tokio::time::timeout(Duration::from_millis(timeout_ms), send)
        .await
        .map_err(|_| format!("DELETE {url} timed out after {timeout_ms}ms"))?
        .map_err(|e| format!("DELETE {url} failed: {e}"))?;
    let status = resp.status().as_u16();
    // Drain (and discard) the body to release the connection.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

// ---------------------------------------------------------------------------
// sql cleaner.
// ---------------------------------------------------------------------------

/// Build the parameterised DELETE. The id is always a bound parameter; only the
/// (validated) table/key identifiers are interpolated. MySQL uses `?`,
/// PostgreSQL uses `$1`.
fn delete_statement(table: &str, key: &str, url: &str) -> String {
    let placeholder = if url.starts_with("mysql") { "?" } else { "$1" };
    format!("DELETE FROM {table} WHERE {key} = {placeholder}")
}

/// Issues parameterised SQL `DELETE`s through a pooled `sqlx` connection.
struct SqlCleaner {
    pool: AnyPool,
    statement: String,
}

impl Cleaner for SqlCleaner {
    fn delete(&self, resource: &str) -> DeleteOutcome {
        match runtime().block_on(self.execute(resource)) {
            Ok(()) => DeleteOutcome::Deleted,
            Err(e) => DeleteOutcome::Failed(e),
        }
    }
}

impl SqlCleaner {
    async fn execute(&self, id: &str) -> Result<(), String> {
        sqlx::query(&self.statement)
            .bind(id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("SQL delete failed: {e}"))
    }
}

/// Open the pool and build the cleaner for the `sql` strategy.
async fn connect_sql(
    url: &str,
    table: &str,
    key: &str,
    concurrency: usize,
) -> Result<SqlCleaner, String> {
    ensure_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(concurrency.max(1) as u32)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
        .map_err(|e| format!("cannot connect to `{url}`: {e}"))?;
    Ok(SqlCleaner {
        pool,
        statement: delete_statement(table, key, url),
    })
}

/// Build the strategy-specific cleaner. `http-delete` is lazy (no connection
/// until the first DELETE); `sql` opens its pool now so a bad URL fails `start`.
fn build_cleaner(config: &Config) -> Result<Box<dyn Cleaner>, String> {
    match &config.strategy {
        Strategy::HttpDelete { base, headers } => Ok(Box::new(HttpCleaner {
            base: base.clone(),
            headers: headers.clone(),
            timeout_ms: DELETE_TIMEOUT_MS,
        })),
        Strategy::Sql { url, table, key } => {
            let cleaner = runtime().block_on(connect_sql(url, table, key, config.concurrency))?;
            Ok(Box::new(cleaner))
        }
    }
}

// ---------------------------------------------------------------------------
// Tracking endpoint — VUs write one id (or URL) per line; the service buffers.
// ---------------------------------------------------------------------------

/// A running tracking endpoint. Handed to `stop()` for teardown + drain.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    addr: String,
    registry: Arc<Mutex<Vec<String>>>,
    cleaner: Box<dyn Cleaner>,
    concurrency: usize,
    continue_on_error: bool,
}

/// The service plugin instance.
#[derive(Default)]
struct DataCleanup {
    handle: Option<ServerHandle>,
    /// The stats from the most recent `stop()` (surfaced for inspection/tests).
    stats: CleanupStats,
}

impl DataCleanup {
    fn start_config(&mut self, config: Config) -> Result<String, String> {
        // Build the cleaner first so a bad strategy/URL fails before we bind.
        let cleaner = build_cleaner(&config)?;

        let listener = TcpListener::bind(&config.bind)
            .map_err(|e| format!("cannot bind tracking endpoint {}: {e}", config.bind))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let registry = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let accept = spawn_ingest_loop(listener, registry.clone(), shutdown.clone());

        self.handle = Some(ServerHandle {
            shutdown,
            accept: Some(accept),
            addr: addr.clone(),
            registry,
            cleaner,
            concurrency: config.concurrency,
            continue_on_error: config.continue_on_error,
        });
        Ok(addr)
    }
}

/// Accept VU connections and buffer every id they write into `registry`.
fn spawn_ingest_loop(
    listener: TcpListener,
    registry: Arc<Mutex<Vec<String>>>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let registry = registry.clone();
                    let shutdown = shutdown.clone();
                    std::thread::spawn(move || handle_ingest(stream, registry, shutdown));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    })
}

/// Serve one VU connection: for every line written, buffer it as a tracked
/// resource and acknowledge with `OK\n` so a `socket` request completes.
fn handle_ingest(stream: TcpStream, registry: Arc<Mutex<Vec<String>>>, shutdown: Arc<AtomicBool>) {
    let _ = stream.set_nonblocking(false);
    let mut reader = BufReader::new(match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut writer = stream;
    let mut line = String::new();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return, // client closed
            Ok(_) => {}
            Err(_) => return,
        }
        let value = line.trim();
        if !value.is_empty() {
            let mut guard = registry.lock().unwrap_or_else(|e| e.into_inner());
            guard.push(value.to_string());
        }
        if writer.write_all(b"OK\n").is_err() {
            return;
        }
        let _ = writer.flush();
    }
}

// ---------------------------------------------------------------------------
// Service ABI.
// ---------------------------------------------------------------------------

impl FfiService for DataCleanup {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if let Some(h) = self.handle.as_ref() {
            // Already running: return the existing address rather than rebind.
            return ROk(RString::from(h.addr.clone()));
        }
        let config = match parse_config(config_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        match self.start_config(config) {
            Ok(addr) => ROk(RString::from(addr)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // Idempotent: a no-op when never started or already stopped.
        let Some(mut handle) = self.handle.take() else {
            return;
        };
        // Stop ingesting, then join the accept loop so the registry is final.
        handle.shutdown.store(true, Ordering::Relaxed);
        if let Some(join) = handle.accept.take() {
            let _ = join.join();
        }
        let resources = {
            let mut guard = handle.registry.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        let stats = run_cleanup(
            handle.cleaner.as_ref(),
            resources,
            handle.concurrency,
            handle.continue_on_error,
        );
        eprintln!(
            "data-cleanup: cleanup_resources_deleted={} cleanup_errors={}",
            stats.deleted, stats.errors
        );
        self.stats = stats;
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description":
                "Tracks the resources a run creates and deletes them at run end (HTTP or SQL)",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(DataCleanup::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline. Config parsing, statement/URL building and the drain
// loop are exercised directly; the drain runs through a scripted mock cleaner,
// never a real socket, HTTP connection or database.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A scripted cleaner: records how many resources it processed and fails any
    /// whose value contains `fail_marker`. (Not named `new` on purpose.)
    struct MockCleaner {
        processed: Arc<AtomicUsize>,
        fail_marker: Option<&'static str>,
    }

    impl Cleaner for MockCleaner {
        fn delete(&self, resource: &str) -> DeleteOutcome {
            self.processed.fetch_add(1, Ordering::Relaxed);
            match self.fail_marker {
                Some(m) if resource.contains(m) => DeleteOutcome::Failed("boom".to_string()),
                _ => DeleteOutcome::Deleted,
            }
        }
    }

    fn mock(fail_marker: Option<&'static str>) -> (MockCleaner, Arc<AtomicUsize>) {
        let processed = Arc::new(AtomicUsize::new(0));
        (
            MockCleaner {
                processed: processed.clone(),
                fail_marker,
            },
            processed,
        )
    }

    fn resources(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // -- config parsing -----------------------------------------------------

    #[test]
    fn config_requires_strategy() {
        assert!(parse_config("{}").unwrap_err().contains("strategy"));
    }

    #[test]
    fn config_rejects_unknown_strategy() {
        let err = parse_config(r#"{"strategy":"nuke"}"#).unwrap_err();
        assert!(err.contains("unknown strategy"), "{err}");
    }

    #[test]
    fn http_delete_requires_base() {
        let err = parse_config(r#"{"strategy":"http-delete"}"#).unwrap_err();
        assert!(err.contains("base"), "{err}");
    }

    #[test]
    fn http_delete_defaults_and_headers() {
        let cfg = parse_config(
            r#"{"strategy":"http-delete","base":"https://api/v1/orders",
                "headers":{"Authorization":"Bearer t"}}"#,
        )
        .unwrap();
        assert_eq!(cfg.concurrency, 8);
        assert!(cfg.continue_on_error);
        assert_eq!(cfg.bind, "127.0.0.1:0");
        match cfg.strategy {
            Strategy::HttpDelete { base, headers } => {
                assert_eq!(base, "https://api/v1/orders");
                assert_eq!(headers, vec![("Authorization".into(), "Bearer t".into())]);
            }
            other => panic!("expected http-delete, got {other:?}"),
        }
    }

    #[test]
    fn http_delete_rejects_non_string_header() {
        let err =
            parse_config(r#"{"strategy":"http-delete","base":"https://api","headers":{"X":1}}"#)
                .unwrap_err();
        assert!(err.contains("must be a string"), "{err}");
    }

    #[test]
    fn sql_requires_url_and_table() {
        assert!(parse_config(r#"{"strategy":"sql","table":"t"}"#)
            .unwrap_err()
            .contains("url"));
        assert!(
            parse_config(r#"{"strategy":"sql","url":"postgres://h/db"}"#)
                .unwrap_err()
                .contains("table")
        );
    }

    #[test]
    fn sql_defaults_key_and_overrides() {
        let cfg = parse_config(
            r#"{"strategy":"sql","url":"postgres://h/db","table":"orders",
                "concurrency":4,"continue_on_error":false}"#,
        )
        .unwrap();
        assert_eq!(cfg.concurrency, 4);
        assert!(!cfg.continue_on_error);
        assert_eq!(
            cfg.strategy,
            Strategy::Sql {
                url: "postgres://h/db".into(),
                table: "orders".into(),
                key: "id".into(),
            }
        );
    }

    #[test]
    fn sql_rejects_unsafe_identifiers() {
        let err = parse_config(
            r#"{"strategy":"sql","url":"postgres://h/db","table":"orders;DROP TABLE x"}"#,
        )
        .unwrap_err();
        assert!(err.contains("invalid `table`"), "{err}");
    }

    #[test]
    fn concurrency_is_clamped_to_at_least_one() {
        let cfg =
            parse_config(r#"{"strategy":"http-delete","base":"https://api","concurrency":0}"#)
                .unwrap();
        assert_eq!(cfg.concurrency, 1);
    }

    // -- url + statement building -------------------------------------------

    #[test]
    fn bare_id_is_appended_absolute_url_is_kept() {
        assert_eq!(
            resource_url("https://api/v1/orders", "42"),
            "https://api/v1/orders/42"
        );
        // Trailing slash on base / leading slash on id collapse to one.
        assert_eq!(
            resource_url("https://api/v1/orders/", "/42"),
            "https://api/v1/orders/42"
        );
        assert_eq!(
            resource_url("https://api/v1/orders", "https://api/v1/orders/7"),
            "https://api/v1/orders/7"
        );
    }

    #[test]
    fn http_status_maps_404_and_2xx_to_deleted() {
        assert_eq!(http_outcome(200), DeleteOutcome::Deleted);
        assert_eq!(http_outcome(204), DeleteOutcome::Deleted);
        assert_eq!(http_outcome(404), DeleteOutcome::Deleted);
        assert!(matches!(http_outcome(403), DeleteOutcome::Failed(_)));
        assert!(matches!(http_outcome(500), DeleteOutcome::Failed(_)));
    }

    #[test]
    fn delete_statement_picks_placeholder_by_scheme() {
        assert_eq!(
            delete_statement("orders", "id", "postgres://h/db"),
            "DELETE FROM orders WHERE id = $1"
        );
        assert_eq!(
            delete_statement("orders", "order_id", "mysql://h/db"),
            "DELETE FROM orders WHERE order_id = ?"
        );
    }

    #[test]
    fn valid_ident_accepts_safe_and_rejects_unsafe() {
        assert!(valid_ident("orders"));
        assert!(valid_ident("public.orders"));
        assert!(valid_ident("order_id_2"));
        assert!(!valid_ident(""));
        assert!(!valid_ident("orders;drop"));
        assert!(!valid_ident("a b"));
    }

    // -- drain loop through the mock cleaner --------------------------------

    #[test]
    fn cleanup_of_empty_registry_is_a_noop() {
        let (cleaner, processed) = mock(None);
        let stats = run_cleanup(&cleaner, Vec::new(), 4, true);
        assert_eq!(stats, CleanupStats::default());
        assert_eq!(processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cleanup_deletes_every_resource() {
        let (cleaner, processed) = mock(None);
        let stats = run_cleanup(&cleaner, resources(&["1", "2", "3"]), 4, true);
        assert_eq!(
            stats,
            CleanupStats {
                deleted: 3,
                errors: 0
            }
        );
        assert_eq!(processed.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn cleanup_counts_errors_and_continues() {
        let (cleaner, processed) = mock(Some("bad"));
        let stats = run_cleanup(&cleaner, resources(&["ok1", "bad", "ok2"]), 4, true);
        assert_eq!(
            stats,
            CleanupStats {
                deleted: 2,
                errors: 1
            }
        );
        // continue_on_error: every resource was still attempted.
        assert_eq!(processed.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn cleanup_stops_at_first_error_when_configured() {
        let (cleaner, processed) = mock(Some("bad"));
        // Serial (concurrency 1) so the stop point is deterministic: ok1, bad,
        // then abort before ok2/ok3.
        let stats = run_cleanup(&cleaner, resources(&["ok1", "bad", "ok2", "ok3"]), 1, false);
        assert_eq!(
            stats,
            CleanupStats {
                deleted: 1,
                errors: 1
            }
        );
        assert_eq!(processed.load(Ordering::Relaxed), 2);
    }

    // -- service lifecycle --------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = DataCleanup::default();
        svc.stop();
        svc.stop(); // second stop must not panic
        assert!(svc.handle.is_none());
        assert_eq!(svc.stats, CleanupStats::default());
    }

    #[test]
    fn start_rejects_bad_config_without_binding() {
        let mut svc = DataCleanup::default();
        // Missing `base` -> start fails at parse, before any socket is bound.
        let res = svc.start(RString::from(r#"{"strategy":"http-delete"}"#));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn info_declares_service_kind() {
        let v: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "data-cleanup");
    }
}
