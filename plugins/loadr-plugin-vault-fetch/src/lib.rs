//! `loadr-plugin-vault-fetch` — a native **service** plugin that fetches KV
//! secrets from [HashiCorp Vault] once at run start and serves the same shared
//! values to every VU.
//!
//! # Why a service plugin
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> bound_addr` and an idempotent `stop()`. On `start`
//! this plugin:
//!
//! 1. authenticates to Vault — a pre-issued **token**, or an **AppRole** login
//!    exchanged for one — over the project's own hyper + hyper-rustls stack, so
//!    there is no Vault SDK and no `vault` binary, failing fast if the address
//!    is malformed or the credential is rejected;
//! 2. reads the KV secret at `path` **once** and caches every field, then binds
//!    a tiny local line endpoint (`127.0.0.1:0` by default) and returns its
//!    bound address.
//!
//! Every VU that opens that endpoint and writes a field name gets that field's
//! **cached value** back (an empty line for an unknown field, or the whole
//! secret as JSON for `*`). Because the read happens once, centrally, before any
//! VU begins, the fetch never touches the hot path and a Vault outage or a wrong
//! `path` fails the run at startup rather than as a wave of auth failures under
//! load. Secrets never live in the plan file: the plan refers to the fetched
//! fields, and only the bootstrap credential (from `${env.…}`) is supplied.
//!
//! When `renew` is set and the login returned a renewable lease, a background
//! task keeps the token alive for the length of the run; `stop()` revokes an
//! AppRole-minted token on the way out, best effort.
//!
//! # Tests
//!
//! Vault is reached through a [`VaultApi`] seam, so the login / read / renew /
//! lifecycle logic is exercised entirely offline against a scripted mock, never
//! a real socket, and the wire parsing is covered by pure functions.
//!
//! [HashiCorp Vault]: https://www.vaultproject.io/

use std::collections::BTreeMap;
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
use http_body_util::{BodyExt, Full};
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::Value;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "vault-fetch";

/// Never schedule a renewal sooner than this — guards against a tiny lease TTL
/// busy-looping the renew call.
const MIN_RENEW: Duration = Duration::from_secs(1);
/// After a failed renewal, retry on this short interval rather than the (now
/// unknown) lease TTL.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Auth methods + a login session.
// ---------------------------------------------------------------------------

/// Exactly one Vault auth method.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Auth {
    /// A pre-issued Vault token, used directly (no login round-trip).
    Token(String),
    /// An AppRole `role_id` / `secret_id` exchanged for a token.
    AppRole { role_id: String, secret_id: String },
}

/// The outcome of a login: a usable token and the lease TTL in seconds
/// (`0` = unknown / non-renewable).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    token: String,
    lease_duration: u64,
}

// ---------------------------------------------------------------------------
// Vault wire helpers — pure functions, unit-tested directly.
// ---------------------------------------------------------------------------

/// Join a base address and a suffix into one URL, tolerating a trailing slash on
/// the address and a leading slash on the suffix.
fn join_url(addr: &str, suffix: &str) -> String {
    format!(
        "{}/{}",
        addr.trim_end_matches('/'),
        suffix.trim_start_matches('/')
    )
}

fn login_url(addr: &str) -> String {
    join_url(addr, "v1/auth/approle/login")
}

fn kv_url(addr: &str, path: &str) -> String {
    join_url(addr, &format!("v1/{}", path.trim_start_matches('/')))
}

fn renew_url(addr: &str) -> String {
    join_url(addr, "v1/auth/token/renew-self")
}

fn revoke_url(addr: &str) -> String {
    join_url(addr, "v1/auth/token/revoke-self")
}

/// The AppRole login request body.
fn approle_login_body(role_id: &str, secret_id: &str) -> String {
    serde_json::json!({ "role_id": role_id, "secret_id": secret_id }).to_string()
}

/// Join a Vault `errors` array into one message.
fn join_errors(errs: &[Value]) -> String {
    errs.iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Extract a human-readable error from a Vault error body, falling back to a
/// truncated copy of the raw body.
fn first_error(body: &str) -> String {
    let from_errors = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| {
            v.get("errors")
                .and_then(Value::as_array)
                .map(|e| join_errors(e))
        })
        .filter(|s| !s.is_empty());
    from_errors.unwrap_or_else(|| body.chars().take(200).collect())
}

/// Render a KV field value as a string: strings verbatim, `null` as empty, and
/// any other JSON scalar/structure as its compact JSON encoding.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Parse a login response into a [`Session`]. A body without a non-empty
/// `auth.client_token` (or carrying a Vault `errors` array) is an error, so a
/// bad credential surfaces at the login rather than as a wall of 403s.
fn parse_login_response(body: &str) -> Result<Session, String> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| format!("vault login returned non-JSON body: {e}"))?;
    if let Some(errs) = v.get("errors").and_then(Value::as_array) {
        if !errs.is_empty() {
            return Err(format!("vault login rejected: {}", join_errors(errs)));
        }
    }
    let auth = v
        .get("auth")
        .ok_or_else(|| "vault login response missing `auth`".to_string())?;
    let token = auth
        .get("client_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "vault login response missing `auth.client_token`".to_string())?
        .to_string();
    let lease_duration = auth
        .get("lease_duration")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Ok(Session {
        token,
        lease_duration,
    })
}

/// Extract the KV secret fields from a read response. Handles KV v2 (fields
/// nested under `data.data`) and KV v1 (fields directly under `data`).
fn extract_kv_fields(body: &str) -> Result<BTreeMap<String, String>, String> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| format!("vault read returned non-JSON body: {e}"))?;
    if let Some(errs) = v.get("errors").and_then(Value::as_array) {
        if !errs.is_empty() {
            return Err(format!("vault read rejected: {}", join_errors(errs)));
        }
    }
    let data = v
        .get("data")
        .ok_or_else(|| "vault read response missing `data`".to_string())?;
    // KV v2 nests the secret under `data.data`; KV v1 puts it directly in `data`.
    let fields = match data.get("data") {
        Some(inner @ Value::Object(_)) => inner,
        _ => data,
    };
    let obj = fields
        .as_object()
        .ok_or_else(|| "vault read response `data` is not an object".to_string())?;
    Ok(obj
        .iter()
        .map(|(k, val)| (k.clone(), value_to_string(val)))
        .collect())
}

/// Parse a renew response into the new lease TTL in seconds.
fn parse_renew_response(body: &str) -> Result<u64, String> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| format!("vault renew returned non-JSON body: {e}"))?;
    if let Some(errs) = v.get("errors").and_then(Value::as_array) {
        if !errs.is_empty() {
            return Err(format!("vault renew rejected: {}", join_errors(errs)));
        }
    }
    Ok(v.get("auth")
        .and_then(|a| a.get("lease_duration"))
        .and_then(Value::as_u64)
        .unwrap_or(0))
}

/// When to renew next: half the lease TTL, floored at [`MIN_RENEW`].
fn renew_after(lease_secs: u64) -> Duration {
    Duration::from_secs(lease_secs / 2).max(MIN_RENEW)
}

// ---------------------------------------------------------------------------
// Vault API — a seam so the fetcher can be unit-tested without a socket.
// ---------------------------------------------------------------------------

/// The Vault operations `start()` / `stop()` / the renew loop need. An `Err` is
/// any failure that must fail the run (transport, timeout, non-2xx, bad body).
trait VaultApi: Send + Sync {
    /// Log in (or accept a static token) and return the session.
    fn login(&self, auth: &Auth) -> Result<Session, String>;
    /// Read the KV secret at `path`, returning its fields.
    fn read_kv(&self, token: &str, path: &str) -> Result<BTreeMap<String, String>, String>;
    /// Renew the token's lease, returning the new TTL in seconds.
    fn renew(&self, token: &str) -> Result<u64, String>;
    /// Revoke the token (best effort; failures are ignored).
    fn revoke(&self, token: &str);
}

/// The hyper HTTPS client (pure-Rust TLS, cheaply cloneable, internally pooled).
type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build vault-fetch plugin tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also lets the same connector reach a plaintext `addr` (e.g. a local dev
    // Vault in `-dev` mode).
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(tls)
}

fn client() -> &'static HttpClient {
    static CLIENT: OnceCell<HttpClient> = OnceCell::new();
    CLIENT.get_or_init(build_client)
}

/// Issue one Vault HTTP request and return `(status, body)`. `token` sets the
/// `X-Vault-Token` header, `namespace` the `X-Vault-Namespace` header.
async fn vault_http(
    client: &HttpClient,
    method: &str,
    url: &str,
    token: Option<&str>,
    namespace: Option<&str>,
    body: Option<Bytes>,
    timeout_ms: u64,
) -> Result<(u16, String), String> {
    let mut builder = Request::builder()
        .method(method)
        .uri(url)
        .header(ACCEPT, "application/json");
    if let Some(t) = token {
        builder = builder.header("X-Vault-Token", t);
    }
    if let Some(ns) = namespace {
        builder = builder.header("X-Vault-Namespace", ns);
    }
    let payload = match body {
        Some(b) => {
            builder = builder.header(CONTENT_TYPE, "application/json");
            Full::new(b)
        }
        None => Full::new(Bytes::new()),
    };
    let request = builder
        .body(payload)
        .map_err(|e| format!("building vault request failed: {e}"))?;

    let send = client.request(request);
    let resp = if timeout_ms == 0 {
        send.await
            .map_err(|e| format!("request to {url} failed: {e}"))?
    } else {
        tokio::time::timeout(Duration::from_millis(timeout_ms), send)
            .await
            .map_err(|_| format!("request to {url} timed out after {timeout_ms}ms"))?
            .map_err(|e| format!("request to {url} failed: {e}"))?
    };
    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("reading vault response failed: {e}"))?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

/// The real Vault API, over hyper.
struct HyperVault {
    addr: String,
    namespace: Option<String>,
    timeout_ms: u64,
}

impl HyperVault {
    fn ns(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
}

impl VaultApi for HyperVault {
    fn login(&self, auth: &Auth) -> Result<Session, String> {
        match auth {
            // A static token needs no round-trip; it has no lease we own.
            Auth::Token(t) => Ok(Session {
                token: t.clone(),
                lease_duration: 0,
            }),
            Auth::AppRole { role_id, secret_id } => {
                let body = Bytes::from(approle_login_body(role_id, secret_id));
                let (status, text) = runtime().block_on(vault_http(
                    client(),
                    "POST",
                    &login_url(&self.addr),
                    None,
                    self.ns(),
                    Some(body),
                    self.timeout_ms,
                ))?;
                if !(200..300).contains(&status) {
                    return Err(format!(
                        "vault approle login returned HTTP {status}: {}",
                        first_error(&text)
                    ));
                }
                parse_login_response(&text)
            }
        }
    }

    fn read_kv(&self, token: &str, path: &str) -> Result<BTreeMap<String, String>, String> {
        let (status, text) = runtime().block_on(vault_http(
            client(),
            "GET",
            &kv_url(&self.addr, path),
            Some(token),
            self.ns(),
            None,
            self.timeout_ms,
        ))?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "vault read of `{path}` returned HTTP {status}: {}",
                first_error(&text)
            ));
        }
        extract_kv_fields(&text)
    }

    fn renew(&self, token: &str) -> Result<u64, String> {
        let (status, text) = runtime().block_on(vault_http(
            client(),
            "POST",
            &renew_url(&self.addr),
            Some(token),
            self.ns(),
            None,
            self.timeout_ms,
        ))?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "vault token renew returned HTTP {status}: {}",
                first_error(&text)
            ));
        }
        parse_renew_response(&text)
    }

    fn revoke(&self, token: &str) {
        let _ = runtime().block_on(vault_http(
            client(),
            "POST",
            &revoke_url(&self.addr),
            Some(token),
            self.ns(),
            None,
            self.timeout_ms,
        ));
    }
}

/// Log in and read the KV secret in one shot. Fails fast on either step so a bad
/// credential or wrong path rejects the plan before any VU begins.
fn fetch_secrets(
    api: &dyn VaultApi,
    auth: &Auth,
    path: &str,
) -> Result<(Session, BTreeMap<String, String>), String> {
    let session = api.login(auth)?;
    let secrets = api.read_kv(&session.token, path)?;
    Ok((session, secrets))
}

// ---------------------------------------------------------------------------
// Renewer — keeps a renewable lease alive; counts successful renewals.
// ---------------------------------------------------------------------------

struct Renewer {
    api: Arc<dyn VaultApi>,
    token: String,
    /// One per successful lease renewal.
    renewals: Arc<AtomicU64>,
}

impl Renewer {
    /// Renew once; on success bump the counter and return when to renew next.
    fn renew_once(&self) -> Result<Duration, String> {
        let lease = self.api.renew(&self.token)?;
        self.renewals.fetch_add(1, Ordering::Relaxed);
        Ok(renew_after(lease))
    }
}

// ---------------------------------------------------------------------------
// Config parsing.
// ---------------------------------------------------------------------------

fn require_str(cfg: &Value, key: &str) -> Result<String, String> {
    cfg.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("config requires a non-empty `{key}` string"))
}

/// Parse a duration from a config value: a number of seconds, or a string like
/// `10s` / `5m` / `500ms` (a bare number of seconds is also accepted).
fn parse_duration_value(v: Option<&Value>) -> Option<Duration> {
    match v {
        Some(Value::Number(n)) => n.as_u64().map(Duration::from_secs),
        Some(Value::String(s)) => parse_duration_str(s),
        _ => None,
    }
}

fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit) = match s.find(|c: char| c.is_ascii_alphabetic()) {
        Some(i) => (s[..i].trim(), s[i..].trim()),
        None => (s, "s"),
    };
    let n: u64 = num.parse().ok()?;
    let secs = match unit {
        "ms" => return Some(Duration::from_millis(n)),
        "" | "s" | "sec" | "secs" => n,
        "m" | "min" | "mins" => n.checked_mul(60)?,
        "h" | "hr" | "hrs" => n.checked_mul(3600)?,
        _ => return None,
    };
    Some(Duration::from_secs(secs))
}

fn validate_http_url(raw: &str) -> Result<(), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid addr `{raw}`: {e}"))?;
    match url.scheme() {
        "http" | "https" => Ok(()),
        other => Err(format!("addr scheme must be http(s), got `{other}`")),
    }
}

/// Parse the `auth` block into exactly one method.
fn parse_auth(cfg: &Value) -> Result<Auth, String> {
    let auth = cfg
        .get("auth")
        .ok_or_else(|| "config requires an `auth` object".to_string())?;
    let token = auth
        .get("token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let approle = auth.get("approle");
    match (token, approle) {
        (Some(_), Some(_)) => Err("auth must set exactly one of `token` or `approle`".to_string()),
        (Some(t), None) => Ok(Auth::Token(t.to_string())),
        (None, Some(ar)) => {
            let role_id = require_str(ar, "role_id")
                .map_err(|_| "approle auth requires a non-empty `role_id`".to_string())?;
            let secret_id = require_str(ar, "secret_id")
                .map_err(|_| "approle auth requires a non-empty `secret_id`".to_string())?;
            Ok(Auth::AppRole { role_id, secret_id })
        }
        (None, None) => Err("auth requires either `token` or `approle`".to_string()),
    }
}

/// Everything `start()` needs, parsed from the config JSON.
#[derive(Debug)]
struct StartConfig {
    addr: String,
    path: String,
    namespace: Option<String>,
    auth: Auth,
    renew: bool,
    timeout_ms: u64,
    bind: String,
}

fn parse_config(config_json: &str) -> Result<StartConfig, String> {
    let cfg: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let addr = require_str(&cfg, "addr")?;
    validate_http_url(&addr)?;
    let path = require_str(&cfg, "path")?;
    let auth = parse_auth(&cfg)?;

    let namespace = cfg
        .get("namespace")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let renew = cfg.get("renew").and_then(Value::as_bool).unwrap_or(false);
    let timeout_ms = parse_duration_value(cfg.get("timeout"))
        .filter(|d| *d > Duration::ZERO)
        .unwrap_or(Duration::from_secs(10))
        .as_millis() as u64;
    let bind = cfg
        .get("bind")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1:0")
        .to_string();

    Ok(StartConfig {
        addr,
        path,
        namespace,
        auth,
        renew,
        timeout_ms,
        bind,
    })
}

// ---------------------------------------------------------------------------
// The service: a local line endpoint serving the fetched KV fields.
// ---------------------------------------------------------------------------

/// A running secret endpoint. Handed off to `stop()` for teardown.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    renew: Option<JoinHandle<()>>,
    addr: String,
    api: Arc<dyn VaultApi>,
    token: String,
    /// Revoke `token` on stop (only for a token we minted ourselves).
    revoke_on_stop: bool,
}

/// The service plugin instance.
#[derive(Default)]
struct VaultFetch {
    handle: Option<ServerHandle>,
}

impl VaultFetch {
    fn start_config(&mut self, cfg: StartConfig) -> Result<String, String> {
        let api: Arc<dyn VaultApi> = Arc::new(HyperVault {
            addr: cfg.addr.clone(),
            namespace: cfg.namespace.clone(),
            timeout_ms: cfg.timeout_ms,
        });
        self.start_with(cfg, api)
    }

    fn start_with(&mut self, cfg: StartConfig, api: Arc<dyn VaultApi>) -> Result<String, String> {
        // Log in and read the secret up front — fail fast on a bad credential
        // or wrong path before anything is bound.
        let (session, secrets) = fetch_secrets(api.as_ref(), &cfg.auth, &cfg.path)?;
        let cache = Arc::new(Mutex::new(secrets));

        let listener = TcpListener::bind(&cfg.bind)
            .map_err(|e| format!("cannot bind secret endpoint {}: {e}", cfg.bind))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let accept = spawn_accept_loop(listener, cache, shutdown.clone());

        // Only keep the lease alive when asked and when the login gave us a
        // renewable one (a static token has no lease we own).
        let renew = if cfg.renew && session.lease_duration > 0 {
            let renewer = Renewer {
                api: api.clone(),
                token: session.token.clone(),
                renewals: Arc::new(AtomicU64::new(0)),
            };
            let initial = renew_after(session.lease_duration);
            Some(spawn_renew_loop(renewer, initial, shutdown.clone()))
        } else {
            None
        };

        let revoke_on_stop = matches!(cfg.auth, Auth::AppRole { .. });
        self.handle = Some(ServerHandle {
            shutdown,
            accept: Some(accept),
            renew,
            addr: addr.clone(),
            api,
            token: session.token,
            revoke_on_stop,
        });
        Ok(addr)
    }
}

/// The background task: sleep until the next renewal, renew, repeat. Sleeps in
/// short slices so `stop()` is observed promptly.
fn spawn_renew_loop(
    renewer: Renewer,
    initial: Duration,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut wait = initial;
        loop {
            let mut slept = Duration::ZERO;
            while slept < wait {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                let step = Duration::from_millis(200).min(wait - slept);
                std::thread::sleep(step);
                slept += step;
            }
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            wait = renewer.renew_once().unwrap_or(RETRY_BACKOFF);
        }
    })
}

/// Accept loop: each VU connection is served on its own thread, all sharing the
/// single secret cache.
fn spawn_accept_loop(
    listener: TcpListener,
    cache: Arc<Mutex<BTreeMap<String, String>>>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let cache = cache.clone();
                    let shutdown = shutdown.clone();
                    std::thread::spawn(move || handle_client(stream, cache, shutdown));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    })
}

/// Serve one VU connection: for every request line (a field name), write back
/// that field's cached value plus a newline. `*` (or an empty line) returns the
/// whole secret as a JSON object; an unknown field returns an empty line.
fn handle_client(
    stream: TcpStream,
    cache: Arc<Mutex<BTreeMap<String, String>>>,
    shutdown: Arc<AtomicBool>,
) {
    let _ = stream.set_nonblocking(false);
    let mut reader = BufReader::new(match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut writer = stream;
    let mut request = String::new();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        request.clear();
        match reader.read_line(&mut request) {
            Ok(0) => return, // client closed
            Ok(_) => {}
            Err(_) => return,
        }
        let key = request.trim();
        let value = {
            let guard = cache.lock().unwrap_or_else(|p| p.into_inner());
            if key.is_empty() || key == "*" {
                serde_json::to_string(&*guard).unwrap_or_default()
            } else {
                guard.get(key).cloned().unwrap_or_default()
            }
        };
        // Guard the line protocol: a stray newline in a value would desync it.
        let value = value.replace(['\n', '\r'], " ");
        if writer.write_all(format!("{value}\n").as_bytes()).is_err() {
            return;
        }
        let _ = writer.flush();
    }
}

impl FfiService for VaultFetch {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if let Some(h) = self.handle.as_ref() {
            // Already running: return the existing address rather than re-fetch.
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
            if let Some(join) = handle.accept.take() {
                let _ = join.join();
            }
            if let Some(join) = handle.renew.take() {
                let _ = join.join();
            }
            if handle.revoke_on_stop {
                handle.api.revoke(&handle.token);
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
                "Fetches KV secrets from HashiCorp Vault at run start and serves them to VUs",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(VaultFetch::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline; Vault is reached through a scripted mock, never a real
// socket, and the wire parsing is covered by pure functions.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    /// A scripted in-memory Vault. Each op pops the next queued reply; `renew`
    /// falls back to a long lease so a waking renew loop never runs dry.
    #[derive(Default)]
    struct MockVault {
        login: Mutex<VecDeque<Result<Session, String>>>,
        kv: Mutex<VecDeque<Result<BTreeMap<String, String>, String>>>,
        renew: Mutex<VecDeque<Result<u64, String>>>,
        logins: AtomicUsize,
        reads: AtomicUsize,
        renews: AtomicUsize,
        revokes: AtomicUsize,
    }

    impl VaultApi for MockVault {
        fn login(&self, _auth: &Auth) -> Result<Session, String> {
            self.logins.fetch_add(1, Ordering::Relaxed);
            self.login
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("mock: no scripted login".to_string()))
        }

        fn read_kv(&self, _token: &str, _path: &str) -> Result<BTreeMap<String, String>, String> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.kv
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("mock: no scripted read".to_string()))
        }

        fn renew(&self, _token: &str) -> Result<u64, String> {
            self.renews.fetch_add(1, Ordering::Relaxed);
            self.renew.lock().unwrap().pop_front().unwrap_or(Ok(3600))
        }

        fn revoke(&self, _token: &str) {
            self.revokes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn session(token: &str, lease: u64) -> Session {
        Session {
            token: token.to_string(),
            lease_duration: lease,
        }
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A mock scripted with a single successful login + read.
    fn ready_mock(session: Session, secrets: BTreeMap<String, String>) -> Arc<MockVault> {
        Arc::new(MockVault {
            login: Mutex::new(VecDeque::from(vec![Ok(session)])),
            kv: Mutex::new(VecDeque::from(vec![Ok(secrets)])),
            ..Default::default()
        })
    }

    const TOKEN_CFG: &str =
        r#"{"addr":"https://vault:8200","path":"secret/data/app","auth":{"token":"tok"}}"#;
    const APPROLE_CFG: &str = r#"{"addr":"https://vault:8200","path":"secret/data/app","auth":{"approle":{"role_id":"r","secret_id":"s"}}}"#;

    // -- URL building --------------------------------------------------------

    #[test]
    fn builds_vault_urls_and_normalises_slashes() {
        assert_eq!(
            login_url("https://vault:8200"),
            "https://vault:8200/v1/auth/approle/login"
        );
        // Trailing slash on addr + leading slash tolerance on path.
        assert_eq!(
            kv_url("https://vault:8200/", "/secret/data/app"),
            "https://vault:8200/v1/secret/data/app"
        );
        assert_eq!(
            renew_url("https://vault:8200"),
            "https://vault:8200/v1/auth/token/renew-self"
        );
        assert_eq!(
            revoke_url("https://vault:8200"),
            "https://vault:8200/v1/auth/token/revoke-self"
        );
    }

    #[test]
    fn approle_body_carries_credentials() {
        let body = approle_login_body("role-1", "secret-1");
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["role_id"], "role-1");
        assert_eq!(v["secret_id"], "secret-1");
    }

    // -- login response parsing ---------------------------------------------

    #[test]
    fn parses_login_token_and_lease() {
        let s = parse_login_response(r#"{"auth":{"client_token":"s.abc","lease_duration":1800}}"#)
            .unwrap();
        assert_eq!(s.token, "s.abc");
        assert_eq!(s.lease_duration, 1800);
        // A missing lease_duration defaults to 0 (non-renewable).
        let s = parse_login_response(r#"{"auth":{"client_token":"s.def"}}"#).unwrap();
        assert_eq!(s.lease_duration, 0);
    }

    #[test]
    fn login_parse_rejects_errors_and_missing_token() {
        let err = parse_login_response(r#"{"errors":["permission denied"]}"#).unwrap_err();
        assert!(err.contains("permission denied"), "{err}");
        assert!(parse_login_response(r#"{"auth":{}}"#).is_err());
        assert!(parse_login_response(r#"{"auth":{"client_token":""}}"#).is_err());
        assert!(parse_login_response("not json").is_err());
    }

    // -- KV field extraction -------------------------------------------------

    #[test]
    fn extracts_kv_v2_nested_fields() {
        // KV v2: fields under data.data, alongside data.metadata.
        let body = r#"{"data":{"data":{"db_password":"hunter2","port":5432,"on":true},"metadata":{"version":3}}}"#;
        let fields = extract_kv_fields(body).unwrap();
        assert_eq!(fields.get("db_password").unwrap(), "hunter2");
        // Non-string scalars are rendered as their JSON encoding.
        assert_eq!(fields.get("port").unwrap(), "5432");
        assert_eq!(fields.get("on").unwrap(), "true");
        // metadata is not a field of the secret.
        assert!(!fields.contains_key("metadata"));
    }

    #[test]
    fn extracts_kv_v1_flat_fields() {
        let body = r#"{"data":{"api_token":"t-123"}}"#;
        let fields = extract_kv_fields(body).unwrap();
        assert_eq!(fields.get("api_token").unwrap(), "t-123");
    }

    #[test]
    fn kv_extraction_rejects_errors_and_bad_shape() {
        let err = extract_kv_fields(r#"{"errors":["no policy"]}"#).unwrap_err();
        assert!(err.contains("no policy"), "{err}");
        assert!(extract_kv_fields(r#"{}"#).is_err());
        assert!(extract_kv_fields(r#"{"data":42}"#).is_err());
        assert!(extract_kv_fields("not json").is_err());
    }

    #[test]
    fn value_to_string_renders_scalars() {
        assert_eq!(value_to_string(&serde_json::json!("s")), "s");
        assert_eq!(value_to_string(&serde_json::json!(7)), "7");
        assert_eq!(value_to_string(&serde_json::json!(true)), "true");
        assert_eq!(value_to_string(&Value::Null), "");
    }

    // -- renew parsing + scheduling -----------------------------------------

    #[test]
    fn parses_renew_lease_and_rejects_errors() {
        assert_eq!(
            parse_renew_response(r#"{"auth":{"lease_duration":3600}}"#).unwrap(),
            3600
        );
        assert_eq!(parse_renew_response(r#"{"auth":{}}"#).unwrap(), 0);
        assert!(parse_renew_response(r#"{"errors":["denied"]}"#).is_err());
    }

    #[test]
    fn renew_after_halves_and_floors() {
        assert_eq!(renew_after(3600), Duration::from_secs(1800));
        // Tiny leases never renew faster than MIN_RENEW.
        assert_eq!(renew_after(1), MIN_RENEW);
        assert_eq!(renew_after(0), MIN_RENEW);
    }

    // -- duration parsing ----------------------------------------------------

    #[test]
    fn parses_duration_forms() {
        assert_eq!(parse_duration_str("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration_str("5m"), Some(Duration::from_secs(300)));
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("45"), Some(Duration::from_secs(45)));
        assert_eq!(parse_duration_str("nope"), None);
        assert_eq!(
            parse_duration_value(Some(&serde_json::json!(30))),
            Some(Duration::from_secs(30))
        );
    }

    // -- auth parsing --------------------------------------------------------

    #[test]
    fn parses_token_and_approle_auth() {
        let token = parse_auth(&serde_json::json!({"auth":{"token":"t"}})).unwrap();
        assert_eq!(token, Auth::Token("t".to_string()));
        let approle =
            parse_auth(&serde_json::json!({"auth":{"approle":{"role_id":"r","secret_id":"s"}}}))
                .unwrap();
        assert_eq!(
            approle,
            Auth::AppRole {
                role_id: "r".to_string(),
                secret_id: "s".to_string()
            }
        );
    }

    #[test]
    fn auth_parse_rejects_conflicts_and_gaps() {
        // Both methods set.
        let both = parse_auth(
            &serde_json::json!({"auth":{"token":"t","approle":{"role_id":"r","secret_id":"s"}}}),
        )
        .unwrap_err();
        assert!(both.contains("exactly one"), "{both}");
        // Neither method set.
        assert!(parse_auth(&serde_json::json!({"auth":{}})).is_err());
        // Missing auth block entirely.
        assert!(parse_auth(&serde_json::json!({})).is_err());
        // AppRole missing secret_id.
        assert!(parse_auth(&serde_json::json!({"auth":{"approle":{"role_id":"r"}}})).is_err());
    }

    // -- config parsing ------------------------------------------------------

    #[test]
    fn config_requires_addr_path_auth() {
        let no_addr =
            parse_config(r#"{"path":"secret/data/app","auth":{"token":"t"}}"#).unwrap_err();
        assert!(no_addr.contains("addr"), "{no_addr}");
        let no_path =
            parse_config(r#"{"addr":"https://v:8200","auth":{"token":"t"}}"#).unwrap_err();
        assert!(no_path.contains("path"), "{no_path}");
        let no_auth =
            parse_config(r#"{"addr":"https://v:8200","path":"secret/data/app"}"#).unwrap_err();
        assert!(no_auth.contains("auth"), "{no_auth}");
    }

    #[test]
    fn config_rejects_non_http_addr() {
        let err = parse_config(
            r#"{"addr":"ftp://v:8200","path":"secret/data/app","auth":{"token":"t"}}"#,
        )
        .unwrap_err();
        assert!(err.contains("scheme"), "{err}");
    }

    #[test]
    fn config_defaults_and_overrides() {
        let cfg = parse_config(TOKEN_CFG).unwrap();
        assert_eq!(cfg.addr, "https://vault:8200");
        assert_eq!(cfg.path, "secret/data/app");
        assert_eq!(cfg.auth, Auth::Token("tok".to_string()));
        assert!(!cfg.renew);
        assert_eq!(cfg.timeout_ms, 10_000);
        assert_eq!(cfg.bind, "127.0.0.1:0");
        assert_eq!(cfg.namespace, None);

        let cfg = parse_config(
            r#"{"addr":"https://vault:8200","path":"secret/data/app","auth":{"approle":{"role_id":"r","secret_id":"s"}},"namespace":"team-a","renew":true,"timeout":"5s","bind":"127.0.0.1:7777"}"#,
        )
        .unwrap();
        assert_eq!(cfg.namespace.as_deref(), Some("team-a"));
        assert!(cfg.renew);
        assert_eq!(cfg.timeout_ms, 5_000);
        assert_eq!(cfg.bind, "127.0.0.1:7777");
    }

    // -- fetch_secrets (mock) ------------------------------------------------

    #[test]
    fn fetch_secrets_logs_in_then_reads() {
        let api = ready_mock(session("s.tok", 1800), map(&[("db", "hunter2")]));
        let (sess, secrets) = fetch_secrets(
            api.as_ref(),
            &Auth::Token("ignored".to_string()),
            "secret/data/app",
        )
        .unwrap();
        assert_eq!(sess.token, "s.tok");
        assert_eq!(secrets.get("db").unwrap(), "hunter2");
        assert_eq!(api.logins.load(Ordering::Relaxed), 1);
        assert_eq!(api.reads.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fetch_secrets_surfaces_login_and_read_errors() {
        // Login fails -> no read attempted.
        let api = Arc::new(MockVault {
            login: Mutex::new(VecDeque::from(vec![Err("bad creds".to_string())])),
            ..Default::default()
        });
        assert!(fetch_secrets(api.as_ref(), &Auth::Token("t".into()), "p").is_err());
        assert_eq!(api.reads.load(Ordering::Relaxed), 0);

        // Login ok, read fails.
        let api = Arc::new(MockVault {
            login: Mutex::new(VecDeque::from(vec![Ok(session("s", 0))])),
            kv: Mutex::new(VecDeque::from(vec![Err("no policy".to_string())])),
            ..Default::default()
        });
        assert!(fetch_secrets(api.as_ref(), &Auth::Token("t".into()), "p").is_err());
    }

    // -- renewer -------------------------------------------------------------

    #[test]
    fn renewer_counts_and_schedules() {
        let api = Arc::new(MockVault {
            renew: Mutex::new(VecDeque::from(vec![Ok(2000u64)])),
            ..Default::default()
        });
        let renewals = Arc::new(AtomicU64::new(0));
        let renewer = Renewer {
            api: api.clone(),
            token: "s.tok".to_string(),
            renewals: renewals.clone(),
        };
        let next = renewer.renew_once().unwrap();
        assert_eq!(next, Duration::from_secs(1000));
        assert_eq!(renewals.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn renewer_surfaces_error_without_counting() {
        let api = Arc::new(MockVault {
            renew: Mutex::new(VecDeque::from(vec![Err("lease expired".to_string())])),
            ..Default::default()
        });
        let renewals = Arc::new(AtomicU64::new(0));
        let renewer = Renewer {
            api,
            token: "s.tok".to_string(),
            renewals: renewals.clone(),
        };
        assert!(renewer.renew_once().is_err());
        assert_eq!(renewals.load(Ordering::Relaxed), 0);
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = VaultFetch::default();
        svc.stop();
        svc.stop();
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_rejects_bad_config() {
        let mut svc = VaultFetch::default();
        // Missing auth -> start fails before anything is fetched or bound.
        let res = svc.start(RString::from(
            r#"{"addr":"https://v:8200","path":"secret/data/app"}"#,
        ));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn start_with_fails_fast_on_login_error() {
        let cfg = parse_config(APPROLE_CFG).unwrap();
        let api = Arc::new(MockVault {
            login: Mutex::new(VecDeque::from(vec![Err("denied".to_string())])),
            ..Default::default()
        });
        let mut svc = VaultFetch::default();
        assert!(svc.start_with(cfg, api).is_err());
        assert!(svc.handle.is_none());
    }

    #[test]
    fn approle_start_binds_and_revokes_on_stop() {
        let cfg = parse_config(APPROLE_CFG).unwrap();
        // renew defaults to false, so no renew thread even with a lease.
        let api = ready_mock(
            session("s.minted", 3600),
            map(&[("db_password", "hunter2")]),
        );
        let mut svc = VaultFetch::default();
        let addr = svc
            .start_with(cfg, api.clone() as Arc<dyn VaultApi>)
            .expect("start");
        assert!(addr.starts_with("127.0.0.1:"), "{addr}");
        assert!(svc.handle.is_some());

        // A second start() returns the existing address without re-fetching.
        let again = svc.start(RString::from("{}"));
        assert!(matches!(again, ROk(_)));
        assert_eq!(api.logins.load(Ordering::Relaxed), 1);

        svc.stop();
        assert!(svc.handle.is_none());
        // An AppRole-minted token is revoked on the way out.
        assert_eq!(api.revokes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn token_auth_start_does_not_revoke() {
        let cfg = parse_config(TOKEN_CFG).unwrap();
        let api = ready_mock(session("caller-token", 0), map(&[("api_token", "t-123")]));
        let mut svc = VaultFetch::default();
        svc.start_with(cfg, api.clone() as Arc<dyn VaultApi>)
            .expect("start");
        svc.stop();
        // A caller-supplied static token is left untouched.
        assert_eq!(api.revokes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "vault-fetch");
    }
}
