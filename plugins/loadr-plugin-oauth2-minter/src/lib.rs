//! `loadr-plugin-oauth2-minter` — a native **service** plugin that mints, caches
//! and auto-refreshes an OAuth2 bearer token and hands the same live token to
//! every VU.
//!
//! # Why a service plugin
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> bound_addr` and an idempotent `stop()`. On `start`
//! this plugin:
//!
//! 1. runs an OAuth2 **client-credentials** (or **refresh-token**) grant against
//!    the token endpoint **once** — over the project's own hyper + hyper-rustls
//!    stack, so there is no OAuth SDK and no extra C dependency — failing fast if
//!    the URL is malformed or the credentials are rejected;
//! 2. caches the resulting bearer token and binds a tiny local line endpoint
//!    (`127.0.0.1:0` by default), returning its bound address.
//!
//! Every VU that opens that endpoint and reads a line gets the **current shared
//! token**. A background task re-mints the token at `expires_in − refresh_skew`,
//! so the fleet always attaches a valid `Authorization` header without any VU
//! ever making its own auth round-trip: the grant happens once, centrally, and
//! the result is shared — a token provider, not a per-request cost.
//!
//! # Tests
//!
//! The grant is reached through a [`TokenEndpoint`] seam, so the mint / cache /
//! refresh-scheduling logic is exercised entirely offline against a scripted
//! mock, never a real socket.

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
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
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

const NAME: &str = "oauth2-minter";

/// Never schedule a refresh sooner than this — guards against an endpoint that
/// reports a tiny (or skew-eclipsed) `expires_in` busy-looping the grant.
const MIN_REFRESH: Duration = Duration::from_secs(1);
/// After a failed grant, retry on this short interval rather than the (now
/// unknown) token lifetime.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Grant request + form encoding.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrantType {
    ClientCredentials,
    RefreshToken,
}

impl GrantType {
    fn as_str(self) -> &'static str {
        match self {
            GrantType::ClientCredentials => "client_credentials",
            GrantType::RefreshToken => "refresh_token",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthStyle {
    /// Credentials as form fields in the request body.
    Body,
    /// Credentials as an HTTP Basic `Authorization` header.
    Basic,
}

/// Everything a single grant needs.
#[derive(Debug, Clone)]
struct GrantRequest {
    grant_type: GrantType,
    client_id: String,
    client_secret: String,
    refresh_token: Option<String>,
    scope: Option<String>,
    audience: Option<String>,
    auth_style: AuthStyle,
}

/// Build the `application/x-www-form-urlencoded` request body and, for
/// `auth_style = basic`, the `Authorization` header value. In `basic` mode the
/// client id/secret ride the header and are kept out of the body.
fn build_form(grant: &GrantRequest) -> (String, Option<String>) {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    ser.append_pair("grant_type", grant.grant_type.as_str());
    if let Some(rt) = grant.refresh_token.as_deref() {
        ser.append_pair("refresh_token", rt);
    }
    if let Some(scope) = grant.scope.as_deref() {
        ser.append_pair("scope", scope);
    }
    if let Some(audience) = grant.audience.as_deref() {
        ser.append_pair("audience", audience);
    }
    let basic = match grant.auth_style {
        AuthStyle::Basic => {
            let raw = format!("{}:{}", grant.client_id, grant.client_secret);
            let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
            Some(format!("Basic {encoded}"))
        }
        AuthStyle::Body => {
            ser.append_pair("client_id", &grant.client_id);
            ser.append_pair("client_secret", &grant.client_secret);
            None
        }
    };
    (ser.finish(), basic)
}

// ---------------------------------------------------------------------------
// Token response parsing.
// ---------------------------------------------------------------------------

/// A minted access token and its (optional) lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    access_token: String,
    expires_in: Option<u64>,
}

/// Coerce an `expires_in` JSON value (number or numeric string) to seconds.
fn expires_in_secs(v: &Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(f) = v.as_f64() {
        if f.is_finite() && f > 0.0 {
            return Some(f as u64);
        }
    }
    if let Some(s) = v.as_str() {
        return s.trim().parse::<u64>().ok();
    }
    None
}

/// Parse a token-endpoint JSON body into a [`Token`]. A body without a
/// non-empty `access_token` is an error, so a malformed grant surfaces at the
/// mint rather than as a wall of 401s under load.
fn parse_token_response(body: &str) -> Result<Token, String> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| format!("token endpoint returned non-JSON body: {e}"))?;
    let access_token = v
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "token response missing `access_token`".to_string())?
        .to_string();
    let expires_in = v.get("expires_in").and_then(expires_in_secs);
    Ok(Token {
        access_token,
        expires_in,
    })
}

/// When to run the next grant: `expires_in` (or `default_ttl` when the endpoint
/// omits it) minus the skew, floored at [`MIN_REFRESH`].
fn refresh_after(expires_in: Option<u64>, skew: Duration, default_ttl: Duration) -> Duration {
    let ttl = expires_in.map(Duration::from_secs).unwrap_or(default_ttl);
    ttl.saturating_sub(skew).max(MIN_REFRESH)
}

// ---------------------------------------------------------------------------
// Token endpoint — a seam so the minter can be unit-tested without a socket.
// ---------------------------------------------------------------------------

/// Runs one grant against the token endpoint. An `Err` is any failure that must
/// be counted as a refresh error (transport, timeout, non-2xx, unparseable body).
trait TokenEndpoint: Send + Sync {
    fn fetch(&self, grant: &GrantRequest) -> Result<Token, String>;
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
            .expect("build oauth2-minter tokio runtime")
    })
}

fn build_client() -> HttpClient {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also lets the same connector reach a plaintext `token_url` (e.g. a local
    // identity provider in tests/dev).
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

/// POST the grant form and return `(status, body)`.
async fn post_token(
    client: &HttpClient,
    url: &str,
    body: Bytes,
    basic_auth: Option<String>,
    timeout_ms: u64,
) -> Result<(u16, String), String> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(url)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(ACCEPT, "application/json");
    if let Some(auth) = basic_auth {
        builder = builder.header(AUTHORIZATION, auth);
    }
    let request = builder
        .body(Full::new(body))
        .map_err(|e| format!("building token request failed: {e}"))?;

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
        .map_err(|e| format!("reading token response failed: {e}"))?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

/// The real token endpoint, over hyper.
struct HyperTokenEndpoint {
    url: String,
    timeout_ms: u64,
}

impl TokenEndpoint for HyperTokenEndpoint {
    fn fetch(&self, grant: &GrantRequest) -> Result<Token, String> {
        let (form, basic) = build_form(grant);
        let body = Bytes::from(form);
        let (status, text) = runtime().block_on(post_token(
            client(),
            &self.url,
            body,
            basic,
            self.timeout_ms,
        ))?;
        if !(200..300).contains(&status) {
            return Err(format!("token endpoint returned HTTP {status}"));
        }
        parse_token_response(&text)
    }
}

// ---------------------------------------------------------------------------
// Minter — mint, cache, and compute the next refresh delay.
// ---------------------------------------------------------------------------

/// Owns the grant and the shared token cache. `mint()` runs one grant, updates
/// the cache on success, and returns how long until the next refresh.
struct Minter {
    endpoint: Box<dyn TokenEndpoint>,
    grant: GrantRequest,
    skew: Duration,
    default_ttl: Duration,
    /// The current shared bearer token, read by every VU connection.
    cache: Arc<Mutex<String>>,
    /// One per successful grant (initial mint + every refresh).
    refreshes: AtomicU64,
    /// One per failed grant attempt.
    errors: AtomicU64,
}

impl Minter {
    fn new(
        endpoint: Box<dyn TokenEndpoint>,
        grant: GrantRequest,
        skew: Duration,
        default_ttl: Duration,
        cache: Arc<Mutex<String>>,
    ) -> Self {
        Minter {
            endpoint,
            grant,
            skew,
            default_ttl,
            cache,
            refreshes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Run one grant. On success the cache is updated and the returned duration
    /// is when to refresh next; on failure the error counter ticks and the
    /// error is surfaced.
    fn mint(&self) -> Result<Duration, String> {
        match self.endpoint.fetch(&self.grant) {
            Ok(token) => {
                {
                    let mut guard = self.cache.lock().unwrap_or_else(|p| p.into_inner());
                    *guard = token.access_token;
                }
                self.refreshes.fetch_add(1, Ordering::Relaxed);
                Ok(refresh_after(token.expires_in, self.skew, self.default_ttl))
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
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

fn opt_str(cfg: &Value, key: &str) -> Option<String> {
    cfg.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Parse a duration from a config value: a number of seconds, or a string like
/// `30s` / `5m` / `2h` / `500ms` (a bare number of seconds is also accepted).
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
    let url = Url::parse(raw).map_err(|e| format!("invalid token_url `{raw}`: {e}"))?;
    match url.scheme() {
        "http" | "https" => Ok(()),
        other => Err(format!("token_url scheme must be http(s), got `{other}`")),
    }
}

/// Everything `start()` needs, parsed from the config JSON.
#[derive(Debug)]
struct StartConfig {
    token_url: String,
    bind: String,
    timeout_ms: u64,
    refresh_skew: Duration,
    default_ttl: Duration,
    grant: GrantRequest,
}

fn parse_config(config_json: &str) -> Result<StartConfig, String> {
    let cfg: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let token_url = require_str(&cfg, "token_url")?;
    validate_http_url(&token_url)?;
    let client_id = require_str(&cfg, "client_id")?;
    let client_secret = require_str(&cfg, "client_secret")?;

    let grant_type = match cfg
        .get("grant_type")
        .and_then(Value::as_str)
        .unwrap_or("client_credentials")
    {
        "client_credentials" => GrantType::ClientCredentials,
        "refresh_token" => GrantType::RefreshToken,
        other => {
            return Err(format!(
                "unknown grant_type `{other}` (use `client_credentials` or `refresh_token`)"
            ))
        }
    };

    let refresh_token = opt_str(&cfg, "refresh_token");
    if grant_type == GrantType::RefreshToken && refresh_token.is_none() {
        return Err("grant_type `refresh_token` requires a non-empty `refresh_token`".to_string());
    }

    let auth_style = match cfg
        .get("auth_style")
        .and_then(Value::as_str)
        .unwrap_or("body")
    {
        "body" => AuthStyle::Body,
        "basic" => AuthStyle::Basic,
        other => {
            return Err(format!(
                "unknown auth_style `{other}` (use `body` or `basic`)"
            ))
        }
    };

    let bind = cfg
        .get("bind")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1:0")
        .to_string();
    let timeout_ms = cfg
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(10_000);
    let refresh_skew =
        parse_duration_value(cfg.get("refresh_skew")).unwrap_or(Duration::from_secs(30));
    let default_ttl = parse_duration_value(cfg.get("default_ttl"))
        .filter(|d| *d > Duration::ZERO)
        .unwrap_or(Duration::from_secs(3600));

    Ok(StartConfig {
        token_url,
        bind,
        timeout_ms,
        refresh_skew,
        default_ttl,
        grant: GrantRequest {
            grant_type,
            client_id,
            client_secret,
            refresh_token,
            scope: opt_str(&cfg, "scope"),
            audience: opt_str(&cfg, "audience"),
            auth_style,
        },
    })
}

// ---------------------------------------------------------------------------
// The service: a local line endpoint serving the shared, auto-refreshed token.
// ---------------------------------------------------------------------------

/// A running minter endpoint. Handed off to `stop()` for teardown.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    refresh: Option<JoinHandle<()>>,
    addr: String,
}

/// The service plugin instance.
#[derive(Default)]
struct OAuth2Minter {
    handle: Option<ServerHandle>,
}

impl OAuth2Minter {
    fn start_config(&mut self, cfg: StartConfig) -> Result<String, String> {
        let endpoint: Box<dyn TokenEndpoint> = Box::new(HyperTokenEndpoint {
            url: cfg.token_url.clone(),
            timeout_ms: cfg.timeout_ms,
        });
        self.start_with(cfg, endpoint)
    }

    fn start_with(
        &mut self,
        cfg: StartConfig,
        endpoint: Box<dyn TokenEndpoint>,
    ) -> Result<String, String> {
        let cache = Arc::new(Mutex::new(String::new()));
        let minter = Minter::new(
            endpoint,
            cfg.grant,
            cfg.refresh_skew,
            cfg.default_ttl,
            cache.clone(),
        );

        // Initial grant — fail fast so a bad token_url/credential rejects the
        // plan before any VU begins rather than surfacing mid-load.
        let next = minter.mint()?;

        let listener = TcpListener::bind(&cfg.bind)
            .map_err(|e| format!("cannot bind token endpoint {}: {e}", cfg.bind))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let accept = spawn_accept_loop(listener, cache, shutdown.clone());
        let refresh = spawn_refresh_loop(minter, next, shutdown.clone());

        self.handle = Some(ServerHandle {
            shutdown,
            accept: Some(accept),
            refresh: Some(refresh),
            addr: addr.clone(),
        });
        Ok(addr)
    }
}

/// The background task: sleep until the next refresh, re-mint, repeat. Sleeps in
/// short slices so `stop()` is observed promptly.
fn spawn_refresh_loop(
    minter: Minter,
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
            wait = match minter.mint() {
                Ok(next) => next,
                // Keep the last good token in the cache and retry soon.
                Err(_) => RETRY_BACKOFF,
            };
        }
    })
}

/// Accept loop: each VU connection is served on its own thread, all sharing the
/// single token cache.
fn spawn_accept_loop(
    listener: TcpListener,
    cache: Arc<Mutex<String>>,
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

/// Serve one VU connection: for every request line, write back the current
/// shared token plus a newline. The client drives the pace.
fn handle_client(stream: TcpStream, cache: Arc<Mutex<String>>, shutdown: Arc<AtomicBool>) {
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
        let token = {
            let guard = cache.lock().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        };
        // Guard the line protocol: a stray newline in a token would desync it.
        let token = token.replace(['\n', '\r'], "");
        if writer.write_all(format!("{token}\n").as_bytes()).is_err() {
            return;
        }
        let _ = writer.flush();
    }
}

impl FfiService for OAuth2Minter {
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
            if let Some(join) = handle.accept.take() {
                let _ = join.join();
            }
            if let Some(join) = handle.refresh.take() {
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
                "Mints, caches and auto-refreshes an OAuth2 bearer token shared by every VU",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(OAuth2Minter::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline; the grant is reached through a scripted mock endpoint,
// never a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    /// A scripted in-memory token endpoint. Each `fetch()` returns the next
    /// queued reply, falling back to `default_reply` once the queue drains.
    struct MockEndpoint {
        scripted: Mutex<VecDeque<Result<Token, String>>>,
        default_reply: Result<Token, String>,
        calls: AtomicUsize,
    }

    impl MockEndpoint {
        fn scripted(replies: Vec<Result<Token, String>>) -> Self {
            MockEndpoint {
                scripted: Mutex::new(VecDeque::from(replies)),
                default_reply: Err("mock: no more scripted replies".to_string()),
                calls: AtomicUsize::new(0),
            }
        }

        /// Always returns the same token — used where the refresh thread might
        /// wake during a test and must not run dry.
        fn always(token: Token) -> Self {
            MockEndpoint {
                scripted: Mutex::new(VecDeque::new()),
                default_reply: Ok(token),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl TokenEndpoint for MockEndpoint {
        fn fetch(&self, _grant: &GrantRequest) -> Result<Token, String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut q = self.scripted.lock().unwrap();
            q.pop_front().unwrap_or_else(|| self.default_reply.clone())
        }
    }

    fn token(access: &str, expires_in: Option<u64>) -> Token {
        Token {
            access_token: access.to_string(),
            expires_in,
        }
    }

    fn grant() -> GrantRequest {
        GrantRequest {
            grant_type: GrantType::ClientCredentials,
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: None,
            scope: None,
            audience: None,
            auth_style: AuthStyle::Body,
        }
    }

    // -- form encoding -------------------------------------------------------

    #[test]
    fn body_style_puts_credentials_in_form() {
        let mut g = grant();
        g.scope = Some("api.read api.write".to_string());
        g.audience = Some("aud".to_string());
        let (body, basic) = build_form(&g);
        assert!(basic.is_none(), "body style must not set a Basic header");
        assert!(body.contains("grant_type=client_credentials"), "{body}");
        assert!(body.contains("client_id=id"), "{body}");
        assert!(body.contains("client_secret=secret"), "{body}");
        // Spaces in scope are form-encoded.
        assert!(body.contains("scope=api.read+api.write"), "{body}");
        assert!(body.contains("audience=aud"), "{body}");
    }

    #[test]
    fn basic_style_sets_header_and_omits_credentials_from_body() {
        let mut g = grant();
        g.auth_style = AuthStyle::Basic;
        let (body, basic) = build_form(&g);
        let header = basic.expect("basic header");
        // base64("id:secret") == "aWQ6c2VjcmV0"
        assert_eq!(header, "Basic aWQ6c2VjcmV0");
        assert!(!body.contains("client_secret"), "{body}");
        assert!(!body.contains("client_id"), "{body}");
        assert!(body.contains("grant_type=client_credentials"), "{body}");
    }

    #[test]
    fn refresh_grant_includes_refresh_token() {
        let mut g = grant();
        g.grant_type = GrantType::RefreshToken;
        g.refresh_token = Some("rt-123".to_string());
        let (body, _) = build_form(&g);
        assert!(body.contains("grant_type=refresh_token"), "{body}");
        assert!(body.contains("refresh_token=rt-123"), "{body}");
    }

    // -- token response parsing ---------------------------------------------

    #[test]
    fn parses_token_with_expiry() {
        let t = parse_token_response(r#"{"access_token":"abc","expires_in":3600}"#).unwrap();
        assert_eq!(t.access_token, "abc");
        assert_eq!(t.expires_in, Some(3600));
    }

    #[test]
    fn parses_string_expiry_and_missing_expiry() {
        let t = parse_token_response(r#"{"access_token":"abc","expires_in":"120"}"#).unwrap();
        assert_eq!(t.expires_in, Some(120));
        let t = parse_token_response(r#"{"access_token":"abc"}"#).unwrap();
        assert_eq!(t.expires_in, None);
    }

    #[test]
    fn rejects_missing_access_token_and_bad_json() {
        assert!(parse_token_response(r#"{"expires_in":60}"#).is_err());
        assert!(parse_token_response(r#"{"access_token":""}"#).is_err());
        assert!(parse_token_response("not json").is_err());
    }

    #[test]
    fn expires_in_secs_coerces_numbers_and_strings() {
        assert_eq!(expires_in_secs(&serde_json::json!(3600)), Some(3600));
        assert_eq!(expires_in_secs(&serde_json::json!(60.9)), Some(60));
        assert_eq!(expires_in_secs(&serde_json::json!("300")), Some(300));
        assert_eq!(expires_in_secs(&serde_json::json!("nope")), None);
        assert_eq!(expires_in_secs(&serde_json::json!(-5)), None);
    }

    // -- refresh scheduling --------------------------------------------------

    #[test]
    fn refresh_after_subtracts_skew_and_floors() {
        // 3600s lifetime, 30s skew -> 3570s.
        assert_eq!(
            refresh_after(
                Some(3600),
                Duration::from_secs(30),
                Duration::from_secs(1000)
            ),
            Duration::from_secs(3570)
        );
        // Skew larger than the lifetime is floored at MIN_REFRESH, not zero.
        assert_eq!(
            refresh_after(Some(10), Duration::from_secs(30), Duration::from_secs(1000)),
            MIN_REFRESH
        );
        // No expires_in -> fall back to default_ttl (minus skew).
        assert_eq!(
            refresh_after(None, Duration::from_secs(30), Duration::from_secs(3600)),
            Duration::from_secs(3570)
        );
    }

    // -- duration parsing ----------------------------------------------------

    #[test]
    fn parses_duration_forms() {
        assert_eq!(parse_duration_str("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration_str("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration_str("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("45"), Some(Duration::from_secs(45)));
        assert_eq!(parse_duration_str("nonsense"), None);
        assert_eq!(
            parse_duration_value(Some(&serde_json::json!(90))),
            Some(Duration::from_secs(90))
        );
    }

    // -- config parsing ------------------------------------------------------

    fn base_cfg() -> serde_json::Value {
        serde_json::json!({
            "token_url": "https://id.example.com/oauth2/token",
            "client_id": "cid",
            "client_secret": "csecret"
        })
    }

    #[test]
    fn config_requires_core_fields() {
        let no_url = parse_config(r#"{"client_id":"a","client_secret":"b"}"#).unwrap_err();
        assert!(no_url.contains("token_url"), "{no_url}");
        let no_id = parse_config(r#"{"token_url":"https://x/","client_secret":"b"}"#).unwrap_err();
        assert!(no_id.contains("client_id"), "{no_id}");
        let no_secret = parse_config(r#"{"token_url":"https://x/","client_id":"a"}"#).unwrap_err();
        assert!(no_secret.contains("client_secret"), "{no_secret}");
    }

    #[test]
    fn config_rejects_non_http_url() {
        let mut cfg = base_cfg();
        cfg["token_url"] = serde_json::json!("ftp://id.example.com/token");
        let err = parse_config(&cfg.to_string()).unwrap_err();
        assert!(err.contains("scheme"), "{err}");
    }

    #[test]
    fn config_refresh_grant_requires_refresh_token() {
        let mut cfg = base_cfg();
        cfg["grant_type"] = serde_json::json!("refresh_token");
        let err = parse_config(&cfg.to_string()).unwrap_err();
        assert!(err.contains("refresh_token"), "{err}");

        cfg["refresh_token"] = serde_json::json!("rt");
        let parsed = parse_config(&cfg.to_string()).unwrap();
        assert_eq!(parsed.grant.grant_type, GrantType::RefreshToken);
        assert_eq!(parsed.grant.refresh_token.as_deref(), Some("rt"));
    }

    #[test]
    fn config_rejects_unknown_enums() {
        let mut cfg = base_cfg();
        cfg["grant_type"] = serde_json::json!("password");
        assert!(parse_config(&cfg.to_string())
            .unwrap_err()
            .contains("grant_type"));

        let mut cfg = base_cfg();
        cfg["auth_style"] = serde_json::json!("query");
        assert!(parse_config(&cfg.to_string())
            .unwrap_err()
            .contains("auth_style"));
    }

    #[test]
    fn config_defaults_and_overrides() {
        let parsed = parse_config(&base_cfg().to_string()).unwrap();
        assert_eq!(parsed.bind, "127.0.0.1:0");
        assert_eq!(parsed.timeout_ms, 10_000);
        assert_eq!(parsed.refresh_skew, Duration::from_secs(30));
        assert_eq!(parsed.default_ttl, Duration::from_secs(3600));
        assert_eq!(parsed.grant.grant_type, GrantType::ClientCredentials);
        assert_eq!(parsed.grant.auth_style, AuthStyle::Body);

        let mut cfg = base_cfg();
        cfg["bind"] = serde_json::json!("127.0.0.1:9999");
        cfg["timeout_ms"] = serde_json::json!(2500);
        cfg["refresh_skew"] = serde_json::json!("1m");
        cfg["default_ttl"] = serde_json::json!(600);
        cfg["auth_style"] = serde_json::json!("basic");
        cfg["scope"] = serde_json::json!("read");
        cfg["audience"] = serde_json::json!("aud");
        let parsed = parse_config(&cfg.to_string()).unwrap();
        assert_eq!(parsed.bind, "127.0.0.1:9999");
        assert_eq!(parsed.timeout_ms, 2500);
        assert_eq!(parsed.refresh_skew, Duration::from_secs(60));
        assert_eq!(parsed.default_ttl, Duration::from_secs(600));
        assert_eq!(parsed.grant.auth_style, AuthStyle::Basic);
        assert_eq!(parsed.grant.scope.as_deref(), Some("read"));
        assert_eq!(parsed.grant.audience.as_deref(), Some("aud"));
    }

    // -- minter: mint / cache / counters ------------------------------------

    #[test]
    fn mint_updates_cache_and_counts_refresh() {
        let cache = Arc::new(Mutex::new(String::new()));
        let endpoint = Box::new(MockEndpoint::scripted(vec![Ok(token("tok-1", Some(3600)))]));
        let minter = Minter::new(
            endpoint,
            grant(),
            Duration::from_secs(30),
            Duration::from_secs(3600),
            cache.clone(),
        );
        let next = minter.mint().unwrap();
        assert_eq!(next, Duration::from_secs(3570));
        assert_eq!(*cache.lock().unwrap(), "tok-1");
        assert_eq!(minter.refreshes.load(Ordering::Relaxed), 1);
        assert_eq!(minter.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn mint_error_counts_and_keeps_cache() {
        let cache = Arc::new(Mutex::new("stale".to_string()));
        let endpoint = Box::new(MockEndpoint::scripted(vec![Err("boom".to_string())]));
        let minter = Minter::new(
            endpoint,
            grant(),
            Duration::from_secs(30),
            Duration::from_secs(3600),
            cache.clone(),
        );
        assert!(minter.mint().is_err());
        // The last good token is untouched by a failed refresh.
        assert_eq!(*cache.lock().unwrap(), "stale");
        assert_eq!(minter.refreshes.load(Ordering::Relaxed), 0);
        assert_eq!(minter.errors.load(Ordering::Relaxed), 1);
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = OAuth2Minter::default();
        svc.stop();
        svc.stop();
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_rejects_bad_config() {
        let mut svc = OAuth2Minter::default();
        // Missing client_id/secret -> start fails before any grant is attempted.
        let res = svc.start(RString::from(
            r#"{"token_url":"https://id.example.com/token"}"#,
        ));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn start_with_mock_binds_and_fails_fast_on_grant_error() {
        // Initial grant fails -> start_with surfaces the error, binds nothing.
        let cfg = parse_config(&base_cfg().to_string()).unwrap();
        let mut svc = OAuth2Minter::default();
        let endpoint = Box::new(MockEndpoint::scripted(vec![Err("bad creds".to_string())]));
        let res = svc.start_with(cfg, endpoint);
        assert!(res.is_err());
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_with_mock_serves_the_shared_token() {
        // A successful initial grant binds the endpoint; the cache holds the
        // token and stop() tears everything down. The `always` mock keeps the
        // refresh thread fed if it wakes during the test.
        let mut cfg = parse_config(&base_cfg().to_string()).unwrap();
        // Long skew/ttl so the refresh thread stays asleep for the test.
        cfg.refresh_skew = Duration::from_secs(1);
        let endpoint = Box::new(MockEndpoint::always(token("shared-token", Some(3600))));
        let mut svc = OAuth2Minter::default();
        let addr = svc.start_with(cfg, endpoint).expect("start");
        assert!(addr.starts_with("127.0.0.1:"), "{addr}");
        assert!(svc.handle.is_some());

        // A second start() returns the existing address without rebinding.
        let again = svc.start(RString::from("{}"));
        assert!(matches!(again, ROk(_)));

        svc.stop();
        assert!(svc.handle.is_none());
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "oauth2-minter");
    }
}
