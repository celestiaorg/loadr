//! HTTP/1.1 + HTTP/2 protocol handler built directly on hyper's low-level
//! connection API so every request phase (DNS, connect, TLS, send, TTFB,
//! receive) is measured individually.

use std::collections::HashMap;
use std::io::Read as _;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::header::{
    HeaderName, HeaderValue, ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, COOKIE,
    HOST, SET_COOKIE, USER_AGENT,
};
use http_body_util::BodyExt as _;
use hyper::body::{Body, Frame, SizeHint};
use hyper_util::rt::{TokioExecutor, TokioIo};
use loadr_config::{HttpDefaults, HttpVersionPref};
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, ProtocolResponse, Timings};
use loadr_core::vu::VuContext;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use url::Url;

use crate::net::{host_port, ms_since, resolve, IoStream};

/// Default User-Agent header.
pub const DEFAULT_USER_AGENT: &str = "loadr/0.1";

// ---------------------------------------------------------------------------
// Timed request body
// ---------------------------------------------------------------------------

pin_project_lite::pin_project! {
    /// A `Full`-like body that records when hyper pulled the final chunk, so
    /// `sending_ms` (request write) can be separated from `waiting_ms` (TTFB).
    struct TimedBody {
        #[pin]
        inner: http_body_util::Full<Bytes>,
        sent_at: Arc<OnceLock<Instant>>,
    }
}

impl TimedBody {
    fn new(data: Bytes) -> (Self, Arc<OnceLock<Instant>>) {
        let sent_at = Arc::new(OnceLock::new());
        (
            TimedBody {
                inner: http_body_util::Full::new(data),
                sent_at: sent_at.clone(),
            },
            sent_at,
        )
    }
}

impl Body for TimedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.project();
        let res = this.inner.poll_frame(cx);
        if res.is_ready() {
            // `Full` yields at most one frame, so any Ready means the body has
            // been handed off to hyper's write buffer.
            let _ = this.sent_at.set(Instant::now());
        }
        res
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

// ---------------------------------------------------------------------------
// Per-VU connection pool
// ---------------------------------------------------------------------------

type PoolKey = (String, String, u16);

/// `(request, body-sent timestamp cell, approximate request size in bytes)`.
type BuiltRequest = (http::Request<TimedBody>, Arc<OnceLock<Instant>>, u64);

enum PooledSender {
    H1(hyper::client::conn::http1::SendRequest<TimedBody>),
    H2(hyper::client::conn::http2::SendRequest<TimedBody>),
}

impl PooledSender {
    fn is_closed(&self) -> bool {
        match self {
            PooledSender::H1(s) => s.is_closed(),
            PooledSender::H2(s) => s.is_closed(),
        }
    }

    async fn ready(&mut self) -> Result<(), hyper::Error> {
        match self {
            PooledSender::H1(s) => s.ready().await,
            PooledSender::H2(s) => s.ready().await,
        }
    }

    fn is_h2(&self) -> bool {
        matches!(self, PooledSender::H2(_))
    }

    async fn send(
        &mut self,
        req: http::Request<TimedBody>,
    ) -> Result<http::Response<hyper::body::Incoming>, hyper::Error> {
        match self {
            PooledSender::H1(s) => s.send_request(req).await,
            PooledSender::H2(s) => s.send_request(req).await,
        }
    }
}

/// Per-VU connection pool keyed by `(scheme, host, port)`, stored in
/// `VuContext::extensions`.
#[derive(Default)]
struct HttpPool {
    conns: HashMap<PoolKey, PooledSender>,
}

/// Per-VU HTTP cache (JMeter HTTP Cache Manager). Stores cacheable GET
/// responses and serves fresh ones without a network round trip; revalidates
/// stale-but-validatable ones with `If-None-Match` / `If-Modified-Since`.
#[derive(Default)]
struct HttpCache {
    entries: HashMap<String, CacheEntry>,
}

struct CacheEntry {
    status: i64,
    status_text: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    bytes_received: u64,
    protocol_version: String,
    stored_at: Instant,
    max_age: std::time::Duration,
    etag: Option<String>,
    last_modified: Option<String>,
}

impl CacheEntry {
    fn is_fresh(&self) -> bool {
        self.stored_at.elapsed() < self.max_age
    }

    fn to_response(&self, url: &str, cache_state: &str) -> ProtocolResponse {
        ProtocolResponse {
            status: self.status,
            status_text: self.status_text.clone(),
            headers: self.headers.clone(),
            body: self.body.clone(),
            timings: Timings::default(),
            bytes_sent: 0,
            bytes_received: self.bytes_received,
            protocol_version: self.protocol_version.clone(),
            error: None,
            url: url.to_string(),
            extras: serde_json::json!({ "cache": cache_state }),
            grpc_protobuf_outcomes: Vec::new(),
        }
    }
}

/// Parse `Cache-Control: max-age=N` (seconds) from response headers; returns
/// `None` when the response must not be cached.
fn cache_max_age(headers: &[(String, String)]) -> Option<std::time::Duration> {
    let cc = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("cache-control"))
        .map(|(_, v)| v.to_ascii_lowercase());
    if let Some(cc) = &cc {
        if cc.contains("no-store") || cc.contains("private") {
            return None;
        }
        for part in cc.split(',') {
            let part = part.trim();
            if let Some(secs) = part.strip_prefix("max-age=") {
                if let Ok(n) = secs.parse::<u64>() {
                    return Some(std::time::Duration::from_secs(n));
                }
            }
        }
    }
    None
}

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// HTTP/1.1 + HTTP/2 handler with hand-rolled connections for phase timings,
/// per-VU keep-alive pooling, compression, redirects, cookies and proxies.
pub struct HttpHandler {
    version: HttpVersionPref,
    compression: bool,
    keep_alive: bool,
    proxy: Option<Url>,
    tls: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
    /// Drop response bodies after reading (keep byte counts).
    discard_bodies: bool,
    /// Inject a W3C `traceparent` header on every request.
    tracing: bool,
    /// Hostname → address overrides (`host`/`host:port` → `ip`/`ip:port`).
    hosts: std::collections::HashMap<String, String>,
    /// Simulate a per-VU HTTP cache (JMeter HTTP Cache Manager).
    cache: bool,
    /// Dump full requests/responses (set by `--http-debug`).
    http_debug: bool,
}

impl HttpHandler {
    /// Build the handler (and its rustls config) once from the test defaults.
    /// `base_dir` resolves relative TLS file paths.
    pub fn new(defaults: &HttpDefaults, base_dir: &std::path::Path) -> Result<Self, ProtocolError> {
        let alpn: Vec<Vec<u8>> = match defaults.version {
            HttpVersionPref::Auto => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
            HttpVersionPref::Http1 => vec![b"http/1.1".to_vec()],
            HttpVersionPref::Http2 | HttpVersionPref::Http2PriorKnowledge => vec![b"h2".to_vec()],
        };
        let tls = crate::tls::client_config(&defaults.tls, base_dir, alpn)?;
        let proxy = match &defaults.proxy {
            Some(p) => Some(Url::parse(p).map_err(|e| {
                ProtocolError::InvalidRequest(format!("invalid proxy url `{p}`: {e}"))
            })?),
            None => None,
        };
        Ok(HttpHandler {
            version: defaults.version,
            compression: defaults.compression,
            keep_alive: defaults.keep_alive,
            proxy,
            tls: Arc::new(tls),
            server_name: defaults.tls.server_name.clone(),
            discard_bodies: defaults.discard_response_bodies,
            tracing: defaults.tracing,
            hosts: defaults
                .hosts
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                .collect(),
            cache: defaults.cache,
            http_debug: std::env::var_os("LOADR_HTTP_DEBUG").is_some(),
        })
    }

    /// Apply a `hosts` override to a (host, port), returning the address to dial.
    fn override_host(&self, host: &str, port: u16) -> Option<String> {
        if self.hosts.is_empty() {
            return None;
        }
        let hp = format!("{}:{port}", host.to_ascii_lowercase());
        if let Some(mapped) = self.hosts.get(&hp) {
            return Some(if mapped.contains(':') {
                mapped.clone()
            } else {
                format!("{mapped}:{port}")
            });
        }
        self.hosts.get(&host.to_ascii_lowercase()).map(|mapped| {
            if mapped.contains(':') {
                mapped.clone()
            } else {
                format!("{mapped}:{port}")
            }
        })
    }

    /// Establish a new connection: DNS → TCP → (CONNECT tunnel) → (TLS) →
    /// hyper handshake. Fills `timings` phase-by-phase. Errors are transport
    /// failures reported as strings (they become `ProtocolResponse::error`).
    async fn connect(&self, url: &Url, timings: &mut Timings) -> Result<PooledSender, String> {
        let https = url.scheme() == "https";
        let (target_host, target_port) = host_port(url)?;
        let via_proxy = self.proxy.is_some();

        // Where to dial: the proxy when configured, otherwise the target.
        let dial_url = match &self.proxy {
            Some(p) => p.clone(),
            None => url.clone(),
        };
        let dial_host = dial_url
            .host()
            .ok_or_else(|| format!("url `{dial_url}` has no host"))?;
        let dial_port = dial_url
            .port_or_known_default()
            .ok_or_else(|| format!("url `{dial_url}` has no port"))?;

        // `hosts` override: resolve to a fixed address, bypassing DNS.
        let addr = match self.override_host(&dial_host.to_string(), dial_port) {
            Some(mapped) => {
                let start = Instant::now();
                let addr = tokio::net::lookup_host(&mapped)
                    .await
                    .map_err(|e| format!("hosts override `{mapped}` is invalid: {e}"))?
                    .next()
                    .ok_or_else(|| format!("hosts override `{mapped}` resolved to nothing"))?;
                timings.dns_ms = ms_since(start);
                addr
            }
            None => resolve(&dial_host, dial_port, timings).await?,
        };

        let start = Instant::now();
        let mut tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connection to {addr} failed: {e}"))?;
        let _ = tcp.set_nodelay(true);
        timings.connect_ms = ms_since(start);

        if via_proxy && https {
            let start = Instant::now();
            proxy_tunnel(&mut tcp, &target_host, target_port).await?;
            timings.connect_ms += ms_since(start);
        }

        let use_h2;
        let stream: Box<dyn IoStream> = if https {
            let start = Instant::now();
            let connector = tokio_rustls::TlsConnector::from(self.tls.clone());
            let sni = crate::tls::server_name(self.server_name.as_deref(), url)
                .map_err(|e| e.to_string())?;
            let tls = connector.connect(sni, tcp).await.map_err(|e| {
                format!("tls handshake with {target_host}:{target_port} failed: {e}")
            })?;
            timings.tls_ms = ms_since(start);
            use_h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
            Box::new(tls)
        } else {
            // Plaintext: HTTP/2 only with prior knowledge (and never to a proxy,
            // which speaks HTTP/1.1 absolute-form).
            use_h2 = self.version == HttpVersionPref::Http2PriorKnowledge && !via_proxy;
            Box::new(tcp)
        };

        let io = TokioIo::new(stream);
        let start = Instant::now();
        let sender = if use_h2 {
            let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
                .await
                .map_err(|e| format!("h2 handshake failed: {e}"))?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::debug!(error = %e, "h2 connection ended with error");
                }
            });
            PooledSender::H2(sender)
        } else {
            let (sender, conn) = hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| format!("http handshake failed: {e}"))?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::debug!(error = %e, "http/1.1 connection ended with error");
                }
            });
            PooledSender::H1(sender)
        };
        timings.connect_ms += ms_since(start);
        Ok(sender)
    }

    /// Build the `http::Request` for one hop, returning the request, the
    /// cell recording when the body was handed to hyper, and the approximate
    /// request size in bytes.
    fn build_request(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
        url: &Url,
        method: &http::Method,
        body: Bytes,
        h2: bool,
    ) -> Result<BuiltRequest, ProtocolError> {
        let absolute_form = h2 || (self.proxy.is_some() && url.scheme() == "http");
        let uri: http::Uri = if absolute_form {
            url.as_str()
                .parse()
                .map_err(|e| ProtocolError::InvalidRequest(format!("invalid url `{url}`: {e}")))?
        } else {
            let mut pq = url.path().to_string();
            if let Some(q) = url.query() {
                pq.push('?');
                pq.push_str(q);
            }
            pq.parse()
                .map_err(|e| ProtocolError::InvalidRequest(format!("invalid path `{pq}`: {e}")))?
        };

        let body_len = body.len();
        let (timed_body, sent_at) = TimedBody::new(body);
        let mut builder = http::Request::builder().method(method.clone()).uri(uri);
        let headers = builder.headers_mut().ok_or_else(|| {
            ProtocolError::InvalidRequest("could not build request headers".to_string())
        })?;

        for (name, value) in &request.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                ProtocolError::InvalidRequest(format!("invalid header name `{name}`: {e}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|e| {
                ProtocolError::InvalidRequest(format!("invalid value for header `{name}`: {e}"))
            })?;
            headers.append(name, value);
        }

        if !h2 && !headers.contains_key(HOST) {
            let host = host_header_value(url);
            headers.insert(
                HOST,
                HeaderValue::from_str(&host).map_err(|e| {
                    ProtocolError::InvalidRequest(format!("invalid host `{host}`: {e}"))
                })?,
            );
        }
        if !headers.contains_key(USER_AGENT) {
            headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        }
        if self.compression && !headers.contains_key(ACCEPT_ENCODING) {
            headers.insert(
                ACCEPT_ENCODING,
                HeaderValue::from_static("gzip, deflate, br"),
            );
        }
        if body_len > 0 && !headers.contains_key(CONTENT_LENGTH) {
            headers.insert(CONTENT_LENGTH, HeaderValue::from(body_len));
        }
        if !h2 && !self.keep_alive {
            headers.insert(CONNECTION, HeaderValue::from_static("close"));
        }
        if ctx.cookies.auto {
            if let Some(cookie_header) = ctx.cookies.header_for(url) {
                let merged = match headers.get(COOKIE).and_then(|v| v.to_str().ok()) {
                    Some(existing) => format!("{existing}; {cookie_header}"),
                    None => cookie_header,
                };
                if let Ok(value) = HeaderValue::from_str(&merged) {
                    headers.insert(COOKIE, value);
                }
            }
        }
        // W3C trace context propagation (like k6's tracing).
        if self.tracing && !headers.contains_key("traceparent") {
            let mut seed = ctx.vu_id;
            let traceparent = make_traceparent(&mut seed);
            if let Ok(value) = HeaderValue::from_str(&traceparent) {
                if let Ok(name) = http::header::HeaderName::from_bytes(b"traceparent") {
                    headers.insert(name, value);
                }
            }
        }
        if self.http_debug {
            tracing::info!(
                target: "loadr::http_debug",
                "→ {method} {url}\n{}",
                debug_headers(headers)
            );
        }

        let bytes_sent = approx_request_size(method, url, headers, body_len);
        let req = builder
            .body(timed_body)
            .map_err(|e| ProtocolError::InvalidRequest(format!("could not build request: {e}")))?;
        Ok((req, sent_at, bytes_sent))
    }

    /// Execute one hop (no redirect handling): acquire a connection, send,
    /// read the whole body, store cookies, return the pooled connection.
    async fn one_request(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
        url: &Url,
        method: &http::Method,
        body: Bytes,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let (host, port) = host_port(url).map_err(ProtocolError::InvalidRequest)?;
        let key: PoolKey = (url.scheme().to_string(), host, port);
        let mut timings = Timings::default();

        // Try to reuse a pooled connection for this VU.
        let pooled = {
            let pool = ctx.extensions.get_or_insert_with(HttpPool::default);
            pool.conns.remove(&key)
        };
        let mut sender = match pooled {
            Some(mut s) if !s.is_closed() => match s.ready().await {
                Ok(()) => Some(s),
                Err(_) => None,
            },
            _ => None,
        };
        let reused = sender.is_some();
        let mut sender = match sender.take() {
            Some(s) => s,
            None => match self.connect(url, &mut timings).await {
                Ok(s) => s,
                Err(msg) => {
                    return Ok(transport_error_response(msg, timings, url));
                }
            },
        };
        if !reused {
            timings.blocked_ms = timings.dns_ms + timings.connect_ms + timings.tls_ms;
        }

        let (req, sent_at, bytes_sent) =
            self.build_request(ctx, request, url, method, body, sender.is_h2())?;

        // Send and wait for the response head.
        let send_start = Instant::now();
        let response = match sender.send(req).await {
            Ok(r) => r,
            Err(e) => {
                timings.sending_ms = ms_since(send_start);
                timings.duration_ms = timings.sending_ms;
                return Ok(transport_error_response(
                    format!("request to {url} failed: {e}"),
                    timings,
                    url,
                ));
            }
        };
        let head_at = Instant::now();
        let body_sent_at = sent_at
            .get()
            .copied()
            .unwrap_or(send_start)
            .clamp(send_start, head_at);
        timings.sending_ms = (body_sent_at - send_start).as_secs_f64() * 1000.0;
        timings.waiting_ms = (head_at - body_sent_at).as_secs_f64() * 1000.0;

        let (parts, mut incoming) = response.into_parts();

        // Read the (possibly compressed) body to the end.
        let mut raw = BytesMut::new();
        let mut read_error = None;
        while let Some(frame) = incoming.frame().await {
            match frame {
                Ok(f) => {
                    if let Ok(data) = f.into_data() {
                        raw.extend_from_slice(&data);
                    }
                }
                Err(e) => {
                    read_error = Some(format!("error reading response body: {e}"));
                    break;
                }
            }
        }
        timings.receiving_ms = ms_since(head_at);
        timings.duration_ms = timings.sending_ms + timings.waiting_ms + timings.receiving_ms;

        // Store cookies from every Set-Cookie occurrence.
        for value in parts.headers.get_all(SET_COOKIE) {
            if let Ok(v) = value.to_str() {
                ctx.cookies.store_from_header(url, v);
            }
        }

        let raw_len = raw.len() as u64;
        let mut body_bytes: Bytes = raw.freeze();
        if self.compression && read_error.is_none() {
            if let Some(encoding) = parts
                .headers
                .get(CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok())
            {
                match decompress(encoding, &body_bytes) {
                    Ok(Some(decoded)) => body_bytes = decoded.into(),
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(encoding, error = %e, "failed to decompress response body");
                    }
                }
            }
        }

        let bytes_received = approx_response_size(&parts, raw_len);

        // Return the connection to the per-VU pool.
        let conn_close = parts
            .headers
            .get(CONNECTION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("close"))
            .unwrap_or(false);
        if self.keep_alive && !conn_close && !sender.is_closed() {
            let pool = ctx.extensions.get_or_insert_with(HttpPool::default);
            pool.conns.insert(key, sender);
        }

        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();

        if self.http_debug {
            let preview = String::from_utf8_lossy(&body_bytes);
            let preview = preview.chars().take(2000).collect::<String>();
            tracing::info!(
                target: "loadr::http_debug",
                "← {} {}\n{}\n{}",
                parts.status.as_u16(),
                url,
                headers.iter().map(|(k, v)| format!("{k}: {v}")).collect::<Vec<_>>().join("\n"),
                preview
            );
        }

        // Discard the body if requested (keeps byte counts; extraction sees nothing).
        if self.discard_bodies {
            body_bytes = Bytes::new();
        }

        Ok(ProtocolResponse {
            status: parts.status.as_u16() as i64,
            status_text: parts
                .status
                .canonical_reason()
                .unwrap_or_default()
                .to_string(),
            headers,
            body: body_bytes,
            timings,
            bytes_sent,
            bytes_received,
            protocol_version: version_str(parts.version).to_string(),
            error: read_error,
            url: url.to_string(),
            extras: serde_json::Value::Null,
            grpc_protobuf_outcomes: Vec::new(),
        })
    }

    /// Redirect-following request loop; timings and byte counts accumulate
    /// across hops, while status/headers/body come from the final hop.
    async fn run(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
        mut url: Url,
        mut method: http::Method,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let mut body = request.body.clone();
        let mut total = Timings::default();
        let mut bytes_sent = 0u64;
        let mut bytes_received = 0u64;
        let mut redirects = 0u32;

        loop {
            let mut hop = self
                .one_request(ctx, request, &url, &method, body.clone())
                .await?;
            total.dns_ms += hop.timings.dns_ms;
            total.connect_ms += hop.timings.connect_ms;
            total.tls_ms += hop.timings.tls_ms;
            total.sending_ms += hop.timings.sending_ms;
            total.waiting_ms += hop.timings.waiting_ms;
            total.receiving_ms += hop.timings.receiving_ms;
            total.duration_ms += hop.timings.duration_ms;
            total.blocked_ms += hop.timings.blocked_ms;
            bytes_sent += hop.bytes_sent;
            bytes_received += hop.bytes_received;

            let is_redirect = matches!(hop.status, 301 | 302 | 303 | 307 | 308);
            if hop.error.is_none() && is_redirect && request.follow_redirects {
                if redirects >= request.max_redirects {
                    hop.error = Some(format!(
                        "stopped after {} redirects (max_redirects)",
                        request.max_redirects
                    ));
                } else if let Some(location) = hop.header("location").map(str::to_string) {
                    match url.join(&location) {
                        Ok(next) => {
                            redirects += 1;
                            if matches!(hop.status, 301..=303) && method != http::Method::HEAD {
                                method = http::Method::GET;
                                body = Bytes::new();
                            }
                            tracing::debug!(from = %url, to = %next, status = hop.status, "following redirect");
                            url = next;
                            continue;
                        }
                        Err(e) => {
                            hop.error =
                                Some(format!("invalid redirect location `{location}`: {e}"));
                        }
                    }
                }
            }

            hop.timings = total;
            hop.bytes_sent = bytes_sent;
            hop.bytes_received = bytes_received;
            hop.url = url.to_string();
            return Ok(hop);
        }
    }
}

#[async_trait]
impl ProtocolHandler for HttpHandler {
    fn name(&self) -> &str {
        "http"
    }

    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let url = Url::parse(&request.url).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid url `{}`: {e}", request.url))
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ProtocolError::InvalidRequest(format!(
                "http handler cannot handle scheme `{}`",
                url.scheme()
            )));
        }
        let method = parse_method(&request.method)?;

        let start = Instant::now();
        // HTTP cache: serve fresh GETs from cache, revalidate stale ones.
        if self.cache && method == http::Method::GET {
            return self.run_cached(ctx, request, url, method, start).await;
        }
        match tokio::time::timeout(request.timeout, self.run(ctx, request, url, method)).await {
            Ok(result) => result,
            Err(_) => Ok(timeout_response(request, start)),
        }
    }
}

impl HttpHandler {
    /// Cache-aware GET: fresh hit → no network; stale-with-validator → conditional
    /// request (304 → serve cached); otherwise fetch and store if cacheable.
    async fn run_cached(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
        url: Url,
        method: http::Method,
        start: Instant,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let key = url.to_string();

        // 1. Fresh hit.
        let validator = {
            let cache = ctx.extensions.get_or_insert_with(HttpCache::default);
            if let Some(entry) = cache.entries.get(&key) {
                if entry.is_fresh() {
                    return Ok(entry.to_response(&key, "hit"));
                }
                (entry.etag.clone(), entry.last_modified.clone())
            } else {
                (None, None)
            }
        };

        // 2. Build the (possibly conditional) request.
        let mut effective = request.clone();
        let (etag, last_mod) = validator;
        if let Some(etag) = &etag {
            effective
                .headers
                .push(("If-None-Match".into(), etag.clone()));
        }
        if let Some(lm) = &last_mod {
            effective
                .headers
                .push(("If-Modified-Since".into(), lm.clone()));
        }

        let result =
            match tokio::time::timeout(request.timeout, self.run(ctx, &effective, url, method))
                .await
            {
                Ok(r) => r?,
                Err(_) => return Ok(timeout_response(request, start)),
            };

        // 3. 304 Not Modified → serve the cached body, refresh freshness.
        if result.status == 304 {
            let cache = ctx.extensions.get_or_insert_with(HttpCache::default);
            if let Some(entry) = cache.entries.get_mut(&key) {
                entry.stored_at = Instant::now();
                if let Some(age) = cache_max_age(&result.headers) {
                    entry.max_age = age;
                }
                let mut resp = entry.to_response(&key, "revalidated");
                resp.timings = result.timings;
                resp.bytes_sent = result.bytes_sent;
                return Ok(resp);
            }
        }

        // 4. Store cacheable 200 responses.
        if result.status == 200 && result.error.is_none() {
            if let Some(max_age) = cache_max_age(&result.headers) {
                let cache = ctx.extensions.get_or_insert_with(HttpCache::default);
                cache.entries.insert(
                    key.clone(),
                    CacheEntry {
                        status: result.status,
                        status_text: result.status_text.clone(),
                        headers: result.headers.clone(),
                        body: result.body.clone(),
                        bytes_received: result.bytes_received,
                        protocol_version: result.protocol_version.clone(),
                        stored_at: Instant::now(),
                        max_age,
                        etag: header_value(&result.headers, "etag"),
                        last_modified: header_value(&result.headers, "last-modified"),
                    },
                );
            }
        }

        let mut result = result;
        if result.extras.is_null() {
            result.extras = serde_json::json!({ "cache": "miss" });
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a W3C `traceparent` header: `00-<16-byte trace id>-<8-byte span id>-01`.
/// Trace ids need only be unique, not cryptographically random, so this uses a
/// SplitMix64 PRNG seeded from a monotonic counter and the wall clock.
fn make_traceparent(seed: &mut u64) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    *seed = seed
        .wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
        .wrapping_add(now);
    let mut next = || {
        // SplitMix64.
        *seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let trace = format!("{:016x}{:016x}", next(), next());
    let span = format!("{:016x}", next());
    format!("00-{trace}-{span}-01")
}

/// Render request headers for `--http-debug` output.
fn debug_headers(headers: &http::HeaderMap) -> String {
    headers
        .iter()
        .map(|(k, v)| format!("{}: {}", k, String::from_utf8_lossy(v.as_bytes())))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn parse_method(method: &str) -> Result<http::Method, ProtocolError> {
    if method.is_empty() {
        return Ok(http::Method::GET);
    }
    http::Method::from_bytes(method.to_ascii_uppercase().as_bytes())
        .map_err(|e| ProtocolError::InvalidRequest(format!("invalid method `{method}`: {e}")))
}

pub(crate) fn timeout_response(request: &PreparedRequest, start: Instant) -> ProtocolResponse {
    let elapsed = ms_since(start);
    ProtocolResponse {
        status: 0,
        error: Some(format!("request timed out after {:?}", request.timeout)),
        url: request.url.clone(),
        timings: Timings {
            duration_ms: elapsed,
            ..Timings::default()
        },
        ..ProtocolResponse::default()
    }
}

fn transport_error_response(message: String, timings: Timings, url: &Url) -> ProtocolResponse {
    ProtocolResponse {
        status: 0,
        error: Some(message),
        timings,
        url: url.to_string(),
        ..ProtocolResponse::default()
    }
}

fn version_str(v: http::Version) -> &'static str {
    match v {
        http::Version::HTTP_09 => "HTTP/0.9",
        http::Version::HTTP_10 => "HTTP/1.0",
        http::Version::HTTP_11 => "HTTP/1.1",
        http::Version::HTTP_2 => "HTTP/2",
        http::Version::HTTP_3 => "HTTP/3",
        _ => "HTTP",
    }
}

fn host_header_value(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    // `Url::port()` is `None` when the port is the scheme default.
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

/// Approximate on-the-wire request size: request line + headers + body.
fn approx_request_size(
    method: &http::Method,
    url: &Url,
    headers: &http::HeaderMap,
    body_len: usize,
) -> u64 {
    let mut size = method.as_str().len() + 1 + url.path().len() + 1 + "HTTP/1.1".len() + 2;
    if let Some(q) = url.query() {
        size += q.len() + 1;
    }
    for (name, value) in headers {
        size += name.as_str().len() + 2 + value.as_bytes().len() + 2;
    }
    size += 2 + body_len;
    size as u64
}

/// Approximate on-the-wire response size: status line + headers + raw body.
fn approx_response_size(parts: &http::response::Parts, raw_body_len: u64) -> u64 {
    let mut size = "HTTP/1.1".len() + 1 + 3 + 1 + 2;
    size += parts.status.canonical_reason().unwrap_or_default().len();
    for (name, value) in &parts.headers {
        size += name.as_str().len() + 2 + value.as_bytes().len() + 2;
    }
    size += 2;
    size as u64 + raw_body_len
}

/// Decompress `data` according to a `Content-Encoding` value. Returns
/// `Ok(None)` for identity/unknown encodings (body passed through).
fn decompress(encoding: &str, data: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut out = Vec::new();
    match encoding.trim().to_ascii_lowercase().as_str() {
        "gzip" | "x-gzip" => {
            flate2::read::GzDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip: {e}"))?;
            Ok(Some(out))
        }
        "deflate" => {
            // Try zlib-wrapped first (per spec), then raw deflate (common quirk).
            if flate2::read::ZlibDecoder::new(data)
                .read_to_end(&mut out)
                .is_ok()
            {
                return Ok(Some(out));
            }
            out.clear();
            flate2::read::DeflateDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(|e| format!("deflate: {e}"))?;
            Ok(Some(out))
        }
        "br" => {
            brotli::Decompressor::new(data, 4096)
                .read_to_end(&mut out)
                .map_err(|e| format!("brotli: {e}"))?;
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

/// Send an HTTP CONNECT request over `tcp` and wait for a 2xx response.
async fn proxy_tunnel(tcp: &mut TcpStream, host: &str, port: u16) -> Result<(), String> {
    let req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    tcp.write_all(req.as_bytes())
        .await
        .map_err(|e| format!("proxy CONNECT write failed: {e}"))?;
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        let n = tcp
            .read(&mut chunk)
            .await
            .map_err(|e| format!("proxy CONNECT read failed: {e}"))?;
        if n == 0 {
            return Err("proxy closed the connection during CONNECT".to_string());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Err("proxy CONNECT response too large".to_string());
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status_line = head.lines().next().unwrap_or_default();
    let ok = status_line
        .split_whitespace()
        .nth(1)
        .map(|code| code.starts_with('2'))
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(format!("proxy CONNECT failed: {status_line}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompress_gzip_round_trip() {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"hello world").unwrap();
        let compressed = enc.finish().unwrap();
        let out = decompress("gzip", &compressed).unwrap().unwrap();
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn decompress_deflate_round_trip() {
        use std::io::Write as _;
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"abc").unwrap();
        let compressed = enc.finish().unwrap();
        let out = decompress("deflate", &compressed).unwrap().unwrap();
        assert_eq!(out, b"abc");
    }

    #[test]
    fn decompress_brotli_round_trip() {
        let mut compressed = Vec::new();
        {
            use std::io::Write as _;
            let mut enc = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
            enc.write_all(b"brotli body").unwrap();
        }
        let out = decompress("br", &compressed).unwrap().unwrap();
        assert_eq!(out, b"brotli body");
    }

    #[test]
    fn unknown_encoding_passes_through() {
        assert!(decompress("identity", b"x").unwrap().is_none());
    }

    #[test]
    fn host_header_includes_non_default_port() {
        let url = Url::parse("http://example.com:8080/x").unwrap();
        assert_eq!(host_header_value(&url), "example.com:8080");
        let url = Url::parse("https://example.com/x").unwrap();
        assert_eq!(host_header_value(&url), "example.com");
    }

    #[test]
    fn parse_method_defaults_to_get() {
        assert_eq!(parse_method("").unwrap(), http::Method::GET);
        assert_eq!(parse_method("post").unwrap(), http::Method::POST);
        assert!(parse_method("BAD METHOD").is_err());
    }
}
