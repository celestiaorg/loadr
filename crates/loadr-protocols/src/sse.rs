//! Server-Sent Events (SSE) protocol handler.
//!
//! Performs an HTTP/1.1 `GET` with `Accept: text/event-stream` and reads the
//! event stream frame-by-frame, parsing the SSE wire format (`event:`,
//! `data:`, `id:`, `retry:` fields dispatched on a blank line). The handler
//! connects over a raw TCP (or TLS, for `https`/`sses`) stream using hyper's
//! low-level client connection so the body can be consumed incrementally and
//! per-phase timings (connect, TLS, TTFB) measured directly.
//!
//! Aliases: `sse://` maps to `http://`, `sses://` maps to `https://`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{ACCEPT, CACHE_CONTROL, CONNECTION, COOKIE, HOST};
use http_body_util::BodyExt as _;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use loadr_config::HttpDefaults;
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{PreparedRequest, ProtocolHandler, ProtocolResponse, Timings};
use loadr_core::vu::VuContext;
use tokio::net::TcpStream;
use url::Url;

use crate::net::{host_port, ms_since, resolve, IoStream};

/// Maximum number of parsed events retained in `extras.events`.
const EVENTS_CAP: usize = 100;

/// A single parsed Server-Sent Event.
#[derive(Debug, Clone, Default)]
struct SseEvent {
    /// `event:` field (defaults to `message` when absent, per the spec).
    event_type: String,
    /// Concatenated `data:` lines (joined with `\n`).
    data: String,
    /// `id:` field, if present.
    id: Option<String>,
}

impl SseEvent {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": self.event_type,
            "data": self.data,
            "id": self.id,
        })
    }
}

/// Limits controlling how long the stream is read.
#[derive(Debug, Clone)]
struct SseLimits {
    /// Stop after this many events have been dispatched.
    max_events: Option<u64>,
    /// Stop once an event's data contains this substring.
    until: Option<String>,
    /// Stop after this wall-clock duration (capped by the request timeout).
    duration: Option<Duration>,
}

impl SseLimits {
    /// Parse limits defensively from `request.options.plugin`.
    fn from_plugin(plugin: Option<&serde_json::Value>) -> SseLimits {
        let mut limits = SseLimits {
            max_events: None,
            until: None,
            duration: None,
        };
        let Some(obj) = plugin.and_then(serde_json::Value::as_object) else {
            return limits;
        };
        if let Some(n) = obj.get("events").and_then(serde_json::Value::as_u64) {
            limits.max_events = Some(n);
        }
        if let Some(s) = obj.get("until").and_then(serde_json::Value::as_str) {
            if !s.is_empty() {
                limits.until = Some(s.to_string());
            }
        }
        if let Some(s) = obj.get("duration").and_then(serde_json::Value::as_str) {
            if let Some(d) = parse_duration(s) {
                limits.duration = Some(d);
            }
        }
        limits
    }
}

/// Parse a short duration string such as `10s`, `500ms`, `2m` or a bare number
/// of seconds. Returns `None` for anything unparseable.
fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(num) = raw.strip_suffix("ms") {
        return num.trim().parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(num) = raw.strip_suffix('s') {
        return num.trim().parse::<f64>().ok().map(Duration::from_secs_f64);
    }
    if let Some(num) = raw.strip_suffix('m') {
        return num
            .trim()
            .parse::<f64>()
            .ok()
            .map(|m| Duration::from_secs_f64(m * 60.0));
    }
    raw.parse::<f64>().ok().map(Duration::from_secs_f64)
}

/// Incremental SSE wire-format parser. Feed it raw bytes; it yields complete
/// events as blank-line dispatch boundaries are crossed.
#[derive(Default)]
struct SseParser {
    /// Bytes not yet split into a complete line.
    buffer: Vec<u8>,
    /// Fields accumulated for the event currently being built.
    event_type: Option<String>,
    data: Vec<String>,
    id: Option<String>,
    /// True once any field of the current event has been seen.
    have_fields: bool,
}

impl SseParser {
    /// Push raw bytes and return any events completed by a blank line.
    fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        // Process complete lines (terminated by \n; a trailing \r is stripped).
        while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=pos).collect();
            line.pop(); // drop the \n
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line).into_owned();
            if let Some(event) = self.feed_line(&line) {
                events.push(event);
            }
        }
        events
    }

    /// Process one logical line. Returns an event when a blank line dispatches.
    fn feed_line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.dispatch();
        }
        // Comment lines start with ':' and are ignored.
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        self.have_fields = true;
        match field {
            "event" => self.event_type = Some(value.to_string()),
            "data" => self.data.push(value.to_string()),
            "id" => self.id = Some(value.to_string()),
            // `retry:` is recognised but not acted upon (single-shot reads).
            _ => {}
        }
        None
    }

    /// Emit the accumulated event (if any) and reset for the next one.
    fn dispatch(&mut self) -> Option<SseEvent> {
        if !self.have_fields {
            return None;
        }
        let event = SseEvent {
            event_type: self
                .event_type
                .take()
                .unwrap_or_else(|| "message".to_string()),
            data: self.data.join("\n"),
            id: self.id.take(),
        };
        self.data.clear();
        self.have_fields = false;
        Some(event)
    }
}

/// Server-Sent Events protocol handler.
pub struct SseHandler {
    tls: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
}

impl SseHandler {
    /// Build the handler. The rustls config (HTTP/1.1 ALPN) is built once from
    /// `defaults.tls`; `base_dir` resolves relative TLS file paths.
    pub fn new(defaults: &HttpDefaults, base_dir: &std::path::Path) -> Result<Self, ProtocolError> {
        let tls = crate::tls::client_config(&defaults.tls, base_dir, vec![b"http/1.1".to_vec()])?;
        Ok(SseHandler {
            tls: Arc::new(tls),
            server_name: defaults.tls.server_name.clone(),
        })
    }
}

/// Normalise `sse`/`sses` schemes to `http`/`https`.
///
/// The `url` crate disallows switching between non-special and special
/// schemes via `set_scheme`, so the rewrite is done on the raw string before
/// parsing.
fn normalise_url(raw: &str) -> Result<Url, ProtocolError> {
    let rewritten = if let Some(rest) = raw.strip_prefix("sses://") {
        format!("https://{rest}")
    } else if let Some(rest) = raw.strip_prefix("sse://") {
        format!("http://{rest}")
    } else {
        raw.to_string()
    };
    let url = Url::parse(&rewritten)
        .map_err(|e| ProtocolError::InvalidRequest(format!("invalid url `{raw}`: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ProtocolError::InvalidRequest(format!(
            "sse handler cannot handle scheme `{}`",
            url.scheme()
        )));
    }
    Ok(url)
}

/// Transport outcome of a stream attempt: either a full response or an error
/// string carried alongside whatever timings/state were gathered.
enum StreamError {
    Transport { message: String, timings: Timings },
    Invalid(ProtocolError),
}

impl SseHandler {
    #[allow(clippy::too_many_lines)]
    async fn stream(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
        url: &Url,
        limits: &SseLimits,
        deadline: tokio::time::Instant,
    ) -> Result<ProtocolResponse, StreamError> {
        let mut timings = Timings::default();
        let transport =
            |message: String, timings: Timings| StreamError::Transport { message, timings };

        // --- Connect: DNS → TCP → (TLS) ---
        let (host, port) = host_port(url).map_err(|e| transport(e, timings))?;
        let url_host = url.host().ok_or_else(|| {
            StreamError::Invalid(ProtocolError::InvalidRequest(format!(
                "url `{url}` has no host"
            )))
        })?;
        let addr = resolve(&url_host, port, &mut timings)
            .await
            .map_err(|e| transport(e, timings))?;

        let connect_start = Instant::now();
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| transport(format!("connection to {addr} failed: {e}"), timings))?;
        let _ = tcp.set_nodelay(true);
        timings.connect_ms = ms_since(connect_start);

        let stream: Box<dyn IoStream> = if url.scheme() == "https" {
            let tls_start = Instant::now();
            let connector = tokio_rustls::TlsConnector::from(self.tls.clone());
            let sni = crate::tls::server_name(self.server_name.as_deref(), url)
                .map_err(StreamError::Invalid)?;
            let tls = connector.connect(sni, tcp).await.map_err(|e| {
                transport(
                    format!("tls handshake with {host}:{port} failed: {e}"),
                    timings,
                )
            })?;
            timings.tls_ms = ms_since(tls_start);
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        timings.blocked_ms = timings.dns_ms + timings.connect_ms + timings.tls_ms;

        // --- Drive an HTTP/1.1 client connection ---
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| transport(format!("http handshake failed: {e}"), timings))?;
        // The connection task drives I/O; it ends when the body is dropped.
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });

        let req = match build_request(ctx, request, url, &host, port) {
            Ok(req) => req,
            Err(e) => {
                conn_task.abort();
                return Err(StreamError::Invalid(e));
            }
        };
        let bytes_sent = approx_request_size(&req);

        let send_start = Instant::now();
        let response = match sender.send_request(req).await {
            Ok(resp) => resp,
            Err(e) => {
                conn_task.abort();
                return Err(transport(format!("request failed: {e}"), timings));
            }
        };
        timings.waiting_ms = ms_since(send_start);

        let status = response.status();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();

        let (events, bytes_received, error) =
            read_events(response.into_body(), limits, deadline, &mut timings).await;
        conn_task.abort();

        timings.duration_ms = timings.sending_ms + timings.waiting_ms + timings.receiving_ms;

        let last_event = events.last().cloned().unwrap_or_default();
        let capped: Vec<serde_json::Value> = events
            .iter()
            .take(EVENTS_CAP)
            .map(SseEvent::to_json)
            .collect();

        tracing::debug!(
            url = %url,
            status = status.as_u16(),
            events = events.len(),
            duration_ms = timings.duration_ms,
            "sse stream finished"
        );

        Ok(ProtocolResponse {
            status: i64::from(status.as_u16()),
            status_text: status.canonical_reason().unwrap_or("").to_string(),
            headers,
            body: Bytes::from(last_event.data.clone().into_bytes()),
            timings,
            bytes_sent,
            bytes_received,
            protocol_version: "sse".to_string(),
            error,
            url: url.to_string(),
            extras: serde_json::json!({
                "events_received": events.len(),
                "last_event": last_event.to_json(),
                "events": capped,
            }),
            grpc_protobuf_outcomes: Vec::new(),
        })
    }
}

/// Build the SSE GET request with default + caller headers and cookies.
fn build_request(
    ctx: &mut VuContext,
    request: &PreparedRequest,
    url: &Url,
    host: &str,
    port: u16,
) -> Result<http::Request<http_body_util::Empty<Bytes>>, ProtocolError> {
    let method = http::Method::from_bytes(request.method.to_ascii_uppercase().as_bytes())
        .map_err(|e| ProtocolError::InvalidRequest(format!("invalid method: {e}")))?;
    if method != http::Method::GET {
        return Err(ProtocolError::InvalidRequest(format!(
            "sse requires GET, got `{}`",
            request.method
        )));
    }

    let path = match url.query() {
        Some(q) => format!("{}?{}", url.path(), q),
        None => url.path().to_string(),
    };
    let authority =
        if (url.scheme() == "https" && port == 443) || (url.scheme() == "http" && port == 80) {
            host.to_string()
        } else {
            format!("{host}:{port}")
        };

    let mut builder = http::Request::builder()
        .method(http::Method::GET)
        .uri(&path)
        .header(HOST, &authority)
        .header(ACCEPT, "text/event-stream")
        .header(CACHE_CONTROL, "no-cache")
        .header(CONNECTION, "keep-alive");

    for (name, value) in &request.headers {
        let name = http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| ProtocolError::InvalidRequest(format!("invalid header `{name}`: {e}")))?;
        let value = http::HeaderValue::from_str(value)
            .map_err(|e| ProtocolError::InvalidRequest(format!("invalid header value: {e}")))?;
        builder = builder.header(name, value);
    }

    if let Some(cookie) = ctx.cookies.header_for(url) {
        let value = http::HeaderValue::from_str(&cookie)
            .map_err(|e| ProtocolError::InvalidRequest(format!("invalid cookie header: {e}")))?;
        builder = builder.header(COOKIE, value);
    }

    builder
        .body(http_body_util::Empty::<Bytes>::new())
        .map_err(|e| ProtocolError::InvalidRequest(format!("cannot build request: {e}")))
}

/// Rough on-the-wire request size (request line + headers).
fn approx_request_size(req: &http::Request<http_body_util::Empty<Bytes>>) -> u64 {
    let mut size = req.method().as_str().len() + req.uri().path().len() + 12;
    for (name, value) in req.headers() {
        size += name.as_str().len() + value.as_bytes().len() + 4;
    }
    size as u64
}

/// Read the streaming body frame-by-frame until a stop condition is met.
async fn read_events(
    mut body: Incoming,
    limits: &SseLimits,
    deadline: tokio::time::Instant,
    timings: &mut Timings,
) -> (Vec<SseEvent>, u64, Option<String>) {
    let read_deadline = match limits.duration {
        Some(d) => (tokio::time::Instant::now() + d).min(deadline),
        None => deadline,
    };

    let mut parser = SseParser::default();
    let mut events: Vec<SseEvent> = Vec::new();
    let mut bytes_received: u64 = 0;
    let mut error: Option<String> = None;
    let recv_start = Instant::now();

    'outer: loop {
        if let Some(max) = limits.max_events {
            if events.len() as u64 >= max {
                break;
            }
        }
        match tokio::time::timeout_at(read_deadline, body.frame()).await {
            Err(_) => {
                // Deadline elapsing is a normal end of a duration-bounded read;
                // only treat it as an error when no explicit budget was set.
                if limits.duration.is_none() && limits.max_events.is_none() {
                    // Pure timeout-bounded read: reaching the deadline is fine.
                }
                break;
            }
            Ok(None) => break, // stream closed
            Ok(Some(Err(e))) => {
                error = Some(format!("stream read failed: {e}"));
                break;
            }
            Ok(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    bytes_received += data.len() as u64;
                    for event in parser.push(data) {
                        let matched = limits
                            .until
                            .as_deref()
                            .is_some_and(|needle| event.data.contains(needle));
                        events.push(event);
                        if matched {
                            break 'outer;
                        }
                        if let Some(max) = limits.max_events {
                            if events.len() as u64 >= max {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
    }
    timings.receiving_ms = ms_since(recv_start);
    (events, bytes_received, error)
}

#[async_trait]
impl ProtocolHandler for SseHandler {
    fn name(&self) -> &str {
        "sse"
    }

    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let url = normalise_url(&request.url)?;
        let limits = SseLimits::from_plugin(request.options.plugin.as_ref());
        let deadline = tokio::time::Instant::now() + request.timeout;

        match self.stream(ctx, request, &url, &limits, deadline).await {
            Ok(response) => Ok(response),
            Err(StreamError::Transport { message, timings }) => Ok(ProtocolResponse {
                status: 0,
                error: Some(message),
                timings,
                protocol_version: "sse".to_string(),
                url: url.to_string(),
                ..ProtocolResponse::default()
            }),
            Err(StreamError::Invalid(e)) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_event() {
        let mut p = SseParser::default();
        let events = p.push(b"event: tick\ndata: hello\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "tick");
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn multiline_data_joined() {
        let mut p = SseParser::default();
        let events = p.push(b"data: line1\ndata: line2\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line1\nline2");
        assert_eq!(events[0].event_type, "message");
    }

    #[test]
    fn handles_split_chunks_and_crlf() {
        let mut p = SseParser::default();
        assert!(p.push(b"event: a\r\nda").is_empty());
        let events = p.push(b"ta: x\r\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "a");
        assert_eq!(events[0].data, "x");
    }

    #[test]
    fn id_and_comments() {
        let mut p = SseParser::default();
        let events = p.push(b": comment\nid: 42\ndata: y\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_deref(), Some("42"));
        assert_eq!(events[0].data, "y");
    }

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("3"), Some(Duration::from_secs(3)));
        assert_eq!(parse_duration("bogus"), None);
    }

    #[test]
    fn limits_from_plugin() {
        let plugin = serde_json::json!({ "events": 5, "until": "done", "duration": "1s" });
        let limits = SseLimits::from_plugin(Some(&plugin));
        assert_eq!(limits.max_events, Some(5));
        assert_eq!(limits.until.as_deref(), Some("done"));
        assert_eq!(limits.duration, Some(Duration::from_secs(1)));
    }

    #[test]
    fn scheme_normalisation() {
        assert_eq!(normalise_url("sse://h/p").unwrap().scheme(), "http");
        assert_eq!(normalise_url("sses://h/p").unwrap().scheme(), "https");
        assert_eq!(normalise_url("http://h/p").unwrap().scheme(), "http");
        assert!(normalise_url("ftp://h/p").is_err());
    }
}
