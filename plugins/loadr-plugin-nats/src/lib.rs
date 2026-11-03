//! `loadr-plugin-nats` — a native protocol plugin that adds NATS as a loadr
//! load-test target.
//!
//! # How it plugs in
//!
//! loadr's native protocol ABI ([`FfiProtocol`]) is synchronous: the host calls
//! `execute(&self, request_json) -> response_json` on a single shared plugin
//! instance (`Send + Sync`), created once via `make_protocol()`. There is no
//! per-VU state across the FFI boundary, so this plugin owns all of its own
//! machinery:
//!
//! * The NATS client protocol is spoken directly over a **blocking
//!   `std::net::TcpStream`** — there is no `async-nats` crate and therefore no
//!   async runtime and no native-tls/OpenSSL dependency, so this cdylib
//!   cross-compiles cleanly for every release target.
//! * An internal connection pool keyed by `host:port` (plus user, so distinct
//!   credentials never share a socket): `OnceLock<Mutex<HashMap<String,
//!   Vec<Box<dyn NatsIo>>>>>`. A request checks out an idle connection (running
//!   the `INFO`/`CONNECT` handshake only on a fresh socket), reuses it for the
//!   exchange, and returns it for the next caller — so concurrent VUs reuse a
//!   small set of sockets per distinct server rather than reconnecting on every
//!   message. A connection left in an error state (transport failure, timeout)
//!   is dropped instead of returned, so the next caller transparently
//!   re-establishes it.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. The
//! connection target comes from `url` (`nats://[user[:password]@]host[:port]`)
//! and the operation from the request's `plugin:` block (`options`):
//!
//! * `operation` — `publish` (default) or `request`.
//! * `subject`   — the subject to publish/request on (required).
//! * `body`      — the payload (a string, or a JSON object/array serialised
//!   compactly); falls back to the request body.
//! * `reply_to`  — an optional reply subject set on a `publish`.
//!
//! The response is JSON `{ status, status_text, body_b64, duration_ms, error,
//! extras }`. `status` is `0` on success (a publish the server accepted, or a
//! reply received) and `1` on failure (a `-ERR` from the server, a request
//! timeout, or a connection failure), with `error` set. A `request` returns the
//! reply payload as the body. The host derives `nats_reqs` / `nats_req_duration`
//! from the `nats` plugin name.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "nats";

/// The NATS standard client port, used when the URL omits one.
const DEFAULT_PORT: u16 = 4222;

// ---------------------------------------------------------------------------
// Target + credentials.
// ---------------------------------------------------------------------------

/// Credentials taken from the URL userinfo, sent in the `CONNECT` handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Creds {
    user: String,
    pass: Option<String>,
}

/// A parsed connection target.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    /// `host:port`, the TCP dial address.
    addr: String,
    creds: Option<Creds>,
}

impl Target {
    /// Pool key: the dial address, namespaced by user so that requests using
    /// different credentials to the same server never share a socket.
    fn pool_key(&self) -> String {
        match &self.creds {
            Some(c) => format!("{}@{}", c.user, self.addr),
            None => self.addr.clone(),
        }
    }
}

/// Parse and validate the target URL.
fn parse_target(raw: &str) -> Result<Target, String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    if url.scheme() != "nats" {
        return Err(format!(
            "nats plugin cannot handle scheme `{}`",
            url.scheme()
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("`{raw}` has no host"))?
        .to_string();
    let port = url.port().unwrap_or(DEFAULT_PORT);
    let addr = format!("{host}:{port}");
    let creds = if url.username().is_empty() {
        None
    } else {
        Some(Creds {
            user: url.username().to_string(),
            pass: url.password().map(str::to_string),
        })
    };
    Ok(Target { addr, creds })
}

// ---------------------------------------------------------------------------
// Operation parsing.
// ---------------------------------------------------------------------------

/// A single NATS operation described by the request's `plugin:` block.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    /// Fire-and-forget publish, confirmed by a `PING`/`PONG` flush.
    Publish {
        subject: String,
        reply_to: Option<String>,
        body: Vec<u8>,
    },
    /// Request/reply against a responder.
    Request { subject: String, body: Vec<u8> },
}

impl Op {
    /// Operation name for `extras`.
    fn name(&self) -> &'static str {
        match self {
            Op::Publish { .. } => "publish",
            Op::Request { .. } => "request",
        }
    }

    fn subject(&self) -> &str {
        match self {
            Op::Publish { subject, .. } | Op::Request { subject, .. } => subject,
        }
    }
}

/// Resolve the message payload: prefer `plugin.body` (a string used verbatim, a
/// JSON object/array serialised compactly), else the request body.
fn resolve_body(request: &FfiRequest, opts: Option<&serde_json::Value>) -> Result<Vec<u8>, String> {
    if let Some(value) = opts.and_then(|o| o.get("body")) {
        return Ok(match value {
            serde_json::Value::String(s) => s.clone().into_bytes(),
            serde_json::Value::Null => Vec::new(),
            other => serde_json::to_string(other)
                .map_err(|e| format!("cannot encode nats body: {e}"))?
                .into_bytes(),
        });
    }
    if request.body_b64.is_empty() {
        return Ok(Vec::new());
    }
    base64_decode(&request.body_b64)
}

/// Build the [`Op`] from the request's `plugin:` block.
fn parse_op(request: &FfiRequest) -> Result<Op, String> {
    let opts = request.options.as_ref();
    let operation = opts
        .and_then(|o| o.get("operation"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("publish");
    let subject = opts
        .and_then(|o| o.get("subject"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "nats plugin requires a non-empty `subject`".to_string())?;
    let body = resolve_body(request, opts)?;
    match operation {
        "publish" | "pub" => {
            let reply_to = opts
                .and_then(|o| o.get("reply_to"))
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            Ok(Op::Publish {
                subject,
                reply_to,
                body,
            })
        }
        "request" | "req" => Ok(Op::Request { subject, body }),
        other => Err(format!(
            "unknown nats operation `{other}` (want `publish` or `request`)"
        )),
    }
}

// ---------------------------------------------------------------------------
// Wire protocol.
// ---------------------------------------------------------------------------

/// A parsed NATS protocol frame (server → client). Only the frames the plugin
/// needs are represented; others (e.g. `HMSG`) surface as an error.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    /// `INFO {json}` — sent once by the server on connect. The advertised
    /// options are not needed (the plugin never upgrades to TLS), so the JSON
    /// body is discarded.
    Info,
    /// `MSG <subject> <sid> [reply-to] <#bytes>` + payload. Only the `sid`
    /// (to match the reply to our subscription) and the payload are retained.
    Msg { sid: String, payload: Vec<u8> },
    /// `PING` — a server keepalive; the client answers `PONG`.
    Ping,
    /// `PONG` — the answer to a client `PING` flush.
    Pong,
    /// `+OK` — a verbose-mode acknowledgement.
    Ok,
    /// `-ERR <msg>` — a protocol error.
    Err(String),
}

/// Encode a `PUB` (payload follows the control line, then CRLF).
fn encode_pub(subject: &str, reply_to: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    match reply_to {
        Some(reply) => {
            out.extend_from_slice(format!("PUB {subject} {reply} {}\r\n", body.len()).as_bytes())
        }
        None => out.extend_from_slice(format!("PUB {subject} {}\r\n", body.len()).as_bytes()),
    }
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a `SUB <subject> <sid>`.
fn encode_sub(subject: &str, sid: &str) -> Vec<u8> {
    format!("SUB {subject} {sid}\r\n").into_bytes()
}

/// Encode an `UNSUB <sid> <max_msgs>` (auto-unsubscribe after `max_msgs`).
fn encode_unsub(sid: &str, max_msgs: u32) -> Vec<u8> {
    format!("UNSUB {sid} {max_msgs}\r\n").into_bytes()
}

/// Encode the `CONNECT {json}` handshake line, including credentials if any.
fn encode_connect(creds: Option<&Creds>) -> Vec<u8> {
    let mut obj = serde_json::json!({
        "verbose": false,
        "pedantic": false,
        "tls_required": false,
        "name": "loadr",
        "lang": "rust",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": 1,
    });
    if let Some(creds) = creds {
        obj["user"] = serde_json::Value::String(creds.user.clone());
        if let Some(pass) = &creds.pass {
            obj["pass"] = serde_json::Value::String(pass.clone());
        }
    }
    format!("CONNECT {obj}\r\n").into_bytes()
}

/// Read one CRLF-terminated control line (without the trailing CRLF).
fn read_line<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        reader
            .read_exact(&mut byte)
            .map_err(|e| format!("read failed: {e}"))?;
        if byte[0] == b'\r' {
            reader
                .read_exact(&mut byte)
                .map_err(|e| format!("read failed: {e}"))?;
            if byte[0] == b'\n' {
                break;
            }
            line.push(b'\r');
            line.push(byte[0]);
        } else {
            line.push(byte[0]);
        }
    }
    Ok(line)
}

/// Read one NATS protocol frame from `reader`.
fn read_frame<R: Read>(reader: &mut R) -> Result<Frame, String> {
    let line = read_line(reader)?;
    let text = String::from_utf8_lossy(&line);
    let text = text.trim();
    let mut parts = text.splitn(2, |c: char| c.is_ascii_whitespace());
    let op = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();
    match op.to_ascii_uppercase().as_str() {
        "PING" => Ok(Frame::Ping),
        "PONG" => Ok(Frame::Pong),
        "+OK" => Ok(Frame::Ok),
        "-ERR" => Ok(Frame::Err(rest.trim_matches('\'').to_string())),
        "INFO" => Ok(Frame::Info),
        "MSG" => {
            // `MSG <subject> <sid> [reply-to] <#bytes>`.
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let (sid, size) = match fields.as_slice() {
                [_subject, sid, size] => ((*sid).to_string(), *size),
                [_subject, sid, _reply, size] => ((*sid).to_string(), *size),
                _ => return Err(format!("malformed MSG line: `{text}`")),
            };
            let n: usize = size
                .parse()
                .map_err(|_| format!("invalid MSG byte count: `{size}`"))?;
            let mut payload = vec![0u8; n];
            reader
                .read_exact(&mut payload)
                .map_err(|e| format!("read failed: {e}"))?;
            let mut crlf = [0u8; 2];
            reader
                .read_exact(&mut crlf)
                .map_err(|e| format!("read failed: {e}"))?;
            Ok(Frame::Msg { sid, payload })
        }
        other => Err(format!("unexpected NATS protocol op: `{other}`")),
    }
}

// ---------------------------------------------------------------------------
// Connection abstraction — a seam so the operation logic can be unit-tested
// without a real socket.
// ---------------------------------------------------------------------------

/// A live NATS connection: send bytes, read one frame, adjust the read
/// deadline. A returned `Err` is a *transport* failure and drops the socket; a
/// server `-ERR` surfaces as `Ok(Frame::Err(..))`.
trait NatsIo: Send {
    fn send(&mut self, buf: &[u8]) -> Result<(), String>;
    fn read_frame(&mut self) -> Result<Frame, String>;
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String>;
}

/// Creates fresh, handshaken [`NatsIo`] connections.
trait ConnFactory: Send {
    fn connect(&self) -> Result<Box<dyn NatsIo>, String>;
}

/// A real blocking TCP connection to a NATS server.
struct TcpConn {
    reader: BufReader<TcpStream>,
}

impl NatsIo for TcpConn {
    fn send(&mut self, buf: &[u8]) -> Result<(), String> {
        self.reader
            .get_mut()
            .write_all(buf)
            .map_err(|e| format!("send failed: {e}"))?;
        self.reader
            .get_mut()
            .flush()
            .map_err(|e| format!("flush failed: {e}"))
    }

    fn read_frame(&mut self) -> Result<Frame, String> {
        read_frame(&mut self.reader)
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.reader
            .get_ref()
            .set_read_timeout(timeout)
            .map_err(|e| format!("set_read_timeout failed: {e}"))
    }
}

/// Opens handshaken `TcpConn`s to `addr`.
struct TcpConnFactory {
    addr: String,
    creds: Option<Creds>,
}

impl ConnFactory for TcpConnFactory {
    fn connect(&self) -> Result<Box<dyn NatsIo>, String> {
        let stream = TcpStream::connect(&self.addr)
            .map_err(|e| format!("connection to {} failed: {e}", self.addr))?;
        let _ = stream.set_nodelay(true);
        let mut conn = TcpConn {
            reader: BufReader::new(stream),
        };
        client_handshake(&mut conn, self.creds.as_ref())?;
        Ok(Box::new(conn))
    }
}

/// Run the NATS client handshake: read the server `INFO`, send `CONNECT`, then
/// flush with `PING` and wait for `PONG`.
fn client_handshake(io: &mut dyn NatsIo, creds: Option<&Creds>) -> Result<(), String> {
    match io.read_frame()? {
        Frame::Info => {}
        other => return Err(format!("expected INFO on connect, got {other:?}")),
    }
    io.send(&encode_connect(creds))?;
    io.send(b"PING\r\n")?;
    loop {
        match io.read_frame()? {
            Frame::Pong => return Ok(()),
            Frame::Ok => continue,
            Frame::Ping => io.send(b"PONG\r\n")?,
            Frame::Err(e) => return Err(format!("CONNECT rejected: {e}")),
            other => return Err(format!("unexpected frame during handshake: {other:?}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Operations.
// ---------------------------------------------------------------------------

/// Publish one message and confirm the server accepted it via a `PING`/`PONG`
/// flush. A `-ERR` before the `PONG` is a failure.
fn do_publish(
    io: &mut dyn NatsIo,
    subject: &str,
    reply_to: Option<&str>,
    body: &[u8],
) -> Result<(), String> {
    io.send(&encode_pub(subject, reply_to, body))?;
    io.send(b"PING\r\n")?;
    loop {
        match io.read_frame()? {
            Frame::Pong => return Ok(()),
            Frame::Err(e) => return Err(e),
            Frame::Ping => io.send(b"PONG\r\n")?,
            // A stray `+OK`/`INFO`/`MSG` before the flush is ignored.
            Frame::Ok | Frame::Info | Frame::Msg { .. } => continue,
        }
    }
}

/// Send a request and wait for the reply on a private inbox. `SUB` + `UNSUB 1`
/// auto-removes the subscription after the single reply, so a pooled connection
/// is left clean for reuse.
fn do_request(
    io: &mut dyn NatsIo,
    subject: &str,
    body: &[u8],
    inbox: &str,
    sid: &str,
) -> Result<Vec<u8>, String> {
    io.send(&encode_sub(inbox, sid))?;
    io.send(&encode_unsub(sid, 1))?;
    io.send(&encode_pub(subject, Some(inbox), body))?;
    loop {
        match io.read_frame()? {
            Frame::Msg { sid: got, payload } if got == sid => return Ok(payload),
            Frame::Err(e) => return Err(e),
            Frame::Ping => io.send(b"PONG\r\n")?,
            // Ignore anything that is not our reply (stale MSG, +OK, INFO).
            Frame::Msg { .. } | Frame::Ok | Frame::Info | Frame::Pong => continue,
        }
    }
}

/// Perform one operation on `io`. Returns the reply body (empty for publish).
fn perform(io: &mut dyn NatsIo, op: &Op, id: u64) -> Result<Vec<u8>, String> {
    match op {
        Op::Publish {
            subject,
            reply_to,
            body,
        } => do_publish(io, subject, reply_to.as_deref(), body).map(|()| Vec::new()),
        Op::Request { subject, body } => {
            let inbox = format!("_INBOX.{:x}.{:x}", std::process::id(), id);
            let sid = id.to_string();
            do_request(io, subject, body, &inbox, &sid)
        }
    }
}

// ---------------------------------------------------------------------------
// Connection pool.
// ---------------------------------------------------------------------------

/// Idle connection pool keyed by [`Target::pool_key`]. A `Vec` is a simple LIFO
/// free-list: an operation checks out an idle connection (or makes a fresh one)
/// and returns it on success, so concurrent VUs reuse sockets per server.
#[allow(clippy::type_complexity)]
fn pools() -> &'static Mutex<HashMap<String, Vec<Box<dyn NatsIo>>>> {
    static POOLS: OnceLock<Mutex<HashMap<String, Vec<Box<dyn NatsIo>>>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn checkout(key: &str) -> Option<Box<dyn NatsIo>> {
    let mut guard = pools().lock().ok()?;
    guard.get_mut(key)?.pop()
}

fn checkin(key: &str, conn: Box<dyn NatsIo>) {
    if let Ok(mut guard) = pools().lock() {
        guard.entry(key.to_string()).or_default().push(conn);
    }
}

/// A monotonic id used to build unique inbox subjects / subscription ids.
fn next_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Run one operation, checking out / re-establishing a pooled connection. On a
/// transport failure of a pooled connection, transparently reconnect once.
fn run(
    factory: &dyn ConnFactory,
    key: &str,
    timeout: Option<Duration>,
    op: &Op,
    id: u64,
) -> Result<Vec<u8>, String> {
    // Try a pooled connection first; on any transport error it is dropped (not
    // returned) and we fall through to a fresh connect.
    if let Some(mut conn) = checkout(key) {
        if conn.set_read_timeout(timeout).is_ok() {
            if let Ok(body) = perform(conn.as_mut(), op, id) {
                let _ = conn.set_read_timeout(None);
                checkin(key, conn);
                return Ok(body);
            }
        }
        // Drop the dead/aborted connection; reconnect below.
    }

    let mut conn = factory.connect()?;
    conn.set_read_timeout(timeout)?;
    let body = perform(conn.as_mut(), op, id)?;
    let _ = conn.set_read_timeout(None);
    checkin(key, conn);
    Ok(body)
}

// ---------------------------------------------------------------------------
// FFI handler.
// ---------------------------------------------------------------------------

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

/// A successful operation → `status = 0`.
fn ok_response(op: &Op, body: Vec<u8>, latency_ms: f64) -> FfiResponse {
    FfiResponse {
        status: 0,
        status_text: "OK".to_string(),
        headers: Vec::new(),
        body_b64: base64_encode(&body),
        duration_ms: latency_ms,
        error: None,
        extras: serde_json::json!({
            "operation": op.name(),
            "subject": op.subject(),
        }),
    }
}

/// A failed operation (server `-ERR`, timeout, transport error) → `status = 1`.
fn error_response(latency_ms: f64, error: String) -> FfiResponse {
    FfiResponse {
        status: 1,
        status_text: "ERROR".to_string(),
        headers: Vec::new(),
        body_b64: String::new(),
        duration_ms: latency_ms,
        error: Some(error),
        extras: serde_json::json!({}),
    }
}

fn handle(request_json: &str) -> FfiResponse {
    let started = Instant::now();
    let request: FfiRequest = match serde_json::from_str(request_json) {
        Ok(r) => r,
        Err(e) => return error_response(0.0, format!("invalid request JSON: {e}")),
    };
    let target = match parse_target(&request.url) {
        Ok(t) => t,
        Err(e) => return error_response(elapsed_ms(started), e),
    };
    let op = match parse_op(&request) {
        Ok(o) => o,
        Err(e) => return error_response(elapsed_ms(started), e),
    };
    let timeout = match request.timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
    let factory = TcpConnFactory {
        addr: target.addr.clone(),
        creds: target.creds.clone(),
    };
    let id = next_id();
    match run(&factory, &target.pool_key(), timeout, &op, id) {
        Ok(body) => ok_response(&op, body, elapsed_ms(started)),
        Err(e) => error_response(elapsed_ms(started), e),
    }
}

// ---------------------------------------------------------------------------
// base64 (self-contained, standard alphabet) — the host base64-encodes request
// bodies and expects base64 response bodies. Avoids pulling in a crate.
// ---------------------------------------------------------------------------

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = input
        .bytes()
        .filter(|b| *b != b'\n' && *b != b'\r')
        .collect();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        let mut pad = 0;
        for (i, &b) in chunk.iter().enumerate() {
            if b == b'=' {
                pad += 1;
                buf[i] = 0;
            } else {
                buf[i] = val(b).ok_or_else(|| "invalid base64 in request body".to_string())?;
            }
        }
        let n = (u32::from(buf[0]) << 18)
            | (u32::from(buf[1]) << 12)
            | (u32::from(buf[2]) << 6)
            | u32::from(buf[3]);
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

fn base64_encode(input: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHA[(n >> 18 & 0x3f) as usize] as char);
        out.push(ALPHA[(n >> 12 & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHA[(n >> 6 & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHA[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

// ---------------------------------------------------------------------------
// ABI export.
// ---------------------------------------------------------------------------

struct NatsProto;

impl FfiProtocol for NatsProto {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn execute(&self, request_json: RString) -> RString {
        let resp = handle(request_json.as_str());
        match serde_json::to_string(&resp) {
            Ok(json) => RString::from(json),
            Err(e) => RString::from(format!(
                "{{\"status\":1,\"error\":\"cannot encode response: {e}\"}}"
            )),
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "protocol",
            "description": "NATS protocol: publish and request/reply over a pooled TCP connection",
            "schemes": ["nats"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(NatsProto, abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RSome(make_protocol),
        make_service: RNone,
        make_data_source: RNone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    // -- test seams ----------------------------------------------------------

    /// A scripted [`NatsIo`]: replays queued frames and records what was sent,
    /// so the operation logic is exercised without a socket.
    struct MockIo {
        frames: VecDeque<Result<Frame, String>>,
        sent: Vec<Vec<u8>>,
    }

    /// Build a `MockIo` from scripted frames. Not named `new` on purpose.
    fn mock_io(frames: Vec<Result<Frame, String>>) -> MockIo {
        MockIo {
            frames: VecDeque::from(frames),
            sent: Vec::new(),
        }
    }

    impl NatsIo for MockIo {
        fn send(&mut self, buf: &[u8]) -> Result<(), String> {
            self.sent.push(buf.to_vec());
            Ok(())
        }

        fn read_frame(&mut self) -> Result<Frame, String> {
            self.frames
                .pop_front()
                .unwrap_or_else(|| Err("mock: no more scripted frames".to_string()))
        }

        fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), String> {
            Ok(())
        }
    }

    /// Hands out pre-built [`MockIo`]s in order, counting connects so tests can
    /// assert that a reconnect happened.
    struct MockFactory {
        scripts: Mutex<VecDeque<Vec<Result<Frame, String>>>>,
        connects: Arc<AtomicUsize>,
    }

    /// Build a factory over scripted per-connection frame lists.
    fn mock_factory(
        scripts: Vec<Vec<Result<Frame, String>>>,
    ) -> (Box<dyn ConnFactory>, Arc<AtomicUsize>) {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = MockFactory {
            scripts: Mutex::new(VecDeque::from(scripts)),
            connects: connects.clone(),
        };
        (Box::new(factory), connects)
    }

    impl ConnFactory for MockFactory {
        fn connect(&self) -> Result<Box<dyn NatsIo>, String> {
            self.connects.fetch_add(1, Ordering::Relaxed);
            match self.scripts.lock().unwrap().pop_front() {
                Some(frames) => Ok(Box::new(mock_io(frames))),
                None => Err("mock: no more scripted connections".to_string()),
            }
        }
    }

    fn sent_str(io: &MockIo, idx: usize) -> String {
        String::from_utf8_lossy(&io.sent[idx]).into_owned()
    }

    fn req(url: &str, plugin: Option<serde_json::Value>) -> FfiRequest {
        FfiRequest {
            name: "n".into(),
            method: "POST".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: plugin,
            config: serde_json::Value::Null,
        }
    }

    // -- target parsing ------------------------------------------------------

    #[test]
    fn parses_nats_urls() {
        let t = parse_target("nats://127.0.0.1:4222").unwrap();
        assert_eq!(t.addr, "127.0.0.1:4222");
        assert_eq!(t.creds, None);
        assert_eq!(t.pool_key(), "127.0.0.1:4222");

        // Default port when omitted.
        let t = parse_target("nats://msg.example.com").unwrap();
        assert_eq!(t.addr, "msg.example.com:4222");

        // Userinfo → credentials, and the user namespaces the pool key.
        let t = parse_target("nats://alice:secret@host:4300").unwrap();
        assert_eq!(t.addr, "host:4300");
        assert_eq!(
            t.creds,
            Some(Creds {
                user: "alice".into(),
                pass: Some("secret".into()),
            })
        );
        assert_eq!(t.pool_key(), "alice@host:4300");

        // User without password.
        let t = parse_target("nats://token@host").unwrap();
        assert_eq!(
            t.creds,
            Some(Creds {
                user: "token".into(),
                pass: None,
            })
        );
    }

    #[test]
    fn rejects_bad_targets() {
        assert!(parse_target("redis://h:6379").is_err());
        assert!(parse_target("not a url").is_err());
    }

    // -- operation parsing ---------------------------------------------------

    #[test]
    fn parse_op_defaults_to_publish() {
        let r = req("nats://h", Some(serde_json::json!({ "subject": "events" })));
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Publish {
                subject: "events".into(),
                reply_to: None,
                body: Vec::new(),
            }
        );
    }

    #[test]
    fn parse_op_publish_with_reply_and_string_body() {
        let r = req(
            "nats://h",
            Some(serde_json::json!({
                "operation": "publish",
                "subject": "events",
                "reply_to": "inbox.1",
                "body": "hello",
            })),
        );
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Publish {
                subject: "events".into(),
                reply_to: Some("inbox.1".into()),
                body: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn parse_op_request_with_json_body_compacted() {
        let r = req(
            "nats://h",
            Some(serde_json::json!({
                "operation": "request",
                "subject": "rpc.echo",
                "body": { "a": 1 },
            })),
        );
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Request {
                subject: "rpc.echo".into(),
                body: br#"{"a":1}"#.to_vec(),
            }
        );
    }

    #[test]
    fn parse_op_body_falls_back_to_request_body() {
        let mut r = req("nats://h", Some(serde_json::json!({ "subject": "s" })));
        r.body_b64 = base64_encode(b"payload");
        match parse_op(&r).unwrap() {
            Op::Publish { body, .. } => assert_eq!(body, b"payload"),
            other => panic!("expected publish, got {other:?}"),
        }
    }

    #[test]
    fn parse_op_missing_subject_is_error() {
        let r = req(
            "nats://h",
            Some(serde_json::json!({ "operation": "publish" })),
        );
        assert_eq!(
            parse_op(&r).unwrap_err(),
            "nats plugin requires a non-empty `subject`"
        );
    }

    #[test]
    fn parse_op_unknown_operation_is_error() {
        let r = req(
            "nats://h",
            Some(serde_json::json!({ "operation": "flush", "subject": "s" })),
        );
        assert!(parse_op(&r).unwrap_err().contains("unknown nats operation"));
    }

    // -- wire encoding -------------------------------------------------------

    #[test]
    fn encodes_pub() {
        assert_eq!(
            encode_pub("events.ingest", None, b"hi"),
            b"PUB events.ingest 2\r\nhi\r\n"
        );
        assert_eq!(
            encode_pub("rpc.echo", Some("_INBOX.1"), b"ping"),
            b"PUB rpc.echo _INBOX.1 4\r\nping\r\n"
        );
        // Empty payload still declares a zero length.
        assert_eq!(encode_pub("s", None, b""), b"PUB s 0\r\n\r\n");
    }

    #[test]
    fn encodes_sub_and_unsub() {
        assert_eq!(encode_sub("_INBOX.9", "7"), b"SUB _INBOX.9 7\r\n");
        assert_eq!(encode_unsub("7", 1), b"UNSUB 7 1\r\n");
    }

    #[test]
    fn encodes_connect_with_and_without_creds() {
        let plain = String::from_utf8(encode_connect(None)).unwrap();
        assert!(plain.starts_with("CONNECT "));
        assert!(plain.ends_with("\r\n"));
        let json: serde_json::Value =
            serde_json::from_str(plain.trim_start_matches("CONNECT ").trim()).unwrap();
        assert_eq!(json["verbose"], false);
        assert_eq!(json["lang"], "rust");
        assert!(json.get("user").is_none());

        let creds = Creds {
            user: "alice".into(),
            pass: Some("secret".into()),
        };
        let with = String::from_utf8(encode_connect(Some(&creds))).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(with.trim_start_matches("CONNECT ").trim()).unwrap();
        assert_eq!(json["user"], "alice");
        assert_eq!(json["pass"], "secret");
    }

    // -- frame reading -------------------------------------------------------

    #[test]
    fn reads_control_frames() {
        assert_eq!(read_frame(&mut &b"PING\r\n"[..]).unwrap(), Frame::Ping);
        assert_eq!(read_frame(&mut &b"PONG\r\n"[..]).unwrap(), Frame::Pong);
        assert_eq!(read_frame(&mut &b"+OK\r\n"[..]).unwrap(), Frame::Ok);
        assert_eq!(
            read_frame(&mut &b"-ERR 'Unknown Protocol Operation'\r\n"[..]).unwrap(),
            Frame::Err("Unknown Protocol Operation".into())
        );
        assert_eq!(
            read_frame(&mut &b"INFO {\"server_id\":\"x\"}\r\n"[..]).unwrap(),
            Frame::Info
        );
    }

    #[test]
    fn reads_msg_frames() {
        assert_eq!(
            read_frame(&mut &b"MSG rpc.echo 7 4\r\npong\r\n"[..]).unwrap(),
            Frame::Msg {
                sid: "7".into(),
                payload: b"pong".to_vec(),
            }
        );
        // A MSG with a reply-to field: only the sid and payload are retained.
        assert_eq!(
            read_frame(&mut &b"MSG s 2 _INBOX.9 2\r\nhi\r\n"[..]).unwrap(),
            Frame::Msg {
                sid: "2".into(),
                payload: b"hi".to_vec(),
            }
        );
    }

    #[test]
    fn rejects_malformed_frames() {
        assert!(read_frame(&mut &b"MSG only two\r\n"[..]).is_err());
        assert!(read_frame(&mut &b"WAT something\r\n"[..]).is_err());
    }

    // -- publish orchestration ----------------------------------------------

    #[test]
    fn publish_flushes_and_confirms() {
        let mut io = mock_io(vec![Ok(Frame::Pong)]);
        do_publish(&mut io, "events", None, b"hi").unwrap();
        // First the PUB, then the PING flush.
        assert_eq!(sent_str(&io, 0), "PUB events 2\r\nhi\r\n");
        assert_eq!(sent_str(&io, 1), "PING\r\n");
    }

    #[test]
    fn publish_answers_server_ping_before_pong() {
        let mut io = mock_io(vec![Ok(Frame::Ping), Ok(Frame::Pong)]);
        do_publish(&mut io, "s", None, b"x").unwrap();
        // PUB, PING, then the PONG answer to the server's keepalive PING.
        assert_eq!(sent_str(&io, 2), "PONG\r\n");
    }

    #[test]
    fn publish_reports_server_error() {
        let mut io = mock_io(vec![Ok(Frame::Err("Permissions Violation".into()))]);
        assert_eq!(
            do_publish(&mut io, "s", None, b"x").unwrap_err(),
            "Permissions Violation"
        );
    }

    #[test]
    fn publish_propagates_transport_error() {
        let mut io = mock_io(vec![Err("read failed: broken pipe".into())]);
        assert!(do_publish(&mut io, "s", None, b"x").is_err());
    }

    // -- request orchestration ----------------------------------------------

    #[test]
    fn request_subscribes_publishes_and_returns_reply() {
        let mut io = mock_io(vec![Ok(Frame::Msg {
            sid: "5".into(),
            payload: b"pong".to_vec(),
        })]);
        let body = do_request(&mut io, "rpc.echo", b"ping", "_INBOX.5", "5").unwrap();
        assert_eq!(body, b"pong");
        assert_eq!(sent_str(&io, 0), "SUB _INBOX.5 5\r\n");
        assert_eq!(sent_str(&io, 1), "UNSUB 5 1\r\n");
        assert_eq!(sent_str(&io, 2), "PUB rpc.echo _INBOX.5 4\r\nping\r\n");
    }

    #[test]
    fn request_ignores_mismatched_sid() {
        let mut io = mock_io(vec![
            Ok(Frame::Msg {
                sid: "99".into(),
                payload: b"nope".to_vec(),
            }),
            Ok(Frame::Msg {
                sid: "5".into(),
                payload: b"yes".to_vec(),
            }),
        ]);
        let body = do_request(&mut io, "s", b"q", "_INBOX.5", "5").unwrap();
        assert_eq!(body, b"yes");
    }

    #[test]
    fn request_reports_error() {
        let mut io = mock_io(vec![Ok(Frame::Err("No Responders".into()))]);
        assert_eq!(
            do_request(&mut io, "s", b"q", "_INBOX.5", "5").unwrap_err(),
            "No Responders"
        );
    }

    // -- handshake -----------------------------------------------------------

    #[test]
    fn handshake_reads_info_sends_connect_and_flushes() {
        let mut io = mock_io(vec![Ok(Frame::Info), Ok(Frame::Pong)]);
        client_handshake(&mut io, None).unwrap();
        assert!(sent_str(&io, 0).starts_with("CONNECT "));
        assert_eq!(sent_str(&io, 1), "PING\r\n");
    }

    #[test]
    fn handshake_without_info_is_error() {
        let mut io = mock_io(vec![Ok(Frame::Pong)]);
        assert!(client_handshake(&mut io, None).is_err());
    }

    // -- run + pool retry ----------------------------------------------------

    #[test]
    fn run_connects_and_performs_publish() {
        let (factory, connects) = mock_factory(vec![vec![Ok(Frame::Pong)]]);
        let key = "run_connects_and_performs_publish";
        let op = Op::Publish {
            subject: "s".into(),
            reply_to: None,
            body: b"x".to_vec(),
        };
        let body = run(factory.as_ref(), key, None, &op, 1).unwrap();
        assert!(body.is_empty());
        assert_eq!(connects.load(Ordering::Relaxed), 1);
        // The healthy connection is returned to the pool for reuse.
        assert!(checkout(key).is_some());
    }

    #[test]
    fn run_reconnects_when_pooled_connection_fails() {
        // A pooled connection whose first read is a transport error, then a
        // fresh connection that succeeds → one retry, two connects total.
        let (factory, connects) =
            mock_factory(vec![vec![Err("dead socket".into())], vec![Ok(Frame::Pong)]]);
        let key = "run_reconnects_when_pooled_connection_fails";
        let op = Op::Publish {
            subject: "s".into(),
            reply_to: None,
            body: b"x".to_vec(),
        };
        // Seed the pool with the first (doomed) connection.
        checkin(key, factory.connect().unwrap());
        assert_eq!(connects.load(Ordering::Relaxed), 1);

        run(factory.as_ref(), key, None, &op, 1).unwrap();
        // Pooled conn failed → dropped → one fresh reconnect.
        assert_eq!(connects.load(Ordering::Relaxed), 2);
    }

    // -- responses -----------------------------------------------------------

    #[test]
    fn ok_response_encodes_body_and_extras() {
        let op = Op::Request {
            subject: "rpc.echo".into(),
            body: Vec::new(),
        };
        let resp = ok_response(&op, b"pong".to_vec(), 1.5);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_none());
        assert_eq!(base64_decode(&resp.body_b64).unwrap(), b"pong");
        assert_eq!(resp.extras["operation"], "request");
        assert_eq!(resp.extras["subject"], "rpc.echo");
    }

    #[test]
    fn error_response_sets_status_one() {
        let resp = error_response(2.0, "boom".into());
        assert_eq!(resp.status, 1);
        assert_eq!(resp.error.as_deref(), Some("boom"));
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 1);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req("redis://h:6379", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 1);
        assert!(resp.error.unwrap().contains("cannot handle scheme"));
    }

    #[test]
    fn info_declares_scheme() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "nats");
        assert_eq!(v["schemes"][0], "nats");
    }

    #[test]
    fn base64_roundtrips() {
        for s in ["", "a", "ab", "abc", "abcd", "ping", "hello world"] {
            let enc = base64_encode(s.as_bytes());
            let dec = base64_decode(&enc).unwrap();
            assert_eq!(dec, s.as_bytes(), "roundtrip {s:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Integration: a real NATS server. Skips unless the env var is set.
    //   docker run -p 4222:4222 nats:latest
    //   LOADR_TEST_NATS_URL=nats://127.0.0.1:4222 cargo test -p loadr-plugin-nats
    // -----------------------------------------------------------------------

    #[test]
    fn nats_publish_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_NATS_URL") else {
            eprintln!("skipping: LOADR_TEST_NATS_URL not set");
            return;
        };
        let r = req(
            &url,
            Some(serde_json::json!({
                "operation": "publish",
                "subject": format!("loadr.test.{}", std::process::id()),
                "body": "hello",
            })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert!(resp.error.is_none(), "publish error: {:?}", resp.error);
        assert_eq!(resp.status, 0);
    }

    #[test]
    fn nats_connection_failure_is_reported() {
        if std::env::var("LOADR_TEST_NATS_URL").is_err() {
            eprintln!("skipping: LOADR_TEST_NATS_URL not set");
            return;
        }
        // Port 1 is never a NATS server; the driver reports an error, not a panic.
        let r = req(
            "nats://127.0.0.1:1",
            Some(serde_json::json!({ "subject": "s", "body": "x" })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert_eq!(resp.status, 1);
        assert!(resp.error.is_some());
    }
}
