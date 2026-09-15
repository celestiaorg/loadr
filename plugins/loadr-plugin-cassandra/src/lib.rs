//! `loadr-plugin-cassandra` — a native protocol plugin that adds **Apache
//! Cassandra** and **ScyllaDB** as a loadr load-test target by speaking the
//! **CQL v4 binary protocol** directly over a raw TCP socket.
//!
//! # How it plugs in
//!
//! loadr's native protocol ABI ([`FfiProtocol`]) is synchronous: the host calls
//! `execute(&self, request_json) -> response_json` on a single shared plugin
//! instance (`Send + Sync`), created once via `make_protocol()`. There is no
//! per-VU state across the FFI boundary, so this plugin owns all of its own
//! machinery:
//!
//! * The CQL client is **hand-rolled** over a blocking `std::net::TcpStream` —
//!   there is no `scylla` / `cassandra-cpp` driver and therefore no async
//!   runtime and no C toolchain / OpenSSL dependency, so this cdylib
//!   cross-compiles cleanly for every release target. The frame header
//!   (`version`/`flags`/`stream`/`opcode`/`length`), the `STARTUP` -> `READY`
//!   handshake, the `QUERY` request (opcode `0x07`) and the `RESULT` response
//!   (opcode `0x08`) are all encoded/parsed by this crate.
//! * An internal connection pool keyed by `host:port/keyspace`:
//!   `OnceLock<Mutex<HashMap<String, Vec<Box<dyn CqlIo>>>>>`. A request checks
//!   out an idle connection (running the `STARTUP`/`READY` handshake — and a
//!   `USE <keyspace>` — only on a fresh socket), reuses it for one statement,
//!   and returns it for the next caller. A connection left in a transport-error
//!   state is dropped instead of returned, so the next caller transparently
//!   reconnects.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. The
//! connection target comes from `url` (`cql://host[:port]/keyspace`) and the
//! statement from the request's `plugin:` block (`options`), either at the top
//! level or nested under a `cql` key:
//!
//! * `query`       — the CQL statement to run (positional `?` placeholders).
//! * `params`      — positional bind values (text; type inferred).
//! * `consistency` — the CQL consistency level (default `one`).
//!
//! As a shorthand, a request with no `query` uses its **body** as the statement
//! text (no params); an empty statement is rejected.
//!
//! The response is JSON `{ status, status_text, body_b64, duration_ms, error,
//! extras }`. Following the plugin's documented contract, `status` is **`1`** on
//! success (the statement executed) and **`0`** on failure (a CQL error, or a
//! connection/transport failure), with `error` set. The response body is the
//! row count rendered as text; `extras.rows` carries the same count and
//! `extras.backend` is `cassandra`. The host derives `cassandra_reqs` /
//! `cassandra_req_duration` from the `cassandra` plugin name.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use base64::Engine as _;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "cassandra";

/// The CQL native-protocol port, used when the URL omits one.
const DEFAULT_PORT: u16 = 9042;

// CQL binary-protocol opcodes (the subset this plugin uses).
const OP_ERROR: u8 = 0x00;
const OP_STARTUP: u8 = 0x01;
const OP_READY: u8 = 0x02;
const OP_AUTHENTICATE: u8 = 0x03;
const OP_QUERY: u8 = 0x07;
const OP_RESULT: u8 = 0x08;

/// Request-direction frame version byte (protocol v4).
const REQUEST_VERSION: u8 = 0x04;

/// A single request/response cycle runs alone on a pooled connection, so a fixed
/// stream id is sufficient.
const STREAM: i16 = 0;

/// CQL consistency level `ONE`, the default when none is requested.
const CONSISTENCY_ONE: u16 = 0x0001;

// ---------------------------------------------------------------------------
// Target parsing.
// ---------------------------------------------------------------------------

/// A parsed connection target.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    /// `host:port`, the TCP dial address.
    addr: String,
    /// The session keyspace selected by the URL path (may be empty).
    keyspace: String,
}

impl Target {
    /// Pool key: the dial address namespaced by keyspace, so sessions bound to
    /// different keyspaces on the same node never share a socket.
    fn pool_key(&self) -> String {
        format!("{}/{}", self.addr, self.keyspace)
    }
}

/// Parse and validate the `cql://host[:port]/keyspace` target URL.
fn parse_target(raw: &str) -> Result<Target, String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    if url.scheme() != "cql" {
        return Err(format!(
            "cassandra plugin cannot handle scheme `{}`",
            url.scheme()
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("`{raw}` has no host"))?
        .to_string();
    let port = url.port().unwrap_or(DEFAULT_PORT);
    let addr = format!("{host}:{port}");
    // The first path segment names the session keyspace (may be absent).
    let keyspace = url
        .path()
        .trim_matches('/')
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();
    Ok(Target { addr, keyspace })
}

// ---------------------------------------------------------------------------
// Statement parsing (the request's `plugin:` block).
// ---------------------------------------------------------------------------

/// A single bind value, already serialised to CQL wire bytes (or an explicit
/// null).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Bind {
    Null,
    Value(Vec<u8>),
}

/// One CQL statement described by the request.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Op {
    query: String,
    consistency: u16,
    values: Vec<Bind>,
}

/// Look up a field either at the top level of the `plugin:` block or nested
/// under a `cql` key, so both `plugin: { query: .. }` and
/// `plugin: { cql: { query: .. } }` work.
fn opt_field<'a>(opts: Option<&'a serde_json::Value>, name: &str) -> Option<&'a serde_json::Value> {
    let opts = opts?;
    if let Some(v) = opts.get(name) {
        return Some(v);
    }
    opts.get("cql").and_then(|c| c.get(name))
}

/// Infer a CQL wire encoding for a textual bind value: an integer binds as an
/// `int` (or `bigint` if it overflows 32 bits), a decimal as a `double`,
/// everything else as UTF-8 text. Matches the documented type inference.
fn infer_value(text: &str) -> Vec<u8> {
    if let Ok(i) = text.parse::<i32>() {
        return i.to_be_bytes().to_vec();
    }
    if let Ok(i) = text.parse::<i64>() {
        return i.to_be_bytes().to_vec();
    }
    if let Ok(f) = text.parse::<f64>() {
        return f.to_be_bytes().to_vec();
    }
    text.as_bytes().to_vec()
}

/// Turn a JSON param into a [`Bind`]. Strings and numbers infer a type; an
/// explicit JSON null binds as a CQL null; a bool binds as a CQL `boolean`.
fn bind_value(value: &serde_json::Value) -> Bind {
    match value {
        serde_json::Value::Null => Bind::Null,
        serde_json::Value::String(s) => Bind::Value(infer_value(s)),
        serde_json::Value::Bool(b) => Bind::Value(vec![u8::from(*b)]),
        serde_json::Value::Number(n) => Bind::Value(infer_value(&n.to_string())),
        other => Bind::Value(infer_value(&other.to_string())),
    }
}

/// Map a consistency-level name to its CQL `[consistency]` code.
fn parse_consistency(name: &str) -> Result<u16, String> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "any" => 0x0000,
        "one" => 0x0001,
        "two" => 0x0002,
        "three" => 0x0003,
        "quorum" => 0x0004,
        "all" => 0x0005,
        "local_quorum" | "local-quorum" => 0x0006,
        "each_quorum" | "each-quorum" => 0x0007,
        "serial" => 0x0008,
        "local_serial" | "local-serial" => 0x0009,
        "local_one" | "local-one" => 0x000a,
        other => return Err(format!("unknown cql consistency `{other}`")),
    })
}

/// Build the [`Op`] from the request's `plugin:` block, falling back to the body
/// for the statement text.
fn parse_op(request: &FfiRequest) -> Result<Op, String> {
    let opts = request.options.as_ref();
    let query = match opt_field(opts, "query").and_then(serde_json::Value::as_str) {
        Some(q) if !q.trim().is_empty() => q.to_string(),
        _ => {
            let body = if request.body_b64.is_empty() {
                String::new()
            } else {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&request.body_b64)
                    .map_err(|e| format!("invalid body base64: {e}"))?;
                String::from_utf8(bytes)
                    .map_err(|_| "request body is not valid UTF-8 CQL".to_string())?
            };
            if body.trim().is_empty() {
                return Err(
                    "cassandra plugin requires a `cql.query` (or a non-empty body)".to_string(),
                );
            }
            body
        }
    };
    let consistency = match opt_field(opts, "consistency").and_then(serde_json::Value::as_str) {
        Some(c) => parse_consistency(c)?,
        None => CONSISTENCY_ONE,
    };
    let values = match opt_field(opts, "params") {
        Some(serde_json::Value::Array(arr)) => arr.iter().map(bind_value).collect(),
        Some(serde_json::Value::Null) | None => Vec::new(),
        Some(other) => return Err(format!("cql `params` must be an array, got `{other}`")),
    };
    Ok(Op {
        query,
        consistency,
        values,
    })
}

// ---------------------------------------------------------------------------
// Wire encoding — frame header + CQL primitive types.
// ---------------------------------------------------------------------------

/// Append a CQL `[string]` (2-byte length + UTF-8).
fn write_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Append a CQL `[long string]` (4-byte length + UTF-8).
fn write_long_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Encode a complete CQL request frame: the 9-byte header
/// (`version`/`flags`/`stream`/`opcode`/`length`) followed by `body`.
fn encode_frame(opcode: u8, stream: i16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + body.len());
    out.push(REQUEST_VERSION);
    out.push(0x00); // flags: no compression, no tracing
    out.extend_from_slice(&stream.to_be_bytes());
    out.push(opcode);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// The `STARTUP` body: a `[string map]` requesting CQL 3.0.0, uncompressed.
fn startup_body() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes()); // one entry
    write_string(&mut body, "CQL_VERSION");
    write_string(&mut body, "3.0.0");
    body
}

/// Encode a `QUERY` body: `[long string]` statement, `[consistency]`, a single
/// v4 flags byte, then the optional bound values (`[short]` count + `[bytes]`
/// values, `-1` length for a null).
fn encode_query_body(op: &Op) -> Vec<u8> {
    let mut body = Vec::new();
    write_long_string(&mut body, &op.query);
    body.extend_from_slice(&op.consistency.to_be_bytes());
    let flags: u8 = if op.values.is_empty() { 0x00 } else { 0x01 };
    body.push(flags);
    if !op.values.is_empty() {
        body.extend_from_slice(&(op.values.len() as u16).to_be_bytes());
        for value in &op.values {
            match value {
                Bind::Null => body.extend_from_slice(&(-1i32).to_be_bytes()),
                Bind::Value(bytes) => {
                    body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    body.extend_from_slice(bytes);
                }
            }
        }
    }
    body
}

// ---------------------------------------------------------------------------
// Wire decoding — a raw frame + a cursor over CQL primitive types.
// ---------------------------------------------------------------------------

/// A decoded CQL response frame. Only the fields the plugin acts on are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawFrame {
    opcode: u8,
    body: Vec<u8>,
}

/// Read one complete CQL frame (9-byte header + declared body) from `reader`.
fn read_raw_frame<R: Read>(reader: &mut R) -> Result<RawFrame, String> {
    let mut header = [0u8; 9];
    reader
        .read_exact(&mut header)
        .map_err(|e| format!("read failed: {e}"))?;
    let opcode = header[4];
    let len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .map_err(|e| format!("read failed: {e}"))?;
    Ok(RawFrame { opcode, body })
}

/// A forward-only cursor over a CQL frame body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| "cql frame length overflow".to_string())?;
        if end > self.buf.len() {
            return Err("truncated cql frame".to_string());
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn short(&mut self) -> Result<u16, String> {
        let s = self.take(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }

    fn int(&mut self) -> Result<i32, String> {
        let s = self.take(4)?;
        Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }

    /// Read a CQL `[string]` (2-byte length + UTF-8).
    fn string(&mut self) -> Result<String, String> {
        let n = self.short()? as usize;
        let bytes = self.take(n)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8 in cql string".to_string())
    }

    /// Skip a CQL `[bytes]` value (4-byte length; `-1` is null).
    fn skip_bytes(&mut self) -> Result<(), String> {
        let n = self.int()?;
        if n >= 0 {
            self.take(n as usize)?;
        }
        Ok(())
    }
}

/// Skip a CQL `<type>` option (`[short]` id plus a type-dependent value) so the
/// cursor lands on the row count that follows the rows metadata.
fn skip_type(reader: &mut Reader) -> Result<(), String> {
    let id = reader.short()?;
    match id {
        0x0000 => {
            reader.string()?; // custom: a class name [string]
        }
        0x0020 | 0x0022 => skip_type(reader)?, // list / set: one element type
        0x0021 => {
            // map: key type + value type
            skip_type(reader)?;
            skip_type(reader)?;
        }
        0x0030 => {
            // UDT: keyspace, name, then <n> (field name, type) pairs
            reader.string()?;
            reader.string()?;
            let n = reader.short()?;
            for _ in 0..n {
                reader.string()?;
                skip_type(reader)?;
            }
        }
        0x0031 => {
            // tuple: <n> element types
            let n = reader.short()?;
            for _ in 0..n {
                skip_type(reader)?;
            }
        }
        _ => {} // primitive type: no trailing value
    }
    Ok(())
}

/// Parse a `RESULT` of kind `Rows`, returning the row count. The rows metadata
/// is skipped (only its shape matters) to reach the `[int]` row count.
fn parse_rows(reader: &mut Reader) -> Result<i64, String> {
    let flags = reader.int()?;
    let col_count = reader.int()?;
    if flags & 0x0002 != 0 {
        reader.skip_bytes()?; // paging state
    }
    if flags & 0x0004 == 0 {
        // Metadata (column specs) present.
        let global = flags & 0x0001 != 0;
        if global {
            reader.string()?; // keyspace
            reader.string()?; // table
        }
        if col_count < 0 {
            return Err("negative cql column count".to_string());
        }
        for _ in 0..col_count {
            if !global {
                reader.string()?; // per-column keyspace
                reader.string()?; // per-column table
            }
            reader.string()?; // column name
            skip_type(reader)?;
        }
    }
    let rows = reader.int()?;
    Ok(i64::from(rows.max(0)))
}

/// Parse a `RESULT` frame body, returning the row count (0 for non-row results
/// such as writes / `USE`).
fn parse_result(body: &[u8]) -> Result<i64, String> {
    let mut reader = Reader::new(body);
    let kind = reader.int()?;
    match kind {
        0x0001 => Ok(0),                   // Void (writes)
        0x0002 => parse_rows(&mut reader), // Rows
        0x0003 => Ok(0),                   // Set_keyspace
        0x0004 => Ok(0),                   // Prepared
        0x0005 => Ok(0),                   // Schema_change
        other => Err(format!("unexpected cql RESULT kind {other:#x}")),
    }
}

/// Parse an `ERROR` frame body (`[int]` code + `[string]` message) into a
/// human-readable message.
fn parse_error(body: &[u8]) -> String {
    let mut reader = Reader::new(body);
    match (reader.int(), reader.string()) {
        (Ok(code), Ok(msg)) => format!("cql error [0x{code:04x}]: {msg}"),
        _ => "malformed cql ERROR frame".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Connection abstraction — a seam so protocol logic can be unit-tested without
// a real socket.
// ---------------------------------------------------------------------------

/// A live CQL connection: send bytes, read one frame, adjust the read deadline.
/// A returned `Err` is a *transport* failure and drops the socket; a CQL
/// `ERROR` frame surfaces as `Ok(RawFrame { opcode: OP_ERROR, .. })`.
trait CqlIo: Send {
    fn send(&mut self, buf: &[u8]) -> Result<(), String>;
    fn read_frame(&mut self) -> Result<RawFrame, String>;
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String>;
}

/// Creates fresh, handshaken [`CqlIo`] connections.
trait ConnFactory: Send {
    fn connect(&self) -> Result<Box<dyn CqlIo>, String>;
}

/// A real blocking TCP connection to a CQL node.
struct TcpConn {
    reader: BufReader<TcpStream>,
}

impl CqlIo for TcpConn {
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

    fn read_frame(&mut self) -> Result<RawFrame, String> {
        read_raw_frame(&mut self.reader)
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.reader
            .get_ref()
            .set_read_timeout(timeout)
            .map_err(|e| format!("set_read_timeout failed: {e}"))
    }
}

/// Opens handshaken `TcpConn`s to `addr`, binding the session to `keyspace`.
struct TcpConnFactory {
    addr: String,
    keyspace: String,
}

impl ConnFactory for TcpConnFactory {
    fn connect(&self) -> Result<Box<dyn CqlIo>, String> {
        let stream = TcpStream::connect(&self.addr)
            .map_err(|e| format!("connection to {} failed: {e}", self.addr))?;
        let _ = stream.set_nodelay(true);
        let mut conn = TcpConn {
            reader: BufReader::new(stream),
        };
        client_handshake(&mut conn, &self.keyspace)?;
        Ok(Box::new(conn))
    }
}

/// Run the CQL client handshake: `STARTUP` -> `READY`, then `USE <keyspace>` if
/// the URL selected one.
fn client_handshake(io: &mut dyn CqlIo, keyspace: &str) -> Result<(), String> {
    io.send(&encode_frame(OP_STARTUP, STREAM, &startup_body()))?;
    let frame = io.read_frame()?;
    match frame.opcode {
        OP_READY => {}
        OP_AUTHENTICATE => {
            return Err(
                "cql server requires authentication, which this plugin does not support"
                    .to_string(),
            )
        }
        OP_ERROR => return Err(parse_error(&frame.body)),
        other => return Err(format!("unexpected opcode {other:#x} in reply to STARTUP")),
    }
    if !keyspace.is_empty() {
        let use_stmt = format!("USE \"{}\"", keyspace.replace('"', "\"\""));
        let op = Op {
            query: use_stmt,
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        io.send(&encode_frame(OP_QUERY, STREAM, &encode_query_body(&op)))?;
        let frame = io.read_frame()?;
        match frame.opcode {
            OP_RESULT => {}
            OP_ERROR => return Err(parse_error(&frame.body)),
            other => return Err(format!("unexpected opcode {other:#x} selecting keyspace")),
        }
    }
    Ok(())
}

/// The result of running one statement on a healthy connection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// The statement executed; carries the row count (0 for writes).
    Rows(i64),
    /// The server rejected the statement with a CQL `ERROR`; the connection is
    /// still healthy and is returned to the pool.
    CqlError(String),
}

/// Send one `QUERY` and interpret the response. `Err` is a transport failure
/// (drop the socket); `Ok(Outcome::CqlError)` is an application error (keep it).
fn perform(io: &mut dyn CqlIo, op: &Op) -> Result<Outcome, String> {
    io.send(&encode_frame(OP_QUERY, STREAM, &encode_query_body(op)))?;
    let frame = io.read_frame()?;
    match frame.opcode {
        // A malformed RESULT is reported as an application error rather than a
        // transport failure: the socket is still frame-aligned (the body was
        // consumed by length), so re-running would needlessly re-execute writes.
        OP_RESULT => Ok(match parse_result(&frame.body) {
            Ok(rows) => Outcome::Rows(rows),
            Err(e) => Outcome::CqlError(e),
        }),
        OP_ERROR => Ok(Outcome::CqlError(parse_error(&frame.body))),
        other => Err(format!("unexpected opcode {other:#x} in query response")),
    }
}

// ---------------------------------------------------------------------------
// Connection pool.
// ---------------------------------------------------------------------------

/// Idle connection pool keyed by [`Target::pool_key`]. A `Vec` is a LIFO
/// free-list: an operation checks out an idle connection (or makes a fresh one)
/// and returns it on success, so concurrent VUs reuse sockets per target.
#[allow(clippy::type_complexity)]
fn pools() -> &'static Mutex<HashMap<String, Vec<Box<dyn CqlIo>>>> {
    static POOLS: OnceLock<Mutex<HashMap<String, Vec<Box<dyn CqlIo>>>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn checkout(key: &str) -> Option<Box<dyn CqlIo>> {
    let mut guard = pools().lock().ok()?;
    guard.get_mut(key)?.pop()
}

fn checkin(key: &str, conn: Box<dyn CqlIo>) {
    if let Ok(mut guard) = pools().lock() {
        guard.entry(key.to_string()).or_default().push(conn);
    }
}

/// Run one statement, checking out / re-establishing a pooled connection. On a
/// transport failure of a pooled connection, transparently reconnect once. A
/// CQL error keeps the connection (it is healthy) and is *not* retried.
fn run(
    factory: &dyn ConnFactory,
    key: &str,
    timeout: Option<Duration>,
    op: &Op,
) -> Result<Outcome, String> {
    // Try a pooled connection first; on any transport error it is dropped (not
    // returned) and we fall through to a fresh connect.
    if let Some(mut conn) = checkout(key) {
        if conn.set_read_timeout(timeout).is_ok() {
            if let Ok(outcome) = perform(conn.as_mut(), op) {
                let _ = conn.set_read_timeout(None);
                checkin(key, conn);
                return Ok(outcome);
            }
        }
        // Drop the dead/aborted connection; reconnect below.
    }

    let mut conn = factory.connect()?;
    conn.set_read_timeout(timeout)?;
    let outcome = perform(conn.as_mut(), op)?;
    let _ = conn.set_read_timeout(None);
    checkin(key, conn);
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// FFI handler.
// ---------------------------------------------------------------------------

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

/// A successful statement → `status = 1`, body = row count as text.
fn ok_response(rows: i64, latency_ms: f64) -> FfiResponse {
    FfiResponse {
        status: 1,
        status_text: "OK".to_string(),
        headers: Vec::new(),
        body_b64: base64::engine::general_purpose::STANDARD.encode(rows.to_string().as_bytes()),
        duration_ms: latency_ms,
        error: None,
        extras: serde_json::json!({
            "backend": "cassandra",
            "rows": rows,
        }),
    }
}

/// A failed statement (CQL error, timeout, transport error) → `status = 0`.
fn error_response(latency_ms: f64, error: String) -> FfiResponse {
    FfiResponse {
        status: 0,
        status_text: "ERROR".to_string(),
        headers: Vec::new(),
        body_b64: String::new(),
        duration_ms: latency_ms,
        error: Some(error),
        extras: serde_json::json!({ "backend": "cassandra" }),
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
        keyspace: target.keyspace.clone(),
    };
    match run(&factory, &target.pool_key(), timeout, &op) {
        Ok(Outcome::Rows(rows)) => ok_response(rows, elapsed_ms(started)),
        Ok(Outcome::CqlError(msg)) => error_response(elapsed_ms(started), msg),
        Err(transport) => error_response(elapsed_ms(started), transport),
    }
}

// ---------------------------------------------------------------------------
// ABI export.
// ---------------------------------------------------------------------------

struct CassandraProto;

impl FfiProtocol for CassandraProto {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn execute(&self, request_json: RString) -> RString {
        let resp = handle(request_json.as_str());
        match serde_json::to_string(&resp) {
            Ok(json) => RString::from(json),
            Err(e) => RString::from(format!(
                "{{\"status\":0,\"error\":\"cannot encode response: {e}\"}}"
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
            "description": "Cassandra / ScyllaDB (CQL v4): one QUERY per request over a pooled TCP socket",
            "schemes": ["cql"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(CassandraProto, abi_stable::erased_types::TD_Opaque)
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // -- test seams ----------------------------------------------------------

    /// A scripted [`CqlIo`]: replays queued frames and records what was sent.
    struct MockIo {
        frames: VecDeque<Result<RawFrame, String>>,
        sent: Vec<Vec<u8>>,
    }

    /// Build a `MockIo` from scripted frames. Not named `new` on purpose.
    fn mock_io(frames: Vec<Result<RawFrame, String>>) -> MockIo {
        MockIo {
            frames: VecDeque::from(frames),
            sent: Vec::new(),
        }
    }

    impl CqlIo for MockIo {
        fn send(&mut self, buf: &[u8]) -> Result<(), String> {
            self.sent.push(buf.to_vec());
            Ok(())
        }

        fn read_frame(&mut self) -> Result<RawFrame, String> {
            self.frames
                .pop_front()
                .unwrap_or_else(|| Err("mock: no more scripted frames".to_string()))
        }

        fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), String> {
            Ok(())
        }
    }

    /// Hands out pre-built [`MockIo`]s in order, counting connects so tests can
    /// assert that a reconnect happened. Does NOT run the handshake (the scripts
    /// carry only the per-operation frames).
    struct MockFactory {
        scripts: Mutex<VecDeque<Vec<Result<RawFrame, String>>>>,
        connects: Arc<AtomicUsize>,
    }

    /// Build a factory over scripted per-connection frame lists.
    fn mock_factory(
        scripts: Vec<Vec<Result<RawFrame, String>>>,
    ) -> (Box<dyn ConnFactory>, Arc<AtomicUsize>) {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = MockFactory {
            scripts: Mutex::new(VecDeque::from(scripts)),
            connects: connects.clone(),
        };
        (Box::new(factory), connects)
    }

    impl ConnFactory for MockFactory {
        fn connect(&self) -> Result<Box<dyn CqlIo>, String> {
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
            name: "c".into(),
            method: "POST".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: plugin,
            config: serde_json::Value::Null,
        }
    }

    /// Build a `RESULT` of kind `Rows` with a global table spec, two columns
    /// (`id int`, `name varchar`) and the given row count. Row content is
    /// omitted — [`parse_rows`] stops at the row count.
    fn rows_result(count: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&0x0002i32.to_be_bytes()); // kind = Rows
        body.extend_from_slice(&0x0001i32.to_be_bytes()); // metadata flags = global
        body.extend_from_slice(&2i32.to_be_bytes()); // column count
        write_string(&mut body, "ks");
        write_string(&mut body, "t");
        write_string(&mut body, "id");
        body.extend_from_slice(&0x0009u16.to_be_bytes()); // int
        write_string(&mut body, "name");
        body.extend_from_slice(&0x000du16.to_be_bytes()); // varchar
        body.extend_from_slice(&count.to_be_bytes()); // row count
        body
    }

    fn error_result(code: i32, msg: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&code.to_be_bytes());
        write_string(&mut body, msg);
        body
    }

    fn set_keyspace_result(ks: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&0x0003i32.to_be_bytes()); // kind = Set_keyspace
        write_string(&mut body, ks);
        body
    }

    fn frame(opcode: u8, body: Vec<u8>) -> RawFrame {
        RawFrame { opcode, body }
    }

    // -- target parsing ------------------------------------------------------

    #[test]
    fn parses_cql_urls() {
        let t = parse_target("cql://127.0.0.1:9042/loadr").unwrap();
        assert_eq!(t.addr, "127.0.0.1:9042");
        assert_eq!(t.keyspace, "loadr");
        assert_eq!(t.pool_key(), "127.0.0.1:9042/loadr");

        // Default port when omitted.
        let t = parse_target("cql://db.example.com/ks").unwrap();
        assert_eq!(t.addr, "db.example.com:9042");
        assert_eq!(t.keyspace, "ks");

        // No keyspace.
        let t = parse_target("cql://host:9999").unwrap();
        assert_eq!(t.addr, "host:9999");
        assert_eq!(t.keyspace, "");
        assert_eq!(t.pool_key(), "host:9999/");
    }

    #[test]
    fn rejects_bad_targets() {
        assert!(parse_target("nats://h:4222").is_err());
        assert!(parse_target("not a url").is_err());
    }

    // -- statement parsing ---------------------------------------------------

    #[test]
    fn parse_op_reads_top_level_query() {
        let r = req(
            "cql://h/k",
            Some(serde_json::json!({ "query": "SELECT * FROM t" })),
        );
        let op = parse_op(&r).unwrap();
        assert_eq!(op.query, "SELECT * FROM t");
        assert_eq!(op.consistency, CONSISTENCY_ONE);
        assert!(op.values.is_empty());
    }

    #[test]
    fn parse_op_reads_nested_cql_block_with_params_and_consistency() {
        let r = req(
            "cql://h/k",
            Some(serde_json::json!({
                "cql": {
                    "query": "SELECT id FROM users WHERE id = ?",
                    "params": ["42", "bob", "3.5", true],
                    "consistency": "local_quorum",
                },
            })),
        );
        let op = parse_op(&r).unwrap();
        assert_eq!(op.query, "SELECT id FROM users WHERE id = ?");
        assert_eq!(op.consistency, 0x0006);
        assert_eq!(
            op.values,
            vec![
                Bind::Value(42i32.to_be_bytes().to_vec()),
                Bind::Value(b"bob".to_vec()),
                Bind::Value(3.5f64.to_be_bytes().to_vec()),
                Bind::Value(vec![1]),
            ]
        );
    }

    #[test]
    fn parse_op_falls_back_to_body() {
        let mut r = req("cql://h/k", None);
        r.body_b64 =
            base64::engine::general_purpose::STANDARD.encode(b"SELECT now() FROM system.local");
        let op = parse_op(&r).unwrap();
        assert_eq!(op.query, "SELECT now() FROM system.local");
    }

    #[test]
    fn parse_op_empty_statement_is_error() {
        let r = req("cql://h/k", Some(serde_json::json!({ "query": "   " })));
        assert!(parse_op(&r).unwrap_err().contains("requires a `cql.query`"));

        let r = req("cql://h/k", None);
        assert!(parse_op(&r).is_err());
    }

    #[test]
    fn parse_op_unknown_consistency_is_error() {
        let r = req(
            "cql://h/k",
            Some(serde_json::json!({ "query": "SELECT 1", "consistency": "sometimes" })),
        );
        assert!(parse_op(&r)
            .unwrap_err()
            .contains("unknown cql consistency"));
    }

    #[test]
    fn parse_op_non_array_params_is_error() {
        let r = req(
            "cql://h/k",
            Some(serde_json::json!({ "query": "SELECT 1", "params": "nope" })),
        );
        assert!(parse_op(&r).unwrap_err().contains("must be an array"));
    }

    #[test]
    fn infer_value_types() {
        assert_eq!(infer_value("42"), 42i32.to_be_bytes().to_vec());
        assert_eq!(
            infer_value("9999999999"),
            9_999_999_999i64.to_be_bytes().to_vec()
        );
        assert_eq!(infer_value("2.5"), 2.5f64.to_be_bytes().to_vec());
        assert_eq!(infer_value("hello"), b"hello".to_vec());
    }

    #[test]
    fn bind_value_handles_null() {
        assert_eq!(bind_value(&serde_json::Value::Null), Bind::Null);
        assert_eq!(
            bind_value(&serde_json::json!(7)),
            Bind::Value(7i32.to_be_bytes().to_vec())
        );
    }

    // -- wire encoding -------------------------------------------------------

    #[test]
    fn encodes_frame_header() {
        let f = encode_frame(OP_STARTUP, 0, b"ab");
        assert_eq!(f[0], REQUEST_VERSION); // version
        assert_eq!(f[1], 0x00); // flags
        assert_eq!(&f[2..4], &0i16.to_be_bytes()); // stream
        assert_eq!(f[4], OP_STARTUP); // opcode
        assert_eq!(&f[5..9], &2u32.to_be_bytes()); // length
        assert_eq!(&f[9..], b"ab"); // body
    }

    #[test]
    fn startup_body_requests_cql_version() {
        let body = startup_body();
        let mut r = Reader::new(&body);
        assert_eq!(r.short().unwrap(), 1); // one entry
        assert_eq!(r.string().unwrap(), "CQL_VERSION");
        assert_eq!(r.string().unwrap(), "3.0.0");
    }

    #[test]
    fn encodes_query_without_values() {
        let op = Op {
            query: "SELECT 1".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        let body = encode_query_body(&op);
        let mut r = Reader::new(&body);
        let len = r.int().unwrap() as usize; // long string length
        assert_eq!(r.take(len).unwrap(), b"SELECT 1");
        assert_eq!(r.short().unwrap(), CONSISTENCY_ONE); // consistency
        assert_eq!(r.take(1).unwrap(), &[0x00]); // flags: no values
    }

    #[test]
    fn encodes_query_with_values_and_null() {
        let op = Op {
            query: "INSERT INTO t (a, b) VALUES (?, ?)".into(),
            consistency: CONSISTENCY_ONE,
            values: vec![Bind::Value(vec![0, 0, 0, 7]), Bind::Null],
        };
        let body = encode_query_body(&op);
        let mut r = Reader::new(&body);
        let len = r.int().unwrap() as usize;
        r.take(len).unwrap();
        assert_eq!(r.short().unwrap(), CONSISTENCY_ONE);
        assert_eq!(r.take(1).unwrap(), &[0x01]); // flags: values present
        assert_eq!(r.short().unwrap(), 2); // value count
        assert_eq!(r.int().unwrap(), 4); // first value length
        assert_eq!(r.take(4).unwrap(), &[0, 0, 0, 7]);
        assert_eq!(r.int().unwrap(), -1); // null value
    }

    // -- frame decoding ------------------------------------------------------

    #[test]
    fn reads_raw_frame() {
        // version 0x84, flags 0, stream 0, opcode READY, length 3, body "abc".
        let bytes = [
            0x84u8, 0x00, 0x00, 0x00, OP_READY, 0x00, 0x00, 0x00, 0x03, b'a', b'b', b'c',
        ];
        let f = read_raw_frame(&mut &bytes[..]).unwrap();
        assert_eq!(f.opcode, OP_READY);
        assert_eq!(f.body, b"abc");
    }

    #[test]
    fn reads_raw_frame_rejects_truncation() {
        // Declares 4 body bytes but supplies only 1.
        let bytes = [
            0x84u8, 0x00, 0x00, 0x00, OP_READY, 0x00, 0x00, 0x00, 0x04, b'x',
        ];
        assert!(read_raw_frame(&mut &bytes[..]).is_err());
    }

    // -- RESULT / ERROR parsing ---------------------------------------------

    #[test]
    fn parse_result_counts_rows() {
        assert_eq!(parse_result(&rows_result(3)).unwrap(), 3);
    }

    #[test]
    fn parse_result_void_is_zero_rows() {
        let body = 0x0001i32.to_be_bytes().to_vec(); // kind = Void
        assert_eq!(parse_result(&body).unwrap(), 0);
    }

    #[test]
    fn parse_result_set_keyspace_is_zero_rows() {
        assert_eq!(parse_result(&set_keyspace_result("loadr")).unwrap(), 0);
    }

    #[test]
    fn parse_result_no_metadata_flag_reaches_row_count() {
        let mut body = Vec::new();
        body.extend_from_slice(&0x0002i32.to_be_bytes()); // Rows
        body.extend_from_slice(&0x0004i32.to_be_bytes()); // flags = No_metadata
        body.extend_from_slice(&2i32.to_be_bytes()); // column count
        body.extend_from_slice(&5i32.to_be_bytes()); // row count
        assert_eq!(parse_result(&body).unwrap(), 5);
    }

    #[test]
    fn parse_result_skips_collection_column_types() {
        // One column `tags list<varchar>`, global spec, one row.
        let mut body = Vec::new();
        body.extend_from_slice(&0x0002i32.to_be_bytes()); // Rows
        body.extend_from_slice(&0x0001i32.to_be_bytes()); // global spec
        body.extend_from_slice(&1i32.to_be_bytes()); // column count
        write_string(&mut body, "ks");
        write_string(&mut body, "t");
        write_string(&mut body, "tags");
        body.extend_from_slice(&0x0020u16.to_be_bytes()); // list
        body.extend_from_slice(&0x000du16.to_be_bytes()); // of varchar
        body.extend_from_slice(&1i32.to_be_bytes()); // row count
        assert_eq!(parse_result(&body).unwrap(), 1);
    }

    #[test]
    fn parse_error_extracts_message() {
        let msg = parse_error(&error_result(0x2200, "Invalid query"));
        assert!(msg.contains("Invalid query"));
        assert!(msg.contains("0x2200"));
    }

    // -- handshake -----------------------------------------------------------

    #[test]
    fn handshake_startup_ready_no_keyspace() {
        let mut io = mock_io(vec![Ok(frame(OP_READY, Vec::new()))]);
        client_handshake(&mut io, "").unwrap();
        // Only a STARTUP is sent.
        assert_eq!(io.sent.len(), 1);
        assert_eq!(io.sent[0][4], OP_STARTUP);
    }

    #[test]
    fn handshake_selects_keyspace() {
        let mut io = mock_io(vec![
            Ok(frame(OP_READY, Vec::new())),
            Ok(frame(OP_RESULT, set_keyspace_result("loadr"))),
        ]);
        client_handshake(&mut io, "loadr").unwrap();
        assert_eq!(io.sent.len(), 2);
        assert_eq!(io.sent[1][4], OP_QUERY);
        assert!(sent_str(&io, 1).contains("USE \"loadr\""));
    }

    #[test]
    fn handshake_authenticate_is_error() {
        let mut io = mock_io(vec![Ok(frame(OP_AUTHENTICATE, Vec::new()))]);
        assert!(client_handshake(&mut io, "")
            .unwrap_err()
            .contains("authentication"));
    }

    #[test]
    fn handshake_error_frame_is_reported() {
        let mut io = mock_io(vec![Ok(frame(
            OP_ERROR,
            error_result(0x000a, "Protocol error"),
        ))]);
        assert!(client_handshake(&mut io, "")
            .unwrap_err()
            .contains("Protocol error"));
    }

    // -- perform -------------------------------------------------------------

    #[test]
    fn perform_returns_row_count() {
        let mut io = mock_io(vec![Ok(frame(OP_RESULT, rows_result(2)))]);
        let op = Op {
            query: "SELECT * FROM t".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        assert_eq!(perform(&mut io, &op).unwrap(), Outcome::Rows(2));
    }

    #[test]
    fn perform_surfaces_cql_error_without_dropping() {
        let mut io = mock_io(vec![Ok(frame(OP_ERROR, error_result(0x2200, "bad query")))]);
        let op = Op {
            query: "SELECT".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        match perform(&mut io, &op).unwrap() {
            Outcome::CqlError(msg) => assert!(msg.contains("bad query")),
            other => panic!("expected CqlError, got {other:?}"),
        }
    }

    #[test]
    fn perform_transport_error_propagates() {
        let mut io = mock_io(vec![Err("read failed: broken pipe".into())]);
        let op = Op {
            query: "SELECT 1".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        assert!(perform(&mut io, &op).is_err());
    }

    // -- run + pool ----------------------------------------------------------

    #[test]
    fn run_connects_and_pools_healthy_connection() {
        let (factory, connects) = mock_factory(vec![vec![Ok(frame(OP_RESULT, rows_result(4)))]]);
        let key = "run_connects_and_pools_healthy_connection";
        let op = Op {
            query: "SELECT * FROM t".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        assert_eq!(
            run(factory.as_ref(), key, None, &op).unwrap(),
            Outcome::Rows(4)
        );
        assert_eq!(connects.load(Ordering::Relaxed), 1);
        assert!(checkout(key).is_some());
    }

    #[test]
    fn run_keeps_connection_on_cql_error() {
        let (factory, connects) = mock_factory(vec![vec![Ok(frame(
            OP_ERROR,
            error_result(0x2200, "nope"),
        ))]]);
        let key = "run_keeps_connection_on_cql_error";
        // Seed the pool with a connection whose statement will error.
        checkin(key, factory.connect().unwrap());
        assert_eq!(connects.load(Ordering::Relaxed), 1);
        let op = Op {
            query: "SELECT".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        match run(factory.as_ref(), key, None, &op).unwrap() {
            Outcome::CqlError(msg) => assert!(msg.contains("nope")),
            other => panic!("expected CqlError, got {other:?}"),
        }
        // A CQL error is not a transport failure: no reconnect, conn returned.
        assert_eq!(connects.load(Ordering::Relaxed), 1);
        assert!(checkout(key).is_some());
    }

    #[test]
    fn run_reconnects_when_pooled_connection_fails() {
        let (factory, connects) = mock_factory(vec![
            vec![Err("dead socket".into())],
            vec![Ok(frame(OP_RESULT, rows_result(1)))],
        ]);
        let key = "run_reconnects_when_pooled_connection_fails";
        checkin(key, factory.connect().unwrap());
        assert_eq!(connects.load(Ordering::Relaxed), 1);
        let op = Op {
            query: "SELECT 1".into(),
            consistency: CONSISTENCY_ONE,
            values: Vec::new(),
        };
        assert_eq!(
            run(factory.as_ref(), key, None, &op).unwrap(),
            Outcome::Rows(1)
        );
        // Pooled conn failed → dropped → one fresh reconnect.
        assert_eq!(connects.load(Ordering::Relaxed), 2);
    }

    // -- responses / handle --------------------------------------------------

    #[test]
    fn ok_response_encodes_row_count_and_extras() {
        let resp = ok_response(3, 1.5);
        assert_eq!(resp.status, 1);
        assert!(resp.error.is_none());
        let body = base64::engine::general_purpose::STANDARD
            .decode(&resp.body_b64)
            .unwrap();
        assert_eq!(body, b"3");
        assert_eq!(resp.extras["backend"], "cassandra");
        assert_eq!(resp.extras["rows"], 3);
    }

    #[test]
    fn error_response_sets_status_zero() {
        let resp = error_response(2.0, "boom".into());
        assert_eq!(resp.status, 0);
        assert_eq!(resp.error.as_deref(), Some("boom"));
        assert_eq!(resp.extras["backend"], "cassandra");
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req("nats://h:4222", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.unwrap().contains("cannot handle scheme"));
    }

    #[test]
    fn info_declares_scheme_and_kind() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "cassandra");
        assert_eq!(v["schemes"][0], "cql");
    }

    // -----------------------------------------------------------------------
    // Integration: a real Cassandra/ScyllaDB node. Skips unless the env var is
    // set.
    //   docker run -p 9042:9042 cassandra:latest
    //   LOADR_TEST_CQL_URL=cql://127.0.0.1:9042 cargo test -p loadr-plugin-cassandra
    // -----------------------------------------------------------------------

    #[test]
    fn cql_select_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_CQL_URL") else {
            eprintln!("skipping: LOADR_TEST_CQL_URL not set");
            return;
        };
        let r = req(
            &url,
            Some(serde_json::json!({ "query": "SELECT release_version FROM system.local" })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert!(resp.error.is_none(), "select error: {:?}", resp.error);
        assert_eq!(resp.status, 1);
    }

    #[test]
    fn cql_connection_failure_is_reported() {
        if std::env::var("LOADR_TEST_CQL_URL").is_err() {
            eprintln!("skipping: LOADR_TEST_CQL_URL not set");
            return;
        }
        // Port 1 is never a CQL node; the driver reports an error, not a panic.
        let r = req(
            "cql://127.0.0.1:1",
            Some(serde_json::json!({ "query": "SELECT 1" })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
