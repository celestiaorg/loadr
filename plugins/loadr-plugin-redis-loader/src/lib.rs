//! `loadr-plugin-redis-loader` — a native **service** plugin that turns a Redis
//! list or stream into a distributed shared data feeder for VUs.
//!
//! # Why a service plugin
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> bound_addr` and an idempotent `stop()`. On `start`
//! this plugin:
//!
//! 1. connects to Redis over raw RESP (no `redis` crate, no OpenSSL, no async
//!    runtime — just a blocking `std::net::TcpStream`, reusing the `redis`
//!    plugin's socket approach) and validates the target with a `PING`;
//! 2. binds a tiny local line endpoint (`127.0.0.1:0` by default) and returns
//!    its bound address.
//!
//! Every VU that opens that endpoint and reads a line gets the *next* shared
//! value: the service pops it from the Redis list (`LPOP` / `BLPOP` / `LMOVE`)
//! or reads it from the stream (`XREAD`). Because the backing key lives in
//! Redis, the feed is shared across **every worker** pointed at the same
//! server — a distributed feeder that hands each value out once (list) or
//! fans stream entries in, without every VU needing its own Redis client.
//!
//! # Feed modes
//!
//! * `exhaust` (default) — pop until the source is empty, then report "done".
//! * `block` — wait up to `block_ms` for a value (`BLPOP` / `XREAD BLOCK`);
//!   an empty result is a timeout, not exhaustion, so later reads can still
//!   succeed.
//! * `cycle` — rotate a list forever with `LMOVE key key LEFT RIGHT`, so the
//!   same pool of values is served round-robin and never runs out.
//!
//! # Reconnection
//!
//! The RESP connection is lazy and self-healing: if a pooled socket has been
//! dropped by the server, the failing command transparently reconnects once
//! and retries, so a VU never sees a transient disconnect.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use url::Url;

const NAME: &str = "redis-loader";

// ---------------------------------------------------------------------------
// RESP wire format (blocking, std-only) — the same protocol the `redis` plugin
// speaks, adapted from tokio's async I/O to blocking `std::io`.
// ---------------------------------------------------------------------------

/// A parsed RESP reply.
#[derive(Debug, Clone, PartialEq)]
enum RespValue {
    /// `+OK`
    Simple(String),
    /// `-ERR ...`
    Error(String),
    /// `:123`
    Integer(i64),
    /// `$...` bulk string.
    Bulk(Vec<u8>),
    /// `*...` array.
    Array(Vec<RespValue>),
    /// `$-1` / `*-1` null.
    Nil,
}

impl RespValue {
    /// Plain-text rendering used for a fed value.
    fn to_text(&self) -> String {
        match self {
            RespValue::Simple(s) | RespValue::Error(s) => s.clone(),
            RespValue::Integer(n) => n.to_string(),
            RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            RespValue::Nil => String::new(),
            RespValue::Array(_) => self.to_json().to_string(),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            RespValue::Simple(s) | RespValue::Error(s) => serde_json::Value::String(s.clone()),
            RespValue::Integer(n) => serde_json::Value::Number((*n).into()),
            RespValue::Bulk(b) => {
                serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
            }
            RespValue::Array(items) => {
                serde_json::Value::Array(items.iter().map(RespValue::to_json).collect())
            }
            RespValue::Nil => serde_json::Value::Null,
        }
    }
}

/// Encode an argv as a RESP array of bulk strings.
fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read one CRLF-terminated line (without the trailing CRLF).
fn read_line<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    loop {
        let byte = read_u8(reader)?;
        if byte == b'\r' {
            let next = read_u8(reader)?;
            if next == b'\n' {
                break;
            }
            line.push(b'\r');
            line.push(next);
        } else {
            line.push(byte);
        }
    }
    Ok(line)
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, String> {
    let mut b = [0u8; 1];
    reader
        .read_exact(&mut b)
        .map_err(|e| format!("read failed: {e}"))?;
    Ok(b[0])
}

/// Read a single RESP reply from `reader`.
fn read_reply<R: Read>(reader: &mut R) -> Result<RespValue, String> {
    let prefix = read_u8(reader)?;
    match prefix {
        b'+' => Ok(RespValue::Simple(
            String::from_utf8_lossy(&read_line(reader)?).into_owned(),
        )),
        b'-' => Ok(RespValue::Error(
            String::from_utf8_lossy(&read_line(reader)?).into_owned(),
        )),
        b':' => {
            let line = read_line(reader)?;
            let n = String::from_utf8_lossy(&line)
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("invalid integer reply: {e}"))?;
            Ok(RespValue::Integer(n))
        }
        b'$' => {
            let line = read_line(reader)?;
            let len = String::from_utf8_lossy(&line)
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("invalid bulk length: {e}"))?;
            if len < 0 {
                return Ok(RespValue::Nil);
            }
            let mut buf = vec![0u8; len as usize];
            reader
                .read_exact(&mut buf)
                .map_err(|e| format!("read failed: {e}"))?;
            let mut crlf = [0u8; 2];
            reader
                .read_exact(&mut crlf)
                .map_err(|e| format!("read failed: {e}"))?;
            Ok(RespValue::Bulk(buf))
        }
        b'*' => {
            let line = read_line(reader)?;
            let len = String::from_utf8_lossy(&line)
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("invalid array length: {e}"))?;
            if len < 0 {
                return Ok(RespValue::Nil);
            }
            let mut items = Vec::with_capacity(len.max(0) as usize);
            for _ in 0..len {
                items.push(read_reply(reader)?);
            }
            Ok(RespValue::Array(items))
        }
        other => Err(format!("unexpected RESP prefix byte: {other:#x}")),
    }
}

// ---------------------------------------------------------------------------
// Connection abstraction — a seam so the feeder can be unit-tested without a
// real socket.
// ---------------------------------------------------------------------------

/// A live RESP connection: send an encoded command, get one reply. A returned
/// `Err` is a *transport* failure (dropped socket) and triggers a reconnect;
/// a RESP `-ERR` reply is returned as `Ok(RespValue::Error(..))`.
trait RespConn: Send {
    fn command(&mut self, payload: &[u8]) -> Result<RespValue, String>;
}

/// Creates fresh [`RespConn`]s. A new connection is made lazily and again after
/// a transport error, giving transparent reconnection.
trait ConnFactory: Send {
    fn connect(&self) -> Result<Box<dyn RespConn>, String>;
}

/// A real blocking TCP connection to Redis.
struct TcpConn {
    reader: BufReader<TcpStream>,
}

impl RespConn for TcpConn {
    fn command(&mut self, payload: &[u8]) -> Result<RespValue, String> {
        self.reader
            .get_mut()
            .write_all(payload)
            .map_err(|e| format!("send failed: {e}"))?;
        self.reader
            .get_mut()
            .flush()
            .map_err(|e| format!("flush failed: {e}"))?;
        read_reply(&mut self.reader)
    }
}

/// Opens `TcpConn`s to `addr`, applying `SELECT db` when a db is given.
struct TcpConnFactory {
    addr: String,
    db: Option<u32>,
    read_timeout: Duration,
}

impl ConnFactory for TcpConnFactory {
    fn connect(&self) -> Result<Box<dyn RespConn>, String> {
        let stream = TcpStream::connect(&self.addr)
            .map_err(|e| format!("connection to {} failed: {e}", self.addr))?;
        let _ = stream.set_nodelay(true);
        stream
            .set_read_timeout(Some(self.read_timeout))
            .map_err(|e| format!("set_read_timeout failed: {e}"))?;
        let mut conn = TcpConn {
            reader: BufReader::new(stream),
        };
        if let Some(db) = self.db {
            let select = encode_command(&[b"SELECT", db.to_string().as_bytes()]);
            if let RespValue::Error(e) = conn.command(&select)? {
                return Err(format!("SELECT {db} failed: {e}"));
            }
        }
        Ok(Box::new(conn))
    }
}

// ---------------------------------------------------------------------------
// Feed plan + feeder core.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedKind {
    List,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedMode {
    Exhaust,
    Block,
    Cycle,
}

/// What to feed and how.
#[derive(Debug, Clone)]
struct FeedPlan {
    key: String,
    kind: FeedKind,
    mode: FeedMode,
    block_ms: u64,
    count: u64,
}

impl FeedPlan {
    /// The RESP command to fetch the next value(s), given the current stream
    /// cursor.
    fn command(&self, last_id: &str) -> Vec<u8> {
        let key = self.key.as_bytes();
        match self.kind {
            FeedKind::List => match self.mode {
                FeedMode::Exhaust => encode_command(&[b"LPOP", key]),
                FeedMode::Block => {
                    // BLPOP takes a timeout in (fractional) seconds.
                    let secs = format!("{:.3}", self.block_ms as f64 / 1000.0);
                    encode_command(&[b"BLPOP", key, secs.as_bytes()])
                }
                FeedMode::Cycle => encode_command(&[b"LMOVE", key, key, b"LEFT", b"RIGHT"]),
            },
            FeedKind::Stream => {
                let count = self.count.max(1).to_string();
                if self.mode == FeedMode::Block {
                    let block = self.block_ms.to_string();
                    encode_command(&[
                        b"XREAD",
                        b"COUNT",
                        count.as_bytes(),
                        b"BLOCK",
                        block.as_bytes(),
                        b"STREAMS",
                        key,
                        last_id.as_bytes(),
                    ])
                } else {
                    encode_command(&[
                        b"XREAD",
                        b"COUNT",
                        count.as_bytes(),
                        b"STREAMS",
                        key,
                        last_id.as_bytes(),
                    ])
                }
            }
        }
    }
}

/// Serves the next shared value, reconnecting on a dropped socket and buffering
/// stream entries between reads.
struct Feeder {
    factory: Box<dyn ConnFactory>,
    conn: Option<Box<dyn RespConn>>,
    plan: FeedPlan,
    /// Buffered stream entries not yet handed out.
    buf: VecDeque<String>,
    /// Stream cursor: the last id read (starts at "0" = from the beginning).
    last_id: String,
    /// Set once an `exhaust`/`cycle` source is empty; further reads yield None.
    done: bool,
}

impl Feeder {
    fn new(factory: Box<dyn ConnFactory>, plan: FeedPlan) -> Self {
        Feeder {
            factory,
            conn: None,
            plan,
            buf: VecDeque::new(),
            last_id: "0".to_string(),
            done: false,
        }
    }

    /// Send one command, reconnecting once on a transport failure of a pooled
    /// connection.
    fn exchange(&mut self, payload: &[u8]) -> Result<RespValue, String> {
        if self.conn.is_none() {
            self.conn = Some(self.factory.connect()?);
        }
        match self.conn.as_mut().unwrap().command(payload) {
            Ok(reply) => Ok(reply),
            Err(_transport) => {
                // The socket was dropped; discard it and reconnect once.
                self.conn = None;
                let mut fresh = self.factory.connect()?;
                let reply = fresh.command(payload)?;
                self.conn = Some(fresh);
                Ok(reply)
            }
        }
    }

    /// Return the next value, `Ok(None)` when the source is exhausted (or a
    /// `block` read timed out), `Err` on a hard/RESP error.
    fn next(&mut self) -> Result<Option<String>, String> {
        if let Some(v) = self.buf.pop_front() {
            return Ok(Some(v));
        }
        if self.done {
            return Ok(None);
        }
        let payload = self.plan.command(&self.last_id);
        let reply = self.exchange(&payload)?;
        match self.plan.kind {
            FeedKind::List => self.interpret_list(reply),
            FeedKind::Stream => self.interpret_stream(reply),
        }
    }

    fn interpret_list(&mut self, reply: RespValue) -> Result<Option<String>, String> {
        match self.plan.mode {
            FeedMode::Block => match reply {
                // BLPOP returns [key, value].
                RespValue::Array(items) => match items.get(1) {
                    Some(v) => Ok(Some(v.to_text())),
                    None => Ok(None),
                },
                // Timeout: no value now, but not permanently exhausted.
                RespValue::Nil => Ok(None),
                RespValue::Error(e) => Err(e),
                other => Ok(Some(other.to_text())),
            },
            FeedMode::Exhaust | FeedMode::Cycle => match reply {
                RespValue::Nil => {
                    // Empty list: nothing to pop or rotate.
                    self.done = true;
                    Ok(None)
                }
                RespValue::Error(e) => Err(e),
                other => Ok(Some(other.to_text())),
            },
        }
    }

    fn interpret_stream(&mut self, reply: RespValue) -> Result<Option<String>, String> {
        match reply {
            RespValue::Nil => {
                if self.plan.mode != FeedMode::Block {
                    self.done = true;
                }
                Ok(None)
            }
            RespValue::Error(e) => Err(e),
            RespValue::Array(streams) => {
                for stream in streams {
                    // Each element is [stream_name, [ [id, [f, v, ...]], ... ]].
                    let RespValue::Array(pair) = stream else {
                        continue;
                    };
                    let Some(RespValue::Array(entries)) = pair.get(1) else {
                        continue;
                    };
                    for entry in entries {
                        let RespValue::Array(kv) = entry else {
                            continue;
                        };
                        let id = kv.first().map(RespValue::to_text).unwrap_or_default();
                        if !id.is_empty() {
                            self.last_id = id.clone();
                        }
                        let fields = fields_to_json(kv.get(1));
                        let value = serde_json::json!({ "id": id, "fields": fields }).to_string();
                        self.buf.push_back(value);
                    }
                }
                if let Some(v) = self.buf.pop_front() {
                    Ok(Some(v))
                } else {
                    if self.plan.mode != FeedMode::Block {
                        self.done = true;
                    }
                    Ok(None)
                }
            }
            other => Err(format!("unexpected stream reply: {other:?}")),
        }
    }
}

/// Turn a RESP `[field, value, field, value, ...]` array into a JSON object.
fn fields_to_json(v: Option<&RespValue>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    if let Some(RespValue::Array(items)) = v {
        let mut it = items.iter();
        while let (Some(f), Some(val)) = (it.next(), it.next()) {
            map.insert(f.to_text(), serde_json::Value::String(val.to_text()));
        }
    }
    serde_json::Value::Object(map)
}

// ---------------------------------------------------------------------------
// Config parsing.
// ---------------------------------------------------------------------------

/// Parse and validate the target URL, returning `(host:port, db)`.
fn parse_target(raw: &str) -> Result<(String, Option<u32>), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    match url.scheme() {
        "redis" | "rediss" => {}
        other => return Err(format!("redis-loader cannot handle scheme `{other}`")),
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("`{raw}` has no host"))?
        .to_string();
    let port = url.port().unwrap_or(6379);
    let key = format!("{host}:{port}");
    let db = match url.path().trim_start_matches('/') {
        "" => None,
        digits => Some(
            digits
                .parse::<u32>()
                .map_err(|_| format!("invalid db `{digits}`"))?,
        ),
    };
    Ok((key, db))
}

/// The pieces `start()` needs, parsed from the config JSON.
#[derive(Debug)]
struct StartConfig {
    addr: String,
    db: Option<u32>,
    bind: String,
    plan: FeedPlan,
    block_ms: u64,
}

fn parse_config(config_json: &str) -> Result<StartConfig, String> {
    let cfg: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let url = cfg
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("redis://127.0.0.1:6379");
    let (addr, db) = parse_target(url)?;

    let key = cfg
        .get("key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "config requires a non-empty `key` string".to_string())?
        .to_string();

    let kind = match cfg.get("source").and_then(|v| v.as_str()).unwrap_or("list") {
        "list" => FeedKind::List,
        "stream" => FeedKind::Stream,
        other => return Err(format!("unknown source `{other}` (use `list` or `stream`)")),
    };

    let mode = match cfg
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("exhaust")
    {
        "exhaust" => FeedMode::Exhaust,
        "block" => FeedMode::Block,
        "cycle" => FeedMode::Cycle,
        other => {
            return Err(format!(
                "unknown mode `{other}` (use `exhaust`, `block` or `cycle`)"
            ))
        }
    };
    if kind == FeedKind::Stream && mode == FeedMode::Cycle {
        return Err("`cycle` mode is only valid for `source = list`".to_string());
    }

    let bind = cfg
        .get("bind")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1:0")
        .to_string();

    let block_ms = cfg.get("block_ms").and_then(|v| v.as_u64()).unwrap_or(5000);
    let count = cfg.get("count").and_then(|v| v.as_u64()).unwrap_or(16);

    Ok(StartConfig {
        addr,
        db,
        bind,
        block_ms,
        plan: FeedPlan {
            key,
            kind,
            mode,
            block_ms,
            count,
        },
    })
}

// ---------------------------------------------------------------------------
// The service: a local line endpoint in front of the shared Redis feed.
// ---------------------------------------------------------------------------

/// A running feeder endpoint. Handed off to `stop()` for teardown.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    addr: String,
}

/// The service plugin instance.
#[derive(Default)]
struct RedisLoader {
    handle: Option<ServerHandle>,
}

impl RedisLoader {
    fn start_config(&mut self, cfg: StartConfig) -> Result<String, String> {
        // Validate the Redis target eagerly so a misconfigured plan fails fast.
        let read_timeout = Duration::from_millis(cfg.block_ms.saturating_add(2000).max(2000));
        let factory: Box<dyn ConnFactory> = Box::new(TcpConnFactory {
            addr: cfg.addr,
            db: cfg.db,
            read_timeout,
        });
        let mut probe = factory.connect()?;
        if let RespValue::Error(e) = probe.command(&encode_command(&[b"PING"]))? {
            return Err(format!("PING failed: {e}"));
        }
        drop(probe);

        // Bind the local feeder endpoint.
        let listener = TcpListener::bind(&cfg.bind)
            .map_err(|e| format!("cannot bind feeder endpoint {}: {e}", cfg.bind))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let feeder = Arc::new(Mutex::new(Feeder::new(factory, cfg.plan)));
        let shutdown = Arc::new(AtomicBool::new(false));
        let accept = spawn_accept_loop(listener, feeder, shutdown.clone());

        self.handle = Some(ServerHandle {
            shutdown,
            accept: Some(accept),
            addr: addr.clone(),
        });
        Ok(addr)
    }
}

/// Spawn the accept loop that serves VUs. Each accepted connection is handled
/// on its own thread, sharing the single Redis-backed [`Feeder`].
fn spawn_accept_loop(
    listener: TcpListener,
    feeder: Arc<Mutex<Feeder>>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let feeder = feeder.clone();
                    let shutdown = shutdown.clone();
                    std::thread::spawn(move || handle_client(stream, feeder, shutdown));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    })
}

/// Serve one VU connection: for every request line the client sends, write back
/// the next feed value plus a newline. An empty line (just `\n`) signals the
/// source is exhausted. The client drives the pace by reading one value at a
/// time.
fn handle_client(stream: TcpStream, feeder: Arc<Mutex<Feeder>>, shutdown: Arc<AtomicBool>) {
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
        let value = {
            let mut guard = match feeder.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.next()
        };
        let line = match value {
            Ok(Some(v)) => {
                // Guard the line protocol: newlines in a value would desync it.
                let v = v.replace(['\n', '\r'], " ");
                format!("{v}\n")
            }
            Ok(None) => "\n".to_string(), // exhausted / timed out
            Err(_) => "\n".to_string(),   // surface as no-value, keep serving
        };
        if writer.write_all(line.as_bytes()).is_err() {
            return;
        }
        let _ = writer.flush();
    }
}

impl FfiService for RedisLoader {
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
            // The accept loop polls a nonblocking listener, so it observes the
            // shutdown flag within its 50ms poll interval and returns; join it.
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
                "Distributed shared data feeder backed by a Redis list or stream (raw RESP)",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(RedisLoader::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline; the feeder is exercised through a scripted mock
// connection, never a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// A scripted in-memory connection. Each `command()` returns the next
    /// queued result; a queued `Err` models a dropped socket.
    struct MockConn {
        replies: VecDeque<Result<RespValue, String>>,
    }

    impl RespConn for MockConn {
        fn command(&mut self, _payload: &[u8]) -> Result<RespValue, String> {
            self.replies
                .pop_front()
                .unwrap_or(Err("mock: no more scripted replies".to_string()))
        }
    }

    /// Hands out pre-built [`MockConn`]s in order, counting connects so tests
    /// can assert reconnection happened.
    struct MockFactory {
        conns: Mutex<VecDeque<VecDeque<Result<RespValue, String>>>>,
        connects: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockFactory {
        fn build(
            scripts: Vec<Vec<Result<RespValue, String>>>,
        ) -> (Box<dyn ConnFactory>, Arc<std::sync::atomic::AtomicUsize>) {
            let connects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let factory = MockFactory {
                conns: Mutex::new(scripts.into_iter().map(VecDeque::from).collect()),
                connects: connects.clone(),
            };
            (Box::new(factory), connects)
        }
    }

    impl ConnFactory for MockFactory {
        fn connect(&self) -> Result<Box<dyn RespConn>, String> {
            self.connects.fetch_add(1, Ordering::Relaxed);
            let mut guard = self.conns.lock().unwrap();
            match guard.pop_front() {
                Some(replies) => Ok(Box::new(MockConn { replies })),
                None => Err("mock: no more scripted connections".to_string()),
            }
        }
    }

    fn plan(kind: FeedKind, mode: FeedMode) -> FeedPlan {
        FeedPlan {
            key: "feed".to_string(),
            kind,
            mode,
            block_ms: 1000,
            count: 8,
        }
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::Bulk(s.as_bytes().to_vec())
    }

    // -- RESP wire format ----------------------------------------------------

    #[test]
    fn reads_resp_values() {
        assert_eq!(
            read_reply(&mut &b"+OK\r\n"[..]).unwrap(),
            RespValue::Simple("OK".into())
        );
        assert_eq!(
            read_reply(&mut &b"-ERR bad\r\n"[..]).unwrap(),
            RespValue::Error("ERR bad".into())
        );
        assert_eq!(
            read_reply(&mut &b":42\r\n"[..]).unwrap(),
            RespValue::Integer(42)
        );
        assert_eq!(
            read_reply(&mut &b"$5\r\nhello\r\n"[..]).unwrap(),
            bulk("hello")
        );
        assert_eq!(read_reply(&mut &b"$-1\r\n"[..]).unwrap(), RespValue::Nil);
        assert_eq!(
            read_reply(&mut &b"*2\r\n:1\r\n$1\r\nx\r\n"[..]).unwrap(),
            RespValue::Array(vec![RespValue::Integer(1), bulk("x")])
        );
    }

    #[test]
    fn encodes_resp_command() {
        assert_eq!(
            encode_command(&[b"LPOP", b"feed"]),
            b"*2\r\n$4\r\nLPOP\r\n$4\r\nfeed\r\n"
        );
    }

    #[test]
    fn parses_redis_urls() {
        assert_eq!(
            parse_target("redis://127.0.0.1:6379").unwrap(),
            ("127.0.0.1:6379".to_string(), None)
        );
        assert_eq!(
            parse_target("redis://localhost/3").unwrap(),
            ("localhost:6379".to_string(), Some(3))
        );
        assert!(parse_target("http://x:1").is_err());
    }

    // -- config --------------------------------------------------------------

    #[test]
    fn config_requires_key() {
        let err = parse_config(r#"{"url":"redis://h:6379"}"#).unwrap_err();
        assert!(err.contains("key"), "{err}");
    }

    #[test]
    fn config_defaults_and_overrides() {
        let cfg = parse_config(
            r#"{"url":"redis://h:6379/2","key":"jobs","source":"stream","mode":"block","block_ms":250,"count":4,"bind":"127.0.0.1:9999"}"#,
        )
        .unwrap();
        assert_eq!(cfg.addr, "h:6379");
        assert_eq!(cfg.db, Some(2));
        assert_eq!(cfg.bind, "127.0.0.1:9999");
        assert_eq!(cfg.block_ms, 250);
        assert_eq!(cfg.plan.kind, FeedKind::Stream);
        assert_eq!(cfg.plan.mode, FeedMode::Block);
        assert_eq!(cfg.plan.count, 4);
    }

    #[test]
    fn config_rejects_cycle_stream() {
        let err = parse_config(r#"{"key":"s","source":"stream","mode":"cycle"}"#).unwrap_err();
        assert!(err.contains("cycle"), "{err}");
    }

    // -- command selection ---------------------------------------------------

    #[test]
    fn list_mode_selects_command() {
        assert_eq!(
            plan(FeedKind::List, FeedMode::Exhaust).command("0"),
            encode_command(&[b"LPOP", b"feed"])
        );
        assert_eq!(
            plan(FeedKind::List, FeedMode::Cycle).command("0"),
            encode_command(&[b"LMOVE", b"feed", b"feed", b"LEFT", b"RIGHT"])
        );
        // block uses BLPOP with a fractional-second timeout.
        let blpop = plan(FeedKind::List, FeedMode::Block).command("0");
        assert!(blpop.starts_with(b"*3\r\n$5\r\nBLPOP\r\n"), "{blpop:?}");
    }

    // -- LPOP returns values, then exhausts ---------------------------------

    #[test]
    fn lpop_returns_values_then_exhausts() {
        let (factory, connects) =
            MockFactory::build(vec![vec![Ok(bulk("a")), Ok(bulk("b")), Ok(RespValue::Nil)]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Exhaust));
        assert_eq!(feeder.next().unwrap(), Some("a".to_string()));
        assert_eq!(feeder.next().unwrap(), Some("b".to_string()));
        // Nil = exhausted, and it stays exhausted without another command.
        assert_eq!(feeder.next().unwrap(), None);
        assert_eq!(feeder.next().unwrap(), None);
        assert_eq!(connects.load(Ordering::Relaxed), 1, "one connect, reused");
    }

    // -- exhaustion: block vs cycle -----------------------------------------

    #[test]
    fn block_timeout_does_not_exhaust() {
        // BLPOP timeout (Nil) yields None but the feeder can still serve later.
        let (factory, _) = MockFactory::build(vec![vec![
            Ok(RespValue::Nil),
            Ok(RespValue::Array(vec![bulk("feed"), bulk("late")])),
        ]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Block));
        assert_eq!(feeder.next().unwrap(), None); // timeout, not done
        assert!(!feeder.done);
        assert_eq!(feeder.next().unwrap(), Some("late".to_string()));
    }

    #[test]
    fn cycle_mode_never_exhausts() {
        // LMOVE keeps returning rotated values; the feeder never marks done.
        let (factory, _) =
            MockFactory::build(vec![vec![Ok(bulk("x")), Ok(bulk("y")), Ok(bulk("x"))]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Cycle));
        assert_eq!(feeder.next().unwrap(), Some("x".to_string()));
        assert_eq!(feeder.next().unwrap(), Some("y".to_string()));
        assert_eq!(feeder.next().unwrap(), Some("x".to_string()));
        assert!(!feeder.done);
    }

    // -- stream reads --------------------------------------------------------

    #[test]
    fn stream_read_returns_buffered_entries() {
        // One XREAD reply carrying two entries; the feeder buffers and serves
        // them one at a time and advances its cursor.
        let reply = RespValue::Array(vec![RespValue::Array(vec![
            bulk("mystream"),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    bulk("1-1"),
                    RespValue::Array(vec![bulk("v"), bulk("alpha")]),
                ]),
                RespValue::Array(vec![
                    bulk("1-2"),
                    RespValue::Array(vec![bulk("v"), bulk("beta")]),
                ]),
            ]),
        ])]);
        // Second call returns Nil (no more) -> exhausted.
        let (factory, connects) = MockFactory::build(vec![vec![Ok(reply), Ok(RespValue::Nil)]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::Stream, FeedMode::Exhaust));

        let first = feeder.next().unwrap().expect("first entry");
        assert!(first.contains("alpha"), "{first}");
        // Buffered: no new command needed for the second entry.
        let second = feeder.next().unwrap().expect("second entry");
        assert!(second.contains("beta"), "{second}");
        assert_eq!(feeder.last_id, "1-2", "cursor advanced");

        // Buffer drained -> issues another XREAD, gets Nil -> exhausted.
        assert_eq!(feeder.next().unwrap(), None);
        assert_eq!(connects.load(Ordering::Relaxed), 1);
    }

    // -- reconnection on a dropped socket -----------------------------------

    #[test]
    fn reconnects_on_dropped_socket() {
        // First connection's command fails (dropped socket); the feeder must
        // reconnect and retry on a fresh connection, returning the value.
        let (factory, connects) = MockFactory::build(vec![
            vec![Err("broken pipe".to_string())],
            vec![Ok(bulk("recovered"))],
        ]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Exhaust));
        assert_eq!(feeder.next().unwrap(), Some("recovered".to_string()));
        assert_eq!(
            connects.load(Ordering::Relaxed),
            2,
            "reconnected exactly once"
        );
    }

    #[test]
    fn reconnect_failure_is_surfaced() {
        // Command fails and no further connection can be made -> hard error.
        let (factory, _) = MockFactory::build(vec![vec![Err("reset".to_string())]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Exhaust));
        assert!(feeder.next().is_err());
    }

    // -- RESP error reply is a hard error -----------------------------------

    #[test]
    fn resp_error_reply_is_error() {
        let (factory, _) = MockFactory::build(vec![vec![Ok(RespValue::Error("WRONGTYPE".into()))]]);
        let mut feeder = Feeder::new(factory, plan(FeedKind::List, FeedMode::Exhaust));
        assert!(feeder.next().is_err());
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = RedisLoader::default();
        svc.stop();
        svc.stop(); // second stop must not panic
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_rejects_bad_config() {
        let mut svc = RedisLoader::default();
        // Missing `key` -> start fails, nothing bound, stop still a no-op.
        let res = svc.start(RString::from(r#"{"url":"redis://127.0.0.1:6379"}"#));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "redis-loader");
    }
}
