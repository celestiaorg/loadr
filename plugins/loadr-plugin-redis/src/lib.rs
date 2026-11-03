//! `loadr-plugin-redis` — a native protocol plugin that adds Redis (RESP) as a
//! loadr load-test target.
//!
//! # How it plugs in
//!
//! loadr's native protocol ABI ([`FfiProtocol`]) is synchronous: the host
//! calls `execute(&self, request_json) -> response_json` on a single shared
//! plugin instance (`Send + Sync`), created once via `make_protocol()`. There
//! is no per-VU state across the FFI boundary, so this plugin owns all of its
//! async machinery:
//!
//! * A single multi-thread Tokio runtime, created once, on which every call
//!   `block_on`s the raw RESP exchange.
//! * An internal connection pool keyed by `host:port`
//!   (`OnceCell<Mutex<HashMap<String, Vec<PooledConn>>>>`). A live, idle
//!   connection is checked out for a command and returned afterwards, so
//!   concurrent VUs share a small set of reused sockets per distinct server
//!   rather than reconnecting on every command. A connection left in an error
//!   state is dropped instead of being returned, so the next caller transparently
//!   re-establishes it.
//!
//! The whole protocol is spoken directly over a TCP socket — there is no
//! `redis` crate and therefore no native-tls/OpenSSL dependency, so this cdylib
//! cross-compiles cleanly for every release target.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. We read
//! the connection target from `url` (`redis://host:port[/db]`) and the command
//! in priority order from:
//!
//! * `options.plugin.command` — `{ "command": ["GET", "key"] }` (preferred), or
//! * the request `body` — a single space-separated command (`"GET key"`).
//!
//! The response is JSON `{ status, status_text, body_b64, duration_ms, error,
//! extras }`. `status` is `0` on a successful reply and `1` on a RESP error
//! reply (`error` is then set with the server message), matching the old
//! built-in handler so existing `status` checks keep working. `extras` carries
//! `reply_type` and the decoded `value`. The host derives `redis_reqs` /
//! `redis_req_duration` from the `redis` plugin name.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use once_cell::sync::OnceCell;
use tokio::io::{AsyncReadExt, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "redis";

/// A live, pooled connection plus the db index it was `SELECT`ed onto. Only
/// connections that finished a command cleanly are returned to the pool.
struct PooledConn {
    conn: BufReader<TcpStream>,
    db: Option<u32>,
}

/// The single Tokio runtime the plugin uses to drive the async socket I/O.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build redis plugin tokio runtime")
    })
}

/// Idle connection pool keyed by `host:port`. A `Vec` acts as a simple LIFO
/// free-list: a command checks out an idle connection (or makes a fresh one)
/// and returns it on success, so concurrent VUs reuse sockets instead of
/// reconnecting per command.
fn pools() -> &'static Mutex<HashMap<String, Vec<PooledConn>>> {
    static POOLS: OnceCell<Mutex<HashMap<String, Vec<PooledConn>>>> = OnceCell::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Check out an idle connection for `key` matching the requested `db`, if any.
fn checkout(key: &str, db: Option<u32>) -> Option<PooledConn> {
    let mut guard = pools().lock().expect("redis pool lock");
    let list = guard.get_mut(key)?;
    // Prefer a connection already on the right db; otherwise take any (it will
    // be re-`SELECT`ed). Scan from the back so the common same-db case is O(1).
    if let Some(idx) = list.iter().rposition(|c| c.db == db) {
        return Some(list.swap_remove(idx));
    }
    list.pop()
}

/// Return a healthy connection to the pool for reuse.
fn checkin(key: &str, conn: PooledConn) {
    let mut guard = pools().lock().expect("redis pool lock");
    guard.entry(key.to_string()).or_default().push(conn);
}

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
    /// Wire-format type tag used in `extras.reply_type`.
    fn type_name(&self) -> &'static str {
        match self {
            RespValue::Simple(_) => "string",
            RespValue::Error(_) => "error",
            RespValue::Integer(_) => "integer",
            RespValue::Bulk(_) => "bulk",
            RespValue::Array(_) => "array",
            RespValue::Nil => "nil",
        }
    }

    /// JSON rendering for `extras.value`.
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

    /// Plain-text/body rendering of the reply.
    fn to_body(&self) -> Vec<u8> {
        match self {
            RespValue::Simple(s) | RespValue::Error(s) => s.clone().into_bytes(),
            RespValue::Integer(n) => n.to_string().into_bytes(),
            RespValue::Bulk(b) => b.clone(),
            RespValue::Nil => Vec::new(),
            RespValue::Array(_) => self.to_json().to_string().into_bytes(),
        }
    }
}

/// Parse and validate the target URL, returning `(host:port key, db)`.
fn parse_target(raw: &str) -> Result<(String, Option<u32>), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    match url.scheme() {
        "redis" | "rediss" => {}
        other => return Err(format!("redis plugin cannot handle scheme `{other}`")),
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

/// Resolve the command argv from plugin options or the request body.
fn command_args(request: &FfiRequest) -> Result<Vec<Vec<u8>>, String> {
    if let Some(plugin) = &request.options {
        if let Some(arr) = plugin.get("command").and_then(serde_json::Value::as_array) {
            let mut args = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    serde_json::Value::String(s) => args.push(s.clone().into_bytes()),
                    serde_json::Value::Number(n) => args.push(n.to_string().into_bytes()),
                    other => {
                        return Err(format!(
                            "redis command args must be strings/numbers, got `{other}`"
                        ))
                    }
                }
            }
            if !args.is_empty() {
                return Ok(args);
            }
        }
    }
    let body = base64_decode(&request.body_b64)?;
    let text = String::from_utf8_lossy(&body);
    let args: Vec<Vec<u8>> = text
        .split_whitespace()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    if args.is_empty() {
        return Err(
            "no redis command provided (set the `plugin.command` option or a request body)"
                .to_string(),
        );
    }
    Ok(args)
}

/// Encode an argv as a RESP array of bulk strings.
fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
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
async fn read_line<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    loop {
        let byte = reader
            .read_u8()
            .await
            .map_err(|e| format!("read failed: {e}"))?;
        if byte == b'\r' {
            let next = reader
                .read_u8()
                .await
                .map_err(|e| format!("read failed: {e}"))?;
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

/// Read a single RESP reply from `reader`.
async fn read_reply<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<RespValue, String> {
    let prefix = reader
        .read_u8()
        .await
        .map_err(|e| format!("read failed: {e}"))?;
    match prefix {
        b'+' => {
            let line = read_line(reader).await?;
            Ok(RespValue::Simple(
                String::from_utf8_lossy(&line).into_owned(),
            ))
        }
        b'-' => {
            let line = read_line(reader).await?;
            Ok(RespValue::Error(
                String::from_utf8_lossy(&line).into_owned(),
            ))
        }
        b':' => {
            let line = read_line(reader).await?;
            let n = String::from_utf8_lossy(&line)
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("invalid integer reply: {e}"))?;
            Ok(RespValue::Integer(n))
        }
        b'$' => {
            let line = read_line(reader).await?;
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
                .await
                .map_err(|e| format!("read failed: {e}"))?;
            // Consume the trailing CRLF.
            let mut crlf = [0u8; 2];
            reader
                .read_exact(&mut crlf)
                .await
                .map_err(|e| format!("read failed: {e}"))?;
            Ok(RespValue::Bulk(buf))
        }
        b'*' => {
            let line = read_line(reader).await?;
            let len = String::from_utf8_lossy(&line)
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("invalid array length: {e}"))?;
            if len < 0 {
                return Ok(RespValue::Nil);
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(Box::pin(read_reply(reader)).await?);
            }
            Ok(RespValue::Array(items))
        }
        other => Err(format!("unexpected RESP prefix byte: {other:#x}")),
    }
}

/// Send `payload` on `conn` and read one reply.
async fn exchange(conn: &mut BufReader<TcpStream>, payload: &[u8]) -> Result<RespValue, String> {
    conn.get_mut()
        .write_all(payload)
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    conn.get_mut()
        .flush()
        .await
        .map_err(|e| format!("flush failed: {e}"))?;
    read_reply(conn).await
}

/// Establish a fresh connection to `key`, applying `SELECT db` if requested.
async fn connect(key: &str, db: Option<u32>) -> Result<BufReader<TcpStream>, String> {
    let stream = TcpStream::connect(key)
        .await
        .map_err(|e| format!("connection to {key} failed: {e}"))?;
    let _ = stream.set_nodelay(true);
    let mut conn = BufReader::new(stream);
    if let Some(db) = db {
        let select = encode_command(&[b"SELECT".to_vec(), db.to_string().into_bytes()]);
        if let RespValue::Error(e) = exchange(&mut conn, &select).await? {
            return Err(format!("SELECT {db} failed: {e}"));
        }
    }
    Ok(conn)
}

/// Run one command, checking out / re-establishing a pooled connection. On a
/// transport failure of a pooled connection, transparently reconnect once.
async fn run(key: &str, db: Option<u32>, payload: &[u8]) -> Result<RespValue, String> {
    // Try a pooled connection first; on any I/O error it is dropped (not
    // returned) and we fall through to a fresh connect.
    if let Some(mut pooled) = checkout(key, db) {
        if pooled.db == db {
            match exchange(&mut pooled.conn, payload).await {
                Ok(reply) => {
                    checkin(key, pooled);
                    return Ok(reply);
                }
                Err(_) => { /* drop the dead connection; reconnect below */ }
            }
        }
        // A connection on a different db is dropped rather than re-SELECTed
        // mid-pool; a fresh, correctly-selected one is made below.
    }

    let mut conn = connect(key, db).await?;
    let reply = exchange(&mut conn, payload).await?;
    checkin(key, PooledConn { conn, db });
    Ok(reply)
}

struct RedisProto;

/// Build the JSON `FfiResponse` the host expects. A RESP error reply maps to
/// `status = 1` + `error`, a successful reply to `status = 0`, matching the
/// old built-in so `status`-based checks keep working.
fn reply_response(reply: &RespValue, latency_ms: f64) -> FfiResponse {
    let (status, status_text, error) = match reply {
        RespValue::Error(msg) => (1, msg.clone(), Some(msg.clone())),
        _ => (0, "OK".to_string(), None),
    };
    FfiResponse {
        status,
        status_text,
        headers: Vec::new(),
        body_b64: base64_encode(&reply.to_body()),
        duration_ms: latency_ms,
        error,
        extras: serde_json::json!({
            "reply_type": reply.type_name(),
            "value": reply.to_json(),
        }),
    }
}

/// An error response (transport failure, bad request, timeout).
fn error_response(latency_ms: f64, error: String) -> FfiResponse {
    FfiResponse {
        status: 0,
        status_text: "ERROR".to_string(),
        headers: Vec::new(),
        body_b64: String::new(),
        duration_ms: latency_ms,
        error: Some(error),
        extras: serde_json::json!({}),
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn handle(request_json: &str) -> FfiResponse {
    let started = Instant::now();
    let request: FfiRequest = match serde_json::from_str(request_json) {
        Ok(r) => r,
        Err(e) => return error_response(0.0, format!("invalid request JSON: {e}")),
    };
    let (key, db) = match parse_target(&request.url) {
        Ok(t) => t,
        Err(e) => return error_response(elapsed_ms(started), e),
    };
    let args = match command_args(&request) {
        Ok(a) => a,
        Err(e) => return error_response(elapsed_ms(started), e),
    };
    let payload = encode_command(&args);
    let exec = async {
        if request.timeout_ms == 0 {
            run(&key, db, &payload).await
        } else {
            tokio::time::timeout(
                Duration::from_millis(request.timeout_ms),
                run(&key, db, &payload),
            )
            .await
            .unwrap_or_else(|_| Err(format!("request timed out after {}ms", request.timeout_ms)))
        }
    };
    match runtime().block_on(exec) {
        Ok(reply) => reply_response(&reply, elapsed_ms(started)),
        Err(e) => error_response(elapsed_ms(started), e),
    }
}

/// Minimal standard-alphabet base64 decode of the request body (padded). The
/// host base64-encodes the request body with the standard alphabet.
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
    for c in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        let mut pad = 0;
        for (i, &b) in c.iter().enumerate() {
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

/// Minimal standard-alphabet base64 encode (padded) for the response body.
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

impl FfiProtocol for RedisProto {
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
            "description": "Redis (RESP) protocol: commands over a pooled TCP connection",
            "schemes": ["redis", "rediss"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(RedisProto, abi_stable::erased_types::TD_Opaque)
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

    fn req(url: &str, plugin: Option<serde_json::Value>) -> FfiRequest {
        FfiRequest {
            name: "r".into(),
            method: "GET".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: plugin,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn parses_redis_urls() {
        let (key, db) = parse_target("redis://127.0.0.1:6379").unwrap();
        assert_eq!(key, "127.0.0.1:6379");
        assert_eq!(db, None);
        let (key, db) = parse_target("redis://localhost/3").unwrap();
        assert_eq!(key, "localhost:6379");
        assert_eq!(db, Some(3));
        let (key, _) = parse_target("rediss://cache:6380").unwrap();
        assert_eq!(key, "cache:6380");
        assert!(parse_target("http://x:1").is_err());
        assert!(parse_target("not a url").is_err());
    }

    #[test]
    fn encodes_resp_command() {
        let cmd = encode_command(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
        assert_eq!(cmd, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[test]
    fn command_args_from_plugin() {
        let r = req(
            "redis://h:6379",
            Some(serde_json::json!({ "command": ["GET", "mykey"] })),
        );
        let args = command_args(&r).unwrap();
        assert_eq!(args, vec![b"GET".to_vec(), b"mykey".to_vec()]);
    }

    #[test]
    fn command_args_numbers_stringified() {
        let r = req(
            "redis://h:6379",
            Some(serde_json::json!({ "command": ["EXPIRE", "k", 60] })),
        );
        let args = command_args(&r).unwrap();
        assert_eq!(
            args,
            vec![b"EXPIRE".to_vec(), b"k".to_vec(), b"60".to_vec()]
        );
    }

    #[test]
    fn command_args_from_body_fallback() {
        let mut r = req("redis://h:6379", None);
        r.body_b64 = base64_encode(b"PING");
        let args = command_args(&r).unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
    }

    #[test]
    fn empty_command_rejected() {
        let r = req("redis://h:6379", None);
        assert!(command_args(&r).is_err());
    }

    #[tokio::test]
    async fn reads_resp_values() {
        assert_eq!(
            read_reply(&mut &b"+OK\r\n"[..]).await.unwrap(),
            RespValue::Simple("OK".to_string())
        );
        assert_eq!(
            read_reply(&mut &b"-ERR bad\r\n"[..]).await.unwrap(),
            RespValue::Error("ERR bad".to_string())
        );
        assert_eq!(
            read_reply(&mut &b":42\r\n"[..]).await.unwrap(),
            RespValue::Integer(42)
        );
        assert_eq!(
            read_reply(&mut &b"$5\r\nhello\r\n"[..]).await.unwrap(),
            RespValue::Bulk(b"hello".to_vec())
        );
        assert_eq!(
            read_reply(&mut &b"$-1\r\n"[..]).await.unwrap(),
            RespValue::Nil
        );
        let arr = read_reply(&mut &b"*2\r\n:1\r\n$1\r\nx\r\n"[..])
            .await
            .unwrap();
        assert_eq!(
            arr,
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Bulk(b"x".to_vec())])
        );
    }

    #[test]
    fn base64_roundtrips() {
        for s in ["", "a", "ab", "abc", "abcd", "PING", "SET k v"] {
            let enc = base64_encode(s.as_bytes());
            let dec = base64_decode(&enc).unwrap();
            assert_eq!(dec, s.as_bytes(), "roundtrip {s:?}");
        }
    }

    #[test]
    fn reply_value_renderings() {
        assert_eq!(RespValue::Simple("OK".into()).to_body(), b"OK");
        assert_eq!(RespValue::Integer(7).to_body(), b"7");
        assert_eq!(RespValue::Bulk(b"hi".to_vec()).to_body(), b"hi");
        assert!(RespValue::Nil.to_body().is_empty());
        assert_eq!(RespValue::Integer(7).type_name(), "integer");
        assert_eq!(RespValue::Bulk(vec![]).type_name(), "bulk");
        assert_eq!(RespValue::Nil.to_json(), serde_json::Value::Null);
    }

    #[test]
    fn reply_response_ok_and_error() {
        let ok = reply_response(&RespValue::Simple("OK".into()), 1.5);
        assert_eq!(ok.status, 0);
        assert!(ok.error.is_none());
        assert_eq!(ok.extras["reply_type"], "string");
        assert_eq!(base64_decode(&ok.body_b64).unwrap(), b"OK");

        let err = reply_response(&RespValue::Error("ERR bad".into()), 1.0);
        assert_eq!(err.status, 1);
        assert_eq!(err.error.as_deref(), Some("ERR bad"));
        assert_eq!(err.extras["reply_type"], "error");
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req("http://h/db", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "redis");
        assert_eq!(v["schemes"][0], "redis");
        assert_eq!(v["schemes"][1], "rediss");
    }

    // -----------------------------------------------------------------------
    // Integration: a real Redis server. Skips unless the env var is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d redis
    //   LOADR_TEST_REDIS_URL=redis://127.0.0.1:6379 \
    //     cargo test -p loadr-plugin-redis
    // -----------------------------------------------------------------------

    fn exec(url: &str, command: &[&str]) -> FfiResponse {
        let command: Vec<serde_json::Value> =
            command.iter().map(|a| serde_json::json!(a)).collect();
        let r = req(url, Some(serde_json::json!({ "command": command })));
        let json = serde_json::to_string(&r).unwrap();
        handle(&json)
    }

    #[test]
    fn redis_set_get_incr_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_REDIS_URL") else {
            eprintln!("skipping: LOADR_TEST_REDIS_URL not set");
            return;
        };
        let key = format!("it-{}", std::process::id());

        // SET establishes the pooled connection.
        let set = exec(&url, &["SET", &key, "hello"]);
        assert!(set.error.is_none(), "set error: {:?}", set.error);
        assert_eq!(set.status, 0);
        assert_eq!(base64_decode(&set.body_b64).unwrap(), b"OK");

        // GET reuses the pooled connection.
        let get = exec(&url, &["GET", &key]);
        assert!(get.error.is_none(), "get error: {:?}", get.error);
        assert_eq!(get.status, 0);
        assert_eq!(base64_decode(&get.body_b64).unwrap(), b"hello");
        assert_eq!(get.extras["reply_type"], "bulk");
        assert_eq!(get.extras["value"], "hello");

        // INCR returns an integer reply.
        let ctr = format!("{key}:n");
        let incr = exec(&url, &["INCR", &ctr]);
        assert!(incr.error.is_none(), "incr error: {:?}", incr.error);
        assert_eq!(incr.extras["reply_type"], "integer");

        // Clean up.
        let _ = exec(&url, &["DEL", &key, &ctr]);
    }

    #[test]
    fn redis_ping_and_error_reply() {
        let Ok(url) = std::env::var("LOADR_TEST_REDIS_URL") else {
            eprintln!("skipping: LOADR_TEST_REDIS_URL not set");
            return;
        };
        let ping = exec(&url, &["PING"]);
        assert!(ping.error.is_none(), "ping error: {:?}", ping.error);
        assert_eq!(ping.status, 0);
        assert_eq!(base64_decode(&ping.body_b64).unwrap(), b"PONG");
        assert_eq!(ping.extras["reply_type"], "string");

        // An unknown command yields a RESP error: status 1 + error set.
        let bogus = exec(&url, &["FROBNICATE", "x"]);
        assert_eq!(bogus.status, 1);
        assert!(bogus.error.is_some());
        assert_eq!(bogus.extras["reply_type"], "error");
    }

    #[test]
    fn redis_select_db() {
        let Ok(base) = std::env::var("LOADR_TEST_REDIS_URL") else {
            eprintln!("skipping: LOADR_TEST_REDIS_URL not set");
            return;
        };
        // Target db 1 explicitly via the URL path.
        let url = format!("{}/1", base.trim_end_matches('/'));
        let resp = exec(&url, &["PING"]);
        assert!(
            resp.error.is_none(),
            "select-db ping error: {:?}",
            resp.error
        );
        assert_eq!(resp.status, 0);
    }

    #[test]
    fn connection_failure_is_reported() {
        // Port 1 is never listening; the driver reports an error, not a panic.
        let resp = exec("redis://127.0.0.1:1", &["PING"]);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
