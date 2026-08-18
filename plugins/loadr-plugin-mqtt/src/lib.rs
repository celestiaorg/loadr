//! `loadr-plugin-mqtt` — a native protocol plugin that adds MQTT 3.1.1 as a
//! loadr load-test target.
//!
//! # How it plugs in
//!
//! loadr's native protocol ABI ([`FfiProtocol`]) is synchronous: the host calls
//! `execute(&self, request_json) -> response_json` on a single shared plugin
//! instance (`Send + Sync`), created once via `make_protocol()`. There is no
//! per-VU state across the FFI boundary, so this plugin owns all of its own
//! machinery:
//!
//! * The MQTT 3.1.1 control packets are **hand-rolled** — a fixed header
//!   (packet type + flags), a remaining-length varint and the packet body — and
//!   spoken directly over a **blocking `std::net::TcpStream`**. There is no
//!   `rumqttc`/`paho` client, therefore no async runtime and no native-tls /
//!   OpenSSL dependency, so this cdylib cross-compiles cleanly for every release
//!   target (exactly like the `nats` plugin hand-rolls its line protocol).
//! * An internal connection pool keyed by `host:port` (plus user, so distinct
//!   credentials never share a socket): `OnceLock<Mutex<HashMap<String,
//!   Vec<Box<dyn MqttIo>>>>>`. A request checks out an idle connection (running
//!   the `CONNECT`/`CONNACK` handshake only on a fresh socket), reuses it for
//!   the exchange, and returns it for the next caller. A connection left in an
//!   error state (transport failure, timeout) is dropped instead of returned,
//!   so the next caller transparently re-establishes it.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. The
//! connection target comes from `url` (`mqtt://[user[:password]@]host[:port]`)
//! and the operation from the request's `plugin:` block (`options`):
//!
//! * `operation` — `publish` or `subscribe` (required).
//! * `topic`     — the topic to publish to, or the topic filter to subscribe on
//!   (required).
//! * `qos`       — `0` (at most once, default), `1` (at least once) or `2`
//!   (exactly once).
//! * `body`      — the payload for a `publish` (a string used verbatim, or a
//!   JSON object/array serialised compactly); falls back to the request body.
//! * `retain`    — set the MQTT retain flag on a `publish` (default `false`).
//! * `timeout`   — how long a `subscribe` waits for a message (a duration
//!   string like `"250ms"`, or a number of milliseconds); defaults to the
//!   request timeout.
//!
//! The response is JSON `{ status, status_text, body_b64, duration_ms, error,
//! extras }`. `status` is `0` on success (a publish the broker accepted / a
//! confirmed QoS 1-2 publish, or a subscribe that received a message) and `1`
//! on failure (a broker refusal, an unconfirmed publish, a subscribe timeout,
//! or a connection failure), with `error` set. A `subscribe` returns the
//! received payload as the body. The host derives `mqtt_reqs` /
//! `mqtt_req_duration` from the `mqtt` plugin name, and marks the request failed
//! whenever `error` is set.

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
use serde_json::Value;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "mqtt";

/// The MQTT standard (plaintext) port, used when the URL omits one.
const DEFAULT_PORT: u16 = 1883;

/// Keep-alive advertised in `CONNECT`. `0` disables the broker's keep-alive
/// timer, so a pooled connection that sits idle between requests is not dropped
/// for inactivity — the plugin never needs to send `PINGREQ`.
const KEEP_ALIVE: u16 = 0;

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
    /// different credentials to the same broker never share a socket.
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
    if url.scheme() != "mqtt" {
        return Err(format!(
            "mqtt plugin cannot handle scheme `{}` (only plaintext `mqtt` is served)",
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

/// A single MQTT operation described by the request's `plugin:` block.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    /// Publish one message at `qos`, confirmed by the broker's acknowledgement
    /// for QoS 1 (`PUBACK`) and QoS 2 (`PUBREC`/`PUBCOMP`).
    Publish {
        topic: String,
        qos: u8,
        retain: bool,
        body: Vec<u8>,
    },
    /// Subscribe on a topic filter and return the first delivered message.
    Subscribe {
        topic: String,
        qos: u8,
        /// Optional per-request wait override; `None` falls back to the request
        /// timeout.
        timeout: Option<Duration>,
    },
}

impl Op {
    /// Operation name for `extras`.
    fn name(&self) -> &'static str {
        match self {
            Op::Publish { .. } => "publish",
            Op::Subscribe { .. } => "subscribe",
        }
    }

    fn topic(&self) -> &str {
        match self {
            Op::Publish { topic, .. } | Op::Subscribe { topic, .. } => topic,
        }
    }

    fn qos(&self) -> u8 {
        match self {
            Op::Publish { qos, .. } | Op::Subscribe { qos, .. } => *qos,
        }
    }
}

/// Resolve the message payload: prefer `plugin.body` (a string used verbatim, a
/// JSON object/array serialised compactly), else the request body.
fn resolve_body(request: &FfiRequest, opts: Option<&Value>) -> Result<Vec<u8>, String> {
    if let Some(value) = opts.and_then(|o| o.get("body")) {
        return Ok(match value {
            Value::String(s) => s.clone().into_bytes(),
            Value::Null => Vec::new(),
            other => serde_json::to_string(other)
                .map_err(|e| format!("cannot encode mqtt body: {e}"))?
                .into_bytes(),
        });
    }
    if request.body_b64.is_empty() {
        return Ok(Vec::new());
    }
    base64_decode(&request.body_b64)
}

/// Parse the `qos` option (`0`, `1` or `2`, default `0`).
fn parse_qos(opts: Option<&Value>) -> Result<u8, String> {
    match opts.and_then(|o| o.get("qos")) {
        None | Some(Value::Null) => Ok(0),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(q @ 0..=2) => Ok(q as u8),
            _ => Err(format!("qos must be 0, 1 or 2 (got {n})")),
        },
        Some(other) => Err(format!("qos must be a number 0, 1 or 2 (got {other})")),
    }
}

/// Parse an optional `timeout` (a duration string such as `"250ms"` / `"5s"` /
/// `"1m"`, or a bare number of milliseconds).
fn parse_timeout(value: &Value) -> Result<Option<Duration>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Number(n) => {
            let ms = n
                .as_f64()
                .filter(|v| *v >= 0.0)
                .ok_or_else(|| format!("timeout must be a non-negative number (got {n})"))?;
            Ok(Some(Duration::from_millis(ms as u64)))
        }
        Value::String(s) => parse_duration_str(s).map(Some),
        other => Err(format!("invalid timeout `{other}`")),
    }
}

/// Parse a duration string with an optional `ms` / `s` / `m` suffix (bare
/// numbers are milliseconds).
fn parse_duration_str(raw: &str) -> Result<Duration, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("empty timeout".to_string());
    }
    let (num, scale) = if let Some(rest) = s.strip_suffix("ms") {
        (rest, 1.0)
    } else if let Some(rest) = s.strip_suffix('s') {
        (rest, 1000.0)
    } else if let Some(rest) = s.strip_suffix('m') {
        (rest, 60_000.0)
    } else {
        (s, 1.0)
    };
    let value: f64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid timeout `{raw}`"))?;
    if value < 0.0 {
        return Err(format!("timeout must be non-negative (got `{raw}`)"));
    }
    Ok(Duration::from_millis((value * scale) as u64))
}

/// Build the [`Op`] from the request's `plugin:` block.
fn parse_op(request: &FfiRequest) -> Result<Op, String> {
    let opts = request.options.as_ref();
    let operation = opts
        .and_then(|o| o.get("operation"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            "mqtt plugin requires an `operation` (`publish` or `subscribe`)".to_string()
        })?;
    let topic = opts
        .and_then(|o| o.get("topic"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "mqtt plugin requires a non-empty `topic`".to_string())?;
    let qos = parse_qos(opts)?;
    match operation {
        "publish" | "pub" => {
            let retain = opts
                .and_then(|o| o.get("retain"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let body = resolve_body(request, opts)?;
            Ok(Op::Publish {
                topic,
                qos,
                retain,
                body,
            })
        }
        "subscribe" | "sub" => {
            let timeout = match opts.and_then(|o| o.get("timeout")) {
                Some(v) => parse_timeout(v)?,
                None => None,
            };
            Ok(Op::Subscribe {
                topic,
                qos,
                timeout,
            })
        }
        other => Err(format!(
            "unknown mqtt operation `{other}` (want `publish` or `subscribe`)"
        )),
    }
}

// ---------------------------------------------------------------------------
// Wire protocol — hand-rolled MQTT 3.1.1 control packets.
// ---------------------------------------------------------------------------

/// MQTT control packet type nibbles (the high 4 bits of the fixed header).
mod pkt {
    pub const CONNECT: u8 = 1;
    pub const CONNACK: u8 = 2;
    pub const PUBLISH: u8 = 3;
    pub const PUBACK: u8 = 4;
    pub const PUBREC: u8 = 5;
    pub const PUBREL: u8 = 6;
    pub const PUBCOMP: u8 = 7;
    pub const SUBSCRIBE: u8 = 8;
    pub const SUBACK: u8 = 9;
    pub const PINGRESP: u8 = 13;
}

/// A parsed MQTT control packet (broker → client). Only the packets the plugin
/// needs are represented in full; anything else surfaces as [`Packet::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Packet {
    /// `CONNACK` — the reply to `CONNECT`. `return_code` `0` means accepted.
    ConnAck { return_code: u8 },
    /// `PUBLISH` — an inbound message (during `subscribe`).
    Publish {
        qos: u8,
        packet_id: Option<u16>,
        payload: Vec<u8>,
    },
    /// `PUBACK` — QoS 1 publish acknowledgement.
    PubAck { packet_id: u16 },
    /// `PUBREC` — QoS 2 publish received (first half of the sender flow).
    PubRec { packet_id: u16 },
    /// `PUBREL` — QoS 2 publish release (second half of the receiver flow).
    PubRel { packet_id: u16 },
    /// `PUBCOMP` — QoS 2 publish complete.
    PubComp { packet_id: u16 },
    /// `SUBACK` — the reply to `SUBSCRIBE`, with a granted-QoS/failure code per
    /// requested topic filter.
    SubAck { packet_id: u16, codes: Vec<u8> },
    /// `PINGRESP` — the answer to a keep-alive `PINGREQ`.
    PingResp,
    /// Any other packet type; carried so the caller can report it.
    Other { packet_type: u8 },
}

/// Encode a 16-bit-length-prefixed byte string (MQTT "UTF-8 string" / binary).
fn encode_field(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Encode a remaining-length varint (1–4 bytes, 7 bits each, MSB = continue).
fn encode_remaining_length(mut len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out
}

/// Wrap a body in a fixed header (`type<<4 | flags` + remaining length).
fn frame(packet_type: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + body.len());
    out.push((packet_type << 4) | (flags & 0x0f));
    out.extend_from_slice(&encode_remaining_length(body.len()));
    out.extend_from_slice(body);
    out
}

/// Encode a `CONNECT` for MQTT 3.1.1 (protocol level 4), clean session, with
/// the client id and optional username/password.
fn encode_connect(client_id: &str, creds: Option<&Creds>, keep_alive: u16) -> Vec<u8> {
    let mut body = Vec::new();
    // Variable header: protocol name, level, connect flags, keep-alive.
    body.extend_from_slice(&encode_field(b"MQTT"));
    body.push(0x04); // protocol level 4 = MQTT 3.1.1
    let mut flags = 0x02u8; // clean session
    if let Some(c) = creds {
        flags |= 0x80; // username present
        if c.pass.is_some() {
            flags |= 0x40; // password present
        }
    }
    body.push(flags);
    body.extend_from_slice(&keep_alive.to_be_bytes());
    // Payload: client id, then credentials (order matters).
    body.extend_from_slice(&encode_field(client_id.as_bytes()));
    if let Some(c) = creds {
        body.extend_from_slice(&encode_field(c.user.as_bytes()));
        if let Some(pass) = &c.pass {
            body.extend_from_slice(&encode_field(pass.as_bytes()));
        }
    }
    frame(pkt::CONNECT, 0, &body)
}

/// Encode a `PUBLISH`. `packet_id` must be `Some` for QoS 1/2 and `None` for
/// QoS 0.
fn encode_publish(
    topic: &str,
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
    payload: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_field(topic.as_bytes()));
    if let Some(id) = packet_id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut flags = (qos & 0x03) << 1;
    if retain {
        flags |= 0x01;
    }
    frame(pkt::PUBLISH, flags, &body)
}

/// Encode a bare packet-identifier acknowledgement (`PUBACK`/`PUBREC`/`PUBCOMP`).
fn encode_ack(packet_type: u8, flags: u8, packet_id: u16) -> Vec<u8> {
    frame(packet_type, flags, &packet_id.to_be_bytes())
}

/// Encode a `SUBSCRIBE` for a single topic filter at `qos`. The reserved flags
/// nibble is `0x02`.
fn encode_subscribe(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&encode_field(topic.as_bytes()));
    body.push(qos & 0x03);
    frame(pkt::SUBSCRIBE, 0x02, &body)
}

/// Read a remaining-length varint from `reader`.
fn read_remaining_length<R: Read>(reader: &mut R) -> Result<usize, String> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for _ in 0..4 {
        let mut b = [0u8; 1];
        reader
            .read_exact(&mut b)
            .map_err(|e| format!("read failed: {e}"))?;
        value += (b[0] & 0x7f) as usize * multiplier;
        if b[0] & 0x80 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
    }
    Err("malformed remaining length (more than 4 bytes)".to_string())
}

/// Read one whole MQTT control packet from `reader`.
fn read_packet<R: Read>(reader: &mut R) -> Result<Packet, String> {
    let mut header = [0u8; 1];
    reader
        .read_exact(&mut header)
        .map_err(|e| format!("read failed: {e}"))?;
    let packet_type = header[0] >> 4;
    let flags = header[0] & 0x0f;
    let remaining = read_remaining_length(reader)?;
    let mut body = vec![0u8; remaining];
    reader
        .read_exact(&mut body)
        .map_err(|e| format!("read failed: {e}"))?;
    parse_packet(packet_type, flags, &body)
}

/// Read a big-endian `u16` packet identifier from the front of `body`.
fn read_packet_id(body: &[u8]) -> Result<u16, String> {
    match body {
        [hi, lo, ..] => Ok(u16::from_be_bytes([*hi, *lo])),
        _ => Err("packet too short for a packet identifier".to_string()),
    }
}

/// Turn a packet type + flags + body into a [`Packet`].
fn parse_packet(packet_type: u8, flags: u8, body: &[u8]) -> Result<Packet, String> {
    match packet_type {
        pkt::CONNACK => match body {
            [_session_present, return_code, ..] => Ok(Packet::ConnAck {
                return_code: *return_code,
            }),
            _ => Err("malformed CONNACK".to_string()),
        },
        pkt::PUBLISH => {
            let qos = (flags >> 1) & 0x03;
            if body.len() < 2 {
                return Err("malformed PUBLISH".to_string());
            }
            let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
            let mut idx = 2 + topic_len;
            if body.len() < idx {
                return Err("malformed PUBLISH topic".to_string());
            }
            let packet_id = if qos > 0 {
                let slice = body
                    .get(idx..idx + 2)
                    .ok_or_else(|| "malformed PUBLISH packet id".to_string())?;
                idx += 2;
                Some(u16::from_be_bytes([slice[0], slice[1]]))
            } else {
                None
            };
            Ok(Packet::Publish {
                qos,
                packet_id,
                payload: body[idx..].to_vec(),
            })
        }
        pkt::PUBACK => read_packet_id(body).map(|packet_id| Packet::PubAck { packet_id }),
        pkt::PUBREC => read_packet_id(body).map(|packet_id| Packet::PubRec { packet_id }),
        pkt::PUBREL => read_packet_id(body).map(|packet_id| Packet::PubRel { packet_id }),
        pkt::PUBCOMP => read_packet_id(body).map(|packet_id| Packet::PubComp { packet_id }),
        pkt::SUBACK => {
            let packet_id = read_packet_id(body)?;
            Ok(Packet::SubAck {
                packet_id,
                codes: body[2..].to_vec(),
            })
        }
        pkt::PINGRESP => Ok(Packet::PingResp),
        other => Ok(Packet::Other { packet_type: other }),
    }
}

/// Human-readable reason for a non-zero `CONNACK` return code.
fn connack_reason(code: u8) -> String {
    let reason = match code {
        1 => "unacceptable protocol version",
        2 => "client identifier rejected",
        3 => "server unavailable",
        4 => "bad username or password",
        5 => "not authorized",
        _ => "connection refused",
    };
    format!("broker refused CONNECT: {reason} (code {code})")
}

// ---------------------------------------------------------------------------
// Connection abstraction — a seam so the operation logic can be unit-tested
// without a real socket.
// ---------------------------------------------------------------------------

/// A live MQTT connection: send bytes, read one packet, adjust the read
/// deadline. A returned `Err` is a *transport* failure and drops the socket; a
/// broker refusal surfaces as an `Ok(Packet::ConnAck { .. })` / error string.
trait MqttIo: Send {
    fn send(&mut self, buf: &[u8]) -> Result<(), String>;
    fn read_packet(&mut self) -> Result<Packet, String>;
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String>;
}

/// Creates fresh, handshaken [`MqttIo`] connections.
trait ConnFactory: Send {
    fn connect(&self) -> Result<Box<dyn MqttIo>, String>;
}

/// A real blocking TCP connection to an MQTT broker.
struct TcpConn {
    reader: BufReader<TcpStream>,
}

impl MqttIo for TcpConn {
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

    fn read_packet(&mut self) -> Result<Packet, String> {
        read_packet(&mut self.reader)
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
    fn connect(&self) -> Result<Box<dyn MqttIo>, String> {
        let stream = TcpStream::connect(&self.addr)
            .map_err(|e| format!("connection to {} failed: {e}", self.addr))?;
        let _ = stream.set_nodelay(true);
        let mut conn = TcpConn {
            reader: BufReader::new(stream),
        };
        // A per-connection client id: pooled connections are distinct sockets,
        // and MQTT brokers evict a duplicate client id, so each must differ.
        let client_id = format!("loadr-{:x}-{:x}", std::process::id(), next_id());
        client_handshake(&mut conn, &client_id, self.creds.as_ref(), KEEP_ALIVE)?;
        Ok(Box::new(conn))
    }
}

/// Run the MQTT client handshake: send `CONNECT`, expect an accepted `CONNACK`.
fn client_handshake(
    io: &mut dyn MqttIo,
    client_id: &str,
    creds: Option<&Creds>,
    keep_alive: u16,
) -> Result<(), String> {
    io.send(&encode_connect(client_id, creds, keep_alive))?;
    match io.read_packet()? {
        Packet::ConnAck { return_code: 0 } => Ok(()),
        Packet::ConnAck { return_code } => Err(connack_reason(return_code)),
        other => Err(format!("expected CONNACK on connect, got {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Operations.
// ---------------------------------------------------------------------------

/// Publish one message and, for QoS 1/2, wait for the broker's acknowledgement.
fn do_publish(
    io: &mut dyn MqttIo,
    topic: &str,
    qos: u8,
    retain: bool,
    body: &[u8],
    packet_id: u16,
) -> Result<(), String> {
    let id = if qos > 0 { Some(packet_id) } else { None };
    io.send(&encode_publish(topic, qos, retain, id, body))?;
    match qos {
        // At most once: success as soon as the packet is written.
        0 => Ok(()),
        // At least once: wait for PUBACK matching our packet id.
        1 => wait_for(io, "PUBACK", |p| match p {
            Packet::PubAck { packet_id: got } if *got == packet_id => Some(Ok(())),
            _ => None,
        }),
        // Exactly once: PUBREC → send PUBREL → PUBCOMP.
        2 => {
            wait_for(io, "PUBREC", |p| match p {
                Packet::PubRec { packet_id: got } if *got == packet_id => Some(Ok(())),
                _ => None,
            })?;
            io.send(&encode_ack(pkt::PUBREL, 0x02, packet_id))?;
            wait_for(io, "PUBCOMP", |p| match p {
                Packet::PubComp { packet_id: got } if *got == packet_id => Some(Ok(())),
                _ => None,
            })
        }
        other => Err(format!("invalid QoS {other}")),
    }
}

/// Subscribe and return the first message delivered on the topic filter.
fn do_subscribe(
    io: &mut dyn MqttIo,
    topic: &str,
    qos: u8,
    packet_id: u16,
) -> Result<Vec<u8>, String> {
    io.send(&encode_subscribe(packet_id, topic, qos))?;
    wait_for(io, "SUBACK", |p| match p {
        Packet::SubAck {
            packet_id: got,
            codes,
        } if *got == packet_id => Some(if codes.iter().any(|c| c & 0x80 != 0) {
            Err("broker rejected subscription".to_string())
        } else {
            Ok(())
        }),
        _ => None,
    })?;
    // Wait for a delivered PUBLISH; acknowledge it per its own QoS.
    let (msg_qos, msg_id, payload) = wait_for(io, "PUBLISH", |p| match p {
        Packet::Publish {
            qos,
            packet_id,
            payload,
        } => Some(Ok((*qos, *packet_id, payload.clone()))),
        _ => None,
    })?;
    ack_incoming(io, msg_qos, msg_id)?;
    Ok(payload)
}

/// Acknowledge a received `PUBLISH` according to its QoS.
fn ack_incoming(io: &mut dyn MqttIo, qos: u8, packet_id: Option<u16>) -> Result<(), String> {
    match qos {
        0 => Ok(()),
        1 => {
            let id = packet_id.ok_or_else(|| "QoS 1 PUBLISH missing packet id".to_string())?;
            io.send(&encode_ack(pkt::PUBACK, 0, id))
        }
        2 => {
            let id = packet_id.ok_or_else(|| "QoS 2 PUBLISH missing packet id".to_string())?;
            io.send(&encode_ack(pkt::PUBREC, 0, id))?;
            wait_for(io, "PUBREL", |p| match p {
                Packet::PubRel { packet_id: got } if *got == id => Some(Ok(())),
                _ => None,
            })?;
            io.send(&encode_ack(pkt::PUBCOMP, 0, id))
        }
        other => Err(format!("invalid QoS {other} on delivered message")),
    }
}

/// Read packets until `want` returns `Some(result)`. A benign `PINGRESP` is
/// skipped; any other unmatched packet is a protocol error (the connection is
/// used by one operation at a time, so nothing else is expected). A stalled
/// broker surfaces via the read timeout as a transport error.
fn wait_for<T>(
    io: &mut dyn MqttIo,
    expected: &str,
    want: impl Fn(&Packet) -> Option<Result<T, String>>,
) -> Result<T, String> {
    loop {
        let packet = io.read_packet()?;
        if let Some(result) = want(&packet) {
            return result;
        }
        match packet {
            Packet::PingResp => continue,
            other => return Err(format!("expected {expected}, got {other:?}")),
        }
    }
}

/// Perform one operation on `io`. Returns the reply body (empty for publish).
fn perform(io: &mut dyn MqttIo, op: &Op, packet_id: u16) -> Result<Vec<u8>, String> {
    match op {
        Op::Publish {
            topic,
            qos,
            retain,
            body,
        } => do_publish(io, topic, *qos, *retain, body, packet_id).map(|()| Vec::new()),
        Op::Subscribe { topic, qos, .. } => do_subscribe(io, topic, *qos, packet_id),
    }
}

// ---------------------------------------------------------------------------
// Connection pool.
// ---------------------------------------------------------------------------

/// Idle connection pool keyed by [`Target::pool_key`]. A `Vec` is a simple LIFO
/// free-list: an operation checks out an idle connection (or makes a fresh one)
/// and returns it on success, so concurrent VUs reuse sockets per broker.
#[allow(clippy::type_complexity)]
fn pools() -> &'static Mutex<HashMap<String, Vec<Box<dyn MqttIo>>>> {
    static POOLS: OnceLock<Mutex<HashMap<String, Vec<Box<dyn MqttIo>>>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn checkout(key: &str) -> Option<Box<dyn MqttIo>> {
    let mut guard = pools().lock().ok()?;
    guard.get_mut(key)?.pop()
}

fn checkin(key: &str, conn: Box<dyn MqttIo>) {
    if let Ok(mut guard) = pools().lock() {
        guard.entry(key.to_string()).or_default().push(conn);
    }
}

/// A monotonic id used for unique client ids and packet identifiers.
fn next_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Derive a non-zero 16-bit packet identifier (MQTT forbids `0`).
fn packet_id_from(id: u64) -> u16 {
    match (id & 0xffff) as u16 {
        0 => 1,
        v => v,
    }
}

/// Run one operation, checking out / re-establishing a pooled connection. On a
/// transport failure of a pooled connection, transparently reconnect once.
fn run(
    factory: &dyn ConnFactory,
    key: &str,
    timeout: Option<Duration>,
    op: &Op,
    packet_id: u16,
) -> Result<Vec<u8>, String> {
    // Try a pooled connection first; on any transport error it is dropped (not
    // returned) and we fall through to a fresh connect.
    if let Some(mut conn) = checkout(key) {
        if conn.set_read_timeout(timeout).is_ok() {
            if let Ok(body) = perform(conn.as_mut(), op, packet_id) {
                let _ = conn.set_read_timeout(None);
                checkin(key, conn);
                return Ok(body);
            }
        }
        // Drop the dead/aborted connection; reconnect below.
    }

    let mut conn = factory.connect()?;
    conn.set_read_timeout(timeout)?;
    let body = perform(conn.as_mut(), op, packet_id)?;
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
            "topic": op.topic(),
            "qos": op.qos(),
        }),
    }
}

/// A failed operation (broker refusal, timeout, transport error) → `status = 1`.
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
    let base_timeout = match request.timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    };
    // A subscribe may override the wait; publish always uses the request timeout.
    let timeout = match &op {
        Op::Subscribe {
            timeout: Some(t), ..
        } => Some(*t),
        _ => base_timeout,
    };
    let factory = TcpConnFactory {
        addr: target.addr.clone(),
        creds: target.creds.clone(),
    };
    let packet_id = packet_id_from(next_id());
    match run(&factory, &target.pool_key(), timeout, &op, packet_id) {
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

struct MqttProto;

impl FfiProtocol for MqttProto {
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
            "description": "MQTT 3.1.1 protocol: publish and subscribe over a pooled TCP connection",
            "schemes": ["mqtt"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(MqttProto, abi_stable::erased_types::TD_Opaque)
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

    /// A scripted [`MqttIo`]: replays queued packets and records what was sent,
    /// so the operation logic is exercised without a socket.
    struct MockIo {
        packets: VecDeque<Result<Packet, String>>,
        sent: Vec<Vec<u8>>,
    }

    /// Build a `MockIo` from scripted packets. Not named `new` on purpose.
    fn mock_io(packets: Vec<Result<Packet, String>>) -> MockIo {
        MockIo {
            packets: VecDeque::from(packets),
            sent: Vec::new(),
        }
    }

    impl MqttIo for MockIo {
        fn send(&mut self, buf: &[u8]) -> Result<(), String> {
            self.sent.push(buf.to_vec());
            Ok(())
        }

        fn read_packet(&mut self) -> Result<Packet, String> {
            self.packets
                .pop_front()
                .unwrap_or_else(|| Err("mock: no more scripted packets".to_string()))
        }

        fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), String> {
            Ok(())
        }
    }

    /// Hands out pre-built [`MockIo`]s in order, counting connects so tests can
    /// assert that a reconnect happened.
    struct MockFactory {
        scripts: Mutex<VecDeque<Vec<Result<Packet, String>>>>,
        connects: Arc<AtomicUsize>,
    }

    /// Build a factory over scripted per-connection packet lists.
    fn mock_factory(
        scripts: Vec<Vec<Result<Packet, String>>>,
    ) -> (Box<dyn ConnFactory>, Arc<AtomicUsize>) {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = MockFactory {
            scripts: Mutex::new(VecDeque::from(scripts)),
            connects: connects.clone(),
        };
        (Box::new(factory), connects)
    }

    impl ConnFactory for MockFactory {
        fn connect(&self) -> Result<Box<dyn MqttIo>, String> {
            self.connects.fetch_add(1, Ordering::Relaxed);
            match self.scripts.lock().unwrap().pop_front() {
                Some(packets) => Ok(Box::new(mock_io(packets))),
                None => Err("mock: no more scripted connections".to_string()),
            }
        }
    }

    fn sent(io: &MockIo, idx: usize) -> Vec<u8> {
        io.sent[idx].clone()
    }

    fn req(url: &str, plugin: Option<Value>) -> FfiRequest {
        FfiRequest {
            name: "m".into(),
            method: "POST".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: plugin,
            config: Value::Null,
        }
    }

    // -- target parsing ------------------------------------------------------

    #[test]
    fn parses_mqtt_urls() {
        let t = parse_target("mqtt://127.0.0.1:1883").unwrap();
        assert_eq!(t.addr, "127.0.0.1:1883");
        assert_eq!(t.creds, None);
        assert_eq!(t.pool_key(), "127.0.0.1:1883");

        // Default port when omitted.
        let t = parse_target("mqtt://broker.example.com").unwrap();
        assert_eq!(t.addr, "broker.example.com:1883");

        // Userinfo → credentials, and the user namespaces the pool key.
        let t = parse_target("mqtt://alice:secret@host:1900").unwrap();
        assert_eq!(t.addr, "host:1900");
        assert_eq!(
            t.creds,
            Some(Creds {
                user: "alice".into(),
                pass: Some("secret".into()),
            })
        );
        assert_eq!(t.pool_key(), "alice@host:1900");
    }

    #[test]
    fn rejects_bad_targets() {
        // TLS variant is not served by this raw-TCP build.
        assert!(parse_target("mqtts://h:8883").is_err());
        assert!(parse_target("nats://h:4222").is_err());
        assert!(parse_target("not a url").is_err());
    }

    // -- operation parsing ---------------------------------------------------

    #[test]
    fn parse_op_publish_defaults() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({ "operation": "publish", "topic": "s/1" })),
        );
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Publish {
                topic: "s/1".into(),
                qos: 0,
                retain: false,
                body: Vec::new(),
            }
        );
    }

    #[test]
    fn parse_op_publish_with_qos_retain_and_string_body() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({
                "operation": "publish",
                "topic": "s/1",
                "qos": 1,
                "retain": true,
                "body": "hello",
            })),
        );
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Publish {
                topic: "s/1".into(),
                qos: 1,
                retain: true,
                body: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn parse_op_publish_json_body_compacted() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({
                "operation": "publish",
                "topic": "s",
                "body": { "a": 1 },
            })),
        );
        match parse_op(&r).unwrap() {
            Op::Publish { body, .. } => assert_eq!(body, br#"{"a":1}"#.to_vec()),
            other => panic!("expected publish, got {other:?}"),
        }
    }

    #[test]
    fn parse_op_body_falls_back_to_request_body() {
        let mut r = req(
            "mqtt://h",
            Some(serde_json::json!({ "operation": "publish", "topic": "s" })),
        );
        r.body_b64 = base64_encode(b"payload");
        match parse_op(&r).unwrap() {
            Op::Publish { body, .. } => assert_eq!(body, b"payload"),
            other => panic!("expected publish, got {other:?}"),
        }
    }

    #[test]
    fn parse_op_subscribe_with_timeout() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({
                "operation": "subscribe",
                "topic": "s/+/x",
                "qos": 2,
                "timeout": "250ms",
            })),
        );
        assert_eq!(
            parse_op(&r).unwrap(),
            Op::Subscribe {
                topic: "s/+/x".into(),
                qos: 2,
                timeout: Some(Duration::from_millis(250)),
            }
        );
    }

    #[test]
    fn parse_op_missing_operation_is_error() {
        let r = req("mqtt://h", Some(serde_json::json!({ "topic": "s" })));
        assert!(parse_op(&r)
            .unwrap_err()
            .contains("requires an `operation`"));
    }

    #[test]
    fn parse_op_missing_topic_is_error() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({ "operation": "publish" })),
        );
        assert!(parse_op(&r).unwrap_err().contains("non-empty `topic`"));
    }

    #[test]
    fn parse_op_unknown_operation_is_error() {
        let r = req(
            "mqtt://h",
            Some(serde_json::json!({ "operation": "unsub", "topic": "s" })),
        );
        assert!(parse_op(&r).unwrap_err().contains("unknown mqtt operation"));
    }

    #[test]
    fn parse_qos_rejects_out_of_range() {
        let opts = serde_json::json!({ "qos": 3 });
        assert!(parse_qos(Some(&opts)).unwrap_err().contains("0, 1 or 2"));
        let opts = serde_json::json!({ "qos": "hi" });
        assert!(parse_qos(Some(&opts)).is_err());
        assert_eq!(parse_qos(None).unwrap(), 0);
    }

    #[test]
    fn parse_timeout_forms() {
        assert_eq!(
            parse_timeout(&serde_json::json!("250ms")).unwrap(),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            parse_timeout(&serde_json::json!("2s")).unwrap(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            parse_timeout(&serde_json::json!("1m")).unwrap(),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            parse_timeout(&serde_json::json!(500)).unwrap(),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_timeout(&Value::Null).unwrap(), None);
        assert!(parse_timeout(&serde_json::json!("nope")).is_err());
    }

    // -- wire encoding -------------------------------------------------------

    #[test]
    fn encodes_remaining_length() {
        assert_eq!(encode_remaining_length(0), vec![0x00]);
        assert_eq!(encode_remaining_length(127), vec![0x7f]);
        // 128 → 0x80 0x01, 16383 → 0xFF 0x7F (spec examples).
        assert_eq!(encode_remaining_length(128), vec![0x80, 0x01]);
        assert_eq!(encode_remaining_length(16_383), vec![0xff, 0x7f]);
    }

    #[test]
    fn encodes_field() {
        assert_eq!(
            encode_field(b"MQTT"),
            vec![0x00, 0x04, b'M', b'Q', b'T', b'T']
        );
        assert_eq!(encode_field(b""), vec![0x00, 0x00]);
    }

    #[test]
    fn encodes_publish_qos0_and_qos1() {
        // QoS 0: no packet identifier.
        assert_eq!(
            encode_publish("a/b", 0, false, None, b"hi"),
            vec![0x30, 0x07, 0x00, 0x03, b'a', b'/', b'b', b'h', b'i'],
        );
        // QoS 1, retain: flags = (1<<1)|1 = 0x03, packet id present.
        assert_eq!(
            encode_publish("t", 1, true, Some(0x0102), b"x"),
            vec![0x33, 0x06, 0x00, 0x01, b't', 0x01, 0x02, b'x'],
        );
    }

    #[test]
    fn encodes_subscribe() {
        // type 8, flags 0x02 → 0x82; body = pid + topic + requested qos.
        assert_eq!(
            encode_subscribe(0x000a, "t/#", 1),
            vec![0x82, 0x08, 0x00, 0x0a, 0x00, 0x03, b't', b'/', b'#', 0x01],
        );
    }

    #[test]
    fn encodes_acks() {
        assert_eq!(
            encode_ack(pkt::PUBACK, 0, 0x0102),
            vec![0x40, 0x02, 0x01, 0x02]
        );
        assert_eq!(
            encode_ack(pkt::PUBREL, 0x02, 0x0102),
            vec![0x62, 0x02, 0x01, 0x02]
        );
    }

    #[test]
    fn encodes_connect_with_and_without_creds() {
        let plain = encode_connect("cid", None, 0);
        // Fixed header CONNECT, then MQTT protocol name + level 4.
        assert_eq!(plain[0], 0x10);
        assert_eq!(&plain[2..8], b"\x00\x04MQTT");
        assert_eq!(plain[8], 0x04);
        assert_eq!(plain[9], 0x02); // clean session only

        let with = encode_connect(
            "cid",
            Some(&Creds {
                user: "u".into(),
                pass: Some("p".into()),
            }),
            0,
        );
        // Username + password flags set.
        assert_eq!(with[9], 0x02 | 0x80 | 0x40);
    }

    // -- packet parsing ------------------------------------------------------

    #[test]
    fn reads_connack() {
        assert_eq!(
            read_packet(&mut &[0x20u8, 0x02, 0x00, 0x00][..]).unwrap(),
            Packet::ConnAck { return_code: 0 }
        );
        assert_eq!(
            read_packet(&mut &[0x20u8, 0x02, 0x00, 0x05][..]).unwrap(),
            Packet::ConnAck { return_code: 5 }
        );
    }

    #[test]
    fn reads_puback_and_suback() {
        assert_eq!(
            read_packet(&mut &[0x40u8, 0x02, 0x01, 0x02][..]).unwrap(),
            Packet::PubAck { packet_id: 0x0102 }
        );
        assert_eq!(
            read_packet(&mut &[0x90u8, 0x03, 0x00, 0x0a, 0x01][..]).unwrap(),
            Packet::SubAck {
                packet_id: 0x000a,
                codes: vec![0x01],
            }
        );
    }

    #[test]
    fn reads_publish_qos0_and_qos1() {
        // QoS 0 PUBLISH: 0x30, len, topic, payload.
        let bytes = [0x30u8, 0x07, 0x00, 0x03, b'a', b'/', b'b', b'h', b'i'];
        assert_eq!(
            read_packet(&mut &bytes[..]).unwrap(),
            Packet::Publish {
                qos: 0,
                packet_id: None,
                payload: b"hi".to_vec(),
            }
        );
        // QoS 1 PUBLISH: flags 0x02, includes packet id.
        let bytes = [0x32u8, 0x06, 0x00, 0x01, b't', 0x01, 0x02, b'x'];
        assert_eq!(
            read_packet(&mut &bytes[..]).unwrap(),
            Packet::Publish {
                qos: 1,
                packet_id: Some(0x0102),
                payload: b"x".to_vec(),
            }
        );
    }

    #[test]
    fn roundtrips_publish_through_the_wire() {
        let encoded = encode_publish("sensors/1", 2, false, Some(7), b"reading");
        let parsed = read_packet(&mut &encoded[..]).unwrap();
        assert_eq!(
            parsed,
            Packet::Publish {
                qos: 2,
                packet_id: Some(7),
                payload: b"reading".to_vec(),
            }
        );
    }

    #[test]
    fn rejects_malformed_packets() {
        // CONNACK with too few bytes.
        assert!(read_packet(&mut &[0x20u8, 0x00][..]).is_err());
        // A remaining length that claims more bytes than are present.
        assert!(read_packet(&mut &[0x40u8, 0x05, 0x00][..]).is_err());
    }

    // -- publish orchestration ----------------------------------------------

    #[test]
    fn publish_qos0_is_fire_and_forget() {
        let mut io = mock_io(vec![]);
        do_publish(&mut io, "t", 0, false, b"hi", 1).unwrap();
        // Only the PUBLISH is sent; nothing is awaited.
        assert_eq!(io.sent.len(), 1);
        assert_eq!(sent(&io, 0), encode_publish("t", 0, false, None, b"hi"));
    }

    #[test]
    fn publish_qos1_waits_for_puback() {
        let mut io = mock_io(vec![Ok(Packet::PubAck { packet_id: 9 })]);
        do_publish(&mut io, "t", 1, false, b"hi", 9).unwrap();
        assert_eq!(sent(&io, 0), encode_publish("t", 1, false, Some(9), b"hi"));
    }

    #[test]
    fn publish_qos1_wrong_puback_id_is_error() {
        let mut io = mock_io(vec![Ok(Packet::PubAck { packet_id: 1 })]);
        assert!(do_publish(&mut io, "t", 1, false, b"hi", 9).is_err());
    }

    #[test]
    fn publish_qos2_runs_full_handshake() {
        let mut io = mock_io(vec![
            Ok(Packet::PubRec { packet_id: 4 }),
            Ok(Packet::PubComp { packet_id: 4 }),
        ]);
        do_publish(&mut io, "t", 2, false, b"x", 4).unwrap();
        // PUBLISH, then PUBREL after PUBREC.
        assert_eq!(sent(&io, 0), encode_publish("t", 2, false, Some(4), b"x"));
        assert_eq!(sent(&io, 1), encode_ack(pkt::PUBREL, 0x02, 4));
    }

    #[test]
    fn publish_propagates_transport_error() {
        let mut io = mock_io(vec![Err("read failed: broken pipe".into())]);
        assert!(do_publish(&mut io, "t", 1, false, b"x", 1).is_err());
    }

    // -- subscribe orchestration --------------------------------------------

    #[test]
    fn subscribe_returns_first_message_qos0() {
        let mut io = mock_io(vec![
            Ok(Packet::SubAck {
                packet_id: 3,
                codes: vec![0x00],
            }),
            Ok(Packet::Publish {
                qos: 0,
                packet_id: None,
                payload: b"reading".to_vec(),
            }),
        ]);
        let body = do_subscribe(&mut io, "s/+", 0, 3).unwrap();
        assert_eq!(body, b"reading");
        assert_eq!(sent(&io, 0), encode_subscribe(3, "s/+", 0));
    }

    #[test]
    fn subscribe_acks_qos1_message() {
        let mut io = mock_io(vec![
            Ok(Packet::SubAck {
                packet_id: 3,
                codes: vec![0x01],
            }),
            Ok(Packet::Publish {
                qos: 1,
                packet_id: Some(88),
                payload: b"x".to_vec(),
            }),
        ]);
        let body = do_subscribe(&mut io, "s", 1, 3).unwrap();
        assert_eq!(body, b"x");
        // A PUBACK for the delivered message's own packet id is sent.
        assert_eq!(sent(&io, 1), encode_ack(pkt::PUBACK, 0, 88));
    }

    #[test]
    fn subscribe_rejected_subscription_is_error() {
        let mut io = mock_io(vec![Ok(Packet::SubAck {
            packet_id: 3,
            codes: vec![0x80],
        })]);
        assert_eq!(
            do_subscribe(&mut io, "s", 0, 3).unwrap_err(),
            "broker rejected subscription"
        );
    }

    // -- handshake -----------------------------------------------------------

    #[test]
    fn handshake_accepts_connack_zero() {
        let mut io = mock_io(vec![Ok(Packet::ConnAck { return_code: 0 })]);
        client_handshake(&mut io, "cid", None, 0).unwrap();
        assert_eq!(sent(&io, 0), encode_connect("cid", None, 0));
    }

    #[test]
    fn handshake_rejected_connack_is_error() {
        let mut io = mock_io(vec![Ok(Packet::ConnAck { return_code: 5 })]);
        assert!(client_handshake(&mut io, "cid", None, 0)
            .unwrap_err()
            .contains("not authorized"));
    }

    // -- run + pool retry ----------------------------------------------------

    #[test]
    fn run_connects_and_performs_publish() {
        let (factory, connects) = mock_factory(vec![vec![Ok(Packet::PubAck { packet_id: 1 })]]);
        let key = "run_connects_and_performs_publish";
        let op = Op::Publish {
            topic: "t".into(),
            qos: 1,
            retain: false,
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
        let (factory, connects) = mock_factory(vec![
            vec![Err("dead socket".into())],
            vec![Ok(Packet::PubAck { packet_id: 1 })],
        ]);
        let key = "run_reconnects_when_pooled_connection_fails";
        let op = Op::Publish {
            topic: "t".into(),
            qos: 1,
            retain: false,
            body: b"x".to_vec(),
        };
        // Seed the pool with the first (doomed) connection.
        checkin(key, factory.connect().unwrap());
        assert_eq!(connects.load(Ordering::Relaxed), 1);

        run(factory.as_ref(), key, None, &op, 1).unwrap();
        assert_eq!(connects.load(Ordering::Relaxed), 2);
    }

    // -- responses -----------------------------------------------------------

    #[test]
    fn ok_response_encodes_body_and_extras() {
        let op = Op::Subscribe {
            topic: "s/+".into(),
            qos: 1,
            timeout: None,
        };
        let resp = ok_response(&op, b"reading".to_vec(), 1.5);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_none());
        assert_eq!(base64_decode(&resp.body_b64).unwrap(), b"reading");
        assert_eq!(resp.extras["operation"], "subscribe");
        assert_eq!(resp.extras["topic"], "s/+");
        assert_eq!(resp.extras["qos"], 1);
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
        let json = serde_json::to_string(&req("mqtts://h:8883", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 1);
        assert!(resp.error.unwrap().contains("cannot handle scheme"));
    }

    #[test]
    fn packet_id_is_never_zero() {
        assert_eq!(packet_id_from(0), 1);
        assert_eq!(packet_id_from(0x1_0000), 1);
        assert_eq!(packet_id_from(7), 7);
    }

    #[test]
    fn info_declares_scheme() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "mqtt");
        assert_eq!(v["schemes"][0], "mqtt");
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
    // Integration: a real MQTT broker. Skips unless the env var is set.
    //   docker run -p 1883:1883 eclipse-mosquitto:latest \
    //     mosquitto -c /mosquitto-no-auth.conf
    //   LOADR_TEST_MQTT_URL=mqtt://127.0.0.1:1883 cargo test -p loadr-plugin-mqtt
    // -----------------------------------------------------------------------

    #[test]
    fn mqtt_publish_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_MQTT_URL") else {
            eprintln!("skipping: LOADR_TEST_MQTT_URL not set");
            return;
        };
        let r = req(
            &url,
            Some(serde_json::json!({
                "operation": "publish",
                "topic": format!("loadr/test/{}", std::process::id()),
                "qos": 1,
                "body": "hello",
            })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert!(resp.error.is_none(), "publish error: {:?}", resp.error);
        assert_eq!(resp.status, 0);
    }

    #[test]
    fn mqtt_connection_failure_is_reported() {
        if std::env::var("LOADR_TEST_MQTT_URL").is_err() {
            eprintln!("skipping: LOADR_TEST_MQTT_URL not set");
            return;
        }
        // Port 1 is never an MQTT broker; the driver reports an error, not a panic.
        let r = req(
            "mqtt://127.0.0.1:1",
            Some(serde_json::json!({ "operation": "publish", "topic": "s", "body": "x" })),
        );
        let resp = handle(&serde_json::to_string(&r).unwrap());
        assert_eq!(resp.status, 1);
        assert!(resp.error.is_some());
    }
}
