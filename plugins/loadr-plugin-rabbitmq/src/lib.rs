//! `loadr-plugin-rabbitmq` — a native protocol plugin that adds RabbitMQ
//! (AMQP 0.9.1) as a loadr load-test target.
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
//!   `block_on`s the async [`lapin`] AMQP client.
//! * An internal connection/channel pool keyed by the AMQP connection URI
//!   (`OnceCell<Mutex<HashMap<String, ConnHandle>>>`), so one TCP connection
//!   plus a publishing channel is established once per distinct URI and reused
//!   across every call and VU. `lapin::Channel` is a cheaply-cloned handle to a
//!   multiplexed channel, so sharing one is the right model under load.
//!
//! [`lapin`] is pure Rust (no C / system-lib dependencies), so this cdylib
//! cross-compiles to every loadr release target. TLS (`amqps://`) is wired to
//! `rustls` only — `native-tls`/OpenSSL is disabled.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. We read
//! the connection URI from `url` and the operation parameters from
//! `options.plugin` (populated from the YAML request's `plugin:` block, with
//! `${...}` already interpolated by the host):
//!
//! ```jsonc
//! {
//!   "operation":   "publish" | "get",  // required
//!   "exchange":    "",                  // publish: target exchange (default "")
//!   "routing_key": "work",              // publish: routing key (or `queue`)
//!   "queue":       "work",              // get: queue to consume from (or `routing_key`)
//!   "body":        "hello",             // publish: message body (string)
//!   "declare_queue": true,              // optional: declare `queue` first (durable)
//!   "ack":         true                 // get: acknowledge the message (default true)
//! }
//! ```
//!
//! The response is JSON `{ ok, latency_ms, msgs, error }` where `msgs` is the
//! number of messages published (1) or consumed (0 or 1) by the call. The host
//! turns this into `rabbitmq_reqs` / `rabbitmq_req_duration` / `rabbitmq_msgs`
//! metrics (it reads the `extras.msgs` field back).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use lapin::options::{BasicGetOptions, BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "rabbitmq";

/// The single Tokio runtime the plugin uses to drive the async AMQP client.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build rabbitmq plugin tokio runtime")
    })
}

/// A pooled connection + its publishing channel. `lapin`'s `Connection` and
/// `Channel` are cheap-to-clone handles to a multiplexed AMQP connection, so
/// one per distinct URI is the right model — established on first use and
/// reused across every call and VU.
#[derive(Clone)]
struct ConnHandle {
    // The `Connection` is held so the underlying socket stays open for the
    // lifetime of the channel; `lapin::Connection` is not `Clone`, so an `Arc`
    // keeps it alive while the handle is cloned across calls/VUs.
    _conn: Arc<Connection>,
    channel: Channel,
}

/// Connection pool keyed by AMQP URI.
fn conns() -> &'static Mutex<HashMap<String, ConnHandle>> {
    static CONNS: OnceCell<Mutex<HashMap<String, ConnHandle>>> = OnceCell::new();
    CONNS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Validate the URL scheme — only AMQP schemes are served here.
fn check_scheme(raw: &str) -> Result<(), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    match url.scheme() {
        "amqp" | "amqps" | "rabbitmq" => Ok(()),
        other => Err(format!("rabbitmq plugin cannot handle scheme `{other}`")),
    }
}

/// `rabbitmq://` is an alias the host may route to us; `lapin` only parses the
/// canonical `amqp`/`amqps` schemes, so normalise it before connecting.
fn canonical_uri(raw: &str) -> String {
    if let Some(rest) = raw.strip_prefix("rabbitmq://") {
        format!("amqp://{rest}")
    } else {
        raw.to_string()
    }
}

/// Build the lapin connection properties, driving it on our tokio runtime.
///
/// The tokio *executor* is cross-platform, so we always install it. The tokio
/// *reactor* (`tokio-reactor-trait`) is implemented only on Unix — it wraps
/// `tokio::io::unix::AsyncFd`, so it does not exist on Windows. On Windows we
/// therefore leave lapin's bundled (async-io) reactor in place; it is fully
/// cross-platform and runs the socket I/O while our tokio runtime drives the
/// futures. This keeps the single `cdylib` building on all five release
/// targets without an OpenSSL or platform-specific dependency.
fn connection_properties() -> ConnectionProperties {
    let props =
        ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
    #[cfg(unix)]
    let props = props.with_reactor(tokio_reactor_trait::Tokio);
    props
}

/// Get (or lazily create + cache) the connection/channel for `uri`.
async fn conn_for(uri: &str) -> Result<ConnHandle, String> {
    if let Some(c) = conns().lock().expect("conns lock").get(uri).cloned() {
        return Ok(c);
    }
    let props = connection_properties();
    let conn = Connection::connect(&canonical_uri(uri), props)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| format!("create channel failed: {e}"))?;
    let handle = ConnHandle {
        _conn: Arc::new(conn),
        channel,
    };
    let mut guard = conns().lock().expect("conns lock");
    // Another thread may have inserted while we awaited; keep the first.
    Ok(guard.entry(uri.to_string()).or_insert(handle).clone())
}

/// Parsed plugin options for one request.
struct AmqpOp {
    operation: String,
    exchange: String,
    routing_key: String,
    queue: Option<String>,
    body: Vec<u8>,
    declare_queue: bool,
    ack: bool,
}

impl AmqpOp {
    fn from_request(req: &FfiRequest) -> Result<AmqpOp, String> {
        let opts = req
            .options
            .as_ref()
            .ok_or_else(|| "missing `plugin:` options for rabbitmq request".to_string())?;
        let operation = opts
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "rabbitmq request requires `operation`".to_string())?
            .to_ascii_lowercase();

        let exchange = opts
            .get("exchange")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routing_key = opts
            .get("routing_key")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let queue = opts
            .get("queue")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        // For publish, `routing_key` selects the queue (default exchange) or the
        // binding key; fall back to `queue` if only that is set. For get, the
        // `queue` is the source; fall back to `routing_key` for symmetry.
        let routing_key = routing_key.or_else(|| queue.clone()).unwrap_or_default();

        // Body may be a string (UTF-8) or any JSON value (serialised compactly).
        let body = match opts.get("body") {
            Some(serde_json::Value::String(s)) => s.clone().into_bytes(),
            Some(serde_json::Value::Null) | None => Vec::new(),
            Some(other) => other.to_string().into_bytes(),
        };

        Ok(AmqpOp {
            operation,
            exchange,
            routing_key,
            queue,
            body,
            declare_queue: opts
                .get("declare_queue")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            ack: opts.get("ack").and_then(|v| v.as_bool()).unwrap_or(true),
        })
    }

    /// The queue to consume from: explicit `queue`, else the routing key.
    fn get_queue(&self) -> Result<&str, String> {
        self.queue
            .as_deref()
            .filter(|s| !s.is_empty())
            .or(Some(self.routing_key.as_str()))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "get requires `queue` (or `routing_key`)".to_string())
    }
}

/// Declare a durable queue (idempotent) so publishes/gets have a destination.
async fn declare(channel: &Channel, queue: &str) -> Result<(), String> {
    channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| format!("queue_declare failed: {e}"))?;
    Ok(())
}

/// Run one operation; returns the count of messages published/consumed.
async fn run_op(uri: &str, op: &AmqpOp) -> Result<i64, String> {
    let handle = conn_for(uri).await?;
    let channel = &handle.channel;

    match op.operation.as_str() {
        "publish" => {
            if op.declare_queue {
                if let Some(q) = op.queue.as_deref().filter(|s| !s.is_empty()) {
                    declare(channel, q).await?;
                } else if op.exchange.is_empty() && !op.routing_key.is_empty() {
                    declare(channel, &op.routing_key).await?;
                }
            }
            let confirm = channel
                .basic_publish(
                    &op.exchange,
                    &op.routing_key,
                    BasicPublishOptions::default(),
                    &op.body,
                    BasicProperties::default(),
                )
                .await
                .map_err(|e| format!("publish failed: {e}"))?;
            // Resolve the publisher confirm so a broker NACK surfaces as an
            // error rather than a silently-dropped message.
            confirm
                .await
                .map_err(|e| format!("publish confirm failed: {e}"))?;
            Ok(1)
        }
        "get" => {
            let queue = op.get_queue()?;
            if op.declare_queue {
                declare(channel, queue).await?;
            }
            let got = channel
                .basic_get(queue, BasicGetOptions { no_ack: !op.ack })
                .await
                .map_err(|e| format!("get failed: {e}"))?;
            match got {
                Some(msg) => {
                    if op.ack {
                        msg.ack(lapin::options::BasicAckOptions::default())
                            .await
                            .map_err(|e| format!("ack failed: {e}"))?;
                    }
                    Ok(1)
                }
                // Empty queue is not an error: the request succeeded, it just
                // found nothing. Reported as 0 messages.
                None => Ok(0),
            }
        }
        other => Err(format!("unknown rabbitmq operation `{other}`")),
    }
}

struct RabbitProto;

/// Build the JSON `FfiResponse` the host expects. `extras.msgs` drives the
/// `rabbitmq_msgs` counter and `error` the request-failed rate.
fn response(msgs: i64, latency_ms: f64, error: Option<String>) -> FfiResponse {
    let ok = error.is_none();
    FfiResponse {
        status: i64::from(ok),
        status_text: if ok { "OK" } else { "ERROR" }.to_string(),
        headers: Vec::new(),
        body_b64: String::new(),
        duration_ms: latency_ms,
        error,
        extras: serde_json::json!({
            "ok": ok,
            "latency_ms": latency_ms,
            "msgs": msgs,
        }),
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn handle(request_json: &str) -> FfiResponse {
    let started = Instant::now();
    let request: FfiRequest = match serde_json::from_str(request_json) {
        Ok(r) => r,
        Err(e) => return response(0, 0.0, Some(format!("invalid request JSON: {e}"))),
    };
    if let Err(e) = check_scheme(&request.url) {
        return response(0, elapsed_ms(started), Some(e));
    }
    let op = match AmqpOp::from_request(&request) {
        Ok(op) => op,
        Err(e) => return response(0, elapsed_ms(started), Some(e)),
    };
    let exec = async {
        if request.timeout_ms == 0 {
            run_op(&request.url, &op).await
        } else {
            tokio::time::timeout(
                std::time::Duration::from_millis(request.timeout_ms),
                run_op(&request.url, &op),
            )
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "operation timed out after {}ms",
                    request.timeout_ms
                ))
            })
        }
    };
    match runtime().block_on(exec) {
        Ok(msgs) => response(msgs, elapsed_ms(started), None),
        Err(e) => response(0, elapsed_ms(started), Some(e)),
    }
}

impl FfiProtocol for RabbitProto {
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
            "description": "RabbitMQ (AMQP 0.9.1) protocol: publish/get messages",
            "schemes": ["amqp", "amqps", "rabbitmq"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(RabbitProto, abi_stable::erased_types::TD_Opaque)
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

    fn req(url: &str, plugin: serde_json::Value) -> FfiRequest {
        FfiRequest {
            name: "t".into(),
            method: "POST".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: Some(plugin),
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn accepts_amqp_schemes() {
        assert!(check_scheme("amqp://u:p@h:5672/vhost").is_ok());
        assert!(check_scheme("amqps://h/").is_ok());
        assert!(check_scheme("rabbitmq://h:5672").is_ok());
        assert!(check_scheme("postgres://h/db").is_err());
        assert!(check_scheme("http://h/db").is_err());
        assert!(check_scheme("not a url").is_err());
    }

    #[test]
    fn canonical_uri_rewrites_alias() {
        assert_eq!(
            canonical_uri("rabbitmq://u:p@h:5672/v"),
            "amqp://u:p@h:5672/v"
        );
        assert_eq!(canonical_uri("amqp://h/v"), "amqp://h/v");
        assert_eq!(canonical_uri("amqps://h/v"), "amqps://h/v");
    }

    #[test]
    fn parse_publish() {
        let r = req(
            "amqp://h/",
            serde_json::json!({
                "operation": "publish",
                "routing_key": "work",
                "body": "hello",
            }),
        );
        let op = AmqpOp::from_request(&r).expect("parses");
        assert_eq!(op.operation, "publish");
        assert_eq!(op.exchange, "");
        assert_eq!(op.routing_key, "work");
        assert_eq!(op.body, b"hello");
        assert!(op.ack); // default
    }

    #[test]
    fn parse_publish_with_exchange() {
        let r = req(
            "amqp://h/",
            serde_json::json!({
                "operation": "publish",
                "exchange": "events",
                "routing_key": "user.created",
                "body": {"id": 1},
                "declare_queue": true,
            }),
        );
        let op = AmqpOp::from_request(&r).expect("parses");
        assert_eq!(op.exchange, "events");
        assert_eq!(op.routing_key, "user.created");
        assert!(op.declare_queue);
        // JSON object body is serialised compactly.
        assert_eq!(op.body, br#"{"id":1}"#);
    }

    #[test]
    fn routing_key_falls_back_to_queue() {
        let r = req(
            "amqp://h/",
            serde_json::json!({"operation": "publish", "queue": "work", "body": "x"}),
        );
        let op = AmqpOp::from_request(&r).expect("parses");
        assert_eq!(op.routing_key, "work");
    }

    #[test]
    fn parse_get() {
        let r = req(
            "amqp://h/",
            serde_json::json!({"operation": "get", "queue": "work", "ack": false}),
        );
        let op = AmqpOp::from_request(&r).expect("parses");
        assert_eq!(op.operation, "get");
        assert_eq!(op.get_queue().unwrap(), "work");
        assert!(!op.ack);
    }

    #[test]
    fn get_queue_uses_routing_key_fallback() {
        let r = req(
            "amqp://h/",
            serde_json::json!({"operation": "get", "routing_key": "work"}),
        );
        let op = AmqpOp::from_request(&r).expect("parses");
        assert_eq!(op.get_queue().unwrap(), "work");
    }

    #[test]
    fn get_without_queue_errors() {
        let r = req("amqp://h/", serde_json::json!({"operation": "get"}));
        let op = AmqpOp::from_request(&r).expect("parses");
        assert!(op.get_queue().is_err());
    }

    #[test]
    fn missing_operation_errors() {
        let r = req("amqp://h/", serde_json::json!({"queue": "x"}));
        assert!(AmqpOp::from_request(&r).is_err());
    }

    #[test]
    fn missing_options_errors() {
        let r = FfiRequest {
            name: "t".into(),
            method: "POST".into(),
            url: "amqp://h/".into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 1000,
            options: None,
            config: serde_json::Value::Null,
        };
        assert!(AmqpOp::from_request(&r).is_err());
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req(
            "postgres://h/db",
            serde_json::json!({"operation": "get", "queue": "q"}),
        ))
        .unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn response_shape_ok() {
        let resp = response(1, 1.5, None);
        assert_eq!(resp.status, 1);
        assert_eq!(resp.extras["msgs"], 1);
        assert_eq!(resp.extras["ok"], true);
        assert!(resp.error.is_none());
    }

    #[test]
    fn response_shape_error() {
        let resp = response(0, 2.0, Some("boom".into()));
        assert_eq!(resp.status, 0);
        assert_eq!(resp.status_text, "ERROR");
        assert!(resp.error.is_some());
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "rabbitmq");
        assert_eq!(v["schemes"][0], "amqp");
        assert_eq!(v["schemes"][1], "amqps");
        assert_eq!(v["schemes"][2], "rabbitmq");
    }

    // -----------------------------------------------------------------------
    // Integration: a real RabbitMQ broker. Skips unless the env var is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d rabbitmq
    //   LOADR_TEST_AMQP_URL=amqp://loadr:loadr@127.0.0.1:5672/%2f \
    //     cargo test -p loadr-plugin-rabbitmq
    // -----------------------------------------------------------------------

    fn exec(url: &str, plugin: serde_json::Value) -> FfiResponse {
        let json = serde_json::to_string(&req(url, plugin)).unwrap();
        handle(&json)
    }

    #[test]
    fn rabbitmq_publish_get_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_AMQP_URL") else {
            eprintln!("skipping: LOADR_TEST_AMQP_URL not set");
            return;
        };

        // Unique queue per test process so parallel runs don't interfere.
        let queue = format!("it-{}", std::process::id());

        // Publish to a freshly-declared durable queue (default exchange routes
        // by routing key == queue name). First call establishes the connection.
        let pub_resp = exec(
            &url,
            serde_json::json!({
                "operation": "publish",
                "routing_key": queue,
                "body": "hello-loadr",
                "declare_queue": true,
                "queue": queue,
            }),
        );
        assert!(
            pub_resp.error.is_none(),
            "publish error: {:?}",
            pub_resp.error
        );
        assert_eq!(pub_resp.status, 1);
        assert_eq!(pub_resp.extras["msgs"], 1);

        // Consume it back (connection reused).
        let get_resp = exec(
            &url,
            serde_json::json!({"operation": "get", "queue": queue}),
        );
        assert!(get_resp.error.is_none(), "get error: {:?}", get_resp.error);
        assert_eq!(get_resp.status, 1);
        assert_eq!(get_resp.extras["msgs"], 1, "one message consumed");

        // Queue is now empty: a further get succeeds with zero messages.
        let empty = exec(
            &url,
            serde_json::json!({"operation": "get", "queue": queue}),
        );
        assert!(empty.error.is_none(), "get error: {:?}", empty.error);
        assert_eq!(empty.status, 1);
        assert_eq!(empty.extras["msgs"], 0, "empty queue yields zero");
    }

    #[test]
    fn connection_failure_is_reported() {
        // Port 1 is never listening; the client reports an error, not a panic.
        let resp = exec(
            "amqp://guest:guest@127.0.0.1:1/%2f",
            serde_json::json!({"operation": "get", "queue": "x"}),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
