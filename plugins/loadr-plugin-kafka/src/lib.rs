//! `loadr-plugin-kafka` — a native protocol plugin that adds Apache Kafka as a
//! loadr load-test target.
//!
//! # How it plugs in
//!
//! loadr's native protocol ABI ([`FfiProtocol`]) is synchronous: the host calls
//! `execute(&self, request_json) -> response_json` on a single shared plugin
//! instance (`Send + Sync`), created once via `make_protocol()`. There is no
//! per-VU state across the FFI boundary, so this plugin owns all of its async
//! machinery:
//!
//! * A single multi-thread Tokio runtime, created once, on which every call
//!   `block_on`s the async `rskafka` client.
//! * An internal client pool keyed by the broker authority parsed from the
//!   request URL (`OnceCell<Mutex<HashMap<String, Arc<Client>>>>`), so one
//!   `rskafka::client::Client` (a cheap-to-share connection manager) is created
//!   once per distinct broker URL and reused across every call and VU.
//! * A per-`(broker, topic, partition)` `PartitionClient` cache layered on top,
//!   so a partition handle is established once and reused.
//!
//! The Kafka client is [`rskafka`] — a PURE-RUST client (no `librdkafka` / C
//! toolchain), so this cdylib cross-compiles cleanly to every release target.
//! Compression codecs (the only C-backed parts of rskafka) are disabled; the
//! plugin produces/fetches uncompressed, keeping the tree fully pure-Rust.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. We read
//! the broker + topic from `url` (`kafka://host:9092/topic`) and the operation
//! parameters from `options.plugin` (the YAML request's `plugin:` block, with
//! `${...}` already interpolated by the host):
//!
//! ```jsonc
//! {
//!   "operation": "produce" | "fetch", // required
//!   "topic":     "events",            // optional; else taken from the URL path
//!   "key":       "k1",                // produce: optional record key
//!   "value":     "hello",             // produce: record value (string)
//!   "partition": 0,                   // optional (default 0)
//!   "offset":    0,                   // fetch: start offset (default 0)
//!   "max_bytes": 1000000,             // fetch: max bytes (default 1 MiB)
//!   "max_wait_ms": 500                // fetch: broker max wait (default 500)
//! }
//! ```
//!
//! The response is JSON `{ ok, latency_ms, offset, records, msgs, error }`. The
//! host turns this into `kafka_reqs` / `kafka_req_duration` / `kafka_msgs`
//! metrics (it reads the `extras.msgs` field back: 1 per produced record, N for
//! a fetch returning N records).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use once_cell::sync::OnceCell;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "kafka";
const DEFAULT_MAX_BYTES: i32 = 1_000_000;
const DEFAULT_MAX_WAIT_MS: i32 = 500;

/// The single Tokio runtime the plugin uses to drive the async client.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build kafka plugin tokio runtime")
    })
}

/// Broker clients keyed by broker authority (`host:port`). A `Client` is a
/// shared connection manager, so one per distinct broker URL is the right model.
fn clients() -> &'static Mutex<HashMap<String, Arc<Client>>> {
    static CLIENTS: OnceCell<Mutex<HashMap<String, Arc<Client>>>> = OnceCell::new();
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Partition clients keyed by `broker|topic|partition`, established once and
/// reused across every call and VU.
type PartitionKey = String;
fn partitions() -> &'static Mutex<HashMap<PartitionKey, Arc<PartitionClient>>> {
    static PARTS: OnceCell<Mutex<HashMap<PartitionKey, Arc<PartitionClient>>>> = OnceCell::new();
    PARTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Parsed connection target: the broker bootstrap address(es) and the topic
/// from the URL path (`kafka://host:9092/topic`).
struct Target {
    /// Cache key + bootstrap list: the broker authority, e.g. `host:9092`.
    broker: String,
    /// Bootstrap broker addresses for the rskafka `ClientBuilder`.
    bootstrap: Vec<String>,
    /// Topic from the URL path, if present.
    url_topic: Option<String>,
}

/// Parse `kafka://host:9092/topic` into its broker authority and topic.
fn parse_target(raw: &str) -> Result<Target, String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    if url.scheme() != "kafka" {
        return Err(format!(
            "kafka plugin cannot handle scheme `{}`",
            url.scheme()
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("kafka url `{raw}` has no host"))?;
    let port = url.port().unwrap_or(9092);
    let broker = format!("{host}:{port}");
    let url_topic = url
        .path()
        .trim_matches('/')
        .split('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(Target {
        broker: broker.clone(),
        bootstrap: vec![broker],
        url_topic,
    })
}

/// Get (or lazily create + cache) the broker client for `target`.
async fn client_for(target: &Target) -> Result<Arc<Client>, String> {
    if let Some(c) = clients()
        .lock()
        .expect("clients lock")
        .get(&target.broker)
        .cloned()
    {
        return Ok(c);
    }
    let client = ClientBuilder::new(target.bootstrap.clone())
        .build()
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let client = Arc::new(client);
    let mut guard = clients().lock().expect("clients lock");
    // Another thread may have inserted while we awaited; keep the first.
    Ok(guard.entry(target.broker.clone()).or_insert(client).clone())
}

/// Get (or lazily create + cache) the partition client for the topic+partition.
async fn partition_for(
    target: &Target,
    topic: &str,
    partition: i32,
) -> Result<Arc<PartitionClient>, String> {
    let key = format!("{}|{}|{}", target.broker, topic, partition);
    if let Some(p) = partitions().lock().expect("parts lock").get(&key).cloned() {
        return Ok(p);
    }
    let client = client_for(target).await?;
    let pc = client
        .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
        .await
        .map_err(|e| format!("partition client for `{topic}`/{partition} failed: {e}"))?;
    let pc = Arc::new(pc);
    let mut guard = partitions().lock().expect("parts lock");
    Ok(guard.entry(key).or_insert(pc).clone())
}

/// Parsed plugin options for one Kafka request.
struct KafkaOp {
    operation: String,
    topic: Option<String>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    partition: i32,
    offset: i64,
    max_bytes: i32,
    max_wait_ms: i32,
}

/// Coerce a JSON value to bytes for a record key/value: strings pass through,
/// other scalars use their JSON text so numbers/bools are usable too.
fn to_bytes(v: &serde_json::Value) -> Option<Vec<u8>> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone().into_bytes()),
        other => Some(other.to_string().into_bytes()),
    }
}

impl KafkaOp {
    fn from_request(req: &FfiRequest) -> Result<KafkaOp, String> {
        let opts = req
            .options
            .as_ref()
            .ok_or_else(|| "missing `plugin:` options for kafka request".to_string())?;
        let operation = opts
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "kafka request requires `operation`".to_string())?
            .to_ascii_lowercase();

        let key = opts.get("key").and_then(to_bytes);
        let value = opts.get("value").and_then(to_bytes);
        let partition = opts
            .get("partition")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32;
        let offset = opts
            .get("offset")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let max_bytes = opts
            .get("max_bytes")
            .and_then(serde_json::Value::as_i64)
            .map(|n| n as i32)
            .unwrap_or(DEFAULT_MAX_BYTES);
        let max_wait_ms = opts
            .get("max_wait_ms")
            .and_then(serde_json::Value::as_i64)
            .map(|n| n as i32)
            .unwrap_or(DEFAULT_MAX_WAIT_MS);

        Ok(KafkaOp {
            operation,
            topic: opts
                .get("topic")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            key,
            value,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        })
    }
}

/// Outcome of one operation: the offset (produce) or count of records (fetch),
/// plus the number of messages handled (for the `kafka_msgs` counter).
struct OpResult {
    offset: i64,
    records: i64,
    msgs: i64,
}

async fn run_op(target: &Target, op: &KafkaOp) -> Result<OpResult, String> {
    let topic = op
        .topic
        .clone()
        .or_else(|| target.url_topic.clone())
        .ok_or_else(|| {
            "no topic: set `topic` in the request or include it in the URL path".to_string()
        })?;
    let pc = partition_for(target, &topic, op.partition).await?;

    match op.operation.as_str() {
        "produce" => {
            let record = Record {
                key: op.key.clone(),
                value: op.value.clone(),
                headers: std::collections::BTreeMap::new(),
                timestamp: chrono::Utc::now(),
            };
            let offsets = pc
                .produce(vec![record], Compression::NoCompression)
                .await
                .map_err(|e| format!("produce failed: {e}"))?;
            let offset = offsets.first().copied().unwrap_or(-1);
            Ok(OpResult {
                offset,
                records: 1,
                msgs: 1,
            })
        }
        "fetch" => {
            let (records, _high_watermark) = pc
                .fetch_records(op.offset, 1..op.max_bytes, op.max_wait_ms)
                .await
                .map_err(|e| format!("fetch failed: {e}"))?;
            let n = records.len() as i64;
            let last = records.last().map(|r| r.offset).unwrap_or(op.offset);
            Ok(OpResult {
                offset: last,
                records: n,
                msgs: n,
            })
        }
        other => Err(format!("unknown kafka operation `{other}`")),
    }
}

struct KafkaProto;

/// Build the JSON `FfiResponse` the host expects. `extras.msgs` drives the
/// `kafka_msgs` counter and `error` the request-failed rate.
fn response(result: Option<&OpResult>, latency_ms: f64, error: Option<String>) -> FfiResponse {
    let ok = error.is_none();
    let (offset, records, msgs) = match result {
        Some(r) => (r.offset, r.records, r.msgs),
        None => (-1, 0, 0),
    };
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
            "offset": offset,
            "records": records,
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
        Err(e) => return response(None, 0.0, Some(format!("invalid request JSON: {e}"))),
    };
    let target = match parse_target(&request.url) {
        Ok(t) => t,
        Err(e) => return response(None, elapsed_ms(started), Some(e)),
    };
    let op = match KafkaOp::from_request(&request) {
        Ok(op) => op,
        Err(e) => return response(None, elapsed_ms(started), Some(e)),
    };
    let exec = async {
        if request.timeout_ms == 0 {
            run_op(&target, &op).await
        } else {
            tokio::time::timeout(
                std::time::Duration::from_millis(request.timeout_ms),
                run_op(&target, &op),
            )
            .await
            .unwrap_or_else(|_| Err(format!("kafka op timed out after {}ms", request.timeout_ms)))
        }
    };
    match runtime().block_on(exec) {
        Ok(result) => response(Some(&result), elapsed_ms(started), None),
        Err(e) => response(None, elapsed_ms(started), Some(e)),
    }
}

impl FfiProtocol for KafkaProto {
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
            "description": "Apache Kafka protocol: produce/fetch via rskafka",
            "schemes": ["kafka"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(KafkaProto, abi_stable::erased_types::TD_Opaque)
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
            name: "t".into(),
            method: "POST".into(),
            url: url.into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 5000,
            options: plugin,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn parse_target_extracts_broker_and_topic() {
        let t = parse_target("kafka://broker:9092/events").unwrap();
        assert_eq!(t.broker, "broker:9092");
        assert_eq!(t.bootstrap, vec!["broker:9092".to_string()]);
        assert_eq!(t.url_topic.as_deref(), Some("events"));
    }

    #[test]
    fn parse_target_defaults_port_and_allows_no_topic() {
        let t = parse_target("kafka://broker").unwrap();
        assert_eq!(t.broker, "broker:9092");
        assert!(t.url_topic.is_none());
    }

    #[test]
    fn parse_target_rejects_wrong_scheme() {
        assert!(parse_target("http://broker:9092/t").is_err());
        assert!(parse_target("not a url").is_err());
    }

    #[test]
    fn parse_produce_op() {
        let r = req(
            "kafka://b:9092/events",
            Some(serde_json::json!({
                "operation": "produce",
                "key": "k1",
                "value": "hello",
                "partition": 2,
            })),
        );
        let op = KafkaOp::from_request(&r).unwrap();
        assert_eq!(op.operation, "produce");
        assert_eq!(op.key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(op.value.as_deref(), Some(&b"hello"[..]));
        assert_eq!(op.partition, 2);
    }

    #[test]
    fn parse_fetch_op_defaults() {
        let r = req(
            "kafka://b:9092/events",
            Some(serde_json::json!({ "operation": "fetch" })),
        );
        let op = KafkaOp::from_request(&r).unwrap();
        assert_eq!(op.operation, "fetch");
        assert_eq!(op.partition, 0);
        assert_eq!(op.offset, 0);
        assert_eq!(op.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(op.max_wait_ms, DEFAULT_MAX_WAIT_MS);
    }

    #[test]
    fn to_bytes_coerces_scalars() {
        assert_eq!(to_bytes(&serde_json::json!("s")), Some(b"s".to_vec()));
        assert_eq!(to_bytes(&serde_json::json!(42)), Some(b"42".to_vec()));
        assert_eq!(to_bytes(&serde_json::json!(true)), Some(b"true".to_vec()));
        assert_eq!(to_bytes(&serde_json::Value::Null), None);
    }

    #[test]
    fn topic_override_takes_precedence() {
        let r = req(
            "kafka://b:9092/url-topic",
            Some(serde_json::json!({ "operation": "produce", "topic": "opt-topic" })),
        );
        let op = KafkaOp::from_request(&r).unwrap();
        assert_eq!(op.topic.as_deref(), Some("opt-topic"));
    }

    #[test]
    fn missing_operation_errors() {
        let r = req(
            "kafka://b:9092/t",
            Some(serde_json::json!({ "value": "x" })),
        );
        assert!(KafkaOp::from_request(&r).is_err());
    }

    #[test]
    fn missing_options_errors() {
        let r = req("kafka://b:9092/t", None);
        assert!(KafkaOp::from_request(&r).is_err());
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req("http://b/t", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn response_shape_ok() {
        let resp = response(
            Some(&OpResult {
                offset: 7,
                records: 3,
                msgs: 3,
            }),
            1.5,
            None,
        );
        assert_eq!(resp.status, 1);
        assert_eq!(resp.extras["offset"], 7);
        assert_eq!(resp.extras["records"], 3);
        assert_eq!(resp.extras["msgs"], 3);
        assert_eq!(resp.extras["ok"], true);
        assert!(resp.error.is_none());
    }

    #[test]
    fn response_shape_error() {
        let resp = response(None, 2.0, Some("boom".to_string()));
        assert_eq!(resp.status, 0);
        assert_eq!(resp.extras["msgs"], 0);
        assert_eq!(resp.error.as_deref(), Some("boom"));
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "kafka");
        assert_eq!(v["schemes"][0], "kafka");
    }

    // -----------------------------------------------------------------------
    // Integration: a real Kafka broker. Skips unless LOADR_TEST_KAFKA_URL is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d kafka kafka-init
    //   LOADR_TEST_KAFKA_URL=kafka://127.0.0.1:9092/loadr-demo \
    //     cargo test -p loadr-plugin-kafka
    // -----------------------------------------------------------------------

    fn exec(url: &str, plugin: serde_json::Value) -> FfiResponse {
        let json = serde_json::to_string(&req(url, Some(plugin))).unwrap();
        handle(&json)
    }

    #[test]
    fn kafka_produce_then_fetch_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_KAFKA_URL") else {
            eprintln!("skipping: LOADR_TEST_KAFKA_URL not set");
            return;
        };

        // Produce a uniquely-tagged record; first call establishes the client.
        let tag = format!("it-{}", std::process::id());
        let prod = exec(
            &url,
            serde_json::json!({
                "operation": "produce",
                "key": tag,
                "value": format!("hello-{tag}"),
            }),
        );
        assert!(prod.error.is_none(), "produce error: {:?}", prod.error);
        assert_eq!(prod.status, 1);
        assert_eq!(prod.extras["msgs"], 1);
        let offset = prod.extras["offset"].as_i64().expect("offset");
        assert!(offset >= 0, "offset should be assigned, got {offset}");

        // Fetch from the produced offset (pool reused) — at least our record.
        let fetch = exec(
            &url,
            serde_json::json!({
                "operation": "fetch",
                "offset": offset,
                "max_wait_ms": 1000,
            }),
        );
        assert!(fetch.error.is_none(), "fetch error: {:?}", fetch.error);
        assert_eq!(fetch.status, 1);
        assert!(
            fetch.extras["records"].as_i64().unwrap() >= 1,
            "expected at least one record at offset {offset}"
        );
    }

    #[test]
    fn kafka_connection_failure_is_reported() {
        // Port 1 is never listening; the client reports an error, not a panic.
        let resp = exec(
            "kafka://127.0.0.1:1/t",
            serde_json::json!({ "operation": "produce", "value": "x" }),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
