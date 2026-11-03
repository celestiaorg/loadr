//! `loadr-plugin-elasticsearch` — a native protocol plugin that adds
//! Elasticsearch as a loadr load-test target.
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
//!   `block_on`s the async HTTP request.
//! * An internal connection pool keyed by the request's base URL
//!   (`OnceCell<Mutex<HashMap<String, EsClient>>>`). A hyper-util legacy
//!   `Client` is itself an internally-pooled, cheaply-cloneable handle, so one
//!   per distinct base URL is created once and reused across every call and VU.
//!
//! Elasticsearch's API is plain HTTP/JSON, so this plugin talks to it directly
//! over the project's existing **hyper + hyper-rustls** stack rather than the
//! heavy official `elasticsearch` crate. `hyper-rustls` uses `ring` + webpki
//! roots — pure-Rust TLS, no system OpenSSL — so the cdylib cross-compiles
//! cleanly for every release target (linux gnu x64/arm64, macOS x64/arm64,
//! windows-msvc).
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. We read
//! the base URL from `url` (scheme `elasticsearch://` / `es://` is mapped onto
//! `http://`, `http(s)://` is used as-is; basic-auth credentials in the URL are
//! honoured) and the operation parameters from `options.plugin` (populated from
//! the YAML request's `plugin:` block, with `${...}` already interpolated):
//!
//! ```jsonc
//! {
//!   "operation": "index" | "get" | "search" | "bulk",
//!   "index":     "products",         // required (target index / alias)
//!   "id":        "abc",              // index (optional), get (required)
//!   "document":  { ... },            // index: the document body
//!   "query":     { ... },            // search: an ES query DSL body
//!   "operations": [ { ... } ]        // bulk: NDJSON action/source lines as
//!                                     //       JSON objects (see below)
//! }
//! ```
//!
//! The response is JSON `{ ok, latency_ms, docs, hits, error }`:
//!   * `docs`  — documents written (index = 1, bulk = items succeeded).
//!   * `hits`  — search hits returned (search only).
//!
//! The host turns this into `elasticsearch_reqs` / `elasticsearch_req_duration`
//! / `elasticsearch_docs` metrics (it reads the `extras.docs` field back).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use base64::Engine;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "elasticsearch";

/// A pooled HTTP client paired with its resolved base origin + optional
/// basic-auth header. Cheap to clone: the inner `Client` is a shared,
/// internally-pooled handle.
#[derive(Clone)]
struct EsClient {
    http: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// `scheme://host[:port]` with no trailing slash, no userinfo.
    base: String,
    /// `Basic <b64>` header value when the URL carried userinfo, else `None`.
    auth: Option<String>,
}

/// The single Tokio runtime the plugin uses to drive async HTTP.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build elasticsearch plugin tokio runtime")
    })
}

/// Client pool keyed by the original request URL. One hyper client (and thus
/// one connection pool) per distinct base URL, reused across every call and VU.
fn clients() -> &'static Mutex<HashMap<String, EsClient>> {
    static CLIENTS: OnceCell<Mutex<HashMap<String, EsClient>>> = OnceCell::new();
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Map a request URL to an HTTP(S) base origin + basic-auth header.
///
/// `elasticsearch://` and `es://` are aliases mapped onto plain `http://`;
/// `http://` / `https://` are used as-is. Userinfo (`user:pass@`) becomes a
/// `Basic` auth header and is stripped from the origin.
fn parse_target(raw: &str) -> Result<(String, Option<String>), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    let scheme = match url.scheme() {
        "elasticsearch" | "es" => "http",
        "http" => "http",
        "https" => "https",
        other => {
            return Err(format!(
                "elasticsearch plugin cannot handle scheme `{other}`"
            ))
        }
    };
    let host = url
        .host_str()
        .ok_or_else(|| format!("url `{raw}` has no host"))?;
    let base = match url.port() {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    };

    let auth = if url.username().is_empty() && url.password().is_none() {
        None
    } else {
        // Percent-decode userinfo back to raw bytes before encoding the header.
        let user = percent_decode(url.username());
        let pass = url.password().map(percent_decode).unwrap_or_default();
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        Some(format!("Basic {token}"))
    };
    Ok((base, auth))
}

/// Minimal percent-decoding of URL userinfo (`%XX` -> byte), lossy UTF-8.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_val(bytes[i + 1]);
            let lo = hex_val(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Get (or lazily create + cache) the client for `raw_url`.
fn client_for(raw_url: &str) -> Result<EsClient, String> {
    if let Some(c) = clients()
        .lock()
        .expect("clients lock")
        .get(raw_url)
        .cloned()
    {
        return Ok(c);
    }
    let (base, auth) = parse_target(raw_url)?;
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // lets the same connector serve both plaintext (the harness) and TLS.
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let http = Client::builder(TokioExecutor::new()).build(tls);
    let client = EsClient { http, base, auth };
    let mut guard = clients().lock().expect("clients lock");
    Ok(guard.entry(raw_url.to_string()).or_insert(client).clone())
}

/// An Elasticsearch operation parsed from one request's `plugin:` block.
enum EsOp {
    /// `PUT /{index}/_doc/{id}` (or `POST /{index}/_doc` when no id).
    Index {
        index: String,
        id: Option<String>,
        document: serde_json::Value,
    },
    /// `GET /{index}/_doc/{id}`.
    Get { index: String, id: String },
    /// `POST /{index}/_search` with an optional query DSL body.
    Search {
        index: String,
        query: Option<serde_json::Value>,
    },
    /// `POST /{index}/_bulk` (or `POST /_bulk`) with NDJSON lines.
    Bulk {
        index: Option<String>,
        operations: Vec<serde_json::Value>,
    },
}

impl EsOp {
    fn from_request(req: &FfiRequest) -> Result<EsOp, String> {
        let opts = req
            .options
            .as_ref()
            .ok_or_else(|| "missing `plugin:` options for elasticsearch request".to_string())?;
        let operation = opts
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "elasticsearch request requires `operation`".to_string())?
            .to_ascii_lowercase();
        let index = opts
            .get("index")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let id = opts.get("id").and_then(|v| v.as_str()).map(str::to_string);

        match operation.as_str() {
            "index" => {
                let document = opts
                    .get("document")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .ok_or_else(|| "index requires `document`".to_string())?;
                Ok(EsOp::Index {
                    index: index.ok_or_else(|| "index requires `index`".to_string())?,
                    id,
                    document,
                })
            }
            "get" => Ok(EsOp::Get {
                index: index.ok_or_else(|| "get requires `index`".to_string())?,
                id: id.ok_or_else(|| "get requires `id`".to_string())?,
            }),
            "search" => Ok(EsOp::Search {
                index: index.ok_or_else(|| "search requires `index`".to_string())?,
                query: opts.get("query").filter(|v| !v.is_null()).cloned(),
            }),
            "bulk" => {
                let operations = match opts.get("operations") {
                    Some(serde_json::Value::Array(arr)) => arr.clone(),
                    _ => return Err("bulk requires `operations` (a JSON array)".to_string()),
                };
                if operations.is_empty() {
                    return Err("bulk `operations` is empty".to_string());
                }
                Ok(EsOp::Bulk { index, operations })
            }
            other => Err(format!("unknown elasticsearch operation `{other}`")),
        }
    }
}

/// Outcome of one operation: documents written and search hits returned.
struct OpResult {
    docs: i64,
    hits: i64,
}

/// One HTTP exchange: method + path + optional JSON body. Returns the status
/// code and the parsed JSON body (Elasticsearch always replies JSON).
async fn http_json(
    client: &EsClient,
    method: &str,
    path: &str,
    content_type: &str,
    body: Bytes,
    timeout_ms: u64,
) -> Result<(u16, serde_json::Value), String> {
    let uri = format!("{}{}", client.base, path);
    let mut builder = Request::builder()
        .method(method)
        .uri(&uri)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .header(hyper::header::ACCEPT, "application/json");
    if let Some(auth) = &client.auth {
        builder = builder.header(hyper::header::AUTHORIZATION, auth);
    }
    let request = builder
        .body(Full::new(body))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.http.request(request);
    let resp = if timeout_ms == 0 {
        send.await
            .map_err(|e| format!("request to {uri} failed: {e}"))?
    } else {
        tokio::time::timeout(Duration::from_millis(timeout_ms), send)
            .await
            .map_err(|_| format!("request to {uri} timed out after {timeout_ms}ms"))?
            .map_err(|e| format!("request to {uri} failed: {e}"))?
    };

    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("reading response body failed: {e}"))?
        .to_bytes();
    let json: serde_json::Value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into()))
    };
    Ok((status, json))
}

/// Render bulk `operations` as Elasticsearch NDJSON (one JSON object per line,
/// trailing newline). The host passes structured action/source objects; we
/// serialise each onto its own line.
fn bulk_ndjson(operations: &[serde_json::Value]) -> Result<Bytes, String> {
    let mut out = String::new();
    for op in operations {
        let line = serde_json::to_string(op).map_err(|e| format!("encoding bulk line: {e}"))?;
        out.push_str(&line);
        out.push('\n');
    }
    Ok(Bytes::from(out))
}

/// Count documents successfully written by a `_bulk` response (items whose
/// per-action status is 2xx).
fn bulk_success_count(body: &serde_json::Value) -> i64 {
    let Some(items) = body.get("items").and_then(|v| v.as_array()) else {
        return 0;
    };
    items
        .iter()
        .filter(|item| {
            // Each item is `{ "<action>": { "status": <code>, ... } }`.
            item.as_object()
                .and_then(|m| m.values().next())
                .and_then(|action| action.get("status"))
                .and_then(serde_json::Value::as_i64)
                .map(|s| (200..300).contains(&s))
                .unwrap_or(false)
        })
        .count() as i64
}

async fn run_op(raw_url: &str, op: &EsOp, timeout_ms: u64) -> Result<OpResult, String> {
    let client = client_for(raw_url)?;
    match op {
        EsOp::Index {
            index,
            id,
            document,
        } => {
            let (method, path) = match id {
                Some(id) => ("PUT", format!("/{index}/_doc/{}", enc(id))),
                None => ("POST", format!("/{index}/_doc")),
            };
            let body = Bytes::from(
                serde_json::to_vec(document).map_err(|e| format!("encoding document: {e}"))?,
            );
            let (status, json) =
                http_json(&client, method, &path, "application/json", body, timeout_ms).await?;
            check_status(status, &json)?;
            Ok(OpResult { docs: 1, hits: 0 })
        }
        EsOp::Get { index, id } => {
            let path = format!("/{index}/_doc/{}", enc(id));
            let (status, json) = http_json(
                &client,
                "GET",
                &path,
                "application/json",
                Bytes::new(),
                timeout_ms,
            )
            .await?;
            check_status(status, &json)?;
            // `found: true` means a document came back.
            let found = json
                .get("found")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            Ok(OpResult {
                docs: 0,
                hits: i64::from(found),
            })
        }
        EsOp::Search { index, query } => {
            let path = format!("/{index}/_search");
            let body = match query {
                Some(q) => {
                    Bytes::from(serde_json::to_vec(q).map_err(|e| format!("encoding query: {e}"))?)
                }
                None => Bytes::from_static(b"{\"query\":{\"match_all\":{}}}"),
            };
            let (status, json) =
                http_json(&client, "POST", &path, "application/json", body, timeout_ms).await?;
            check_status(status, &json)?;
            let hits = json
                .get("hits")
                .and_then(|h| h.get("hits"))
                .and_then(serde_json::Value::as_array)
                .map(|a| a.len() as i64)
                .unwrap_or(0);
            Ok(OpResult { docs: 0, hits })
        }
        EsOp::Bulk { index, operations } => {
            let path = match index {
                Some(idx) => format!("/{idx}/_bulk"),
                None => "/_bulk".to_string(),
            };
            let body = bulk_ndjson(operations)?;
            let (status, json) = http_json(
                &client,
                "POST",
                &path,
                "application/x-ndjson",
                body,
                timeout_ms,
            )
            .await?;
            check_status(status, &json)?;
            // The bulk endpoint returns 200 even with per-item failures; surface
            // them as an error when `errors: true` so the request is marked bad.
            if json
                .get("errors")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                return Err(format!(
                    "bulk had item errors: {}",
                    first_bulk_error(&json).unwrap_or_else(|| "unknown".into())
                ));
            }
            Ok(OpResult {
                docs: bulk_success_count(&json),
                hits: 0,
            })
        }
    }
}

/// Percent-encode a path segment (index id) so ids with `/` or spaces are safe.
fn enc(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for &b in seg.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Turn a non-2xx HTTP status into an error, pulling the ES `error.reason` when
/// present for a readable message.
fn check_status(status: u16, json: &serde_json::Value) -> Result<(), String> {
    if (200..300).contains(&status) {
        return Ok(());
    }
    let reason = json
        .get("error")
        .and_then(|e| e.get("reason").or_else(|| e.get("type")))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            json.get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        });
    match reason {
        Some(r) => Err(format!("HTTP {status}: {r}")),
        None => Err(format!("HTTP {status}")),
    }
}

/// First per-item error reason from a `_bulk` response (for diagnostics).
fn first_bulk_error(json: &serde_json::Value) -> Option<String> {
    let items = json.get("items")?.as_array()?;
    for item in items {
        let action = item.as_object()?.values().next()?;
        if let Some(err) = action.get("error") {
            return err
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some(err.to_string()));
        }
    }
    None
}

struct EsProto;

/// Build the JSON `FfiResponse` the host expects. `extras.docs` drives the
/// `elasticsearch_docs` counter; `error` the request-failed rate.
fn response(docs: i64, hits: i64, latency_ms: f64, error: Option<String>) -> FfiResponse {
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
            "docs": docs,
            "hits": hits,
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
        Err(e) => return response(0, 0, 0.0, Some(format!("invalid request JSON: {e}"))),
    };
    let op = match EsOp::from_request(&request) {
        Ok(op) => op,
        Err(e) => return response(0, 0, elapsed_ms(started), Some(e)),
    };
    match runtime().block_on(run_op(&request.url, &op, request.timeout_ms)) {
        Ok(r) => response(r.docs, r.hits, elapsed_ms(started), None),
        Err(e) => response(0, 0, elapsed_ms(started), Some(e)),
    }
}

impl FfiProtocol for EsProto {
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
            "description": "Elasticsearch protocol: index/get/search/bulk over the HTTP/JSON REST API",
            "schemes": ["elasticsearch", "es"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(EsProto, abi_stable::erased_types::TD_Opaque)
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
    fn parse_target_maps_schemes() {
        assert_eq!(
            parse_target("elasticsearch://h:9200").unwrap(),
            ("http://h:9200".to_string(), None)
        );
        assert_eq!(
            parse_target("es://h:9200").unwrap(),
            ("http://h:9200".to_string(), None)
        );
        assert_eq!(
            parse_target("http://h:9200").unwrap(),
            ("http://h:9200".to_string(), None)
        );
        assert_eq!(
            parse_target("https://h").unwrap(),
            ("https://h".to_string(), None)
        );
        assert!(parse_target("mongodb://h").is_err());
        assert!(parse_target("not a url").is_err());
    }

    #[test]
    fn parse_target_extracts_basic_auth() {
        let (base, auth) = parse_target("elasticsearch://elastic:pass@h:9200").unwrap();
        assert_eq!(base, "http://h:9200");
        // base64("elastic:pass")
        assert_eq!(auth.as_deref(), Some("Basic ZWxhc3RpYzpwYXNz"));
    }

    #[test]
    fn parse_target_decodes_percent_userinfo() {
        // password "p@ss" url-encoded as p%40ss
        let (_, auth) = parse_target("es://u:p%40ss@h:9200").unwrap();
        // base64("u:p@ss")
        assert_eq!(auth.as_deref(), Some("Basic dTpwQHNz"));
    }

    #[test]
    fn parse_index_one() {
        let op = EsOp::from_request(&req(
            "es://h:9200",
            serde_json::json!({
                "operation": "index",
                "index": "products",
                "id": "1",
                "document": {"name": "ada"},
            }),
        ))
        .expect("parses");
        match op {
            EsOp::Index { index, id, .. } => {
                assert_eq!(index, "products");
                assert_eq!(id.as_deref(), Some("1"));
            }
            _ => panic!("expected index op"),
        }
    }

    #[test]
    fn parse_index_requires_document() {
        assert!(EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "index", "index": "p"}),
        ))
        .is_err());
    }

    #[test]
    fn parse_get_requires_id() {
        assert!(EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "get", "index": "p"}),
        ))
        .is_err());
        let op = EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "get", "index": "p", "id": "9"}),
        ))
        .expect("parses");
        matches!(op, EsOp::Get { .. });
    }

    #[test]
    fn parse_search_query_optional() {
        let op = EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "search", "index": "p"}),
        ))
        .expect("parses");
        match op {
            EsOp::Search { query, .. } => assert!(query.is_none()),
            _ => panic!("expected search op"),
        }
    }

    #[test]
    fn parse_bulk_requires_nonempty_operations() {
        assert!(EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "bulk", "operations": []}),
        ))
        .is_err());
        let op = EsOp::from_request(&req(
            "es://h",
            serde_json::json!({
                "operation": "bulk",
                "index": "p",
                "operations": [{"index": {}}, {"name": "x"}],
            }),
        ))
        .expect("parses");
        match op {
            EsOp::Bulk { operations, .. } => assert_eq!(operations.len(), 2),
            _ => panic!("expected bulk op"),
        }
    }

    #[test]
    fn unknown_operation_errors() {
        assert!(EsOp::from_request(&req(
            "es://h",
            serde_json::json!({"operation": "delete", "index": "p"}),
        ))
        .is_err());
    }

    #[test]
    fn missing_options_errors() {
        let mut r = req("es://h", serde_json::json!({}));
        r.options = None;
        assert!(EsOp::from_request(&r).is_err());
    }

    #[test]
    fn bulk_ndjson_has_trailing_newline_per_line() {
        let nd = bulk_ndjson(&[
            serde_json::json!({"index": {"_id": "1"}}),
            serde_json::json!({"name": "ada"}),
        ])
        .unwrap();
        let s = String::from_utf8(nd.to_vec()).unwrap();
        assert_eq!(s, "{\"index\":{\"_id\":\"1\"}}\n{\"name\":\"ada\"}\n");
    }

    #[test]
    fn bulk_success_counts_2xx_items() {
        let body = serde_json::json!({
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 200}},
                {"index": {"status": 409}},
            ]
        });
        assert_eq!(bulk_success_count(&body), 2);
    }

    #[test]
    fn check_status_extracts_reason() {
        let body = serde_json::json!({"error": {"reason": "no such index", "type": "x"}});
        let err = check_status(404, &body).unwrap_err();
        assert!(err.contains("404"));
        assert!(err.contains("no such index"));
        assert!(check_status(200, &serde_json::Value::Null).is_ok());
    }

    #[test]
    fn enc_escapes_unsafe_chars() {
        assert_eq!(enc("abc-1_2.3"), "abc-1_2.3");
        assert_eq!(enc("a/b c"), "a%2Fb%20c");
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
            "mongodb://h",
            serde_json::json!({"operation": "search", "index": "p"}),
        ))
        .unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn response_shape_ok() {
        let resp = response(1, 3, 1.5, None);
        assert_eq!(resp.status, 1);
        assert_eq!(resp.extras["docs"], 1);
        assert_eq!(resp.extras["hits"], 3);
        assert_eq!(resp.extras["ok"], true);
        assert!(resp.error.is_none());
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "elasticsearch");
        assert_eq!(v["schemes"][0], "elasticsearch");
        assert_eq!(v["schemes"][1], "es");
    }

    // -----------------------------------------------------------------------
    // Integration: a real Elasticsearch server. Skips unless the env var is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d elasticsearch
    //   LOADR_TEST_ES_URL=http://127.0.0.1:9200 \
    //     cargo test -p loadr-plugin-elasticsearch
    // -----------------------------------------------------------------------

    fn exec(url: &str, plugin: serde_json::Value) -> FfiResponse {
        let json = serde_json::to_string(&req(url, plugin)).unwrap();
        handle(&json)
    }

    #[test]
    fn es_index_search_bulk_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_ES_URL") else {
            eprintln!("skipping: LOADR_TEST_ES_URL not set");
            return;
        };
        let index = format!("loadr-it-{}", std::process::id());

        // Index one document (first call establishes the pooled client).
        let id = format!("doc-{}", std::process::id());
        let ins = exec(
            &url,
            serde_json::json!({
                "operation": "index",
                "index": index,
                "id": id,
                "document": {"name": "it-item", "price": 7.0, "stock": 2},
            }),
        );
        assert!(ins.error.is_none(), "index error: {:?}", ins.error);
        assert_eq!(ins.extras["docs"], 1);

        // Bulk-index a couple more (pool reused).
        let bulk = exec(
            &url,
            serde_json::json!({
                "operation": "bulk",
                "index": index,
                "operations": [
                    {"index": {}}, {"name": "b1", "price": 1.0},
                    {"index": {}}, {"name": "b2", "price": 2.0},
                ],
            }),
        );
        assert!(bulk.error.is_none(), "bulk error: {:?}", bulk.error);
        assert_eq!(bulk.extras["docs"], 2);

        // Get the indexed doc back.
        let got = exec(
            &url,
            serde_json::json!({"operation": "get", "index": index, "id": id}),
        );
        assert!(got.error.is_none(), "get error: {:?}", got.error);
        assert_eq!(got.extras["hits"], 1);

        // Search for the writes. Newly-indexed docs only become searchable
        // after a refresh (ES auto-refreshes ~1s by default), so poll a few
        // times rather than racing the refresh interval.
        let mut hits = 0;
        for _ in 0..20 {
            let search = exec(
                &url,
                serde_json::json!({
                    "operation": "search",
                    "index": index,
                    "query": {"query": {"match_all": {}}, "size": 50},
                }),
            );
            assert!(search.error.is_none(), "search error: {:?}", search.error);
            hits = search.extras["hits"].as_i64().unwrap();
            if hits >= 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        assert!(hits >= 1, "expected search hits after indexing, got {hits}");
    }

    #[test]
    fn es_missing_index_is_reported() {
        let Ok(url) = std::env::var("LOADR_TEST_ES_URL") else {
            eprintln!("skipping: LOADR_TEST_ES_URL not set");
            return;
        };
        let resp = exec(
            &url,
            serde_json::json!({
                "operation": "get",
                "index": "no-such-index-xyz",
                "id": "1",
            }),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn es_connection_failure_is_reported() {
        // Port 1 is never listening; surfaced as an error, not a panic.
        let resp = exec(
            "http://127.0.0.1:1",
            serde_json::json!({"operation": "search", "index": "x"}),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
