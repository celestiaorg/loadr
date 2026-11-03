//! `loadr-plugin-mongo` — a native protocol plugin that adds MongoDB as a
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
//!   `block_on`s the async `mongodb` driver.
//! * An internal connection pool keyed by the Mongo connection URI
//!   (`OnceCell<Mutex<HashMap<String, mongodb::Client>>>`), so a `Client`
//!   (itself an internally-pooled, cheaply-cloneable handle) is created once
//!   per distinct URI and reused across every call and VU.
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
//!   "operation": "insert" | "find" | "update" | "delete" | "aggregate" | "command",
//!   "database":  "mydb",            // optional; else taken from the URI path
//!   "collection": "users",          // required except for `command`
//!   "document":  { ... },           // insert: one doc, or use `documents`
//!   "documents": [ { ... } ],       // insert: many docs
//!   "filter":    { ... },           // find/update/delete
//!   "update":    { "$set": { ... } },// update
//!   "pipeline":  [ { ... } ],       // aggregate
//!   "command":   { ... },           // command (raw db command)
//!   "limit":     100,               // find (optional)
//!   "multi":     true               // update/delete many (default false)
//! }
//! ```
//!
//! The response is JSON `{ ok, latency_ms, docs, error }` where `docs` is the
//! count of documents inserted / matched / modified / returned / affected. The
//! host turns this into `mongo_reqs` / `mongo_req_duration` / `mongo_docs`
//! metrics (see the `extras.docs` and `error` fields it reads back).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use mongodb::bson::{self, Document};
use mongodb::options::ClientOptions;
use mongodb::Client;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "mongo";

/// The single Tokio runtime the plugin uses to drive the async driver.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build mongo plugin tokio runtime")
    })
}

/// Connection pool keyed by Mongo URI. A `mongodb::Client` is itself an
/// internally pooled, cheap-to-clone handle, so one per URI is the right model.
fn clients() -> &'static Mutex<HashMap<String, Client>> {
    static CLIENTS: OnceCell<Mutex<HashMap<String, Client>>> = OnceCell::new();
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Get (or lazily create + cache) the client for `uri`.
async fn client_for(uri: &str) -> mongodb::error::Result<Client> {
    if let Some(c) = clients().lock().expect("clients lock").get(uri).cloned() {
        return Ok(c);
    }
    let opts = ClientOptions::parse(uri).await?;
    let default_db = opts.default_database.clone();
    let client = Client::with_options(opts)?;
    let _ = default_db; // parsed for validation; db is resolved per-request.
    let mut guard = clients().lock().expect("clients lock");
    // Another thread may have inserted while we awaited; keep the first.
    Ok(guard.entry(uri.to_string()).or_insert(client).clone())
}

/// The default database from a connection URI's path (`mongodb://h/dbname`).
fn db_from_uri(uri: &str) -> Option<String> {
    let after = uri.split("://").nth(1)?;
    let path = after.split('/').nth(1)?;
    let db = path.split('?').next().unwrap_or("");
    if db.is_empty() {
        None
    } else {
        Some(db.to_string())
    }
}

/// Parsed plugin options for one request.
struct MongoOp {
    operation: String,
    database: Option<String>,
    collection: Option<String>,
    document: Option<Document>,
    documents: Option<Vec<Document>>,
    filter: Document,
    update: Option<Document>,
    pipeline: Vec<Document>,
    command: Option<Document>,
    limit: Option<i64>,
    multi: bool,
}

fn to_doc(v: &serde_json::Value) -> Result<Document, String> {
    if !v.is_object() {
        return Err("expected a JSON object".to_string());
    }
    bson::serialize_to_document(v).map_err(|e| e.to_string())
}

impl MongoOp {
    fn from_request(req: &FfiRequest) -> Result<MongoOp, String> {
        let opts = req
            .options
            .as_ref()
            .ok_or_else(|| "missing `plugin:` options for mongo request".to_string())?;
        let operation = opts
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "mongo request requires `operation`".to_string())?
            .to_ascii_lowercase();

        let document = match opts.get("document") {
            Some(v) if !v.is_null() => Some(to_doc(v)?),
            _ => None,
        };
        let documents = match opts.get("documents") {
            Some(serde_json::Value::Array(arr)) => {
                Some(arr.iter().map(to_doc).collect::<Result<Vec<_>, _>>()?)
            }
            _ => None,
        };
        let filter = match opts.get("filter") {
            Some(v) if !v.is_null() => to_doc(v)?,
            _ => Document::new(),
        };
        let update = match opts.get("update") {
            Some(v) if !v.is_null() => Some(to_doc(v)?),
            _ => None,
        };
        let pipeline = match opts.get("pipeline") {
            Some(serde_json::Value::Array(arr)) => {
                arr.iter().map(to_doc).collect::<Result<Vec<_>, _>>()?
            }
            _ => Vec::new(),
        };
        let command = match opts.get("command") {
            Some(v) if !v.is_null() => Some(to_doc(v)?),
            _ => None,
        };

        Ok(MongoOp {
            operation,
            database: opts
                .get("database")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            collection: opts
                .get("collection")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            document,
            documents,
            filter,
            update,
            pipeline,
            command,
            limit: opts.get("limit").and_then(|v| v.as_i64()),
            multi: opts.get("multi").and_then(|v| v.as_bool()).unwrap_or(false),
        })
    }

    fn collection_name(&self) -> Result<&str, String> {
        self.collection
            .as_deref()
            .ok_or_else(|| "mongo request requires `collection`".to_string())
    }
}

/// Outcome of one operation: docs affected/returned and the raw result.
struct OpResult {
    docs: i64,
}

async fn run_op(uri: &str, op: &MongoOp) -> Result<OpResult, String> {
    let client = client_for(uri).await.map_err(|e| e.to_string())?;
    let db_name = op
        .database
        .clone()
        .or_else(|| db_from_uri(uri))
        .ok_or_else(|| {
            "no database: set `database` in the request or include it in the URI path".to_string()
        })?;
    let db = client.database(&db_name);

    match op.operation.as_str() {
        "insert" => {
            let coll = db.collection::<Document>(op.collection_name()?);
            if let Some(docs) = &op.documents {
                if docs.is_empty() {
                    return Ok(OpResult { docs: 0 });
                }
                let res = coll.insert_many(docs).await.map_err(|e| e.to_string())?;
                Ok(OpResult {
                    docs: res.inserted_ids.len() as i64,
                })
            } else if let Some(doc) = &op.document {
                coll.insert_one(doc).await.map_err(|e| e.to_string())?;
                Ok(OpResult { docs: 1 })
            } else {
                Err("insert requires `document` or `documents`".to_string())
            }
        }
        "find" => {
            let coll = db.collection::<Document>(op.collection_name()?);
            let mut find = coll.find(op.filter.clone());
            if let Some(limit) = op.limit {
                find = find.limit(limit);
            }
            let mut cursor = find.await.map_err(|e| e.to_string())?;
            let mut count = 0i64;
            use futures_util::TryStreamExt;
            while cursor
                .try_next()
                .await
                .map_err(|e| e.to_string())?
                .is_some()
            {
                count += 1;
            }
            Ok(OpResult { docs: count })
        }
        "update" => {
            let coll = db.collection::<Document>(op.collection_name()?);
            let update = op
                .update
                .clone()
                .ok_or_else(|| "update requires `update`".to_string())?;
            let res = if op.multi {
                coll.update_many(op.filter.clone(), update).await
            } else {
                coll.update_one(op.filter.clone(), update).await
            }
            .map_err(|e| e.to_string())?;
            // Modified + upserted gives the affected-doc count.
            let upserted = i64::from(res.upserted_id.is_some());
            Ok(OpResult {
                docs: res.modified_count as i64 + upserted,
            })
        }
        "delete" => {
            let coll = db.collection::<Document>(op.collection_name()?);
            let res = if op.multi {
                coll.delete_many(op.filter.clone()).await
            } else {
                coll.delete_one(op.filter.clone()).await
            }
            .map_err(|e| e.to_string())?;
            Ok(OpResult {
                docs: res.deleted_count as i64,
            })
        }
        "aggregate" => {
            let coll = db.collection::<Document>(op.collection_name()?);
            let mut cursor = coll
                .aggregate(op.pipeline.clone())
                .await
                .map_err(|e| e.to_string())?;
            let mut count = 0i64;
            use futures_util::TryStreamExt;
            while cursor
                .try_next()
                .await
                .map_err(|e| e.to_string())?
                .is_some()
            {
                count += 1;
            }
            Ok(OpResult { docs: count })
        }
        "command" => {
            let command = op
                .command
                .clone()
                .ok_or_else(|| "command requires `command`".to_string())?;
            db.run_command(command).await.map_err(|e| e.to_string())?;
            Ok(OpResult { docs: 0 })
        }
        other => Err(format!("unknown mongo operation `{other}`")),
    }
}

struct MongoProto;

/// Build the JSON `FfiResponse` the host expects. `extras.docs` and `error`
/// drive the `mongo_docs` counter and request-failed rate respectively.
fn response(docs: i64, latency_ms: f64, error: Option<String>) -> FfiResponse {
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
        }),
    }
}

fn handle(request_json: &str) -> FfiResponse {
    let started = Instant::now();
    let request: FfiRequest = match serde_json::from_str(request_json) {
        Ok(r) => r,
        Err(e) => return response(0, 0.0, Some(format!("invalid request JSON: {e}"))),
    };
    let op = match MongoOp::from_request(&request) {
        Ok(op) => op,
        Err(e) => return response(0, elapsed_ms(started), Some(e)),
    };
    match runtime().block_on(run_op(&request.url, &op)) {
        Ok(result) => response(result.docs, elapsed_ms(started), None),
        Err(e) => response(0, elapsed_ms(started), Some(e)),
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

impl FfiProtocol for MongoProto {
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
            "description": "MongoDB protocol: insert/find/update/delete/aggregate/command",
            "schemes": ["mongodb", "mongo"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(MongoProto, abi_stable::erased_types::TD_Opaque)
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
            timeout_ms: 1000,
            options: Some(plugin),
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn db_from_uri_extracts_path() {
        assert_eq!(db_from_uri("mongodb://h:27017/shop"), Some("shop".into()));
        assert_eq!(
            db_from_uri("mongodb://h/shop?retryWrites=true"),
            Some("shop".into())
        );
        assert_eq!(db_from_uri("mongodb://h:27017"), None);
        assert_eq!(db_from_uri("mongodb://h:27017/"), None);
    }

    #[test]
    fn parse_insert_one() {
        let r = req(
            "mongodb://h/db",
            serde_json::json!({
                "operation": "insert",
                "collection": "users",
                "document": {"name": "ada", "n": 1},
            }),
        );
        let op = MongoOp::from_request(&r).expect("parses");
        assert_eq!(op.operation, "insert");
        assert_eq!(op.collection_name().unwrap(), "users");
        assert!(op.document.is_some());
        assert!(op.documents.is_none());
    }

    #[test]
    fn parse_find_with_filter_and_limit() {
        let r = req(
            "mongodb://h/db",
            serde_json::json!({
                "operation": "find",
                "collection": "users",
                "filter": {"active": true},
                "limit": 10,
            }),
        );
        let op = MongoOp::from_request(&r).expect("parses");
        assert_eq!(op.operation, "find");
        assert_eq!(op.limit, Some(10));
        assert!(op.filter.get_bool("active").unwrap());
    }

    #[test]
    fn parse_aggregate_pipeline() {
        let r = req(
            "mongodb://h/db",
            serde_json::json!({
                "operation": "aggregate",
                "collection": "orders",
                "pipeline": [{"$group": {"_id": "$customer", "total": {"$sum": "$amount"}}}],
            }),
        );
        let op = MongoOp::from_request(&r).expect("parses");
        assert_eq!(op.pipeline.len(), 1);
    }

    #[test]
    fn missing_operation_errors() {
        let r = req("mongodb://h/db", serde_json::json!({"collection": "x"}));
        assert!(MongoOp::from_request(&r).is_err());
    }

    #[test]
    fn missing_options_errors() {
        let r = FfiRequest {
            name: "t".into(),
            method: "POST".into(),
            url: "mongodb://h/db".into(),
            headers: Vec::new(),
            body_b64: String::new(),
            timeout_ms: 1000,
            options: None,
            config: serde_json::Value::Null,
        };
        assert!(MongoOp::from_request(&r).is_err());
    }

    #[test]
    fn collection_required_for_find() {
        let r = req("mongodb://h/db", serde_json::json!({"operation": "find"}));
        let op = MongoOp::from_request(&r).expect("parses");
        assert!(op.collection_name().is_err());
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn response_shape_ok() {
        let resp = response(3, 1.5, None);
        assert_eq!(resp.status, 1);
        assert_eq!(resp.extras["docs"], 3);
        assert_eq!(resp.extras["ok"], true);
        assert!(resp.error.is_none());
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["schemes"][0], "mongodb");
        assert_eq!(v["schemes"][1], "mongo");
    }

    // -----------------------------------------------------------------------
    // Integration: real MongoDB. Skips unless LOADR_TEST_MONGO_URL is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d mongo
    //   LOADR_TEST_MONGO_URL=mongodb://loadr:loadr@127.0.0.1:27017/loadr \
    //     cargo test -p loadr-plugin-mongo
    // -----------------------------------------------------------------------

    fn exec(url: &str, plugin: serde_json::Value) -> FfiResponse {
        let json = serde_json::to_string(&req(url, plugin)).unwrap();
        handle(&json)
    }

    #[test]
    fn mongo_insert_find_aggregate_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_MONGO_URL") else {
            eprintln!("skipping: LOADR_TEST_MONGO_URL not set");
            return;
        };

        // Insert a uniquely-tagged document, first call establishes the client.
        let tag = format!("it-{}", std::process::id());
        let ins = exec(
            &url,
            serde_json::json!({
                "operation": "insert",
                "collection": "products",
                "document": {"name": "it-item", "price": 7.0, "stock": 2, "tags": [tag]},
            }),
        );
        assert!(ins.error.is_none(), "insert error: {:?}", ins.error);
        assert_eq!(ins.status, 1);
        assert_eq!(ins.extras["docs"], 1);

        // Find it back (pool reused).
        let find = exec(
            &url,
            serde_json::json!({
                "operation": "find",
                "collection": "products",
                "filter": {"tags": tag},
            }),
        );
        assert!(find.error.is_none(), "find error: {:?}", find.error);
        assert_eq!(find.extras["docs"], 1);

        // Aggregate over the seeded data returns at least one group.
        let agg = exec(
            &url,
            serde_json::json!({
                "operation": "aggregate",
                "collection": "products",
                "pipeline": [
                    {"$group": {"_id": null, "n": {"$sum": 1}}},
                ],
            }),
        );
        assert!(agg.error.is_none(), "aggregate error: {:?}", agg.error);
        assert_eq!(agg.extras["docs"], 1);

        // Update the inserted doc, then delete it (clean up).
        let upd = exec(
            &url,
            serde_json::json!({
                "operation": "update",
                "collection": "products",
                "filter": {"tags": tag},
                "update": {"$set": {"stock": 9}},
            }),
        );
        assert!(upd.error.is_none(), "update error: {:?}", upd.error);
        assert_eq!(upd.extras["docs"], 1, "one doc modified");

        let del = exec(
            &url,
            serde_json::json!({
                "operation": "delete",
                "collection": "products",
                "filter": {"tags": tag},
                "multi": true,
            }),
        );
        assert!(del.error.is_none(), "delete error: {:?}", del.error);
        assert_eq!(del.extras["docs"], 1, "one doc deleted");
    }

    #[test]
    fn mongo_command_ping() {
        let Ok(url) = std::env::var("LOADR_TEST_MONGO_URL") else {
            eprintln!("skipping: LOADR_TEST_MONGO_URL not set");
            return;
        };
        let resp = exec(
            &url,
            serde_json::json!({"operation": "command", "command": {"ping": 1}}),
        );
        assert!(resp.error.is_none(), "ping error: {:?}", resp.error);
        assert_eq!(resp.status, 1);
    }

    #[test]
    fn mongo_connection_failure_is_reported() {
        // Port 1 is never listening; the driver reports an error, not a panic.
        let resp = exec(
            "mongodb://127.0.0.1:1/db?serverSelectionTimeoutMS=500",
            serde_json::json!({"operation": "find", "collection": "x"}),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
