//! `loadr-plugin-postgres` — a native protocol plugin that adds PostgreSQL as a
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
//!   `block_on`s the async `sqlx` driver.
//! * An internal connection pool keyed by the database connection URI
//!   (`OnceCell<Mutex<HashMap<String, sqlx::PgPool>>>`), so a `Pool` (itself a
//!   set of pooled, cheaply-cloneable connections) is created once per distinct
//!   URI and reused across every call and VU.
//!
//! The heavy `sqlx` driver lives only inside this plugin's dynamic library,
//! never in the loadr core binary. This crate enables ONLY the `postgres`
//! `sqlx` feature, so it never pulls in `sqlx-mysql` (and thus not its
//! transitive `rsa` dependency / RUSTSEC-2023-0071) — a PostgreSQL-only build
//! is fully advisory-clean. MySQL lives in the separate `loadr-plugin-mysql`.
//!
//! # Request / response contract
//!
//! The host hands the plugin a JSON [`loadr_plugin_api::FfiRequest`]. We read
//! the connection URI from `url` and the query from `options.plugin` (populated
//! from the YAML request's `sql:` block, with `${...}` already interpolated by
//! the host):
//!
//! ```jsonc
//! {
//!   "query":  "SELECT * FROM t WHERE id = $1", // required (or via request body)
//!   "params": ["42", 7, null]                   // optional positional binds
//! }
//! ```
//!
//! The response is JSON `{ ok, latency_ms, rows, error }` where `rows` is the
//! number of rows returned (SELECT/…) or affected (INSERT/UPDATE/DELETE). The
//! host turns this into `postgres_reqs` / `postgres_req_duration` /
//! `postgres_rows` metrics (it reads the `extras.rows` field back).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use once_cell::sync::OnceCell;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "postgres";

/// The single Tokio runtime the plugin uses to drive the async driver.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build postgres plugin tokio runtime")
    })
}

/// Connection pools keyed by URI. A `sqlx::Pool` is a cheap-to-clone handle to
/// a shared set of pooled connections, so one per distinct URI is the right
/// model — established on first use and reused across every call and VU.
fn pg_pools() -> &'static Mutex<HashMap<String, PgPool>> {
    static POOLS: OnceCell<Mutex<HashMap<String, PgPool>>> = OnceCell::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Validate the URL scheme — only `postgres`/`postgresql` are served here.
fn check_scheme(raw: &str) -> Result<(), String> {
    let url = Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    match url.scheme() {
        "postgres" | "postgresql" => Ok(()),
        other => Err(format!("postgres plugin cannot handle scheme `{other}`")),
    }
}

/// The resolved query for one request.
struct SqlQuery {
    query: String,
    params: Vec<String>,
}

impl SqlQuery {
    /// Resolve the query + params from plugin options, falling back to the
    /// request body as the query text.
    fn from_request(req: &FfiRequest) -> Result<SqlQuery, String> {
        if let Some(plugin) = &req.options {
            if let Some(query) = plugin.get("query").and_then(serde_json::Value::as_str) {
                let params = plugin
                    .get("params")
                    .and_then(serde_json::Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Null => String::new(),
                                other => other.to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                if query.trim().is_empty() {
                    return Err("sql query is empty".to_string());
                }
                return Ok(SqlQuery {
                    query: query.to_string(),
                    params,
                });
            }
        }
        let body = base64_decode(&req.body_b64)?;
        let query = String::from_utf8_lossy(&body).trim().to_string();
        if query.is_empty() {
            return Err(
                "no sql query provided (set the `sql.query` option or a request body)".to_string(),
            );
        }
        Ok(SqlQuery {
            query,
            params: Vec::new(),
        })
    }
}

/// Minimal base64 decode of the request body (standard alphabet, padded). The
/// host base64-encodes the request body with the standard alphabet; decode it
/// here without pulling in another crate.
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
    let mut chunk = bytes.chunks(4);
    for c in &mut chunk {
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

/// True when a statement is expected to return a result set (SELECT / WITH /
/// SHOW / VALUES / EXPLAIN / postgres `RETURNING`). Used to pick between the
/// row-returning and the affected-rows execution paths.
fn returns_rows(query: &str) -> bool {
    let head: String = query
        .trim_start()
        .chars()
        .take_while(|c| c.is_alphabetic())
        .flat_map(char::to_lowercase)
        .collect();
    matches!(
        head.as_str(),
        "select" | "with" | "show" | "values" | "explain" | "table" | "describe" | "desc"
    ) || query.to_lowercase().contains(" returning ")
}

/// Bind a single string parameter with an inferred type so that comparisons
/// against numeric columns work (PostgreSQL in particular will not implicitly
/// cast `text` to `numeric`). Integers bind as `i64`, decimals as `f64`,
/// everything else as text.
fn bind_pg<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    p: &'q str,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    if let Ok(i) = p.parse::<i64>() {
        query.bind(i)
    } else if let Ok(f) = p.parse::<f64>() {
        query.bind(f)
    } else {
        query.bind(p)
    }
}

/// Get (or lazily create + cache) the postgres pool for `uri`.
async fn pg_pool_for(uri: &str) -> Result<PgPool, sqlx::Error> {
    if let Some(p) = pg_pools().lock().expect("pg pool lock").get(uri).cloned() {
        return Ok(p);
    }
    let pool = PgPoolOptions::new().max_connections(8).connect(uri).await?;
    let mut guard = pg_pools().lock().expect("pg pool lock");
    Ok(guard.entry(uri.to_string()).or_insert(pool).clone())
}

/// Run the query against PostgreSQL. Returns the number of rows returned by a
/// row-producing statement, or rows affected by a DML statement.
async fn run_query(uri: &str, q: &SqlQuery) -> Result<u64, String> {
    let pool = pg_pool_for(uri)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let mut query = sqlx::query(&q.query);
    for p in &q.params {
        query = bind_pg(query, p);
    }
    if returns_rows(&q.query) {
        Ok(query
            .fetch_all(&pool)
            .await
            .map_err(|e| e.to_string())?
            .len() as u64)
    } else {
        Ok(query
            .execute(&pool)
            .await
            .map_err(|e| e.to_string())?
            .rows_affected())
    }
}

struct PostgresProto;

/// Build the JSON `FfiResponse` the host expects. `extras.rows` drives the
/// `postgres_rows` counter and `error` the request-failed rate.
fn response(rows: u64, latency_ms: f64, error: Option<String>) -> FfiResponse {
    let ok = error.is_none();
    FfiResponse {
        status: i64::from(ok),
        status_text: if ok { "OK" } else { "ERROR" }.to_string(),
        headers: Vec::new(),
        // The body is the row count as text, so body-based checks/extraction
        // still work, matching the old built-in handler.
        body_b64: base64_encode(rows.to_string().as_bytes()),
        duration_ms: latency_ms,
        error,
        extras: serde_json::json!({
            "ok": ok,
            "latency_ms": latency_ms,
            "rows": rows,
            "backend": "postgres",
        }),
    }
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
    let q = match SqlQuery::from_request(&request) {
        Ok(q) => q,
        Err(e) => return response(0, elapsed_ms(started), Some(e)),
    };
    let exec = async {
        if request.timeout_ms == 0 {
            run_query(&request.url, &q).await
        } else {
            tokio::time::timeout(
                std::time::Duration::from_millis(request.timeout_ms),
                run_query(&request.url, &q),
            )
            .await
            .unwrap_or_else(|_| Err(format!("query timed out after {}ms", request.timeout_ms)))
        }
    };
    match runtime().block_on(exec) {
        Ok(rows) => response(rows, elapsed_ms(started), None),
        Err(e) => response(0, elapsed_ms(started), Some(e)),
    }
}

impl FfiProtocol for PostgresProto {
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
            "description": "PostgreSQL protocol: queries via sqlx",
            "schemes": ["postgres", "postgresql"],
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(PostgresProto, abi_stable::erased_types::TD_Opaque)
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
            name: "q".into(),
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
    fn accepts_postgres_schemes() {
        assert!(check_scheme("postgres://u:p@h:5432/db").is_ok());
        assert!(check_scheme("postgresql://h/db").is_ok());
        assert!(check_scheme("mysql://h:3306/db").is_err());
        assert!(check_scheme("http://h/db").is_err());
        assert!(check_scheme("not a url").is_err());
    }

    #[test]
    fn query_from_plugin_options() {
        let r = req(
            "postgres://h/db",
            Some(serde_json::json!({
                "query": "SELECT * FROM t WHERE id = $1",
                "params": ["42", 7, null],
            })),
        );
        let q = SqlQuery::from_request(&r).unwrap();
        assert_eq!(q.query, "SELECT * FROM t WHERE id = $1");
        assert_eq!(
            q.params,
            vec!["42".to_string(), "7".to_string(), String::new()]
        );
    }

    #[test]
    fn query_from_body_fallback() {
        let mut r = req("postgres://h/db", None);
        r.body_b64 = base64_encode(b"  SELECT 1  ");
        let q = SqlQuery::from_request(&r).unwrap();
        assert_eq!(q.query, "SELECT 1");
        assert!(q.params.is_empty());
    }

    #[test]
    fn empty_query_rejected() {
        let r = req("postgres://h/db", None);
        assert!(SqlQuery::from_request(&r).is_err());

        let r = req(
            "postgres://h/db",
            Some(serde_json::json!({ "query": "   " })),
        );
        assert!(SqlQuery::from_request(&r).is_err());
    }

    #[test]
    fn returns_rows_classifies_statements() {
        assert!(returns_rows("SELECT 1"));
        assert!(returns_rows("  with x as (...) select *"));
        assert!(returns_rows("INSERT INTO t VALUES (1) RETURNING id"));
        assert!(!returns_rows("INSERT INTO t VALUES (1)"));
        assert!(!returns_rows("UPDATE t SET a = 1"));
    }

    #[test]
    fn base64_roundtrips() {
        for s in ["", "a", "ab", "abc", "abcd", "SELECT 42"] {
            let enc = base64_encode(s.as_bytes());
            let dec = base64_decode(&enc).unwrap();
            assert_eq!(dec, s.as_bytes(), "roundtrip {s:?}");
        }
    }

    #[test]
    fn handle_invalid_json_is_error_response() {
        let resp = handle("not json");
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn handle_bad_scheme_is_error_response() {
        let json = serde_json::to_string(&req("mysql://h/db", None)).unwrap();
        let resp = handle(&json);
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn response_shape_ok() {
        let resp = response(3, 1.5, None);
        assert_eq!(resp.status, 1);
        assert_eq!(resp.extras["rows"], 3);
        assert_eq!(resp.extras["backend"], "postgres");
        assert_eq!(resp.extras["ok"], true);
        assert!(resp.error.is_none());
    }

    #[test]
    fn info_declares_schemes() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "protocol");
        assert_eq!(v["name"], "postgres");
        assert_eq!(v["schemes"][0], "postgres");
        assert_eq!(v["schemes"][1], "postgresql");
    }

    // -----------------------------------------------------------------------
    // Integration: a real PostgreSQL server. Skips unless the env var is set.
    //   docker compose -f examples/harness/docker-compose.yml up -d postgres
    //   LOADR_TEST_POSTGRES_URL=postgres://loadr:loadr@127.0.0.1:5432/loadr \
    //     cargo test -p loadr-plugin-postgres
    // -----------------------------------------------------------------------

    fn exec(url: &str, plugin: serde_json::Value) -> FfiResponse {
        let json = serde_json::to_string(&req(url, Some(plugin))).unwrap();
        handle(&json)
    }

    #[test]
    fn postgres_select_insert_roundtrip() {
        let Ok(url) = std::env::var("LOADR_TEST_POSTGRES_URL") else {
            eprintln!("skipping: LOADR_TEST_POSTGRES_URL not set");
            return;
        };

        // First call establishes the pool.
        let sel = exec(
            &url,
            serde_json::json!({
                "query": "SELECT id, name, price FROM products WHERE price < $1 ORDER BY price",
                "params": ["1000"],
            }),
        );
        assert!(sel.error.is_none(), "select error: {:?}", sel.error);
        assert_eq!(sel.status, 1);
        assert!(sel.extras["rows"].as_u64().is_some());

        // Insert one row (pool reused), reports rows affected.
        let ins = exec(
            &url,
            serde_json::json!({
                "query": "INSERT INTO products (name, price, stock) VALUES ($1, $2, $3)",
                "params": [format!("it-{}", std::process::id()), "1.23", "5"],
            }),
        );
        assert!(ins.error.is_none(), "insert error: {:?}", ins.error);
        assert_eq!(ins.extras["rows"], 1);
    }

    #[test]
    fn postgres_query_error_is_reported() {
        let Ok(url) = std::env::var("LOADR_TEST_POSTGRES_URL") else {
            eprintln!("skipping: LOADR_TEST_POSTGRES_URL not set");
            return;
        };
        let resp = exec(
            &url,
            serde_json::json!({ "query": "SELECT * FROM no_such_table" }),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }

    #[test]
    fn connection_failure_is_reported() {
        // Port 1 is never listening; the driver reports an error, not a panic.
        let resp = exec(
            "postgres://u:p@127.0.0.1:1/db?connect_timeout=1",
            serde_json::json!({ "query": "SELECT 1" }),
        );
        assert_eq!(resp.status, 0);
        assert!(resp.error.is_some());
    }
}
