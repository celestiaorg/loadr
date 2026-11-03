//! `loadr-plugin-sql-feeder` — a native **service** plugin that turns a live
//! SQL result set into a loadr data feeder.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle: the
//! host calls `start(config_json)` once, before the run, and `stop()` once
//! afterwards (idempotently). This plugin uses `start()` to:
//!
//! 1. connect to a database (`url`, a `postgres://` or `mysql://` URL),
//! 2. run a single `SELECT` (`query`), and
//! 3. write the rows — one JSON object per row, column names as keys — to the
//!    `output` path as a JSON array.
//!
//! A `data:` source of `type: json` pointed at that same `output` path then
//! feeds the rows to VUs during the run (`${data.<source>.<column>}`). The
//! `start()` return value is the absolute-or-relative `output` path that was
//! written, mirroring the "returns a plugin-defined string (e.g. bound addr)"
//! contract of the service ABI.
//!
//! The heavy `sqlx` driver lives only inside this plugin's dynamic library,
//! never in the loadr core binary. The `any` sqlx driver is used so one plugin
//! serves both PostgreSQL and MySQL — the concrete driver is selected from the
//! URL scheme at connect time. `rustls` TLS avoids a system OpenSSL dependency
//! so this cdylib cross-compiles cleanly.
//!
//! # Configuration
//!
//! ```jsonc
//! {
//!   "url":    "postgres://loadr:loadr@db:5432/loadr", // required
//!   "query":  "SELECT id, email FROM users LIMIT 500", // required
//!   "output": "data/users-from-db.json"                // required feeder path
//! }
//! ```

use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use once_cell::sync::OnceCell;
use serde_json::{Map, Value};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{Column, Row};
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "sql-feeder";

/// The single Tokio runtime the plugin uses to drive the async `sqlx` driver.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build sql-feeder plugin tokio runtime")
    })
}

/// Register the `any` driver's concrete backends (postgres + mysql) exactly
/// once per process. `install_default_drivers` panics if called twice, so it is
/// guarded by a `Once`.
fn ensure_drivers() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

/// The service plugin instance. Holds only lifecycle state; the heavy work all
/// happens synchronously inside `start()`.
#[derive(Default)]
struct SqlFeeder {
    started: bool,
    /// The feeder path written by the last successful `start()`.
    output: Option<String>,
}

/// Pull a non-empty string config field, or return a descriptive error.
fn required_str(config: &Value, key: &str) -> Result<String, String> {
    match config.get(key).and_then(Value::as_str) {
        Some(s) if !s.trim().is_empty() => Ok(s.to_string()),
        _ => Err(format!("config requires a non-empty `{key}` string")),
    }
}

/// Decode one column of a row into a JSON value. `sqlx`'s `Any` backend only
/// exposes a small, portable set of Rust types, so we try them in
/// widest-to-narrowest order and fall back to `null` for anything exotic (or a
/// genuine SQL `NULL`, which decodes as `None` for the first `Option<T>` tried).
fn value_at(row: &AnyRow, i: usize) -> Value {
    if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<Option<i32>, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<Option<bool>, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    Value::Null
}

/// Map decoded rows (each a list of `(column, value)` pairs) to feeder rows
/// (JSON objects keyed by column name). Pure and network-free — the unit tests
/// exercise the row-shape contract through here.
fn rows_to_feeder(rows: Vec<Vec<(String, Value)>>) -> Vec<Map<String, Value>> {
    rows.into_iter()
        .map(|cols| {
            let mut obj = Map::with_capacity(cols.len());
            for (name, value) in cols {
                obj.insert(name, value);
            }
            obj
        })
        .collect()
}

/// Connect, run the SELECT, and return the rows as `(column, value)` pairs.
///
/// An unsupported URL scheme (or any other connection problem) surfaces here as
/// an `Err(String)` rather than a panic, so `start()` can report it verbatim.
async fn fetch_rows(url: &str, query: &str) -> Result<Vec<Vec<(String, Value)>>, String> {
    ensure_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
        .map_err(|e| format!("cannot connect to `{url}`: {e}"))?;

    let result = sqlx::query(query).fetch_all(&pool).await;
    pool.close().await;
    let rows = result.map_err(|e| format!("query failed: {e}"))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let columns = row.columns();
        let mut cols = Vec::with_capacity(columns.len());
        for col in columns {
            cols.push((col.name().to_string(), value_at(row, col.ordinal())));
        }
        out.push(cols);
    }
    Ok(out)
}

impl SqlFeeder {
    /// The whole `start()` flow, factored to return a plain `Result` so the
    /// FFI shim is a thin wrapper. On success returns the written feeder path.
    fn run(&mut self, config_json: &str) -> Result<String, String> {
        let config: Value =
            serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;
        let url = required_str(&config, "url")?;
        let query = required_str(&config, "query")?;
        let output = required_str(&config, "output")?;

        let rows = runtime().block_on(fetch_rows(&url, &query))?;
        let feeder = rows_to_feeder(rows);
        let json = serde_json::to_string_pretty(&feeder)
            .map_err(|e| format!("cannot encode feeder rows: {e}"))?;
        std::fs::write(&output, json)
            .map_err(|e| format!("cannot write feeder file `{output}`: {e}"))?;

        self.output = Some(output.clone());
        self.started = true;
        Ok(output)
    }
}

impl FfiService for SqlFeeder {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        match self.run(config_json.as_str()) {
            Ok(output) => ROk(RString::from(output)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // Idempotent: the feeder file is left in place for the run; we simply
        // clear our lifecycle state, so a second `stop()` is a no-op.
        self.started = false;
        self.output = None;
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "Runs a SELECT once at startup and writes the rows as a JSON feeder file",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(SqlFeeder::default(), abi_stable::erased_types::TD_Opaque)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_map_to_feeder_objects_with_column_keys() {
        let rows = vec![
            vec![
                ("id".to_string(), Value::from(1_i64)),
                ("email".to_string(), Value::from("a@example.com")),
                ("active".to_string(), Value::from(true)),
            ],
            vec![
                ("id".to_string(), Value::from(2_i64)),
                ("email".to_string(), Value::from("b@example.com")),
                ("active".to_string(), Value::Null),
            ],
        ];

        let feeder = rows_to_feeder(rows);
        assert_eq!(feeder.len(), 2);
        assert_eq!(feeder[0].get("id"), Some(&Value::from(1_i64)));
        assert_eq!(feeder[0].get("email"), Some(&Value::from("a@example.com")));
        assert_eq!(feeder[0].get("active"), Some(&Value::Bool(true)));
        assert_eq!(feeder[1].get("active"), Some(&Value::Null));

        // Serialises to the JSON-array-of-objects shape a `type: json` feeder reads.
        let json = serde_json::to_string(&feeder).expect("serialise feeder");
        let parsed: Value = serde_json::from_str(&json).expect("reparse feeder");
        assert!(parsed.is_array());
        assert_eq!(parsed[0]["email"], Value::from("a@example.com"));
    }

    #[test]
    fn empty_result_maps_to_empty_feeder() {
        let feeder = rows_to_feeder(Vec::new());
        assert!(feeder.is_empty());
        // An empty result still serialises to a valid (empty) JSON array.
        assert_eq!(serde_json::to_string(&feeder).unwrap(), "[]");
    }

    #[test]
    fn required_str_rejects_missing_and_blank() {
        let cfg = serde_json::json!({ "url": "postgres://x", "blank": "   " });
        assert!(required_str(&cfg, "url").is_ok());
        assert!(required_str(&cfg, "missing").is_err());
        assert!(required_str(&cfg, "blank").is_err());
    }

    #[test]
    fn start_surfaces_connection_failure_as_error() {
        // An unsupported URL scheme is rejected at driver resolution, before any
        // socket is opened — so this exercises the failure path without touching
        // the network.
        let out = std::env::temp_dir().join("loadr-sql-feeder-should-not-exist.json");
        let config = serde_json::json!({
            "url": "bogusdb://localhost/does-not-exist",
            "query": "SELECT 1",
            "output": out.to_string_lossy(),
        })
        .to_string();

        let mut feeder = SqlFeeder::default();
        match feeder.start(RString::from(config)) {
            RResult::RErr(e) => assert!(
                e.as_str().contains("cannot connect"),
                "unexpected error text: {e}"
            ),
            RResult::ROk(v) => panic!("expected a connection error, got Ok({v})"),
        }
        assert!(
            !feeder.started,
            "failed start must not mark the service started"
        );
        // Nothing should have been written on the failure path.
        assert!(!out.exists());
    }

    #[test]
    fn start_rejects_invalid_config_without_network() {
        let mut feeder = SqlFeeder::default();
        // Missing `url` — fails validation before any connection attempt.
        let cfg = serde_json::json!({ "query": "SELECT 1", "output": "x.json" }).to_string();
        assert!(matches!(feeder.start(RString::from(cfg)), RResult::RErr(_)));
        // Malformed JSON.
        assert!(matches!(
            feeder.start(RString::from("not json")),
            RResult::RErr(_)
        ));
    }

    #[test]
    fn stop_is_idempotent() {
        let mut feeder = SqlFeeder {
            started: true,
            output: Some("data/out.json".to_string()),
        };
        feeder.stop();
        assert!(!feeder.started);
        assert!(feeder.output.is_none());
        // A second stop must not panic and leaves state untouched.
        feeder.stop();
        assert!(!feeder.started);
    }

    #[test]
    fn plugin_info_declares_service_kind() {
        let info: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(info["kind"], "service");
        assert_eq!(info["name"], NAME);
    }
}
