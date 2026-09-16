//! # loadr-plugin-faker-gen
//!
//! A native **service** plugin that starts a local, seeded fake-data generator
//! and materialises its rows as a CSV file. A plain [CSV feeder] then pulls
//! rows from that file exactly like any other CSV data source — so VUs get
//! parameterised, realistic-looking data without shipping a fixture file.
//!
//! ```text
//! plugins:
//!   - name: faker-gen
//!     config:
//!       path: data/generated-users.csv   # feeder reads this
//!       rows: 5000
//!       seed: 42                          # omit for fresh random data each run
//!       columns:
//!         - { name: id,       type: uuid }
//!         - { name: username, type: username }
//!         - { name: email,    type: email }
//!         - { name: age,      type: int, min: 18, max: 80 }
//! data:
//!   users:
//!     type: csv
//!     path: data/generated-users.csv
//! ```
//!
//! Determinism: with an explicit `seed` the produced sequence is byte-for-byte
//! reproducible; without one a fresh seed is drawn per run. Generation is pure
//! Rust (small built-in word lists + a seeded `SmallRng`) — no network, no C
//! dependencies.
//!
//! [CSV feeder]: https://loadr.io

use std::fs;
use std::io::Write as _;
use std::path::PathBuf;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use rand::rngs::SmallRng;
use rand::{RngExt as _, SeedableRng as _};
use serde_json::Value;

const NAME: &str = "faker-gen";
const DEFAULT_ROWS: usize = 1000;

// ---------------------------------------------------------------------------
// Built-in word lists. Small and self-contained so the plugin needs no data
// files and cross-compiles anywhere.
// ---------------------------------------------------------------------------

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Mallory",
    "Niaj", "Olivia", "Peggy", "Rupert", "Sybil", "Trent", "Uma", "Victor", "Wendy",
];

const LAST_NAMES: &[&str] = &[
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
];

const CITIES: &[&str] = &[
    "London", "Paris", "Berlin", "Madrid", "Rome", "Lisbon", "Vienna", "Prague", "Dublin", "Oslo",
    "Tokyo", "Osaka", "Sydney", "Toronto", "Chicago", "Austin", "Denver", "Seattle",
];

const COUNTRIES: &[&str] = &[
    "GB", "FR", "DE", "ES", "IT", "PT", "AT", "CZ", "IE", "NO", "JP", "AU", "CA", "US",
];

const WORDS: &[&str] = &[
    "widget", "gadget", "sprocket", "gizmo", "cog", "lever", "piston", "valve", "rotor", "flange",
    "bracket", "spindle", "coupling", "bearing", "gasket", "washer", "bolt", "nut",
];

const DOMAINS: &[&str] = &[
    "example.com",
    "mail.test",
    "acme.io",
    "demo.dev",
    "sample.net",
];

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// The kind of value a column produces.
#[derive(Debug, Clone)]
enum FieldKind {
    Uuid,
    FirstName,
    LastName,
    FullName,
    Username,
    Email,
    Word,
    City,
    Country,
    Bool,
    Date,
    Int { min: i64, max: i64 },
    Float { min: f64, max: f64 },
}

impl FieldKind {
    /// Parse a `type` string into a [`FieldKind`], reading any numeric bounds
    /// from the surrounding column object.
    fn parse(type_str: &str, obj: &Value) -> Result<FieldKind, String> {
        let kind = match type_str {
            "uuid" => FieldKind::Uuid,
            "first_name" => FieldKind::FirstName,
            "last_name" => FieldKind::LastName,
            "full_name" | "name" => FieldKind::FullName,
            "username" => FieldKind::Username,
            "email" => FieldKind::Email,
            "word" => FieldKind::Word,
            "city" => FieldKind::City,
            "country" => FieldKind::Country,
            "bool" | "boolean" => FieldKind::Bool,
            "date" => FieldKind::Date,
            "int" | "integer" => {
                let min = obj.get("min").and_then(Value::as_i64).unwrap_or(0);
                let max = obj.get("max").and_then(Value::as_i64).unwrap_or(100);
                if max < min {
                    return Err(format!("column `int` has max ({max}) < min ({min})"));
                }
                FieldKind::Int { min, max }
            }
            "float" | "double" => {
                let min = obj.get("min").and_then(Value::as_f64).unwrap_or(0.0);
                let max = obj.get("max").and_then(Value::as_f64).unwrap_or(100.0);
                if max < min {
                    return Err(format!("column `float` has max ({max}) < min ({min})"));
                }
                FieldKind::Float { min, max }
            }
            other => return Err(format!("unknown column type `{other}`")),
        };
        Ok(kind)
    }
}

/// One output column: a header name and the kind of value under it.
#[derive(Debug, Clone)]
struct Column {
    name: String,
    kind: FieldKind,
}

/// The default schema used when the config declares no `columns`.
fn default_columns() -> Vec<Column> {
    vec![
        Column {
            name: "id".into(),
            kind: FieldKind::Uuid,
        },
        Column {
            name: "first_name".into(),
            kind: FieldKind::FirstName,
        },
        Column {
            name: "last_name".into(),
            kind: FieldKind::LastName,
        },
        Column {
            name: "email".into(),
            kind: FieldKind::Email,
        },
        Column {
            name: "username".into(),
            kind: FieldKind::Username,
        },
        Column {
            name: "age".into(),
            kind: FieldKind::Int { min: 18, max: 90 },
        },
        Column {
            name: "city".into(),
            kind: FieldKind::City,
        },
        Column {
            name: "country".into(),
            kind: FieldKind::Country,
        },
        Column {
            name: "active".into(),
            kind: FieldKind::Bool,
        },
    ]
}

/// Resolved generator configuration.
#[derive(Debug, Clone)]
struct Config {
    path: PathBuf,
    rows: usize,
    seed: u64,
    cleanup: bool,
    columns: Vec<Column>,
}

/// Parse the plugin config object into a [`Config`].
fn parse_config(value: &Value) -> Result<Config, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "config must be a JSON object".to_string())?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty())
        .ok_or_else(|| "config requires a non-empty `path` string".to_string())?;

    let rows = obj
        .get("rows")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_ROWS);

    // Explicit seed → reproducible. Absent → a fresh seed for this run.
    let seed = match obj.get("seed") {
        Some(v) => v
            .as_u64()
            .ok_or_else(|| "`seed` must be a non-negative integer".to_string())?,
        None => rand::rng().random::<u64>(),
    };

    let cleanup = obj.get("cleanup").and_then(Value::as_bool).unwrap_or(false);

    let columns = match obj.get("columns") {
        None => default_columns(),
        Some(Value::Array(items)) => {
            if items.is_empty() {
                return Err("`columns` must not be empty".to_string());
            }
            let mut columns = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let col = item
                    .as_object()
                    .ok_or_else(|| format!("column {i} must be an object"))?;
                let name = col
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|n| !n.is_empty())
                    .ok_or_else(|| format!("column {i} requires a non-empty `name`"))?;
                let type_str = col
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("column `{name}` requires a `type`"))?;
                let kind = FieldKind::parse(type_str, item)
                    .map_err(|e| format!("column `{name}`: {e}"))?;
                columns.push(Column {
                    name: name.to_string(),
                    kind,
                });
            }
            columns
        }
        Some(_) => return Err("`columns` must be an array".to_string()),
    };

    Ok(Config {
        path: PathBuf::from(path),
        rows,
        seed,
        cleanup,
        columns,
    })
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// A seeded row generator. Reusing the same seed reproduces the identical
/// sequence of rows.
struct Generator {
    rng: SmallRng,
}

impl Generator {
    fn new(seed: u64) -> Generator {
        Generator {
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Produce one row (one string per column, in column order).
    fn row(&mut self, columns: &[Column]) -> Vec<String> {
        columns.iter().map(|c| self.field(&c.kind)).collect()
    }

    fn pick(&mut self, items: &[&'static str]) -> &'static str {
        items[self.rng.random_range(0..items.len())]
    }

    fn uuid(&mut self) -> String {
        let a = self.rng.random::<u32>();
        let b = self.rng.random::<u16>();
        // Version 4 in the high nibble of the 3rd group.
        let c = (self.rng.random::<u16>() & 0x0fff) | 0x4000;
        // RFC 4122 variant in the high bits of the 4th group.
        let d = (self.rng.random::<u16>() & 0x3fff) | 0x8000;
        let e0 = self.rng.random::<u32>();
        let e1 = self.rng.random::<u16>();
        format!("{a:08x}-{b:04x}-{c:04x}-{d:04x}-{e0:08x}{e1:04x}")
    }

    fn field(&mut self, kind: &FieldKind) -> String {
        match kind {
            FieldKind::Uuid => self.uuid(),
            FieldKind::FirstName => self.pick(FIRST_NAMES).to_string(),
            FieldKind::LastName => self.pick(LAST_NAMES).to_string(),
            FieldKind::FullName => {
                let first = self.pick(FIRST_NAMES);
                let last = self.pick(LAST_NAMES);
                format!("{first} {last}")
            }
            FieldKind::Username => {
                let first = self.pick(FIRST_NAMES).to_lowercase();
                let n = self.rng.random_range(1..10_000);
                format!("{first}{n}")
            }
            FieldKind::Email => {
                let first = self.pick(FIRST_NAMES).to_lowercase();
                let last = self.pick(LAST_NAMES).to_lowercase();
                let domain = self.pick(DOMAINS);
                let n = self.rng.random_range(1..1000);
                format!("{first}.{last}{n}@{domain}")
            }
            FieldKind::Word => self.pick(WORDS).to_string(),
            FieldKind::City => self.pick(CITIES).to_string(),
            FieldKind::Country => self.pick(COUNTRIES).to_string(),
            FieldKind::Bool => {
                if self.rng.random_bool(0.5) {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            FieldKind::Date => {
                let y = self.rng.random_range(1970..=2030);
                let m = self.rng.random_range(1..=12);
                let d = self.rng.random_range(1..=28);
                format!("{y:04}-{m:02}-{d:02}")
            }
            FieldKind::Int { min, max } => {
                let v = if max > min {
                    self.rng.random_range(*min..=*max)
                } else {
                    *min
                };
                v.to_string()
            }
            FieldKind::Float { min, max } => {
                let v = if max > min {
                    self.rng.random_range(*min..*max)
                } else {
                    *min
                };
                format!("{v:.2}")
            }
        }
    }
}

/// Escape a single CSV field per RFC 4180 (quote when it contains a comma,
/// quote or newline; double interior quotes).
fn csv_escape(field: &str) -> String {
    if field.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Generate the CSV file described by `config`. Returns the number of data
/// rows written (header excluded).
fn write_csv(config: &Config) -> std::io::Result<usize> {
    if let Some(parent) = config.path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut file = fs::File::create(&config.path)?;

    let header = config
        .columns
        .iter()
        .map(|c| csv_escape(&c.name))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(file, "{header}")?;

    let mut generator = Generator::new(config.seed);
    for _ in 0..config.rows {
        let line = generator
            .row(&config.columns)
            .iter()
            .map(|f| csv_escape(f))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(file, "{line}")?;
    }
    file.flush()?;
    Ok(config.rows)
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// What `start` produced, kept so `stop` can (optionally) clean it up.
struct Generated {
    path: PathBuf,
    cleanup: bool,
}

/// The service plugin. `start` generates the CSV; `stop` is idempotent and
/// optionally removes the generated file.
#[derive(Default)]
struct FakerGen {
    generated: Option<Generated>,
}

impl FfiService for FakerGen {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        let value: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => {
                return RErr(RString::from(format!(
                    "faker-gen: invalid config JSON: {e}"
                )))
            }
        };
        let config = match parse_config(&value) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(format!("faker-gen: {e}"))),
        };
        match write_csv(&config) {
            Ok(rows) => {
                let path = config.path.display().to_string();
                self.generated = Some(Generated {
                    path: config.path,
                    cleanup: config.cleanup,
                });
                // The handle a CSV feeder should read; the row count is a hint.
                ROk(RString::from(format!("{path} ({rows} rows)")))
            }
            Err(e) => RErr(RString::from(format!(
                "faker-gen: cannot write {}: {e}",
                config.path.display()
            ))),
        }
    }

    fn stop(&mut self) {
        // Idempotent: after the first call `generated` is None and this is a
        // no-op. `remove_file` errors (e.g. already gone) are ignored.
        if let Some(generated) = self.generated.take() {
            if generated.cleanup {
                let _ = fs::remove_file(&generated.path);
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
            "description": "Seeded, pure-Rust fake-data generator: materialises CSV rows a CSV feeder pulls from",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(FakerGen::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests (no network, no external services).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_columns() -> Vec<Column> {
        vec![
            Column {
                name: "id".into(),
                kind: FieldKind::Uuid,
            },
            Column {
                name: "name".into(),
                kind: FieldKind::FullName,
            },
            Column {
                name: "email".into(),
                kind: FieldKind::Email,
            },
            Column {
                name: "age".into(),
                kind: FieldKind::Int { min: 18, max: 65 },
            },
            Column {
                name: "score".into(),
                kind: FieldKind::Float {
                    min: 0.0,
                    max: 10.0,
                },
            },
            Column {
                name: "active".into(),
                kind: FieldKind::Bool,
            },
        ]
    }

    #[test]
    fn same_seed_reproduces_sequence() {
        let cols = sample_columns();
        let mut a = Generator::new(42);
        let mut b = Generator::new(42);
        for _ in 0..200 {
            assert_eq!(a.row(&cols), b.row(&cols));
        }
    }

    #[test]
    fn different_seed_diverges() {
        let cols = sample_columns();
        let mut a = Generator::new(1);
        let mut b = Generator::new(2);
        let ra: Vec<_> = (0..32).map(|_| a.row(&cols)).collect();
        let rb: Vec<_> = (0..32).map(|_| b.row(&cols)).collect();
        assert_ne!(ra, rb, "distinct seeds should not produce identical data");
    }

    #[test]
    fn each_pull_matches_schema() {
        let cols = sample_columns();
        let mut g = Generator::new(7);
        for _ in 0..100 {
            let row = g.row(&cols);
            assert_eq!(row.len(), cols.len(), "one value per column");

            // uuid shape: 36 chars, 5 hyphen-separated groups.
            assert_eq!(row[0].len(), 36);
            assert_eq!(row[0].matches('-').count(), 4);
            // email
            assert!(row[2].contains('@'), "email `{}` has no @", row[2]);
            // int in range
            let age: i64 = row[3].parse().expect("age is an integer");
            assert!((18..=65).contains(&age), "age {age} out of range");
            // float parses and is in range
            let score: f64 = row[4].parse().expect("score is a float");
            assert!((0.0..10.0).contains(&score), "score {score} out of range");
            // bool
            assert!(row[5] == "true" || row[5] == "false", "bool `{}`", row[5]);
        }
    }

    #[test]
    fn parse_defaults() {
        let config = parse_config(&serde_json::json!({ "path": "x.csv" })).expect("parses");
        assert_eq!(config.rows, DEFAULT_ROWS);
        assert!(!config.cleanup);
        assert!(!config.columns.is_empty());
    }

    #[test]
    fn parse_requires_path() {
        let err = parse_config(&serde_json::json!({ "rows": 10 })).expect_err("no path");
        assert!(err.contains("path"), "{err}");
    }

    #[test]
    fn parse_rejects_unknown_type() {
        let err = parse_config(&serde_json::json!({
            "path": "x.csv",
            "columns": [ { "name": "x", "type": "wizardry" } ]
        }))
        .expect_err("unknown type");
        assert!(err.contains("wizardry"), "{err}");
    }

    #[test]
    fn parse_rejects_bad_int_bounds() {
        let err = parse_config(&serde_json::json!({
            "path": "x.csv",
            "columns": [ { "name": "n", "type": "int", "min": 10, "max": 1 } ]
        }))
        .expect_err("bad bounds");
        assert!(err.contains("max"), "{err}");
    }

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("faker-gen-{tag}-{}.csv", std::process::id()))
    }

    fn start_ok(svc: &mut FakerGen, config: &Value) -> String {
        match svc.start(RString::from(config.to_string())) {
            ROk(handle) => handle.into_string(),
            RErr(e) => panic!("start failed: {e}"),
        }
    }

    #[test]
    fn start_generates_file_and_returns_handle() {
        let path = temp_path("start");
        let _ = fs::remove_file(&path);
        let config = serde_json::json!({
            "path": path.to_str().unwrap(),
            "rows": 25,
            "seed": 99,
            "cleanup": true,
            "columns": [
                { "name": "id", "type": "uuid" },
                { "name": "name", "type": "full_name" },
                { "name": "age", "type": "int", "min": 20, "max": 40 }
            ]
        });

        let mut svc = FakerGen::default();
        let handle = start_ok(&mut svc, &config);
        assert!(!handle.is_empty(), "start returns a non-empty handle");

        let content = fs::read_to_string(&path).expect("csv written");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 26, "header + 25 rows");
        assert_eq!(lines[0], "id,name,age");
        for row in &lines[1..] {
            assert_eq!(row.split(',').count(), 3, "row `{row}` has 3 fields");
        }

        // stop() is idempotent; with cleanup it removes the generated file.
        svc.stop();
        svc.stop();
        assert!(!path.exists(), "cleanup removed the file");
    }

    #[test]
    fn start_is_deterministic_end_to_end() {
        let p1 = temp_path("det-a");
        let p2 = temp_path("det-b");
        let _ = fs::remove_file(&p1);
        let _ = fs::remove_file(&p2);
        let cfg = |p: &PathBuf| serde_json::json!({ "path": p.to_str().unwrap(), "rows": 40, "seed": 123 });

        let mut a = FakerGen::default();
        let mut b = FakerGen::default();
        start_ok(&mut a, &cfg(&p1));
        start_ok(&mut b, &cfg(&p2));

        let ca = fs::read_to_string(&p1).expect("a written");
        let cb = fs::read_to_string(&p2).expect("b written");
        assert_eq!(ca, cb, "same seed yields identical CSV");

        let _ = fs::remove_file(&p1);
        let _ = fs::remove_file(&p2);
    }

    #[test]
    fn stop_without_start_is_noop() {
        let mut svc = FakerGen::default();
        svc.stop();
        svc.stop();
        assert!(svc.generated.is_none());
    }

    #[test]
    fn service_name_is_stable() {
        let svc = FakerGen::default();
        assert_eq!(svc.name().as_str(), NAME);
    }
}
