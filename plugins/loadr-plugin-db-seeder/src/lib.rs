//! `loadr-plugin-db-seeder` — a native **service** plugin that brackets a run
//! with database fixtures: setup SQL before the run, teardown SQL after.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle: the
//! host calls `start(config_json)` once, before any VU, and `stop()` once
//! afterwards (idempotently). This plugin uses `start()` to open a connection to
//! the database (`url`, a `postgres://` or `mysql://` URL) and run the `setup`
//! scripts — creating tables, truncating, and inserting the fixtures the test
//! assumes; `stop()` runs the `teardown` scripts to put the database back. The
//! result is a **known state per run**: every execution starts from the same
//! seeded baseline and cleans up after itself.
//!
//! The heavy `sqlx` driver lives only inside this plugin's dynamic library,
//! never in the loadr core binary. The `any` sqlx driver is used so one plugin
//! seeds both PostgreSQL and MySQL — the concrete driver is selected from the
//! URL scheme at connect time. `rustls` TLS avoids a system OpenSSL dependency
//! so this cdylib cross-compiles cleanly.
//!
//! # Configuration
//!
//! ```jsonc
//! {
//!   "url":            "postgres://loadr:loadr@db:5432/loadr", // required
//!   "setup":          ["sql/schema.sql", "TRUNCATE orders;"],  // files or inline
//!   "teardown":       ["sql/clean.sql"],                       // files or inline
//!   "on_setup_error": "abort",   // "abort" (default) | "continue"
//!   "transaction":    false,     // wrap each script in one transaction
//!   "dir":            "."        // base dir for relative script paths
//! }
//! ```
//!
//! # Testability
//!
//! The database is reached only through a small [`StatementExecutor`] seam, so
//! the whole orchestration — script resolution, statement splitting, the
//! `on_setup_error` policy, transaction bracketing and the executed-statement
//! count — is exercised by offline unit tests with a scripted mock executor;
//! nothing in the test suite opens a socket.

use std::path::{Path, PathBuf};
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use once_cell::sync::OnceCell;
use serde_json::Value;
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use tokio::runtime::Runtime;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "db-seeder";

/// The single Tokio runtime the plugin uses to drive the async `sqlx` driver.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build db-seeder plugin tokio runtime")
    })
}

/// Register the `any` driver's concrete backends (postgres + mysql) exactly
/// once per process. `install_default_drivers` panics if called twice, so it is
/// guarded by a `Once`.
fn ensure_drivers() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

// ---------------------------------------------------------------------------
// Config.
// ---------------------------------------------------------------------------

/// What a failing setup statement does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnError {
    /// Fail `start()`; the run never begins.
    Abort,
    /// Log the error and proceed to the next statement.
    Continue,
}

/// Parsed, validated configuration.
#[derive(Debug)]
struct SeedConfig {
    url: String,
    setup: Vec<String>,
    teardown: Vec<String>,
    on_setup_error: OnError,
    transaction: bool,
    dir: String,
}

/// Pull a required non-empty string field.
fn required_str(config: &Value, key: &str) -> Result<String, String> {
    match config.get(key).and_then(Value::as_str) {
        Some(s) if !s.trim().is_empty() => Ok(s.to_string()),
        _ => Err(format!("config requires a non-empty `{key}` string")),
    }
}

/// Parse an optional array-of-strings field (defaults to empty). Rejects a
/// non-array value or a non-string element with a descriptive error.
fn string_array(config: &Value, key: &str) -> Result<Vec<String>, String> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                match item.as_str() {
                    Some(s) => out.push(s.to_string()),
                    None => {
                        return Err(format!(
                            "`{key}[{i}]` must be a string (a path or inline SQL)"
                        ))
                    }
                }
            }
            Ok(out)
        }
        Some(_) => Err(format!("`{key}` must be an array of strings")),
    }
}

fn parse_config(config_json: &str) -> Result<SeedConfig, String> {
    let config: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let url = required_str(&config, "url")?;
    let setup = string_array(&config, "setup")?;
    let teardown = string_array(&config, "teardown")?;

    let on_setup_error = match config.get("on_setup_error").and_then(Value::as_str) {
        None | Some("abort") => OnError::Abort,
        Some("continue") => OnError::Continue,
        Some(other) => {
            return Err(format!(
                "unknown on_setup_error `{other}` (use `abort` or `continue`)"
            ))
        }
    };

    let transaction = config
        .get("transaction")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let dir = config
        .get("dir")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(".")
        .to_string();

    Ok(SeedConfig {
        url,
        setup,
        teardown,
        on_setup_error,
        transaction,
        dir,
    })
}

/// Redact credentials from a connection URL for logging. On a parse failure the
/// whole thing is masked rather than risk leaking a raw credential string.
fn redact_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        }
        Err(_) => "<redacted>".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Script resolution + statement splitting (pure, offline-testable).
// ---------------------------------------------------------------------------

/// A resolved script: a human label and its ordered SQL statements.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScriptPlan {
    label: String,
    statements: Vec<String>,
}

/// A config entry is treated as a file path when it looks like one — a
/// single-line value ending in `.sql`. Anything else is inline SQL.
fn is_sql_file(entry: &str) -> bool {
    let trimmed = entry.trim();
    !trimmed.contains('\n') && trimmed.to_lowercase().ends_with(".sql")
}

/// Resolve one entry into an executable [`ScriptPlan`], reading the file when the
/// entry is a `.sql` path (relative to `dir`) or treating it as inline SQL.
fn resolve_entry(entry: &str, dir: &Path) -> Result<ScriptPlan, String> {
    if is_sql_file(entry) {
        let path = dir.join(entry.trim());
        let sql = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read script `{}`: {e}", path.display()))?;
        Ok(ScriptPlan {
            label: entry.trim().to_string(),
            statements: split_statements(&sql),
        })
    } else {
        Ok(ScriptPlan {
            label: "<inline>".to_string(),
            statements: split_statements(entry),
        })
    }
}

/// Split a SQL script into individual statements on top-level `;`, respecting
/// single-quoted strings (with `''` escapes), `"`/`` ` `` quoted identifiers,
/// `--` line comments, `/* */` block comments and PostgreSQL `$tag$…$tag$`
/// dollar-quoted bodies (so a `;` inside a function body does not split it).
/// Blank statements are dropped.
fn split_statements(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;
    let mut cur = String::new();
    let mut out = Vec::new();

    while i < n {
        let c = chars[i];
        match c {
            // Line comment: consume to end of line (the newline is handled next).
            '-' if i + 1 < n && chars[i + 1] == '-' => {
                while i < n && chars[i] != '\n' {
                    cur.push(chars[i]);
                    i += 1;
                }
            }
            // Block comment.
            '/' if i + 1 < n && chars[i + 1] == '*' => {
                cur.push('/');
                cur.push('*');
                i += 2;
                while i < n && !(chars[i] == '*' && i + 1 < n && chars[i + 1] == '/') {
                    cur.push(chars[i]);
                    i += 1;
                }
                if i < n {
                    cur.push('*');
                    if i + 1 < n {
                        cur.push('/');
                    }
                    i += 2;
                }
            }
            // Quoted string / identifier.
            '\'' | '"' | '`' => {
                let quote = c;
                cur.push(c);
                i += 1;
                while i < n {
                    cur.push(chars[i]);
                    if chars[i] == quote {
                        // Doubled quote inside a single-quoted string is an escape.
                        if quote == '\'' && i + 1 < n && chars[i + 1] == '\'' {
                            cur.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            // Dollar-quoted body ($$…$$ or $tag$…$tag$).
            '$' => {
                if let Some(tag_len) = dollar_tag(&chars, i) {
                    let tag: String = chars[i..i + tag_len].iter().collect();
                    cur.push_str(&tag);
                    i += tag_len;
                    while i < n {
                        if chars[i] == '$' && matches_at(&chars, i, &tag) {
                            cur.push_str(&tag);
                            i += tag_len;
                            break;
                        }
                        cur.push(chars[i]);
                        i += 1;
                    }
                } else {
                    cur.push('$');
                    i += 1;
                }
            }
            ';' => {
                push_statement(&mut out, &cur);
                cur.clear();
                i += 1;
            }
            _ => {
                cur.push(c);
                i += 1;
            }
        }
    }
    push_statement(&mut out, &cur);
    out
}

/// Push `cur` (trimmed) as a statement if it has non-whitespace content.
fn push_statement(out: &mut Vec<String>, cur: &str) {
    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

/// If a dollar-quote tag opens at `i` (`chars[i] == '$'`), return the full tag
/// length (>= 2, e.g. 2 for `$$`), else `None`.
fn dollar_tag(chars: &[char], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < chars.len() {
        let c = chars[j];
        if c == '$' {
            return Some(j - i + 1);
        }
        if c.is_alphanumeric() || c == '_' {
            j += 1;
        } else {
            return None;
        }
    }
    None
}

/// True when `tag` matches the characters starting at `chars[i]`.
fn matches_at(chars: &[char], i: usize, tag: &str) -> bool {
    let tag_chars: Vec<char> = tag.chars().collect();
    if i + tag_chars.len() > chars.len() {
        return false;
    }
    chars[i..i + tag_chars.len()] == tag_chars[..]
}

// ---------------------------------------------------------------------------
// Execution seam.
// ---------------------------------------------------------------------------

/// Executes one SQL statement against the database. A seam so the orchestration
/// can be unit-tested with a scripted mock instead of a real connection.
trait StatementExecutor {
    fn execute(&mut self, sql: &str) -> Result<(), String>;
}

/// The real executor: runs each statement through `sqlx` on a single-connection
/// pool (so transaction control statements land on the same connection).
struct SqlxExecutor<'a> {
    rt: &'static Runtime,
    pool: &'a AnyPool,
}

impl StatementExecutor for SqlxExecutor<'_> {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.rt.block_on(async {
            sqlx::query(sql)
                .execute(self.pool)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }
}

/// Run every entry in order, returning the number of statements that were
/// successfully applied. Under [`OnError::Abort`] the first failure (a missing
/// script, or a statement/transaction error) returns `Err`; under
/// [`OnError::Continue`] it is logged and the next entry is attempted.
fn run_scripts(
    exec: &mut dyn StatementExecutor,
    entries: &[String],
    dir: &Path,
    on_error: OnError,
    transaction: bool,
) -> Result<u64, String> {
    let mut count = 0u64;
    for entry in entries {
        let plan = match resolve_entry(entry, dir) {
            Ok(p) => p,
            Err(e) => {
                if on_error == OnError::Continue {
                    eprintln!("db-seeder: skipping script: {e}");
                    continue;
                }
                return Err(e);
            }
        };
        run_one(exec, &plan, on_error, transaction, &mut count)?;
    }
    Ok(count)
}

/// Run a single script as one unit (optionally bracketed in a transaction). On
/// success `count` is advanced by the number of applied statements; a
/// transaction that rolls back contributes nothing.
fn run_one(
    exec: &mut dyn StatementExecutor,
    plan: &ScriptPlan,
    on_error: OnError,
    transaction: bool,
    count: &mut u64,
) -> Result<(), String> {
    if transaction {
        if let Err(e) = exec.execute("BEGIN") {
            return report(on_error, format!("[{}] BEGIN failed: {e}", plan.label));
        }
    }

    let mut applied = 0u64;
    let mut failure: Option<String> = None;
    for (idx, stmt) in plan.statements.iter().enumerate() {
        match exec.execute(stmt) {
            Ok(()) => applied += 1,
            Err(e) => {
                failure = Some(format!(
                    "[{}] statement {} failed: {e}",
                    plan.label,
                    idx + 1
                ));
                break;
            }
        }
    }

    if transaction {
        if failure.is_some() {
            let _ = exec.execute("ROLLBACK");
        } else if let Err(e) = exec.execute("COMMIT") {
            failure = Some(format!("[{}] COMMIT failed: {e}", plan.label));
        } else {
            *count += applied;
        }
    } else {
        // Non-transactional statements persist as they run.
        *count += applied;
    }

    match failure {
        Some(msg) => report(on_error, msg),
        None => Ok(()),
    }
}

/// Apply the error policy to a failure: `Abort` returns it, `Continue` logs it.
fn report(on_error: OnError, msg: String) -> Result<(), String> {
    if on_error == OnError::Continue {
        eprintln!("db-seeder: {msg}");
        Ok(())
    } else {
        Err(msg)
    }
}

// ---------------------------------------------------------------------------
// Pool wiring (the only network-touching code).
// ---------------------------------------------------------------------------

/// Open a single-connection pool to `url`. `max_connections(1)` guarantees every
/// statement in a run reuses the same connection, which transaction control
/// (`BEGIN`/`COMMIT`/`ROLLBACK`) requires.
fn connect_pool(url: &str) -> Result<AnyPool, String> {
    ensure_drivers();
    runtime()
        .block_on(
            AnyPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(10))
                .connect(url),
        )
        .map_err(|e| format!("cannot connect to `{}`: {e}", redact_url(url)))
}

// ---------------------------------------------------------------------------
// The service.
// ---------------------------------------------------------------------------

/// The service plugin instance. Holds lifecycle state plus what `stop()` needs
/// to re-run teardown against the same database.
#[derive(Default)]
struct DbSeeder {
    started: bool,
    url: Option<String>,
    teardown: Vec<String>,
    transaction: bool,
    dir: String,
}

impl DbSeeder {
    /// The whole `start()` flow, factored to a plain `Result` so the FFI shim is
    /// a thin wrapper. On success returns a human status string (the service
    /// ABI's "plugin-defined string").
    fn run_start(&mut self, config_json: &str) -> Result<String, String> {
        let cfg = parse_config(config_json)?;
        let dir = PathBuf::from(&cfg.dir);

        let pool = connect_pool(&cfg.url)?;
        let result = {
            let mut exec = SqlxExecutor {
                rt: runtime(),
                pool: &pool,
            };
            run_scripts(
                &mut exec,
                &cfg.setup,
                &dir,
                cfg.on_setup_error,
                cfg.transaction,
            )
        };
        runtime().block_on(pool.close());
        let count = result?;

        self.url = Some(cfg.url);
        self.teardown = cfg.teardown;
        self.transaction = cfg.transaction;
        self.dir = cfg.dir;
        self.started = true;
        Ok(format!("db-seeder: applied {count} setup statement(s)"))
    }

    /// Best-effort teardown: reconnect and run the teardown scripts, logging (not
    /// failing) on any error so a run's exit code stays governed by thresholds.
    fn run_teardown(&self) {
        if self.teardown.is_empty() {
            return;
        }
        let Some(url) = self.url.as_deref() else {
            return;
        };
        let pool = match connect_pool(url) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("db-seeder: teardown connect failed: {e}");
                return;
            }
        };
        let dir = PathBuf::from(&self.dir);
        {
            let mut exec = SqlxExecutor {
                rt: runtime(),
                pool: &pool,
            };
            // Teardown is always best-effort, whatever the setup policy was.
            let _ = run_scripts(
                &mut exec,
                &self.teardown,
                &dir,
                OnError::Continue,
                self.transaction,
            );
        }
        runtime().block_on(pool.close());
    }

    /// Clear lifecycle state so a second `stop()` is a no-op.
    fn reset(&mut self) {
        self.started = false;
        self.url = None;
        self.teardown = Vec::new();
        self.transaction = false;
        self.dir = String::new();
    }
}

impl FfiService for DbSeeder {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if self.started {
            // Already seeded: don't re-run setup on a second start.
            return ROk(RString::from("db-seeder: already started"));
        }
        match self.run_start(config_json.as_str()) {
            Ok(status) => ROk(RString::from(status)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        if self.started {
            self.run_teardown();
        }
        self.reset();
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description":
                "Runs setup SQL before a run and teardown SQL after, for a known fixture state per run",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(DbSeeder::default(), abi_stable::erased_types::TD_Opaque)
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
// Tests — all offline; the database is exercised through a scripted mock
// executor, never a real connection.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// Records every statement it is handed and can be scripted to fail on the
    /// first statement whose text equals `fail_on`.
    #[derive(Default)]
    struct MockExec {
        executed: Vec<String>,
        fail_on: Option<String>,
    }

    impl StatementExecutor for MockExec {
        fn execute(&mut self, sql: &str) -> Result<(), String> {
            self.executed.push(sql.to_string());
            if self.fail_on.as_deref() == Some(sql) {
                return Err(format!("boom: {sql}"));
            }
            Ok(())
        }
    }

    fn mock_failing_on(stmt: &str) -> MockExec {
        MockExec {
            executed: Vec::new(),
            fail_on: Some(stmt.to_string()),
        }
    }

    // -- statement splitting -------------------------------------------------

    #[test]
    fn splits_basic_statements() {
        assert_eq!(
            split_statements("CREATE TABLE t (id int); INSERT INTO t VALUES (1);"),
            vec!["CREATE TABLE t (id int)", "INSERT INTO t VALUES (1)"]
        );
    }

    #[test]
    fn trailing_statement_without_semicolon_is_kept() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn blank_and_empty_scripts_yield_no_statements() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("   ;  ; \n ;").is_empty());
    }

    #[test]
    fn semicolon_inside_string_does_not_split() {
        let stmts = split_statements("INSERT INTO t VALUES ('a; b'); SELECT 1;");
        assert_eq!(stmts, vec!["INSERT INTO t VALUES ('a; b')", "SELECT 1"]);
    }

    #[test]
    fn doubled_quote_escape_is_respected() {
        let stmts = split_statements("INSERT INTO t VALUES ('O''Brien; Co'); SELECT 2;");
        assert_eq!(
            stmts,
            vec!["INSERT INTO t VALUES ('O''Brien; Co')", "SELECT 2"]
        );
    }

    #[test]
    fn comments_do_not_split() {
        let sql = "-- comment; with semicolon\nSELECT 1; /* block ; comment */ SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("SELECT 1"));
        // The block comment stays attached to the statement text (harmless to
        // the driver); the point is the inner `;` did not split it.
        assert!(stmts[1].contains("SELECT 2"));
    }

    #[test]
    fn dollar_quoted_body_is_one_statement() {
        let sql = "CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END; $$ LANGUAGE plpgsql; SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2, "{stmts:?}");
        assert!(stmts[0].contains("RETURN 1;"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn tagged_dollar_quote_is_one_statement() {
        let sql = "SELECT $tag$ a ; b $tag$; SELECT 9;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2, "{stmts:?}");
        assert!(stmts[0].contains("a ; b"));
    }

    // -- entry classification / resolution -----------------------------------

    #[test]
    fn classifies_files_and_inline() {
        assert!(is_sql_file("sql/seed.sql"));
        assert!(is_sql_file("SEED.SQL"));
        assert!(!is_sql_file("TRUNCATE orders;"));
        // A multi-line value is inline SQL even if it happens to end in .sql.
        assert!(!is_sql_file("SELECT 1;\n-- x.sql"));
    }

    #[test]
    fn resolves_inline_entry_without_filesystem() {
        let plan = resolve_entry("TRUNCATE a; TRUNCATE b;", Path::new(".")).unwrap();
        assert_eq!(plan.label, "<inline>");
        assert_eq!(plan.statements, vec!["TRUNCATE a", "TRUNCATE b"]);
    }

    #[test]
    fn resolves_file_entry_from_disk() {
        let dir = std::env::temp_dir().join(format!("loadr-db-seeder-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seed.sql");
        std::fs::write(&path, "CREATE TABLE t (id int);\nINSERT INTO t VALUES (1);").unwrap();

        let plan = resolve_entry("seed.sql", &dir).unwrap();
        assert_eq!(plan.label, "seed.sql");
        assert_eq!(plan.statements.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = resolve_entry("nope.sql", Path::new("/definitely/not/here")).unwrap_err();
        assert!(err.contains("cannot read script"), "{err}");
    }

    // -- run orchestration ---------------------------------------------------

    #[test]
    fn runs_all_statements_and_counts() {
        let mut exec = MockExec::default();
        let entries = vec!["SELECT 1; SELECT 2;".to_string(), "SELECT 3".to_string()];
        let count =
            run_scripts(&mut exec, &entries, Path::new("."), OnError::Abort, false).unwrap();
        assert_eq!(count, 3);
        assert_eq!(exec.executed, vec!["SELECT 1", "SELECT 2", "SELECT 3"]);
    }

    #[test]
    fn abort_stops_at_first_failure() {
        let mut exec = mock_failing_on("SELECT 2");
        let entries = vec!["SELECT 1; SELECT 2; SELECT 3;".to_string()];
        let err =
            run_scripts(&mut exec, &entries, Path::new("."), OnError::Abort, false).unwrap_err();
        assert!(err.contains("statement 2 failed"), "{err}");
        // Statement 3 was never attempted.
        assert_eq!(exec.executed, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn continue_skips_failure_and_counts_the_rest() {
        let mut exec = mock_failing_on("SELECT 2");
        // Two entries: the first fails mid-way, the second still runs.
        let entries = vec![
            "SELECT 1; SELECT 2; SELECT 3;".to_string(),
            "SELECT 4".to_string(),
        ];
        let count = run_scripts(
            &mut exec,
            &entries,
            Path::new("."),
            OnError::Continue,
            false,
        )
        .unwrap();
        // SELECT 1 (ok) + SELECT 4 (ok) = 2 applied; SELECT 2 failed, 3 skipped.
        assert_eq!(count, 2);
        assert_eq!(exec.executed, vec!["SELECT 1", "SELECT 2", "SELECT 4"]);
    }

    #[test]
    fn continue_skips_missing_file() {
        let mut exec = MockExec::default();
        let entries = vec!["missing.sql".to_string(), "SELECT 7".to_string()];
        let count = run_scripts(
            &mut exec,
            &entries,
            Path::new("/no/such/dir"),
            OnError::Continue,
            false,
        )
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(exec.executed, vec!["SELECT 7"]);
    }

    #[test]
    fn transaction_mode_brackets_and_commits() {
        let mut exec = MockExec::default();
        let entries = vec!["SELECT 1; SELECT 2;".to_string()];
        let count = run_scripts(&mut exec, &entries, Path::new("."), OnError::Abort, true).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            exec.executed,
            vec!["BEGIN", "SELECT 1", "SELECT 2", "COMMIT"]
        );
    }

    #[test]
    fn transaction_rollback_contributes_no_count() {
        let mut exec = mock_failing_on("SELECT 2");
        let entries = vec!["SELECT 1; SELECT 2;".to_string()];
        let count =
            run_scripts(&mut exec, &entries, Path::new("."), OnError::Continue, true).unwrap();
        // The whole transaction rolled back, so nothing counts even though
        // SELECT 1 executed.
        assert_eq!(count, 0);
        assert_eq!(
            exec.executed,
            vec!["BEGIN", "SELECT 1", "SELECT 2", "ROLLBACK"]
        );
    }

    // -- config parsing ------------------------------------------------------

    #[test]
    fn config_requires_url() {
        let err = parse_config(r#"{"setup":["SELECT 1"]}"#).unwrap_err();
        assert!(err.contains("url"), "{err}");
    }

    #[test]
    fn config_defaults_and_overrides() {
        let cfg = parse_config(
            r#"{"url":"postgres://h/db","setup":["a.sql"],"teardown":["b.sql"],"on_setup_error":"continue","transaction":true,"dir":"fx"}"#,
        )
        .unwrap();
        assert_eq!(cfg.url, "postgres://h/db");
        assert_eq!(cfg.setup, vec!["a.sql"]);
        assert_eq!(cfg.teardown, vec!["b.sql"]);
        assert_eq!(cfg.on_setup_error, OnError::Continue);
        assert!(cfg.transaction);
        assert_eq!(cfg.dir, "fx");
    }

    #[test]
    fn config_defaults_when_minimal() {
        let cfg = parse_config(r#"{"url":"mysql://h/db"}"#).unwrap();
        assert!(cfg.setup.is_empty());
        assert!(cfg.teardown.is_empty());
        assert_eq!(cfg.on_setup_error, OnError::Abort);
        assert!(!cfg.transaction);
        assert_eq!(cfg.dir, ".");
    }

    #[test]
    fn config_rejects_bad_values() {
        assert!(parse_config("not json").is_err());
        assert!(parse_config(r#"{"url":"postgres://h","on_setup_error":"maybe"}"#).is_err());
        // A non-string element in setup is rejected.
        assert!(parse_config(r#"{"url":"postgres://h","setup":[123]}"#).is_err());
        // A non-array setup is rejected.
        assert!(parse_config(r#"{"url":"postgres://h","setup":"SELECT 1"}"#).is_err());
    }

    // -- redaction -----------------------------------------------------------

    #[test]
    fn redacts_url_password() {
        let red = redact_url("postgres://user:secret@host:5432/db");
        assert!(!red.contains("secret"), "{red}");
        assert!(red.contains("user"), "{red}");
        assert!(red.contains("host"), "{red}");
        assert_eq!(redact_url("not a url"), "<redacted>");
    }

    // -- lifecycle -----------------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = DbSeeder::default();
        svc.stop();
        svc.stop(); // second stop must not panic
        assert!(!svc.started);
        assert!(svc.url.is_none());
    }

    #[test]
    fn start_rejects_bad_config_without_network() {
        let mut svc = DbSeeder::default();
        // Missing `url` — fails validation before any connection attempt.
        let res = svc.start(RString::from(r#"{"setup":["SELECT 1"]}"#));
        assert!(matches!(res, RErr(_)));
        assert!(!svc.started);
        // Malformed JSON.
        assert!(matches!(svc.start(RString::from("not json")), RErr(_)));
        svc.stop();
    }

    #[test]
    fn reset_clears_state() {
        let mut svc = DbSeeder {
            started: true,
            url: Some("postgres://h/db".to_string()),
            teardown: vec!["clean.sql".to_string()],
            transaction: true,
            dir: "fx".to_string(),
        };
        svc.reset();
        assert!(!svc.started);
        assert!(svc.url.is_none());
        assert!(svc.teardown.is_empty());
        assert!(!svc.transaction);
        assert_eq!(svc.dir, "");
    }

    #[test]
    fn info_declares_service_kind() {
        let info: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(info["kind"], "service");
        assert_eq!(info["name"], "db-seeder");
    }
}
