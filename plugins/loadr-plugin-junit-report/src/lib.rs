//! `loadr-plugin-junit-report` — a native **output** plugin that turns a run's
//! checks and thresholds into a JUnit XML report.
//!
//! # Why an output plugin
//!
//! loadr's native output ABI ([`FfiOutput`]) follows the same lifecycle as the
//! built-in exporters: `start(config_json)` once before the run,
//! `on_samples` / `on_snapshot` during it, and `finish(summary_json)` once at
//! the end. This plugin does all its work in the two endpoints:
//!
//! * `start` validates the config and opens (truncating) the report file;
//! * `on_samples` and `on_snapshot` are deliberate no-ops — nothing is written
//!   mid-run, so the file appears complete-and-valid or not at all;
//! * `finish` reads the end-of-run summary, maps every `check` and every
//!   `threshold` to a JUnit `<testcase>` under a single `<testsuite>`, and
//!   writes the rendered XML to disk.
//!
//! A passing check/threshold is an empty (green) `<testcase>`; a failing one
//! carries a `<failure>` child whose message names the condition that broke.
//! The suite counts (`tests`, `failures`, `time`) come from the summary and the
//! run's `run_id` rides along as a property so concurrent runs stay
//! distinguishable. The output is a CI-ingestable `junit.xml`.
//!
//! # Testability
//!
//! File I/O sits behind a [`SinkFactory`]/[`Sink`] seam (mirroring the
//! `redis-loader` plugin's `ConnFactory`), so the whole lifecycle — config
//! parsing, summary parsing and XML rendering — is unit-tested against an
//! in-memory sink, never touching the filesystem or the network.

use std::fs::OpenOptions;
use std::io::Write as _;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use serde_json::Value;

const NAME: &str = "junit-report";

// ---------------------------------------------------------------------------
// Output sink — a seam so the plugin can be unit-tested without a real file.
// ---------------------------------------------------------------------------

/// A destination the rendered report is written to.
trait Sink: Send {
    /// Write the whole report, then flush it to durable storage.
    fn write_report(&mut self, xml: &str) -> Result<(), String>;
}

/// Opens [`Sink`]s. A new sink is created (truncating any previous report) in
/// `start`, so re-running in the same workspace replaces the old file.
trait SinkFactory: Send {
    fn open(&self, path: &str) -> Result<Box<dyn Sink>, String>;
}

/// A real file sink: create-or-truncate then append the report bytes.
struct FileSink {
    file: std::fs::File,
}

impl Sink for FileSink {
    fn write_report(&mut self, xml: &str) -> Result<(), String> {
        self.file
            .write_all(xml.as_bytes())
            .map_err(|e| format!("write failed: {e}"))?;
        self.file.flush().map_err(|e| format!("flush failed: {e}"))
    }
}

/// Opens real [`FileSink`]s, truncating the target on open.
struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    fn open(&self, path: &str) -> Result<Box<dyn Sink>, String> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| format!("cannot open {path}: {e}"))?;
        Ok(Box::new(FileSink { file }))
    }
}

// ---------------------------------------------------------------------------
// Config.
// ---------------------------------------------------------------------------

/// Parsed `start` configuration.
#[derive(Debug, PartialEq, Eq)]
struct Config {
    path: String,
}

/// Parse and validate the config JSON. A missing `path` defaults to
/// `junit.xml`; a present-but-empty `path` is an error.
fn parse_config(config_json: &str) -> Result<Config, String> {
    let cfg: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;
    let path = match cfg.get("path") {
        None | Some(Value::Null) => "junit.xml".to_string(),
        Some(v) => match v.as_str() {
            Some(s) if !s.is_empty() => s.to_string(),
            Some(_) => return Err("config `path` must not be empty".to_string()),
            None => return Err("config `path` must be a string".to_string()),
        },
    };
    Ok(Config { path })
}

// ---------------------------------------------------------------------------
// Report model + rendering.
// ---------------------------------------------------------------------------

/// One check from the summary: a named condition asserted `passes + fails`
/// times over the run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckCase {
    name: String,
    passes: u64,
    fails: u64,
}

/// One threshold from the summary: an expression on a metric that either held
/// or broke.
#[derive(Debug, Clone, PartialEq)]
struct ThresholdCase {
    metric: String,
    expression: String,
    observed: Option<f64>,
    passed: bool,
}

/// Everything needed to render the JUnit report, extracted from the run
/// summary.
#[derive(Debug, Clone, PartialEq, Default)]
struct Report {
    suite_name: String,
    run_id: String,
    duration_secs: f64,
    checks: Vec<CheckCase>,
    thresholds: Vec<ThresholdCase>,
}

impl Report {
    /// Extract a [`Report`] from a run-summary JSON object. Missing fields fall
    /// back to safe defaults rather than failing — `finish` has no error
    /// channel, and a best-effort report beats none.
    fn from_summary_json(summary_json: &str) -> Report {
        let v: Value = serde_json::from_str(summary_json).unwrap_or(Value::Null);
        let suite_name = v
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("loadr test")
            .to_string();
        let run_id = v
            .get("run_id")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let duration_secs = v
            .get("duration_secs")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);

        let checks = v
            .get("checks")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|c| CheckCase {
                        name: c
                            .get("name")
                            .and_then(|x| x.as_str())
                            .unwrap_or("unnamed")
                            .to_string(),
                        passes: c.get("passes").and_then(|x| x.as_u64()).unwrap_or(0),
                        fails: c.get("fails").and_then(|x| x.as_u64()).unwrap_or(0),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let thresholds = v
            .get("thresholds")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|t| ThresholdCase {
                        metric: t
                            .get("metric")
                            .and_then(|x| x.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        expression: t
                            .get("expression")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        observed: t.get("observed").and_then(|x| x.as_f64()),
                        // Absent `passed` is treated as a failure so a
                        // malformed entry never masquerades as green.
                        passed: t.get("passed").and_then(|x| x.as_bool()).unwrap_or(false),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Report {
            suite_name,
            run_id,
            duration_secs,
            checks,
            thresholds,
        }
    }

    /// Number of failing checks + failing thresholds.
    fn failure_count(&self) -> u64 {
        let check_failures = self.checks.iter().filter(|c| c.fails > 0).count() as u64;
        let threshold_failures = self.thresholds.iter().filter(|t| !t.passed).count() as u64;
        check_failures + threshold_failures
    }

    /// Total testcases written: one per check + one per threshold.
    fn test_count(&self) -> u64 {
        (self.checks.len() + self.thresholds.len()) as u64
    }

    /// Render the report as a single-`<testsuite>` JUnit document.
    fn render_junit(&self) -> String {
        let time = format!("{:.3}", self.duration_secs);
        let mut cases = String::new();

        for c in &self.checks {
            let name = format!("check: {}", c.name);
            if c.fails == 0 {
                cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"check\"/>\n",
                    xml_escape(&name)
                ));
            } else {
                let total = c.passes + c.fails;
                let msg = format!("{} of {} checks failed", c.fails, total);
                cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"check\">\n      <failure message=\"{}\"/>\n    </testcase>\n",
                    xml_escape(&name),
                    xml_escape(&msg)
                ));
            }
        }

        for t in &self.thresholds {
            let name = format!("threshold: {}: {}", t.metric, t.expression);
            if t.passed {
                cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"threshold\"/>\n",
                    xml_escape(&name)
                ));
            } else {
                let observed = t
                    .observed
                    .map(|v| format!("{v:.2}"))
                    .unwrap_or_else(|| "no samples".to_string());
                let msg = format!("threshold {} failed (observed: {})", t.expression, observed);
                cases.push_str(&format!(
                    "    <testcase name=\"{}\" classname=\"threshold\">\n      <failure message=\"{}\"/>\n    </testcase>\n",
                    xml_escape(&name),
                    xml_escape(&msg)
                ));
            }
        }

        let mut out = String::new();
        out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        out.push_str(&format!(
            "<testsuite name=\"loadr: {}\" tests=\"{}\" failures=\"{}\" time=\"{}\">\n",
            xml_escape(&self.suite_name),
            self.test_count(),
            self.failure_count(),
            time
        ));
        out.push_str(&format!(
            "  <properties>\n    <property name=\"run_id\" value=\"{}\"/>\n  </properties>\n",
            xml_escape(&self.run_id)
        ));
        out.push_str(&cases);
        out.push_str("</testsuite>\n");
        out
    }
}

/// Escape the five XML special characters for safe attribute/text content.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The output plugin instance.
// ---------------------------------------------------------------------------

struct JunitReport {
    factory: Box<dyn SinkFactory>,
    sink: Option<Box<dyn Sink>>,
}

impl Default for JunitReport {
    fn default() -> Self {
        JunitReport {
            factory: Box::new(FileSinkFactory),
            sink: None,
        }
    }
}

impl JunitReport {
    /// Construct with an explicit sink factory (used by tests to inject an
    /// in-memory sink).
    #[allow(dead_code)] // used only by tests
    fn with_factory(factory: Box<dyn SinkFactory>) -> Self {
        JunitReport {
            factory,
            sink: None,
        }
    }
}

impl FfiOutput for JunitReport {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg = match parse_config(config_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        match self.factory.open(&cfg.path) {
            Ok(sink) => {
                self.sink = Some(sink);
                ROk(())
            }
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, _snapshot_json: RString) {}

    fn finish(&mut self, summary_json: RString) {
        let report = Report::from_summary_json(summary_json.as_str());
        let xml = report.render_junit();
        if let Some(sink) = self.sink.as_mut() {
            let _ = sink.write_report(&xml);
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description":
                "Maps checks and thresholds to JUnit testcases and writes junit.xml",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(JunitReport::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RSome(make_output),
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RNone,
    }
}

// ---------------------------------------------------------------------------
// Tests — all offline; the report is rendered into an in-memory sink, never a
// real file or socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// A sink that appends everything written into a shared buffer.
    struct MockSink {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl Sink for MockSink {
        fn write_report(&mut self, xml: &str) -> Result<(), String> {
            self.buf.lock().unwrap().extend_from_slice(xml.as_bytes());
            Ok(())
        }
    }

    /// Hands out [`MockSink`]s over one shared buffer, counting opens so tests
    /// can assert the file was (re)opened exactly once.
    struct MockFactory {
        buf: Arc<Mutex<Vec<u8>>>,
        opens: Arc<AtomicUsize>,
    }

    impl MockFactory {
        #[allow(clippy::type_complexity)]
        fn build() -> (Box<dyn SinkFactory>, Arc<Mutex<Vec<u8>>>, Arc<AtomicUsize>) {
            let buf = Arc::new(Mutex::new(Vec::new()));
            let opens = Arc::new(AtomicUsize::new(0));
            let factory = MockFactory {
                buf: buf.clone(),
                opens: opens.clone(),
            };
            (Box::new(factory), buf, opens)
        }
    }

    impl SinkFactory for MockFactory {
        fn open(&self, _path: &str) -> Result<Box<dyn Sink>, String> {
            self.opens.fetch_add(1, Ordering::Relaxed);
            Ok(Box::new(MockSink {
                buf: self.buf.clone(),
            }))
        }
    }

    /// A factory whose open always fails, to exercise the `start` error path.
    struct FailingFactory;

    impl SinkFactory for FailingFactory {
        fn open(&self, _path: &str) -> Result<Box<dyn Sink>, String> {
            Err("disk on fire".to_string())
        }
    }

    fn captured(buf: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    }

    // -- config --------------------------------------------------------------

    #[test]
    fn config_defaults_path_when_missing() {
        let cfg = parse_config("{}").unwrap();
        assert_eq!(cfg.path, "junit.xml");
    }

    #[test]
    fn config_uses_given_path() {
        let cfg = parse_config(r#"{"path":"reports/out.xml"}"#).unwrap();
        assert_eq!(cfg.path, "reports/out.xml");
    }

    #[test]
    fn config_rejects_empty_path() {
        let err = parse_config(r#"{"path":""}"#).unwrap_err();
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn config_rejects_non_string_path() {
        let err = parse_config(r#"{"path":42}"#).unwrap_err();
        assert!(err.contains("string"), "{err}");
    }

    #[test]
    fn config_rejects_invalid_json() {
        let err = parse_config("{not json").unwrap_err();
        assert!(err.contains("invalid config JSON"), "{err}");
    }

    // -- summary parsing -----------------------------------------------------

    #[test]
    fn parses_checks_and_thresholds_from_summary() {
        let summary = r#"{
            "name": "checkout-load",
            "run_id": "run-42",
            "duration_secs": 12.5,
            "checks": [
                {"name": "status is 200", "passes": 10, "fails": 0},
                {"name": "has order_id", "passes": 7, "fails": 3}
            ],
            "thresholds": [
                {"metric": "http_req_duration", "expression": "p(95)<500", "observed": 420.0, "passed": true},
                {"metric": "http_req_failed", "expression": "rate<0.01", "observed": 0.05, "passed": false}
            ]
        }"#;
        let report = Report::from_summary_json(summary);
        assert_eq!(report.suite_name, "checkout-load");
        assert_eq!(report.run_id, "run-42");
        assert_eq!(report.duration_secs, 12.5);
        assert_eq!(report.checks.len(), 2);
        assert_eq!(
            report.checks[1],
            CheckCase {
                name: "has order_id".to_string(),
                passes: 7,
                fails: 3,
            }
        );
        assert_eq!(report.thresholds.len(), 2);
        assert!(report.thresholds[0].passed);
        assert!(!report.thresholds[1].passed);
        assert_eq!(report.test_count(), 4);
        assert_eq!(report.failure_count(), 2); // one failing check + one failing threshold
    }

    #[test]
    fn summary_defaults_when_fields_missing() {
        let report = Report::from_summary_json("{}");
        assert_eq!(report.suite_name, "loadr test");
        assert_eq!(report.run_id, "unknown");
        assert_eq!(report.duration_secs, 0.0);
        assert!(report.checks.is_empty());
        assert!(report.thresholds.is_empty());
        assert_eq!(report.test_count(), 0);
        assert_eq!(report.failure_count(), 0);
    }

    #[test]
    fn malformed_threshold_counts_as_failure() {
        // Missing `passed` must not read as green.
        let report =
            Report::from_summary_json(r#"{"thresholds":[{"metric":"m","expression":"e"}]}"#);
        assert_eq!(report.failure_count(), 1);
        assert_eq!(report.thresholds[0].observed, None);
    }

    // -- rendering -----------------------------------------------------------

    #[test]
    fn renders_passing_and_failing_cases() {
        let report = Report {
            suite_name: "checkout-load".to_string(),
            run_id: "run-42".to_string(),
            duration_secs: 3.0,
            checks: vec![
                CheckCase {
                    name: "status is 200".to_string(),
                    passes: 10,
                    fails: 0,
                },
                CheckCase {
                    name: "has order_id".to_string(),
                    passes: 7,
                    fails: 3,
                },
            ],
            thresholds: vec![ThresholdCase {
                metric: "http_req_failed".to_string(),
                expression: "rate<0.01".to_string(),
                observed: Some(0.05),
                passed: false,
            }],
        };
        let xml = report.render_junit();

        // Well-formed header + single suite with correct counts.
        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
        assert!(
            xml.contains("<testsuite name=\"loadr: checkout-load\" tests=\"3\" failures=\"2\" time=\"3.000\">"),
            "{xml}"
        );
        // run_id rides along as a property.
        assert!(
            xml.contains("<property name=\"run_id\" value=\"run-42\"/>"),
            "{xml}"
        );
        // A passing check is an empty testcase.
        assert!(
            xml.contains("<testcase name=\"check: status is 200\" classname=\"check\"/>"),
            "{xml}"
        );
        // A failing check carries a failure naming the counts.
        assert!(xml.contains("3 of 10 checks failed"), "{xml}");
        // A failing threshold names the expression and observed value.
        assert!(
            xml.contains("threshold rate&lt;0.01 failed (observed: 0.05)"),
            "{xml}"
        );
        assert!(xml.trim_end().ends_with("</testsuite>"), "{xml}");
    }

    #[test]
    fn escapes_xml_special_characters() {
        assert_eq!(
            xml_escape(r#"<a href="x">&'</a>"#),
            "&lt;a href=&quot;x&quot;&gt;&amp;&apos;&lt;/a&gt;"
        );
    }

    #[test]
    fn threshold_without_samples_says_no_samples() {
        let report = Report {
            thresholds: vec![ThresholdCase {
                metric: "http_req_duration".to_string(),
                expression: "p(95)<500".to_string(),
                observed: None,
                passed: false,
            }],
            ..Report::default()
        };
        let xml = report.render_junit();
        assert!(xml.contains("observed: no samples"), "{xml}");
    }

    // -- lifecycle -----------------------------------------------------------

    #[test]
    fn full_lifecycle_writes_report_to_sink() {
        let (factory, buf, opens) = MockFactory::build();
        let mut plugin = JunitReport::with_factory(factory);

        assert!(matches!(
            plugin.start(RString::from(r#"{"path":"out.xml"}"#)),
            ROk(())
        ));
        // No-ops mid-run write nothing.
        plugin.on_samples(RString::from("[]"));
        plugin.on_snapshot(RString::from(r#"{"series":[]}"#));
        assert!(captured(&buf).is_empty(), "nothing written before finish");

        plugin.finish(RString::from(
            r#"{"name":"t","run_id":"r1","duration_secs":1.0,"checks":[{"name":"ok","passes":1,"fails":0}],"thresholds":[]}"#,
        ));

        let out = captured(&buf);
        assert!(
            out.contains("<testsuite name=\"loadr: t\" tests=\"1\" failures=\"0\""),
            "{out}"
        );
        assert!(out.contains("check: ok"), "{out}");
        assert_eq!(opens.load(Ordering::Relaxed), 1, "opened once at start");
    }

    #[test]
    fn start_surfaces_open_failure() {
        let mut plugin = JunitReport::with_factory(Box::new(FailingFactory));
        let res = plugin.start(RString::from(r#"{"path":"out.xml"}"#));
        assert!(matches!(res, RErr(_)));
        assert!(plugin.sink.is_none());
        // finish without a sink must not panic.
        plugin.finish(RString::from("{}"));
    }

    #[test]
    fn start_rejects_empty_path_without_opening() {
        let (factory, _buf, opens) = MockFactory::build();
        let mut plugin = JunitReport::with_factory(factory);
        let res = plugin.start(RString::from(r#"{"path":""}"#));
        assert!(matches!(res, RErr(_)));
        assert_eq!(
            opens.load(Ordering::Relaxed),
            0,
            "never opened on bad config"
        );
    }

    #[test]
    fn info_declares_output_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "junit-report");
    }

    #[test]
    fn name_reports_plugin_name() {
        let plugin = JunitReport::default();
        assert_eq!(plugin.name().as_str(), "junit-report");
    }
}
