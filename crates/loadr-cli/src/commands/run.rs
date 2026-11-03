//! `loadr run` — standalone runs (optionally with the live web UI) and
//! submission to a distributed controller.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use owo_colors::OwoColorize;

#[derive(Args)]
pub struct RunArgs {
    /// Test definition file
    pub test: PathBuf,
    /// Environment override (`env.<name>` block)
    #[arg(short, long)]
    pub env: Option<String>,
    /// Override VU count (single-scenario tests only)
    #[arg(long)]
    pub vus: Option<u64>,
    /// Override duration (single-scenario tests only), e.g. `2m`
    #[arg(long)]
    pub duration: Option<String>,
    /// Serve the live web UI during the run
    #[arg(long)]
    pub ui: bool,
    /// Web UI bind address
    #[arg(long, default_value = "127.0.0.1:6464")]
    pub ui_bind: String,
    /// Write the end-of-run summary as JSON
    #[arg(long, value_name = "PATH")]
    pub summary_export: Option<PathBuf>,
    /// Write a JUnit XML report (thresholds + checks as testcases) for CI
    #[arg(long, value_name = "PATH")]
    pub junit: Option<PathBuf>,
    /// Extra output, `kind=value` (json=path, csv=path, prometheus=addr,
    /// influxdb=url,db, statsd=addr, otlp=endpoint). Repeatable.
    #[arg(long, value_name = "SPEC")]
    pub output: Vec<String>,
    /// Submit to a controller's API address (e.g. `controller-host:6464`)
    /// instead of running locally
    #[arg(long, value_name = "HOST:PORT")]
    pub controller: Option<String>,
    /// Plugins directory
    #[arg(long, env = "LOADR_PLUGINS_DIR")]
    pub plugins_dir: Option<PathBuf>,
    /// Run only scenarios that carry at least one of these tags (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,
    /// Skip scenarios that carry any of these tags (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub exclude_tags: Vec<String>,
    /// Dump full HTTP requests and responses (verbose; sets LOADR_HTTP_DEBUG).
    #[arg(long)]
    pub http_debug: bool,
    /// Tokio worker threads for the run (default: number of CPUs)
    #[arg(long, env = "LOADR_WORKER_THREADS")]
    pub worker_threads: Option<usize>,
}

pub fn execute(args: RunArgs, quiet: bool) -> anyhow::Result<i32> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.worker_threads {
        builder.worker_threads(n.max(1));
    }
    let runtime = builder.enable_all().build()?;
    runtime.block_on(async move {
        match &args.controller {
            Some(addr) => submit_remote(&args, addr).await,
            None => run_local(args, quiet).await,
        }
    })
}

/// Decide whether the plan needs a JS engine.
fn plan_uses_js(plan: &loadr_config::TestPlan) -> bool {
    if plan.js.is_some() {
        return true;
    }
    fn step_uses(step: &loadr_config::Step) -> bool {
        match step {
            loadr_config::Step::Js(_) => true,
            // `while`/`if` conditions are JS expressions.
            loadr_config::Step::While(_) | loadr_config::Step::If(_) => true,
            loadr_config::Step::Group(g) => g.steps.iter().any(step_uses),
            loadr_config::Step::Repeat(r) => r.steps.iter().any(step_uses),
            loadr_config::Step::Random(r) => {
                r.choices.iter().any(|c| c.steps.iter().any(step_uses))
            }
            loadr_config::Step::Retry(r) => r.until.is_some() || r.steps.iter().any(step_uses),
            loadr_config::Step::Foreach(f) => {
                f.items
                    .as_str()
                    .map(|s| s.contains("${js:"))
                    .unwrap_or(false)
                    || f.steps.iter().any(step_uses)
            }
            loadr_config::Step::Switch(sw) => {
                sw.value.contains("${js:")
                    || sw.cases.values().any(|st| st.iter().any(step_uses))
                    || sw.default.iter().any(step_uses)
            }
            loadr_config::Step::During(d) => d.steps.iter().any(step_uses),
            loadr_config::Step::Parallel(p) => p.branches.iter().any(|b| b.iter().any(step_uses)),
            loadr_config::Step::Rendezvous(_) => false,
            loadr_config::Step::Request(r) => {
                let text_has_js = |s: &str| s.contains("${js:");
                r.url.contains("${js:")
                    || r.headers.values().any(|v| text_has_js(v))
                    || r.params.values().any(|v| text_has_js(v))
                    || r.assert
                        .iter()
                        .chain(r.checks.iter())
                        .any(|c| matches!(c, loadr_config::Condition::Js { .. }))
            }
            loadr_config::Step::ThinkTime(_) => false,
        }
    }
    plan.scenarios
        .values()
        .any(|s| s.exec.is_some() || s.flow.iter().any(step_uses))
}

/// Build a fully wired engine for a plan (protocols, JS, outputs, plugins).
pub fn build_engine(
    plan: loadr_config::TestPlan,
    base_dir: PathBuf,
    run_id: Option<String>,
    extra_outputs: Vec<Box<dyn loadr_core::Output>>,
    plugins_dir: Option<&Path>,
) -> anyhow::Result<(
    loadr_core::Engine,
    Vec<Box<dyn loadr_plugin_api::ServicePlugin>>,
)> {
    let mut protocols = loadr_protocols::builtin_registry(&plan.defaults.http, &base_dir)
        .map_err(|e| anyhow::anyhow!("protocol setup failed: {e}"))?;

    // Browser protocol lives in its own crate (pulls in headless Chrome via CDP).
    // The handler is lazy — Chrome only launches on first `protocol: browser` use.
    protocols.register(std::sync::Arc::new(
        loadr_browser::BrowserHandler::from_config(&plan.defaults.http)
            .map_err(|e| anyhow::anyhow!("browser protocol setup failed: {e}"))?,
    ));

    // Plugins declared in the plan.
    let plugins_dir = plugins_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(loadr_plugin_api::default_plugins_dir);
    let mut outputs = extra_outputs;
    let mut services: Vec<Box<dyn loadr_plugin_api::ServicePlugin>> = Vec::new();
    let mut data_sources: HashMap<String, Box<dyn loadr_core::DataSourcePlugin>> = HashMap::new();
    for plugin_ref in &plan.plugins {
        if !plugin_ref.enabled {
            continue;
        }
        let loaded = loadr_plugin_api::PluginRegistry::load_ref(plugin_ref, &plugins_dir)
            .map_err(|e| anyhow::anyhow!("plugin `{}`: {e}", plugin_ref.name))?;
        match loaded {
            loadr_plugin_api::LoadedPlugin::Protocol(handler) => protocols.register(handler),
            loadr_plugin_api::LoadedPlugin::Output(output) => outputs.push(output),
            loadr_plugin_api::LoadedPlugin::Service {
                service,
                data_source,
            } => {
                if let Some(service) = service {
                    services.push(service);
                }
                if let Some(data_source) = data_source {
                    // Keyed by the plan's `plugins:` name, not the plugin's
                    // self-reported `info().name` -- that's what
                    // `data.<name>.source` and validation refer to.
                    data_sources.insert(plugin_ref.name.clone(), data_source);
                }
            }
            loadr_plugin_api::LoadedPlugin::Extractor(_)
            | loadr_plugin_api::LoadedPlugin::Assertion(_) => {
                tracing::info!(
                    plugin = %plugin_ref.name,
                    "extractor/assertion plugin loaded (used via `type: plugin` extract/assert entries)"
                );
            }
        }
    }

    // Built-in outputs from the plan.
    outputs.extend(
        loadr_outputs::build_outputs(&plan.outputs, &base_dir)
            .map_err(|e| anyhow::anyhow!("output setup failed: {e}"))?,
    );

    // JS engine when needed.
    let script: Option<Arc<dyn loadr_core::ScriptEngine>> = if plan_uses_js(&plan) {
        let default_cfg = loadr_config::JsConfig {
            script: Some(String::new()),
            ..Default::default()
        };
        let cfg = plan.js.clone().unwrap_or(default_cfg);
        Some(Arc::new(
            loadr_js::JsEngine::new(&cfg, &base_dir)
                .map_err(|e| anyhow::anyhow!("JS setup failed: {e}"))?,
        ))
    } else {
        None
    };

    let engine = loadr_core::Engine::new(
        plan,
        base_dir,
        loadr_core::EngineOptions {
            run_id,
            protocols,
            script,
            outputs,
            data_sources,
            ..Default::default()
        },
    )?;
    Ok((engine, services))
}

async fn run_local(args: RunArgs, quiet: bool) -> anyhow::Result<i32> {
    let mut opts = loadr_config::LoadOptions::new();
    opts.env = args.env.clone();
    opts.check_files = true;
    let loaded = match loadr_config::load_file(&args.test, &opts) {
        Ok(l) => l,
        Err(loadr_config::ConfigError::Invalid(diags)) => {
            for d in &diags {
                eprintln!("{}", d.to_string().red());
            }
            anyhow::bail!("test definition is invalid");
        }
        Err(loadr_config::ConfigError::Deserialize(d)) => {
            eprintln!("{}", d.to_string().red());
            anyhow::bail!("test definition failed to parse");
        }
        Err(e) => return Err(e.into()),
    };
    for d in &loaded.diagnostics {
        if d.severity == loadr_config::Severity::Warning && !quiet {
            eprintln!("{}", d.to_string().yellow());
        }
    }
    let mut plan = loaded.plan;

    // --http-debug: the HTTP handler reads this env var.
    if args.http_debug {
        std::env::set_var("LOADR_HTTP_DEBUG", "1");
    }

    // --tags / --exclude-tags: keep only scenarios matching the tag filter.
    if !args.tags.is_empty() || !args.exclude_tags.is_empty() {
        let before = plan.scenarios.len();
        plan.scenarios.retain(|_, s| {
            let tags: std::collections::BTreeSet<&str> =
                s.tags.values().map(String::as_str).collect();
            let included =
                args.tags.is_empty() || args.tags.iter().any(|t| tags.contains(t.as_str()));
            let excluded = args.exclude_tags.iter().any(|t| tags.contains(t.as_str()));
            included && !excluded
        });
        if plan.scenarios.is_empty() {
            anyhow::bail!(
                "no scenarios match the tag filter (had {before}); checked --tags {:?} / --exclude-tags {:?}",
                args.tags,
                args.exclude_tags
            );
        }
        if !quiet {
            eprintln!(
                "{} running {} of {before} scenario(s) after tag filter",
                "→".cyan(),
                plan.scenarios.len()
            );
        }
    }

    // CLI load overrides.
    if args.vus.is_some() || args.duration.is_some() {
        if plan.scenarios.len() != 1 {
            anyhow::bail!("--vus/--duration require a single-scenario test");
        }
        let scenario = plan.scenarios.values_mut().next().expect("one scenario");
        if let Some(v) = args.vus {
            scenario.vus = Some(v);
        }
        if let Some(d) = &args.duration {
            scenario.duration = Some(loadr_config::Dur::parse(d).map_err(|e| anyhow::anyhow!(e))?);
        }
    }

    // Ad-hoc outputs.
    let mut extra_outputs: Vec<Box<dyn loadr_core::Output>> = Vec::new();
    if !args.output.is_empty() {
        let mut configs = Vec::new();
        for spec in &args.output {
            configs
                .push(crate::output_flag::parse_output_flag(spec).map_err(|e| anyhow::anyhow!(e))?);
        }
        extra_outputs.extend(
            loadr_outputs::build_outputs(&configs, &loaded.base_dir)
                .map_err(|e| anyhow::anyhow!(e))?,
        );
    }

    // Capture observe (system-metric correlation) config + thresholds before the
    // plan is moved into the engine; collection happens post-run against the
    // summary, and observe-metric thresholds are evaluated then too.
    let observe_cfg = plan.observe.clone();
    let plan_thresholds = plan.thresholds.clone();

    let (engine, mut services) = build_engine(
        plan,
        loaded.base_dir.clone(),
        None,
        extra_outputs,
        args.plugins_dir.as_deref(),
    )?;
    let handle = engine.handle();

    // Optional live web UI for this run.
    let mut ui_handle = None;
    if args.ui {
        let backend = Arc::new(SingleRunBackend {
            handle: handle.clone(),
            name: args.test.display().to_string(),
            yaml: std::fs::read_to_string(&args.test).unwrap_or_default(),
            summary: parking_lot::Mutex::new(None),
            started_ms: loadr_core::metrics::now_millis(),
        });
        let config = loadr_plugin_webui::WebUiConfig {
            bind: args.ui_bind.parse()?,
            auth: Default::default(),
            backend: backend.clone(),
        };
        let served = loadr_plugin_webui::WebUi::serve(config).await?;
        eprintln!(
            "{} web UI at http://{}/ (run page: /#/runs/{})",
            "→".cyan(),
            served.addr,
            handle.run_id
        );
        ui_handle = Some((served, backend));
    }

    // Ctrl-C: first = graceful stop, second = kill.
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!(
                    "\n{} stopping gracefully (Ctrl-C again to abort)",
                    "!".yellow()
                );
                handle.stop("interrupted by user");
                if tokio::signal::ctrl_c().await.is_ok() {
                    handle.kill("interrupted twice");
                    std::process::exit(130);
                }
            }
        });
    }

    let progress = if quiet {
        None
    } else {
        Some(tokio::spawn(crate::progress::show_progress(handle.clone())))
    };

    // `type: system` observe sources sample local /proc live — start them now.
    let system_samplers = loadr_outputs::observe::start_samplers(&observe_cfg);

    let mut result = engine.run().await?;

    if let Some(p) = progress {
        p.abort();
        eprintln!();
    }

    // Stop the system samplers and take their series (may be empty).
    let system_series = loadr_outputs::observe::stop_samplers(system_samplers);

    // observe: pull system metrics for the run window and overlay them on the
    // timeline so the report shows load↔system correlation. Best-effort — a
    // failing source never fails the run.
    if !observe_cfg.is_empty() && !result.summary.timeline.is_empty() {
        let start_ms = result.summary.started_ms as i64;
        let end_ms = result.summary.ended_ms as i64;
        let step = loadr_outputs::observe::step_for(&result.summary.timeline);
        let mut series =
            loadr_outputs::observe::collect(&observe_cfg, start_ms, end_ms, step).await;
        series.extend(system_series);
        if !series.is_empty() {
            loadr_outputs::observe::attach(&mut result.summary, &series);
            eprintln!(
                "{} observed {} system-metric series for correlation",
                "✓".green(),
                series.len()
            );

            // Evaluate thresholds that target an observed metric (post-run gate
            // on target health). Replace the engine's no-sample placeholders for
            // those metrics, then recompute pass/fail + the exit code.
            let observed_thresholds =
                loadr_outputs::observe::evaluate_thresholds(&plan_thresholds, &series);
            if !observed_thresholds.is_empty() {
                result.summary.thresholds.retain(|t| {
                    !observed_thresholds
                        .iter()
                        .any(|o| o.metric == t.metric && o.expression == t.expression)
                });
                result.summary.thresholds.extend(observed_thresholds);
                result.summary.thresholds_passed =
                    result.summary.thresholds.iter().all(|t| t.passed);
                result.passed = result.summary.thresholds_passed;
            }
        }
    }
    // A JS handleSummary() return value replaces the default console summary.
    if let Some(custom) = &result.custom_summary {
        print!("{custom}");
        if !custom.ends_with('\n') {
            println!();
        }
    } else {
        print!("{}", colorize_summary(&result.summary.render_console()));
    }

    if let Some(path) = &args.summary_export {
        std::fs::write(path, serde_json::to_string_pretty(&result.summary)?)?;
        eprintln!("{} summary exported to {}", "✓".green(), path.display());
    }
    if let Some(path) = &args.junit {
        std::fs::write(path, result.summary.render_junit())?;
        eprintln!("{} JUnit report written to {}", "✓".green(), path.display());
    }
    if let Some((served, backend)) = ui_handle {
        *backend.summary.lock() = Some(result.summary.clone());
        eprintln!(
            "{} web UI still serving results at http://{}/ — Ctrl-C to exit",
            "→".cyan(),
            served.addr
        );
        let _ = tokio::signal::ctrl_c().await;
        served.shutdown().await;
    }
    for service in &mut services {
        service.stop();
    }

    Ok(exit_code(&result))
}

fn exit_code(result: &loadr_core::RunResult) -> i32 {
    if result.passed && result.aborted.is_none() {
        0
    } else {
        loadr_core::EXIT_THRESHOLD_FAILED
    }
}

fn colorize_summary(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.trim_start().starts_with('✓') {
                format!("{}\n", line.green())
            } else if line.trim_start().starts_with('✗') || line.trim_start().starts_with('!') {
                format!("{}\n", line.red())
            } else {
                format!("{line}\n")
            }
        })
        .collect()
}

/// Submit the test to a controller's REST API and stream its progress.
async fn submit_remote(args: &RunArgs, controller: &str) -> anyhow::Result<i32> {
    let yaml = std::fs::read_to_string(&args.test)?;
    let base = if controller.starts_with("http") {
        controller.trim_end_matches('/').to_string()
    } else {
        format!("http://{controller}")
    };
    let client = crate::commands::controller::http_client();

    let body = serde_json::json!({
        "name": args.test.file_stem().and_then(|s| s.to_str()),
        "yaml": yaml,
        "env": args.env,
    });
    let resp = crate::commands::controller::http_json(
        &client,
        http::Method::POST,
        &format!("{base}/api/runs"),
        Some(&body),
    )
    .await?;
    let run_id = resp["run_id"]
        .as_str()
        .or_else(|| resp["id"].as_str())
        .ok_or_else(|| anyhow::anyhow!("controller did not return a run id: {resp}"))?
        .to_string();
    eprintln!("{} submitted run {run_id} to {base}", "→".cyan());

    // Poll until finished.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let info = crate::commands::controller::http_json(
            &client,
            http::Method::GET,
            &format!("{base}/api/runs/{run_id}"),
            None,
        )
        .await?;
        let state = info["run"]["state"]
            .as_str()
            .or_else(|| info["state"].as_str())
            .unwrap_or("unknown");
        match state {
            "finished" | "degraded" | "aborted" | "failed" => {
                let summary = crate::commands::controller::http_json(
                    &client,
                    http::Method::GET,
                    &format!("{base}/api/runs/{run_id}/summary"),
                    None,
                )
                .await?;
                if let Ok(summary) = serde_json::from_value::<loadr_core::Summary>(summary) {
                    print!("{}", colorize_summary(&summary.render_console()));
                    if let Some(path) = &args.summary_export {
                        std::fs::write(path, serde_json::to_string_pretty(&summary)?)?;
                    }
                    if let Some(path) = &args.junit {
                        std::fs::write(path, summary.render_junit())?;
                    }
                    let passed = summary.thresholds_passed && summary.aborted.is_none();
                    return Ok(if passed && state == "finished" {
                        0
                    } else {
                        loadr_core::EXIT_THRESHOLD_FAILED
                    });
                }
                return Ok(if state == "finished" { 0 } else { 1 });
            }
            _ => {
                eprint!("\r  remote run: {state}        ");
            }
        }
    }
}

/// Minimal UiBackend exposing exactly one externally-managed run.
struct SingleRunBackend {
    handle: loadr_core::RunHandle,
    name: String,
    yaml: String,
    summary: parking_lot::Mutex<Option<loadr_core::Summary>>,
    started_ms: u64,
}

#[async_trait::async_trait]
impl loadr_plugin_webui::UiBackend for SingleRunBackend {
    async fn start_test(
        &self,
        _name: Option<String>,
        _yaml: String,
        _env: Option<String>,
    ) -> Result<String, String> {
        Err("this UI is attached to a single `loadr run` invocation; use `loadr controller` to launch tests from the UI".into())
    }

    fn runs(&self) -> Vec<loadr_plugin_webui::RunInfo> {
        let (mut state, mut passed) = match self.handle.status() {
            loadr_core::RunStatus::Pending => ("pending", None),
            loadr_core::RunStatus::Running => ("running", None),
            loadr_core::RunStatus::Stopping => ("stopping", None),
            loadr_core::RunStatus::Finished { passed } => ("finished", Some(passed)),
        };
        let summary = self.summary.lock();
        if summary
            .as_ref()
            .is_some_and(|summary| summary.aborted.is_some())
        {
            state = "aborted";
            passed = Some(false);
        }
        if state == "finished"
            && summary.as_ref().is_none_or(|summary| {
                summary.thresholds.is_empty()
                    || summary
                        .thresholds
                        .iter()
                        .any(|threshold| threshold.observed.is_none())
            })
        {
            passed = None;
        }
        vec![loadr_plugin_webui::RunInfo {
            run_id: self.handle.run_id.to_string(),
            name: Some(self.name.clone()),
            state: state.to_string(),
            passed,
            started_ms: self.started_ms,
            ended_ms: summary.as_ref().map(|s| s.ended_ms),
            observed_ms: loadr_core::metrics::now_millis(),
            scenarios: summary
                .as_ref()
                .map(|s| s.scenarios.clone())
                .unwrap_or_default(),
            agents: Vec::new(),
            contributing_agents: Vec::new(),
            lost_agents: Vec::new(),
            complete: None,
            on_agent_loss: None,
        }]
    }

    fn run_handle(&self, run_id: &str) -> Option<loadr_core::RunHandle> {
        (run_id == &*self.handle.run_id).then(|| self.handle.clone())
    }

    fn run_snapshot(&self, run_id: &str) -> Option<Arc<loadr_core::Snapshot>> {
        (run_id == &*self.handle.run_id).then(|| self.handle.snapshot())
    }

    fn run_thresholds(&self, run_id: &str) -> Vec<loadr_core::ThresholdStatus> {
        if run_id == &*self.handle.run_id {
            self.handle.threshold_statuses().as_ref().clone()
        } else {
            Vec::new()
        }
    }

    fn run_summary(&self, run_id: &str) -> Option<loadr_core::Summary> {
        (run_id == &*self.handle.run_id)
            .then(|| self.summary.lock().clone())
            .flatten()
    }

    async fn stop_run(&self, run_id: &str, kill: bool) -> Result<(), String> {
        if run_id != &*self.handle.run_id {
            return Err("unknown run".into());
        }
        if kill {
            self.handle.kill("killed from web UI");
        } else {
            self.handle.stop("stopped from web UI");
        }
        Ok(())
    }

    async fn pause_run(&self, run_id: &str, paused: bool) -> Result<(), String> {
        if run_id != &*self.handle.run_id {
            return Err("unknown run".into());
        }
        self.handle.pause(paused);
        Ok(())
    }

    async fn scale_run(&self, run_id: &str, scenario: &str, vus: u64) -> Result<(), String> {
        if run_id != &*self.handle.run_id {
            return Err("unknown run".into());
        }
        self.handle.scale(scenario, vus)
    }

    fn agents(&self) -> Vec<loadr_plugin_webui::AgentView> {
        Vec::new()
    }

    fn tests(&self) -> Vec<loadr_plugin_webui::StoredTest> {
        vec![loadr_plugin_webui::StoredTest {
            name: self.name.clone(),
            yaml: self.yaml.clone(),
            updated_ms: self.started_ms,
        }]
    }

    fn save_test(&self, _name: String, _yaml: String) -> Result<(), String> {
        Err("read-only in single-run mode".into())
    }

    fn delete_test(&self, _name: &str) -> Result<(), String> {
        Err("read-only in single-run mode".into())
    }

    fn recent_logs(&self) -> Vec<loadr_plugin_webui::LogLine> {
        Vec::new()
    }

    fn capabilities(&self) -> loadr_plugin_webui::UiCapabilities {
        loadr_plugin_webui::UiCapabilities {
            mode: "single_run".to_string(),
            can_start_runs: false,
            can_edit_tests: false,
            logs_available: false,
            persistent_history: false,
        }
    }
}
