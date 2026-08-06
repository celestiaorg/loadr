//! Scenario compilation and the per-iteration flow interpreter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use loadr_config::{
    Body, DropMode, FailureAction, FaultDistribution, FaultSpec, GroupStep, HttpDefaults, JsStep,
    LatencyFault, RequestStep, Scenario, Step, Template, TestPlan, ThinkTimeSpec, WsMessage,
};

use crate::conditions::{CompiledCondition, ConditionResult};
use crate::error::EngineError;
use crate::extract::{CompiledExtractor, ExtractError};
use crate::metrics::{BuiltinMetrics, MetricKind, MetricRegistry, MetricsBus, Tags};
use crate::pacing::sample_think_time;
use crate::protocol::{
    GrpcRequest, PreparedRequest, ProtocolRegistry, ProtocolResponse, RequestOptions,
    SocketRequest, Timings, WsFrame, WsRequest,
};
use crate::script::{HostHttpRequest, HostHttpResponse, ScriptHost, ScriptLogLevel, VuScript};
use crate::vu::{json_to_string, VuContext};

/// Outcome of one iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IterationOutcome {
    /// Completed (possibly with failed requests — those are samples, not errors).
    Completed,
    /// A `stop`-mode data source ran out: retire this VU.
    StopVu,
    /// An assertion with `abort_scenario` failed.
    AbortScenario,
    /// An assertion with `abort_test` failed.
    AbortTest(String),
}

/// A compiled scenario: templates parsed, regexes compiled, files loaded.
pub struct ScenarioProgram {
    pub name: Arc<str>,
    pub steps: Vec<CompiledStep>,
    /// Exported JS function to call each iteration (after `steps`).
    pub exec: Option<String>,
    /// Exported JS function run once per VU before its first iteration.
    pub on_start: Option<String>,
    /// Exported JS function run once per VU after its last iteration.
    pub on_stop: Option<String>,
    /// Default think time inserted after each request step.
    pub think_time: Option<ThinkTimeSpec>,
    /// Scenario-wide target iteration starts per second (per-VU pacing).
    pub pacing: Option<f64>,
    /// Global request-rate ceiling (Gatling throttle), shared across all VUs.
    pub throttle: Option<Arc<Throttle>>,
    /// Chaos fault injection: extra latency and/or dropped requests.
    pub faults: Option<FaultSpec>,
    /// Named rendezvous barriers, created lazily and shared across VUs.
    pub barriers: parking_lot::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Barrier>>>,
    /// scenario + global tags.
    pub tags: Arc<Tags>,
    pub http: Arc<HttpDefaults>,
    pub cookies_auto: bool,
}

/// A token-bucket-style request-rate limiter shared across a scenario's VUs.
/// Each acquisition is granted a slot exactly `1/rps` after the previous one,
/// so the global request rate never exceeds `rps`.
pub struct Throttle {
    interval: Duration,
    next_slot: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl Throttle {
    pub fn new(requests_per_second: f64) -> Self {
        let rps = requests_per_second.max(1e-9);
        Throttle {
            interval: Duration::from_secs_f64(1.0 / rps),
            next_slot: parking_lot::Mutex::new(None),
        }
    }

    /// Reserve the next slot and return how long to wait before using it.
    fn reserve(&self) -> Duration {
        let now = std::time::Instant::now();
        let mut guard = self.next_slot.lock();
        let slot = match *guard {
            Some(t) if t > now => t,
            _ => now,
        };
        *guard = Some(slot + self.interval);
        slot.saturating_duration_since(now)
    }

    /// Block (async) until a request slot is available.
    pub async fn acquire(&self) {
        let wait = self.reserve();
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

pub enum CompiledStep {
    Request(Box<CompiledRequest>),
    ThinkTime(ThinkTimeSpec),
    Js {
        call: Option<String>,
        script: Option<String>,
    },
    Group {
        name: String,
        steps: Vec<CompiledStep>,
    },
    Repeat {
        times: u64,
        counter: Option<String>,
        steps: Vec<CompiledStep>,
    },
    While {
        condition: String,
        max_iterations: u64,
        steps: Vec<CompiledStep>,
    },
    If {
        condition: String,
        then: Vec<CompiledStep>,
        otherwise: Vec<CompiledStep>,
    },
    Random {
        strategy: loadr_config::SwitchStrategy,
        choices: Vec<CompiledChoice>,
        /// Round-robin cursor (per VU, but the program is shared, so this is a
        /// per-iteration fallback — actual round-robin state lives on the VU).
        round_robin: std::sync::atomic::AtomicU64,
    },
    Foreach {
        items: serde_json::Value,
        var: String,
        index: String,
        steps: Vec<CompiledStep>,
    },
    Switch {
        value: Template,
        cases: Vec<(String, Vec<CompiledStep>)>,
        default: Vec<CompiledStep>,
    },
    During {
        duration: Duration,
        counter: String,
        steps: Vec<CompiledStep>,
    },
    Retry {
        times: u64,
        until: Option<String>,
        backoff: Option<Duration>,
        steps: Vec<CompiledStep>,
    },
    Parallel {
        branches: Vec<Vec<CompiledStep>>,
    },
    Rendezvous {
        name: String,
        users: u64,
        timeout: Duration,
    },
}

pub struct CompiledChoice {
    pub weight: f64,
    pub name: Option<String>,
    pub steps: Vec<CompiledStep>,
}

pub struct CompiledRequest {
    /// Metric `name` tag (template); falls back to the raw URL string.
    pub name: Option<Template>,
    pub display_name: String,
    pub protocol: String,
    pub method: String,
    pub url: Template,
    pub headers: Vec<(String, Template)>,
    pub params: Vec<(String, Template)>,
    pub body: CompiledBody,
    pub timeout: Option<Duration>,
    pub follow_redirects: Option<bool>,
    pub tags: Tags,
    pub extract: Vec<CompiledExtractor>,
    pub assert: Vec<CompiledCondition>,
    pub checks: Vec<CompiledCondition>,
    pub ws: Option<loadr_config::WsOptions>,
    pub grpc: Option<loadr_config::GrpcOptions>,
    /// Template-free gRPC unary message retained for all iterations.
    pub grpc_literal_message: Option<Arc<serde_json::Value>>,
    /// Template-free gRPC streaming messages retained for all iterations.
    pub grpc_literal_messages: Option<Arc<Vec<serde_json::Value>>>,
    pub graphql: Option<loadr_config::GraphqlOptions>,
    pub socket: Option<loadr_config::SocketOptions>,
    pub sse: Option<loadr_config::SseOptions>,
    pub sql: Option<loadr_config::SqlOptions>,
    /// Free-form protocol-plugin options, interpolated and forwarded as
    /// `options.plugin`.
    pub plugin: Option<serde_json::Value>,
}

pub enum CompiledBody {
    None,
    Text(Template),
    Json(serde_json::Value),
    /// File contents, loaded at compile time.
    Bytes(Bytes),
    Form(Vec<(String, Template)>),
    Multipart(Vec<CompiledPart>),
}

pub struct CompiledPart {
    pub name: String,
    pub value: Option<Template>,
    pub bytes: Option<Bytes>,
    pub filename: Option<String>,
    pub content_type: Option<String>,
}

impl ScenarioProgram {
    pub fn compile(
        plan: &TestPlan,
        scenario_name: &str,
        scenario: &Scenario,
        base_dir: &std::path::Path,
    ) -> Result<ScenarioProgram, EngineError> {
        let mut tags = Tags::new();
        for (k, v) in &plan.defaults.tags {
            tags.insert(k.clone(), v.clone());
        }
        for (k, v) in &scenario.tags {
            tags.insert(k.clone(), v.clone());
        }
        tags.insert("scenario".to_string(), scenario_name.to_string());

        let steps = compile_steps(&scenario.flow, base_dir)?;
        Ok(ScenarioProgram {
            name: Arc::from(scenario_name),
            steps,
            exec: scenario.exec.clone(),
            on_start: scenario.on_start.clone(),
            on_stop: scenario.on_stop.clone(),
            think_time: scenario.think_time.or(plan.defaults.think_time),
            pacing: scenario.pacing.map(|p| p.iterations_per_second),
            throttle: scenario
                .throttle
                .map(|t| Arc::new(Throttle::new(t.requests_per_second))),
            faults: scenario.faults,
            barriers: parking_lot::Mutex::new(std::collections::HashMap::new()),
            tags: Arc::new(tags),
            http: Arc::new(plan.defaults.http.clone()),
            cookies_auto: plan.defaults.http.cookies,
        })
    }
}

fn compile_steps(
    steps: &[Step],
    base_dir: &std::path::Path,
) -> Result<Vec<CompiledStep>, EngineError> {
    steps
        .iter()
        .map(|step| {
            Ok(match step {
                Step::Request(req) => {
                    CompiledStep::Request(Box::new(compile_request(req, base_dir)?))
                }
                Step::ThinkTime(tt) => CompiledStep::ThinkTime(*tt),
                Step::Js(js) => match js {
                    JsStep::Code(code) => CompiledStep::Js {
                        call: None,
                        script: Some(code.clone()),
                    },
                    JsStep::Detailed { call, script } => CompiledStep::Js {
                        call: call.clone(),
                        script: script.clone(),
                    },
                },
                Step::Group(GroupStep { name, steps }) => CompiledStep::Group {
                    name: name.clone(),
                    steps: compile_steps(steps, base_dir)?,
                },
                Step::Repeat(r) => CompiledStep::Repeat {
                    times: r.times,
                    counter: r.counter.clone(),
                    steps: compile_steps(&r.steps, base_dir)?,
                },
                Step::While(w) => CompiledStep::While {
                    condition: w.condition.clone(),
                    max_iterations: w.max_iterations.unwrap_or(10_000),
                    steps: compile_steps(&w.steps, base_dir)?,
                },
                Step::If(c) => CompiledStep::If {
                    condition: c.condition.clone(),
                    then: compile_steps(&c.then, base_dir)?,
                    otherwise: compile_steps(&c.otherwise, base_dir)?,
                },
                Step::Random(r) => CompiledStep::Random {
                    strategy: r.strategy,
                    choices: r
                        .choices
                        .iter()
                        .map(|c| {
                            Ok(CompiledChoice {
                                weight: c.weight.unwrap_or(1.0),
                                name: c.name.clone(),
                                steps: compile_steps(&c.steps, base_dir)?,
                            })
                        })
                        .collect::<Result<Vec<_>, EngineError>>()?,
                    round_robin: std::sync::atomic::AtomicU64::new(0),
                },
                Step::Foreach(f) => CompiledStep::Foreach {
                    items: f.items.clone(),
                    var: f.var.clone().unwrap_or_else(|| "item".to_string()),
                    index: f.index.clone().unwrap_or_else(|| "index".to_string()),
                    steps: compile_steps(&f.steps, base_dir)?,
                },
                Step::Switch(s) => CompiledStep::Switch {
                    value: parse_template(&s.value, "switch value")?,
                    cases: s
                        .cases
                        .iter()
                        .map(|(k, steps)| Ok((k.clone(), compile_steps(steps, base_dir)?)))
                        .collect::<Result<Vec<_>, EngineError>>()?,
                    default: compile_steps(&s.default, base_dir)?,
                },
                Step::During(d) => CompiledStep::During {
                    duration: d.duration.as_duration(),
                    counter: d.counter.clone().unwrap_or_else(|| "index".to_string()),
                    steps: compile_steps(&d.steps, base_dir)?,
                },
                Step::Retry(r) => CompiledStep::Retry {
                    times: r.times.unwrap_or(3),
                    until: r.until.clone(),
                    backoff: r.backoff.map(|d| d.as_duration()),
                    steps: compile_steps(&r.steps, base_dir)?,
                },
                Step::Parallel(p) => CompiledStep::Parallel {
                    branches: p
                        .branches
                        .iter()
                        .map(|b| compile_steps(b, base_dir))
                        .collect::<Result<Vec<_>, EngineError>>()?,
                },
                Step::Rendezvous(r) => CompiledStep::Rendezvous {
                    name: r.name.clone(),
                    users: r.users,
                    timeout: r
                        .timeout
                        .map(|d| d.as_duration())
                        .unwrap_or(Duration::from_secs(30)),
                },
            })
        })
        .collect()
}

fn parse_template(s: &str, what: &str) -> Result<Template, EngineError> {
    Template::parse(s).map_err(|e| EngineError::Config(format!("{what}: {e}")))
}

fn compile_request(
    req: &RequestStep,
    base_dir: &std::path::Path,
) -> Result<CompiledRequest, EngineError> {
    let read_file = |path: &std::path::Path| -> Result<Bytes, EngineError> {
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            base_dir.join(path)
        };
        std::fs::read(&resolved)
            .map(Bytes::from)
            .map_err(|e| EngineError::Io {
                path: resolved.display().to_string(),
                source: e,
            })
    };

    let body = match &req.body {
        None => CompiledBody::None,
        Some(Body::Text(t)) => CompiledBody::Text(parse_template(t, "body")?),
        Some(Body::Spec(spec)) => {
            if let Some(json) = &spec.json {
                CompiledBody::Json(json.clone())
            } else if let Some(file) = &spec.file {
                CompiledBody::Bytes(read_file(file)?)
            } else if let Some(form) = &spec.form {
                CompiledBody::Form(
                    form.iter()
                        .map(|(k, v)| Ok((k.clone(), parse_template(v, "form value")?)))
                        .collect::<Result<_, EngineError>>()?,
                )
            } else if let Some(parts) = &spec.multipart {
                CompiledBody::Multipart(
                    parts
                        .iter()
                        .map(|p| {
                            Ok(CompiledPart {
                                name: p.name.clone(),
                                value: p
                                    .value
                                    .as_ref()
                                    .map(|v| parse_template(v, "multipart value"))
                                    .transpose()?,
                                bytes: p.file.as_ref().map(|f| read_file(f)).transpose()?,
                                filename: p.filename.clone().or_else(|| {
                                    p.file.as_ref().and_then(|f| {
                                        f.file_name().map(|n| n.to_string_lossy().to_string())
                                    })
                                }),
                                content_type: p.content_type.clone(),
                            })
                        })
                        .collect::<Result<_, EngineError>>()?,
                )
            } else {
                CompiledBody::None
            }
        }
    };

    let protocol = ProtocolRegistry::infer(req.protocol.as_deref(), &req.url);
    let method = req
        .method
        .clone()
        .unwrap_or_else(|| {
            if req.graphql.is_some() || matches!(body, CompiledBody::None) {
                if req.graphql.is_some() { "POST" } else { "GET" }.to_string()
            } else {
                "POST".to_string()
            }
        })
        .to_ascii_uppercase();

    // Resolve gRPC proto file paths against the test definition directory.
    let grpc = req.grpc.clone().map(|mut g| {
        let resolve = |p: &std::path::PathBuf| {
            if p.is_absolute() {
                p.clone()
            } else {
                base_dir.join(p)
            }
        };
        g.proto_files = g.proto_files.iter().map(&resolve).collect();
        g.proto_includes = g.proto_includes.iter().map(&resolve).collect();
        g
    });
    // Detect template-free gRPC messages once; `prepare` then skips the
    // per-iteration render walk and hands out these Arcs unchanged.
    let grpc_literal_message = grpc
        .as_ref()
        .and_then(|g| g.message.as_ref())
        .filter(|m| json_is_literal(m))
        .cloned()
        .map(Arc::new);
    let grpc_literal_messages = grpc
        .as_ref()
        .filter(|g| !g.messages.is_empty() && g.messages.iter().all(json_is_literal))
        .map(|g| Arc::new(g.messages.clone()));

    Ok(CompiledRequest {
        name: req
            .name
            .as_ref()
            .map(|n| parse_template(n, "request name"))
            .transpose()?,
        display_name: req.name.clone().unwrap_or_else(|| req.url.clone()),
        protocol,
        method,
        url: parse_template(&req.url, "url")?,
        headers: req
            .headers
            .iter()
            .map(|(k, v)| Ok((k.clone(), parse_template(v, "header")?)))
            .collect::<Result<_, EngineError>>()?,
        params: req
            .params
            .iter()
            .map(|(k, v)| Ok((k.clone(), parse_template(v, "param")?)))
            .collect::<Result<_, EngineError>>()?,
        body,
        timeout: req.timeout.map(|d| d.as_duration()),
        follow_redirects: req.follow_redirects,
        tags: req
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        extract: req
            .extract
            .iter()
            .map(|e| CompiledExtractor::compile(e).map_err(|e| EngineError::Config(e.to_string())))
            .collect::<Result<_, _>>()?,
        assert: req
            .assert
            .iter()
            .map(|c| CompiledCondition::compile(c).map_err(EngineError::Config))
            .collect::<Result<_, _>>()?,
        checks: req
            .checks
            .iter()
            .map(|c| CompiledCondition::compile(c).map_err(EngineError::Config))
            .collect::<Result<_, _>>()?,
        ws: req.ws.clone(),
        grpc,
        grpc_literal_message,
        grpc_literal_messages,
        graphql: req.graphql.clone(),
        socket: req.socket.clone(),
        sse: req.sse.clone(),
        sql: req.sql.clone(),
        plugin: req.plugin.clone(),
    })
}

/// Executes iterations of one scenario for one VU at a time.
pub struct FlowRunner {
    pub program: Arc<ScenarioProgram>,
    pub protocols: Arc<ProtocolRegistry>,
    pub builtins: Arc<BuiltinMetrics>,
}

impl FlowRunner {
    /// Run one full iteration: steps, optional `exec` function, metrics.
    pub async fn run_iteration(
        &self,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> IterationOutcome {
        vu.begin_iteration();
        // on_start: once per VU, before its first iteration (Locust on_start).
        if vu.iteration == 1 {
            if let Some(on_start) = &self.program.on_start {
                if let Some(vu_script) = script.as_mut() {
                    if vu_script.has_function(on_start) {
                        let setup = vu.run.setup_data.read().clone();
                        if let Err(e) = run_script(
                            self,
                            vu,
                            vu_script.as_mut(),
                            ScriptInvocation::Call(on_start.clone(), vec![setup]),
                        ) {
                            tracing::warn!(on_start, error = %e, "on_start hook failed");
                            self.record_exception(vu, "on_start", &e.to_string());
                        }
                    }
                }
            }
        }
        let started = Instant::now();
        let mut outcome = self.run_steps(&self.program.steps, vu, script).await;

        if matches!(outcome, IterationOutcome::Completed) {
            if let Some(exec) = &self.program.exec {
                if let Some(vu_script) = script.as_mut() {
                    let setup = vu.run.setup_data.read().clone();
                    let context = serde_json::json!({
                        "vu": vu.vu_id,
                        "iteration": vu.iteration.saturating_sub(1),
                        "scenario": vu.scenario.as_ref(),
                    });
                    let result = run_script(
                        self,
                        vu,
                        vu_script.as_mut(),
                        ScriptInvocation::Call(exec.clone(), vec![setup, context]),
                    );
                    if let Err(e) = result {
                        tracing::warn!(scenario = %self.program.name, error = %e, "exec function failed");
                        let tags = vu.sample_tags(&[("scenario_fn", exec)]);
                        vu.metrics.rate(&self.builtins.http_req_failed, true, &tags);
                        self.record_exception(vu, "exec", &e.to_string());
                    }
                } else {
                    outcome = IterationOutcome::AbortTest(format!(
                        "scenario `{}` requires JS function `{exec}` but no script engine is configured",
                        self.program.name
                    ));
                }
            }
        }

        let tags = vu.sample_tags(&[]);
        vu.metrics.counter(&self.builtins.iterations, 1.0, &tags);
        vu.metrics.trend(
            &self.builtins.iteration_duration,
            started.elapsed().as_secs_f64() * 1000.0,
            &tags,
        );
        outcome
    }

    /// Run the per-VU `on_stop` hook (Locust on_stop), once when the VU retires.
    pub fn run_on_stop(&self, vu: &mut VuContext, script: &mut Option<Box<dyn VuScript>>) {
        let Some(on_stop) = &self.program.on_stop else {
            return;
        };
        // Only fire if the VU actually started (ran at least one iteration).
        if vu.iteration == 0 {
            return;
        }
        if let Some(vu_script) = script.as_mut() {
            if vu_script.has_function(on_stop) {
                let setup = vu.run.setup_data.read().clone();
                if let Err(e) = run_script(
                    self,
                    vu,
                    vu_script.as_mut(),
                    ScriptInvocation::Call(on_stop.clone(), vec![setup]),
                ) {
                    tracing::warn!(on_stop, error = %e, "on_stop hook failed");
                    self.record_exception(vu, "on_stop", &e.to_string());
                }
            }
        }
    }

    fn run_steps<'a>(
        &'a self,
        steps: &'a [CompiledStep],
        vu: &'a mut VuContext,
        script: &'a mut Option<Box<dyn VuScript>>,
    ) -> futures::future::BoxFuture<'a, IterationOutcome> {
        Box::pin(async move {
            for step in steps {
                match step {
                    CompiledStep::Request(req) => {
                        match self.run_request(req, vu, script).await {
                            RequestFlow::Continue => {}
                            RequestFlow::AbortIteration => return IterationOutcome::Completed,
                            RequestFlow::StopVu => return IterationOutcome::StopVu,
                            RequestFlow::AbortScenario => return IterationOutcome::AbortScenario,
                            RequestFlow::AbortTest(reason) => {
                                return IterationOutcome::AbortTest(reason)
                            }
                        }
                        if let Some(tt) = &self.program.think_time {
                            let pause = sample_think_time(tt, &mut vu.rng);
                            tokio::time::sleep(pause).await;
                        }
                    }
                    CompiledStep::ThinkTime(tt) => {
                        let pause = sample_think_time(tt, &mut vu.rng);
                        tokio::time::sleep(pause).await;
                    }
                    CompiledStep::Js { call, script: code } => {
                        let Some(vu_script) = script.as_mut() else {
                            tracing::warn!("js step skipped: no script engine configured");
                            continue;
                        };
                        let invocation = if let Some(name) = call {
                            let setup = vu.run.setup_data.read().clone();
                            ScriptInvocation::Call(name.clone(), vec![setup])
                        } else if let Some(code) = code {
                            ScriptInvocation::Eval(code.clone())
                        } else {
                            continue;
                        };
                        if let Err(e) = run_script(self, vu, vu_script.as_mut(), invocation) {
                            tracing::warn!(error = %e, "js step failed");
                            self.record_exception(vu, "js_step", &e.to_string());
                        }
                    }
                    CompiledStep::Group { name, steps } => {
                        vu.groups.push(name.clone());
                        let outcome = self.run_steps(steps, vu, script).await;
                        vu.groups.pop();
                        if outcome != IterationOutcome::Completed {
                            return outcome;
                        }
                    }
                    CompiledStep::Repeat {
                        times,
                        counter,
                        steps,
                    } => {
                        let var = counter.clone().unwrap_or_else(|| "index".to_string());
                        for i in 0..*times {
                            vu.vars.insert(var.clone(), serde_json::json!(i));
                            let outcome = self.run_steps(steps, vu, script).await;
                            if outcome != IterationOutcome::Completed {
                                return outcome;
                            }
                        }
                    }
                    CompiledStep::While {
                        condition,
                        max_iterations,
                        steps,
                    } => {
                        let mut n = 0u64;
                        while n < *max_iterations {
                            if !self.eval_condition_bool(condition, vu, script) {
                                break;
                            }
                            let outcome = self.run_steps(steps, vu, script).await;
                            if outcome != IterationOutcome::Completed {
                                return outcome;
                            }
                            n += 1;
                        }
                    }
                    CompiledStep::If {
                        condition,
                        then,
                        otherwise,
                    } => {
                        let branch = if self.eval_condition_bool(condition, vu, script) {
                            then
                        } else {
                            otherwise
                        };
                        let outcome = self.run_steps(branch, vu, script).await;
                        if outcome != IterationOutcome::Completed {
                            return outcome;
                        }
                    }
                    CompiledStep::Random {
                        strategy,
                        choices,
                        round_robin,
                    } => {
                        if choices.is_empty() {
                            continue;
                        }
                        let idx = self.pick_branch(*strategy, choices, round_robin, vu);
                        let choice = &choices[idx];
                        let label = choice
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("branch-{idx}"));
                        vu.groups.push(label);
                        let outcome = self.run_steps(&choice.steps, vu, script).await;
                        vu.groups.pop();
                        if outcome != IterationOutcome::Completed {
                            return outcome;
                        }
                    }
                    CompiledStep::Foreach {
                        items,
                        var,
                        index,
                        steps,
                    } => {
                        let list = self.resolve_items(items, vu, script);
                        for (i, element) in list.into_iter().enumerate() {
                            vu.vars.insert(var.clone(), element);
                            vu.vars.insert(index.clone(), serde_json::json!(i));
                            let outcome = self.run_steps(steps, vu, script).await;
                            if outcome != IterationOutcome::Completed {
                                return outcome;
                            }
                        }
                    }
                    CompiledStep::Switch {
                        value,
                        cases,
                        default,
                    } => {
                        let key = render_template(self, value, vu, script).unwrap_or_default();
                        let branch = cases
                            .iter()
                            .find(|(case, _)| *case == key)
                            .map(|(_, steps)| steps)
                            .unwrap_or(default);
                        let outcome = self.run_steps(branch, vu, script).await;
                        if outcome != IterationOutcome::Completed {
                            return outcome;
                        }
                    }
                    CompiledStep::During {
                        duration,
                        counter,
                        steps,
                    } => {
                        let deadline = Instant::now() + *duration;
                        let mut i = 0u64;
                        while Instant::now() < deadline {
                            vu.vars.insert(counter.clone(), serde_json::json!(i));
                            let outcome = self.run_steps(steps, vu, script).await;
                            if outcome != IterationOutcome::Completed {
                                return outcome;
                            }
                            i += 1;
                        }
                    }
                    CompiledStep::Retry {
                        times,
                        until,
                        backoff,
                        steps,
                    } => {
                        for attempt in 0..(*times).max(1) {
                            vu.vars
                                .insert("attempt".to_string(), serde_json::json!(attempt));
                            vu.last_request_failed = false;
                            let outcome = self.run_steps(steps, vu, script).await;
                            if outcome != IterationOutcome::Completed {
                                return outcome;
                            }
                            let ok = match until {
                                Some(expr) => self.eval_condition_bool(expr, vu, script),
                                None => !vu.last_request_failed,
                            };
                            if ok {
                                break;
                            }
                            if attempt + 1 < (*times).max(1) {
                                if let Some(b) = backoff {
                                    tokio::time::sleep(*b).await;
                                }
                            }
                        }
                    }
                    CompiledStep::Parallel { branches } => {
                        let outcome = self.run_parallel(branches, vu, script).await;
                        if outcome != IterationOutcome::Completed {
                            return outcome;
                        }
                    }
                    CompiledStep::Rendezvous {
                        name,
                        users,
                        timeout,
                    } => {
                        let barrier = {
                            let mut map = self.program.barriers.lock();
                            map.entry(name.clone())
                                .or_insert_with(|| {
                                    Arc::new(tokio::sync::Barrier::new((*users).max(1) as usize))
                                })
                                .clone()
                        };
                        if tokio::time::timeout(*timeout, barrier.wait())
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                rendezvous = %name,
                                "rendezvous timed out waiting for {users} VUs; continuing"
                            );
                        }
                    }
                }
            }
            IterationOutcome::Completed
        })
    }

    /// Resolve a `foreach` items spec to a list of JSON values.
    fn resolve_items(
        &self,
        items: &serde_json::Value,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> Vec<serde_json::Value> {
        match items {
            serde_json::Value::Array(a) => a.clone(),
            serde_json::Value::String(s) => {
                // Render the template (handles ${...} and ${js: ...}), then parse JSON.
                match render_str(self, s, vu, script) {
                    Ok(rendered) => match serde_json::from_str::<serde_json::Value>(&rendered) {
                        Ok(serde_json::Value::Array(a)) => a,
                        Ok(other) => vec![other],
                        Err(_) => {
                            tracing::warn!(items = %s, "foreach items did not resolve to a JSON array");
                            Vec::new()
                        }
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "foreach items failed to render");
                        Vec::new()
                    }
                }
            }
            other => vec![other.clone()],
        }
    }

    /// Run branches concurrently within one iteration (k6 `http.batch`).
    /// Each branch gets its own child context (fresh connection pool, copied
    /// cookies and vars); extracted variables merge back afterwards.
    async fn run_parallel(
        &self,
        branches: &[Vec<CompiledStep>],
        vu: &mut VuContext,
        _script: &mut Option<Box<dyn VuScript>>,
    ) -> IterationOutcome {
        let mut children: Vec<VuContext> = (0..branches.len())
            .map(|i| self.child_context(vu, i as u64))
            .collect();
        // No JS in parallel branches: each branch is requests-only.
        let mut scripts: Vec<Option<Box<dyn VuScript>>> = branches.iter().map(|_| None).collect();

        let futures: Vec<_> = branches
            .iter()
            .zip(children.iter_mut())
            .zip(scripts.iter_mut())
            .map(|((branch, child), scr)| self.run_steps(branch, child, scr))
            .collect();
        let outcomes = futures::future::join_all(futures).await;

        // Merge child state back into the parent.
        let mut any_failed = false;
        for child in &children {
            for (k, v) in &child.vars {
                vu.vars.insert(k.clone(), v.clone());
            }
            if child.last_request_failed {
                any_failed = true;
            }
        }
        vu.last_request_failed = any_failed;

        // Propagate the strongest non-Completed outcome (abort > stop).
        for o in outcomes {
            match o {
                IterationOutcome::AbortTest(r) => return IterationOutcome::AbortTest(r),
                IterationOutcome::AbortScenario => return IterationOutcome::AbortScenario,
                _ => {}
            }
        }
        IterationOutcome::Completed
    }

    /// Build a child context for a parallel branch.
    fn child_context(&self, parent: &VuContext, branch: u64) -> VuContext {
        let mut child = VuContext::new(
            parent.vu_id,
            parent.scenario.clone(),
            parent.base_tags.clone(),
            parent.metrics.clone(),
            parent.run.clone(),
            parent.cookies.auto,
        );
        child.iteration = parent.iteration;
        child.groups = parent.groups.clone();
        child.vars = parent.vars.clone();
        child.cookies = parent.cookies.clone();
        child.rng = rand::SeedableRng::seed_from_u64(
            parent.vu_id ^ parent.iteration.wrapping_mul(0x9E37) ^ branch.wrapping_mul(0xC2B2),
        );
        child
    }

    /// Evaluate a JS condition expression to a boolean (false on error / no engine).
    fn eval_condition_bool(
        &self,
        expr: &str,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> bool {
        let Some(vu_script) = script.as_mut() else {
            tracing::warn!("flow condition skipped: no script engine configured");
            return false;
        };
        match run_script(
            self,
            vu,
            vu_script.as_mut(),
            ScriptInvocation::Eval(expr.to_string()),
        ) {
            Ok(v) => is_truthy(&v),
            Err(e) => {
                tracing::warn!(condition = %expr, error = %e, "flow condition errored");
                false
            }
        }
    }

    /// Choose a branch index for a `random` step.
    fn pick_branch(
        &self,
        strategy: loadr_config::SwitchStrategy,
        choices: &[CompiledChoice],
        round_robin: &std::sync::atomic::AtomicU64,
        vu: &mut VuContext,
    ) -> usize {
        use loadr_config::SwitchStrategy;
        use rand::RngExt;
        match strategy {
            SwitchStrategy::RoundRobin => {
                let n = round_robin.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (n as usize) % choices.len()
            }
            SwitchStrategy::Uniform => vu.rng.random_range(0..choices.len()),
            SwitchStrategy::Weighted => {
                let total: f64 = choices.iter().map(|c| c.weight.max(0.0)).sum();
                if total <= 0.0 {
                    return vu.rng.random_range(0..choices.len());
                }
                let mut pick = vu.rng.random_range(0.0..total);
                for (i, c) in choices.iter().enumerate() {
                    pick -= c.weight.max(0.0);
                    if pick < 0.0 {
                        return i;
                    }
                }
                choices.len() - 1
            }
        }
    }

    async fn run_request(
        &self,
        req: &CompiledRequest,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> RequestFlow {
        // 0. Global rate ceiling (Gatling throttle): wait for a request slot.
        if let Some(throttle) = &self.program.throttle {
            throttle.acquire().await;
        }

        // 0.5 Chaos fault injection: extra latency before the request is sent,
        // sampled from the VU's seeded rng so runs stay deterministic.
        if let Some(latency) = self.program.faults.and_then(|f| f.latency) {
            let pause = sample_fault_jitter(&latency, &mut vu.rng);
            if !pause.is_zero() {
                self.note_fault(vu, "latency");
                tokio::time::sleep(pause).await;
            }
        }

        // 1. Render the request.
        let mut prepared = match self.prepare(req, vu, script) {
            Ok(p) => p,
            Err(PrepareError::DataExhausted) => return RequestFlow::StopVu,
            Err(PrepareError::Other(e)) => {
                tracing::error!(request = %req.display_name, error = %e, "failed to prepare request");
                let tags = vu.sample_tags(&[("name", &req.display_name), ("error", "prepare")]);
                vu.metrics.rate(&self.builtins.http_req_failed, true, &tags);
                return RequestFlow::Continue;
            }
        };

        // 2. beforeRequest hook.
        if let Some(vu_script) = script.as_mut() {
            if vu_script.has_function("beforeRequest") {
                let req_json = serde_json::json!({
                    "name": prepared.name,
                    "method": prepared.method,
                    "url": prepared.url,
                    "headers": prepared.headers.iter().cloned().collect::<std::collections::BTreeMap<_,_>>(),
                    "body": String::from_utf8_lossy(&prepared.body),
                });
                let result = run_script(
                    self,
                    vu,
                    vu_script.as_mut(),
                    ScriptInvocation::Call("beforeRequest".into(), vec![req_json]),
                );
                match result {
                    Ok(serde_json::Value::Object(updated)) => {
                        apply_request_overrides(&mut prepared, &updated);
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "beforeRequest hook failed"),
                }
            }
        }

        // 3. Execute.
        let handler = match self.protocols.get(&prepared.protocol) {
            Some(h) => h,
            None => {
                tracing::error!(protocol = %prepared.protocol, "no handler registered");
                let tags = vu.sample_tags(&[("name", &prepared.name), ("error", "protocol")]);
                vu.metrics.rate(&self.builtins.http_req_failed, true, &tags);
                return RequestFlow::Continue;
            }
        };
        // Chaos fault injection: decide whether to drop this request. The
        // decision consumes exactly one rng sample per request regardless of
        // mode, so seeded runs stay deterministic.
        let faults = self.program.faults.unwrap_or_default();
        let drop_fault = faults
            .drop_rate
            .map(|rate| fault_drop_decision(rate, &mut vu.rng))
            .unwrap_or(false);
        let drop_before_send =
            drop_fault && faults.drop_mode.unwrap_or_default() == DropMode::BeforeSend;

        let emitter = RequestMetricEmitter::from_vu(vu, self.builtins.clone());
        let mut metrics_guard = RequestMetricsGuard::new(emitter, &prepared);
        let response = if drop_before_send {
            // Fail as a transport-class error without sending; mirrors the
            // handler-error shape below so it lands in `http_req_failed` and
            // the failure breakdown groups it as `fault_injected`.
            self.note_fault(vu, "drop");
            ProtocolResponse {
                error: Some(FAULT_DROP_ERROR.to_string()),
                url: prepared.url.clone(),
                ..Default::default()
            }
        } else {
            let mut response = match handler.execute(vu, &prepared).await {
                Ok(r) => r,
                Err(e) => ProtocolResponse {
                    error: Some(e.to_string()),
                    url: prepared.url.clone(),
                    ..Default::default()
                },
            };
            if drop_fault {
                // `after_response` mode: the request went out, but the
                // response is discarded and the request failed.
                self.note_fault(vu, "drop");
                response.error = Some(FAULT_DROP_RESPONSE_ERROR.to_string());
            }
            response
        };

        // 4. Metrics.
        metrics_guard.record_completed(&response);

        // 5. Extraction (classic extractors and fused chains).
        let mut chain_flow = RequestFlow::Continue;
        let mut extraction_failed = false;
        for extractor in &req.extract {
            match extractor.extract(&response, &mut vu.rng) {
                Ok(value) => {
                    // A chain with a `check:` records a sample to the `checks`
                    // metric on success, mirroring the standalone `checks:`.
                    if extractor.is_chain_with_check() {
                        let tags = vu.sample_tags(&[("check", extractor.name())]);
                        vu.metrics.rate(&self.builtins.checks, true, &tags);
                    }
                    vu.vars.insert(extractor.name().to_string(), value);
                }
                Err(ExtractError::CheckFailed {
                    name,
                    detail,
                    on_failure,
                }) => {
                    // Inline chain validation failed: record a failed check,
                    // mark the request failed and honour `on_failure`.
                    tracing::debug!(request = %prepared.name, chain = %name, %detail, "chain check failed");
                    let check_tags = vu.sample_tags(&[("check", name.as_str())]);
                    vu.metrics.rate(&self.builtins.checks, false, &check_tags);
                    extraction_failed = true;
                    match on_failure {
                        FailureAction::Continue => {}
                        FailureAction::AbortIteration => chain_flow = RequestFlow::AbortIteration,
                        FailureAction::AbortScenario => chain_flow = RequestFlow::AbortScenario,
                        FailureAction::AbortTest => {
                            chain_flow = RequestFlow::AbortTest(format!(
                                "chain `{name}` check failed on `{}`",
                                prepared.name
                            ));
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(request = %prepared.name, error = %e, "extraction failed");
                    extraction_failed = true;
                }
            }
        }
        if extraction_failed {
            let tags = vu.sample_tags(&[("name", &prepared.name)]);
            vu.metrics.rate(&self.builtins.http_req_failed, true, &tags);
        }

        // 6. afterRequest hook.
        if let Some(vu_script) = script.as_mut() {
            if vu_script.has_function("afterRequest") {
                let res_json = response_to_json(&response);
                let _ = run_script(
                    self,
                    vu,
                    vu_script.as_mut(),
                    ScriptInvocation::Call("afterRequest".into(), vec![res_json]),
                );
            }
        }

        // 7. Assertions (mark failed + flow control) and checks (record only).
        // A failed chain check may already have requested abort; start there.
        let mut flow = chain_flow;
        let mut assert_failed = false;
        for condition in &req.assert {
            let result = self.eval_condition(condition, &response, vu, script);
            if !result.pass {
                assert_failed = true;
                tracing::debug!(
                    request = %prepared.name,
                    assertion = %result.name,
                    detail = result.detail.as_deref().unwrap_or(""),
                    "assertion failed"
                );
                match result.on_failure {
                    FailureAction::Continue => {}
                    FailureAction::AbortIteration => {
                        flow = RequestFlow::AbortIteration;
                    }
                    FailureAction::AbortScenario => {
                        flow = RequestFlow::AbortScenario;
                    }
                    FailureAction::AbortTest => {
                        flow = RequestFlow::AbortTest(format!(
                            "assertion `{}` failed on `{}`",
                            result.name, prepared.name
                        ));
                    }
                }
            }
        }
        if assert_failed {
            let tags = vu.sample_tags(&[("name", &prepared.name)]);
            vu.metrics.rate(&self.builtins.http_req_failed, true, &tags);
        }
        for condition in &req.checks {
            let result = self.eval_condition(condition, &response, vu, script);
            let tags = vu.sample_tags(&[("check", &result.name)]);
            vu.metrics.rate(&self.builtins.checks, result.pass, &tags);
        }
        // Record whether this request failed, so `retry` can react to it.
        vu.last_request_failed = response.failed() || assert_failed || extraction_failed;
        flow
    }

    fn eval_condition(
        &self,
        condition: &CompiledCondition,
        response: &ProtocolResponse,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> ConditionResult {
        let mut result = condition.evaluate(response);
        if let Some(expr) = result.needs_js.take() {
            if let Some(vu_script) = script.as_mut() {
                vu.vars
                    .insert("response".to_string(), response_to_json(response));
                let eval = run_script(
                    self,
                    vu,
                    vu_script.as_mut(),
                    ScriptInvocation::Eval(expr.clone()),
                );
                vu.vars.remove("response");
                match eval {
                    Ok(v) => {
                        result.pass = is_truthy(&v);
                        if !result.pass {
                            result.detail = Some(format!("js expression evaluated to {v}"));
                        }
                    }
                    Err(e) => {
                        result.pass = false;
                        result.detail = Some(format!("js error: {e}"));
                    }
                }
            } else {
                result.pass = false;
                result.detail = Some("js condition requires a script engine".to_string());
            }
        }
        result
    }

    /// Record an injected chaos fault on the `faults_injected` counter,
    /// split-tagged by `kind` (`latency` or `drop`).
    fn note_fault(&self, vu: &VuContext, kind: &str) {
        let tags = vu.sample_tags(&[("kind", kind)]);
        vu.metrics
            .counter(&self.builtins.faults_injected, 1.0, &tags);
    }

    /// Record a JS/script exception so the UI can group failures by cause.
    /// `where_` names the site (e.g. `js_step`, `exec`, `on_start`).
    fn record_exception(&self, vu: &VuContext, where_: &str, message: &str) {
        let normalized = normalize_exception(message);
        let tags = vu.sample_tags(&[("exception", &normalized), ("site", where_)]);
        vu.metrics.counter(&self.builtins.vu_exceptions, 1.0, &tags);
    }

    /// Render a compiled request into a `PreparedRequest`.
    fn prepare(
        &self,
        req: &CompiledRequest,
        vu: &mut VuContext,
        script: &mut Option<Box<dyn VuScript>>,
    ) -> Result<PreparedRequest, PrepareError> {
        let http = self.program.http.clone();

        // URL: render, then join base_url for relative paths.
        let mut url = render_template(self, &req.url, vu, script)?;
        if !url.contains("://") {
            if let Some(base) = &http.base_url {
                let base = render_str(self, base, vu, script)?;
                url = format!(
                    "{}/{}",
                    base.trim_end_matches('/'),
                    url.trim_start_matches('/')
                );
            }
        }

        // Query params.
        if !req.params.is_empty() {
            let mut pairs = Vec::new();
            for (k, v) in &req.params {
                pairs.push(format!(
                    "{}={}",
                    urlencode(k),
                    urlencode(&render_template(self, v, vu, script)?)
                ));
            }
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(&pairs.join("&"));
        }

        // Headers: defaults first, then request-level overrides.
        let mut headers: Vec<(String, String)> = Vec::new();
        let set_header = |headers: &mut Vec<(String, String)>, k: &str, v: String| {
            headers.retain(|(ek, _)| !ek.eq_ignore_ascii_case(k));
            headers.push((k.to_string(), v));
        };
        for (k, v) in &http.headers {
            set_header(&mut headers, k, render_str(self, v, vu, script)?);
        }
        for (k, v) in &req.headers {
            set_header(&mut headers, k, render_template(self, v, vu, script)?);
        }

        // Body.
        let mut content_type: Option<String> = None;
        let body: Bytes = if let Some(gql) = &req.graphql {
            let mut payload = serde_json::Map::new();
            payload.insert(
                "query".to_string(),
                serde_json::Value::String(gql.query.clone()),
            );
            if let Some(vars) = &gql.variables {
                payload.insert(
                    "variables".to_string(),
                    render_json(self, vars, vu, script)?,
                );
            }
            if let Some(op) = &gql.operation_name {
                payload.insert(
                    "operationName".to_string(),
                    serde_json::Value::String(op.clone()),
                );
            }
            content_type = Some("application/json".to_string());
            Bytes::from(serde_json::to_vec(&serde_json::Value::Object(payload)).unwrap_or_default())
        } else {
            match &req.body {
                CompiledBody::None => Bytes::new(),
                CompiledBody::Text(t) => Bytes::from(render_template(self, t, vu, script)?),
                CompiledBody::Json(j) => {
                    content_type = Some("application/json".to_string());
                    Bytes::from(
                        serde_json::to_vec(&render_json(self, j, vu, script)?).unwrap_or_default(),
                    )
                }
                CompiledBody::Bytes(b) => b.clone(),
                CompiledBody::Form(fields) => {
                    content_type = Some("application/x-www-form-urlencoded".to_string());
                    let encoded = fields
                        .iter()
                        .map(|(k, v)| {
                            Ok(format!(
                                "{}={}",
                                urlencode(k),
                                urlencode(&render_template(self, v, vu, script)?)
                            ))
                        })
                        .collect::<Result<Vec<_>, PrepareError>>()?
                        .join("&");
                    Bytes::from(encoded)
                }
                CompiledBody::Multipart(parts) => {
                    let boundary = format!("loadrboundary{}", uuid::Uuid::new_v4().simple());
                    content_type = Some(format!("multipart/form-data; boundary={boundary}"));
                    let mut out: Vec<u8> = Vec::new();
                    for part in parts {
                        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
                        let mut disposition =
                            format!("Content-Disposition: form-data; name=\"{}\"", part.name);
                        if let Some(fname) = &part.filename {
                            disposition.push_str(&format!("; filename=\"{fname}\""));
                        }
                        out.extend_from_slice(disposition.as_bytes());
                        out.extend_from_slice(b"\r\n");
                        if let Some(ct) = &part.content_type {
                            out.extend_from_slice(format!("Content-Type: {ct}\r\n").as_bytes());
                        }
                        out.extend_from_slice(b"\r\n");
                        if let Some(v) = &part.value {
                            out.extend_from_slice(render_template(self, v, vu, script)?.as_bytes());
                        } else if let Some(b) = &part.bytes {
                            out.extend_from_slice(b);
                        }
                        out.extend_from_slice(b"\r\n");
                    }
                    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
                    Bytes::from(out)
                }
            }
        };
        if let Some(ct) = content_type {
            if !headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            {
                headers.push(("Content-Type".to_string(), ct));
            }
        }

        // Protocol-specific options.
        let mut options = RequestOptions::default();
        if let Some(ws) = &req.ws {
            let mut frames = Vec::new();
            for msg in &ws.send {
                frames.push(match msg {
                    WsMessage::Text(t) => WsFrame {
                        payload: Bytes::from(render_str(self, t, vu, script)?),
                        binary: false,
                        delay: None,
                    },
                    WsMessage::Detailed {
                        text,
                        binary_base64,
                        delay,
                    } => {
                        if let Some(t) = text {
                            WsFrame {
                                payload: Bytes::from(render_str(self, t, vu, script)?),
                                binary: false,
                                delay: delay.map(|d| d.as_duration()),
                            }
                        } else if let Some(b64) = binary_base64 {
                            use base64::Engine as _;
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(b64.trim())
                                .map_err(|e| {
                                    PrepareError::Other(format!("invalid binary_base64: {e}"))
                                })?;
                            WsFrame {
                                payload: Bytes::from(bytes),
                                binary: true,
                                delay: delay.map(|d| d.as_duration()),
                            }
                        } else {
                            WsFrame {
                                payload: Bytes::new(),
                                binary: false,
                                delay: delay.map(|d| d.as_duration()),
                            }
                        }
                    }
                });
            }
            options.ws = Some(WsRequest {
                subprotocols: ws.subprotocols.clone(),
                send: frames,
                receive_count: ws.receive_count,
                receive_until: ws.receive_until.clone(),
                session_duration: ws.session_duration.map(|d| d.as_duration()),
            });
        }
        if let Some(grpc) = &req.grpc {
            let message = match &req.grpc_literal_message {
                Some(arc) => Some(arc.clone()),
                None => grpc
                    .message
                    .as_ref()
                    .map(|m| render_json(self, m, vu, script).map(Arc::new))
                    .transpose()?,
            };
            let messages = match &req.grpc_literal_messages {
                Some(arc) => arc.clone(),
                None => Arc::new(
                    grpc.messages
                        .iter()
                        .map(|m| render_json(self, m, vu, script))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            };
            // The handler uses `messages` when non-empty, else `message`;
            // the literal flag must describe whichever it will use.
            let message_literal = if grpc.messages.is_empty() {
                req.grpc_literal_message.is_some()
            } else {
                req.grpc_literal_messages.is_some()
            };
            options.grpc = Some(GrpcRequest {
                proto_files: grpc.proto_files.clone(),
                proto_includes: grpc.proto_includes.clone(),
                reflection: grpc.reflection,
                service: grpc.service.clone(),
                method: grpc.method.clone(),
                message,
                messages,
                message_literal,
                metadata: grpc
                    .metadata
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), render_str(self, v, vu, script)?)))
                    .collect::<Result<_, PrepareError>>()?,
            });
        }
        if let Some(socket) = &req.socket {
            let payload = if let Some(text) = &socket.send_text {
                Bytes::from(render_str(self, text, vu, script)?)
            } else if let Some(hex) = &socket.send_hex {
                let cleaned: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
                let mut bytes = Vec::with_capacity(cleaned.len() / 2);
                let chars: Vec<char> = cleaned.chars().collect();
                for pair in chars.chunks(2) {
                    if pair.len() == 2 {
                        let hi = pair[0].to_digit(16).ok_or_else(|| {
                            PrepareError::Other("invalid hex payload".to_string())
                        })?;
                        let lo = pair[1].to_digit(16).ok_or_else(|| {
                            PrepareError::Other("invalid hex payload".to_string())
                        })?;
                        bytes.push((hi * 16 + lo) as u8);
                    }
                }
                Bytes::from(bytes)
            } else {
                Bytes::new()
            };
            options.socket = Some(SocketRequest {
                payload,
                read_bytes: socket.read_bytes,
                read_until_close: socket.read_until_close,
                read_timeout: socket.read_timeout.map(|d| d.as_duration()),
            });
        }

        // SSE stop-conditions are passed to the handler via the generic plugin
        // options channel (the shape `SseLimits::from_plugin` expects).
        if let Some(sse) = &req.sse {
            let mut obj = serde_json::Map::new();
            if let Some(events) = sse.events {
                obj.insert("events".to_string(), serde_json::json!(events));
            }
            if let Some(until) = &sse.until {
                obj.insert("until".to_string(), serde_json::json!(until));
            }
            if let Some(duration) = sse.duration {
                obj.insert(
                    "duration".to_string(),
                    serde_json::json!(format!("{}ms", duration.as_duration().as_millis())),
                );
            }
            if !obj.is_empty() {
                options.plugin = Some(serde_json::Value::Object(obj));
            }
        }

        // SQL query + params travel through the generic plugin options channel
        // (the shape the SQL handler's `SqlQuery::from_plugin` expects).
        if let Some(sql) = &req.sql {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "query".to_string(),
                serde_json::Value::String(render_str(self, &sql.query, vu, script)?),
            );
            let params = sql
                .params
                .iter()
                .map(|p| Ok(serde_json::Value::String(render_str(self, p, vu, script)?)))
                .collect::<Result<Vec<_>, PrepareError>>()?;
            obj.insert("params".to_string(), serde_json::Value::Array(params));
            options.plugin = Some(serde_json::Value::Object(obj));
        }

        // Free-form protocol-plugin options. String leaves are interpolated
        // (`${...}`) before the plugin sees them, matching the `sql:`/`sse:`
        // channels. A request uses `plugin:` OR a typed block, not both; if
        // both are set the typed block above already populated the channel and
        // this would overwrite it, so only apply when not already set.
        if let Some(plugin_opts) = &req.plugin {
            if options.plugin.is_none() {
                options.plugin = Some(render_json(self, plugin_opts, vu, script)?);
            }
        }

        let name = match &req.name {
            Some(tpl) => render_template(self, tpl, vu, script)?,
            None => req.display_name.clone(),
        };

        Ok(PreparedRequest {
            name,
            protocol: req.protocol.clone(),
            method: req.method.clone(),
            url,
            headers,
            body,
            timeout: req.timeout.unwrap_or(http.timeout.as_duration()),
            follow_redirects: req.follow_redirects.unwrap_or(http.follow_redirects),
            max_redirects: http.max_redirects,
            options,
        })
    }
}

enum RequestFlow {
    Continue,
    AbortIteration,
    StopVu,
    AbortScenario,
    AbortTest(String),
}

struct RequestMetricContext {
    name: String,
    method: String,
    protocol: String,
    url: String,
}

impl From<&PreparedRequest> for RequestMetricContext {
    fn from(request: &PreparedRequest) -> Self {
        RequestMetricContext {
            name: request.name.clone(),
            method: request.method.clone(),
            protocol: request.protocol.clone(),
            url: request.url.clone(),
        }
    }
}

#[derive(Clone)]
struct RequestMetricEmitter {
    metrics: MetricsBus,
    registry: Arc<MetricRegistry>,
    builtins: Arc<BuiltinMetrics>,
    base_tags: Arc<Tags>,
    groups: Vec<String>,
}

impl RequestMetricEmitter {
    fn from_vu(vu: &VuContext, builtins: Arc<BuiltinMetrics>) -> Self {
        RequestMetricEmitter {
            metrics: vu.metrics.clone(),
            registry: vu.run.registry.clone(),
            builtins,
            base_tags: vu.base_tags.clone(),
            groups: vu.groups.clone(),
        }
    }

    fn sample_tags(&self, extras: &[(&str, &str)]) -> Arc<Tags> {
        if extras.is_empty() && self.groups.is_empty() {
            return self.base_tags.clone();
        }
        let mut tags = (*self.base_tags).clone();
        if !self.groups.is_empty() {
            tags.insert("group".to_string(), format!("::{}", self.groups.join("::")));
        }
        for (k, v) in extras {
            tags.insert((*k).to_string(), (*v).to_string());
        }
        Arc::new(tags)
    }

    /// Emit the standard metric families for a completed or cancelled request.
    fn emit_request_metrics(&self, request: &RequestMetricContext, response: &ProtocolResponse) {
        let b = &self.builtins;
        let status = response.status.to_string();
        // Transport errors get a coarse `error_kind` tag so the UI can group
        // failures by cause (timeout / connection / dns / tls / ...).
        let error_kind = response
            .error
            .as_deref()
            .map(classify_transport_error)
            .unwrap_or("");
        let mut tag_pairs: Vec<(&str, &str)> = vec![
            ("name", &request.name),
            ("method", &request.method),
            ("status", &status),
            ("proto", &request.protocol),
        ];
        if !error_kind.is_empty() {
            tag_pairs.push(("error_kind", error_kind));
        }
        let tags = self.sample_tags(&tag_pairs);
        let m = &self.metrics;
        let t = &response.timings;

        m.counter(&b.data_sent, response.bytes_sent as f64, &tags);
        m.counter(&b.data_received, response.bytes_received as f64, &tags);

        match request.protocol.as_str() {
            "http" | "graphql" => {
                m.counter(&b.http_reqs, 1.0, &tags);
                m.trend(&b.http_req_duration, t.duration_ms, &tags);
                m.trend(&b.http_req_blocked, t.blocked_ms, &tags);
                m.trend(&b.http_req_connecting, t.connect_ms, &tags);
                m.trend(&b.http_req_tls_handshaking, t.tls_ms, &tags);
                m.trend(&b.http_req_sending, t.sending_ms, &tags);
                m.trend(&b.http_req_waiting, t.waiting_ms, &tags);
                m.trend(&b.http_req_receiving, t.receiving_ms, &tags);
                m.rate(&b.http_req_failed, response.failed(), &tags);
                if request.protocol == "graphql" {
                    self.emit_named("graphql_reqs", MetricKind::Counter, 1.0, &tags);
                    self.emit_named(
                        "graphql_req_duration",
                        MetricKind::Trend,
                        t.duration_ms,
                        &tags,
                    );
                }
            }
            "ws" => {
                self.emit_named("ws_connecting", MetricKind::Trend, t.blocked_ms, &tags);
                self.emit_named(
                    "ws_session_duration",
                    MetricKind::Trend,
                    t.duration_ms,
                    &tags,
                );
                let sent = response
                    .extras
                    .get("msgs_sent")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let received = response
                    .extras
                    .get("msgs_received")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                self.emit_named("ws_msgs_sent", MetricKind::Counter, sent, &tags);
                self.emit_named("ws_msgs_received", MetricKind::Counter, received, &tags);
                m.rate(&b.http_req_failed, response.error.is_some(), &tags);
            }
            // gRPC is the highest-rate protocol here: use the pre-interned
            // names (like HTTP above) instead of the per-request
            // `format!` + registry lookup of the generic arm. gRPC responses
            // never carry docs/rows/msgs extras, so those probes are skipped.
            "grpc" => {
                m.counter(&b.grpc_reqs, 1.0, &tags);
                m.trend(&b.grpc_req_duration, t.duration_ms, &tags);
                m.rate(&b.http_req_failed, response.failed(), &tags);
            }
            other => {
                // tcp/udp built-ins keep their own family name. The
                // `sse`/`browser` built-ins historically share the generic
                // `plugin` family — preserve that so existing dashboards and
                // thresholds keep working. Everything else is a loaded protocol
                // *plugin*, which gets a family derived from its own protocol
                // name (so the `mongo` plugin emits `mongo_reqs` /
                // `mongo_req_duration` / `mongo_docs`, the `postgres` / `mysql`
                // plugins emit `postgres_reqs` / `mysql_reqs`, and the `redis`
                // plugin emits `redis_reqs` / `redis_req_duration`).
                let family = match other {
                    "tcp" | "udp" => other.to_string(),
                    "sse" | "browser" => "plugin".to_string(),
                    name => metric_family(name),
                };
                self.emit_named(&format!("{family}_reqs"), MetricKind::Counter, 1.0, &tags);
                self.emit_named(
                    &format!("{family}_req_duration"),
                    MetricKind::Trend,
                    t.duration_ms,
                    &tags,
                );
                // Plugin protocols may report a count of affected/returned
                // records: `extras.docs` (e.g. Mongo documents) is surfaced as
                // `<family>_docs`, `extras.rows` (e.g. SQL rows returned or
                // affected) as `<family>_rows`, and `extras.msgs` (e.g. Kafka
                // messages produced or fetched) as `<family>_msgs`.
                if let Some(docs) = response.extras.get("docs").and_then(|v| v.as_f64()) {
                    self.emit_named(&format!("{family}_docs"), MetricKind::Counter, docs, &tags);
                }
                if let Some(rows) = response.extras.get("rows").and_then(|v| v.as_f64()) {
                    self.emit_named(&format!("{family}_rows"), MetricKind::Counter, rows, &tags);
                }
                if let Some(msgs) = response.extras.get("msgs").and_then(|v| v.as_f64()) {
                    self.emit_named(&format!("{family}_msgs"), MetricKind::Counter, msgs, &tags);
                }
                m.rate(&b.http_req_failed, response.failed(), &tags);
            }
        }
    }

    fn emit_named(&self, name: &str, kind: MetricKind, value: f64, tags: &Arc<Tags>) {
        let metric = self
            .registry
            .get(name)
            .map(|d| d.name)
            .unwrap_or_else(|| Arc::from(name));
        self.metrics.emit_value(&metric, kind, value, tags);
    }
}

struct RequestMetricsGuard {
    emitter: RequestMetricEmitter,
    request: RequestMetricContext,
    started: Instant,
    active: bool,
}

impl RequestMetricsGuard {
    fn new(emitter: RequestMetricEmitter, request: &PreparedRequest) -> Self {
        emitter.metrics.begin_request();
        RequestMetricsGuard {
            emitter,
            request: RequestMetricContext::from(request),
            started: Instant::now(),
            active: true,
        }
    }

    fn record_completed(&mut self, response: &ProtocolResponse) {
        if !self.active {
            return;
        }
        self.emitter.emit_request_metrics(&self.request, response);
        self.emitter.metrics.end_request();
        self.active = false;
    }

    fn cancelled_response(&self) -> ProtocolResponse {
        ProtocolResponse {
            error: Some(REQUEST_CANCELLED_ERROR.to_string()),
            url: self.request.url.clone(),
            timings: Timings {
                duration_ms: self.started.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let response = self.cancelled_response();
        self.emitter.emit_request_metrics(&self.request, &response);
        self.emitter.metrics.end_request();
        self.active = false;
    }
}

#[derive(Debug, thiserror::Error)]
enum PrepareError {
    #[error("data source exhausted")]
    DataExhausted,
    #[error("{0}")]
    Other(String),
}

enum ScriptInvocation {
    Call(String, Vec<serde_json::Value>),
    Eval(String),
}

/// Build a `ScriptHost` bridge for lifecycle calls (setup/teardown).
pub fn with_host<R>(
    runner: &FlowRunner,
    vu: &mut VuContext,
    f: impl FnOnce(&mut dyn ScriptHost) -> R,
) -> R {
    let handle = tokio::runtime::Handle::current();
    let mut host = HostBridge {
        vu,
        protocols: runner.protocols.clone(),
        program: runner.program.clone(),
        builtins: runner.builtins.clone(),
        handle,
    };
    tokio::task::block_in_place(|| f(&mut host))
}

/// Bucket a free-form transport error string into a coarse, stable kind so the
/// UI can group failures by cause without exploding on volatile detail.
/// Derive a metric-family prefix from a plugin protocol name. The protocol
/// name is the plugin handler's `name()` (e.g. `mongo`), used to build metric
/// names like `mongo_reqs`. We keep `[a-z0-9_]` and lowercase the rest so a
/// malformed name can never inject odd characters into a metric key; an empty
/// result falls back to `plugin`.
fn metric_family(protocol: &str) -> String {
    let cleaned: String = protocol
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('_');
    if trimmed.is_empty() {
        "plugin".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Error message for a request dropped by fault injection before sending;
/// `classify_transport_error` buckets it as `fault_injected`.
const FAULT_DROP_ERROR: &str = "fault injected: request dropped before send";
/// Error message for a response discarded by `drop_mode: after_response`.
const FAULT_DROP_RESPONSE_ERROR: &str = "fault injected: response dropped";
/// Error message for a request future dropped by executor cancellation.
const REQUEST_CANCELLED_ERROR: &str = "request cancelled";

/// Sample the extra latency for a `faults.latency` spec from the VU's rng:
/// `uniform(0..jitter)` or `gaussian(mean=0, std=jitter)` clamped at zero.
fn sample_fault_jitter(latency: &LatencyFault, rng: &mut impl rand::RngExt) -> Duration {
    use rand_distr::{Distribution, Normal};
    let jitter = latency.jitter.as_duration().as_secs_f64();
    if jitter <= 0.0 {
        return Duration::ZERO;
    }
    match latency.distribution {
        FaultDistribution::Uniform => Duration::from_secs_f64(rng.random_range(0.0..jitter)),
        FaultDistribution::Gaussian => {
            let sampled = Normal::new(0.0, jitter)
                .map(|n| n.sample(rng))
                .unwrap_or(0.0);
            Duration::from_secs_f64(sampled.max(0.0))
        }
    }
}

/// Decide whether to drop a request with probability `rate`, consuming one
/// sample from the VU's seeded rng.
fn fault_drop_decision(rate: f64, rng: &mut impl rand::RngExt) -> bool {
    rate > 0.0 && rng.random_range(0.0..1.0) < rate
}

fn classify_transport_error(error: &str) -> &'static str {
    let e = error.to_ascii_lowercase();
    if e.contains("fault injected") {
        "fault_injected"
    } else if e.contains("cancelled") || e.contains("canceled") {
        "cancelled"
    } else if e.contains("timed out") || e.contains("timeout") {
        "timeout"
    } else if e.contains("dns") || e.contains("resolve") || e.contains("name or service") {
        "dns"
    } else if e.contains("tls") || e.contains("certificate") || e.contains("ssl") {
        "tls"
    } else if e.contains("refused") {
        "connection_refused"
    } else if e.contains("reset") || e.contains("broken pipe") || e.contains("closed") {
        "connection_reset"
    } else if e.contains("connect") {
        "connection"
    } else {
        "transport"
    }
}

/// Collapse a raw exception/error message into a stable grouping key: drop
/// volatile detail (numbers, quoted strings, hex) and cap the length so the
/// failure breakdown groups "the same" exception together.
fn normalize_exception(message: &str) -> String {
    // Take the first line only — JS stack traces follow on later lines.
    let first = message.lines().next().unwrap_or(message).trim();
    let mut out = String::with_capacity(first.len());
    let mut last_was_placeholder = false;
    let mut chars = first.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' | '\'' | '`' => {
                // Skip the quoted literal entirely.
                for q in chars.by_ref() {
                    if q == c {
                        break;
                    }
                }
                if !last_was_placeholder {
                    out.push('…');
                    last_was_placeholder = true;
                }
            }
            d if d.is_ascii_digit() => {
                if !last_was_placeholder {
                    out.push('N');
                    last_was_placeholder = true;
                }
            }
            _ => {
                out.push(c);
                last_was_placeholder = false;
            }
        }
    }
    let trimmed = out.trim();
    let capped: String = trimmed.chars().take(120).collect();
    if capped.is_empty() {
        "exception".to_string()
    } else {
        capped
    }
}

/// Invoke the script engine for this VU via a host bridge.
fn run_script(
    runner: &FlowRunner,
    vu: &mut VuContext,
    script: &mut dyn VuScript,
    invocation: ScriptInvocation,
) -> Result<serde_json::Value, crate::error::ScriptError> {
    let handle = tokio::runtime::Handle::current();
    let mut host = HostBridge {
        vu,
        protocols: runner.protocols.clone(),
        program: runner.program.clone(),
        builtins: runner.builtins.clone(),
        handle,
    };
    tokio::task::block_in_place(|| match invocation {
        ScriptInvocation::Call(name, args) => script.call_function(&mut host, &name, &args),
        ScriptInvocation::Eval(code) => script.eval(&mut host, &code),
    })
}

/// Replace a leading-`$` env reference in a payload magnitude with the env
/// value, so `${payload:kind:$LOADR_SWEEP_DEPTH}` scales under `loadr sweep`.
/// Only the magnitude (after the last `:`) is treated this way; the kind name
/// is left untouched. Unknown env vars resolve to empty (a parse error follows,
/// which is the honest signal that the sweep var wasn't exported).
fn resolve_payload_env(spec: &str) -> String {
    match spec.rsplit_once(':') {
        Some((kind, mag)) if mag.starts_with('$') => {
            let val = std::env::var(&mag[1..]).unwrap_or_default();
            format!("{kind}:{val}")
        }
        _ => spec.to_string(),
    }
}

fn render_template(
    runner: &FlowRunner,
    tpl: &Template,
    vu: &mut VuContext,
    script: &mut Option<Box<dyn VuScript>>,
) -> Result<String, PrepareError> {
    let mut out = String::new();
    for part in &tpl.parts {
        match part {
            loadr_config::Part::Lit(l) => out.push_str(l),
            loadr_config::Part::Expr(expr) => {
                if let Some(spec) = expr.strip_prefix("payload:") {
                    // `${payload:<kind>:<magnitude>}` generates an adversarial
                    // body at request time (never materialised into VU state).
                    // A `$NAME` magnitude reads an environment variable, so
                    // `loadr sweep --var depth=…` can scale the payload via the
                    // exported `LOADR_SWEEP_DEPTH` (or any env var).
                    let resolved = resolve_payload_env(spec);
                    let bytes = loadr_payload::generate_str(&resolved)
                        .map_err(|e| PrepareError::Other(e.to_string()))?;
                    out.push_str(&String::from_utf8_lossy(&bytes));
                } else if let Some(code) = expr.strip_prefix("js:") {
                    let Some(vu_script) = script.as_mut() else {
                        return Err(PrepareError::Other(format!(
                            "`${{js: ...}}` needs a script engine: {expr}"
                        )));
                    };
                    let value = run_script(
                        runner,
                        vu,
                        vu_script.as_mut(),
                        ScriptInvocation::Eval(code.to_string()),
                    )
                    .map_err(|e| PrepareError::Other(e.to_string()))?;
                    out.push_str(&json_to_string(&value));
                } else {
                    match vu.resolve_expr(expr) {
                        Ok(Some(v)) => out.push_str(&v),
                        Ok(None) => {
                            return Err(PrepareError::Other(format!(
                                "unresolved template variable `{expr}`"
                            )))
                        }
                        Err(crate::data::NextRowError::Exhausted(_)) => {
                            return Err(PrepareError::DataExhausted)
                        }
                        Err(e) => return Err(PrepareError::Other(e.to_string())),
                    }
                }
            }
        }
    }
    Ok(out)
}

fn render_str(
    runner: &FlowRunner,
    s: &str,
    vu: &mut VuContext,
    script: &mut Option<Box<dyn VuScript>>,
) -> Result<String, PrepareError> {
    let tpl = Template::parse(s).map_err(|e| PrepareError::Other(e.to_string()))?;
    render_template(runner, &tpl, vu, script)
}

/// True when rendering `value` with [`render_json`] would return it
/// unchanged: every string leaf parses as a literal (no `${...}`) template.
/// Parse failures count as non-literal so they surface through the normal
/// per-iteration render error path.
fn json_is_literal(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => Template::parse(s)
            .map(|tpl| tpl.is_literal())
            .unwrap_or(false),
        serde_json::Value::Array(items) => items.iter().all(json_is_literal),
        serde_json::Value::Object(map) => map.values().all(json_is_literal),
        _ => true,
    }
}

fn render_json(
    runner: &FlowRunner,
    value: &serde_json::Value,
    vu: &mut VuContext,
    script: &mut Option<Box<dyn VuScript>>,
) -> Result<serde_json::Value, PrepareError> {
    Ok(match value {
        serde_json::Value::String(s) => {
            let tpl = Template::parse(s).map_err(|e| PrepareError::Other(e.to_string()))?;
            if tpl.is_literal() {
                serde_json::Value::String(s.clone())
            } else {
                let rendered = render_template(runner, &tpl, vu, script)?;
                if tpl.parts.len() == 1 {
                    serde_json::from_str(&rendered).unwrap_or(serde_json::Value::String(rendered))
                } else {
                    serde_json::Value::String(rendered)
                }
            }
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|v| render_json(runner, v, vu, script))
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), render_json(runner, v, vu, script)?);
            }
            serde_json::Value::Object(out)
        }
        other => other.clone(),
    })
}

fn apply_request_overrides(
    prepared: &mut PreparedRequest,
    updated: &serde_json::Map<String, serde_json::Value>,
) {
    if let Some(serde_json::Value::String(url)) = updated.get("url") {
        prepared.url = url.clone();
    }
    if let Some(serde_json::Value::String(method)) = updated.get("method") {
        prepared.method = method.to_ascii_uppercase();
    }
    if let Some(serde_json::Value::Object(headers)) = updated.get("headers") {
        let mut merged = prepared.headers.clone();
        for (k, v) in headers {
            merged.retain(|(ek, _)| !ek.eq_ignore_ascii_case(k));
            merged.push((k.clone(), json_to_string(v)));
        }
        prepared.headers = merged;
    }
    if let Some(serde_json::Value::String(body)) = updated.get("body") {
        prepared.body = Bytes::from(body.clone());
    }
}

pub fn response_to_json(response: &ProtocolResponse) -> serde_json::Value {
    let headers: serde_json::Map<String, serde_json::Value> = response
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::json!({
        "status": response.status,
        "status_text": response.status_text,
        "body": response.body_text(),
        "headers": headers,
        "duration_ms": response.timings.duration_ms,
        "error": response.error,
        "url": response.url,
        "protocol": response.protocol_version,
    })
}

fn is_truthy(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => true,
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// The `ScriptHost` implementation backing JS stdlib calls.
struct HostBridge<'a> {
    vu: &'a mut VuContext,
    protocols: Arc<ProtocolRegistry>,
    program: Arc<ScenarioProgram>,
    builtins: Arc<BuiltinMetrics>,
    handle: tokio::runtime::Handle,
}

impl HostBridge<'_> {
    fn emit_http_metrics(&mut self, request: &PreparedRequest, response: &ProtocolResponse) {
        let b = &self.builtins;
        let status = response.status.to_string();
        let tags = self.vu.sample_tags(&[
            ("name", &request.name),
            ("method", &request.method),
            ("status", &status),
            ("proto", "http"),
        ]);
        let m = &self.vu.metrics;
        let t = &response.timings;
        m.counter(&b.http_reqs, 1.0, &tags);
        m.trend(&b.http_req_duration, t.duration_ms, &tags);
        m.trend(&b.http_req_blocked, t.blocked_ms, &tags);
        m.trend(&b.http_req_connecting, t.connect_ms, &tags);
        m.trend(&b.http_req_tls_handshaking, t.tls_ms, &tags);
        m.trend(&b.http_req_sending, t.sending_ms, &tags);
        m.trend(&b.http_req_waiting, t.waiting_ms, &tags);
        m.trend(&b.http_req_receiving, t.receiving_ms, &tags);
        m.rate(&b.http_req_failed, response.failed(), &tags);
        m.counter(&b.data_sent, response.bytes_sent as f64, &tags);
        m.counter(&b.data_received, response.bytes_received as f64, &tags);
    }
}

impl ScriptHost for HostBridge<'_> {
    fn http_request(&mut self, req: HostHttpRequest) -> HostHttpResponse {
        let http = &self.program.http;
        let mut url = req.url.clone();
        if !url.contains("://") {
            if let Some(base) = &http.base_url {
                url = format!(
                    "{}/{}",
                    base.trim_end_matches('/'),
                    url.trim_start_matches('/')
                );
            }
        }
        let mut headers: Vec<(String, String)> = http
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, v) in &req.headers {
            headers.retain(|(ek, _)| !ek.eq_ignore_ascii_case(k));
            headers.push((k.clone(), v.clone()));
        }
        let prepared = PreparedRequest {
            name: req.name.clone().unwrap_or_else(|| req.url.clone()),
            protocol: "http".to_string(),
            method: req.method.to_ascii_uppercase(),
            url,
            headers,
            body: req.body.map(Bytes::from).unwrap_or_default(),
            timeout: req
                .timeout_ms
                .map(|ms| Duration::from_secs_f64(ms / 1000.0))
                .unwrap_or(http.timeout.as_duration()),
            follow_redirects: http.follow_redirects,
            max_redirects: http.max_redirects,
            options: RequestOptions::default(),
        };
        let Some(handler) = self.protocols.get("http") else {
            return HostHttpResponse {
                error: Some("no http protocol handler registered".to_string()),
                ..Default::default()
            };
        };
        let response = self.handle.block_on(async {
            match handler.execute(self.vu, &prepared).await {
                Ok(r) => r,
                Err(e) => ProtocolResponse {
                    error: Some(e.to_string()),
                    url: prepared.url.clone(),
                    ..Default::default()
                },
            }
        });
        self.emit_http_metrics(&prepared, &response);
        HostHttpResponse {
            status: response.status,
            status_text: response.status_text.clone(),
            headers: response.headers.clone(),
            body: response.body.to_vec(),
            duration_ms: response.timings.duration_ms,
            timings: response.timings,
            error: response.error.clone(),
            url: response.url.clone(),
            protocol_version: response.protocol_version.clone(),
        }
    }

    fn sleep(&mut self, seconds: f64) {
        if seconds > 0.0 {
            std::thread::sleep(Duration::from_secs_f64(seconds.min(3600.0)));
        }
    }

    fn check(&mut self, name: &str, pass: bool) {
        let tags = self.vu.sample_tags(&[("check", name)]);
        self.vu.metrics.rate(&self.builtins.checks, pass, &tags);
    }

    fn metric_add(
        &mut self,
        metric: &str,
        kind: MetricKind,
        value: f64,
        tags: &[(String, String)],
    ) -> Result<(), String> {
        let name = self.vu.run.registry.register(metric, kind, false, None)?;
        let extra: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let tags = self.vu.sample_tags(&extra);
        self.vu.metrics.emit_value(&name, kind, value, &tags);
        Ok(())
    }

    fn group_push(&mut self, name: &str) {
        self.vu.groups.push(name.to_string());
    }

    fn group_pop(&mut self) {
        self.vu.groups.pop();
    }

    fn log(&mut self, level: ScriptLogLevel, message: &str) {
        match level {
            ScriptLogLevel::Debug => {
                tracing::debug!(target: "loadr::js", vu = self.vu.vu_id, "{message}")
            }
            ScriptLogLevel::Info => {
                tracing::info!(target: "loadr::js", vu = self.vu.vu_id, "{message}")
            }
            ScriptLogLevel::Warn => {
                tracing::warn!(target: "loadr::js", vu = self.vu.vu_id, "{message}")
            }
            ScriptLogLevel::Error => {
                tracing::error!(target: "loadr::js", vu = self.vu.vu_id, "{message}")
            }
        }
    }

    fn env_var(&self, name: &str) -> Option<String> {
        self.vu.run.env.get(name).cloned()
    }

    fn open_file(&self, path: &str) -> Result<Vec<u8>, String> {
        let p = std::path::Path::new(path);
        let resolved = if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.vu.run.base_dir.join(p)
        };
        // Restrict reads to the test definition directory (security posture).
        let canonical = resolved
            .canonicalize()
            .map_err(|e| format!("open({path}): {e}"))?;
        let base = self
            .vu
            .run
            .base_dir
            .canonicalize()
            .map_err(|e| format!("open({path}): {e}"))?;
        if !canonical.starts_with(&base) {
            return Err(format!(
                "open({path}): refusing to read outside the test directory"
            ));
        }
        std::fs::read(&canonical).map_err(|e| format!("open({path}): {e}"))
    }

    fn get_var(&self, name: &str) -> Option<serde_json::Value> {
        self.vu.vars.get(name).cloned()
    }

    fn set_var(&mut self, name: &str, value: serde_json::Value) {
        self.vu.vars.insert(name.to_string(), value);
    }

    fn cookie_get(&self, url: &str, name: &str) -> Option<String> {
        let parsed = url::Url::parse(url).ok()?;
        self.vu.cookies.get(&parsed, name)
    }

    fn cookie_set(&mut self, url: &str, name: &str, value: &str) {
        if let Ok(parsed) = url::Url::parse(url) {
            self.vu.cookies.set(&parsed, name, value);
        }
    }

    fn cookies_clear(&mut self) {
        self.vu.cookies.clear();
    }

    fn vu_info(&self) -> (u64, u64, String) {
        (
            self.vu.vu_id,
            self.vu.iteration.saturating_sub(1),
            self.vu.scenario.to_string(),
        )
    }

    fn data_row(&mut self, source: &str) -> Result<serde_json::Value, String> {
        let row = self.vu.data_row(source).map_err(|e| e.to_string())?;
        Ok(serde_json::Value::Object(
            row.iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod request_metrics_tests {
    use super::{
        RequestMetricContext, RequestMetricEmitter, RequestMetricsGuard, REQUEST_CANCELLED_ERROR,
    };
    use crate::metrics::{BuiltinMetrics, MetricKind, MetricRegistry, MetricsBus, Sample, Tags};
    use crate::protocol::{
        GrpcRequest, PreparedRequest, ProtocolResponse, RequestOptions, Timings,
    };
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::UnboundedReceiver;

    fn test_emitter() -> (MetricsBus, UnboundedReceiver<Sample>, RequestMetricEmitter) {
        let registry = Arc::new(MetricRegistry::with_builtins());
        let builtins = Arc::new(BuiltinMetrics::resolve(&registry));
        let (metrics, receiver) = MetricsBus::new();
        let mut base_tags = Tags::new();
        base_tags.insert("suite".to_string(), "request_metrics".to_string());
        let emitter = RequestMetricEmitter {
            metrics: metrics.clone(),
            registry,
            builtins,
            base_tags: Arc::new(base_tags),
            groups: vec!["checkout".to_string(), "payment".to_string()],
        };
        (metrics, receiver, emitter)
    }

    fn prepared_request(protocol: &str) -> PreparedRequest {
        PreparedRequest {
            name: "checkout".to_string(),
            protocol: protocol.to_string(),
            method: "POST".to_string(),
            url: "https://example.test/checkout".to_string(),
            headers: vec![("authorization".to_string(), "secret".to_string())],
            body: Bytes::from(vec![1, 2, 3, 4]),
            timeout: Duration::from_secs(3),
            follow_redirects: true,
            max_redirects: 4,
            options: RequestOptions {
                grpc: Some(GrpcRequest {
                    service: "checkout.Payment".to_string(),
                    method: "Charge".to_string(),
                    message: Some(Arc::new(serde_json::json!({"id": 1}))),
                    messages: Arc::new(vec![serde_json::json!({"id": 2})]),
                    metadata: vec![("trace-id".to_string(), "abc".to_string())],
                    ..Default::default()
                }),
                plugin: Some(serde_json::json!({
                    "nested": {"expensive": [1, 2, 3]}
                })),
                ..Default::default()
            },
        }
    }

    fn drain(receiver: &mut UnboundedReceiver<Sample>) -> Vec<Sample> {
        let mut samples = Vec::new();
        while let Ok(sample) = receiver.try_recv() {
            samples.push(sample);
        }
        samples
    }

    fn assert_metric_set(samples: &[Sample], expected: &[(&str, MetricKind, f64)]) {
        assert_eq!(
            samples.len(),
            expected.len(),
            "unexpected samples: {samples:#?}"
        );
        for (name, kind, value) in expected {
            let matches: Vec<_> = samples
                .iter()
                .filter(|sample| sample.metric.as_ref() == *name)
                .collect();
            assert_eq!(matches.len(), 1, "expected one `{name}` sample");
            assert_eq!(matches[0].kind, *kind, "kind for `{name}`");
            assert_eq!(matches[0].value, *value, "value for `{name}`");
        }
    }

    fn assert_tags(samples: &[Sample], protocol: &str, status: &str, error_kind: Option<&str>) {
        for sample in samples {
            let tags = &sample.tags;
            assert_eq!(
                tags.get("suite").map(String::as_str),
                Some("request_metrics")
            );
            assert_eq!(
                tags.get("group").map(String::as_str),
                Some("::checkout::payment")
            );
            assert_eq!(tags.get("name").map(String::as_str), Some("checkout"));
            assert_eq!(tags.get("method").map(String::as_str), Some("POST"));
            assert_eq!(tags.get("status").map(String::as_str), Some(status));
            assert_eq!(tags.get("proto").map(String::as_str), Some(protocol));
            assert_eq!(tags.get("error_kind").map(String::as_str), error_kind);
            assert_eq!(tags.len(), if error_kind.is_some() { 7 } else { 6 });
        }
    }

    #[test]
    fn completed_http_request_emits_exactly_once() {
        let (metrics, mut receiver, emitter) = test_emitter();
        let request = prepared_request("http");
        let response = ProtocolResponse {
            status: 502,
            timings: Timings {
                dns_ms: 1.0,
                connect_ms: 2.0,
                tls_ms: 3.0,
                sending_ms: 4.0,
                waiting_ms: 5.0,
                receiving_ms: 6.0,
                duration_ms: 15.0,
                blocked_ms: 7.0,
            },
            bytes_sent: 8,
            bytes_received: 9,
            protocol_version: "HTTP/1.1".to_string(),
            error: Some("connection reset by peer".to_string()),
            url: request.url.clone(),
            ..Default::default()
        };

        let mut guard = RequestMetricsGuard::new(emitter, &request);
        assert_eq!(metrics.requests_in_flight(), 1);
        guard.record_completed(&response);
        guard.record_completed(&response);
        assert_eq!(metrics.requests_in_flight(), 0);
        drop(guard);

        let samples = drain(&mut receiver);
        assert_metric_set(
            &samples,
            &[
                ("data_sent", MetricKind::Counter, 8.0),
                ("data_received", MetricKind::Counter, 9.0),
                ("http_reqs", MetricKind::Counter, 1.0),
                ("http_req_duration", MetricKind::Trend, 15.0),
                ("http_req_blocked", MetricKind::Trend, 7.0),
                ("http_req_connecting", MetricKind::Trend, 2.0),
                ("http_req_tls_handshaking", MetricKind::Trend, 3.0),
                ("http_req_sending", MetricKind::Trend, 4.0),
                ("http_req_waiting", MetricKind::Trend, 5.0),
                ("http_req_receiving", MetricKind::Trend, 6.0),
                ("http_req_failed", MetricKind::Rate, 1.0),
            ],
        );
        assert_tags(&samples, "http", "502", Some("connection_reset"));
    }

    #[test]
    fn graphql_request_preserves_http_and_graphql_families() {
        let (metrics, mut receiver, emitter) = test_emitter();
        let request = prepared_request("graphql");
        let response = ProtocolResponse {
            status: 200,
            timings: Timings {
                duration_ms: 12.0,
                ..Default::default()
            },
            protocol_version: "HTTP/2".to_string(),
            url: request.url.clone(),
            ..Default::default()
        };

        let mut guard = RequestMetricsGuard::new(emitter, &request);
        guard.record_completed(&response);
        drop(guard);

        let samples = drain(&mut receiver);
        assert_eq!(metrics.requests_in_flight(), 0);
        assert_eq!(samples.len(), 13);
        assert_metric_set(
            &samples,
            &[
                ("data_sent", MetricKind::Counter, 0.0),
                ("data_received", MetricKind::Counter, 0.0),
                ("http_reqs", MetricKind::Counter, 1.0),
                ("http_req_duration", MetricKind::Trend, 12.0),
                ("http_req_blocked", MetricKind::Trend, 0.0),
                ("http_req_connecting", MetricKind::Trend, 0.0),
                ("http_req_tls_handshaking", MetricKind::Trend, 0.0),
                ("http_req_sending", MetricKind::Trend, 0.0),
                ("http_req_waiting", MetricKind::Trend, 0.0),
                ("http_req_receiving", MetricKind::Trend, 0.0),
                ("http_req_failed", MetricKind::Rate, 0.0),
                ("graphql_reqs", MetricKind::Counter, 1.0),
                ("graphql_req_duration", MetricKind::Trend, 12.0),
            ],
        );
        assert_tags(&samples, "graphql", "200", None);
    }

    #[test]
    fn grpc_and_socket_requests_keep_builtin_families() {
        for (protocol, version, family) in [
            ("grpc", "grpc", "grpc"),
            ("tcp", "tcp", "tcp"),
            ("udp", "udp", "udp"),
        ] {
            let (metrics, mut receiver, emitter) = test_emitter();
            let request = prepared_request(protocol);
            let response = ProtocolResponse {
                status: 0,
                timings: Timings {
                    duration_ms: 4.0,
                    ..Default::default()
                },
                bytes_sent: 5,
                bytes_received: 6,
                protocol_version: version.to_string(),
                url: request.url.clone(),
                ..Default::default()
            };

            let mut guard = RequestMetricsGuard::new(emitter, &request);
            guard.record_completed(&response);
            drop(guard);

            let samples = drain(&mut receiver);
            assert_eq!(metrics.requests_in_flight(), 0);
            assert_metric_set(
                &samples,
                &[
                    ("data_sent", MetricKind::Counter, 5.0),
                    ("data_received", MetricKind::Counter, 6.0),
                    (&format!("{family}_reqs"), MetricKind::Counter, 1.0),
                    (&format!("{family}_req_duration"), MetricKind::Trend, 4.0),
                    ("http_req_failed", MetricKind::Rate, 0.0),
                ],
            );
            assert_tags(&samples, protocol, "0", None);
        }
    }

    #[test]
    fn loaded_plugin_request_emits_family_and_extra_counters() {
        let (metrics, mut receiver, emitter) = test_emitter();
        let request = prepared_request("custom-db");
        let response = ProtocolResponse {
            status: 0,
            timings: Timings {
                duration_ms: 10.0,
                ..Default::default()
            },
            bytes_sent: 11,
            bytes_received: 12,
            protocol_version: "custom-db".to_string(),
            url: request.url.clone(),
            extras: serde_json::json!({"docs": 13, "rows": 14, "msgs": 15}),
            ..Default::default()
        };

        let mut guard = RequestMetricsGuard::new(emitter, &request);
        guard.record_completed(&response);
        drop(guard);

        let samples = drain(&mut receiver);
        assert_eq!(metrics.requests_in_flight(), 0);
        assert_metric_set(
            &samples,
            &[
                ("data_sent", MetricKind::Counter, 11.0),
                ("data_received", MetricKind::Counter, 12.0),
                ("custom_db_reqs", MetricKind::Counter, 1.0),
                ("custom_db_req_duration", MetricKind::Trend, 10.0),
                ("custom_db_docs", MetricKind::Counter, 13.0),
                ("custom_db_rows", MetricKind::Counter, 14.0),
                ("custom_db_msgs", MetricKind::Counter, 15.0),
                ("http_req_failed", MetricKind::Rate, 0.0),
            ],
        );
        assert_tags(&samples, "custom-db", "0", None);
    }

    #[test]
    fn dropping_active_guard_emits_one_cancelled_request() {
        let (metrics, mut receiver, emitter) = test_emitter();
        let request = prepared_request("http");
        metrics.begin_request();
        let prior_in_flight = metrics.requests_in_flight();

        let mut guard = RequestMetricsGuard::new(emitter, &request);
        assert_eq!(metrics.requests_in_flight(), prior_in_flight + 1);
        guard.started = Instant::now() - Duration::from_millis(25);
        let preview = guard.cancelled_response();
        assert_eq!(preview.url, request.url);
        assert_eq!(preview.error.as_deref(), Some(REQUEST_CANCELLED_ERROR));
        assert!(preview.timings.duration_ms >= 25.0);

        drop(guard);
        assert_eq!(metrics.requests_in_flight(), prior_in_flight);

        let samples = drain(&mut receiver);
        assert_eq!(samples.len(), 11);
        let duration = samples
            .iter()
            .find(|sample| sample.metric.as_ref() == "http_req_duration")
            .expect("http_req_duration");
        assert_eq!(duration.kind, MetricKind::Trend);
        assert!(duration.value >= preview.timings.duration_ms);
        assert_metric_set(
            &samples,
            &[
                ("data_sent", MetricKind::Counter, 0.0),
                ("data_received", MetricKind::Counter, 0.0),
                ("http_reqs", MetricKind::Counter, 1.0),
                ("http_req_duration", MetricKind::Trend, duration.value),
                ("http_req_blocked", MetricKind::Trend, 0.0),
                ("http_req_connecting", MetricKind::Trend, 0.0),
                ("http_req_tls_handshaking", MetricKind::Trend, 0.0),
                ("http_req_sending", MetricKind::Trend, 0.0),
                ("http_req_waiting", MetricKind::Trend, 0.0),
                ("http_req_receiving", MetricKind::Trend, 0.0),
                ("http_req_failed", MetricKind::Rate, 1.0),
            ],
        );
        assert_tags(&samples, "http", "0", Some("cancelled"));

        metrics.end_request();
        assert_eq!(metrics.requests_in_flight(), 0);
    }

    #[test]
    fn guard_retains_only_metric_context_fields() {
        let (metrics, _receiver, emitter) = test_emitter();
        let request = prepared_request("grpc");
        assert!(!request.headers.is_empty());
        assert!(request.body.is_unique());
        assert!(request
            .options
            .grpc
            .as_ref()
            .is_some_and(|grpc| !grpc.messages.is_empty()));
        assert!(request.options.plugin.is_some());

        let guard = RequestMetricsGuard::new(emitter, &request);
        assert!(request.body.is_unique(), "guard cloned the request body");

        let RequestMetricsGuard {
            emitter: _,
            request: context,
            started: _,
            active: _,
        } = &guard;
        let RequestMetricContext {
            name,
            method,
            protocol,
            url,
        } = context;
        assert_eq!(name, &request.name);
        assert_eq!(method, &request.method);
        assert_eq!(protocol, &request.protocol);
        assert_eq!(url, &request.url);
        assert_eq!(
            std::mem::size_of::<RequestMetricContext>(),
            std::mem::size_of::<[String; 4]>()
        );

        drop(guard);
        assert_eq!(metrics.requests_in_flight(), 0);
    }
}

#[cfg(test)]
mod failure_grouping_tests {
    use super::{classify_transport_error, normalize_exception};

    #[test]
    fn classify_transport_error_buckets() {
        assert_eq!(
            classify_transport_error("read timed out after 5s"),
            "timeout"
        );
        assert_eq!(
            classify_transport_error("operation timeout reached"),
            "timeout"
        );
        assert_eq!(
            classify_transport_error("failed to resolve DNS for host"),
            "dns"
        );
        assert_eq!(
            classify_transport_error("TLS certificate verification failed"),
            "tls"
        );
        assert_eq!(
            classify_transport_error("connection refused"),
            "connection_refused"
        );
        assert_eq!(
            classify_transport_error("connection reset by peer"),
            "connection_reset"
        );
        assert_eq!(
            classify_transport_error("could not connect to upstream"),
            "connection"
        );
        assert_eq!(classify_transport_error("request cancelled"), "cancelled");
        assert_eq!(classify_transport_error("request canceled"), "cancelled");
        assert_eq!(classify_transport_error("something weird"), "transport");
    }

    #[test]
    fn normalize_exception_collapses_volatile_detail() {
        // Numbers and quoted strings become placeholders so the same logical
        // exception groups together regardless of runtime values.
        let a = normalize_exception("TypeError: cannot read property 'foo' of undefined at vu 42");
        let b = normalize_exception("TypeError: cannot read property 'bar' of undefined at vu 7");
        assert_eq!(a, b);
        assert!(a.starts_with("TypeError: cannot read property"));
    }

    #[test]
    fn normalize_exception_takes_first_line_and_caps_length() {
        let multi = normalize_exception("Error: boom\n  at foo (script.js:10)\n  at bar");
        assert_eq!(multi, "Error: boom");
        let long = "x".repeat(500);
        assert!(normalize_exception(&long).chars().count() <= 120);
    }

    #[test]
    fn normalize_exception_empty_is_stable() {
        assert_eq!(normalize_exception("   "), "exception");
        assert_eq!(normalize_exception("12345"), "N");
    }
}

#[cfg(test)]
mod fault_injection_tests {
    use super::{
        classify_transport_error, fault_drop_decision, sample_fault_jitter, FAULT_DROP_ERROR,
        FAULT_DROP_RESPONSE_ERROR,
    };
    use loadr_config::{Dur, FaultDistribution, LatencyFault};
    use rand::SeedableRng;
    use std::time::Duration;

    fn rng(seed: u64) -> rand::rngs::SmallRng {
        rand::rngs::SmallRng::seed_from_u64(seed)
    }

    #[test]
    fn uniform_jitter_stays_below_bound() {
        let latency = LatencyFault {
            jitter: Dur::from_millis(50),
            distribution: FaultDistribution::Uniform,
        };
        let mut r = rng(7);
        for _ in 0..1000 {
            let d = sample_fault_jitter(&latency, &mut r);
            assert!(d < Duration::from_millis(50), "sample {d:?} >= jitter");
        }
    }

    #[test]
    fn gaussian_jitter_clamped_at_zero() {
        let latency = LatencyFault {
            jitter: Dur::from_millis(50),
            distribution: FaultDistribution::Gaussian,
        };
        let mut r = rng(7);
        let mut zeros = 0u32;
        let mut positives = 0u32;
        for _ in 0..1000 {
            // Never negative (would panic in from_secs_f64).
            let d = sample_fault_jitter(&latency, &mut r);
            if d.is_zero() {
                zeros += 1;
            } else {
                positives += 1;
            }
        }
        // gaussian(mean=0): roughly half the samples clamp to zero.
        assert!(zeros > 300, "expected ~half zeros, got {zeros}");
        assert!(positives > 300, "expected ~half positives, got {positives}");
    }

    #[test]
    fn jitter_is_deterministic_for_a_seed() {
        for distribution in [FaultDistribution::Uniform, FaultDistribution::Gaussian] {
            let latency = LatencyFault {
                jitter: Dur::from_millis(20),
                distribution,
            };
            let (mut a, mut b) = (rng(42), rng(42));
            for _ in 0..100 {
                assert_eq!(
                    sample_fault_jitter(&latency, &mut a),
                    sample_fault_jitter(&latency, &mut b),
                );
            }
        }
    }

    #[test]
    fn zero_jitter_never_sleeps() {
        let latency = LatencyFault {
            jitter: Dur::ZERO,
            distribution: FaultDistribution::Uniform,
        };
        assert_eq!(sample_fault_jitter(&latency, &mut rng(1)), Duration::ZERO);
    }

    #[test]
    fn drop_decision_is_deterministic_for_a_seed() {
        let (mut a, mut b) = (rng(9), rng(9));
        for _ in 0..1000 {
            assert_eq!(
                fault_drop_decision(0.3, &mut a),
                fault_drop_decision(0.3, &mut b),
            );
        }
    }

    #[test]
    fn drop_decision_respects_rate() {
        // rate 0 never drops.
        let mut r = rng(11);
        assert!((0..1000).all(|_| !fault_drop_decision(0.0, &mut r)));

        let mut r = rng(11);
        let dropped = (0..10_000)
            .filter(|_| fault_drop_decision(0.2, &mut r))
            .count();
        assert!(
            (1500..2500).contains(&dropped),
            "rate 0.2 dropped {dropped}/10000"
        );

        let mut r = rng(11);
        let dropped = (0..1000)
            .filter(|_| fault_drop_decision(0.999, &mut r))
            .count();
        assert!(dropped > 950, "rate 0.999 dropped only {dropped}/1000");
    }

    #[test]
    fn fault_errors_classify_as_fault_injected() {
        assert_eq!(classify_transport_error(FAULT_DROP_ERROR), "fault_injected");
        assert_eq!(
            classify_transport_error(FAULT_DROP_RESPONSE_ERROR),
            "fault_injected"
        );
        // Ordinary transport errors are unaffected.
        assert_eq!(
            classify_transport_error("connection refused"),
            "connection_refused"
        );
    }
}
