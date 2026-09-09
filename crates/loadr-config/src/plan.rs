//! The loadr YAML test definition schema.
//!
//! Every type here derives `JsonSchema` so `loadr schema` can emit a JSON Schema
//! for editor validation/autocomplete.

use std::collections::BTreeMap;
use std::path::PathBuf;

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::duration::Dur;

fn is_false(b: &bool) -> bool {
    !*b
}
fn default_true() -> bool {
    true
}
fn is_true(b: &bool) -> bool {
    *b
}

/// Root of a loadr test definition file.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct TestPlan {
    /// Human-readable test name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Free-form description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Defaults applied to every request (base URL, headers, timeouts, TLS, ...).
    #[serde(default)]
    pub defaults: Defaults,
    /// Named environment overrides; selected with `loadr run -e <name>`.
    /// Each value is a partial test definition deep-merged over this file.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, serde_json::Value>,
    /// Variables available via `${vars.<name>}` interpolation.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub variables: IndexMap<String, serde_json::Value>,
    /// Secrets resolved at runtime from the environment or files; available via
    /// `${secrets.<name>}` and never printed in logs or reports.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub secrets: IndexMap<String, SecretSource>,
    /// Data sources for parameterization, available via `${data.<source>.<column>}`.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub data: IndexMap<String, DataSource>,
    /// Custom metrics that can be recorded from JS or YAML.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub metrics: IndexMap<String, CustomMetric>,
    /// Embedded JavaScript configuration (inline script or external file).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub js: Option<JsConfig>,
    /// Named scenarios; each runs concurrently with its own executor.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub scenarios: IndexMap<String, Scenario>,
    /// Pass/fail criteria over metrics, e.g. `p(95)<400`.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub thresholds: IndexMap<String, ThresholdList>,
    /// Metric outputs/exporters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputConfig>,
    /// System-metric collectors pulled in for load↔system correlation (the
    /// inverse of `outputs`): foreign metrics are normalized and overlaid on the
    /// run timeline. See the `observe` design RFC.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observe: Vec<ObserveConfig>,
    /// Plugins to load for this test.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<PluginRef>,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Defaults applied to all scenarios and requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Defaults {
    /// HTTP defaults (base URL, headers, timeout, TLS, redirects, proxy, ...).
    #[serde(default)]
    pub http: HttpDefaults,
    /// Tags added to every metric sample.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    /// Default think time applied between flow steps (overridable per scenario).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub think_time: Option<ThinkTimeSpec>,
}

/// HTTP client defaults.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct HttpDefaults {
    /// Base URL prepended to relative request URLs, e.g. `https://api.example.com`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Headers sent with every request (request-level headers take precedence).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    /// Request timeout (default `30s`).
    #[serde(default = "HttpDefaults::default_timeout")]
    pub timeout: Dur,
    /// Follow redirects (default `true`).
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub follow_redirects: bool,
    /// Maximum redirects to follow (default `10`).
    #[serde(default = "HttpDefaults::default_max_redirects")]
    pub max_redirects: u32,
    /// HTTP version preference (default `auto` = ALPN-negotiated).
    #[serde(default)]
    pub version: HttpVersionPref,
    /// Send `Accept-Encoding` and decompress responses (default `true`).
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub compression: bool,
    /// Reuse connections across iterations of the same VU (default `true`).
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub keep_alive: bool,
    /// HTTP/HTTPS proxy URL, e.g. `http://proxy.internal:3128`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// TLS settings (custom CAs, client certificates, verification).
    #[serde(default)]
    pub tls: TlsConfig,
    /// Automatic cookie handling (per-VU cookie jar, default `true`).
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub cookies: bool,
    /// Simulate a browser HTTP cache per VU: cacheable GET responses
    /// (`Cache-Control: max-age`, ETag/Last-Modified) are served from cache or
    /// revalidated with conditional requests — JMeter's HTTP Cache Manager.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cache: bool,
    /// Drop response bodies after reading them (saves memory in big runs).
    /// Extraction and body assertions still see the body; nothing is retained.
    #[serde(default, skip_serializing_if = "is_false")]
    pub discard_response_bodies: bool,
    /// Inject a W3C `traceparent` header on every request for distributed-trace
    /// correlation (like k6's tracing). The trace id is per request.
    #[serde(default, skip_serializing_if = "is_false")]
    pub tracing: bool,
    /// Resolve these hostnames to fixed addresses, bypassing DNS
    /// (`host` or `host:port` → `ip` or `ip:port`). k6's `options.hosts`.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub hosts: IndexMap<String, String>,
}

impl HttpDefaults {
    fn default_timeout() -> Dur {
        Dur::from_secs(30)
    }
    fn default_max_redirects() -> u32 {
        10
    }
}

impl Default for HttpDefaults {
    fn default() -> Self {
        HttpDefaults {
            base_url: None,
            headers: IndexMap::new(),
            timeout: Self::default_timeout(),
            follow_redirects: true,
            max_redirects: Self::default_max_redirects(),
            version: HttpVersionPref::default(),
            compression: true,
            keep_alive: true,
            proxy: None,
            tls: TlsConfig::default(),
            cookies: true,
            cache: false,
            discard_response_bodies: false,
            tracing: false,
            hosts: IndexMap::new(),
        }
    }
}

/// Preferred HTTP protocol version.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum HttpVersionPref {
    /// Negotiate via ALPN (HTTP/2 when the server supports it).
    #[default]
    Auto,
    /// Force HTTP/1.1.
    Http1,
    /// Negotiate HTTP/2 over TLS; plaintext URLs use HTTP/1.1.
    Http2,
    /// HTTP/2 with prior knowledge (no upgrade), including over plaintext.
    Http2PriorKnowledge,
}

/// TLS configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct TlsConfig {
    /// Skip server certificate verification (NOT for production use).
    #[serde(default, skip_serializing_if = "is_false")]
    pub insecure_skip_verify: bool,
    /// PEM file with additional trusted root CAs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate for mTLS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    /// PEM client private key for mTLS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    /// Override the SNI server name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
    /// Minimum TLS version: `1.2` or `1.3` (default `1.2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_version: Option<String>,
    /// Maximum TLS version: `1.2` or `1.3` (default: highest supported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_version: Option<String>,
}

// ---------------------------------------------------------------------------
// Scenarios & executors
// ---------------------------------------------------------------------------

/// A named scenario: an executor plus a workload (YAML `flow` and/or JS `exec`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Scenario {
    /// Executor type: how load is scheduled.
    pub executor: ExecutorKind,
    /// Number of VUs (`constant-vus`, `per-vu-iterations`, `shared-iterations`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vus: Option<u64>,
    /// Total run duration (`constant-vus`, `constant-arrival-rate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<Dur>,
    /// Iteration count (`per-vu-iterations`: per VU; `shared-iterations`: total).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u64>,
    /// Initial VU count (`ramping-vus`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_vus: Option<u64>,
    /// Ramp stages (`ramping-vus`: target VUs; `ramping-arrival-rate`: target rate).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<Stage>,
    /// Iterations started per `time_unit` (`constant-arrival-rate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<f64>,
    /// Initial rate (`ramping-arrival-rate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_rate: Option<f64>,
    /// Period the `rate` applies to (default `1s`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_unit: Option<Dur>,
    /// VUs pre-allocated for arrival-rate executors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_allocated_vus: Option<u64>,
    /// Maximum VUs an arrival-rate or externally-controlled executor may grow to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_vus: Option<u64>,
    /// Hard cap on scenario duration for iteration-based executors (default `10m`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration: Option<Dur>,
    /// Delay before this scenario starts, relative to test start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<Dur>,
    /// Time to let in-flight iterations finish after the scenario ends (default `30s`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graceful_stop: Option<Dur>,
    /// Like `graceful_stop` but for VUs removed while ramping down (`ramping-vus`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graceful_ramp_down: Option<Dur>,
    /// Name of an exported JS function to run per iteration (instead of/with `flow`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<String>,
    /// Exported JS function run once per VU before its first iteration
    /// (Locust `on_start` — e.g. log in once per user).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_start: Option<String>,
    /// Exported JS function run once per VU after its last iteration
    /// (Locust `on_stop` — e.g. log out).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_stop: Option<String>,
    /// Declarative iteration body: requests, think time, JS snippets, groups.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flow: Vec<Step>,
    /// Target iteration pacing: a constant-throughput timer for this scenario's VUs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pacing: Option<PacingSpec>,
    /// Default think time between request steps in this scenario.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub think_time: Option<ThinkTimeSpec>,
    /// Throttle: cap requests at a global rate ceiling regardless of the
    /// executor (Gatling `throttle`/`reachRps`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<ThrottleSpec>,
    /// Chaos fault injection: extra latency and/or dropped requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub faults: Option<FaultSpec>,
    /// Tags added to all samples from this scenario.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

/// Chaos / fault-injection hooks applied to every request in a scenario:
/// extra sampled latency before each send and/or a fraction of requests
/// dropped as transport-class failures (`error_kind=fault_injected`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct FaultSpec {
    /// Extra latency injected before each request is sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencyFault>,
    /// Probability in `[0, 1)` that a request is dropped: it is failed as a
    /// transport error instead of completing normally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_rate: Option<f64>,
    /// When the drop takes effect (default `before_send`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_mode: Option<DropMode>,
}

/// Extra per-request latency, sampled from the VU's seeded rng.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct LatencyFault {
    /// Jitter magnitude: the upper bound for `uniform`, the standard
    /// deviation for `gaussian` (mean 0, clamped at zero).
    pub jitter: Dur,
    /// Sampling distribution (default `uniform`).
    #[serde(default, skip_serializing_if = "FaultDistribution::is_default")]
    pub distribution: FaultDistribution,
}

/// Distribution the latency jitter is sampled from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FaultDistribution {
    /// `uniform(0..jitter)`.
    #[default]
    Uniform,
    /// `gaussian(mean=0, std=jitter)`, clamped at zero.
    Gaussian,
}

impl FaultDistribution {
    fn is_default(&self) -> bool {
        *self == FaultDistribution::Uniform
    }
}

/// When a drop-fault takes effect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DropMode {
    /// Fail the request without sending it (default).
    #[default]
    BeforeSend,
    /// Send the request, then discard the response and fail it.
    AfterResponse,
}

/// A request-rate ceiling for a scenario (Gatling-style throttle).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ThrottleSpec {
    /// Maximum requests per second across the whole scenario. Iterations block
    /// before each request until a token is available (token-bucket limiter).
    pub requests_per_second: f64,
}

/// Executor type, matching k6 semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutorKind {
    /// A fixed number of VUs iterating for a duration (closed model).
    #[default]
    ConstantVus,
    /// VU count ramps between stage targets (closed model).
    RampingVus,
    /// Iterations start at a fixed rate regardless of completion (open model).
    ConstantArrivalRate,
    /// Iteration start rate ramps between stage targets (open model).
    RampingArrivalRate,
    /// Each VU runs a fixed number of iterations.
    PerVuIterations,
    /// A shared pool of iterations split among VUs.
    SharedIterations,
    /// VU count and duration controlled at runtime (API/web UI/controller).
    ExternallyControlled,
}

impl ExecutorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutorKind::ConstantVus => "constant-vus",
            ExecutorKind::RampingVus => "ramping-vus",
            ExecutorKind::ConstantArrivalRate => "constant-arrival-rate",
            ExecutorKind::RampingArrivalRate => "ramping-arrival-rate",
            ExecutorKind::PerVuIterations => "per-vu-iterations",
            ExecutorKind::SharedIterations => "shared-iterations",
            ExecutorKind::ExternallyControlled => "externally-controlled",
        }
    }
}

/// One ramp stage: reach `target` over `duration`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    /// Stage length.
    pub duration: Dur,
    /// Target value at the end of the stage (VUs or rate, depending on executor).
    pub target: f64,
}

/// A strict, validated executor specification produced by [`Scenario::executor_spec`].
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorSpec {
    ConstantVus {
        vus: u64,
        duration: std::time::Duration,
    },
    RampingVus {
        start_vus: u64,
        stages: Vec<(std::time::Duration, u64)>,
    },
    ConstantArrivalRate {
        /// Iterations per second (already normalized by `time_unit`).
        rate: f64,
        duration: std::time::Duration,
        pre_allocated_vus: u64,
        max_vus: u64,
    },
    RampingArrivalRate {
        /// Iterations per second at t=0.
        start_rate: f64,
        /// Stages with rates normalized to per-second.
        stages: Vec<(std::time::Duration, f64)>,
        pre_allocated_vus: u64,
        max_vus: u64,
    },
    PerVuIterations {
        vus: u64,
        iterations: u64,
        max_duration: std::time::Duration,
    },
    SharedIterations {
        vus: u64,
        iterations: u64,
        max_duration: std::time::Duration,
    },
    ExternallyControlled {
        max_vus: u64,
        duration: Option<std::time::Duration>,
    },
}

impl ExecutorSpec {
    /// Total scheduled duration of the executor, excluding graceful stop.
    /// `None` means unbounded/externally controlled.
    pub fn scheduled_duration(&self) -> Option<std::time::Duration> {
        match self {
            ExecutorSpec::ConstantVus { duration, .. } => Some(*duration),
            ExecutorSpec::RampingVus { stages, .. } => Some(stages.iter().map(|(d, _)| *d).sum()),
            ExecutorSpec::ConstantArrivalRate { duration, .. } => Some(*duration),
            ExecutorSpec::RampingArrivalRate { stages, .. } => {
                Some(stages.iter().map(|(d, _)| *d).sum())
            }
            ExecutorSpec::PerVuIterations { max_duration, .. }
            | ExecutorSpec::SharedIterations { max_duration, .. } => Some(*max_duration),
            ExecutorSpec::ExternallyControlled { duration, .. } => *duration,
        }
    }

    /// Peak number of VUs this executor may use.
    pub fn peak_vus(&self) -> u64 {
        match self {
            ExecutorSpec::ConstantVus { vus, .. } => *vus,
            ExecutorSpec::RampingVus { start_vus, stages } => stages
                .iter()
                .map(|(_, t)| *t)
                .chain(std::iter::once(*start_vus))
                .max()
                .unwrap_or(0),
            ExecutorSpec::ConstantArrivalRate { max_vus, .. }
            | ExecutorSpec::RampingArrivalRate { max_vus, .. }
            | ExecutorSpec::ExternallyControlled { max_vus, .. } => *max_vus,
            ExecutorSpec::PerVuIterations { vus, .. }
            | ExecutorSpec::SharedIterations { vus, .. } => *vus,
        }
    }
}

impl Scenario {
    /// Validate executor parameters and produce the strict spec.
    pub fn executor_spec(&self) -> Result<ExecutorSpec, String> {
        let time_unit = self
            .time_unit
            .map(|d| d.as_duration())
            .unwrap_or(std::time::Duration::from_secs(1));
        if time_unit.is_zero() {
            return Err("`time_unit` must be greater than zero".into());
        }
        let per_second = |rate: f64| rate / time_unit.as_secs_f64();
        let default_max_duration = std::time::Duration::from_secs(600);

        match self.executor {
            ExecutorKind::ConstantVus => {
                let vus = self.vus.ok_or("`constant-vus` requires `vus`")?;
                let duration = self
                    .duration
                    .ok_or("`constant-vus` requires `duration`")?
                    .as_duration();
                if vus == 0 {
                    return Err("`vus` must be at least 1".into());
                }
                if duration.is_zero() {
                    return Err("`duration` must be greater than zero".into());
                }
                Ok(ExecutorSpec::ConstantVus { vus, duration })
            }
            ExecutorKind::RampingVus => {
                if self.stages.is_empty() {
                    return Err("`ramping-vus` requires at least one entry in `stages`".into());
                }
                let stages = self
                    .stages
                    .iter()
                    .map(|s| {
                        if s.target < 0.0 || s.target.fract() != 0.0 {
                            Err(format!(
                                "`ramping-vus` stage targets must be non-negative integers, got {}",
                                s.target
                            ))
                        } else {
                            Ok((s.duration.as_duration(), s.target as u64))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ExecutorSpec::RampingVus {
                    start_vus: self.start_vus.unwrap_or(0),
                    stages,
                })
            }
            ExecutorKind::ConstantArrivalRate => {
                let rate = self.rate.ok_or("`constant-arrival-rate` requires `rate`")?;
                let duration = self
                    .duration
                    .ok_or("`constant-arrival-rate` requires `duration`")?
                    .as_duration();
                let pre = self
                    .pre_allocated_vus
                    .ok_or("`constant-arrival-rate` requires `pre_allocated_vus`")?;
                if rate <= 0.0 {
                    return Err("`rate` must be greater than zero".into());
                }
                let max_vus = self.max_vus.unwrap_or(pre);
                if max_vus < pre {
                    return Err("`max_vus` cannot be less than `pre_allocated_vus`".into());
                }
                Ok(ExecutorSpec::ConstantArrivalRate {
                    rate: per_second(rate),
                    duration,
                    pre_allocated_vus: pre,
                    max_vus,
                })
            }
            ExecutorKind::RampingArrivalRate => {
                if self.stages.is_empty() {
                    return Err(
                        "`ramping-arrival-rate` requires at least one entry in `stages`".into(),
                    );
                }
                let pre = self
                    .pre_allocated_vus
                    .ok_or("`ramping-arrival-rate` requires `pre_allocated_vus`")?;
                let max_vus = self.max_vus.unwrap_or(pre);
                if max_vus < pre {
                    return Err("`max_vus` cannot be less than `pre_allocated_vus`".into());
                }
                let stages = self
                    .stages
                    .iter()
                    .map(|s| {
                        if s.target < 0.0 {
                            Err("`ramping-arrival-rate` stage targets must be non-negative"
                                .to_string())
                        } else {
                            Ok((s.duration.as_duration(), per_second(s.target)))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ExecutorSpec::RampingArrivalRate {
                    start_rate: per_second(self.start_rate.unwrap_or(0.0)),
                    stages,
                    pre_allocated_vus: pre,
                    max_vus,
                })
            }
            ExecutorKind::PerVuIterations => {
                let vus = self.vus.ok_or("`per-vu-iterations` requires `vus`")?;
                let iterations = self
                    .iterations
                    .ok_or("`per-vu-iterations` requires `iterations`")?;
                if vus == 0 || iterations == 0 {
                    return Err("`vus` and `iterations` must be at least 1".into());
                }
                Ok(ExecutorSpec::PerVuIterations {
                    vus,
                    iterations,
                    max_duration: self
                        .max_duration
                        .map(|d| d.as_duration())
                        .unwrap_or(default_max_duration),
                })
            }
            ExecutorKind::SharedIterations => {
                let vus = self.vus.ok_or("`shared-iterations` requires `vus`")?;
                let iterations = self
                    .iterations
                    .ok_or("`shared-iterations` requires `iterations`")?;
                if vus == 0 || iterations == 0 {
                    return Err("`vus` and `iterations` must be at least 1".into());
                }
                Ok(ExecutorSpec::SharedIterations {
                    vus,
                    iterations,
                    max_duration: self
                        .max_duration
                        .map(|d| d.as_duration())
                        .unwrap_or(default_max_duration),
                })
            }
            ExecutorKind::ExternallyControlled => {
                let max_vus = self
                    .max_vus
                    .ok_or("`externally-controlled` requires `max_vus`")?;
                if max_vus == 0 {
                    return Err("`max_vus` must be at least 1".into());
                }
                Ok(ExecutorSpec::ExternallyControlled {
                    max_vus,
                    duration: self.duration.map(|d| d.as_duration()),
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Flow steps
// ---------------------------------------------------------------------------

/// One step in a scenario flow, written as a single-key mapping:
/// `- request: {...}`, `- think_time: {...}`, `- js: "..."`, `- group: {...}`,
/// plus control flow: `- repeat: {...}`, `- while: {...}`, `- if: {...}`,
/// `- random: {...}`.
#[derive(Debug, Clone)]
pub enum Step {
    /// Execute a protocol request.
    Request(Box<RequestStep>),
    /// Pause (a JMeter-style timer).
    ThinkTime(ThinkTimeSpec),
    /// Run a JavaScript snippet or a named exported function.
    Js(JsStep),
    /// Group steps under a name; samples get a `group` tag.
    Group(GroupStep),
    /// Repeat nested steps a fixed number of times (Gatling `repeat`).
    Repeat(RepeatStep),
    /// Repeat nested steps while a JS condition holds (Gatling `during`/`asLongAs`).
    While(WhileStep),
    /// Branch on a JS condition (Gatling `doIf`).
    If(IfStep),
    /// Pick one branch at random — weighted, uniform or round-robin
    /// (Locust weighted tasks; Gatling `randomSwitch`/`uniformRandomSwitch`/`roundRobinSwitch`).
    Random(RandomStep),
    /// Iterate nested steps over a list (JMeter ForEach, Gatling `foreach`).
    Foreach(ForeachStep),
    /// Branch on a value matching named cases (JMeter Switch, Gatling `doSwitch`).
    Switch(SwitchStep),
    /// Repeat nested steps for a fixed duration (Gatling `during`).
    During(DuringStep),
    /// Retry nested steps until they succeed or an attempt budget runs out (Gatling `tryMax`).
    Retry(RetryStep),
    /// Run nested steps concurrently within one iteration (k6 `http.batch`).
    Parallel(ParallelStep),
    /// Synchronization barrier: hold VUs until `users` have arrived
    /// (JMeter Synchronizing Timer, Gatling `rendezVous`).
    Rendezvous(RendezvousStep),
}

const STEP_KINDS: &[&str] = &[
    "request",
    "think_time",
    "js",
    "group",
    "repeat",
    "while",
    "if",
    "random",
    "foreach",
    "switch",
    "during",
    "retry",
    "parallel",
    "rendezvous",
];

impl Serialize for Step {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Step::Request(r) => map.serialize_entry("request", r)?,
            Step::ThinkTime(t) => map.serialize_entry("think_time", t)?,
            Step::Js(j) => map.serialize_entry("js", j)?,
            Step::Group(g) => map.serialize_entry("group", g)?,
            Step::Repeat(r) => map.serialize_entry("repeat", r)?,
            Step::While(w) => map.serialize_entry("while", w)?,
            Step::If(i) => map.serialize_entry("if", i)?,
            Step::Random(r) => map.serialize_entry("random", r)?,
            Step::Foreach(f) => map.serialize_entry("foreach", f)?,
            Step::Switch(s) => map.serialize_entry("switch", s)?,
            Step::During(d) => map.serialize_entry("during", d)?,
            Step::Retry(r) => map.serialize_entry("retry", r)?,
            Step::Parallel(p) => map.serialize_entry("parallel", p)?,
            Step::Rendezvous(r) => map.serialize_entry("rendezvous", r)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Step {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StepVisitor;

        impl<'de> serde::de::Visitor<'de> for StepVisitor {
            type Value = Step;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "a step mapping with exactly one of: {}",
                    STEP_KINDS.join(", ")
                )
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Step, A::Error> {
                let key: Option<String> = map.next_key()?;
                let key = key.ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        "empty step; expected one of {}",
                        STEP_KINDS.join(", ")
                    ))
                })?;
                let step = match key.as_str() {
                    "request" => Step::Request(map.next_value()?),
                    "think_time" => Step::ThinkTime(map.next_value()?),
                    "js" => Step::Js(map.next_value()?),
                    "group" => Step::Group(map.next_value()?),
                    "repeat" => Step::Repeat(map.next_value()?),
                    "while" => Step::While(map.next_value()?),
                    "if" => Step::If(map.next_value()?),
                    "random" => Step::Random(map.next_value()?),
                    "foreach" => Step::Foreach(map.next_value()?),
                    "switch" => Step::Switch(map.next_value()?),
                    "during" => Step::During(map.next_value()?),
                    "retry" => Step::Retry(map.next_value()?),
                    "parallel" => Step::Parallel(map.next_value()?),
                    "rendezvous" => Step::Rendezvous(map.next_value()?),
                    other => {
                        let mut msg = format!(
                            "unknown step type `{other}`, expected one of {}",
                            STEP_KINDS.join(", ")
                        );
                        let mut best: Option<(f64, &str)> = None;
                        for cand in STEP_KINDS {
                            let score = strsim::jaro_winkler(other, cand);
                            if score > best.map(|(s, _)| s).unwrap_or(0.0) {
                                best = Some((score, cand));
                            }
                        }
                        if let Some((score, cand)) = best {
                            if score >= 0.78 {
                                msg.push_str(&format!(" (did you mean `{cand}`?)"));
                            }
                        }
                        return Err(serde::de::Error::custom(msg));
                    }
                };
                if let Some(extra) = map.next_key::<String>()? {
                    return Err(serde::de::Error::custom(format!(
                        "step has extra key `{extra}`; each step is a single-key mapping (check indentation)"
                    )));
                }
                Ok(step)
            }
        }

        deserializer.deserialize_map(StepVisitor)
    }
}

impl JsonSchema for Step {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Step".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let request = generator.subschema_for::<RequestStep>();
        let think_time = generator.subschema_for::<ThinkTimeSpec>();
        let js = generator.subschema_for::<JsStep>();
        let group = generator.subschema_for::<GroupStep>();
        let repeat = generator.subschema_for::<RepeatStep>();
        let while_ = generator.subschema_for::<WhileStep>();
        let if_ = generator.subschema_for::<IfStep>();
        let random = generator.subschema_for::<RandomStep>();
        let foreach = generator.subschema_for::<ForeachStep>();
        let switch = generator.subschema_for::<SwitchStep>();
        let during = generator.subschema_for::<DuringStep>();
        let retry = generator.subschema_for::<RetryStep>();
        let parallel = generator.subschema_for::<ParallelStep>();
        let rendezvous = generator.subschema_for::<RendezvousStep>();
        schemars::json_schema!({
            "title": "Step",
            "description": "One flow step: a single-key mapping",
            "anyOf": [
                { "type": "object", "properties": { "request": request }, "required": ["request"], "additionalProperties": false },
                { "type": "object", "properties": { "think_time": think_time }, "required": ["think_time"], "additionalProperties": false },
                { "type": "object", "properties": { "js": js }, "required": ["js"], "additionalProperties": false },
                { "type": "object", "properties": { "group": group }, "required": ["group"], "additionalProperties": false },
                { "type": "object", "properties": { "repeat": repeat }, "required": ["repeat"], "additionalProperties": false },
                { "type": "object", "properties": { "while": while_ }, "required": ["while"], "additionalProperties": false },
                { "type": "object", "properties": { "if": if_ }, "required": ["if"], "additionalProperties": false },
                { "type": "object", "properties": { "random": random }, "required": ["random"], "additionalProperties": false },
                { "type": "object", "properties": { "foreach": foreach }, "required": ["foreach"], "additionalProperties": false },
                { "type": "object", "properties": { "switch": switch }, "required": ["switch"], "additionalProperties": false },
                { "type": "object", "properties": { "during": during }, "required": ["during"], "additionalProperties": false },
                { "type": "object", "properties": { "retry": retry }, "required": ["retry"], "additionalProperties": false },
                { "type": "object", "properties": { "parallel": parallel }, "required": ["parallel"], "additionalProperties": false },
                { "type": "object", "properties": { "rendezvous": rendezvous }, "required": ["rendezvous"], "additionalProperties": false }
            ]
        })
    }
}

/// Iterate nested steps over a list, binding each element to a variable.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForeachStep {
    /// What to iterate. A `${...}` template resolving to a JSON array, an
    /// inline array, or a `js:` expression returning an array.
    pub items: serde_json::Value,
    /// Variable name bound to the current element each pass (default `item`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub var: Option<String>,
    /// Variable name bound to the 0-based index each pass (default `index`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Steps run for each element.
    pub steps: Vec<Step>,
}

/// Branch on a value matching named cases.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwitchStep {
    /// The value to switch on (`${...}` template, rendered then matched).
    pub value: String,
    /// Named branches; the case whose key equals the rendered value runs.
    pub cases: IndexMap<String, Vec<Step>>,
    /// Steps run when no case matches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default: Vec<Step>,
}

/// Repeat nested steps for a fixed duration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DuringStep {
    /// How long to keep looping.
    pub duration: Dur,
    /// The steps to repeat.
    pub steps: Vec<Step>,
    /// 0-based loop counter variable (default `index`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counter: Option<String>,
}

/// Retry nested steps until they succeed (no failed request) or the attempt
/// budget runs out.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryStep {
    /// Maximum attempts (default 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub times: Option<u64>,
    /// Optional JS success condition; when set, the block stops as soon as it is
    /// truthy. When unset, an attempt succeeds if it produced no failed request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// Pause between attempts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<Dur>,
    /// The steps to attempt.
    pub steps: Vec<Step>,
}

/// Run nested steps concurrently within one iteration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParallelStep {
    /// Branches to run at the same time. Each branch opens its own connection
    /// (no shared pool); extracted variables and cookies merge back afterwards.
    pub branches: Vec<Vec<Step>>,
}

/// A synchronization barrier.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RendezvousStep {
    /// Barrier name (VUs sharing a name rendezvous together).
    pub name: String,
    /// Release once this many VUs are waiting.
    pub users: u64,
    /// Give up waiting after this long (default `30s`) to avoid deadlock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Dur>,
}

/// Repeat nested steps a fixed number of times.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepeatStep {
    /// How many times to run `steps`.
    pub times: u64,
    /// The steps to repeat.
    pub steps: Vec<Step>,
    /// Variable name exposed to JS as the 0-based loop counter (default `index`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counter: Option<String>,
}

/// Repeat nested steps while a JavaScript condition is truthy.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WhileStep {
    /// JS expression evaluated before each pass; the loop runs while it is truthy.
    pub condition: String,
    /// The steps to repeat.
    pub steps: Vec<Step>,
    /// Hard cap on iterations to prevent runaway loops (default 10000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_iterations: Option<u64>,
}

/// Branch on a JavaScript condition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IfStep {
    /// JS expression; when truthy `then` runs, otherwise `else`.
    pub condition: String,
    /// Steps run when the condition is truthy.
    pub then: Vec<Step>,
    /// Steps run when the condition is falsy.
    #[serde(default, rename = "else", skip_serializing_if = "Vec::is_empty")]
    pub otherwise: Vec<Step>,
}

/// Strategy for choosing a branch in a `random` step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwitchStrategy {
    /// Choose by `weight` (default weight 1.0). Higher weight = more likely.
    #[default]
    Weighted,
    /// Each choice equally likely.
    Uniform,
    /// Cycle through choices in order, one per iteration (per VU).
    RoundRobin,
}

/// Pick one of several branches at random (or round-robin).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RandomStep {
    /// Selection strategy (default `weighted`).
    #[serde(default)]
    pub strategy: SwitchStrategy,
    /// The branches to choose between.
    pub choices: Vec<RandomChoice>,
}

/// One branch of a `random` step.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RandomChoice {
    /// Relative weight for the `weighted` strategy (default 1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
    /// Optional label, used in the `branch` tag on nested samples.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The steps for this branch.
    pub steps: Vec<Step>,
}

/// A JS step: a code string, or an object naming a function/file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum JsStep {
    /// Inline JS code, e.g. `js: "session.vars.n = Math.random()"`.
    Code(String),
    /// Reference to an exported function or a separate inline script.
    Detailed {
        /// Name of an exported function from the test's JS module.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call: Option<String>,
        /// Inline script body.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        script: Option<String>,
    },
}

/// A named group of steps.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GroupStep {
    /// Group name (becomes the `group` tag on nested samples).
    pub name: String,
    /// Steps inside the group.
    pub steps: Vec<Step>,
}

/// JMeter-style timers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThinkTimeSpec {
    /// Fixed pause.
    Constant { duration: Dur },
    /// Uniformly random pause in `[min, max]`.
    Uniform { min: Dur, max: Dur },
    /// Normally distributed pause (truncated at zero).
    Gaussian { mean: Dur, std_dev: Dur },
}

/// Constant-throughput pacing: each VU spaces iteration starts so the scenario
/// approaches `iterations_per_second` overall (JMeter constant throughput timer).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PacingSpec {
    /// Target iteration starts per second across the whole scenario.
    pub iterations_per_second: f64,
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// A protocol request step.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct RequestStep {
    /// Request name used in metrics and reports (defaults to the URL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Protocol: inferred from the URL scheme when omitted
    /// (`http(s)` → http, `ws(s)` → websocket, `grpc(s)` → grpc, `tcp`/`udp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// HTTP method (default `GET`); ignored by non-HTTP protocols.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Absolute URL or path relative to `defaults.http.base_url`. Supports `${...}`.
    pub url: String,
    /// Request headers (HTTP) or metadata (gRPC). Values support `${...}`.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    /// Query parameters appended to the URL.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub params: IndexMap<String, String>,
    /// Request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Body>,
    /// Per-request timeout override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Dur>,
    /// Per-request redirect override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_redirects: Option<bool>,
    /// Tags added to this request's samples.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    /// Values to extract from the response for later steps.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extract: Vec<Extractor>,
    /// Assertions: failures mark the request failed (and can abort, see `on_failure`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assert: Vec<Condition>,
    /// Checks: recorded to the `checks` metric, never fail the request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Condition>,
    /// WebSocket-specific options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws: Option<WsOptions>,
    /// gRPC-specific options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GrpcOptions>,
    /// GraphQL-specific options (sent over HTTP POST).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graphql: Option<GraphqlOptions>,
    /// Raw TCP/UDP socket options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<SocketOptions>,
    /// Server-Sent Events stream options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse: Option<SseOptions>,
    /// SQL (PostgreSQL / MySQL) query options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlOptions>,
    /// Free-form options for protocol *plugins* (e.g. the `mongo` plugin reads
    /// `operation`/`collection`/`document`). Passed through verbatim to the
    /// plugin as `options.plugin` after `${...}` interpolation of string
    /// leaves. The host does not interpret these keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<serde_json::Value>,
}

/// Request body: a plain string, or a structured spec.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Body {
    /// Raw string body (supports `${...}`).
    Text(String),
    /// Structured body.
    Spec(BodySpec),
}

/// Structured request body. Exactly one of the fields must be set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct BodySpec {
    /// JSON body (sets `Content-Type: application/json`). String leaves support `${...}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<serde_json::Value>,
    /// Body read from a file at run start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    /// URL-encoded form (sets `Content-Type: application/x-www-form-urlencoded`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<IndexMap<String, String>>,
    /// Multipart form data (sets `Content-Type: multipart/form-data`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart: Option<Vec<MultipartPart>>,
}

/// One part of a multipart body.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MultipartPart {
    /// Form field name.
    pub name: String,
    /// Literal value (supports `${...}`); mutually exclusive with `file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// File whose contents become the part body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    /// Part content type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Filename sent in the part headers (defaults to the file's name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
}

/// WebSocket request options.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct WsOptions {
    /// Subprotocols offered in the handshake.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subprotocols: Vec<String>,
    /// Messages to send after connecting.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub send: Vec<WsMessage>,
    /// Number of messages to wait for before closing (default: one per sent message).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_count: Option<u64>,
    /// Close when a received text message contains this substring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_until: Option<String>,
    /// Max time to keep the connection open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_duration: Option<Dur>,
}

/// A WebSocket message to send.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum WsMessage {
    /// Text frame (supports `${...}`).
    Text(String),
    /// Structured frame.
    Detailed {
        /// Text payload (mutually exclusive with `binary_base64`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        /// Binary payload, base64-encoded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary_base64: Option<String>,
        /// Pause before sending this frame.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay: Option<Dur>,
    },
}

/// gRPC request options.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct GrpcOptions {
    /// `.proto` files defining the service (compiled in-process; no protoc needed).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proto_files: Vec<PathBuf>,
    /// Include paths for proto imports.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proto_includes: Vec<PathBuf>,
    /// Use server reflection instead of proto files.
    #[serde(default, skip_serializing_if = "is_false")]
    pub reflection: bool,
    /// Fully-qualified service name, e.g. `helloworld.Greeter`.
    pub service: String,
    /// Method name, e.g. `SayHello`.
    pub method: String,
    /// Request message as JSON (string leaves support `${...}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<serde_json::Value>,
    /// Messages for client/bidi streaming calls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<serde_json::Value>,
    /// Send `messages` N times in one call, one frame per copy. Plugin-backed
    /// `${data...}` leaves are re-fetched per frame, so each copy carries a
    /// fresh row (e.g. a distinct signed transaction). Must be >= 1 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_repeat: Option<usize>,
    /// gRPC metadata (in addition to request `headers`).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub metadata: IndexMap<String, String>,
    /// Share a fixed pool of N HTTP/2 channels across all VUs for this endpoint,
    /// round-robined (vs. the default: one connection per VU). Recommended for
    /// high-concurrency runs against a single endpoint. Must be >= 1 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_pool_size: Option<usize>,
    /// Client transport: `channel` (default) or `raw`. The `LOADR_GRPC_TRANSPORT`
    /// env var overrides this for whole-fleet A/B runs.
    #[serde(default, skip_serializing_if = "GrpcTransport::is_default")]
    pub transport: GrpcTransport,
}

/// Client transport driving gRPC calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GrpcTransport {
    /// tonic `Channel`: a tower::buffer queue plus a worker task per channel.
    #[default]
    Channel,
    /// Direct hyper HTTP/2 driven from the VU task (experimental perf path).
    Raw,
}

impl GrpcTransport {
    fn is_default(&self) -> bool {
        *self == GrpcTransport::Channel
    }
}

/// GraphQL request options.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct GraphqlOptions {
    /// The GraphQL query/mutation document.
    pub query: String,
    /// Variables object (string leaves support `${...}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variables: Option<serde_json::Value>,
    /// Operation name when the document contains several.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
}

/// Raw TCP/UDP options.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SocketOptions {
    /// Payload to send: UTF-8 text (supports `${...}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_text: Option<String>,
    /// Payload to send: hex-encoded bytes, e.g. `dead beef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_hex: Option<String>,
    /// Read until this many bytes have been received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_bytes: Option<u64>,
    /// Read until the connection closes (TCP) or first datagram (UDP).
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_until_close: bool,
    /// Read timeout (default: request timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_timeout: Option<Dur>,
}

/// Server-Sent Events options: how long to keep the stream open.
///
/// With none set, the stream is read until the request `timeout` elapses or
/// the server closes the connection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SseOptions {
    /// Stop after this many events have been received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<u64>,
    /// Stop once an event whose `event:` type equals this value arrives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// Stop after this wall-clock duration (capped by the request timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<Dur>,
}

/// SQL request options for the `postgres://` and `mysql://` protocols.
///
/// The query is executed as the "request"; latency, the number of rows
/// returned (SELECT) or affected (INSERT/UPDATE/DELETE), and any database
/// error are recorded as metrics. Parameters are bound positionally using the
/// driver's placeholder syntax (`$1, $2, ...` for postgres, `?` for mysql).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SqlOptions {
    /// The SQL statement to execute (supports `${...}` templating).
    pub query: String,
    /// Positional bind parameters (string leaves support `${...}`). Each value
    /// is bound as text; the database coerces it to the column type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

/// Which of several matches to take.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatchIndex {
    #[default]
    First,
    Last,
    Random,
    All,
}

/// Extract a value from a response into a variable usable in later steps
/// (`${name}`) and in JS (`session.vars.name`).
///
/// Two forms, distinguished by their keys:
/// - the classic **typed** extractor (`{ type: jsonpath, name: ..., ... }`);
/// - the fused **chain** (`{ chain: name, jmespath: ..., as: ..., check: ... }`).
///
/// Both are accepted anywhere `extract:` appears; mix them freely.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Extractor {
    /// Fused extract → coerce → transform → validate → save chain.
    ///
    /// Disambiguated from the classic form by its `chain:` key. See [`ChainSpec`].
    /// Boxed because the chain spec is much larger than the classic variant.
    Chain(Box<ChainSpec>),
    /// Classic single-purpose extractor (`type:` discriminator).
    Classic(ClassicExtractor),
}

/// Extract a value from a response into a variable usable in later steps
/// (`${name}`) and in JS (`session.vars.name`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClassicExtractor {
    /// JSONPath over a JSON body, e.g. `$.items[0].id`.
    Jsonpath {
        /// Variable name to store the result under.
        name: String,
        /// JSONPath expression.
        expression: String,
        /// Value used when nothing matches (otherwise extraction fails).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<MatchIndex>,
    },
    /// Regular expression over the body; capture group 1 by default.
    Regex {
        name: String,
        /// Regular expression with at least one capture group.
        expression: String,
        /// Capture group to extract (default 1; 0 = whole match).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        group: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<MatchIndex>,
    },
    /// XPath 1.0 over an XML body.
    Xpath {
        name: String,
        expression: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
    /// CSS selector over an HTML body.
    Css {
        name: String,
        /// Selector, e.g. `form input[name=csrf]`.
        expression: String,
        /// Attribute to read; omitted = element text.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribute: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<MatchIndex>,
    },
    /// JMeter-style boundary extractor: text between `left` and `right`.
    Boundary {
        name: String,
        left: String,
        right: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<MatchIndex>,
    },
    /// Response header value.
    Header {
        name: String,
        /// Header name, case-insensitive.
        header: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
}

impl ClassicExtractor {
    pub fn name(&self) -> &str {
        match self {
            ClassicExtractor::Jsonpath { name, .. }
            | ClassicExtractor::Regex { name, .. }
            | ClassicExtractor::Xpath { name, .. }
            | ClassicExtractor::Css { name, .. }
            | ClassicExtractor::Boundary { name, .. }
            | ClassicExtractor::Header { name, .. } => name,
        }
    }
}

impl Extractor {
    pub fn name(&self) -> &str {
        match self {
            Extractor::Classic(c) => c.name(),
            Extractor::Chain(c) => &c.name,
        }
    }
}

// ---------------------------------------------------------------------------
// Fused check chains (extract → coerce → transform → validate → save)
// ---------------------------------------------------------------------------

/// A fused, declarative extraction chain (Gatling-style).
///
/// ```yaml
/// extract:
///   - chain: cheapest_in_stock          # variable name to save
///     jmespath: "items[?stock > `0`] | sort_by(@, &price)[0].name"
///     as: string                        # optional: int | float | bool | string
///     transform: [trim, lowercase]      # optional: ordered pipeline
///     default: "none"                   # optional: used when nothing matches
///     check:                            # optional: validate before saving
///       not_empty: true
///       matches: "^[a-z-]+$"
/// ```
///
/// Exactly one source field must be set (`jmespath`, `jsonpath`, `regex`,
/// `header`, `css`, `xpath` or `boundary`). The `chain` key is the variable
/// name; everything else is optional.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ChainSpec {
    /// Variable name to save the final value under (`chain:` key).
    #[serde(rename = "chain")]
    pub name: String,

    // --- source (exactly one) ---
    /// JMESPath query over a JSON body, e.g. `items[?price > \`10\`] | [0].id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jmespath: Option<String>,
    /// JSONPath over a JSON body, e.g. `$.items[0].id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonpath: Option<String>,
    /// Regular expression over the body (uses capture `group`, default 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    /// Response header name (case-insensitive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// CSS selector over an HTML body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub css: Option<String>,
    /// XPath 1.0 over an XML body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xpath: Option<String>,
    /// Boundary-extractor `left` marker (requires `right`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<String>,
    /// Boundary-extractor `right` marker (requires `left`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<String>,

    // --- source modifiers ---
    /// Regex capture group to take (default 1; 0 = whole match). Only for `regex`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<usize>,
    /// CSS attribute to read (omit for element text). Only for `css`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
    /// Which of several matches to take (jmespath/jsonpath/regex/css/boundary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<MatchIndex>,

    // --- coerce / transform / validate / save ---
    /// Coerce the extracted value to this type before transform/validate.
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    pub as_type: Option<CoerceType>,
    /// Ordered transform pipeline applied after coercion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transform: Vec<Transform>,
    /// Value used when the source matches nothing (otherwise the chain fails,
    /// counting the request as failed — same as the classic extractors).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Inline validation: the value must satisfy every check or the chain fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<ChainCheck>,
}

impl ChainSpec {
    /// `as_type` is serialized as `as` (a Rust keyword) — handled via the
    /// `#[serde(rename)]` below; this helper just reads it back.
    pub fn coerce(&self) -> Option<CoerceType> {
        self.as_type
    }
}

/// Type to coerce an extracted value into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoerceType {
    /// Parse as a 64-bit signed integer.
    Int,
    /// Parse as a 64-bit float.
    Float,
    /// Parse as a boolean (`true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`).
    Bool,
    /// Render as a string.
    String,
}

/// One transform applied to an extracted value (in pipeline order).
///
/// String forms (`trim`, `lowercase`, …) need no arguments; the object forms
/// (`{ replace: [from, to] }`, `{ prepend: "Bearer " }`, …) carry one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    /// Trim leading/trailing whitespace.
    Trim,
    /// Lowercase (ASCII + Unicode).
    Lowercase,
    /// Uppercase (ASCII + Unicode).
    Uppercase,
    /// URL-encode (percent-encoding for query/path components).
    UrlEncode,
    /// URL-decode percent-encoding.
    UrlDecode,
    /// Base64-encode (standard alphabet).
    Base64Encode,
    /// Base64-decode (standard alphabet) into UTF-8 text.
    Base64Decode,
    /// Append a literal suffix.
    Append(String),
    /// Prepend a literal prefix, e.g. `Bearer `.
    Prepend(String),
    /// Replace all occurrences of `[from, to]`.
    Replace(Vec<String>),
    /// Take a substring `[start, len]` (character offsets).
    Substring(Vec<usize>),
}

/// Inline validation for a [`ChainSpec`]: every set field must hold.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ChainCheck {
    /// Value must equal this (compared after coercion/transform).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equals: Option<serde_json::Value>,
    /// Value (as text) must match this regular expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matches: Option<String>,
    /// Value (as text) must be one of these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub one_of: Option<Vec<serde_json::Value>>,
    /// Numeric lower bound (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Numeric upper bound (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Value (as text) must be non-empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_empty: Option<bool>,
    /// What to do if validation fails (default: mark the request failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<FailureAction>,
}

// ---------------------------------------------------------------------------
// Assertions & checks
// ---------------------------------------------------------------------------

/// What to do when an assertion fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    /// Mark the request failed and continue (default).
    #[default]
    Continue,
    /// Mark failed and skip the rest of the iteration.
    AbortIteration,
    /// Mark failed and stop this scenario.
    AbortScenario,
    /// Mark failed and stop the whole test.
    AbortTest,
}

/// A condition over a response, usable as an assertion (`assert:`) or check (`checks:`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// Response status code (HTTP status, gRPC status code, ...).
    Status {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Exact status.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<i64>,
        /// Any of these statuses.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        one_of: Option<Vec<i64>>,
        /// Regex over the status, e.g. `2..`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        matches: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Descriptor-aware assertion over a top-level protobuf response field.
    /// Unlike JSONPath, this observes proto3 implicit defaults and explicit
    /// presence without converting the response message to JSON.
    ProtobufField {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Exact field name from the response message's `.proto` definition.
        field: String,
        /// Expected scalar value. Type compatibility is checked against the
        /// response descriptor when the gRPC call is resolved.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<serde_json::Value>,
        /// Require/forbid protobuf presence (default: require). Implicit
        /// proto3 scalars are always semantically present with their default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        /// Optional bounded semantic-failure grouping. Only listed numeric
        /// values receive their own group; all other values collapse to
        /// `other` rather than creating an unbounded metric series.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure_groups: Option<BTreeMap<i64, String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Body contains a substring.
    BodyContains {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        value: String,
        /// Invert: fail when the substring IS present.
        #[serde(default, skip_serializing_if = "is_false")]
        negate: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Body matches a regular expression.
    BodyMatches {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        pattern: String,
        #[serde(default, skip_serializing_if = "is_false")]
        negate: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// JSONPath assertion: existence and/or equality.
    Jsonpath {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        expression: String,
        /// Expected value of the first match.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<serde_json::Value>,
        /// Require/forbid a match (default: require).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// XPath assertion.
    Xpath {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        expression: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Response duration below a limit.
    Duration {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        max: Dur,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Response body size bounds.
    Size {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Response header value.
    Header {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Header name, case-insensitive.
        header: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        contains: Option<String>,
        /// Require/forbid presence (default: require).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exists: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
    /// Evaluate a JS expression; truthy passes. The response is in scope as `response`.
    Js {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        expression: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_failure: Option<FailureAction>,
    },
}

impl Condition {
    pub fn display_name(&self) -> String {
        let explicit = match self {
            Condition::Status { name, .. }
            | Condition::BodyContains { name, .. }
            | Condition::BodyMatches { name, .. }
            | Condition::Jsonpath { name, .. }
            | Condition::ProtobufField { name, .. }
            | Condition::Xpath { name, .. }
            | Condition::Duration { name, .. }
            | Condition::Size { name, .. }
            | Condition::Header { name, .. }
            | Condition::Js { name, .. } => name.clone(),
        };
        if let Some(n) = explicit {
            return n;
        }
        match self {
            Condition::Status {
                equals,
                one_of,
                matches,
                ..
            } => {
                if let Some(e) = equals {
                    format!("status is {e}")
                } else if let Some(o) = one_of {
                    format!("status in {o:?}")
                } else if let Some(m) = matches {
                    format!("status matches {m}")
                } else {
                    "status".to_string()
                }
            }
            Condition::BodyContains { value, negate, .. } => {
                if *negate {
                    format!("body does not contain {value:?}")
                } else {
                    format!("body contains {value:?}")
                }
            }
            Condition::BodyMatches { pattern, .. } => format!("body matches /{pattern}/"),
            Condition::Jsonpath { expression, .. } => format!("jsonpath {expression}"),
            Condition::ProtobufField { field, .. } => format!("protobuf field {field}"),
            Condition::Xpath { expression, .. } => format!("xpath {expression}"),
            Condition::Duration { max, .. } => format!("duration < {max}"),
            Condition::Size { .. } => "body size".to_string(),
            Condition::Header { header, .. } => format!("header {header}"),
            Condition::Js { expression, .. } => format!("js: {expression}"),
        }
    }

    pub fn on_failure(&self) -> FailureAction {
        match self {
            Condition::Status { on_failure, .. }
            | Condition::BodyContains { on_failure, .. }
            | Condition::BodyMatches { on_failure, .. }
            | Condition::Jsonpath { on_failure, .. }
            | Condition::ProtobufField { on_failure, .. }
            | Condition::Xpath { on_failure, .. }
            | Condition::Duration { on_failure, .. }
            | Condition::Size { on_failure, .. }
            | Condition::Header { on_failure, .. }
            | Condition::Js { on_failure, .. } => on_failure.unwrap_or_default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

/// Threshold list for one metric: a single expression or a list of entries.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ThresholdList {
    Single(String),
    Many(Vec<ThresholdEntry>),
}

impl ThresholdList {
    pub fn entries(&self) -> Vec<ThresholdEntry> {
        match self {
            ThresholdList::Single(s) => vec![ThresholdEntry::Expr(s.clone())],
            ThresholdList::Many(v) => v.clone(),
        }
    }
}

/// One threshold: an expression string, or an object with abort options.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ThresholdEntry {
    /// e.g. `p(95)<400`, `rate>0.99`, `avg<200`, `count>1000`.
    Expr(String),
    Detailed {
        /// Threshold expression.
        threshold: String,
        /// Abort the test as soon as this threshold fails.
        #[serde(default, skip_serializing_if = "is_false")]
        abort_on_fail: bool,
        /// Don't evaluate for aborting until this much time has passed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay_abort_eval: Option<Dur>,
    },
}

impl ThresholdEntry {
    pub fn expression(&self) -> &str {
        match self {
            ThresholdEntry::Expr(s) => s,
            ThresholdEntry::Detailed { threshold, .. } => threshold,
        }
    }
    pub fn abort_on_fail(&self) -> bool {
        match self {
            ThresholdEntry::Expr(_) => false,
            ThresholdEntry::Detailed { abort_on_fail, .. } => *abort_on_fail,
        }
    }
    pub fn delay_abort_eval(&self) -> Option<Dur> {
        match self {
            ThresholdEntry::Expr(_) => None,
            ThresholdEntry::Detailed {
                delay_abort_eval, ..
            } => *delay_abort_eval,
        }
    }
}

// ---------------------------------------------------------------------------
// Data, secrets, metrics, JS, outputs, plugins
// ---------------------------------------------------------------------------

/// How rows of a data source are handed to VUs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    /// All VUs draw from one shared, sequential cursor.
    #[default]
    Shared,
    /// Each VU gets its own cursor over the full data set.
    PerVu,
}

/// Behaviour when a data source is exhausted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnEof {
    /// Wrap around to the first row.
    #[default]
    Recycle,
    /// Stop the VU's iterations.
    Stop,
}

/// Row selection order (Gatling feeder strategies).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PickStrategy {
    /// Rows handed out in file order (the cursor advances by one each time).
    #[default]
    Sequential,
    /// A uniformly random row each time (rows may repeat; `on_eof` is ignored).
    Random,
    /// The full set shuffled once per VU, then read in that order.
    Shuffle,
}

/// A data source for parameterization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSource {
    /// CSV file; columns become fields: `${data.<source>.<column>}`.
    Csv {
        path: PathBuf,
        #[serde(default)]
        mode: DataMode,
        #[serde(default)]
        on_eof: OnEof,
        /// Row selection order (default `sequential`).
        #[serde(default)]
        pick: PickStrategy,
        /// Field delimiter (default `,`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delimiter: Option<char>,
        /// First row is a header (default `true`).
        #[serde(default = "default_true", skip_serializing_if = "is_true")]
        has_header: bool,
    },
    /// JSON file: an array of objects, each object a row.
    Json {
        path: PathBuf,
        #[serde(default)]
        mode: DataMode,
        #[serde(default)]
        on_eof: OnEof,
        #[serde(default)]
        pick: PickStrategy,
    },
    /// Inline rows defined in the YAML itself.
    Inline {
        rows: Vec<IndexMap<String, serde_json::Value>>,
        #[serde(default)]
        mode: DataMode,
        #[serde(default)]
        on_eof: OnEof,
        #[serde(default)]
        pick: PickStrategy,
    },
    /// Rows generated on demand by a `data_source`-capable plugin listed
    /// under `plugins:`. Rows are per-request, and per frame within a
    /// streaming request: each pulls a fresh row. A plugin that reports
    /// exhaustion retires the VU (like `on_eof: stop`). No
    /// `mode`/`on_eof`/`pick` — those describe stored-row iteration, which
    /// doesn't apply here.
    Plugin {
        /// Plugin name (a `plugins:` entry) that generates the rows.
        source: String,
        /// Source-level config passed to the plugin at init.
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        config: serde_json::Value,
        /// Run this source's `next_row` and `on_result` calls under
        /// `tokio::task::block_in_place`, so a CPU-heavy (signing, hashing) or
        /// I/O-backed feeder cannot stall the runtime's worker threads. Off by
        /// default: the bracket has a fixed cost that a cheap in-memory
        /// generator should not pay.
        #[serde(default, skip_serializing_if = "is_false")]
        blocking: bool,
        /// Hand every request result that used a row from this source back to
        /// the plugin's `result_sink`. Off by default: the response is
        /// serialised per request, which a plugin that ignores it shouldn't
        /// pay for.
        #[serde(default, skip_serializing_if = "is_false")]
        on_result: bool,
    },
}

/// Where a secret's value comes from.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SecretSource {
    /// Environment variable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// File whose (trimmed) contents are the secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
}

/// Kind of a custom metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricKindSpec {
    Counter,
    Gauge,
    Rate,
    Trend,
}

/// A user-defined metric, recordable from YAML (`js:` steps) and JS.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct CustomMetric {
    pub kind: MetricKindSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// For trend metrics: values are durations in milliseconds.
    #[serde(default, skip_serializing_if = "is_false")]
    pub time: bool,
}

/// Embedded JavaScript configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct JsConfig {
    /// External module file (ES module; may export setup/teardown/default/...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    /// Inline module source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    /// Per-iteration wall-clock limit for JS execution (default `10s`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Dur>,
    /// JS heap limit in MiB per VU runtime (default `64`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
}

/// Metric output/exporter configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    /// Newline-delimited JSON samples + periodic aggregates.
    Json { path: PathBuf },
    /// CSV samples.
    Csv { path: PathBuf },
    /// Prometheus: optional scrape endpoint and/or remote-write push.
    Prometheus {
        /// Scrape endpoint listen address, e.g. `127.0.0.1:9091`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        listen: Option<String>,
        /// Remote-write URL, e.g. `http://prometheus:9090/api/v1/write`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remote_write_url: Option<String>,
        /// Push interval (default `5s`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interval: Option<Dur>,
        /// Keep a standalone scrape endpoint alive for this long after
        /// completion so Prometheus can collect the terminal snapshot.
        /// Delays run exit by as much; disabled by default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_scrape_grace: Option<Dur>,
    },
    /// InfluxDB line protocol over HTTP.
    Influxdb {
        /// e.g. `http://influxdb:8086`.
        url: String,
        /// Database (v1) or bucket (v2).
        database: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        organization: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interval: Option<Dur>,
    },
    /// OpenTelemetry metrics (OTLP).
    Otlp {
        /// e.g. `http://otel-collector:4317` (gRPC) or `:4318` (HTTP).
        endpoint: String,
        #[serde(default)]
        protocol: OtlpProtocol,
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        headers: IndexMap<String, String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interval: Option<Dur>,
    },
    /// StatsD over UDP.
    Statsd {
        /// e.g. `127.0.0.1:8125`.
        address: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// An output provided by a plugin.
    Plugin {
        /// Plugin name (must be listed under `plugins:` or installed).
        name: String,
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        config: serde_json::Value,
    },
}

/// System-metric collector configuration — the inverse of [`OutputConfig`].
/// Each entry pulls foreign metrics in for the run window so they can be
/// correlated with (and overlaid on) the load metrics. `type`-discriminated so
/// new collectors slot in without breaking existing plans.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObserveConfig {
    /// Query a Prometheus server with a PromQL expression as a range query over
    /// the run window.
    Prometheus {
        /// Human label for this series (shown in logs/legend if `as` is unset).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Prometheus base URL, e.g. `http://prometheus:9090`.
        source: String,
        /// PromQL expression, evaluated as a range query.
        query: String,
        /// Canonical metric name shown in the report (default: `name`, else query).
        #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
        as_name: Option<String>,
        /// Unit hint for axis formatting: `ratio | percent | bytes | count | seconds`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<String>,
        /// Optional bearer token (use `${env.VAR}`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    /// Sample the local host's CPU / memory / disk / network from `/proc`,
    /// live on the load timeline. Linux-only; other platforms log a warning
    /// and produce no series.
    System {
        /// Metrics to sample: `cpu | memory | disk | network` (default: all).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        metrics: Vec<String>,
        /// Sampling interval (default `1s`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interval: Option<Dur>,
        /// Series name prefix (default `system`, giving `system_cpu`, …).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_prefix: Option<String>,
    },
}

/// OTLP transport.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    Http,
}

/// Reference to a plugin to load.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PluginRef {
    /// Plugin name (resolved in the plugins directory unless `path` is given).
    pub name: String,
    /// Explicit path to a `.wasm` component or native dynamic library.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// Plugin-specific configuration, passed through verbatim.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub config: serde_json::Value,
    /// Set to `false` to keep the entry but skip loading.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
name: checkout-flow
description: Browse and checkout

defaults:
  http:
    base_url: https://shop.example.com
    headers: { User-Agent: loadr/1.0 }
    timeout: 30s
  tags: { team: payments }

variables:
  api_key: ${env.API_KEY}

secrets:
  db_pass: { env: DB_PASS }

data:
  users:
    type: csv
    path: users.csv
    mode: shared
    on_eof: recycle

metrics:
  pages_viewed: { kind: counter, description: pages }

js:
  script: |
    export function setup() { return { token: "abc" }; }

scenarios:
  browse:
    executor: ramping-vus
    start_vus: 0
    stages:
      - { duration: 2m, target: 50 }
      - { duration: 5m, target: 50 }
    graceful_ramp_down: 10s
    flow:
      - request:
          name: home
          method: GET
          url: /
          extract:
            - { type: regex, name: csrf, expression: 'csrf" value="([^"]+)' }
          assert:
            - { type: status, equals: 200 }
            - { type: body_contains, value: Welcome }
          checks:
            - { type: duration, name: home fast, max: 500ms }
      - think_time: { type: uniform, min: 1s, max: 3s }
      - js: "session.counterAdd('pages_viewed', 1)"
      - group:
          name: checkout
          steps:
            - request:
                method: POST
                url: /cart
                body:
                  json: { item: 42, csrf: "${csrf}" }
  api:
    executor: constant-arrival-rate
    rate: 100
    time_unit: 1s
    duration: 5m
    pre_allocated_vus: 20
    max_vus: 100
    flow:
      - request: { url: /api/health }

thresholds:
  http_req_duration:
    - "p(95)<400"
    - { threshold: "p(99)<800", abort_on_fail: true, delay_abort_eval: 30s }
  checks: "rate>0.99"

outputs:
  - { type: json, path: results.jsonl }
  - { type: prometheus, listen: 127.0.0.1:9091 }

plugins:
  - { name: my-extractor, config: { mode: fast } }
"#;

    #[test]
    fn full_plan_round_trips() {
        let plan: TestPlan = serde_yaml::from_str(FULL).expect("parse");
        assert_eq!(plan.name.as_deref(), Some("checkout-flow"));
        assert_eq!(plan.scenarios.len(), 2);
        let browse = &plan.scenarios["browse"];
        assert_eq!(browse.executor, ExecutorKind::RampingVus);
        assert_eq!(browse.flow.len(), 4);
        match &browse.flow[0] {
            Step::Request(r) => {
                assert_eq!(r.name.as_deref(), Some("home"));
                assert_eq!(r.extract.len(), 1);
                assert_eq!(r.assert.len(), 2);
                assert_eq!(r.checks.len(), 1);
            }
            other => panic!("expected request, got {other:?}"),
        }
        let spec = browse.executor_spec().expect("spec");
        match spec {
            ExecutorSpec::RampingVus { start_vus, stages } => {
                assert_eq!(start_vus, 0);
                assert_eq!(stages.len(), 2);
                assert_eq!(stages[0].1, 50);
            }
            other => panic!("unexpected spec {other:?}"),
        }
        let api_spec = plan.scenarios["api"].executor_spec().expect("spec");
        match api_spec {
            ExecutorSpec::ConstantArrivalRate { rate, max_vus, .. } => {
                assert!((rate - 100.0).abs() < 1e-9);
                assert_eq!(max_vus, 100);
            }
            other => panic!("unexpected spec {other:?}"),
        }
        // Round-trip through YAML again.
        let yaml = serde_yaml::to_string(&plan).expect("serialize");
        let back: TestPlan = serde_yaml::from_str(&yaml).expect("reparse");
        assert_eq!(back.scenarios.len(), 2);
    }

    #[test]
    fn executor_validation_errors() {
        let mut s = Scenario {
            executor: ExecutorKind::ConstantVus,
            ..Default::default()
        };
        assert!(s.executor_spec().unwrap_err().contains("requires `vus`"));
        s.vus = Some(10);
        assert!(s
            .executor_spec()
            .unwrap_err()
            .contains("requires `duration`"));
        s.duration = Some(Dur::from_secs(60));
        assert!(s.executor_spec().is_ok());

        let s = Scenario {
            executor: ExecutorKind::ConstantArrivalRate,
            rate: Some(50.0),
            duration: Some(Dur::from_secs(60)),
            pre_allocated_vus: Some(20),
            max_vus: Some(5),
            ..Default::default()
        };
        assert!(s
            .executor_spec()
            .unwrap_err()
            .contains("`max_vus` cannot be less"));
    }

    #[test]
    fn protobuf_field_condition_round_trips() {
        let yaml = r#"
type: protobuf_field
name: admission_accepted
field: code
equals: 0
failure_groups:
  18: WrongShard
  20: PoolAtCapacity
"#;
        let condition: Condition = serde_yaml::from_str(yaml).expect("parse condition");
        match &condition {
            Condition::ProtobufField {
                name,
                field,
                equals,
                failure_groups,
                ..
            } => {
                assert_eq!(name.as_deref(), Some("admission_accepted"));
                assert_eq!(field, "code");
                assert_eq!(equals.as_ref(), Some(&serde_json::json!(0)));
                assert_eq!(failure_groups.as_ref().unwrap()[&18], "WrongShard");
            }
            other => panic!("expected protobuf condition, got {other:?}"),
        }
        let encoded = serde_yaml::to_string(&condition).expect("serialize condition");
        let _: Condition = serde_yaml::from_str(&encoded).expect("reparse condition");
    }

    #[test]
    fn rate_normalized_by_time_unit() {
        let s = Scenario {
            executor: ExecutorKind::ConstantArrivalRate,
            rate: Some(600.0),
            time_unit: Some(Dur::from_secs(60)),
            duration: Some(Dur::from_secs(60)),
            pre_allocated_vus: Some(10),
            ..Default::default()
        };
        match s.executor_spec().unwrap() {
            ExecutorSpec::ConstantArrivalRate { rate, .. } => {
                assert!(
                    (rate - 10.0).abs() < 1e-9,
                    "600/min should be 10/s, got {rate}"
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn unknown_field_rejected() {
        let err = serde_yaml::from_str::<TestPlan>("scenariosss: {}").unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn faults_block_parses() {
        let s: Scenario = serde_yaml::from_str(
            r#"
executor: constant-vus
vus: 1
duration: 1s
faults:
  latency: { jitter: 50ms, distribution: gaussian }
  drop_rate: 0.02
  drop_mode: before_send
flow:
  - request: { url: https://example.com/ }
"#,
        )
        .expect("parse");
        let faults = s.faults.expect("faults");
        let latency = faults.latency.expect("latency");
        assert_eq!(latency.jitter, Dur::from_millis(50));
        assert_eq!(latency.distribution, FaultDistribution::Gaussian);
        assert_eq!(faults.drop_rate, Some(0.02));
        assert_eq!(faults.drop_mode, Some(DropMode::BeforeSend));
    }

    #[test]
    fn faults_defaults() {
        // All fields optional; distribution defaults to uniform.
        let f: FaultSpec = serde_yaml::from_str("latency: { jitter: 10ms }").expect("parse");
        assert_eq!(
            f.latency.expect("latency").distribution,
            FaultDistribution::Uniform
        );
        assert_eq!(f.drop_rate, None);
        assert_eq!(f.drop_mode, None);
        let f: FaultSpec = serde_yaml::from_str("drop_rate: 0.5").expect("parse");
        assert!(f.latency.is_none());
        assert_eq!(f.drop_rate, Some(0.5));
        assert_eq!(DropMode::default(), DropMode::BeforeSend);
    }

    #[test]
    fn faults_round_trip_and_unknown_fields() {
        let f = FaultSpec {
            latency: Some(LatencyFault {
                jitter: Dur::from_millis(50),
                distribution: FaultDistribution::Gaussian,
            }),
            drop_rate: Some(0.02),
            drop_mode: Some(DropMode::AfterResponse),
        };
        let yaml = serde_yaml::to_string(&f).expect("serialize");
        let back: FaultSpec = serde_yaml::from_str(&yaml).expect("reparse");
        assert_eq!(back, f);
        // Unknown keys are rejected (`deny_unknown_fields`).
        assert!(serde_yaml::from_str::<FaultSpec>("drop_probability: 0.5").is_err());
        assert!(serde_yaml::from_str::<LatencyFault>("jitter: 1s\nshape: gaussian").is_err());
    }

    #[test]
    fn condition_display_names() {
        let c: Condition = serde_yaml::from_str("{ type: status, equals: 200 }").unwrap();
        assert_eq!(c.display_name(), "status is 200");
        let c: Condition =
            serde_yaml::from_str("{ type: duration, max: 500ms, name: fast }").unwrap();
        assert_eq!(c.display_name(), "fast");
    }

    #[test]
    fn plugin_data_source_round_trips() {
        let ds: DataSource =
            serde_yaml::from_str("{ type: plugin, source: tx-signer, config: { chain_id: t-1 } }")
                .expect("parse");
        match &ds {
            DataSource::Plugin {
                source,
                config,
                blocking,
                ..
            } => {
                assert_eq!(source, "tx-signer");
                assert_eq!(config["chain_id"], "t-1");
                assert!(!blocking, "blocking defaults to off");
            }
            other => panic!("expected DataSource::Plugin, got {other:?}"),
        }
        let yaml = serde_yaml::to_string(&ds).expect("serialize");
        let back: DataSource = serde_yaml::from_str(&yaml).expect("reparse");
        assert!(matches!(back, DataSource::Plugin { .. }));
    }

    #[test]
    fn plugin_data_source_blocking_flag_round_trips() {
        let ds: DataSource =
            serde_yaml::from_str("{ type: plugin, source: tx-signer, blocking: true }")
                .expect("parse");
        assert!(matches!(ds, DataSource::Plugin { blocking: true, .. }));
        let yaml = serde_yaml::to_string(&ds).expect("serialize");
        assert!(yaml.contains("blocking: true"), "explicit opt-in survives");
        let back: DataSource = serde_yaml::from_str(&yaml).expect("reparse");
        assert!(matches!(back, DataSource::Plugin { blocking: true, .. }));

        let off: DataSource =
            serde_yaml::from_str("{ type: plugin, source: tx-signer, blocking: false }")
                .expect("parse");
        assert!(matches!(
            off,
            DataSource::Plugin {
                blocking: false,
                ..
            }
        ));
        let yaml = serde_yaml::to_string(&off).expect("serialize");
        assert!(
            !yaml.contains("blocking"),
            "the default is not serialized back out"
        );
    }

    #[test]
    fn plugin_data_source_config_defaults_to_null() {
        let ds: DataSource =
            serde_yaml::from_str("{ type: plugin, source: tx-signer }").expect("parse");
        match &ds {
            DataSource::Plugin { source, config, .. } => {
                assert_eq!(source, "tx-signer");
                assert!(config.is_null());
            }
            other => panic!("expected DataSource::Plugin, got {other:?}"),
        }
    }
}
