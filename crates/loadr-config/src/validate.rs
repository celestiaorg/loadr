//! Semantic validation of a parsed [`TestPlan`], producing friendly diagnostics.

use std::collections::BTreeSet;
use std::path::Path;

use crate::diagnostics::{did_you_mean, Diagnostic, SpanIndex};
use crate::plan::*;
use crate::template::Template;
use crate::threshold::{MetricSelector, ThresholdExpr};

/// Options controlling validation strictness.
#[derive(Debug, Clone, Default)]
pub struct ValidateOptions {
    /// Check that referenced files (CSV data, proto files, body files, JS files)
    /// exist on disk, resolving relative paths against this directory.
    pub check_files_relative_to: Option<std::path::PathBuf>,
}

/// Built-in metric names usable in thresholds besides custom metrics.
pub const BUILTIN_METRICS: &[&str] = &[
    "http_reqs",
    "http_req_duration",
    "http_req_blocked",
    "http_req_connecting",
    "http_req_tls_handshaking",
    "http_req_sending",
    "http_req_waiting",
    "http_req_receiving",
    "http_req_failed",
    "iterations",
    "iteration_duration",
    "dropped_iterations",
    "vus",
    "vus_max",
    "requests_in_flight",
    "checks",
    "faults_injected",
    "data_sent",
    "data_received",
    "ws_connecting",
    "ws_session_duration",
    "ws_msgs_sent",
    "ws_msgs_received",
    "grpc_req_duration",
    "tcp_req_duration",
    "udp_req_duration",
    "graphql_req_duration",
];

const KNOWN_PROTOCOLS: &[&str] = &[
    "http",
    "https",
    "ws",
    "websocket",
    "grpc",
    "graphql",
    "sse",
    "sses",
    "browser",
    "tcp",
    "udp",
];
const HTTP_METHODS: &[&str] = &[
    "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT",
];

/// Validate a plan, locating diagnostics against the original source when given.
pub fn validate(plan: &TestPlan, source: Option<&str>, opts: &ValidateOptions) -> Vec<Diagnostic> {
    let index = source.map(SpanIndex::build).unwrap_or_default();
    let mut diags = Vec::new();
    let mut ctx = Ctx {
        plan,
        opts,
        diags: &mut diags,
    };

    if plan.scenarios.is_empty() {
        ctx.error("scenarios", "a test needs at least one scenario");
    }

    let needs_js = plan
        .scenarios
        .values()
        .any(|s| s.exec.is_some() || s.flow.iter().any(step_uses_js));
    if needs_js && plan.js.is_none() {
        // Inline `js:` snippets work without a module, but `exec:` does not.
        let exec_users: Vec<&str> = plan
            .scenarios
            .iter()
            .filter(|(_, s)| s.exec.is_some())
            .map(|(n, _)| n.as_str())
            .collect();
        if !exec_users.is_empty() {
            ctx.error(
                format!("scenarios.{}.exec", exec_users[0]),
                "`exec` names a JS function, but the test has no `js:` block",
            );
        }
    }
    if let Some(js) = &plan.js {
        if js.file.is_some() && js.script.is_some() {
            ctx.error("js", "set either `js.file` or `js.script`, not both");
        }
        if js.file.is_none() && js.script.is_none() {
            ctx.warning("js", "`js:` block is empty (no `file` or `script`)");
        }
        if let Some(f) = &js.file {
            ctx.check_file("js.file", f);
        }
    }

    // Secrets must name a source.
    for (name, src) in &plan.secrets {
        if src.env.is_none() && src.file.is_none() {
            ctx.error(
                format!("secrets.{name}"),
                "secret needs an `env` variable name or a `file` path",
            );
        }
        if let Some(f) = &src.file {
            ctx.check_file(format!("secrets.{name}.file"), f);
        }
    }

    // Data sources.
    for (name, ds) in &plan.data {
        match ds {
            DataSource::Csv {
                path, delimiter, ..
            } => {
                ctx.check_file(format!("data.{name}.path"), path);
                if let Some(d) = delimiter {
                    if !d.is_ascii() {
                        ctx.error(
                            format!("data.{name}.delimiter"),
                            "CSV delimiter must be a single ASCII character",
                        );
                    }
                }
            }
            DataSource::Json { path, .. } => {
                ctx.check_file(format!("data.{name}.path"), path);
            }
            DataSource::Inline { rows, .. } => {
                if rows.is_empty() {
                    ctx.error(format!("data.{name}.rows"), "inline data has no rows");
                }
            }
        }
    }

    // Scenarios.
    for (name, scenario) in &plan.scenarios {
        let base = format!("scenarios.{name}");
        if let Err(msg) = scenario.executor_spec() {
            ctx.error(format!("{base}.executor"), msg);
        }
        if scenario.flow.is_empty() && scenario.exec.is_none() {
            ctx.error(
                base.clone(),
                "scenario needs a `flow:` (YAML steps) and/or an `exec:` (JS function)",
            );
        }
        if let Some(p) = &scenario.pacing {
            if p.iterations_per_second <= 0.0 {
                ctx.error(
                    format!("{base}.pacing.iterations_per_second"),
                    "pacing rate must be greater than zero",
                );
            }
        }
        if let Some(tt) = &scenario.think_time {
            ctx.check_think_time(format!("{base}.think_time"), tt);
        }
        if let Some(faults) = &scenario.faults {
            ctx.check_faults(format!("{base}.faults"), faults);
        }
        let mut declared: BTreeSet<String> = plan.variables.keys().cloned().collect();
        ctx.check_steps(&base, "flow", &scenario.flow, &mut declared);
    }

    if let Some(tt) = &plan.defaults.think_time {
        ctx.check_think_time("defaults.think_time".to_string(), tt);
    }
    if let Some(proxy) = &plan.defaults.http.proxy {
        if url::Url::parse(proxy).is_err() {
            ctx.error(
                "defaults.http.proxy",
                format!("invalid proxy URL `{proxy}`"),
            );
        }
    }
    if let Some(base_url) = &plan.defaults.http.base_url {
        if Template::parse(base_url)
            .map(|t| t.is_literal())
            .unwrap_or(false)
            && url::Url::parse(base_url).is_err()
        {
            ctx.error(
                "defaults.http.base_url",
                format!("invalid base URL `{base_url}` (must be absolute, e.g. https://host)"),
            );
        }
    }
    for (path, tls_file) in [
        ("defaults.http.tls.ca_file", &plan.defaults.http.tls.ca_file),
        (
            "defaults.http.tls.cert_file",
            &plan.defaults.http.tls.cert_file,
        ),
        (
            "defaults.http.tls.key_file",
            &plan.defaults.http.tls.key_file,
        ),
    ] {
        if let Some(f) = tls_file {
            ctx.check_file(path, f);
        }
    }
    if plan.defaults.http.tls.cert_file.is_some() != plan.defaults.http.tls.key_file.is_some() {
        ctx.error(
            "defaults.http.tls",
            "mTLS needs both `cert_file` and `key_file`",
        );
    }

    // Thresholds.
    // Protocol plugins emit a `<name>_reqs` / `<name>_req_duration` /
    // `<name>_docs` / `<name>_rows` metric family derived from the plugin
    // handler name, so a declared plugin makes those metric names valid
    // threshold targets (`_docs` for Mongo-style document counts, `_rows` for
    // SQL row counts).
    let plugin_metrics: BTreeSet<String> = plan
        .plugins
        .iter()
        .flat_map(|p| {
            [
                format!("{}_reqs", p.name),
                format!("{}_req_duration", p.name),
                format!("{}_docs", p.name),
                format!("{}_rows", p.name),
            ]
        })
        .collect();
    // Observe sources feed the run's metric space too: named external series
    // (`as:`) and the system sampler's `<prefix>_{cpu,memory,disk_io,network}`
    // family are valid threshold targets.
    let observe_metrics: BTreeSet<String> = plan
        .observe
        .iter()
        .flat_map(|o| match o {
            crate::ObserveConfig::Prometheus {
                name,
                as_name,
                query,
                ..
            } => {
                vec![as_name
                    .clone()
                    .or_else(|| name.clone())
                    .unwrap_or_else(|| query.clone())]
            }
            crate::ObserveConfig::System { as_prefix, .. } => {
                let prefix = as_prefix.clone().unwrap_or_else(|| "system".to_string());
                ["cpu", "memory", "disk_io", "network"]
                    .iter()
                    .map(|m| format!("{prefix}_{m}"))
                    .collect()
            }
        })
        .collect();
    let known_metrics: BTreeSet<&str> = BUILTIN_METRICS
        .iter()
        .copied()
        .chain(plan.metrics.keys().map(|s| s.as_str()))
        .chain(plugin_metrics.iter().map(|s| s.as_str()))
        .chain(observe_metrics.iter().map(|s| s.as_str()))
        .collect();
    for (selector_str, list) in &plan.thresholds {
        let path = format!("thresholds.{selector_str}");
        match MetricSelector::parse(selector_str) {
            Ok(sel) => {
                if !known_metrics.contains(sel.metric.as_str()) {
                    let mut d = Diagnostic::warning(
                        path.clone(),
                        format!(
                            "`{}` is not a built-in or custom metric; the threshold will have no samples",
                            sel.metric
                        ),
                    );
                    if let Some(s) = did_you_mean(&sel.metric, known_metrics.iter().copied()) {
                        d = d.with_suggestion(s);
                    }
                    ctx.push(d);
                }
            }
            Err(e) => ctx.error(path.clone(), e.to_string()),
        }
        for entry in list.entries() {
            if let Err(e) = ThresholdExpr::parse(entry.expression()) {
                ctx.error(path.clone(), e.to_string());
            }
        }
    }

    // Outputs.
    for (i, out) in plan.outputs.iter().enumerate() {
        let path = format!("outputs[{i}]");
        match out {
            OutputConfig::Prometheus {
                listen,
                remote_write_url,
                ..
            } => {
                if listen.is_none() && remote_write_url.is_none() {
                    ctx.error(
                        path,
                        "prometheus output needs `listen` and/or `remote_write_url`",
                    );
                } else if let Some(rw) = remote_write_url {
                    if url::Url::parse(rw).is_err() {
                        ctx.error(format!("{path}.remote_write_url"), "invalid URL");
                    }
                }
            }
            OutputConfig::Influxdb { url: u, .. } if url::Url::parse(u).is_err() => {
                ctx.error(format!("{path}.url"), "invalid URL");
            }
            _ => {}
        }
    }

    // Locate all diagnostics.
    diags.into_iter().map(|d| d.locate(&index)).collect()
}

fn step_uses_js(step: &Step) -> bool {
    match step {
        Step::Js(_) => true,
        Step::While(_) | Step::If(_) => true, // conditions are JS expressions
        Step::Retry(r) => r.until.is_some() || r.steps.iter().any(step_uses_js),
        Step::Group(g) => g.steps.iter().any(step_uses_js),
        Step::Repeat(r) => r.steps.iter().any(step_uses_js),
        Step::Random(r) => r.choices.iter().any(|c| c.steps.iter().any(step_uses_js)),
        Step::Foreach(f) => {
            f.items
                .as_str()
                .map(|s| s.contains("${js:"))
                .unwrap_or(false)
                || f.steps.iter().any(step_uses_js)
        }
        Step::Switch(s) => {
            s.value.contains("${js:")
                || s.cases.values().any(|steps| steps.iter().any(step_uses_js))
                || s.default.iter().any(step_uses_js)
        }
        Step::During(d) => d.steps.iter().any(step_uses_js),
        Step::Parallel(p) => p.branches.iter().any(|b| b.iter().any(step_uses_js)),
        Step::Request(_) | Step::ThinkTime(_) | Step::Rendezvous(_) => false,
    }
}

struct Ctx<'a> {
    plan: &'a TestPlan,
    opts: &'a ValidateOptions,
    diags: &'a mut Vec<Diagnostic>,
}

impl Ctx<'_> {
    fn push(&mut self, d: Diagnostic) {
        self.diags.push(d);
    }
    fn error(&mut self, path: impl Into<String>, msg: impl Into<String>) {
        self.diags.push(Diagnostic::error(path, msg));
    }
    fn warning(&mut self, path: impl Into<String>, msg: impl Into<String>) {
        self.diags.push(Diagnostic::warning(path, msg));
    }

    fn check_file(&mut self, path: impl Into<String>, file: &Path) {
        if let Some(root) = &self.opts.check_files_relative_to {
            let resolved = if file.is_absolute() {
                file.to_path_buf()
            } else {
                root.join(file)
            };
            if !resolved.exists() {
                self.error(path, format!("file not found: {}", resolved.display()));
            }
        }
    }

    fn check_think_time(&mut self, path: String, tt: &ThinkTimeSpec) {
        if let ThinkTimeSpec::Uniform { min, max } = tt {
            if min > max {
                self.error(path, "`min` must not exceed `max`");
            }
        }
    }

    fn check_faults(&mut self, path: String, faults: &FaultSpec) {
        if let Some(rate) = faults.drop_rate {
            if !rate.is_finite() || !(0.0..1.0).contains(&rate) {
                self.error(
                    format!("{path}.drop_rate"),
                    "`drop_rate` must be a probability in [0, 1)",
                );
            }
        }
        if let Some(latency) = &faults.latency {
            if latency.jitter.is_zero() {
                self.error(
                    format!("{path}.latency.jitter"),
                    "`jitter` must be greater than zero",
                );
            }
        }
    }

    fn check_steps(
        &mut self,
        base: &str,
        field: &str,
        steps: &[Step],
        declared: &mut BTreeSet<String>,
    ) {
        for (i, step) in steps.iter().enumerate() {
            let path = format!("{base}.{field}[{i}]");
            match step {
                Step::Request(req) => self.check_request(&path, req, declared),
                Step::ThinkTime(tt) => self.check_think_time(format!("{path}.think_time"), tt),
                Step::Js(js) => {
                    if let JsStep::Detailed { call, script } = js {
                        if call.is_none() && script.is_none() {
                            self.error(format!("{path}.js"), "js step needs `call` or `script`");
                        }
                        if call.is_some() && self.plan.js.is_none() {
                            self.error(
                                format!("{path}.js.call"),
                                "`call` names a JS function, but the test has no `js:` block",
                            );
                        }
                    }
                }
                Step::Group(g) => {
                    if g.steps.is_empty() {
                        self.warning(format!("{path}.group"), "empty group");
                    }
                    self.check_steps(&format!("{path}.group"), "steps", &g.steps, declared);
                }
                Step::Repeat(r) => {
                    if r.times == 0 {
                        self.warning(format!("{path}.repeat.times"), "`repeat` runs zero times");
                    }
                    self.check_steps(&format!("{path}.repeat"), "steps", &r.steps, declared);
                }
                Step::While(w) => {
                    if w.condition.trim().is_empty() {
                        self.error(
                            format!("{path}.while.condition"),
                            "`while` needs a condition",
                        );
                    }
                    self.check_steps(&format!("{path}.while"), "steps", &w.steps, declared);
                }
                Step::If(c) => {
                    if c.condition.trim().is_empty() {
                        self.error(format!("{path}.if.condition"), "`if` needs a condition");
                    }
                    self.check_steps(&format!("{path}.if"), "then", &c.then, declared);
                    self.check_steps(&format!("{path}.if"), "else", &c.otherwise, declared);
                }
                Step::Random(r) => {
                    if r.choices.is_empty() {
                        self.error(
                            format!("{path}.random.choices"),
                            "`random` needs at least one choice",
                        );
                    }
                    for (ci, choice) in r.choices.iter().enumerate() {
                        if let Some(w) = choice.weight {
                            if w < 0.0 {
                                self.error(
                                    format!("{path}.random.choices[{ci}].weight"),
                                    "weight must not be negative",
                                );
                            }
                        }
                        self.check_steps(
                            &format!("{path}.random.choices[{ci}]"),
                            "steps",
                            &choice.steps,
                            declared,
                        );
                    }
                }
                Step::Foreach(f) => {
                    if let Some(name) = &f.var {
                        declared.insert(name.clone());
                    } else {
                        declared.insert("item".to_string());
                    }
                    declared.insert(f.index.clone().unwrap_or_else(|| "index".to_string()));
                    self.check_steps(&format!("{path}.foreach"), "steps", &f.steps, declared);
                }
                Step::Switch(s) => {
                    if let Err(e) = Template::parse(&s.value) {
                        self.error(format!("{path}.switch.value"), e.to_string());
                    }
                    if s.cases.is_empty() {
                        self.error(
                            format!("{path}.switch.cases"),
                            "`switch` needs at least one case",
                        );
                    }
                    for (key, steps) in &s.cases {
                        self.check_steps(
                            &format!("{path}.switch.cases.{key}"),
                            "steps",
                            steps,
                            declared,
                        );
                    }
                    self.check_steps(&format!("{path}.switch"), "default", &s.default, declared);
                }
                Step::During(d) => {
                    if d.duration.is_zero() {
                        self.warning(
                            format!("{path}.during.duration"),
                            "`during` duration is zero",
                        );
                    }
                    self.check_steps(&format!("{path}.during"), "steps", &d.steps, declared);
                }
                Step::Retry(r) => {
                    if r.times == Some(0) {
                        self.warning(
                            format!("{path}.retry.times"),
                            "`retry` allows zero attempts",
                        );
                    }
                    self.check_steps(&format!("{path}.retry"), "steps", &r.steps, declared);
                }
                Step::Parallel(p) => {
                    if p.branches.is_empty() {
                        self.error(
                            format!("{path}.parallel.branches"),
                            "`parallel` needs at least one branch",
                        );
                    }
                    for (bi, branch) in p.branches.iter().enumerate() {
                        self.check_steps(
                            &format!("{path}.parallel.branches[{bi}]"),
                            "",
                            branch,
                            declared,
                        );
                    }
                }
                Step::Rendezvous(r) => {
                    if r.users == 0 {
                        self.error(
                            format!("{path}.rendezvous.users"),
                            "`rendezvous` needs `users` >= 1",
                        );
                    }
                }
            }
        }
    }

    fn check_request(&mut self, path: &str, req: &RequestStep, declared: &mut BTreeSet<String>) {
        let rpath = format!("{path}.request");
        if req.url.is_empty() {
            self.error(format!("{rpath}.url"), "request URL is empty");
        }
        match Template::parse(&req.url) {
            Err(e) => self.error(format!("{rpath}.url"), e.to_string()),
            Ok(t) => {
                for expr in t.expressions() {
                    self.check_expr_reference(&format!("{rpath}.url"), expr, declared);
                }
            }
        }
        for (k, v) in &req.headers {
            if let Err(e) = Template::parse(v) {
                self.error(format!("{rpath}.headers.{k}"), e.to_string());
            }
        }
        if let Some(protocol) = &req.protocol {
            if !KNOWN_PROTOCOLS.contains(&protocol.as_str())
                && !self.plan.plugins.iter().any(|p| &p.name == protocol)
            {
                let mut d = Diagnostic::error(
                    format!("{rpath}.protocol"),
                    format!("unknown protocol `{protocol}`"),
                );
                if let Some(s) = did_you_mean(protocol, KNOWN_PROTOCOLS.iter().copied()) {
                    d = d.with_suggestion(s);
                } else {
                    d = d.with_suggestion(
                        "built-in protocols: http, ws, grpc, graphql, tcp, udp — or list a protocol plugin under `plugins:`",
                    );
                }
                self.push(d);
            }
        }
        if let Some(m) = &req.method {
            let upper = m.to_ascii_uppercase();
            if !HTTP_METHODS.contains(&upper.as_str()) {
                let mut d = Diagnostic::warning(
                    format!("{rpath}.method"),
                    format!("non-standard HTTP method `{m}`"),
                );
                if let Some(s) = did_you_mean(&upper, HTTP_METHODS.iter().copied()) {
                    d = d.with_suggestion(s);
                }
                self.push(d);
            }
        }
        if let Some(Body::Spec(spec)) = &req.body {
            let set = [
                spec.json.is_some(),
                spec.file.is_some(),
                spec.form.is_some(),
                spec.multipart.is_some(),
            ]
            .iter()
            .filter(|b| **b)
            .count();
            if set != 1 {
                self.error(
                    format!("{rpath}.body"),
                    "body must set exactly one of `json`, `file`, `form`, `multipart`",
                );
            }
            if let Some(f) = &spec.file {
                self.check_file(format!("{rpath}.body.file"), f);
            }
            if let Some(parts) = &spec.multipart {
                for (pi, part) in parts.iter().enumerate() {
                    if part.value.is_some() == part.file.is_some() {
                        self.error(
                            format!("{rpath}.body.multipart[{pi}]"),
                            "multipart part needs exactly one of `value` or `file`",
                        );
                    }
                    if let Some(f) = &part.file {
                        self.check_file(format!("{rpath}.body.multipart[{pi}].file"), f);
                    }
                }
            }
        }
        // gRPC options.
        if let Some(grpc) = &req.grpc {
            if grpc.proto_files.is_empty() && !grpc.reflection {
                self.error(
                    format!("{rpath}.grpc"),
                    "gRPC needs `proto_files` or `reflection: true`",
                );
            }
            for (fi, f) in grpc.proto_files.iter().enumerate() {
                self.check_file(format!("{rpath}.grpc.proto_files[{fi}]"), f);
            }
            if grpc.message.is_some() && !grpc.messages.is_empty() {
                self.error(
                    format!("{rpath}.grpc"),
                    "set `message` (unary) or `messages` (streaming), not both",
                );
            }
        }
        if let Some(sock) = &req.socket {
            if sock.send_text.is_some() && sock.send_hex.is_some() {
                self.error(
                    format!("{rpath}.socket"),
                    "set `send_text` or `send_hex`, not both",
                );
            }
            if let Some(hex) = &sock.send_hex {
                let cleaned: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
                if !cleaned.len().is_multiple_of(2)
                    || !cleaned.chars().all(|c| c.is_ascii_hexdigit())
                {
                    self.error(
                        format!("{rpath}.socket.send_hex"),
                        "invalid hex payload (use pairs of hex digits, whitespace allowed)",
                    );
                }
            }
        }
        if let Some(sql) = &req.sql {
            if sql.query.trim().is_empty() {
                self.error(
                    format!("{rpath}.sql.query"),
                    "SQL `query` must not be empty",
                );
            }
        }
        // Extractors: regexes must compile; names become available afterwards.
        for (ei, ex) in req.extract.iter().enumerate() {
            let epath = format!("{rpath}.extract[{ei}]");
            match ex {
                Extractor::Classic(ClassicExtractor::Regex {
                    expression, group, ..
                }) => match regex::Regex::new(expression) {
                    Err(e) => self.error(epath, format!("invalid regex: {e}")),
                    Ok(re) => {
                        let g = group.unwrap_or(1);
                        if g > re.captures_len().saturating_sub(1) {
                            self.error(
                                epath,
                                format!(
                                    "regex has {} capture group(s) but `group` is {g}",
                                    re.captures_len() - 1
                                ),
                            );
                        }
                    }
                },
                Extractor::Classic(ClassicExtractor::Jsonpath { expression, .. })
                    if serde_json_path::JsonPath::parse(expression).is_err() =>
                {
                    self.error(epath, format!("invalid JSONPath `{expression}`"));
                }
                Extractor::Chain(c) => self.check_chain(&epath, c),
                _ => {}
            }
            declared.insert(ex.name().to_string());
        }
        for (ci, cond) in req.assert.iter().chain(req.checks.iter()).enumerate() {
            let cpath = format!("{rpath}.assert_or_check[{ci}]");
            match cond {
                Condition::BodyMatches { pattern, .. } => {
                    if let Err(e) = regex::Regex::new(pattern) {
                        self.error(cpath, format!("invalid regex: {e}"));
                    }
                }
                Condition::Status {
                    equals,
                    one_of,
                    matches,
                    ..
                } => {
                    if equals.is_none() && one_of.is_none() && matches.is_none() {
                        self.error(
                            cpath.clone(),
                            "status condition needs `equals`, `one_of` or `matches`",
                        );
                    }
                    if let Some(m) = matches {
                        if let Err(e) = regex::Regex::new(m) {
                            self.error(cpath, format!("invalid regex: {e}"));
                        }
                    }
                }
                Condition::Jsonpath { expression, .. } => {
                    if serde_json_path::JsonPath::parse(expression).is_err() {
                        self.error(cpath, format!("invalid JSONPath `{expression}`"));
                    }
                }
                Condition::Size {
                    min, max, equals, ..
                } => {
                    if min.is_none() && max.is_none() && equals.is_none() {
                        self.error(cpath, "size condition needs `min`, `max` or `equals`");
                    }
                }
                Condition::Header {
                    equals,
                    contains,
                    exists,
                    ..
                } if equals.is_none() && contains.is_none() && exists.is_none() => {
                    self.error(
                        cpath,
                        "header condition needs `equals`, `contains` or `exists`",
                    );
                }
                _ => {}
            }
        }
    }

    /// Validate a fused extraction chain: exactly one source, valid
    /// expressions and a sane check block.
    fn check_chain(&mut self, path: &str, c: &ChainSpec) {
        // Exactly one source must be set.
        let mut sources = Vec::new();
        if c.jmespath.is_some() {
            sources.push("jmespath");
        }
        if c.jsonpath.is_some() {
            sources.push("jsonpath");
        }
        if c.regex.is_some() {
            sources.push("regex");
        }
        if c.header.is_some() {
            sources.push("header");
        }
        if c.css.is_some() {
            sources.push("css");
        }
        if c.xpath.is_some() {
            sources.push("xpath");
        }
        if c.left.is_some() || c.right.is_some() {
            sources.push("boundary");
        }
        match sources.len() {
            0 => self.error(
                path.to_string(),
                "chain has no source; set one of `jmespath`, `jsonpath`, `regex`, \
                 `header`, `css`, `xpath` or `left`/`right`",
            ),
            1 => {}
            _ => self.error(
                path.to_string(),
                format!(
                    "chain has multiple sources ({}); pick one",
                    sources.join(", ")
                ),
            ),
        }
        if (c.left.is_some()) != (c.right.is_some()) {
            self.error(
                path.to_string(),
                "boundary chain needs both `left` and `right`",
            );
        }
        // Validate source expressions.
        if let Some(expr) = &c.jmespath {
            if jmespath::compile(expr).is_err() {
                self.error(path.to_string(), format!("invalid JMESPath `{expr}`"));
            }
        }
        if let Some(expr) = &c.jsonpath {
            if serde_json_path::JsonPath::parse(expr).is_err() {
                self.error(path.to_string(), format!("invalid JSONPath `{expr}`"));
            }
        }
        if let Some(expr) = &c.regex {
            match regex::Regex::new(expr) {
                Err(e) => self.error(path.to_string(), format!("invalid regex: {e}")),
                Ok(re) => {
                    let g = c.group.unwrap_or(1);
                    if g > re.captures_len().saturating_sub(1) {
                        self.error(
                            path.to_string(),
                            format!(
                                "regex has {} capture group(s) but `group` is {g}",
                                re.captures_len() - 1
                            ),
                        );
                    }
                }
            }
        }
        // Validate the check block's regex/bounds.
        if let Some(check) = &c.check {
            if let Some(m) = &check.matches {
                if let Err(e) = regex::Regex::new(m) {
                    self.error(
                        path.to_string(),
                        format!("invalid regex in `check.matches`: {e}"),
                    );
                }
            }
            if let (Some(min), Some(max)) = (check.min, check.max) {
                if min > max {
                    self.error(
                        path.to_string(),
                        format!("`check.min` ({min}) is greater than `check.max` ({max})"),
                    );
                }
            }
            let empty = check.equals.is_none()
                && check.matches.is_none()
                && check.one_of.is_none()
                && check.min.is_none()
                && check.max.is_none()
                && check.not_empty.is_none();
            if empty {
                self.warning(
                    path.to_string(),
                    "chain `check` is empty; remove it or add a constraint",
                );
            }
        }
        // Transform argument arity.
        for t in &c.transform {
            match t {
                Transform::Replace(args) if args.len() != 2 => self.error(
                    path.to_string(),
                    "transform `replace` needs exactly [from, to]",
                ),
                Transform::Substring(args) if args.is_empty() || args.len() > 2 => self.error(
                    path.to_string(),
                    "transform `substring` needs [start] or [start, len]",
                ),
                _ => {}
            }
        }
    }

    /// Warn about references that cannot resolve at runtime.
    fn check_expr_reference(&mut self, path: &str, expr: &str, declared: &BTreeSet<String>) {
        if let Some(rest) = expr.strip_prefix("vars.") {
            if !self.plan.variables.contains_key(rest) {
                let mut d = Diagnostic::error(
                    path.to_string(),
                    format!("`${{vars.{rest}}}` is not defined under `variables:`"),
                );
                if let Some(s) = did_you_mean(rest, self.plan.variables.keys().map(|s| s.as_str()))
                {
                    d = d.with_suggestion(s);
                }
                self.push(d);
            }
        } else if let Some(rest) = expr.strip_prefix("secrets.") {
            if !self.plan.secrets.contains_key(rest) {
                self.error(
                    path.to_string(),
                    format!("`${{secrets.{rest}}}` is not defined under `secrets:`"),
                );
            }
        } else if let Some(rest) = expr.strip_prefix("data.") {
            let source = rest.split('.').next().unwrap_or(rest);
            if !self.plan.data.contains_key(source) {
                let mut d = Diagnostic::error(
                    path.to_string(),
                    format!("`${{data.{source}...}}` references an unknown data source"),
                );
                if let Some(s) = did_you_mean(source, self.plan.data.keys().map(|s| s.as_str())) {
                    d = d.with_suggestion(s);
                }
                self.push(d);
            }
        } else if !expr.starts_with("env.") && !expr.starts_with("js:") {
            // A bare name: extracted variable or built-in. Only warn when we are
            // sure it's not declared anywhere (extractors register their names).
            let builtin = matches!(expr, "vu" | "iteration" | "scenario");
            if !builtin && !declared.contains(expr) {
                self.warning(
                    path.to_string(),
                    format!(
                        "`${{{expr}}}` is not produced by any earlier `extract:` in this scenario; \
                         it must be set from JS at runtime"
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::Severity;

    fn plan_of(yaml: &str) -> TestPlan {
        serde_yaml::from_str(yaml).expect("parse")
    }

    fn errors(yaml: &str) -> Vec<Diagnostic> {
        validate(&plan_of(yaml), Some(yaml), &ValidateOptions::default())
            .into_iter()
            .filter(|d| d.severity == Severity::Error)
            .collect()
    }

    #[test]
    fn empty_plan_needs_scenarios() {
        let diags = errors("name: x");
        assert!(diags.iter().any(|d| d.path == "scenarios"));
    }

    #[test]
    fn valid_plan_has_no_errors() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 5
    duration: 30s
    flow:
      - request: { url: https://example.com/ }
"#;
        assert!(errors(yaml).is_empty());
    }

    #[test]
    fn missing_executor_field_is_located() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    flow:
      - request: { url: https://example.com/ }
"#;
        let diags = errors(yaml);
        let d = diags
            .iter()
            .find(|d| d.message.contains("requires `vus`"))
            .expect("vus error");
        assert!(d.line.is_some(), "should carry a source line");
    }

    #[test]
    fn faults_validation_errors() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    faults:
      latency: { jitter: 0s }
      drop_rate: 1.0
    flow:
      - request: { url: https://example.com/ }
"#;
        let diags = errors(yaml);
        assert!(
            diags
                .iter()
                .any(|d| d.path == "scenarios.s.faults.drop_rate"),
            "{diags:?}"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.path == "scenarios.s.faults.latency.jitter"),
            "{diags:?}"
        );
    }

    #[test]
    fn faults_valid_range_accepted() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    faults:
      latency: { jitter: 50ms, distribution: gaussian }
      drop_rate: 0.0
      drop_mode: after_response
    flow:
      - request: { url: https://example.com/ }
"#;
        assert!(errors(yaml).is_empty(), "{:?}", errors(yaml));
    }

    #[test]
    fn negative_drop_rate_rejected() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    faults: { drop_rate: -0.1 }
    flow:
      - request: { url: https://example.com/ }
"#;
        let diags = errors(yaml);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("[0, 1)") && d.path.ends_with("drop_rate")),
            "{diags:?}"
        );
    }

    #[test]
    fn faults_injected_is_a_known_threshold_metric() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    faults: { drop_rate: 0.5 }
    flow:
      - request: { url: https://example.com/ }
thresholds:
  faults_injected: "count<100"
"#;
        let diags = validate(&plan_of(yaml), Some(yaml), &ValidateOptions::default());
        assert!(
            diags.iter().all(|d| !d.path.starts_with("thresholds")),
            "{diags:?}"
        );
    }

    #[test]
    fn requests_in_flight_is_a_known_threshold_metric() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request: { url: https://example.com/ }
thresholds:
  requests_in_flight: "value<10"
"#;
        let diags = validate(&plan_of(yaml), Some(yaml), &ValidateOptions::default());
        assert!(
            diags.iter().all(|d| !d.path.starts_with("thresholds")),
            "{diags:?}"
        );
    }

    #[test]
    fn bad_regex_extractor_reported() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract:
            - { type: regex, name: x, expression: "([" }
"#;
        let diags = errors(yaml);
        assert!(diags.iter().any(|d| d.message.contains("invalid regex")));
    }

    #[test]
    fn undefined_var_suggests_close_match() {
        let yaml = r#"
variables: { api_key: secret }
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request: { url: "https://example.com/?k=${vars.api_kye}" }
"#;
        let diags = errors(yaml);
        let d = diags
            .iter()
            .find(|d| d.message.contains("api_kye"))
            .expect("undefined var");
        assert_eq!(d.suggestion.as_deref(), Some("did you mean `api_key`?"));
    }

    #[test]
    fn extracted_names_count_as_declared() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract: [ { type: regex, name: token, expression: "t=(\\w+)" } ]
      - request:
          url: "https://example.com/use?t=${token}"
"#;
        let plan = plan_of(yaml);
        let diags = validate(&plan, Some(yaml), &ValidateOptions::default());
        assert!(
            diags.iter().all(|d| !d.message.contains("token")),
            "{diags:?}"
        );
    }

    #[test]
    fn chain_extractor_validates_and_declares() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract:
            - chain: cheapest
              jmespath: "items[?price > `10`] | [0].name"
              as: string
              transform: [trim, lowercase]
              check: { not_empty: true }
      - request:
          url: "https://example.com/use?n=${cheapest}"
"#;
        let plan = plan_of(yaml);
        let diags = validate(&plan, Some(yaml), &ValidateOptions::default());
        let errs: Vec<_> = diags
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .collect();
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
        // The chain's `name` must count as a declared variable.
        assert!(
            diags.iter().all(|d| !d.message.contains("cheapest")),
            "{diags:?}"
        );
    }

    #[test]
    fn chain_without_source_is_rejected() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract:
            - { chain: oops, as: int }
"#;
        let diags = errors(yaml);
        assert!(diags.iter().any(|d| d.message.contains("no source")));
    }

    #[test]
    fn chain_multiple_sources_is_rejected() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract:
            - { chain: oops, jmespath: "a", jsonpath: "$.a" }
"#;
        let diags = errors(yaml);
        assert!(diags.iter().any(|d| d.message.contains("multiple sources")));
    }

    #[test]
    fn chain_bad_jmespath_is_rejected() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: https://example.com/
          extract:
            - { chain: oops, jmespath: "items[?" }
"#;
        let diags = errors(yaml);
        assert!(diags.iter().any(|d| d.message.contains("invalid JMESPath")));
    }

    #[test]
    fn bad_threshold_expression() {
        let yaml = r#"
scenarios:
  s: { executor: constant-vus, vus: 1, duration: 1s, flow: [ { request: { url: https://e.com/ } } ] }
thresholds:
  http_req_duration: [ "p95<400" ]
"#;
        let diags = errors(yaml);
        assert!(diags
            .iter()
            .any(|d| d.message.contains("unknown aggregation")));
    }

    #[test]
    fn unknown_threshold_metric_warns_with_suggestion() {
        let yaml = r#"
scenarios:
  s: { executor: constant-vus, vus: 1, duration: 1s, flow: [ { request: { url: https://e.com/ } } ] }
thresholds:
  http_req_durations: [ "p(95)<400" ]
"#;
        let plan = plan_of(yaml);
        let diags = validate(&plan, Some(yaml), &ValidateOptions::default());
        let d = diags
            .iter()
            .find(|d| d.severity == Severity::Warning && d.path.contains("http_req_durations"))
            .expect("warning");
        assert_eq!(
            d.suggestion.as_deref(),
            Some("did you mean `http_req_duration`?")
        );
    }

    #[test]
    fn plugin_family_threshold_metric_does_not_warn() {
        // A declared protocol plugin makes `<name>_reqs`/`_req_duration`/`_docs`
        // valid threshold targets (no "unknown metric" warning).
        let yaml = r#"
plugins: [ { name: mongo } ]
scenarios:
  s: { executor: constant-vus, vus: 1, duration: 1s, flow: [ { request: { url: "mongodb://h/db", protocol: mongo } } ] }
thresholds:
  mongo_req_duration: [ "p(95)<300ms" ]
  mongo_docs: [ "count>0" ]
"#;
        let plan = plan_of(yaml);
        let diags = validate(&plan, Some(yaml), &ValidateOptions::default());
        assert!(
            !diags
                .iter()
                .any(|d| d.path.contains("mongo_req_duration") || d.path.contains("mongo_docs")),
            "plugin-family metrics must not warn: {diags:?}"
        );
    }

    #[test]
    fn missing_data_file_with_check_enabled() {
        let yaml = r#"
data:
  users: { type: csv, path: ./does-not-exist.csv }
scenarios:
  s: { executor: constant-vus, vus: 1, duration: 1s, flow: [ { request: { url: https://e.com/ } } ] }
"#;
        let plan = plan_of(yaml);
        let tmp = tempfile::tempdir().unwrap();
        let opts = ValidateOptions {
            check_files_relative_to: Some(tmp.path().to_path_buf()),
        };
        let diags = validate(&plan, Some(yaml), &opts);
        assert!(diags.iter().any(|d| d.message.contains("file not found")));
    }
}
