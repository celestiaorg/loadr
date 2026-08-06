//! Per-VU state: variables, cookies, data cursors, protocol extensions, and
//! the template expression resolver.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::SeedableRng;

use crate::cookies::CookieJar;
use crate::data::{DataFeeds, NextRowError, RowIdentity};
use crate::metrics::{MetricRegistry, MetricsBus, Tags};

/// Type-keyed storage protocol handlers use for per-VU state
/// (connection pools, gRPC channels, ...).
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl Extensions {
    pub fn get_mut<T: Any + Send>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut())
    }

    pub fn get_or_insert_with<T: Any + Send>(&mut self, init: impl FnOnce() -> T) -> &mut T {
        self.map
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(init()))
            .downcast_mut()
            .expect("extension type invariant")
    }

    pub fn insert<T: Any + Send>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(value));
    }
}

/// Static, run-wide context shared by all VUs.
pub struct RunContext {
    /// Resolved `variables:` (env-interpolated at startup).
    pub variables: serde_json::Map<String, serde_json::Value>,
    /// Resolved secrets (values are redacted from logs/reports).
    pub secrets: HashMap<String, String>,
    /// Captured process environment.
    pub env: HashMap<String, String>,
    pub data: DataFeeds,
    pub registry: Arc<MetricRegistry>,
    /// Directory of the test definition (for `open()` / file bodies).
    pub base_dir: std::path::PathBuf,
    /// Data from JS `setup()`.
    pub setup_data: parking_lot::RwLock<serde_json::Value>,
}

/// Everything one virtual user owns.
pub struct VuContext {
    pub vu_id: u64,
    pub scenario: Arc<str>,
    pub iteration: u64,
    /// Tags applied to every sample this VU emits (scenario + global tags).
    pub base_tags: Arc<Tags>,
    /// Group stack (innermost last); rendered into the `group` tag as `::a::b`.
    pub groups: Vec<String>,
    /// Per-VU variables: extracted values, JS-set values.
    pub vars: serde_json::Map<String, serde_json::Value>,
    pub cookies: CookieJar,
    pub metrics: MetricsBus,
    pub run: Arc<RunContext>,
    pub rng: SmallRng,
    pub extensions: Extensions,
    /// Per-VU data feeder state (cursors + shuffle orders).
    pub data_state: crate::data::VuFeedState,
    /// Rows fetched this iteration (one row per source per iteration).
    pub current_rows: HashMap<String, Arc<crate::data::Row>>,
    /// The request currently being prepared, if any. Set by
    /// [`VuContext::begin_request`], cleared right after `prepare` returns.
    pub current_request: Option<String>,
    /// Whether the most recent request failed (drives `retry` success).
    pub last_request_failed: bool,
}

impl VuContext {
    pub fn new(
        vu_id: u64,
        scenario: Arc<str>,
        base_tags: Arc<Tags>,
        metrics: MetricsBus,
        run: Arc<RunContext>,
        cookies_auto: bool,
    ) -> Self {
        VuContext {
            vu_id,
            scenario,
            iteration: 0,
            base_tags,
            groups: Vec::new(),
            vars: serde_json::Map::new(),
            cookies: CookieJar::new(cookies_auto),
            // Pinning here (rather than at each emit site) covers every
            // sample source — VUs, the JS host, plugin protocols — with a
            // one-line change (see `MetricsBus::for_vu`).
            metrics: metrics.for_vu(vu_id),
            run,
            rng: SmallRng::seed_from_u64(0x10ad ^ vu_id.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            extensions: Extensions::default(),
            data_state: crate::data::VuFeedState::new(),
            current_rows: HashMap::new(),
            current_request: None,
            last_request_failed: false,
        }
    }

    /// Tags for a sample: base tags + group + extras.
    pub fn sample_tags(&self, extras: &[(&str, &str)]) -> Arc<Tags> {
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

    /// Begin a new iteration: bump the counter, clear per-iteration row cache.
    pub fn begin_iteration(&mut self) {
        self.iteration += 1;
        self.current_rows.clear();
        self.current_request = None;
    }

    /// Begin preparing a request: plugin-backed rows are per-request, so
    /// evict them from the iteration cache and remember the request name
    /// for row context. A no-op when the run has no on-demand sources.
    pub fn begin_request(&mut self, name: &str) {
        if !self.run.data.has_on_demand() {
            return;
        }
        self.current_request = Some(name.to_string());
        let data = &self.run.data;
        self.current_rows.retain(|src, _| !data.is_on_demand(src));
    }

    /// The data row for `source` in the current iteration (fetched once),
    /// or the current request if `source` is plugin-backed (fetched once
    /// per request; see [`VuContext::begin_request`]).
    pub fn data_row(&mut self, source: &str) -> Result<Arc<crate::data::Row>, NextRowError> {
        if let Some(row) = self.current_rows.get(source) {
            return Ok(row.clone());
        }
        let id = RowIdentity {
            vu: self.vu_id,
            iteration: self.iteration.saturating_sub(1),
            scenario: &self.scenario,
            request: self.current_request.as_deref(),
        };
        let row = self
            .run
            .data
            .next_row(source, &mut self.data_state, &mut self.rng, &id)?;
        self.current_rows.insert(source.to_string(), row.clone());
        Ok(row)
    }

    /// Resolve a `${...}` template expression. Returns `None` for unknown
    /// references and `Err` for exhausted `stop`-mode data sources.
    ///
    /// `js:` expressions are NOT handled here — the flow runner intercepts them.
    pub fn resolve_expr(&mut self, expr: &str) -> Result<Option<String>, NextRowError> {
        // Namespaced references.
        if let Some(name) = expr.strip_prefix("env.") {
            return Ok(self.run.env.get(name).cloned());
        }
        if let Some(name) = expr.strip_prefix("vars.") {
            return Ok(self.run.variables.get(name).map(json_to_string));
        }
        if let Some(name) = expr.strip_prefix("secrets.") {
            return Ok(self.run.secrets.get(name).cloned());
        }
        if let Some(rest) = expr.strip_prefix("data.") {
            let (source, column) = match rest.split_once('.') {
                Some((s, c)) => (s, c),
                None => return Ok(None),
            };
            let row = self.data_row(source)?;
            return Ok(row.get(column).cloned());
        }
        // Built-ins.
        match expr {
            "vu" => return Ok(Some(self.vu_id.to_string())),
            "iteration" => return Ok(Some(self.iteration.saturating_sub(1).to_string())),
            "scenario" => return Ok(Some(self.scenario.to_string())),
            _ => {}
        }
        // Bare name: per-VU variable (extracted / JS-set), then static variable.
        if let Some(v) = self.vars.get(expr) {
            return Ok(Some(json_to_string(v)));
        }
        if let Some(v) = self.run.variables.get(expr) {
            return Ok(Some(json_to_string(v)));
        }
        Ok(None)
    }
}

/// Stringify a JSON value the way users expect in interpolation.
pub fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    fn run_ctx() -> Arc<RunContext> {
        let mut variables = serde_json::Map::new();
        variables.insert("api_key".to_string(), serde_json::json!("k-123"));
        let mut env = HashMap::new();
        env.insert("HOME_REGION".to_string(), "eu-west-2".to_string());
        let mut secrets = HashMap::new();
        secrets.insert("db".to_string(), "hunter2".to_string());
        let mut sources = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert("user".to_string(), serde_json::json!("u1"));
        sources.insert(
            "users".to_string(),
            loadr_config::DataSource::Inline {
                rows: vec![row],
                mode: loadr_config::DataMode::Shared,
                on_eof: loadr_config::OnEof::Recycle,
                pick: loadr_config::PickStrategy::Sequential,
            },
        );
        let data =
            DataFeeds::load(&sources, std::path::Path::new("."), HashMap::new()).expect("data");
        Arc::new(RunContext {
            variables,
            secrets,
            env,
            data,
            registry: Arc::new(MetricRegistry::with_builtins()),
            base_dir: ".".into(),
            setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
        })
    }

    fn vu() -> VuContext {
        let (bus, _rx) = MetricsBus::new();
        VuContext::new(
            7,
            Arc::from("browse"),
            Arc::new(Tags::new()),
            bus,
            run_ctx(),
            true,
        )
    }

    #[test]
    fn resolves_namespaces() {
        let mut vu = vu();
        vu.begin_iteration();
        assert_eq!(vu.resolve_expr("vars.api_key").unwrap().unwrap(), "k-123");
        assert_eq!(
            vu.resolve_expr("env.HOME_REGION").unwrap().unwrap(),
            "eu-west-2"
        );
        assert_eq!(vu.resolve_expr("secrets.db").unwrap().unwrap(), "hunter2");
        assert_eq!(vu.resolve_expr("data.users.user").unwrap().unwrap(), "u1");
        assert_eq!(vu.resolve_expr("vu").unwrap().unwrap(), "7");
        assert_eq!(vu.resolve_expr("scenario").unwrap().unwrap(), "browse");
        assert_eq!(vu.resolve_expr("iteration").unwrap().unwrap(), "0");
        assert!(vu.resolve_expr("nope").unwrap().is_none());
    }

    #[test]
    fn extracted_vars_take_precedence() {
        let mut vu = vu();
        vu.vars
            .insert("api_key".to_string(), serde_json::json!("extracted"));
        assert_eq!(vu.resolve_expr("api_key").unwrap().unwrap(), "extracted");
        // Namespaced access still reaches the static variable.
        assert_eq!(vu.resolve_expr("vars.api_key").unwrap().unwrap(), "k-123");
    }

    #[test]
    fn data_row_stable_within_iteration() {
        let mut vu = vu();
        vu.begin_iteration();
        let a = vu.resolve_expr("data.users.user").unwrap().unwrap();
        let b = vu.resolve_expr("data.users.user").unwrap().unwrap();
        assert_eq!(a, b, "same iteration sees the same row");
    }

    #[test]
    fn group_tags() {
        let mut vu = vu();
        vu.groups.push("checkout".to_string());
        vu.groups.push("payment".to_string());
        let tags = vu.sample_tags(&[("name", "pay")]);
        assert_eq!(tags.get("group").unwrap(), "::checkout::payment");
        assert_eq!(tags.get("name").unwrap(), "pay");
    }

    #[test]
    fn extensions_typemap() {
        struct PoolState(u32);
        let mut vu = vu();
        vu.extensions.get_or_insert_with(|| PoolState(1)).0 += 1;
        assert_eq!(vu.extensions.get_mut::<PoolState>().unwrap().0, 2);
    }

    /// (vu, iteration, request name) recorded from one `next_row` call.
    type SeenCall = (u64, u64, Option<String>);

    /// Shared observability handle for [`RecordingPlugin`]: records the
    /// `RowIdentity`-derived context of every `next_row` call and returns a
    /// fresh, distinguishable row (an incrementing counter) each time.
    #[derive(Clone)]
    struct PluginHandle {
        counter: Arc<std::sync::atomic::AtomicU64>,
        seen: Arc<std::sync::Mutex<Vec<SeenCall>>>,
    }

    struct RecordingPlugin {
        handle: PluginHandle,
    }

    impl crate::data::DataSourcePlugin for RecordingPlugin {
        fn name(&self) -> &str {
            "signer"
        }

        fn init(
            &mut self,
            _source_configs: &IndexMap<String, serde_json::Value>,
        ) -> Result<(), String> {
            Ok(())
        }

        fn next_row(
            &self,
            ctx: &crate::data::PluginRowCtx<'_>,
        ) -> Result<crate::data::PluginRowResult, String> {
            let n = self
                .handle
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.handle.seen.lock().unwrap().push((
                ctx.vu,
                ctx.iteration,
                ctx.request.map(str::to_string),
            ));
            let mut row = crate::data::Row::new();
            row.insert("n".to_string(), n.to_string());
            Ok(crate::data::PluginRowResult::Row(row))
        }
    }

    /// A run context with both a memory-backed `users` source and a
    /// plugin-backed `signed` source, plus a handle to inspect the plugin's
    /// calls after the fact.
    fn run_ctx_with_plugin() -> (Arc<RunContext>, PluginHandle) {
        let handle = PluginHandle {
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let plugin = RecordingPlugin {
            handle: handle.clone(),
        };
        let mut sources = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert("user".to_string(), serde_json::json!("u1"));
        sources.insert(
            "users".to_string(),
            loadr_config::DataSource::Inline {
                rows: vec![row],
                mode: loadr_config::DataMode::Shared,
                on_eof: loadr_config::OnEof::Recycle,
                pick: loadr_config::PickStrategy::Sequential,
            },
        );
        sources.insert(
            "signed".to_string(),
            loadr_config::DataSource::Plugin {
                source: "signer".to_string(),
                config: serde_json::Value::Null,
            },
        );
        let mut plugins: HashMap<String, Box<dyn crate::data::DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let data = DataFeeds::load(&sources, std::path::Path::new("."), plugins).expect("data");
        (
            Arc::new(RunContext {
                variables: serde_json::Map::new(),
                secrets: HashMap::new(),
                env: HashMap::new(),
                data,
                registry: Arc::new(MetricRegistry::with_builtins()),
                base_dir: ".".into(),
                setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
            }),
            handle,
        )
    }

    fn vu_with(run: Arc<RunContext>) -> VuContext {
        let (bus, _rx) = MetricsBus::new();
        VuContext::new(
            7,
            Arc::from("browse"),
            Arc::new(Tags::new()),
            bus,
            run,
            true,
        )
    }

    #[test]
    fn plugin_row_stable_within_a_request() {
        let (run, _handle) = run_ctx_with_plugin();
        let mut vu = vu_with(run);
        vu.begin_iteration();
        vu.begin_request("submit");
        let a = vu.resolve_expr("data.signed.n").unwrap().unwrap();
        let b = vu.resolve_expr("data.signed.n").unwrap().unwrap();
        assert_eq!(a, b, "same request sees the same plugin row");
    }

    #[test]
    fn plugin_row_is_fresh_after_begin_request() {
        let (run, _handle) = run_ctx_with_plugin();
        let mut vu = vu_with(run);
        vu.begin_iteration();
        vu.begin_request("submit 1");
        let a = vu.resolve_expr("data.signed.n").unwrap().unwrap();
        vu.begin_request("submit 2");
        let b = vu.resolve_expr("data.signed.n").unwrap().unwrap();
        assert_ne!(a, b, "a new request pulls a fresh plugin row");
    }

    #[test]
    fn memory_feeds_unaffected_by_request_eviction() {
        let (run, _handle) = run_ctx_with_plugin();
        let mut vu = vu_with(run);
        vu.begin_iteration();
        let a = vu.resolve_expr("data.users.user").unwrap().unwrap();
        vu.begin_request("submit 1");
        let b = vu.resolve_expr("data.users.user").unwrap().unwrap();
        vu.begin_request("submit 2");
        let c = vu.resolve_expr("data.users.user").unwrap().unwrap();
        assert_eq!(a, b);
        assert_eq!(b, c, "memory-backed rows are not evicted by begin_request");
    }

    #[test]
    fn plugin_ctx_carries_request_name_and_zero_based_iteration() {
        let (run, handle) = run_ctx_with_plugin();
        let mut vu = vu_with(run);
        vu.begin_iteration();
        vu.begin_request("submit tx");
        vu.resolve_expr("data.signed.n").unwrap();
        let seen = handle.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], (7, 0, Some("submit tx".to_string())));
    }

    #[test]
    fn plugin_row_fetched_outside_request_has_no_request_name() {
        let (run, handle) = run_ctx_with_plugin();
        let mut vu = vu_with(run);
        vu.begin_iteration();
        vu.resolve_expr("data.signed.n").unwrap();
        let seen = handle.seen.lock().unwrap();
        assert_eq!(seen[0].2, None);
    }
}
