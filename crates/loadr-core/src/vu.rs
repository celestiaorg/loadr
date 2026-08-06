//! Per-VU state: variables, cookies, data cursors, protocol extensions, and
//! the template expression resolver.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::SeedableRng;

use crate::cookies::CookieJar;
use crate::data::{DataFeeds, NextRowError};
use crate::metrics::{CachedTags, MetricRegistry, MetricsBus, Tags};

struct TagCacheEntry {
    groups: Vec<String>,
    extras: Vec<(String, String)>,
    tags: CachedTags,
}

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
    base_cached_tags: CachedTags,
    tag_cache: Vec<TagCacheEntry>,
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
        let base_cached_tags = CachedTags::new(base_tags.clone());
        VuContext {
            vu_id,
            scenario,
            iteration: 0,
            base_tags,
            base_cached_tags,
            tag_cache: Vec::new(),
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

    pub(crate) fn cached_sample_tags(&mut self, extras: &[(&str, &str)]) -> CachedTags {
        if extras.is_empty() && self.groups.is_empty() {
            return self.base_cached_tags.clone();
        }
        if let Some(entry) = self.tag_cache.iter().find(|entry| {
            entry.groups == self.groups
                && entry.extras.len() == extras.len()
                && entry.extras.iter().zip(extras).all(
                    |((cached_key, cached_value), (key, value))| {
                        cached_key == key && cached_value == value
                    },
                )
        }) {
            return entry.tags.clone();
        }

        let tags = CachedTags::new(self.sample_tags(extras));
        if self.tag_cache.len() >= 64 {
            self.tag_cache.clear();
        }
        self.tag_cache.push(TagCacheEntry {
            groups: self.groups.clone(),
            extras: extras
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            tags: tags.clone(),
        });
        tags
    }

    /// Begin a new iteration: bump the counter, clear per-iteration row cache.
    pub fn begin_iteration(&mut self) {
        self.iteration += 1;
        self.current_rows.clear();
    }

    /// The data row for `source` in the current iteration (fetched once).
    pub fn data_row(&mut self, source: &str) -> Result<Arc<crate::data::Row>, NextRowError> {
        if let Some(row) = self.current_rows.get(source) {
            return Ok(row.clone());
        }
        let row = self
            .run
            .data
            .next_row(source, &mut self.data_state, &mut self.rng)?;
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
        let data = DataFeeds::load(&sources, std::path::Path::new(".")).expect("data");
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
    fn cached_tags_reuse_arc_and_distinguish_context() {
        let mut vu = vu();
        let first = vu.cached_sample_tags(&[("status", "0")]);
        let again = vu.cached_sample_tags(&[("status", "0")]);
        assert!(Arc::ptr_eq(&first.tags, &again.tags));
        assert_eq!(first.hash, again.hash);

        let failed = vu.cached_sample_tags(&[("status", "13")]);
        assert!(!Arc::ptr_eq(&first.tags, &failed.tags));

        vu.groups.push("checkout".to_string());
        let grouped = vu.cached_sample_tags(&[("status", "0")]);
        assert_eq!(grouped.tags.get("group").unwrap(), "::checkout");
        vu.groups.pop();
        let ungrouped = vu.cached_sample_tags(&[("status", "0")]);
        assert!(Arc::ptr_eq(&first.tags, &ungrouped.tags));
    }

    #[test]
    fn extensions_typemap() {
        struct PoolState(u32);
        let mut vu = vu();
        vu.extensions.get_or_insert_with(|| PoolState(1)).0 += 1;
        assert_eq!(vu.extensions.get_mut::<PoolState>().unwrap().0, 2);
    }
}
