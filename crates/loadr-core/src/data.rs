//! Data parameterization: CSV, inline, and plugin-backed on-demand data
//! sources with shared or per-VU cursors and recycle/stop-at-EOF semantics.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use indexmap::IndexMap;
use loadr_config::{DataMode, DataSource, OnEof, PickStrategy};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::RngExt;

use crate::error::EngineError;

/// One data row: column name → string value.
pub type Row = IndexMap<String, String>;

/// Identity of the caller pulling a row (used only by plugin-backed feeds).
#[derive(Clone, Copy)]
pub struct RowIdentity<'a> {
    pub vu: u64,
    /// 0-based, matches `${iteration}`.
    pub iteration: u64,
    pub scenario: &'a str,
    pub request: Option<&'a str>,
}

/// Full context for one on-demand row generation (hot path).
pub struct PluginRowCtx<'a> {
    pub source: &'a str,
    pub vu: u64,
    pub iteration: u64,
    /// Monotonic per-VU, per-source counter.
    pub seq: u64,
    pub scenario: &'a str,
    pub request: Option<&'a str>,
    /// Core-supplied wall clock.
    pub ts_ms: u64,
}

/// Outcome of one on-demand row generation.
pub enum PluginRowResult {
    Row(Row),
    Exhausted,
}

/// Core-facing `data_source` plugin capability. `next_row` runs on the
/// request hot path and is called concurrently across VU worker threads.
pub trait DataSourcePlugin: Send + Sync {
    fn name(&self) -> &str;

    /// One-time setup before VUs start. `source_configs` maps each
    /// `data.<name>` backed by this plugin to its `config:` value.
    fn init(&mut self, source_configs: &IndexMap<String, serde_json::Value>) -> Result<(), String>;

    fn next_row(&self, ctx: &PluginRowCtx<'_>) -> Result<PluginRowResult, String>;
}

#[derive(Debug)]
struct MemoryFeed {
    rows: Vec<Arc<Row>>,
    mode: DataMode,
    on_eof: OnEof,
    pick: PickStrategy,
    shared_cursor: AtomicUsize,
}

enum Feed {
    Memory(MemoryFeed),
    Plugin {
        plugin: Arc<dyn DataSourcePlugin>,
        /// Fetch rows under `block_in_place` so a slow feeder cannot stall
        /// the runtime (opt-in per source via `blocking: true`).
        blocking: bool,
    },
}

impl std::fmt::Debug for Feed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Feed::Memory(feed) => feed.fmt(f),
            Feed::Plugin { plugin, blocking } => {
                write!(f, "Feed::Plugin({}, blocking: {blocking})", plugin.name())
            }
        }
    }
}

/// Whether a fetch should run under [`tokio::task::block_in_place`]: only
/// when the source opted in, and only where the bracket is legal — inside a
/// multi-thread Tokio runtime. `block_in_place` panics on a current-thread
/// runtime and outside any runtime; both fall back to the plain inline call.
fn bracket_plugin_fetch(blocking: bool) -> bool {
    blocking
        && tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
}

/// Per-VU feeder state: sequential cursors, per-VU shuffle orders, and
/// plugin row sequence counters.
#[derive(Debug, Default)]
pub struct VuFeedState {
    cursors: HashMap<String, usize>,
    shuffles: HashMap<String, Vec<usize>>,
    /// Source → shared counter slot, shared across parallel branches of one
    /// VU (`fork_for_parallel`) so branches never observe the same
    /// `(vu, source, seq)`.
    plugin_sequences: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    /// Branch-local cache of slots from `plugin_sequences`; steady-state
    /// fetches skip the lock and key allocation.
    local_sequences: HashMap<String, Arc<AtomicU64>>,
}

impl VuFeedState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn fork_for_parallel(&self) -> Self {
        Self {
            cursors: HashMap::new(),
            shuffles: HashMap::new(),
            plugin_sequences: Arc::clone(&self.plugin_sequences),
            local_sequences: HashMap::new(),
        }
    }

    fn next_plugin_seq(&mut self, source: &str) -> u64 {
        // Relaxed: uniqueness needs only `fetch_add`'s atomicity; no other
        // memory is published through the slot.
        if let Some(slot) = self.local_sequences.get(source) {
            return slot.fetch_add(1, Ordering::Relaxed);
        }
        let slot = {
            let mut shared = self.plugin_sequences.lock();
            match shared.get(source) {
                Some(slot) => Arc::clone(slot),
                None => {
                    let slot = Arc::new(AtomicU64::new(0));
                    shared.insert(source.to_string(), Arc::clone(&slot));
                    slot
                }
            }
        };
        let seq = slot.fetch_add(1, Ordering::Relaxed);
        self.local_sequences.insert(source.to_string(), slot);
        seq
    }
}

/// All loaded data sources for a test.
#[derive(Debug, Default)]
pub struct DataFeeds {
    feeds: HashMap<String, Feed>,
    has_on_demand: bool,
}

/// Signalled when a `stop`-mode source is exhausted: the VU should retire.
#[derive(Debug, thiserror::Error)]
#[error("data source `{0}` is exhausted")]
pub struct EndOfData(pub String);

impl DataFeeds {
    /// Load every source declared in the plan. CSV paths resolve against
    /// `base_dir`. `plugins` provides one loaded `data_source`-capable
    /// plugin per name that a `type: plugin` source may reference.
    pub fn load(
        sources: &IndexMap<String, DataSource>,
        base_dir: &Path,
        mut plugins: HashMap<String, Box<dyn DataSourcePlugin>>,
    ) -> Result<DataFeeds, EngineError> {
        // Group plugin-backed sources by plugin name so each plugin's
        // `init` sees all `data.*` entries it backs in one call.
        let mut plugin_groups: IndexMap<String, IndexMap<String, serde_json::Value>> =
            IndexMap::new();
        for (name, source) in sources {
            if let DataSource::Plugin {
                source: plugin_name,
                config,
                ..
            } = source
            {
                plugin_groups
                    .entry(plugin_name.clone())
                    .or_default()
                    .insert(name.clone(), config.clone());
            }
        }

        let mut initialized: HashMap<String, Arc<dyn DataSourcePlugin>> = HashMap::new();
        for (plugin_name, group_configs) in &plugin_groups {
            let mut plugin = plugins.remove(plugin_name).ok_or_else(|| EngineError::Data {
                source_name: plugin_name.clone(),
                message: format!(
                    "plugin `{plugin_name}` is not loaded or does not provide the data_source capability"
                ),
            })?;
            plugin
                .init(group_configs)
                .map_err(|message| EngineError::Data {
                    source_name: plugin_name.clone(),
                    message,
                })?;
            initialized.insert(plugin_name.clone(), Arc::from(plugin));
        }

        let mut has_on_demand = false;
        let mut feeds = HashMap::new();
        for (name, source) in sources {
            let feed = match source {
                DataSource::Plugin {
                    source: plugin_name,
                    blocking,
                    ..
                } => {
                    has_on_demand = true;
                    let plugin = initialized
                        .get(plugin_name)
                        .expect("initialized above for every referenced plugin")
                        .clone();
                    Feed::Plugin {
                        plugin,
                        blocking: *blocking,
                    }
                }
                DataSource::Csv {
                    path,
                    mode,
                    on_eof,
                    pick,
                    delimiter,
                    has_header,
                } => {
                    let resolved = if path.is_absolute() {
                        path.clone()
                    } else {
                        base_dir.join(path)
                    };
                    let raw = std::fs::read(&resolved).map_err(|e| EngineError::Data {
                        source_name: name.clone(),
                        message: format!("cannot read {}: {e}", resolved.display()),
                    })?;
                    let mut builder = csv::ReaderBuilder::new();
                    builder.has_headers(*has_header);
                    if let Some(d) = delimiter {
                        builder.delimiter(*d as u8);
                    }
                    let mut reader = builder.from_reader(raw.as_slice());
                    let headers: Vec<String> = if *has_header {
                        reader
                            .headers()
                            .map_err(|e| EngineError::Data {
                                source_name: name.clone(),
                                message: e.to_string(),
                            })?
                            .iter()
                            .map(str::to_string)
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let mut rows = Vec::new();
                    for record in reader.records() {
                        let record = record.map_err(|e| EngineError::Data {
                            source_name: name.clone(),
                            message: e.to_string(),
                        })?;
                        let mut row = Row::new();
                        for (i, field) in record.iter().enumerate() {
                            let key = headers.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                            row.insert(key, field.to_string());
                        }
                        rows.push(Arc::new(row));
                    }
                    if rows.is_empty() {
                        return Err(EngineError::Data {
                            source_name: name.clone(),
                            message: "CSV has no data rows".into(),
                        });
                    }
                    make_feed(rows, *mode, *on_eof, *pick)
                }
                DataSource::Json {
                    path,
                    mode,
                    on_eof,
                    pick,
                } => {
                    let resolved = if path.is_absolute() {
                        path.clone()
                    } else {
                        base_dir.join(path)
                    };
                    let raw = std::fs::read(&resolved).map_err(|e| EngineError::Data {
                        source_name: name.clone(),
                        message: format!("cannot read {}: {e}", resolved.display()),
                    })?;
                    let parsed: serde_json::Value =
                        serde_json::from_slice(&raw).map_err(|e| EngineError::Data {
                            source_name: name.clone(),
                            message: format!("invalid JSON: {e}"),
                        })?;
                    let array = parsed.as_array().ok_or_else(|| EngineError::Data {
                        source_name: name.clone(),
                        message: "JSON data source must be an array of objects".into(),
                    })?;
                    let mut rows = Vec::with_capacity(array.len());
                    for item in array {
                        let obj = item.as_object().ok_or_else(|| EngineError::Data {
                            source_name: name.clone(),
                            message: "each JSON data row must be an object".into(),
                        })?;
                        let row: Row = obj
                            .iter()
                            .map(|(k, v)| (k.clone(), json_to_string(v)))
                            .collect();
                        rows.push(Arc::new(row));
                    }
                    if rows.is_empty() {
                        return Err(EngineError::Data {
                            source_name: name.clone(),
                            message: "JSON data source is empty".into(),
                        });
                    }
                    make_feed(rows, *mode, *on_eof, *pick)
                }
                DataSource::Inline {
                    rows,
                    mode,
                    on_eof,
                    pick,
                } => {
                    let converted: Vec<Arc<Row>> = rows
                        .iter()
                        .map(|r| {
                            Arc::new(
                                r.iter()
                                    .map(|(k, v)| (k.clone(), json_to_string(v)))
                                    .collect::<Row>(),
                            )
                        })
                        .collect();
                    if converted.is_empty() {
                        return Err(EngineError::Data {
                            source_name: name.clone(),
                            message: "inline data has no rows".into(),
                        });
                    }
                    make_feed(converted, *mode, *on_eof, *pick)
                }
            };
            feeds.insert(name.clone(), feed);
        }
        Ok(DataFeeds {
            feeds,
            has_on_demand,
        })
    }

    pub fn has_source(&self, name: &str) -> bool {
        self.feeds.contains_key(name)
    }

    pub fn source_names(&self) -> Vec<&str> {
        self.feeds.keys().map(String::as_str).collect()
    }

    /// Whether any loaded source is plugin-backed (on-demand rows).
    pub fn has_on_demand(&self) -> bool {
        self.has_on_demand
    }

    /// Whether `name` is a plugin-backed (on-demand) source.
    pub fn is_on_demand(&self, name: &str) -> bool {
        matches!(self.feeds.get(name), Some(Feed::Plugin { .. }))
    }

    /// Fetch the next row for `source`, honoring its mode, pick strategy and
    /// EOF behaviour (memory feeds), or invoking the backing plugin
    /// (plugin-backed feeds). `state` holds per-VU cursors/shuffles/seq
    /// counters; `rng` drives random and shuffle selection; `id` identifies
    /// the calling VU/request for plugin-backed feeds.
    pub fn next_row(
        &self,
        source: &str,
        state: &mut VuFeedState,
        rng: &mut impl RngExt,
        id: &RowIdentity<'_>,
    ) -> Result<Arc<Row>, NextRowError> {
        let feed = self
            .feeds
            .get(source)
            .ok_or_else(|| NextRowError::UnknownSource(source.to_string()))?;

        let feed = match feed {
            Feed::Plugin { plugin, blocking } => {
                let seq = state.next_plugin_seq(source);
                let ctx = PluginRowCtx {
                    source,
                    vu: id.vu,
                    iteration: id.iteration,
                    seq,
                    scenario: id.scenario,
                    request: id.request,
                    ts_ms: crate::metrics::now_millis(),
                };
                // The plugin call is synchronous FFI on a runtime worker; a
                // feeder that signs, hashes or does I/O would otherwise stall
                // timers and unrelated VUs for its whole call.
                let fetched = if bracket_plugin_fetch(*blocking) {
                    tokio::task::block_in_place(|| plugin.next_row(&ctx))
                } else {
                    plugin.next_row(&ctx)
                };
                return match fetched {
                    Ok(PluginRowResult::Row(row)) => Ok(Arc::new(row)),
                    Ok(PluginRowResult::Exhausted) => {
                        Err(NextRowError::Exhausted(EndOfData(source.to_string())))
                    }
                    Err(message) => Err(NextRowError::Plugin {
                        source_name: source.to_string(),
                        message,
                    }),
                };
            }
            Feed::Memory(feed) => feed,
        };
        let len = feed.rows.len();

        // Random: independent uniform pick every call; never exhausts.
        if feed.pick == PickStrategy::Random {
            let i = rng.random_range(0..len);
            return Ok(feed.rows[i].clone());
        }

        // Sequential / shuffle both walk an order with a cursor.
        let cursor = match feed.mode {
            DataMode::Shared => feed.shared_cursor.fetch_add(1, Ordering::Relaxed),
            DataMode::PerVu => {
                let c = state.cursors.entry(source.to_string()).or_insert(0);
                let idx = *c;
                *c += 1;
                idx
            }
        };

        // EOF policy applies to the cursor before resolving through the order.
        if cursor >= len && feed.on_eof == OnEof::Stop {
            return Err(NextRowError::Exhausted(EndOfData(source.to_string())));
        }

        let row_index = match feed.pick {
            PickStrategy::Shuffle => {
                // Shuffle = a per-VU permutation of row indices, walked in order.
                let order = state.shuffles.entry(source.to_string()).or_insert_with(|| {
                    let mut order: Vec<usize> = (0..len).collect();
                    order.shuffle(rng);
                    order
                });
                order[cursor % len]
            }
            _ => cursor % len,
        };
        Ok(feed.rows[row_index].clone())
    }
}

fn make_feed(rows: Vec<Arc<Row>>, mode: DataMode, on_eof: OnEof, pick: PickStrategy) -> Feed {
    Feed::Memory(MemoryFeed {
        rows,
        mode,
        on_eof,
        pick,
        shared_cursor: AtomicUsize::new(0),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum NextRowError {
    #[error("unknown data source `{0}`")]
    UnknownSource(String),
    #[error(transparent)]
    Exhausted(#[from] EndOfData),
    #[error("data source `{source_name}`: plugin error: {message}")]
    Plugin {
        source_name: String,
        message: String,
    },
}

fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_config::PickStrategy;
    use rand::SeedableRng;
    use std::io::Write;

    fn rng() -> rand::rngs::SmallRng {
        rand::rngs::SmallRng::seed_from_u64(42)
    }

    fn id() -> RowIdentity<'static> {
        RowIdentity {
            vu: 1,
            iteration: 0,
            scenario: "s",
            request: None,
        }
    }

    /// Shared observability handle for [`FakePlugin`], cloned before the
    /// plugin is boxed so tests can inspect state after `DataFeeds::load`
    /// takes ownership.
    #[derive(Clone, Default)]
    struct FakeHandle {
        calls: Arc<std::sync::atomic::AtomicU64>,
        seen_configs: Arc<std::sync::Mutex<Option<IndexMap<String, serde_json::Value>>>>,
    }

    enum FakeMode {
        EchoSeq,
        AlwaysErr(String),
        AlwaysExhausted,
        /// Signal entry, then sleep before answering — a slow FFI feeder.
        SleepyEcho {
            entered: std::sync::mpsc::SyncSender<()>,
            delay: std::time::Duration,
        },
    }

    struct FakePlugin {
        name: String,
        handle: FakeHandle,
        mode: FakeMode,
    }

    fn fake_plugin(name: &str, mode: FakeMode) -> (FakePlugin, FakeHandle) {
        let handle = FakeHandle::default();
        (
            FakePlugin {
                name: name.to_string(),
                handle: handle.clone(),
                mode,
            },
            handle,
        )
    }

    impl DataSourcePlugin for FakePlugin {
        fn name(&self) -> &str {
            &self.name
        }

        fn init(
            &mut self,
            source_configs: &IndexMap<String, serde_json::Value>,
        ) -> Result<(), String> {
            *self.handle.seen_configs.lock().unwrap() = Some(source_configs.clone());
            Ok(())
        }

        fn next_row(&self, ctx: &PluginRowCtx<'_>) -> Result<PluginRowResult, String> {
            self.handle
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match &self.mode {
                FakeMode::EchoSeq => {
                    let mut row = Row::new();
                    row.insert("seq".to_string(), ctx.seq.to_string());
                    row.insert("vu".to_string(), ctx.vu.to_string());
                    Ok(PluginRowResult::Row(row))
                }
                FakeMode::AlwaysErr(msg) => Err(msg.clone()),
                FakeMode::AlwaysExhausted => Ok(PluginRowResult::Exhausted),
                FakeMode::SleepyEcho { entered, delay } => {
                    let _ = entered.try_send(());
                    std::thread::sleep(*delay);
                    let mut row = Row::new();
                    row.insert("seq".to_string(), ctx.seq.to_string());
                    Ok(PluginRowResult::Row(row))
                }
            }
        }
    }

    /// One plugin-backed source named `rows`, wired to the given fake.
    fn plugin_feeds(plugin: FakePlugin, blocking: bool) -> DataFeeds {
        let name = plugin.name.clone();
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert(name.clone(), Box::new(plugin));
        let mut sources = IndexMap::new();
        sources.insert(
            "rows".to_string(),
            DataSource::Plugin {
                source: name,
                config: serde_json::Value::Null,
                blocking,
            },
        );
        DataFeeds::load(&sources, Path::new("."), plugins).expect("load")
    }

    fn csv_feeds(
        dir: &Path,
        content: &str,
        mode: DataMode,
        on_eof: OnEof,
        pick: PickStrategy,
    ) -> DataFeeds {
        let path = dir.join("users.csv");
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(content.as_bytes()).expect("write");
        let mut sources = IndexMap::new();
        sources.insert(
            "users".to_string(),
            DataSource::Csv {
                path: "users.csv".into(),
                mode,
                on_eof,
                pick,
                delimiter: None,
                has_header: true,
            },
        );
        DataFeeds::load(&sources, dir, HashMap::new()).expect("load")
    }

    #[test]
    fn shared_cursor_distributes_rows() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user,pass\nu1,p1\nu2,p2\nu3,p3\n",
            DataMode::Shared,
            OnEof::Recycle,
            PickStrategy::Sequential,
        );
        let mut st = VuFeedState::new();
        let mut r = rng();
        let r1 = feeds
            .next_row("users", &mut st, &mut r, &id())
            .expect("row");
        let r2 = feeds
            .next_row("users", &mut st, &mut r, &id())
            .expect("row");
        assert_eq!(r1["user"], "u1");
        assert_eq!(r2["user"], "u2");
    }

    #[test]
    fn recycle_wraps() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user\nu1\nu2\n",
            DataMode::Shared,
            OnEof::Recycle,
            PickStrategy::Sequential,
        );
        let mut st = VuFeedState::new();
        let mut r = rng();
        for _ in 0..2 {
            feeds
                .next_row("users", &mut st, &mut r, &id())
                .expect("row");
        }
        let wrapped = feeds
            .next_row("users", &mut st, &mut r, &id())
            .expect("row");
        assert_eq!(wrapped["user"], "u1");
    }

    #[test]
    fn stop_at_eof() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user\nu1\n",
            DataMode::Shared,
            OnEof::Stop,
            PickStrategy::Sequential,
        );
        let mut st = VuFeedState::new();
        let mut r = rng();
        feeds
            .next_row("users", &mut st, &mut r, &id())
            .expect("row");
        assert!(matches!(
            feeds.next_row("users", &mut st, &mut r, &id()),
            Err(NextRowError::Exhausted(_))
        ));
    }

    #[test]
    fn per_vu_cursors_are_independent() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user\nu1\nu2\n",
            DataMode::PerVu,
            OnEof::Recycle,
            PickStrategy::Sequential,
        );
        let (mut vu1, mut vu2) = (VuFeedState::new(), VuFeedState::new());
        let mut r = rng();
        assert_eq!(
            feeds.next_row("users", &mut vu1, &mut r, &id()).unwrap()["user"],
            "u1"
        );
        assert_eq!(
            feeds.next_row("users", &mut vu2, &mut r, &id()).unwrap()["user"],
            "u1"
        );
        assert_eq!(
            feeds.next_row("users", &mut vu1, &mut r, &id()).unwrap()["user"],
            "u2"
        );
    }

    #[test]
    fn random_pick_stays_in_range_and_never_exhausts() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user\nu1\nu2\nu3\n",
            DataMode::Shared,
            OnEof::Stop,
            PickStrategy::Random,
        );
        let mut st = VuFeedState::new();
        let mut r = rng();
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..200 {
            let row = feeds
                .next_row("users", &mut st, &mut r, &id())
                .expect("random never stops");
            seen.insert(row["user"].clone());
        }
        assert!(seen.len() > 1, "random should visit multiple rows");
        assert!(seen
            .iter()
            .all(|u| ["u1", "u2", "u3"].contains(&u.as_str())));
    }

    #[test]
    fn shuffle_covers_all_rows_once_per_cycle() {
        let dir = tempfile::tempdir().expect("tmp");
        let feeds = csv_feeds(
            dir.path(),
            "user\nu1\nu2\nu3\nu4\n",
            DataMode::PerVu,
            OnEof::Recycle,
            PickStrategy::Shuffle,
        );
        let mut st = VuFeedState::new();
        let mut r = rng();
        let mut first_cycle = std::collections::BTreeSet::new();
        for _ in 0..4 {
            let row = feeds
                .next_row("users", &mut st, &mut r, &id())
                .expect("row");
            first_cycle.insert(row["user"].clone());
        }
        // A full cycle visits every row exactly once (just shuffled order).
        assert_eq!(first_cycle.len(), 4);
    }

    #[test]
    fn json_source_loads_objects() {
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::write(
            dir.path().join("d.json"),
            r#"[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]"#,
        )
        .expect("write");
        let mut sources = IndexMap::new();
        sources.insert(
            "items".to_string(),
            DataSource::Json {
                path: "d.json".into(),
                mode: DataMode::Shared,
                on_eof: OnEof::Recycle,
                pick: PickStrategy::Sequential,
            },
        );
        let feeds = DataFeeds::load(&sources, dir.path(), HashMap::new()).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        let row = feeds
            .next_row("items", &mut st, &mut r, &id())
            .expect("row");
        assert_eq!(row["id"], "1");
        assert_eq!(row["name"], "alpha");
    }

    #[test]
    fn inline_rows() {
        let mut sources = IndexMap::new();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(7));
        row.insert("name".to_string(), serde_json::json!("alpha"));
        sources.insert(
            "items".to_string(),
            DataSource::Inline {
                rows: vec![row],
                mode: DataMode::Shared,
                on_eof: OnEof::Recycle,
                pick: PickStrategy::Sequential,
            },
        );
        let feeds = DataFeeds::load(&sources, Path::new("."), HashMap::new()).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        let row = feeds
            .next_row("items", &mut st, &mut r, &id())
            .expect("row");
        assert_eq!(row["id"], "7");
        assert_eq!(row["name"], "alpha");
    }

    #[test]
    fn headerless_csv_gets_positional_columns() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "a,b\nc,d\n").expect("write");
        let mut sources = IndexMap::new();
        sources.insert(
            "d".to_string(),
            DataSource::Csv {
                path: "data.csv".into(),
                mode: DataMode::Shared,
                on_eof: OnEof::Recycle,
                pick: PickStrategy::Sequential,
                delimiter: None,
                has_header: false,
            },
        );
        let feeds = DataFeeds::load(&sources, dir.path(), HashMap::new()).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        let row = feeds.next_row("d", &mut st, &mut r, &id()).expect("row");
        assert_eq!(row["col0"], "a");
        assert_eq!(row["col1"], "b");
    }

    fn plugin_source(plugin_name: &str) -> IndexMap<String, DataSource> {
        let mut sources = IndexMap::new();
        sources.insert(
            "signed".to_string(),
            DataSource::Plugin {
                source: plugin_name.to_string(),
                config: serde_json::Value::Null,
                blocking: false,
            },
        );
        sources
    }

    #[test]
    fn plugin_source_seq_is_monotonic_per_vu_source() {
        let (plugin, _handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let feeds =
            DataFeeds::load(&plugin_source("signer"), Path::new("."), plugins).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        let r1 = feeds
            .next_row("signed", &mut st, &mut r, &id())
            .expect("row");
        let r2 = feeds
            .next_row("signed", &mut st, &mut r, &id())
            .expect("row");
        assert_eq!(r1["seq"], "0");
        assert_eq!(r2["seq"], "1");
    }

    #[test]
    fn plugin_source_seq_is_shared_across_parallel_forks() {
        let (plugin, _handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let feeds =
            DataFeeds::load(&plugin_source("signer"), Path::new("."), plugins).expect("load");
        let parent = VuFeedState::new();
        let mut branch_a = parent.fork_for_parallel();
        let mut branch_b = parent.fork_for_parallel();
        let mut r = rng();

        let r1 = feeds
            .next_row("signed", &mut branch_a, &mut r, &id())
            .expect("row");
        let r2 = feeds
            .next_row("signed", &mut branch_b, &mut r, &id())
            .expect("row");

        assert_eq!(r1["seq"], "0");
        assert_eq!(r2["seq"], "1");
    }

    #[test]
    fn next_plugin_seq_counts_up_in_order() {
        let mut st = VuFeedState::new();
        for expected in 0..10u64 {
            assert_eq!(st.next_plugin_seq("src"), expected);
        }
    }

    #[test]
    fn next_plugin_seq_sources_advance_independently() {
        let mut st = VuFeedState::new();
        assert_eq!(st.next_plugin_seq("a"), 0);
        assert_eq!(st.next_plugin_seq("a"), 1);
        assert_eq!(st.next_plugin_seq("b"), 0);
        assert_eq!(st.next_plugin_seq("a"), 2);
        assert_eq!(st.next_plugin_seq("b"), 1);
    }

    #[test]
    fn next_plugin_seq_late_fork_continues_from_shared_value() {
        let mut parent = VuFeedState::new();
        for _ in 0..5 {
            parent.next_plugin_seq("src");
        }
        let mut branch = parent.fork_for_parallel();
        assert_eq!(branch.next_plugin_seq("src"), 5);
        assert_eq!(parent.next_plugin_seq("src"), 6);
    }

    #[test]
    fn next_plugin_seq_unique_across_threaded_forks() {
        const ROUNDS: usize = 100;
        const PER_BRANCH: u64 = 20;
        let parent = VuFeedState::new();
        let mut seen: Vec<u64> = Vec::new();
        for _ in 0..ROUNDS {
            // Fresh forks each round: both branches start with a cold
            // local cache, so their first fetches race the shared-map
            // slot resolution from both sides.
            let mut a = parent.fork_for_parallel();
            let mut b = parent.fork_for_parallel();
            let (va, vb) = std::thread::scope(|s| {
                let ta = s.spawn(move || {
                    (0..PER_BRANCH)
                        .map(|_| a.next_plugin_seq("src"))
                        .collect::<Vec<_>>()
                });
                let tb = s.spawn(move || {
                    (0..PER_BRANCH)
                        .map(|_| b.next_plugin_seq("src"))
                        .collect::<Vec<_>>()
                });
                (ta.join().expect("branch a"), tb.join().expect("branch b"))
            });
            seen.extend(va);
            seen.extend(vb);
        }
        seen.sort_unstable();
        let expected: Vec<u64> = (0..ROUNDS as u64 * 2 * PER_BRANCH).collect();
        assert_eq!(seen, expected, "sequence values must be exactly 0..total");
    }

    #[test]
    fn plugin_error_maps_to_next_row_error_plugin() {
        let (plugin, _handle) = fake_plugin("signer", FakeMode::AlwaysErr("boom".to_string()));
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let feeds =
            DataFeeds::load(&plugin_source("signer"), Path::new("."), plugins).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        match feeds.next_row("signed", &mut st, &mut r, &id()) {
            Err(NextRowError::Plugin {
                source_name,
                message,
            }) => {
                assert_eq!(source_name, "signed");
                assert_eq!(message, "boom");
            }
            other => panic!("expected NextRowError::Plugin, got {other:?}"),
        }
    }

    #[test]
    fn plugin_exhausted_maps_to_next_row_error_exhausted() {
        let (plugin, _handle) = fake_plugin("signer", FakeMode::AlwaysExhausted);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let feeds =
            DataFeeds::load(&plugin_source("signer"), Path::new("."), plugins).expect("load");
        let mut st = VuFeedState::new();
        let mut r = rng();
        assert!(matches!(
            feeds.next_row("signed", &mut st, &mut r, &id()),
            Err(NextRowError::Exhausted(_))
        ));
    }

    #[test]
    fn missing_plugin_fails_at_load_naming_source() {
        let err = DataFeeds::load(&plugin_source("signer"), Path::new("."), HashMap::new())
            .expect_err("missing plugin should fail load");
        match err {
            EngineError::Data {
                source_name,
                message,
            } => {
                assert_eq!(source_name, "signer");
                assert!(
                    message.contains("not loaded or does not provide the data_source capability")
                );
            }
            other => panic!("expected EngineError::Data, got {other:?}"),
        }
    }

    #[test]
    fn plugin_init_receives_grouped_source_configs() {
        let mut sources = IndexMap::new();
        sources.insert(
            "a".to_string(),
            DataSource::Plugin {
                source: "signer".to_string(),
                config: serde_json::json!({"k": "a"}),
                blocking: false,
            },
        );
        sources.insert(
            "b".to_string(),
            DataSource::Plugin {
                source: "signer".to_string(),
                config: serde_json::json!({"k": "b"}),
                blocking: false,
            },
        );
        let (plugin, handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        DataFeeds::load(&sources, Path::new("."), plugins).expect("load");
        let configs = handle
            .seen_configs
            .lock()
            .unwrap()
            .clone()
            .expect("init should have been called");
        assert_eq!(configs["a"], serde_json::json!({"k": "a"}));
        assert_eq!(configs["b"], serde_json::json!({"k": "b"}));
    }

    #[test]
    fn plugin_next_row_is_callable_concurrently() {
        let (plugin, handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let feeds = Arc::new(
            DataFeeds::load(&plugin_source("signer"), Path::new("."), plugins).expect("load"),
        );

        let workers: Vec<_> = (0..8u64)
            .map(|vu| {
                let feeds = Arc::clone(&feeds);
                std::thread::spawn(move || {
                    let mut st = VuFeedState::new();
                    let mut r = rng();
                    let id = RowIdentity {
                        vu,
                        iteration: 0,
                        scenario: "s",
                        request: None,
                    };
                    for _ in 0..50 {
                        feeds.next_row("signed", &mut st, &mut r, &id).expect("row");
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().expect("worker thread panicked");
        }
        assert_eq!(
            handle.calls.load(std::sync::atomic::Ordering::SeqCst),
            8 * 50
        );
    }

    #[test]
    fn has_on_demand_and_is_on_demand_flags() {
        let (plugin, _handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let mut plugins: HashMap<String, Box<dyn DataSourcePlugin>> = HashMap::new();
        plugins.insert("signer".to_string(), Box::new(plugin));
        let mut sources = plugin_source("signer");
        sources.insert(
            "static_rows".to_string(),
            DataSource::Inline {
                rows: vec![IndexMap::new()],
                mode: DataMode::Shared,
                on_eof: OnEof::Recycle,
                pick: PickStrategy::Sequential,
            },
        );
        let feeds = DataFeeds::load(&sources, Path::new("."), plugins).expect("load");
        assert!(feeds.has_on_demand());
        assert!(feeds.is_on_demand("signed"));
        assert!(!feeds.is_on_demand("static_rows"));
    }

    #[test]
    fn bracket_decision_respects_flag_and_runtime_flavor() {
        // No runtime on this thread: always inline.
        assert!(!bracket_plugin_fetch(true));
        assert!(!bracket_plugin_fetch(false));

        let multi = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("runtime");
        multi.block_on(async {
            assert!(bracket_plugin_fetch(true));
            assert!(!bracket_plugin_fetch(false), "off stays inline everywhere");
        });

        let current = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        current.block_on(async {
            assert!(
                !bracket_plugin_fetch(true),
                "block_in_place panics on a current_thread runtime; fall back inline"
            );
        });
    }

    /// The data-source twin of the protocol-side runtime-liveness regression:
    /// on a saturated one-worker runtime, a 300ms blocking-flagged fetch must
    /// not delay a concurrent 50ms timer — `block_in_place` demotes the
    /// worker and Tokio installs a replacement. The unchanged inline path
    /// would hold the timer for roughly the whole fetch.
    #[test]
    fn blocking_fetch_does_not_stall_the_runtime_timer() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .expect("runtime");
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (plugin, _handle) = fake_plugin(
            "sleepy",
            FakeMode::SleepyEcho {
                entered: entered_tx,
                delay: std::time::Duration::from_millis(300),
            },
        );
        let feeds = Arc::new(plugin_feeds(plugin, true));

        runtime.block_on(async {
            let fetch = tokio::spawn({
                let feeds = feeds.clone();
                async move {
                    let mut st = VuFeedState::new();
                    let mut r = rng();
                    feeds.next_row("rows", &mut st, &mut r, &id())
                }
            });
            // Arm the timer only once the fetch is provably inside next_row,
            // so task-start order cannot make the test pass accidentally.
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("the fetch entered the plugin");
            let armed = std::time::Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let timer_elapsed = armed.elapsed();
            assert!(
                timer_elapsed < std::time::Duration::from_millis(200),
                "the timer must fire while the 300ms fetch is in flight, took {timer_elapsed:?}"
            );
            let row = fetch
                .await
                .expect("fetch task join")
                .expect("the fetched row is returned intact");
            assert!(row.get("seq").is_some(), "the row survives the bracket");
        });
    }

    /// A blocking-flagged source on a current_thread runtime neither panics
    /// (block_in_place would) nor deadlocks — the flavor guard goes inline.
    #[test]
    fn blocking_flag_falls_back_inline_on_a_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let (plugin, _handle) = fake_plugin("signer", FakeMode::EchoSeq);
        let feeds = plugin_feeds(plugin, true);

        runtime.block_on(async {
            let mut st = VuFeedState::new();
            let mut r = rng();
            let row = feeds.next_row("rows", &mut st, &mut r, &id()).expect("row");
            assert!(row.get("seq").is_some());
        });
    }
}
