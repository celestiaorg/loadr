//! Example native job plugin (`file-gen`).
//!
//! Writes `rows` JSONL records to `path` on its own pool of `threads`
//! workers. loadr never schedules the work: it calls `start` once, polls
//! `progress` for the live dashboard, and calls `stop` when the job is done
//! or the user stops the run. Tuning (`threads`, `batch`) is entirely the
//! plugin's business.
//!
//! Workers claim batches of row indexes from a shared counter, render each
//! batch into a private buffer, then take the file lock only to write it, so
//! generation runs in parallel and the file is never interleaved mid-line.
//! Rows are a pure function of `(seed, index)`: same config, same rows (the
//! order of batches in the file depends on scheduling).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use serde::Deserialize;

const NAME: &str = "file-gen";

#[derive(Deserialize)]
#[serde(default)]
struct Config {
    path: String,
    rows: u64,
    /// 0 = one per CPU.
    threads: usize,
    batch: u64,
    seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            path: "generated.jsonl".to_string(),
            rows: 1_000_000,
            threads: 0,
            batch: 10_000,
            seed: 42,
        }
    }
}

/// State the workers and `progress` share. Everything `progress` reads is an
/// atomic, so a poll never waits on a worker.
struct Shared {
    rows: u64,
    batch: u64,
    seed: u64,
    /// Next unclaimed row index.
    next: AtomicU64,
    /// Rows written to the file.
    done: AtomicU64,
    bytes: AtomicU64,
    /// Workers still running.
    alive: AtomicUsize,
    cancel: AtomicBool,
    failed: AtomicBool,
    error: Mutex<Option<String>>,
    out: Mutex<Option<BufWriter<File>>>,
}

impl Shared {
    fn fail(&self, error: String) {
        let mut slot = self.error.lock().unwrap_or_else(|e| e.into_inner());
        slot.get_or_insert(error);
        self.failed.store(true, Ordering::Release);
        self.cancel.store(true, Ordering::Release);
    }

    fn work(&self) {
        let mut buf = String::new();
        while !self.cancel.load(Ordering::Acquire) {
            let start = self.next.fetch_add(self.batch, Ordering::Relaxed);
            if start >= self.rows {
                break;
            }
            let end = start.saturating_add(self.batch).min(self.rows);
            buf.clear();
            for index in start..end {
                render_row(&mut buf, self.seed, index);
            }
            let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
            let Some(file) = out.as_mut() else {
                break; // stopped and closed under us
            };
            if let Err(e) = file.write_all(buf.as_bytes()) {
                drop(out);
                self.fail(format!("write failed: {e}"));
                break;
            }
            drop(out);
            self.bytes.fetch_add(buf.len() as u64, Ordering::Relaxed);
            self.done.fetch_add(end - start, Ordering::Relaxed);
        }
    }
}

/// One JSONL record, derived only from `(seed, index)`.
fn render_row(buf: &mut String, seed: u64, index: u64) {
    use std::fmt::Write as _;
    let h = splitmix64(seed ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let _ = writeln!(
        buf,
        r#"{{"id":{index},"user":"user-{}","amount":{}.{:02},"flag":{}}}"#,
        h % 100_000,
        (h >> 17) % 10_000,
        (h >> 40) % 100,
        (h >> 63) == 1,
    );
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[derive(Default)]
pub struct FileGen {
    shared: Option<Arc<Shared>>,
    threads: usize,
    workers: Vec<JoinHandle<()>>,
}

impl FileGen {
    fn progress_json(&self) -> serde_json::Value {
        let Some(shared) = &self.shared else {
            return serde_json::json!({"state": "running", "done": 0});
        };
        let done = shared.done.load(Ordering::Relaxed);
        let alive = shared.alive.load(Ordering::Acquire);
        let state = if shared.failed.load(Ordering::Acquire) {
            "failed"
        } else if alive == 0 && done >= shared.rows {
            "finished"
        } else {
            "running"
        };
        let error = shared
            .error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        serde_json::json!({
            "state": state,
            "done": done,
            "total": shared.rows,
            "unit": "rows",
            "error": error,
            "metrics": {
                "bytes_written": shared.bytes.load(Ordering::Relaxed),
                "active_threads": alive,
                "threads": self.threads,
            },
        })
    }
}

impl FfiService for FileGen {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if self.shared.is_some() {
            return RErr(RString::from("already started"));
        }
        let config: Config = match serde_json::from_str(config_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(format!("invalid config: {e}"))),
        };
        let file = match File::create(&config.path) {
            Ok(f) => f,
            Err(e) => return RErr(RString::from(format!("cannot create {}: {e}", config.path))),
        };
        let threads = if config.threads == 0 {
            std::thread::available_parallelism().map_or(1, |n| n.get())
        } else {
            config.threads
        };
        let shared = Arc::new(Shared {
            rows: config.rows,
            batch: config.batch.max(1),
            seed: config.seed,
            next: AtomicU64::new(0),
            done: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            alive: AtomicUsize::new(threads),
            cancel: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            error: Mutex::new(None),
            out: Mutex::new(Some(BufWriter::with_capacity(1 << 20, file))),
        });
        self.workers = (0..threads)
            .map(|i| {
                let shared = shared.clone();
                std::thread::Builder::new()
                    .name(format!("file-gen-{i}"))
                    .spawn(move || {
                        shared.work();
                        shared.alive.fetch_sub(1, Ordering::AcqRel);
                    })
                    .expect("spawn worker")
            })
            .collect();
        self.threads = threads;
        self.shared = Some(shared);
        ROk(RString::from(config.path))
    }

    fn stop(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        shared.cancel.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        let out = shared.out.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut file) = out {
            if let Err(e) = file.flush() {
                shared.fail(format!("flush failed: {e}"));
            }
        }
    }

    fn is_job(&self) -> bool {
        true
    }

    fn progress(&self) -> RString {
        RString::from(self.progress_json().to_string())
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "Generates a JSONL file on its own thread pool, reporting live progress",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(FileGen::default(), abi_stable::erased_types::TD_Opaque)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn run_to_end(gen: &mut FileGen) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let p = gen.progress_json();
            if p["state"] != "running" || Instant::now() > deadline {
                gen.stop();
                return gen.progress_json();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn writes_every_row_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let mut gen = FileGen::default();
        let config = serde_json::json!({
            "path": path, "rows": 10_007, "threads": 4, "batch": 100,
        });
        assert!(gen.start(RString::from(config.to_string())).is_ok());
        let p = run_to_end(&mut gen);
        assert_eq!(p["state"], "finished");
        assert_eq!(p["done"], 10_007);
        assert_eq!(p["metrics"]["threads"], 4);

        let text = std::fs::read_to_string(&path).unwrap();
        let mut ids: Vec<u64> = text
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..10_007).collect::<Vec<_>>());
        assert_eq!(p["metrics"]["bytes_written"], text.len() as u64);
    }

    #[test]
    fn stop_cancels_midway_and_keeps_whole_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let mut gen = FileGen::default();
        let config = serde_json::json!({"path": path, "rows": u64::MAX, "threads": 2});
        assert!(gen.start(RString::from(config.to_string())).is_ok());
        std::thread::sleep(Duration::from_millis(50));
        gen.stop();
        let p = gen.progress_json();
        assert_eq!(
            p["state"], "running",
            "not finished: the host marks it stopped"
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count() as u64, p["done"].as_u64().unwrap());
    }

    #[test]
    fn unwritable_path_fails_start() {
        let mut gen = FileGen::default();
        let config = serde_json::json!({"path": "/nonexistent-dir/x.jsonl"});
        assert!(gen.start(RString::from(config.to_string())).is_err());
    }

    #[test]
    fn rows_are_deterministic() {
        let (mut a, mut b) = (String::new(), String::new());
        render_row(&mut a, 7, 123);
        render_row(&mut b, 7, 123);
        assert_eq!(a, b);
        let row: serde_json::Value = serde_json::from_str(&a).unwrap();
        assert_eq!(row["id"], 123);
    }
}
