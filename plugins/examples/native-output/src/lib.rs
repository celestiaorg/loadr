//! Example native output plugin (`file-report`).
//!
//! Appends one line per snapshot (the series count) and the final summary to
//! the file named in config `{"path": "..."}`.

use std::fs::{File, OpenOptions};
use std::io::Write as _;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "file-report";

#[derive(Default)]
struct FileReport {
    file: Option<File>,
    snapshots: u64,
}

impl FileReport {
    fn write_line(&mut self, line: &str) {
        if let Some(file) = self.file.as_mut() {
            let _ = writeln!(file, "{line}");
        }
    }
}

impl FfiOutput for FileReport {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let config: serde_json::Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        let path = match config.get("path").and_then(|v| v.as_str()) {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => return RErr(RString::from("config requires a non-empty `path` string")),
        };
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => {
                self.file = Some(file);
                ROk(())
            }
            Err(e) => RErr(RString::from(format!("cannot open {path}: {e}"))),
        }
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        let series = serde_json::from_str::<serde_json::Value>(snapshot_json.as_str())
            .ok()
            .and_then(|v| v.get("series").and_then(|s| s.as_array()).map(Vec::len))
            .unwrap_or(0);
        self.snapshots += 1;
        let n = self.snapshots;
        self.write_line(&format!("snapshot {n}: series={series}"));
    }

    fn finish(&mut self, summary_json: RString) {
        let summary: serde_json::Value =
            serde_json::from_str(summary_json.as_str()).unwrap_or(serde_json::Value::Null);
        let run_id = summary
            .get("run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let duration = summary
            .get("duration_secs")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let metrics = summary
            .get("metrics")
            .and_then(|v| v.as_array())
            .map(Vec::len)
            .unwrap_or(0);
        let n = self.snapshots;
        self.write_line(&format!(
            "summary: run_id={run_id} duration_secs={duration:.3} metrics={metrics} snapshots={n}"
        ));
        if let Some(file) = self.file.as_mut() {
            let _ = file.flush();
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Appends snapshot series counts and the final summary to a file",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(FileReport::default(), abi_stable::erased_types::TD_Opaque)
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
