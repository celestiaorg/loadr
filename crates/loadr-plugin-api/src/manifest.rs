//! `plugin.toml` manifests describing installed plugins.
//!
//! A plugin installation is a directory containing a `plugin.toml` next to
//! its artifact (a `.wasm` component or a native dynamic library):
//!
//! ```toml
//! [plugin]
//! name = "upper-boundary"
//! version = "0.1.0"
//! kind = "extractor"          # extractor | assertion | output | protocol | service
//! type = "wasm"               # wasm | native
//! entry = "plugin.wasm"       # relative to the plugin directory
//! description = "..."
//!
//! [config]                    # optional defaults, merged under PluginRef.config
//! left = "<<"
//! right = ">>"
//! ```

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// What a plugin provides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    Extractor,
    Assertion,
    Output,
    Protocol,
    Service,
}

impl PluginKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginKind::Extractor => "extractor",
            PluginKind::Assertion => "assertion",
            PluginKind::Output => "output",
            PluginKind::Protocol => "protocol",
            PluginKind::Service => "service",
        }
    }

    pub fn parse(s: &str) -> Option<PluginKind> {
        match s {
            "extractor" => Some(PluginKind::Extractor),
            "assertion" => Some(PluginKind::Assertion),
            "output" => Some(PluginKind::Output),
            "protocol" => Some(PluginKind::Protocol),
            "service" => Some(PluginKind::Service),
            _ => None,
        }
    }
}

impl std::fmt::Display for PluginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a plugin is packaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    Wasm,
    Native,
}

/// The ABI a `type = "native"` dynamic library exposes.
///
/// Optional in `plugin.toml` (`abi = "native"` | `abi = "c"`); when absent the
/// loader auto-detects by probing for the C-ABI entry symbol, so this is only
/// a hint/override. Ignored for `type = "wasm"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PluginAbi {
    /// The `abi_stable` Rust-to-Rust ABI ([`crate::native`]). The default.
    #[default]
    Native,
    /// The plain C ABI ([`crate::cabi`]) for non-Rust plugins.
    C,
}

#[derive(Debug, Deserialize)]
struct ManifestFile {
    plugin: ManifestPlugin,
    #[serde(default)]
    config: Option<toml::Table>,
}

#[derive(Debug, Deserialize)]
struct ManifestPlugin {
    name: String,
    version: String,
    kind: PluginKind,
    #[serde(rename = "type")]
    plugin_type: PluginType,
    entry: String,
    /// Optional ABI hint for `type = "native"` libraries (`native` | `c`).
    /// When omitted the loader auto-detects.
    #[serde(default)]
    abi: Option<PluginAbi>,
    #[serde(default)]
    description: String,
    /// URL scheme(s) this protocol plugin serves (e.g. `["mongodb", "mongo"]`).
    /// Only meaningful for `kind = "protocol"`; the host maps a request URL
    /// scheme to this plugin once installed.
    #[serde(default)]
    schemes: Vec<String>,
    /// Informational capability list (e.g. `["data_source"]` for a
    /// `kind = "service"` plugin). Authoritative capability detection is
    /// the loaded module's `make_data_source`/`make_service` presence, not
    /// this field.
    #[serde(default)]
    capabilities: Vec<String>,
}

/// A parsed, resolved plugin manifest.
#[derive(Debug, Clone)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub kind: PluginKind,
    pub plugin_type: PluginType,
    /// Optional ABI hint for native libraries. `None` means "auto-detect".
    pub abi: Option<PluginAbi>,
    /// Absolute path to the plugin artifact.
    pub entry: PathBuf,
    pub description: String,
    /// Default configuration from `[config]` (JSON object, or null).
    pub default_config: serde_json::Value,
    /// URL scheme(s) a protocol plugin serves (from `[plugin].schemes`).
    /// Empty for non-protocol plugins or protocol plugins routed only by an
    /// explicit `protocol:` matching the plugin name.
    pub schemes: Vec<String>,
    /// Informational capability list from `[plugin].capabilities`.
    pub capabilities: Vec<String>,
    /// The plugin's installation directory.
    pub dir: PathBuf,
    /// False when a `disabled` marker file is present in `dir`.
    pub enabled: bool,
}

impl PluginManifest {
    /// Parse a manifest from TOML, resolving `entry` against `dir`.
    pub fn parse(toml_str: &str, dir: &Path) -> Result<PluginManifest, PluginError> {
        let file: ManifestFile = toml::from_str(toml_str).map_err(|e| PluginError::Manifest {
            path: dir.join("plugin.toml").display().to_string(),
            message: e.to_string(),
        })?;
        let default_config = match file.config {
            Some(table) => serde_json::to_value(&table).map_err(|e| PluginError::Manifest {
                path: dir.join("plugin.toml").display().to_string(),
                message: format!("cannot convert [config] to JSON: {e}"),
            })?,
            None => serde_json::Value::Null,
        };
        let p = file.plugin;
        Ok(PluginManifest {
            name: p.name,
            version: p.version,
            kind: p.kind,
            plugin_type: p.plugin_type,
            abi: p.abi,
            entry: dir.join(&p.entry),
            description: p.description,
            schemes: p.schemes,
            capabilities: p.capabilities,
            default_config,
            dir: dir.to_path_buf(),
            enabled: !dir.join(crate::registry::DISABLED_MARKER).exists(),
        })
    }

    /// Read and parse `<dir>/plugin.toml`.
    pub fn load(dir: &Path) -> Result<PluginManifest, PluginError> {
        let path = dir.join("plugin.toml");
        let text = std::fs::read_to_string(&path).map_err(|e| PluginError::io(&path, e))?;
        PluginManifest::parse(&text, dir)
    }

    /// The plugin's default config with `overrides` merged on top
    /// (shallow object merge; non-object overrides win wholesale).
    pub fn merged_config(&self, overrides: &serde_json::Value) -> serde_json::Value {
        merge_config(&self.default_config, overrides)
    }
}

/// Shallow-merge two JSON configs: keys in `overrides` win.
pub fn merge_config(
    defaults: &serde_json::Value,
    overrides: &serde_json::Value,
) -> serde_json::Value {
    match (defaults, overrides) {
        (serde_json::Value::Object(d), serde_json::Value::Object(o)) => {
            let mut merged = d.clone();
            for (k, v) in o {
                merged.insert(k.clone(), v.clone());
            }
            serde_json::Value::Object(merged)
        }
        (d, serde_json::Value::Null) => d.clone(),
        (_, o) => o.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"
[plugin]
name = "upper-boundary"
version = "0.2.0"
kind = "extractor"
type = "wasm"
entry = "plugin.wasm"
description = "Boundary extractor"

[config]
left = "<<"
right = ">>"
depth = 3
"#;

    #[test]
    fn parses_full_manifest() {
        let m = PluginManifest::parse(MANIFEST, Path::new("/plugins/upper-boundary"))
            .expect("manifest parses");
        assert_eq!(m.name, "upper-boundary");
        assert_eq!(m.version, "0.2.0");
        assert_eq!(m.kind, PluginKind::Extractor);
        assert_eq!(m.plugin_type, PluginType::Wasm);
        assert_eq!(
            m.entry,
            PathBuf::from("/plugins/upper-boundary/plugin.wasm")
        );
        assert_eq!(m.description, "Boundary extractor");
        assert_eq!(m.default_config["left"], "<<");
        assert_eq!(m.default_config["depth"], 3);
    }

    #[test]
    fn parses_protocol_schemes() {
        let m = PluginManifest::parse(
            r#"
[plugin]
name = "mongo"
version = "1.0.0"
kind = "protocol"
type = "native"
entry = "libmongo.so"
schemes = ["mongodb", "mongo"]
"#,
            Path::new("/p/mongo"),
        )
        .expect("manifest parses");
        assert_eq!(m.kind, PluginKind::Protocol);
        assert_eq!(m.schemes, vec!["mongodb".to_string(), "mongo".to_string()]);
    }

    #[test]
    fn schemes_default_empty() {
        let m = PluginManifest::parse(MANIFEST, Path::new("/p/x")).expect("parses");
        assert!(m.schemes.is_empty());
    }

    #[test]
    fn capabilities_default_empty_and_parse_when_present() {
        let m = PluginManifest::parse(MANIFEST, Path::new("/p/x")).expect("parses");
        assert!(m.capabilities.is_empty());

        let m = PluginManifest::parse(
            r#"
[plugin]
name = "tx-signer"
version = "0.1.0"
kind = "service"
type = "native"
entry = "libtx_signer.so"
capabilities = ["data_source"]
"#,
            Path::new("/p/tx-signer"),
        )
        .expect("manifest parses");
        assert_eq!(m.capabilities, vec!["data_source".to_string()]);
    }

    #[test]
    fn abi_defaults_to_none_when_absent() {
        // Backward compatibility: existing manifests have no `abi` key.
        let m = PluginManifest::parse(MANIFEST, Path::new("/p/x")).expect("parses");
        assert_eq!(m.abi, None, "absent abi key means auto-detect");
    }

    #[test]
    fn parses_explicit_c_abi() {
        let m = PluginManifest::parse(
            r#"
[plugin]
name = "cecho"
version = "0.1.0"
kind = "protocol"
type = "native"
abi = "c"
entry = "libcecho.so"
"#,
            Path::new("/p/cecho"),
        )
        .expect("manifest parses");
        assert_eq!(m.abi, Some(PluginAbi::C));
    }

    #[test]
    fn parses_explicit_native_abi() {
        let m = PluginManifest::parse(
            r#"
[plugin]
name = "x"
version = "0.1.0"
kind = "protocol"
type = "native"
abi = "native"
entry = "libx.so"
"#,
            Path::new("/p/x"),
        )
        .expect("manifest parses");
        assert_eq!(m.abi, Some(PluginAbi::Native));
    }

    #[test]
    fn rejects_bad_abi() {
        let err = PluginManifest::parse(
            r#"
[plugin]
name = "x"
version = "0.1.0"
kind = "protocol"
type = "native"
abi = "wasm32"
entry = "libx.so"
"#,
            Path::new("/p/x"),
        )
        .expect_err("invalid abi");
        assert!(matches!(err, PluginError::Manifest { .. }), "{err}");
    }

    #[test]
    fn parses_minimal_manifest() {
        let m = PluginManifest::parse(
            r#"
[plugin]
name = "echo-proto"
version = "0.1.0"
kind = "protocol"
type = "native"
entry = "libnative_protocol.so"
"#,
            Path::new("/p/echo"),
        )
        .expect("manifest parses");
        assert_eq!(m.kind, PluginKind::Protocol);
        assert_eq!(m.plugin_type, PluginType::Native);
        assert_eq!(m.description, "");
        assert!(m.default_config.is_null());
    }

    #[test]
    fn rejects_bad_kind() {
        let err = PluginManifest::parse(
            r#"
[plugin]
name = "x"
version = "0.1.0"
kind = "wizard"
type = "wasm"
entry = "x.wasm"
"#,
            Path::new("/p/x"),
        )
        .expect_err("invalid kind");
        assert!(matches!(err, PluginError::Manifest { .. }), "{err}");
    }

    #[test]
    fn rejects_missing_fields() {
        let err = PluginManifest::parse("[plugin]\nname = \"x\"\n", Path::new("/p/x"))
            .expect_err("missing fields");
        assert!(matches!(err, PluginError::Manifest { .. }), "{err}");
    }

    #[test]
    fn merge_config_semantics() {
        let defaults = serde_json::json!({"a": 1, "b": 2});
        let overrides = serde_json::json!({"b": 3, "c": 4});
        let merged = merge_config(&defaults, &overrides);
        assert_eq!(merged, serde_json::json!({"a": 1, "b": 3, "c": 4}));
        assert_eq!(
            merge_config(&defaults, &serde_json::Value::Null),
            defaults,
            "null override keeps defaults"
        );
        assert_eq!(
            merge_config(&serde_json::Value::Null, &overrides),
            overrides,
            "object override replaces null defaults"
        );
    }
}
