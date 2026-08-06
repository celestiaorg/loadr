//! Local `plugin.toml` manifest for one native feeder library.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::PluginError;

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
    entry: String,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Clone)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub entry: PathBuf,
    pub description: String,
    pub default_config: serde_json::Value,
    pub dir: PathBuf,
    pub enabled: bool,
}

impl PluginManifest {
    pub fn parse(source: &str, dir: &Path) -> Result<Self, PluginError> {
        let file: ManifestFile = toml::from_str(source).map_err(|error| PluginError::Manifest {
            path: dir.join("plugin.toml").display().to_string(),
            message: error.to_string(),
        })?;
        let default_config = file
            .config
            .map(|table| {
                serde_json::to_value(table).map_err(|error| PluginError::Manifest {
                    path: dir.join("plugin.toml").display().to_string(),
                    message: format!("cannot convert [config] to JSON: {error}"),
                })
            })
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        Ok(Self {
            name: file.plugin.name,
            version: file.plugin.version,
            entry: dir.join(file.plugin.entry),
            description: file.plugin.description,
            default_config,
            dir: dir.to_path_buf(),
            enabled: !dir.join(crate::registry::DISABLED_MARKER).exists(),
        })
    }

    pub fn load(dir: &Path) -> Result<Self, PluginError> {
        let path = dir.join("plugin.toml");
        let source =
            std::fs::read_to_string(&path).map_err(|error| PluginError::io(&path, error))?;
        Self::parse(&source, dir)
    }

    pub fn merged_config(&self, overrides: &serde_json::Value) -> serde_json::Value {
        merge_config(&self.default_config, overrides)
    }
}

pub fn merge_config(
    defaults: &serde_json::Value,
    overrides: &serde_json::Value,
) -> serde_json::Value {
    match (defaults, overrides) {
        (serde_json::Value::Object(defaults), serde_json::Value::Object(overrides)) => {
            let mut merged = defaults.clone();
            merged.extend(overrides.clone());
            serde_json::Value::Object(merged)
        }
        (defaults, serde_json::Value::Null) => defaults.clone(),
        (_, overrides) => overrides.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_feeder_manifest() {
        let manifest = PluginManifest::parse(
            r#"
[plugin]
name = "tx-signer"
version = "2.0.0"
entry = "libtx_signer.so"
description = "signed transactions"

[config]
seed = 7
"#,
            Path::new("/feeders/tx-signer"),
        )
        .expect("manifest");
        assert_eq!(manifest.name, "tx-signer");
        assert_eq!(manifest.default_config["seed"], 7);
        assert_eq!(
            manifest.entry,
            Path::new("/feeders/tx-signer/libtx_signer.so")
        );
    }
}
