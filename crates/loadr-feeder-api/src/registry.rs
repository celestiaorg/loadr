//! Local feeder discovery, loading, enable/disable, and installation.

use std::path::{Path, PathBuf};

use loadr_core::DataSourcePlugin;

use crate::error::PluginError;
use crate::manifest::PluginManifest;
use crate::native::NativeFeeder;

/// Marker file that disables a feeder without uninstalling it.
pub const DISABLED_MARKER: &str = "disabled";

/// The default feeder directory: `$LOADR_PLUGINS_DIR` or `~/.loadr/plugins`.
pub fn default_plugins_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("LOADR_PLUGINS_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .unwrap_or_else(|| ".".into());
    Path::new(&home).join(".loadr").join("plugins")
}

/// Discovery and loading entry points for native feeder libraries.
pub struct PluginRegistry;

impl PluginRegistry {
    /// Scan `dir` for feeder installations. Invalid entries are skipped with
    /// a warning so one broken local install does not hide the others.
    pub fn discover(dir: &Path) -> Result<Vec<PluginManifest>, PluginError> {
        let mut manifests = Vec::new();
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(manifests),
            Err(error) => return Err(PluginError::io(dir, error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| PluginError::io(dir, error))?;
            let path = entry.path();
            if !path.is_dir() || !path.join("plugin.toml").is_file() {
                continue;
            }
            match PluginManifest::load(&path) {
                Ok(manifest) => manifests.push(manifest),
                Err(error) => {
                    tracing::warn!(dir = %path.display(), %error, "skipping invalid feeder")
                }
            }
        }
        manifests.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(manifests)
    }

    /// Resolve a plan `plugins:` reference and construct its data source.
    /// Relative explicit paths are resolved against the plan's base directory.
    pub fn load_ref(
        plugin_ref: &loadr_config::PluginRef,
        plugins_dir: &Path,
        base_dir: &Path,
    ) -> Result<Box<dyn DataSourcePlugin>, PluginError> {
        if let Some(configured_path) = &plugin_ref.path {
            let path = if configured_path.is_absolute() {
                configured_path.clone()
            } else {
                base_dir.join(configured_path)
            };
            let manifest = path
                .parent()
                .filter(|dir| dir.join("plugin.toml").is_file())
                .map(PluginManifest::load)
                .transpose()?;
            let config = manifest
                .as_ref()
                .map(|manifest| manifest.merged_config(&plugin_ref.config))
                .unwrap_or_else(|| plugin_ref.config.clone());
            let feeder = NativeFeeder::load(&path)?;
            return Ok(Box::new(feeder.make_data_source(config)));
        }

        let manifest = Self::discover(plugins_dir)?
            .into_iter()
            .find(|manifest| manifest.name == plugin_ref.name && manifest.enabled)
            .ok_or_else(|| PluginError::NotFound {
                name: plugin_ref.name.clone(),
                dir: plugins_dir.display().to_string(),
            })?;
        let feeder = NativeFeeder::load(&manifest.entry)?;
        Ok(Box::new(feeder.make_data_source(
            manifest.merged_config(&plugin_ref.config),
        )))
    }

    /// Enable or disable an installed feeder by toggling its marker file.
    pub fn set_enabled(plugins_dir: &Path, name: &str, enabled: bool) -> Result<(), PluginError> {
        let manifest = Self::discover(plugins_dir)?
            .into_iter()
            .find(|manifest| manifest.name == name)
            .ok_or_else(|| PluginError::NotFound {
                name: name.to_string(),
                dir: plugins_dir.display().to_string(),
            })?;
        let marker = manifest.dir.join(DISABLED_MARKER);
        if enabled {
            match std::fs::remove_file(&marker) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(PluginError::io(&marker, error)),
            }
        } else {
            std::fs::write(&marker, []).map_err(|error| PluginError::io(&marker, error))
        }
    }

    /// Install a feeder by copying the files in a local manifest directory.
    pub fn install_from_dir(src: &Path, plugins_dir: &Path) -> Result<PluginManifest, PluginError> {
        let source_manifest = PluginManifest::load(src)?;
        let destination = plugins_dir.join(&source_manifest.name);
        std::fs::create_dir_all(&destination)
            .map_err(|error| PluginError::io(&destination, error))?;
        for entry in std::fs::read_dir(src).map_err(|error| PluginError::io(src, error))? {
            let entry = entry.map_err(|error| PluginError::io(src, error))?;
            let from = entry.path();
            if from.is_file() {
                let to = destination.join(entry.file_name());
                std::fs::copy(&from, &to).map_err(|error| PluginError::io(&to, error))?;
            }
        }
        PluginManifest::load(&destination)
    }
}
