//! Local feeder removal.

use std::path::Path;

use crate::PluginError;

/// Remove an installed feeder. Returns `false` when it was not installed.
pub fn remove(plugins_dir: &Path, name: &str) -> Result<bool, PluginError> {
    let manifest = crate::PluginRegistry::discover(plugins_dir)?
        .into_iter()
        .find(|manifest| manifest.name == name);
    let Some(manifest) = manifest else {
        return Ok(false);
    };
    std::fs::remove_dir_all(&manifest.dir)
        .map_err(|error| PluginError::io(&manifest.dir, error))?;
    Ok(true)
}
