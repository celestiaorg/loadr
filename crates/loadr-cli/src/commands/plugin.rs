//! Local native feeder installation and discovery.

use std::path::PathBuf;

use clap::Subcommand;
use owo_colors::OwoColorize;

#[derive(Subcommand)]
pub enum PluginCommand {
    /// List locally installed feeder plugins.
    List {
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
    /// Install a feeder from a local directory containing `plugin.toml`.
    Install {
        source: PathBuf,
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
    /// Show one installed feeder.
    Info {
        name: String,
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
    /// Remove an installed feeder.
    Remove {
        name: String,
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
    /// Enable an installed feeder.
    Enable {
        name: String,
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
    /// Disable an installed feeder without deleting it.
    Disable {
        name: String,
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
    },
}

fn dir(flag: Option<PathBuf>) -> PathBuf {
    flag.unwrap_or_else(loadr_feeder_api::default_plugins_dir)
}

pub fn execute(cmd: PluginCommand) -> anyhow::Result<i32> {
    match cmd {
        PluginCommand::List { plugins_dir } => {
            let dir = dir(plugins_dir);
            let manifests = loadr_feeder_api::PluginRegistry::discover(&dir)?;
            if manifests.is_empty() {
                println!("no feeder plugins installed in {}", dir.display());
                return Ok(0);
            }
            println!(
                "{:<24} {:<10} {:<8} {}",
                "NAME".bold(),
                "VERSION".bold(),
                "STATE".bold(),
                "ENTRY".bold()
            );
            for manifest in manifests {
                let state = if manifest.enabled {
                    "enabled".green().to_string()
                } else {
                    "disabled".red().to_string()
                };
                println!(
                    "{:<24} {:<10} {:<8} {}",
                    manifest.name,
                    manifest.version,
                    state,
                    manifest.entry.display()
                );
            }
            Ok(0)
        }
        PluginCommand::Install {
            source,
            plugins_dir,
        } => {
            let target = dir(plugins_dir);
            let manifest = loadr_feeder_api::PluginRegistry::install_from_dir(&source, &target)?;
            println!(
                "{} installed `{}` v{} into {}",
                "✓".green(),
                manifest.name,
                manifest.version,
                target.display()
            );
            Ok(0)
        }
        PluginCommand::Info { name, plugins_dir } => {
            let root = dir(plugins_dir);
            let manifest = loadr_feeder_api::PluginRegistry::discover(&root)?
                .into_iter()
                .find(|manifest| manifest.name == name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "feeder plugin `{name}` is not installed in {}",
                        root.display()
                    )
                })?;
            println!("{}: {}", "name".bold(), manifest.name);
            println!("{}: {}", "version".bold(), manifest.version);
            println!("{}: {}", "entry".bold(), manifest.entry.display());
            println!("{}: {}", "enabled".bold(), manifest.enabled);
            if !manifest.description.is_empty() {
                println!("{}: {}", "description".bold(), manifest.description);
            }
            Ok(0)
        }
        PluginCommand::Remove { name, plugins_dir } => {
            let root = dir(plugins_dir);
            if !loadr_feeder_api::remove(&root, &name)? {
                anyhow::bail!(
                    "feeder plugin `{name}` is not installed in {}",
                    root.display()
                );
            }
            println!("{} removed `{name}`", "✓".green());
            Ok(0)
        }
        PluginCommand::Enable { name, plugins_dir } => {
            loadr_feeder_api::PluginRegistry::set_enabled(&dir(plugins_dir), &name, true)?;
            println!("{} `{name}` enabled", "✓".green());
            Ok(0)
        }
        PluginCommand::Disable { name, plugins_dir } => {
            loadr_feeder_api::PluginRegistry::set_enabled(&dir(plugins_dir), &name, false)?;
            println!("{} `{name}` disabled", "✓".green());
            Ok(0)
        }
    }
}
