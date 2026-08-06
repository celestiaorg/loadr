//! `loadr agent` — join a controller and generate load on demand.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Args;
use owo_colors::OwoColorize;

#[derive(Args)]
pub struct AgentArgs {
    /// Controller address to join, e.g. `controller-host:7625`
    #[arg(long, value_name = "HOST:PORT")]
    pub join: String,
    /// Agent name shown in the fleet view (default: hostname)
    #[arg(long)]
    pub name: Option<String>,
    /// Stable agent id (default: generated; set for stable identity across restarts)
    #[arg(long)]
    pub id: Option<String>,
    /// Agent label, `key=value` (repeatable); used for agent targeting
    #[arg(long, value_name = "KEY=VALUE")]
    pub label: Vec<String>,
    /// Working directory for shipped data files
    #[arg(long, default_value = "/tmp/loadr-agent")]
    pub work_dir: PathBuf,
    /// CA bundle to verify the controller's TLS certificate
    #[arg(long)]
    pub tls_ca: Option<PathBuf>,
    /// Client certificate for mTLS
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// Client private key for mTLS
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Override the TLS server name
    #[arg(long)]
    pub tls_domain: Option<String>,
    /// Tokio worker threads for the agent (default: number of CPUs)
    #[arg(long, env = "LOADR_WORKER_THREADS")]
    pub worker_threads: Option<usize>,
    /// Directory containing locally installed feeder plugins.
    #[arg(long, env = "LOADR_PLUGINS_DIR")]
    pub plugins_dir: Option<PathBuf>,
}

pub fn execute(args: AgentArgs) -> anyhow::Result<i32> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.worker_threads {
        builder.worker_threads(n.max(1));
    }
    let runtime = builder.enable_all().build()?;
    runtime.block_on(async move {
        let mut labels = HashMap::new();
        for label in &args.label {
            let (k, v) = label
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("label `{label}` must be key=value"))?;
            labels.insert(k.to_string(), v.to_string());
        }
        let tls_enabled = args.tls_ca.is_some() || args.tls_cert.is_some();
        let scheme = if tls_enabled { "https" } else { "http" };
        let controller_addr = if args.join.starts_with("http") {
            args.join.clone()
        } else {
            format!("{scheme}://{}", args.join)
        };

        let name = args.name.clone().unwrap_or_else(|| {
            std::env::var("HOSTNAME").unwrap_or_else(|_| "loadr-agent".to_string())
        });

        // Every agent has the same gRPC transport surface.
        let protocols: loadr_agent::ProtocolFactory = Arc::new(|plan, base_dir| {
            loadr_grpc::builtin_registry(&plan.defaults.http, base_dir).map_err(|e| e.to_string())
        });
        // Data-source plugins declared in the plan. The controller ships no
        // plugin binaries: they resolve on this host from LOADR_PLUGINS_DIR
        // or ~/.loadr/plugins (or an explicit `path:` in the plan).
        let plugins_dir = args
            .plugins_dir
            .clone()
            .unwrap_or_else(loadr_feeder_api::default_plugins_dir);
        let data_sources: loadr_agent::DataSourceFactory =
            Arc::new(move |plugin_refs, base_dir| {
                let mut sources: HashMap<String, Box<dyn loadr_core::DataSourcePlugin>> =
                    HashMap::new();
                for plugin_ref in plugin_refs {
                    if !plugin_ref.enabled {
                        continue;
                    }
                    let data_source = loadr_feeder_api::PluginRegistry::load_ref(
                        plugin_ref,
                        &plugins_dir,
                        base_dir,
                    )
                    .map_err(|e| format!("plugin `{}`: {e}", plugin_ref.name))?;
                    sources.insert(plugin_ref.name.clone(), data_source);
                }
                Ok(sources)
            });

        let config = loadr_agent::AgentConfig {
            controller_addr: controller_addr.clone(),
            agent_id: args.id.clone(),
            agent_name: name.clone(),
            labels,
            tls: tls_enabled.then(|| loadr_agent::AgentTls {
                ca_pem: args.tls_ca.clone(),
                cert_pem: args.tls_cert.clone(),
                key_pem: args.tls_key.clone(),
                domain: args.tls_domain.clone(),
            }),
            work_dir: args.work_dir.clone(),
            deps: loadr_agent::RunnerDeps {
                protocols,
                script: None,
                data_sources: Some(data_sources),
            },
        };

        eprintln!(
            "{} agent `{name}` joining {controller_addr} (Ctrl-C to leave)",
            "→".cyan()
        );
        let shutdown = tokio_util::sync::CancellationToken::new();
        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                eprintln!("\nleaving the fleet...");
                shutdown.cancel();
            });
        }
        loadr_agent::Agent::run(config, shutdown).await?;
        Ok(0)
    })
}
