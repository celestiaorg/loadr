//! The `loadr` binary.

mod commands;
mod output_flag;
mod progress;
mod report_html;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "loadr",
    version = loadr_core::build_info::VERSION_WITH_REVISION,
    about = "A modern load testing platform: k6 + JMeter in one binary",
    long_about = "loadr runs declarative YAML load tests with embedded JavaScript, six built-in \
                  protocols, plugins, distributed agents and a live web UI.\n\
                  Docs: https://loadr.io/docs/",
    propagate_version = true
)]
struct Cli {
    /// Reduce output (errors only)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    quiet: u8,
    /// Increase output (-v: debug, -vv: trace)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Disable colored output
    #[arg(long, global = true)]
    no_color: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a test (standalone, or submit to a controller)
    Run(commands::run::RunArgs),
    /// Validate test files and print diagnostics
    Validate(commands::validate::ValidateArgs),
    /// Convert JMeter .jmx, k6 .js, HAR or access-log files to loadr YAML
    Convert(commands::convert::ConvertArgs),
    /// Compare two run summaries and flag regressions
    Compare(commands::compare::CompareArgs),
    /// Run a plan across a parameter matrix and compare the results
    Sweep(commands::sweep::SweepArgs),
    /// Generate adversarial payloads for algorithmic-complexity (DoS) testing
    Payload(commands::payload::PayloadArgs),
    /// Record live HTTP(S) traffic through a proxy into an auto-correlated scenario
    Record(commands::record::RecordArgs),
    /// Generate a scenario from an API contract (OpenAPI) with example data
    Gen(commands::gen::GenArgs),
    /// Explain a run summary in plain language (root-cause read)
    Explain(commands::explain::ExplainArgs),
    /// Generate a scenario from a natural-language description (LLM)
    Scenario(commands::scenario::ScenarioArgs),
    /// Durable run history + statistical regression detection
    #[command(subcommand)]
    History(commands::history::HistoryCommand),
    /// Run the distributed-mode controller
    Controller(commands::controller::ControllerArgs),
    /// Run a load-generating agent
    Agent(commands::agent::AgentArgs),
    /// Manage plugins
    #[command(subcommand)]
    Plugin(commands::plugin::PluginCommand),
    /// Render an HTML report from a summary JSON file
    Report(commands::report::ReportArgs),
    /// Print the JSON Schema for loadr test definitions
    Schema,
    /// Generate shell completions
    Completions {
        /// Shell to generate for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Print version information
    Version,
}

fn main() {
    let cli = Cli::parse();
    init_tracing(cli.quiet, cli.verbose);
    if cli.no_color || std::env::var_os("NO_COLOR").is_some() {
        owo_colors::set_override(false);
    }

    let code = match run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    };
    std::process::exit(code);
}

fn run(cli: Cli) -> anyhow::Result<i32> {
    match cli.command {
        Command::Run(args) => commands::run::execute(args, cli.quiet > 0),
        Command::Validate(args) => commands::validate::execute(args),
        Command::Convert(args) => commands::convert::execute(args),
        Command::Compare(args) => commands::compare::execute(args),
        Command::Sweep(args) => commands::sweep::execute(args),
        Command::Payload(args) => commands::payload::execute(args),
        Command::Record(args) => commands::record::execute(args),
        Command::Gen(args) => commands::gen::execute(args),
        Command::Explain(args) => commands::explain::execute(args),
        Command::Scenario(args) => commands::scenario::execute(args),
        Command::History(cmd) => commands::history::execute(cmd),
        Command::Controller(args) => commands::controller::execute(args),
        Command::Agent(args) => commands::agent::execute(args),
        Command::Plugin(cmd) => commands::plugin::execute(cmd),
        Command::Report(args) => commands::report::execute(args),
        Command::Schema => {
            println!(
                "{}",
                serde_json::to_string_pretty(&loadr_config::json_schema())?
            );
            Ok(0)
        }
        Command::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(shell, &mut Cli::command(), "loadr", &mut std::io::stdout());
            Ok(0)
        }
        Command::Version => {
            println!("loadr {}", loadr_core::build_info::VERSION_WITH_REVISION);
            println!(
                "  protocols: http/1.1, http/2, websocket, sse, grpc, graphql, tcp, udp, browser"
            );
            println!("  js engine: QuickJS (rquickjs)");
            println!("  arch: {}", std::env::consts::ARCH);
            Ok(0)
        }
    }
}

fn init_tracing(quiet: u8, verbose: u8) {
    use tracing_subscriber::EnvFilter;
    let default = match (quiet, verbose) {
        (q, _) if q > 0 => "error",
        (_, 0) => "warn,loadr=info",
        (_, 1) => "info,loadr=debug",
        _ => "debug,loadr=trace",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
