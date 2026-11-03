//! Built-in metric outputs/exporters for loadr.
//!
//! One module per output: [`json::JsonOutput`], [`csv_out::CsvOutput`],
//! [`prometheus::PrometheusOutput`], [`influxdb::InfluxdbOutput`],
//! [`otlp::OtlpOutput`] and [`statsd::StatsdOutput`]. [`build_outputs`]
//! constructs them from a test plan's `outputs:` section.

use std::time::Duration;

use loadr_config::OutputConfig;
use loadr_core::error::EngineError;
use loadr_core::output::Output;

pub mod csv_out;
mod http_client;
pub mod influxdb;
pub mod json;
pub mod observe;
pub mod otlp;
pub mod prometheus;
pub mod statsd;
#[cfg(test)]
mod test_support;

pub use csv_out::CsvOutput;
pub use influxdb::InfluxdbOutput;
pub use json::JsonOutput;
pub use otlp::OtlpOutput;
pub use prometheus::PrometheusOutput;
pub use statsd::StatsdOutput;

/// Default push interval for the interval-driven outputs.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

fn interval_or_default(interval: &Option<loadr_config::Dur>) -> Duration {
    match interval {
        Some(d) if !d.is_zero() => d.as_duration(),
        _ => DEFAULT_INTERVAL,
    }
}

/// Construct all built-in outputs from a test plan's `outputs:` section.
///
/// `OutputConfig::Plugin` entries are handled by the plugin system and are
/// skipped here. Relative file paths (`json`/`csv`) resolve against the
/// current working directory; `base_dir` is accepted for signature stability
/// but not currently used.
pub fn build_outputs(
    configs: &[OutputConfig],
    base_dir: &std::path::Path,
) -> Result<Vec<Box<dyn Output>>, EngineError> {
    let _ = base_dir;
    let mut outputs: Vec<Box<dyn Output>> = Vec::new();
    for config in configs {
        match config {
            OutputConfig::Json { path } => {
                outputs.push(Box::new(JsonOutput::new(path.clone())));
            }
            OutputConfig::Csv { path } => {
                outputs.push(Box::new(CsvOutput::new(path.clone())));
            }
            OutputConfig::Prometheus {
                listen,
                remote_write_url,
                interval,
                final_scrape_grace,
            } => {
                if listen.is_none() && remote_write_url.is_none() {
                    return Err(EngineError::Config(
                        "prometheus output requires `listen` and/or `remote_write_url`".to_string(),
                    ));
                }
                let mut output = PrometheusOutput::new(
                    listen.clone(),
                    remote_write_url.clone(),
                    interval_or_default(interval),
                );
                if let Some(grace) = final_scrape_grace {
                    output = output.with_final_scrape_grace(grace.as_duration());
                }
                outputs.push(Box::new(output));
            }
            OutputConfig::Influxdb {
                url,
                database,
                token,
                organization,
                interval,
            } => {
                outputs.push(Box::new(InfluxdbOutput::new(
                    url.clone(),
                    database.clone(),
                    token.clone(),
                    organization.clone(),
                    interval_or_default(interval),
                )));
            }
            OutputConfig::Otlp {
                endpoint,
                protocol,
                headers,
                interval,
            } => {
                outputs.push(Box::new(OtlpOutput::new(
                    endpoint.clone(),
                    *protocol,
                    headers.clone(),
                    interval_or_default(interval),
                )));
            }
            OutputConfig::Statsd { address, prefix } => {
                outputs.push(Box::new(StatsdOutput::new(address.clone(), prefix.clone())));
            }
            // Plugin outputs are constructed by the plugin system.
            OutputConfig::Plugin { .. } => {}
        }
    }
    Ok(outputs)
}

/// Generated protobuf types (compiled by `build.rs` with protox).
pub mod proto {
    /// Prometheus remote-write wire types.
    pub mod prometheus {
        #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }

    /// OTLP (OpenTelemetry protocol) wire types.
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.common.v1.rs"
                    ));
                }
            }
            pub mod resource {
                pub mod v1 {
                    #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.resource.v1.rs"
                    ));
                }
            }
            pub mod metrics {
                pub mod v1 {
                    #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.metrics.v1.rs"
                    ));
                }
            }
            pub mod collector {
                pub mod metrics {
                    pub mod v1 {
                        #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.collector.metrics.v1.rs"
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_config::OtlpProtocol;

    #[test]
    fn builds_all_non_plugin_outputs() {
        let configs = vec![
            OutputConfig::Json {
                path: "out.jsonl".into(),
            },
            OutputConfig::Csv {
                path: "out.csv".into(),
            },
            OutputConfig::Prometheus {
                listen: Some("127.0.0.1:0".into()),
                remote_write_url: None,
                interval: None,
                final_scrape_grace: None,
            },
            OutputConfig::Influxdb {
                url: "http://127.0.0.1:8086".into(),
                database: "loadr".into(),
                token: None,
                organization: None,
                interval: None,
            },
            OutputConfig::Otlp {
                endpoint: "127.0.0.1:4317".into(),
                protocol: OtlpProtocol::Grpc,
                headers: indexmap::IndexMap::new(),
                interval: None,
            },
            OutputConfig::Statsd {
                address: "127.0.0.1:8125".into(),
                prefix: None,
            },
            OutputConfig::Plugin {
                name: "custom".into(),
                config: serde_json::Value::Null,
            },
        ];
        let outputs = build_outputs(&configs, std::path::Path::new(".")).expect("build");
        let names: Vec<&str> = outputs.iter().map(|o| o.name()).collect();
        assert_eq!(
            names,
            vec!["json", "csv", "prometheus", "influxdb", "otlp", "statsd"]
        );
    }

    #[test]
    fn prometheus_requires_listen_or_remote_write() {
        let configs = vec![OutputConfig::Prometheus {
            listen: None,
            remote_write_url: None,
            interval: None,
            final_scrape_grace: None,
        }];
        match build_outputs(&configs, std::path::Path::new(".")) {
            Err(EngineError::Config(msg)) => {
                assert!(msg.contains("prometheus"), "{msg}");
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected a config error"),
        }
    }

    #[test]
    fn interval_defaults_to_five_seconds() {
        assert_eq!(interval_or_default(&None), DEFAULT_INTERVAL);
        assert_eq!(
            interval_or_default(&Some(loadr_config::Dur::ZERO)),
            DEFAULT_INTERVAL
        );
        assert_eq!(
            interval_or_default(&Some(loadr_config::Dur::from_secs(2))),
            Duration::from_secs(2)
        );
    }
}
