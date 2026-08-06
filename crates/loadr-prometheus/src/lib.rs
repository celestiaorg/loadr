//! Prometheus scrape and remote-write output for loadr.

use std::time::Duration;

use loadr_config::OutputConfig;
use loadr_core::error::EngineError;
use loadr_core::output::Output;

mod http_client;
pub mod prometheus;
#[cfg(test)]
mod test_support;

pub use prometheus::PrometheusOutput;

pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

fn interval_or_default(interval: &Option<loadr_config::Dur>) -> Duration {
    match interval {
        Some(duration) if !duration.is_zero() => duration.as_duration(),
        _ => DEFAULT_INTERVAL,
    }
}

pub fn build_outputs(
    configs: &[OutputConfig],
    _base_dir: &std::path::Path,
) -> Result<Vec<Box<dyn Output>>, EngineError> {
    configs
        .iter()
        .map(|config| match config {
            OutputConfig::Prometheus {
                listen,
                remote_write_url,
                interval,
                final_scrape_grace,
            } => {
                if listen.is_none() && remote_write_url.is_none() {
                    return Err(EngineError::Config(
                        "prometheus output requires `listen` and/or `remote_write_url`".into(),
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
                Ok(Box::new(output) as Box<dyn Output>)
            }
        })
        .collect()
}

pub mod proto {
    pub mod prometheus {
        #![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_requires_a_destination() {
        let configs = vec![OutputConfig::Prometheus {
            listen: None,
            remote_write_url: None,
            interval: None,
            final_scrape_grace: None,
        }];
        assert!(build_outputs(&configs, std::path::Path::new(".")).is_err());
    }
}
