//! Parsing for `--output kind=value` flags.

use loadr_config::OutputConfig;

/// Parse `json=path`, `csv=path`,
/// `prometheus=listen_addr[,final_scrape_grace=dur]`,
/// `influxdb=url,database`, `statsd=addr`, `otlp=endpoint`.
pub fn parse_output_flag(spec: &str) -> Result<OutputConfig, String> {
    let (kind, value) = spec
        .split_once('=')
        .ok_or_else(|| format!("invalid --output `{spec}`; expected kind=value"))?;
    match kind {
        "json" => Ok(OutputConfig::Json { path: value.into() }),
        "csv" => Ok(OutputConfig::Csv { path: value.into() }),
        "prometheus" => {
            let (listen, options) = match value.split_once(',') {
                Some((addr, rest)) => (addr, rest),
                None => (value, ""),
            };
            let mut final_scrape_grace = None;
            for option in options.split(',').filter(|o| !o.is_empty()) {
                match option.split_once('=') {
                    Some(("final_scrape_grace", dur)) => {
                        final_scrape_grace = Some(
                            loadr_config::Dur::parse(dur)
                                .map_err(|e| format!("prometheus final_scrape_grace: {e}"))?,
                        );
                    }
                    _ => {
                        return Err(format!(
                            "unknown prometheus output option `{option}` \
                             (supported: final_scrape_grace=<duration>)"
                        ))
                    }
                }
            }
            Ok(OutputConfig::Prometheus {
                listen: Some(listen.to_string()),
                remote_write_url: None,
                interval: None,
                final_scrape_grace,
            })
        }
        "influxdb" => {
            let (url, database) = value
                .split_once(',')
                .ok_or_else(|| "influxdb output needs url,database".to_string())?;
            Ok(OutputConfig::Influxdb {
                url: url.to_string(),
                database: database.to_string(),
                token: None,
                organization: None,
                interval: None,
            })
        }
        "statsd" => Ok(OutputConfig::Statsd {
            address: value.to_string(),
            prefix: None,
        }),
        "otlp" => Ok(OutputConfig::Otlp {
            endpoint: value.to_string(),
            protocol: Default::default(),
            headers: Default::default(),
            interval: None,
        }),
        other => Err(format!(
            "unknown output kind `{other}` (json, csv, prometheus, influxdb, statsd, otlp)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kinds() {
        assert!(matches!(
            parse_output_flag("json=a.jsonl").unwrap(),
            OutputConfig::Json { .. }
        ));
        assert!(matches!(
            parse_output_flag("prometheus=127.0.0.1:9091").unwrap(),
            OutputConfig::Prometheus {
                final_scrape_grace: None,
                ..
            }
        ));
        match parse_output_flag("prometheus=127.0.0.1:9091,final_scrape_grace=10s").unwrap() {
            OutputConfig::Prometheus {
                listen,
                final_scrape_grace,
                ..
            } => {
                assert_eq!(listen.as_deref(), Some("127.0.0.1:9091"));
                assert_eq!(
                    final_scrape_grace.map(|d| d.as_duration()),
                    Some(std::time::Duration::from_secs(10))
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(parse_output_flag("prometheus=127.0.0.1:9091,bogus=1").is_err());
        assert!(matches!(
            parse_output_flag("influxdb=http://x:8086,db").unwrap(),
            OutputConfig::Influxdb { .. }
        ));
        assert!(parse_output_flag("nope=1").is_err());
        assert!(parse_output_flag("json").is_err());
    }
}
