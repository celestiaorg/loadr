//! Bridges a job-capable [`ServicePlugin`] to `loadr_core::Job`.

use loadr_core::{Job, JobPlacement, JobProgress};

use crate::traits::ServicePlugin;

/// A service plugin that reported `is_job`, bound to the config its `start`
/// receives (manifest `[config]` merged with the plan's `plugins:` entry).
pub struct ServiceJob {
    service: Box<dyn ServicePlugin>,
    config: serde_json::Value,
}

impl ServiceJob {
    pub fn new(service: Box<dyn ServicePlugin>, config: serde_json::Value) -> Self {
        ServiceJob { service, config }
    }
}

impl Job for ServiceJob {
    fn name(&self) -> &str {
        self.service.name()
    }

    fn start(&mut self, placement: JobPlacement) -> Result<(), String> {
        let config = start_config(&self.config, placement)?;
        self.service
            .start(&config)
            .map(drop)
            .map_err(|e| e.to_string())
    }

    fn progress(&mut self) -> Result<JobProgress, String> {
        parse_progress(&self.service.progress())
    }

    fn stop(&mut self) {
        self.service.stop();
    }
}

/// The plugin's config plus where this instance sits in the run: every agent
/// runs the job, and `agent_index`/`agent_count` let it pick its own share.
fn start_config(
    config: &serde_json::Value,
    placement: JobPlacement,
) -> Result<serde_json::Value, String> {
    let mut object = match config {
        serde_json::Value::Object(map) => map.clone(),
        serde_json::Value::Null => serde_json::Map::new(),
        other => return Err(format!("job config must be an object, got {other}")),
    };
    object.insert("agent_index".to_string(), placement.agent_index.into());
    object.insert("agent_count".to_string(), placement.agent_count.into());
    Ok(serde_json::Value::Object(object))
}

fn parse_progress(json: &str) -> Result<JobProgress, String> {
    if json.trim().is_empty() {
        return Err("the plugin reports itself as a job but returned no progress".to_string());
    }
    serde_json::from_str(json).map_err(|e| format!("invalid progress JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_core::ReportedState;

    #[test]
    fn start_config_carries_the_placement() {
        let placement = JobPlacement {
            agent_index: 1,
            agent_count: 4,
        };
        let config = start_config(&serde_json::json!({"rows": 10}), placement).unwrap();
        assert_eq!(
            config,
            serde_json::json!({"rows": 10, "agent_index": 1, "agent_count": 4})
        );
        let config = start_config(&serde_json::Value::Null, JobPlacement::default()).unwrap();
        assert_eq!(
            config,
            serde_json::json!({"agent_index": 0, "agent_count": 1})
        );
        assert!(start_config(&serde_json::json!([1]), placement).is_err());
    }

    #[test]
    fn empty_progress_is_an_error() {
        assert!(parse_progress("").is_err());
        assert!(parse_progress("{\"state\":\"nope\"}").is_err());
    }

    #[test]
    fn progress_is_parsed() {
        let p = parse_progress(r#"{"state":"finished","done":3,"total":3}"#).unwrap();
        assert_eq!(p.state, ReportedState::Finished);
        assert_eq!(p.total, Some(3.0));
    }
}
