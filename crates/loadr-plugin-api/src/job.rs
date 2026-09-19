//! Bridges a job-capable [`ServicePlugin`] to `loadr_core::Job`.

use loadr_core::{Job, JobProgress};

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

    fn start(&mut self) -> Result<(), String> {
        self.service
            .start(&self.config)
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
