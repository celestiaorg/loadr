//! Zero-I/O protocol handler for measuring loadr's own hot-path throughput.

use async_trait::async_trait;
use loadr_core::{PreparedRequest, ProtocolError, ProtocolHandler, ProtocolResponse, VuContext};

/// Accepts a prepared request and immediately reports success.
#[derive(Debug, Default)]
pub struct NoopHandler;

impl NoopHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolHandler for NoopHandler {
    fn name(&self) -> &str {
        "noop"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        Ok(ProtocolResponse {
            status: 200,
            status_text: "OK".to_string(),
            bytes_sent: request.body.len() as u64,
            protocol_version: "noop".to_string(),
            url: request.url.clone(),
            ..Default::default()
        })
    }
}
