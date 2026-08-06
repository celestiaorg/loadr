//! Dynamic gRPC load generation with unary, client-streaming,
//! server-streaming, and bidirectional-streaming call shapes.

mod grpc;
mod grpc_transport;
mod net;
mod tls;

use std::sync::Arc;

pub use grpc::GrpcHandler;

use loadr_core::{ProtocolError, ProtocolRegistry};

/// Build the gRPC-only protocol registry.
pub fn builtin_registry(
    defaults: &loadr_config::HttpDefaults,
    base_dir: &std::path::Path,
) -> Result<ProtocolRegistry, ProtocolError> {
    let mut registry = ProtocolRegistry::new();
    registry.register(Arc::new(GrpcHandler::new(defaults, base_dir)?));
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_only_grpc() {
        let registry = builtin_registry(
            &loadr_config::HttpDefaults::default(),
            std::path::Path::new("."),
        )
        .expect("registry");
        assert!(registry.get("grpc").is_some());
        assert!(registry.get("http").is_none());
    }
}
