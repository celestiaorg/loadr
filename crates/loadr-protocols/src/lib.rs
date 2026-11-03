//! # loadr-protocols
//!
//! Built-in protocol handlers for [loadr](https://loadr.io):
//!
//! - [`HttpHandler`] — HTTP/1.1 + HTTP/2 on hyper's low-level connection API
//!   with per-phase timings (DNS, connect, TLS, send, TTFB, receive), per-VU
//!   keep-alive pooling, compression, redirects, cookies and proxy support.
//! - [`GraphqlHandler`] — GraphQL-over-HTTP with `errors` post-processing.
//! - [`WsHandler`] — WebSocket sessions (send/receive scripts).
//! - [`SseHandler`] — Server-Sent Events streams (`sse://`/`sses://`).
//! - [`GrpcHandler`] — dynamic gRPC from `.proto` files (compiled in-process)
//!   or server reflection; all four call shapes.
//! - [`TcpHandler`] / [`UdpHandler`] — raw socket round trips.
//! - [`NoopHandler`] — zero-I/O success responses for engine self-testing.
//!
//! Redis, PostgreSQL and MySQL are no longer built in: each is its own
//! runtime-loadable native protocol plugin (`loadr plugin install redis` /
//! `postgres` / `mysql`) so heavy or transport-specific drivers stay out of the
//! core binary. The MySQL plugin alone carries `sqlx-mysql`'s transitive `rsa`
//! dependency; the PostgreSQL and Redis plugins are advisory-clean.
//!
//! Use [`builtin_registry`] to build a [`ProtocolRegistry`] with everything
//! registered under its YAML name plus scheme aliases.

mod graphql;
mod grpc;
mod grpc_transport;
mod http;
mod net;
mod noop;
mod socket;
mod sse;
mod tls;
mod ws;

use std::sync::Arc;

pub use graphql::GraphqlHandler;
pub use grpc::GrpcHandler;
pub use http::{HttpHandler, DEFAULT_USER_AGENT};
pub use noop::NoopHandler;
pub use socket::{TcpHandler, UdpHandler};
pub use sse::SseHandler;
pub use ws::WsHandler;

use loadr_core::{ProtocolError, ProtocolRegistry};

/// Build the registry of built-in protocol handlers.
///
/// Registers `http` (alias `https`), `graphql` (sharing the HTTP handler's
/// transport), `ws` (alias `websocket`), `sse` (alias `sses`), `grpc`, `tcp`
/// `udp` and `noop`. TLS client configuration is built once, here, from
/// `http_defaults.tls`; `base_dir` resolves relative TLS/proto file paths.
pub fn builtin_registry(
    http_defaults: &loadr_config::HttpDefaults,
    base_dir: &std::path::Path,
) -> Result<ProtocolRegistry, ProtocolError> {
    let mut registry = ProtocolRegistry::new();

    let http = Arc::new(HttpHandler::new(http_defaults, base_dir)?);
    registry.register(http.clone());
    registry.register_alias("https", "http");

    registry.register(Arc::new(GraphqlHandler::new(http)));

    registry.register(Arc::new(WsHandler::new(http_defaults, base_dir)?));
    registry.register_alias("websocket", "ws");

    registry.register(Arc::new(SseHandler::new(http_defaults, base_dir)?));
    registry.register_alias("sses", "sse");

    registry.register(Arc::new(GrpcHandler::new(http_defaults, base_dir)?));

    registry.register(Arc::new(TcpHandler::new()));
    registry.register(Arc::new(UdpHandler::new()));
    registry.register(Arc::new(NoopHandler::new()));

    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_all_builtins_and_aliases() {
        let defaults = loadr_config::HttpDefaults::default();
        let registry = builtin_registry(&defaults, std::path::Path::new(".")).expect("registry");
        for name in [
            "http",
            "https",
            "graphql",
            "ws",
            "websocket",
            "sse",
            "sses",
            "grpc",
            "tcp",
            "udp",
            "noop",
        ] {
            assert!(registry.get(name).is_some(), "missing handler `{name}`");
        }
        assert_eq!(
            registry.get("https").map(|h| h.name().to_string()),
            Some("http".into())
        );
        assert_eq!(
            registry.get("websocket").map(|h| h.name().to_string()),
            Some("ws".into())
        );
        assert_eq!(
            registry.get("sses").map(|h| h.name().to_string()),
            Some("sse".into())
        );
    }

    #[test]
    fn invalid_proxy_url_is_rejected() {
        let defaults = loadr_config::HttpDefaults {
            proxy: Some("::not a url::".to_string()),
            ..Default::default()
        };
        assert!(builtin_registry(&defaults, std::path::Path::new(".")).is_err());
    }
}
