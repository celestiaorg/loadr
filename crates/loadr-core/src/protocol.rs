//! The protocol abstraction: handlers execute prepared requests for VUs and
//! return responses with detailed phase timings.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::ProtocolError;
use crate::vu::VuContext;

/// Phase timings for one request, all in milliseconds.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct Timings {
    /// DNS resolution.
    pub dns_ms: f64,
    /// TCP connect.
    pub connect_ms: f64,
    /// TLS handshake.
    pub tls_ms: f64,
    /// Writing the request.
    pub sending_ms: f64,
    /// First byte wait (TTFB) after the request was sent.
    pub waiting_ms: f64,
    /// Reading the response body.
    pub receiving_ms: f64,
    /// sending + waiting + receiving (excludes connection setup).
    pub duration_ms: f64,
    /// Time spent acquiring a connection (dns + connect + tls when not reused).
    pub blocked_ms: f64,
}

/// A fully rendered request, ready for a protocol handler.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    /// Metric name tag for this request.
    pub name: String,
    /// Resolved protocol: `http`, `ws`, `grpc`, `graphql`, `tcp`, `udp`, or a plugin name.
    pub protocol: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub timeout: Duration,
    pub follow_redirects: bool,
    pub max_redirects: u32,
    /// Protocol-specific options, already template-rendered.
    pub options: RequestOptions,
}

/// Protocol-specific options.
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    pub ws: Option<WsRequest>,
    pub grpc: Option<GrpcRequest>,
    pub socket: Option<SocketRequest>,
    /// Free-form options for plugin protocols.
    pub plugin: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
pub struct WsRequest {
    pub subprotocols: Vec<String>,
    pub send: Vec<WsFrame>,
    pub receive_count: Option<u64>,
    pub receive_until: Option<String>,
    pub session_duration: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct WsFrame {
    pub payload: Bytes,
    pub binary: bool,
    pub delay: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct GrpcRequest {
    pub proto_files: Vec<std::path::PathBuf>,
    pub proto_includes: Vec<std::path::PathBuf>,
    pub reflection: bool,
    pub service: String,
    pub method: String,
    /// Unary request message (JSON-encoded).
    pub message: Option<Arc<serde_json::Value>>,
    /// Streaming request messages.
    pub messages: Arc<Vec<serde_json::Value>>,
    /// True when the message JSON contains no template substitutions: the
    /// `Arc`(s) above are then the compile-time values, handed out unchanged
    /// every iteration (stable identity), so the handler may cache the
    /// encoded body per `Arc`.
    pub message_literal: bool,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct SocketRequest {
    pub payload: Bytes,
    pub read_bytes: Option<u64>,
    pub read_until_close: bool,
    pub read_timeout: Option<Duration>,
}

/// The response from a protocol handler.
#[derive(Debug, Clone, Default)]
pub struct ProtocolResponse {
    /// HTTP status, gRPC status code, or 0 when not applicable.
    pub status: i64,
    pub status_text: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub timings: Timings,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// e.g. `HTTP/1.1`, `HTTP/2`, `grpc`, `ws`.
    pub protocol_version: String,
    /// Transport-level failure, if any (the request still produced samples).
    pub error: Option<String>,
    /// Final URL after redirects.
    pub url: String,
    /// Protocol-specific extras (ws message counts, grpc messages, ...).
    pub extras: serde_json::Value,
}

impl ProtocolResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn body_text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Failed = transport error or status >= 400 (HTTP) / non-zero (gRPC).
    pub fn failed(&self) -> bool {
        if self.error.is_some() {
            return true;
        }
        match self.protocol_version.as_str() {
            v if v.starts_with("HTTP") => self.status >= 400,
            "grpc" => self.status != 0,
            _ => false,
        }
    }
}

/// A protocol implementation (built-in or plugin-provided).
#[async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Protocol name as used in YAML (`http`, `ws`, ...).
    fn name(&self) -> &str;

    /// Execute one request for a VU. Transport failures should be reported via
    /// `ProtocolResponse::error` where possible (so timings/bytes still count);
    /// `Err` is for situations where no meaningful response exists.
    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError>;
}

/// Process-global map of URL scheme -> protocol-name, populated by the host
/// when it loads protocol *plugins* that declare the scheme(s) they serve
/// (via `plugin.toml`'s `[plugin].schemes` or the plugin's `info().schemes`).
///
/// Built-in scheme inference always wins; this map only extends `infer` so a
/// runtime-loaded protocol plugin — which cannot edit core — can claim a URL
/// scheme like `mongodb://`. The map is keyed by lowercase scheme and maps to
/// the registered protocol handler name (the plugin's `name()`), which the
/// `ProtocolRegistry` then resolves to a handler.
fn plugin_scheme_aliases() -> &'static RwLock<HashMap<String, String>> {
    static ALIASES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    ALIASES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register URL scheme(s) that route to a loaded protocol plugin named
/// `protocol`. Idempotent; later registrations for the same scheme win.
///
/// Built-in schemes are never overridden by this map (see [`ProtocolRegistry::infer`]).
pub fn register_plugin_schemes(protocol: &str, schemes: &[String]) {
    if schemes.is_empty() {
        return;
    }
    let mut map = plugin_scheme_aliases().write().expect("scheme alias lock");
    for scheme in schemes {
        map.insert(scheme.to_ascii_lowercase(), protocol.to_string());
    }
}

/// Look up a plugin-declared scheme alias. Returns the protocol-handler name.
pub fn plugin_scheme(scheme: &str) -> Option<String> {
    plugin_scheme_aliases()
        .read()
        .expect("scheme alias lock")
        .get(&scheme.to_ascii_lowercase())
        .cloned()
}

/// Clear all registered plugin scheme aliases (test support).
#[doc(hidden)]
pub fn clear_plugin_schemes() {
    plugin_scheme_aliases()
        .write()
        .expect("scheme alias lock")
        .clear();
}

/// Registry of protocol handlers, keyed by name with scheme aliases.
#[derive(Default, Clone)]
pub struct ProtocolRegistry {
    handlers: HashMap<String, Arc<dyn ProtocolHandler>>,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, handler: Arc<dyn ProtocolHandler>) {
        self.handlers.insert(handler.name().to_string(), handler);
    }

    pub fn register_alias(&mut self, alias: &str, target: &str) {
        if let Some(h) = self.handlers.get(target).cloned() {
            self.handlers.insert(alias.to_string(), h);
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ProtocolHandler>> {
        self.handlers.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.handlers.keys().cloned().collect();
        v.sort();
        v
    }

    /// Infer the protocol from an explicit setting or the URL scheme.
    ///
    /// Built-in schemes are matched first; an unknown scheme is then looked up
    /// against plugin-declared scheme aliases ([`register_plugin_schemes`])
    /// before falling back to `http`. An explicit `protocol:` that is neither a
    /// built-in alias nor a plugin scheme is passed through verbatim (so it can
    /// match a loaded plugin handler by name).
    pub fn infer(explicit: Option<&str>, url: &str) -> String {
        if let Some(p) = explicit {
            return match p {
                "https" => "http".to_string(),
                "websocket" | "wss" => "ws".to_string(),
                "sses" => "sse".to_string(),
                other => plugin_scheme(other).unwrap_or_else(|| other.to_string()),
            };
        }
        let scheme = url.split("://").next().unwrap_or("");
        match scheme {
            "http" | "https" => "http".to_string(),
            "ws" | "wss" => "ws".to_string(),
            "grpc" | "grpcs" => "grpc".to_string(),
            "sse" | "sses" => "sse".to_string(),
            "tcp" => "tcp".to_string(),
            "udp" => "udp".to_string(),
            other => plugin_scheme(other).unwrap_or_else(|| "http".to_string()),
        }
    }
}

impl std::fmt::Debug for ProtocolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolRegistry")
            .field("handlers", &self.names())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_protocols() {
        assert_eq!(ProtocolRegistry::infer(None, "https://x/"), "http");
        assert_eq!(ProtocolRegistry::infer(None, "wss://x/"), "ws");
        assert_eq!(ProtocolRegistry::infer(None, "grpc://x/"), "grpc");
        assert_eq!(ProtocolRegistry::infer(None, "tcp://x:9"), "tcp");
        assert_eq!(ProtocolRegistry::infer(None, "sse://x/"), "sse");
        assert_eq!(ProtocolRegistry::infer(None, "sses://x/"), "sse");
        // Redis is a plugin now: its scheme falls back to http until the plugin
        // claims `redis`/`rediss` via register_plugin_schemes.
        assert_eq!(ProtocolRegistry::infer(None, "redis://x:6379"), "http");
        assert_eq!(ProtocolRegistry::infer(None, "/relative"), "http");
        assert_eq!(
            ProtocolRegistry::infer(Some("graphql"), "https://x/"),
            "graphql"
        );
        assert_eq!(
            ProtocolRegistry::infer(Some("websocket"), "https://x/"),
            "ws"
        );
        assert_eq!(ProtocolRegistry::infer(Some("sses"), "sses://x/"), "sse");
        assert_eq!(
            ProtocolRegistry::infer(Some("browser"), "https://x/"),
            "browser"
        );
    }

    #[test]
    fn plugin_scheme_routing() {
        clear_plugin_schemes();
        // Unknown scheme falls back to http until a plugin claims it.
        assert_eq!(ProtocolRegistry::infer(None, "mongodb://h/db"), "http");
        register_plugin_schemes("mongo", &["mongodb".to_string(), "mongo".to_string()]);
        // Now the URL scheme routes to the plugin handler name.
        assert_eq!(ProtocolRegistry::infer(None, "mongodb://h/db"), "mongo");
        assert_eq!(ProtocolRegistry::infer(None, "MONGO://h/db"), "mongo");
        // Explicit `protocol:` matching a plugin scheme also resolves.
        assert_eq!(ProtocolRegistry::infer(Some("mongodb"), "x"), "mongo");
        // Built-in schemes always win over plugin aliases.
        register_plugin_schemes("rogue", &["https".to_string()]);
        assert_eq!(ProtocolRegistry::infer(None, "https://h/"), "http");
        clear_plugin_schemes();
    }

    #[test]
    fn response_failed_semantics() {
        let mut r = ProtocolResponse {
            status: 200,
            protocol_version: "HTTP/1.1".into(),
            ..Default::default()
        };
        assert!(!r.failed());
        r.status = 500;
        assert!(r.failed());
        r.status = 200;
        r.error = Some("boom".into());
        assert!(r.failed());
        let g = ProtocolResponse {
            status: 0,
            protocol_version: "grpc".into(),
            ..Default::default()
        };
        assert!(!g.failed());
    }
}
