//! The axum web server: auth middleware, REST routes, SSE streams and the
//! embedded single-page app.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::UiBackend;

/// Authentication for the API: HTTP Basic and/or bearer tokens.
/// Empty (the default) means the UI is open.
#[derive(Clone, Default)]
pub struct AuthConfig {
    /// Username and password for `Authorization: Basic ...`.
    pub basic: Option<(String, String)>,
    /// Accepted bearer tokens (`Authorization: Bearer ...` or `?token=`).
    pub tokens: Vec<String>,
}

/// Configuration for [`WebUi::serve`].
pub struct WebUiConfig {
    /// Address to bind; use port 0 for an ephemeral port. Default `127.0.0.1:6464`.
    pub bind: SocketAddr,
    pub auth: AuthConfig,
    pub backend: Arc<dyn UiBackend>,
}

impl WebUiConfig {
    /// The default bind address: localhost only, port 6464.
    pub fn default_bind() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 6464))
    }
}

/// Errors starting the web UI.
#[derive(Debug, thiserror::Error)]
pub enum WebUiError {
    #[error("cannot bind web UI to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("web UI listener error: {0}")]
    Listener(std::io::Error),
}

/// A running web UI server.
pub struct WebUiHandle {
    /// The actual bound address (useful with port 0).
    pub addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl WebUiHandle {
    /// Gracefully shut the server down (aborts after a short grace period).
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if tokio::time::timeout(Duration::from_secs(3), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

/// The web UI server.
pub struct WebUi;

impl WebUi {
    /// Bind and serve the management UI; returns once the listener is ready.
    pub async fn serve(config: WebUiConfig) -> Result<WebUiHandle, WebUiError> {
        let listener = tokio::net::TcpListener::bind(config.bind)
            .await
            .map_err(|source| WebUiError::Bind {
                addr: config.bind,
                source,
            })?;
        let addr = listener.local_addr().map_err(WebUiError::Listener)?;

        let state = Arc::new(AppState {
            backend: config.backend,
            auth: AuthChecker::new(&config.auth),
        });
        let app = router(state);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.changed().await;
            });
            if let Err(e) = serve.await {
                tracing::error!(error = %e, "web UI server error");
            }
        });
        tracing::info!(%addr, "loadr web UI listening");

        Ok(WebUiHandle {
            addr,
            shutdown_tx,
            task,
        })
    }
}

/// Shared state for all handlers.
pub(crate) struct AppState {
    pub(crate) backend: Arc<dyn UiBackend>,
    pub(crate) auth: AuthChecker,
}

fn router(state: Arc<AppState>) -> Router {
    let api = Router::new()
        .route("/capabilities", get(crate::api::capabilities))
        .route("/overview", get(crate::api::overview))
        .route(
            "/runs",
            get(crate::api::list_runs).post(crate::api::start_run),
        )
        .route("/runs/{id}", get(crate::api::run_detail))
        .route("/runs/{id}/snapshot", get(crate::api::run_snapshot))
        .route("/runs/{id}/summary", get(crate::api::run_summary))
        .route("/runs/{id}/stop", post(crate::api::stop_run))
        .route("/runs/{id}/pause", post(crate::api::pause_run))
        .route("/runs/{id}/scale", post(crate::api::scale_run))
        .route("/runs/{id}/stream", get(crate::stream::run_stream))
        .route("/stream", get(crate::stream::overview_stream))
        .route("/agents", get(crate::api::agents))
        .route("/tests", get(crate::api::list_tests))
        .route(
            "/tests/{name}",
            put(crate::api::put_test).delete(crate::api::delete_test),
        )
        .route("/validate", post(crate::api::validate))
        .route("/logs", get(crate::api::logs))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/version", get(crate::api::version))
        .nest("/api", api)
        .fallback(get(static_handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Credential checker comparing SHA-256 digests in constant time.
pub(crate) struct AuthChecker {
    basic: Option<[u8; 32]>,
    tokens: Vec<[u8; 32]>,
}

impl AuthChecker {
    fn new(cfg: &AuthConfig) -> Self {
        AuthChecker {
            basic: cfg
                .basic
                .as_ref()
                .map(|(user, pass)| sha256(format!("{user}:{pass}").as_bytes())),
            tokens: cfg.tokens.iter().map(|t| sha256(t.as_bytes())).collect(),
        }
    }

    fn enabled(&self) -> bool {
        self.basic.is_some() || !self.tokens.is_empty()
    }

    fn check_basic(&self, b64: &str) -> bool {
        let Some(expected) = &self.basic else {
            return false;
        };
        use base64::Engine as _;
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
            return false;
        };
        ct_eq(&sha256(&decoded), expected)
    }

    fn check_token(&self, token: &str) -> bool {
        let digest = sha256(token.as_bytes());
        // Check every token so timing does not leak which one matched.
        let mut ok = false;
        for t in &self.tokens {
            if ct_eq(&digest, t) {
                ok = true;
            }
        }
        ok
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

pub(crate) async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    if !state.auth.enabled() {
        return next.run(req).await;
    }
    if let Some(value) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(rest) = value.strip_prefix("Basic ") {
            if state.auth.check_basic(rest) {
                return next.run(req).await;
            }
        }
        if let Some(rest) = value.strip_prefix("Bearer ") {
            if state.auth.check_token(rest.trim()) {
                return next.run(req).await;
            }
        }
    }
    // `?token=` for EventSource/WebSocket clients that cannot set headers.
    if let Some(token) = req.uri().query().and_then(|q| query_param(q, "token")) {
        if state.auth.check_token(&token) {
            return next.run(req).await;
        }
    }
    let challenge = if state.auth.basic.is_some() {
        r#"Basic realm="loadr", charset="UTF-8""#
    } else {
        "Bearer"
    };
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, challenge)],
        Json(serde_json::json!({ "error": "unauthorized" })),
    )
        .into_response()
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (k, v) = match pair.split_once('=') {
            Some(kv) => kv,
            None => (pair, ""),
        };
        if k == key {
            return Some(percent_decode(v));
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).and_then(|h| {
                    std::str::from_utf8(h)
                        .ok()
                        .and_then(|h| u8::from_str_radix(h, 16).ok())
                });
                match hex {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// Embedded SPA
// ---------------------------------------------------------------------------

#[derive(rust_embed::RustEmbed)]
#[folder = "ui/"]
struct Assets;

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    match Assets::get(path) {
        Some(file) => asset_response(path, file),
        // SPA fallback: unknown non-asset paths get the shell.
        None => match Assets::get("index.html") {
            Some(file) => asset_response("index.html", file),
            None => (StatusCode::NOT_FOUND, "not found").into_response(),
        },
    }
}

fn asset_response(path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let cache = if path == "index.html" {
        "no-cache"
    } else {
        "max-age=3600"
    };
    (
        [
            (header::CONTENT_TYPE, content_type(path)),
            (header::CACHE_CONTROL, cache),
        ],
        file.data.into_owned(),
    )
        .into_response()
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_checker_accepts_valid_credentials() {
        let checker = AuthChecker::new(&AuthConfig {
            basic: Some(("admin".to_string(), "s3cret".to_string())),
            tokens: vec!["tok-1".to_string(), "tok-2".to_string()],
        });
        assert!(checker.enabled());
        use base64::Engine as _;
        let good = base64::engine::general_purpose::STANDARD.encode("admin:s3cret");
        let bad = base64::engine::general_purpose::STANDARD.encode("admin:nope");
        assert!(checker.check_basic(&good));
        assert!(!checker.check_basic(&bad));
        assert!(checker.check_token("tok-2"));
        assert!(!checker.check_token("tok-3"));
    }

    #[test]
    fn query_param_decodes() {
        assert_eq!(
            query_param("a=1&token=ab%20c+d", "token").as_deref(),
            Some("ab c d")
        );
        assert_eq!(query_param("a=1", "token"), None);
    }

    #[test]
    fn content_types() {
        assert!(content_type("index.html").contains("text/html"));
        assert!(content_type("app.js").contains("javascript"));
        assert!(content_type("style.css").contains("text/css"));
    }
}
