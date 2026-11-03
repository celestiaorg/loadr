//! WebSocket handler built on tokio-tungstenite with hand-rolled connection
//! phases (DNS/TCP/TLS timed individually, upgrade handshake as `waiting`).

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use loadr_config::HttpDefaults;
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{
    PreparedRequest, ProtocolHandler, ProtocolResponse, Timings, WsRequest,
};
use loadr_core::vu::VuContext;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

use crate::net::{host_port, ms_since, resolve, IoStream};

/// WebSocket protocol handler.
pub struct WsHandler {
    tls: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
}

impl WsHandler {
    /// Build the handler; the rustls config (no ALPN — WebSocket runs over
    /// HTTP/1.1) is built once. `base_dir` resolves relative TLS file paths.
    pub fn new(defaults: &HttpDefaults, base_dir: &std::path::Path) -> Result<Self, ProtocolError> {
        let tls = crate::tls::client_config(&defaults.tls, base_dir, Vec::new())?;
        Ok(WsHandler {
            tls: Arc::new(tls),
            server_name: defaults.tls.server_name.clone(),
        })
    }
}

#[async_trait]
impl ProtocolHandler for WsHandler {
    fn name(&self) -> &str {
        "ws"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let url = Url::parse(&request.url).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid url `{}`: {e}", request.url))
        })?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(ProtocolError::InvalidRequest(format!(
                "ws handler cannot handle scheme `{}`",
                url.scheme()
            )));
        }
        let ws_opts = request.options.ws.clone().unwrap_or_default();

        let start = Instant::now();
        let deadline = tokio::time::Instant::now() + request.timeout;
        match self.session(request, &url, &ws_opts, deadline).await {
            Ok(response) => Ok(response),
            Err(SessionError::Transport { message, timings }) => Ok(ProtocolResponse {
                status: 0,
                error: Some(message),
                timings,
                url: url.to_string(),
                ..ProtocolResponse::default()
            }),
            Err(SessionError::Timeout { timings }) => {
                let mut timings = timings;
                timings.duration_ms = ms_since(start) - timings.blocked_ms;
                Ok(ProtocolResponse {
                    status: 0,
                    error: Some(format!("request timed out after {:?}", request.timeout)),
                    timings,
                    url: url.to_string(),
                    ..ProtocolResponse::default()
                })
            }
            Err(SessionError::Invalid(e)) => Err(e),
        }
    }
}

enum SessionError {
    Transport { message: String, timings: Timings },
    Timeout { timings: Timings },
    Invalid(ProtocolError),
}

impl WsHandler {
    async fn session(
        &self,
        request: &PreparedRequest,
        url: &Url,
        ws_opts: &WsRequest,
        deadline: tokio::time::Instant,
    ) -> Result<ProtocolResponse, SessionError> {
        let mut timings = Timings::default();
        let transport =
            |message: String, timings: Timings| SessionError::Transport { message, timings };

        // --- Connect: DNS → TCP → (TLS) ---
        let (host, port) = host_port(url).map_err(|e| transport(e, timings))?;
        let url_host = url.host().ok_or_else(|| {
            SessionError::Invalid(ProtocolError::InvalidRequest(format!(
                "url `{url}` has no host"
            )))
        })?;
        let addr = tokio::time::timeout_at(deadline, resolve(&url_host, port, &mut timings))
            .await
            .map_err(|_| SessionError::Timeout { timings })?
            .map_err(|e| transport(e, timings))?;

        let connect_start = Instant::now();
        let tcp = tokio::time::timeout_at(deadline, TcpStream::connect(addr))
            .await
            .map_err(|_| SessionError::Timeout { timings })?
            .map_err(|e| transport(format!("connection to {addr} failed: {e}"), timings))?;
        let _ = tcp.set_nodelay(true);
        timings.connect_ms = ms_since(connect_start);

        let stream: Box<dyn IoStream> = if url.scheme() == "wss" {
            let tls_start = Instant::now();
            let connector = tokio_rustls::TlsConnector::from(self.tls.clone());
            let sni = crate::tls::server_name(self.server_name.as_deref(), url)
                .map_err(SessionError::Invalid)?;
            let tls = tokio::time::timeout_at(deadline, connector.connect(sni, tcp))
                .await
                .map_err(|_| SessionError::Timeout { timings })?
                .map_err(|e| {
                    transport(
                        format!("tls handshake with {host}:{port} failed: {e}"),
                        timings,
                    )
                })?;
            timings.tls_ms = ms_since(tls_start);
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        timings.blocked_ms = timings.dns_ms + timings.connect_ms + timings.tls_ms;

        // --- Upgrade handshake (counted as waiting) ---
        let mut hs_request = url.as_str().into_client_request().map_err(|e| {
            SessionError::Invalid(ProtocolError::InvalidRequest(format!(
                "invalid websocket url `{url}`: {e}"
            )))
        })?;
        if !ws_opts.subprotocols.is_empty() {
            let value = ws_opts.subprotocols.join(", ");
            let value = http::HeaderValue::from_str(&value).map_err(|e| {
                SessionError::Invalid(ProtocolError::InvalidRequest(format!(
                    "invalid subprotocols: {e}"
                )))
            })?;
            hs_request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", value);
        }
        for (name, value) in &request.headers {
            let name: http::HeaderName = name.parse().map_err(|e| {
                SessionError::Invalid(ProtocolError::InvalidRequest(format!(
                    "invalid header name `{name}`: {e}"
                )))
            })?;
            let value = http::HeaderValue::from_str(value).map_err(|e| {
                SessionError::Invalid(ProtocolError::InvalidRequest(format!(
                    "invalid value for header `{name}`: {e}"
                )))
            })?;
            hs_request.headers_mut().insert(name, value);
        }

        let hs_start = Instant::now();
        let (mut ws, hs_response) = tokio::time::timeout_at(
            deadline,
            tokio_tungstenite::client_async(hs_request, stream),
        )
        .await
        .map_err(|_| SessionError::Timeout { timings })?
        .map_err(|e| transport(format!("websocket handshake failed: {e}"), timings))?;
        timings.waiting_ms = ms_since(hs_start);

        // --- Send frames ---
        let mut msgs_sent: u64 = 0;
        let mut bytes_sent: u64 = 0;
        let mut sending_ms = 0.0;
        for frame in &ws_opts.send {
            if let Some(delay) = frame.delay {
                let sleep_until = tokio::time::Instant::now() + delay;
                tokio::time::sleep_until(sleep_until.min(deadline)).await;
                if tokio::time::Instant::now() >= deadline {
                    timings.sending_ms = sending_ms;
                    return Err(SessionError::Timeout { timings });
                }
            }
            let message = if frame.binary {
                Message::Binary(frame.payload.clone())
            } else {
                Message::Text(String::from_utf8_lossy(&frame.payload).into_owned().into())
            };
            let send_start = Instant::now();
            let sent = tokio::time::timeout_at(deadline, ws.send(message)).await;
            sending_ms += ms_since(send_start);
            match sent {
                Ok(Ok(())) => {
                    msgs_sent += 1;
                    bytes_sent += frame.payload.len() as u64;
                }
                Ok(Err(e)) => {
                    timings.sending_ms = sending_ms;
                    return Err(transport(format!("websocket send failed: {e}"), timings));
                }
                Err(_) => {
                    timings.sending_ms = sending_ms;
                    return Err(SessionError::Timeout { timings });
                }
            }
        }
        timings.sending_ms = sending_ms;

        // --- Receive until count / substring / session duration / timeout ---
        let target = match (&ws_opts.receive_count, &ws_opts.receive_until) {
            (Some(n), _) => *n,
            (None, Some(_)) => u64::MAX,
            (None, None) => msgs_sent.max(1),
        };
        let session_deadline = ws_opts
            .session_duration
            .map(|d| tokio::time::Instant::now() + d);
        let recv_deadline = session_deadline.map_or(deadline, |s| s.min(deadline));

        let mut msgs_received: u64 = 0;
        let mut bytes_received: u64 = 0;
        let mut last_message: Option<Message> = None;
        let mut error: Option<String> = None;
        let recv_start = Instant::now();
        while msgs_received < target {
            match tokio::time::timeout_at(recv_deadline, ws.next()).await {
                Err(_) => {
                    // Session deadline elapsing is a normal close; the overall
                    // request timeout is a failure.
                    if session_deadline.is_some_and(|s| s <= deadline) {
                        break;
                    }
                    error = Some(format!("request timed out after {:?}", request.timeout));
                    break;
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    error = Some(format!("websocket receive failed: {e}"));
                    break;
                }
                Ok(Some(Ok(message))) => match message {
                    Message::Text(text) => {
                        msgs_received += 1;
                        bytes_received += text.len() as u64;
                        let matched = ws_opts
                            .receive_until
                            .as_deref()
                            .is_some_and(|needle| text.as_str().contains(needle));
                        last_message = Some(Message::Text(text));
                        if matched {
                            break;
                        }
                    }
                    Message::Binary(data) => {
                        msgs_received += 1;
                        bytes_received += data.len() as u64;
                        last_message = Some(Message::Binary(data));
                    }
                    Message::Close(_) => break,
                    // Ping/Pong/Frame are control noise; tungstenite answers pings.
                    _ => {}
                },
            }
        }
        timings.receiving_ms = ms_since(recv_start);
        timings.duration_ms = timings.sending_ms + timings.waiting_ms + timings.receiving_ms;

        let _ = ws.close(None).await;
        tracing::debug!(
            url = %url,
            msgs_sent,
            msgs_received,
            duration_ms = timings.duration_ms,
            "websocket session finished"
        );

        let (body, last_text): (Bytes, Option<String>) = match &last_message {
            Some(Message::Text(t)) => (
                Bytes::copy_from_slice(t.as_str().as_bytes()),
                Some(t.as_str().to_string()),
            ),
            Some(Message::Binary(b)) => (b.clone(), None),
            _ => (Bytes::new(), None),
        };
        let headers: Vec<(String, String)> = hs_response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();

        Ok(ProtocolResponse {
            status: 101,
            status_text: "Switching Protocols".to_string(),
            headers,
            body,
            timings,
            bytes_sent,
            bytes_received,
            protocol_version: "ws".to_string(),
            error,
            url: url.to_string(),
            extras: serde_json::json!({
                "msgs_sent": msgs_sent,
                "msgs_received": msgs_received,
                "last_message": last_text,
            }),
            grpc_protobuf_outcomes: Vec::new(),
        })
    }
}
