//! Raw TCP and UDP protocol handlers (`tcp://host:port`, `udp://host:port`).
//!
//! For sockets `status` is always 0; failures are reported via
//! `ProtocolResponse::error` and byte counts are exact.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{
    PreparedRequest, ProtocolHandler, ProtocolResponse, SocketRequest, Timings,
};
use loadr_core::vu::VuContext;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpStream, UdpSocket};
use url::Url;

use crate::net::{ms_since, resolve};

fn parse_target(raw: &str, scheme: &str) -> Result<Url, ProtocolError> {
    let url = Url::parse(raw)
        .map_err(|e| ProtocolError::InvalidRequest(format!("invalid url `{raw}`: {e}")))?;
    if url.scheme() != scheme {
        return Err(ProtocolError::InvalidRequest(format!(
            "{scheme} handler cannot handle scheme `{}`",
            url.scheme()
        )));
    }
    if url.host_str().is_none() || url.port().is_none() {
        return Err(ProtocolError::InvalidRequest(format!(
            "`{raw}` must be {scheme}://host:port"
        )));
    }
    Ok(url)
}

fn socket_response(
    protocol: &str,
    url: &Url,
    body: Bytes,
    timings: Timings,
    bytes_sent: u64,
    error: Option<String>,
) -> ProtocolResponse {
    ProtocolResponse {
        status: 0,
        status_text: String::new(),
        headers: Vec::new(),
        bytes_received: body.len() as u64,
        body,
        timings,
        bytes_sent,
        protocol_version: protocol.to_string(),
        error,
        url: url.to_string(),
        extras: serde_json::Value::Null,
        grpc_protobuf_outcomes: Vec::new(),
    }
}

enum ReadMode {
    Exact(usize),
    UntilClose,
    Single,
}

impl ReadMode {
    fn from_options(opts: &SocketRequest) -> ReadMode {
        if let Some(n) = opts.read_bytes {
            ReadMode::Exact(n as usize)
        } else if opts.read_until_close {
            ReadMode::UntilClose
        } else {
            ReadMode::Single
        }
    }
}

// ---------------------------------------------------------------------------
// TCP
// ---------------------------------------------------------------------------

/// Raw TCP handler: connect, write payload, read per [`SocketRequest`].
#[derive(Default)]
pub struct TcpHandler;

impl TcpHandler {
    pub fn new() -> Self {
        TcpHandler
    }

    async fn run(
        &self,
        url: &Url,
        opts: &SocketRequest,
        payload: &Bytes,
        read_timeout: Duration,
    ) -> Result<ProtocolResponse, String> {
        let mut timings = Timings::default();
        let host = url
            .host()
            .ok_or_else(|| format!("url `{url}` has no host"))?;
        let port = url
            .port()
            .ok_or_else(|| format!("url `{url}` has no port"))?;
        let addr = resolve(&host, port, &mut timings).await?;

        let connect_start = Instant::now();
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connection to {addr} failed: {e}"))?;
        let _ = stream.set_nodelay(true);
        timings.connect_ms = ms_since(connect_start);
        timings.blocked_ms = timings.dns_ms + timings.connect_ms;

        let send_start = Instant::now();
        stream
            .write_all(payload)
            .await
            .map_err(|e| format!("send failed: {e}"))?;
        timings.sending_ms = ms_since(send_start);

        let (body, error) = read_tcp(
            &mut stream,
            ReadMode::from_options(opts),
            read_timeout,
            &mut timings,
        )
        .await;
        timings.duration_ms = timings.sending_ms + timings.waiting_ms + timings.receiving_ms;
        tracing::debug!(url = %url, sent = payload.len(), received = body.len(), "tcp exchange finished");
        Ok(socket_response(
            "tcp",
            url,
            body,
            timings,
            payload.len() as u64,
            error,
        ))
    }
}

/// Read from `stream` per `mode`. The first read is timed as `waiting_ms`
/// (TTFB), the rest as `receiving_ms`. A timeout produces an error string
/// alongside any partial data.
async fn read_tcp(
    stream: &mut TcpStream,
    mode: ReadMode,
    read_timeout: Duration,
    timings: &mut Timings,
) -> (Bytes, Option<String>) {
    let deadline = tokio::time::Instant::now() + read_timeout;
    let mut received: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut first = true;
    let mut error = None;

    loop {
        let done = match &mode {
            ReadMode::Exact(n) => received.len() >= *n,
            ReadMode::UntilClose => false,
            ReadMode::Single => !received.is_empty(),
        };
        if done {
            break;
        }
        let read_start = Instant::now();
        let result = tokio::time::timeout_at(deadline, stream.read(&mut chunk)).await;
        let elapsed = ms_since(read_start);
        if first {
            timings.waiting_ms = elapsed;
        } else {
            timings.receiving_ms += elapsed;
        }
        match result {
            Err(_) => {
                error = Some(format!("read timed out after {read_timeout:?}"));
                break;
            }
            Ok(Err(e)) => {
                error = Some(format!("read failed: {e}"));
                break;
            }
            Ok(Ok(0)) => break, // EOF
            Ok(Ok(n)) => {
                received.extend_from_slice(&chunk[..n]);
                first = false;
            }
        }
    }
    (Bytes::from(received), error)
}

#[async_trait]
impl ProtocolHandler for TcpHandler {
    fn name(&self) -> &str {
        "tcp"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let url = parse_target(&request.url, "tcp")?;
        let opts = request.options.socket.clone().unwrap_or_default();
        let payload = if opts.payload.is_empty() {
            request.body.clone()
        } else {
            opts.payload.clone()
        };
        let read_timeout = opts.read_timeout.unwrap_or(request.timeout);

        let start = Instant::now();
        match tokio::time::timeout(
            request.timeout,
            self.run(&url, &opts, &payload, read_timeout),
        )
        .await
        {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(message)) => Ok(socket_response(
                "tcp",
                &url,
                Bytes::new(),
                Timings::default(),
                0,
                Some(message),
            )),
            Err(_) => {
                let mut response = crate::http::timeout_response(request, start);
                response.protocol_version = "tcp".to_string();
                Ok(response)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UDP
// ---------------------------------------------------------------------------

/// Raw UDP handler: bind an ephemeral socket, send one datagram, receive
/// one datagram (or loop until `read_bytes`).
#[derive(Default)]
pub struct UdpHandler;

impl UdpHandler {
    pub fn new() -> Self {
        UdpHandler
    }

    async fn run(
        &self,
        url: &Url,
        opts: &SocketRequest,
        payload: &Bytes,
        read_timeout: Duration,
    ) -> Result<ProtocolResponse, String> {
        let mut timings = Timings::default();
        let host = url
            .host()
            .ok_or_else(|| format!("url `{url}` has no host"))?;
        let port = url
            .port()
            .ok_or_else(|| format!("url `{url}` has no port"))?;
        let addr = resolve(&host, port, &mut timings).await?;
        timings.blocked_ms = timings.dns_ms;

        let bind_addr: SocketAddr = if addr.is_ipv4() {
            "0.0.0.0:0".parse().map_err(|e| format!("bind: {e}"))?
        } else {
            "[::]:0".parse().map_err(|e| format!("bind: {e}"))?
        };
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| format!("udp bind failed: {e}"))?;

        let send_start = Instant::now();
        socket
            .send_to(payload, addr)
            .await
            .map_err(|e| format!("send failed: {e}"))?;
        timings.sending_ms = ms_since(send_start);

        let target = opts.read_bytes.map(|n| n as usize);
        let deadline = tokio::time::Instant::now() + read_timeout;
        let mut received: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        let mut first = true;
        let mut error = None;
        loop {
            if let Some(n) = target {
                if received.len() >= n {
                    break;
                }
            } else if !received.is_empty() {
                break; // default: a single datagram
            }
            let read_start = Instant::now();
            let result = tokio::time::timeout_at(deadline, socket.recv(&mut chunk)).await;
            let elapsed = ms_since(read_start);
            if first {
                timings.waiting_ms = elapsed;
            } else {
                timings.receiving_ms += elapsed;
            }
            match result {
                Err(_) => {
                    error = Some(format!("read timed out after {read_timeout:?}"));
                    break;
                }
                Ok(Err(e)) => {
                    error = Some(format!("recv failed: {e}"));
                    break;
                }
                Ok(Ok(n)) => {
                    received.extend_from_slice(&chunk[..n]);
                    first = false;
                }
            }
        }
        timings.duration_ms = timings.sending_ms + timings.waiting_ms + timings.receiving_ms;
        tracing::debug!(url = %url, sent = payload.len(), received = received.len(), "udp exchange finished");
        Ok(socket_response(
            "udp",
            url,
            Bytes::from(received),
            timings,
            payload.len() as u64,
            error,
        ))
    }
}

#[async_trait]
impl ProtocolHandler for UdpHandler {
    fn name(&self) -> &str {
        "udp"
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let url = parse_target(&request.url, "udp")?;
        let opts = request.options.socket.clone().unwrap_or_default();
        let payload = if opts.payload.is_empty() {
            request.body.clone()
        } else {
            opts.payload.clone()
        };
        let read_timeout = opts.read_timeout.unwrap_or(request.timeout);

        let start = Instant::now();
        match tokio::time::timeout(
            request.timeout,
            self.run(&url, &opts, &payload, read_timeout),
        )
        .await
        {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(message)) => Ok(socket_response(
                "udp",
                &url,
                Bytes::new(),
                Timings::default(),
                0,
                Some(message),
            )),
            Err(_) => {
                let mut response = crate::http::timeout_response(request, start);
                response.protocol_version = "udp".to_string();
                Ok(response)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tcp_and_udp_urls() {
        let url = parse_target("tcp://127.0.0.1:9000", "tcp").unwrap();
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(9000));
        assert!(parse_target("udp://localhost:53", "udp").is_ok());
        assert!(parse_target("http://x:1", "tcp").is_err());
        assert!(parse_target("tcp://nohost", "tcp").is_err());
    }

    #[test]
    fn read_mode_selection() {
        let mut opts = SocketRequest::default();
        assert!(matches!(ReadMode::from_options(&opts), ReadMode::Single));
        opts.read_until_close = true;
        assert!(matches!(
            ReadMode::from_options(&opts),
            ReadMode::UntilClose
        ));
        opts.read_bytes = Some(4);
        assert!(matches!(ReadMode::from_options(&opts), ReadMode::Exact(4)));
    }
}
