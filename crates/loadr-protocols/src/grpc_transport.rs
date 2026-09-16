//! Raw hyper HTTP/2 transport for gRPC (`transport: raw`).
//!
//! Drives `hyper::client::conn::http2::SendRequest` directly from the VU task
//! — the same pattern the HTTP handler uses — instead of going through
//! `tonic::transport::Channel`, which is a tower::buffer: a bounded mpsc queue
//! plus a dedicated worker task per channel, costing extra cross-task wakeups
//! and `Box<dyn Service>` dispatch on every request. [`RawChannel`] implements
//! `tower::Service` so it slots into `tonic::client::Grpc::with_origin`; the
//! codec, call shapes and error mapping in `grpc.rs` are transport-agnostic.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use loadr_core::error::ProtocolError;
use loadr_core::protocol::Timings;
use rustls::pki_types::ServerName;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tonic::codegen::Service;
use tonic::Status;

use crate::net::IoStream;

/// After a failed dial, fail fast for this long instead of dialing per call.
const DIAL_COOLDOWN: Duration = Duration::from_millis(500);

/// Max concurrent streams per raw connection
/// (`LOADR_GRPC_MAX_STREAMS_PER_CONN`). hyper's h2 dispatch queue is
/// unbounded, so without this gate a slow server grows pending streams
/// without limit.
fn max_streams_per_conn() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("LOADR_GRPC_MAX_STREAMS_PER_CONN")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(512)
            .max(1)
    })
}

/// TLS material for `grpcs://` dials: rustls config (ALPN `h2`) plus the
/// resolved SNI name. Built once per endpoint by the handler from
/// `crate::tls::client_config`, so unlike the tonic path it honors
/// `insecure_skip_verify` and `min_version`/`max_version`.
pub(crate) struct TlsParams {
    pub(crate) config: Arc<rustls::ClientConfig>,
    pub(crate) server_name: ServerName<'static>,
}

pin_project_lite::pin_project! {
    /// Response body that keeps the in-flight permit until it is fully read
    /// or dropped (including request-timeout cancellation, which resets the
    /// h2 stream), so the semaphore tracks live streams rather than live
    /// calls.
    pub(crate) struct PermitBody {
        #[pin]
        inner: hyper::body::Incoming,
        _permit: OwnedSemaphorePermit,
    }
}

impl hyper::body::Body for PermitBody {
    type Data = bytes::Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        self.project().inner.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// Cloneable handle to one logical HTTP/2 connection, dialed lazily and
/// re-dialed on demand. Clones share the connection, the in-flight gate and
/// the dial state, so a `RawChannel` can be handed out per VU or pooled and
/// round-robined exactly like a tonic `Channel`.
#[derive(Clone)]
pub(crate) struct RawChannel {
    shared: Arc<RawShared>,
}

struct RawShared {
    /// `http(s)://host:port`; scheme+authority for `Grpc::with_origin`.
    origin: http::Uri,
    /// The same endpoint as a URL: dial target.
    url: url::Url,
    tls: Option<TlsParams>,
    /// In-flight stream gate; permits ride the response bodies.
    limit: Arc<Semaphore>,
    /// Latest live sender. Read-locked briefly per request; the guard is
    /// never held across an await.
    conn: RwLock<Option<SendRequest<tonic::body::Body>>>,
    /// Serializes dials (singleflight) and remembers the last failure for
    /// the cooldown window.
    dial: Mutex<DialGate>,
}

#[derive(Default)]
struct DialGate {
    cooldown_until: Option<Instant>,
    last_error: String,
}

impl RawChannel {
    pub(crate) fn new(endpoint: &str, tls: Option<TlsParams>) -> Result<Self, ProtocolError> {
        let origin: http::Uri = endpoint.parse().map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid grpc endpoint `{endpoint}`: {e}"))
        })?;
        let url = url::Url::parse(endpoint).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid grpc endpoint `{endpoint}`: {e}"))
        })?;
        Ok(RawChannel {
            shared: Arc::new(RawShared {
                origin,
                url,
                tls,
                limit: Arc::new(Semaphore::new(max_streams_per_conn())),
                conn: RwLock::new(None),
                dial: Mutex::new(DialGate::default()),
            }),
        })
    }

    /// Scheme+authority URI for `tonic::client::Grpc::with_origin`.
    pub(crate) fn origin(&self) -> &http::Uri {
        &self.shared.origin
    }
}

impl Service<http::Request<tonic::body::Body>> for RawChannel {
    type Response = http::Response<PermitBody>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Status>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        // Liveness and capacity are gated inside `call`: tonic awaits the
        // returned future immediately, so backpressure there is equivalent
        // and needs no state carried between poll_ready and call.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let shared = self.shared.clone();
        Box::pin(async move { shared.call(req).await })
    }
}

impl RawShared {
    async fn call(
        self: Arc<Self>,
        req: http::Request<tonic::body::Body>,
    ) -> Result<http::Response<PermitBody>, Status> {
        // Acquired before the sender so dial waiters are bounded too.
        let permit = self
            .limit
            .clone()
            .acquire_owned()
            .await
            .expect("in-flight semaphore is never closed");
        let mut sender = self.sender().await?;
        let response = sender
            .send_request(req)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        Ok(response.map(|inner| PermitBody {
            inner,
            _permit: permit,
        }))
    }

    /// A live `SendRequest`, dialing (one task at a time) when there is none.
    async fn sender(&self) -> Result<SendRequest<tonic::body::Body>, Status> {
        if let Some(sender) = self.live_sender() {
            return Ok(sender);
        }
        let mut gate = self.dial.lock().await;
        // Another task may have dialed while this one waited for the gate.
        if let Some(sender) = self.live_sender() {
            return Ok(sender);
        }
        if let Some(until) = gate.cooldown_until {
            if Instant::now() < until {
                return Err(Status::unavailable(format!(
                    "connection failed: {}",
                    gate.last_error
                )));
            }
        }
        match self.dial().await {
            Ok(sender) => {
                *self.conn.write().unwrap_or_else(PoisonError::into_inner) = Some(sender.clone());
                gate.cooldown_until = None;
                Ok(sender)
            }
            Err(message) => {
                gate.cooldown_until = Some(Instant::now() + DIAL_COOLDOWN);
                gate.last_error = message.clone();
                Err(Status::unavailable(format!("connection failed: {message}")))
            }
        }
    }

    fn live_sender(&self) -> Option<SendRequest<tonic::body::Body>> {
        self.conn
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|sender| !sender.is_closed())
            .cloned()
    }

    /// DNS → TCP → (TLS) → h2 handshake, mirroring the HTTP handler's dial.
    /// Windows and keepalive match `pooled_endpoint()` in grpc.rs so the
    /// raw/channel A/B differs only in the client stack, not in h2 tuning.
    async fn dial(&self) -> Result<SendRequest<tonic::body::Body>, String> {
        let host = self
            .url
            .host()
            .ok_or_else(|| format!("url `{}` has no host", self.url))?;
        let port = self
            .url
            .port_or_known_default()
            .ok_or_else(|| format!("url `{}` has no port", self.url))?;
        // Setup time is deliberately not phase-tracked: gRPC timings report a
        // single elapsed figure, for both transports alike.
        let addrs = crate::net::resolve_all(&host, port, &mut Timings::default()).await?;
        let tcp = crate::net::connect_tcp(&addrs).await?;
        let _ = tcp.set_nodelay(true);

        let stream: Box<dyn IoStream> = match &self.tls {
            Some(tls) => {
                let connector = tokio_rustls::TlsConnector::from(tls.config.clone());
                let stream = connector
                    .connect(tls.server_name.clone(), tcp)
                    .await
                    .map_err(|e| format!("tls handshake with {host}:{port} failed: {e}"))?;
                Box::new(stream)
            }
            None => Box::new(tcp),
        };

        // The timer is mandatory: hyper panics when keepalive arms a sleep
        // without one.
        let (sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .initial_stream_window_size(4 * 1024 * 1024)
            .initial_connection_window_size(8 * 1024 * 1024)
            .keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|e| format!("h2 handshake failed: {e}"))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "grpc h2 connection ended with error");
            }
        });
        Ok(sender)
    }
}
