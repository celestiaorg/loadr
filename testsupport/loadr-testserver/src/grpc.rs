use std::net::SocketAddr;
use std::pin::Pin;

use futures::Stream;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming};

use crate::TestServerError;

/// Generated protobuf/tonic code for `loadr.test.Echo`.
pub mod pb {
    #![allow(clippy::all, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/loadr.test.rs"));
}

/// The compiled `FileDescriptorSet` for `proto/echo.proto`, usable for
/// dynamic codecs or reflection clients.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/echo_descriptor.bin"));

use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

type EchoResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[derive(Debug, Default)]
struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn unary_echo(&self, request: Request<EchoRequest>) -> EchoResult<EchoResponse> {
        let req = request.into_inner();
        if req.fail_transport {
            return Err(Status::resource_exhausted("requested transport failure"));
        }
        Ok(Response::new(EchoResponse {
            message: req.message,
            index: 0,
            payload: req.payload,
            code: req.response_code,
            owner_hint: req.owner_hint,
        }))
    }

    type ServerStreamEchoStream = ResponseStream;

    async fn server_stream_echo(
        &self,
        request: Request<EchoRequest>,
    ) -> EchoResult<Self::ServerStreamEchoStream> {
        let req = request.into_inner();
        let repeat = if req.repeat > 0 { req.repeat } else { 3 };
        let message = req.message;
        let payload = req.payload;
        let code = req.response_code;
        let owner_hint = req.owner_hint;
        let stream = futures::stream::iter((0..repeat).map(move |index| {
            Ok(EchoResponse {
                message: message.clone(),
                index,
                payload: payload.clone(),
                code,
                owner_hint,
            })
        }));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn client_stream_echo(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<EchoResponse> {
        let mut inbound = request.into_inner();
        let mut combined = String::new();
        let mut payload = Vec::new();
        let mut code = 0;
        let mut owner_hint = None;
        let mut count = 0i32;
        while let Some(req) = inbound.message().await? {
            combined.push_str(&req.message);
            payload = req.payload;
            code = req.response_code;
            owner_hint = req.owner_hint;
            count += 1;
        }
        Ok(Response::new(EchoResponse {
            message: combined,
            index: count,
            payload,
            code,
            owner_hint,
        }))
    }

    type BidiEchoStream = ResponseStream;

    async fn bidi_echo(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<Self::BidiEchoStream> {
        let inbound = request.into_inner();
        let stream =
            futures::stream::unfold((Some(inbound), 0i32), |(inbound, index)| async move {
                let mut inbound = inbound?;
                match inbound.message().await {
                    Ok(Some(req)) => Some((
                        Ok(EchoResponse {
                            message: req.message,
                            index,
                            payload: req.payload,
                            code: req.response_code,
                            owner_hint: req.owner_hint,
                        }),
                        (Some(inbound), index + 1),
                    )),
                    Ok(None) => None,
                    Err(status) => Some((Err(status), (None, index))),
                }
            });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// In-process tonic gRPC echo server implementing `loadr.test.Echo`:
///
/// - `UnaryEcho` echoes the request message (index 0).
/// - `ServerStreamEcho` sends `repeat` responses (default 3 when `repeat <= 0`)
///   with incrementing indexes.
/// - `ClientStreamEcho` concatenates all request messages; `index` carries the
///   message count.
/// - `BidiEcho` echoes each request with an incrementing index.
///
/// Also serves gRPC v1 server reflection. Shuts down on drop.
pub struct GrpcEchoServer {
    /// Bound address (always `127.0.0.1`; port is ephemeral unless spawned
    /// via [`spawn_on`](Self::spawn_on)).
    pub addr: SocketAddr,
    scheme: &'static str,
    cert_pem: Option<String>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl GrpcEchoServer {
    /// Spawns the server on `127.0.0.1` with an ephemeral port.
    pub async fn spawn() -> Result<Self, TestServerError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        Self::serve(listener, None).await
    }

    /// Spawns on a specific address — e.g. to come back up on the same port
    /// after a shutdown in reconnect tests.
    pub async fn spawn_on(addr: SocketAddr) -> Result<Self, TestServerError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Self::serve(listener, None).await
    }

    /// Spawns a TLS server with a self-signed certificate (valid for
    /// `localhost` and `127.0.0.1`, ALPN `h2`). Trust it client-side via
    /// [`cert_pem`](Self::cert_pem) or connect with verification disabled.
    pub async fn spawn_tls() -> Result<Self, TestServerError> {
        let certified = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .map_err(|e| TestServerError::Tls(e.to_string()))?;
        let cert_pem = certified.cert.pem();
        let identity =
            tonic::transport::Identity::from_pem(&cert_pem, certified.signing_key.serialize_pem());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut server = Self::serve(listener, Some(identity)).await?;
        server.cert_pem = Some(cert_pem);
        Ok(server)
    }

    async fn serve(
        listener: tokio::net::TcpListener,
        tls: Option<tonic::transport::Identity>,
    ) -> Result<Self, TestServerError> {
        let addr = listener.local_addr()?;
        let scheme = if tls.is_some() { "https" } else { "http" };
        let (tx, rx) = oneshot::channel::<()>();
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| TestServerError::Grpc(e.to_string()))?;
        let incoming = futures::stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await.map(|(stream, _)| {
                // Raw accept bypasses tonic's TcpIncoming, which is what
                // normally disables Nagle; without this, multi-write h2
                // responses stall ~40ms on delayed ACKs under load.
                let _ = stream.set_nodelay(true);
                stream
            });
            Some((accepted, listener))
        });
        let mut builder = tonic::transport::Server::builder();
        if let Some(identity) = tls {
            builder = builder
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .map_err(|e| TestServerError::Tls(e.to_string()))?;
        }
        tokio::spawn(async move {
            let result = builder
                .add_service(EchoServer::new(EchoService))
                .add_service(reflection)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = rx.await;
                })
                .await;
            if let Err(e) = result {
                tracing::warn!(error = %e, "grpc test server exited with error");
            }
            tracing::debug!("grpc test server stopped");
        });
        tracing::debug!(%addr, "grpc test server listening");
        Ok(Self {
            addr,
            scheme,
            cert_pem: None,
            shutdown: Some(tx),
        })
    }

    /// PEM certificate when spawned with [`spawn_tls`](Self::spawn_tls).
    pub fn cert_pem(&self) -> Option<&str> {
        self.cert_pem.as_deref()
    }

    /// Base URL suitable for `EchoClient::connect`, e.g. `http://127.0.0.1:54321`
    /// or `https://127.0.0.1:54321` for a TLS server.
    pub fn url(&self) -> String {
        format!("{}://{}", self.scheme, self.addr)
    }

    /// Alias for [`url`](Self::url).
    pub fn base_url(&self) -> String {
        self.url()
    }

    /// The compiled `FileDescriptorSet` bytes for `proto/echo.proto`.
    pub fn file_descriptor_set_bytes() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }

    /// Stops the server. Also happens automatically on drop.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for GrpcEchoServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}
