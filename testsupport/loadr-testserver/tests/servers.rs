//! Integration tests exercising every endpoint of every test server.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use http::header;
use http::{HeaderMap, Request, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_tungstenite::tungstenite::Message;

use loadr_testserver::pb::echo_client::EchoClient;
use loadr_testserver::pb::EchoRequest;
use loadr_testserver::{
    GrpcEchoServer, HttpTestServer, TcpEchoServer, UdpEchoServer, WsEchoServer,
};

const TIMEOUT: Duration = Duration::from_secs(30);

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(TIMEOUT, fut).await.expect("timed out")
}

async fn send(request: Request<Full<Bytes>>) -> (StatusCode, HeaderMap, Bytes) {
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let response = within(client.request(request))
        .await
        .expect("request failed");
    let (parts, body) = response.into_parts();
    let body = within(body.collect())
        .await
        .expect("body read failed")
        .to_bytes();
    (parts.status, parts.headers, body)
}

async fn get(url: &str) -> (StatusCode, HeaderMap, Bytes) {
    let request = Request::builder()
        .uri(url)
        .body(Full::default())
        .expect("request build failed");
    send(request).await
}

fn json(body: &Bytes) -> serde_json::Value {
    serde_json::from_slice(body).expect("response was not valid JSON")
}

#[tokio::test]
async fn http_root_welcome() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, _, body) = get(&server.url("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"Welcome to loadr-testserver");
    assert!(server.base_url().starts_with("http://127.0.0.1:"));
}

#[tokio::test]
async fn http_echo_reflects_request() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let request = Request::builder()
        .method("POST")
        .uri(server.url("/echo?foo=bar&n=2"))
        .header("x-test-header", "test-value")
        .body(Full::new(Bytes::from_static(b"hello body")))
        .expect("request build failed");
    let (status, _, body) = send(request).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json(&body);
    assert_eq!(doc["method"], "POST");
    assert_eq!(doc["path"], "/echo");
    assert_eq!(doc["query"]["foo"], "bar");
    assert_eq!(doc["query"]["n"], "2");
    assert_eq!(doc["headers"]["x-test-header"], "test-value");
    assert_eq!(doc["body"], "hello body");
    assert_eq!(doc["body_base64"], "aGVsbG8gYm9keQ==");
}

#[tokio::test]
async fn http_status_codes() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    for code in [200u16, 204, 404, 418, 503] {
        let (status, _, _) = get(&server.url(&format!("/status/{code}"))).await;
        assert_eq!(status.as_u16(), code, "for /status/{code}");
    }
}

#[tokio::test]
async fn http_delay() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let started = std::time::Instant::now();
    let (status, _, body) = get(&server.url("/delay/100")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"delayed 100ms");
    assert!(started.elapsed() >= Duration::from_millis(100));
}

#[tokio::test]
async fn http_json_doc() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, headers, body) = get(&server.url("/json")).await;
    assert_eq!(status, StatusCode::OK);
    let content_type = headers[header::CONTENT_TYPE].to_str().expect("ct");
    assert!(content_type.starts_with("application/json"));
    let doc = json(&body);
    assert_eq!(doc["token"], "tok-123");
    assert_eq!(doc["items"][0]["name"], "alpha");
    assert_eq!(doc["items"][1]["id"], 2);
    assert_eq!(doc["nested"]["deep"]["value"], 42);
}

#[tokio::test]
async fn http_xml_doc() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, headers, body) = get(&server.url("/xml")).await;
    assert_eq!(status, StatusCode::OK);
    let content_type = headers[header::CONTENT_TYPE].to_str().expect("ct");
    assert!(content_type.starts_with("application/xml"));
    assert_eq!(
        &body[..],
        br#"<catalog><item id="1"><name>alpha</name></item><item id="2"><name>beta</name></item></catalog>"#
    );
}

#[tokio::test]
async fn http_html_doc() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, headers, body) = get(&server.url("/html")).await;
    assert_eq!(status, StatusCode::OK);
    let content_type = headers[header::CONTENT_TYPE].to_str().expect("ct");
    assert!(content_type.starts_with("text/html"));
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(text.contains(r#"<input name="csrf" value="csrf-token-xyz">"#));
    assert!(text.contains(r#"<a href="/json">"#));
    assert!(text.contains(r#"<a href="/xml">"#));
}

#[tokio::test]
async fn http_cookies_set_and_echo() {
    let server = HttpTestServer::spawn().await.expect("spawn");

    // Setting cookies returns Set-Cookie headers; no request cookies yet.
    let (status, headers, body) = get(&server.url("/cookies/set?alpha=1&beta=two")).await;
    assert_eq!(status, StatusCode::OK);
    let set: Vec<String> = headers
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().expect("set-cookie utf8").to_string())
        .collect();
    assert_eq!(set.len(), 2);
    assert!(set.iter().any(|c| c.starts_with("alpha=1")));
    assert!(set.iter().any(|c| c.starts_with("beta=two")));
    assert_eq!(json(&body), serde_json::json!({}));

    // Request cookies are echoed back as JSON.
    let request = Request::builder()
        .uri(server.url("/cookies"))
        .header(header::COOKIE, "alpha=1; beta=two")
        .body(Full::default())
        .expect("request build failed");
    let (status, _, body) = send(request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json(&body),
        serde_json::json!({"alpha": "1", "beta": "two"})
    );
}

#[tokio::test]
async fn http_gzip() {
    use std::io::Read as _;
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, headers, body) = get(&server.url("/gzip")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
    let mut decoder = flate2::read::GzDecoder::new(&body[..]);
    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded).expect("gunzip");
    assert_eq!(decoded, r#"{"compressed":true}"#);
}

#[tokio::test]
async fn http_redirect_chain() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let mut url = server.url("/redirect/3");
    let mut hops = 0;
    loop {
        let (status, headers, body) = get(&url).await;
        if status == StatusCode::FOUND {
            hops += 1;
            assert!(hops <= 10, "redirect loop");
            let location = headers[header::LOCATION].to_str().expect("location");
            url = server.url(location);
        } else {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(&body[..], b"done");
            break;
        }
    }
    assert_eq!(hops, 3);
}

#[tokio::test]
async fn http_login_json_and_form() {
    let server = HttpTestServer::spawn().await.expect("spawn");

    // JSON login
    let request = Request::builder()
        .method("POST")
        .uri(server.url("/login"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"username":"alice","password":"secret"}"#,
        )))
        .expect("request build failed");
    let (status, headers, body) = send(request).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json(&body);
    let token = doc["token"].as_str().expect("token");
    assert!(token.starts_with("sess-"));
    assert_eq!(token.len(), "sess-".len() + 32);
    let set_cookie = headers[header::SET_COOKIE].to_str().expect("set-cookie");
    assert!(set_cookie.starts_with(&format!("session={token}")));
    assert!(set_cookie.contains("HttpOnly"));

    // Form login (with percent-encoding)
    let request = Request::builder()
        .method("POST")
        .uri(server.url("/login"))
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Full::new(Bytes::from_static(
            b"username=bob&password=p%40ss+word",
        )))
        .expect("request build failed");
    let (status, headers, body) = send(request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json(&body)["token"]
        .as_str()
        .expect("token")
        .starts_with("sess-"));
    assert!(headers.contains_key(header::SET_COOKIE));

    // Missing credentials
    let request = Request::builder()
        .method("POST")
        .uri(server.url("/login"))
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Full::new(Bytes::from_static(b"username=carol")))
        .expect("request build failed");
    let (status, _, _) = send(request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_large_body() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let (status, _, body) = get(&server.url("/large/8")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.len(), 8 * 1024);
    assert!(body.iter().all(|&b| b == b'x'));
}

#[tokio::test]
async fn http_headers_echo() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    let request = Request::builder()
        .uri(server.url("/headers"))
        .header("x-custom", "abc")
        .header("x-multi", "one")
        .header("x-multi", "two")
        .body(Full::default())
        .expect("request build failed");
    let (status, _, body) = send(request).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json(&body);
    assert_eq!(doc["x-custom"], "abc");
    assert_eq!(doc["x-multi"], "one, two");
}

#[tokio::test]
async fn http_counter_increments() {
    let server = HttpTestServer::spawn().await.expect("spawn");
    for expected in 1..=3u64 {
        let (status, _, body) = get(&server.url("/counter")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], expected.to_string().as_bytes());
    }
}

#[tokio::test]
async fn http_shutdown_stops_accepting() {
    let mut server = HttpTestServer::spawn().await.expect("spawn");
    let addr = server.addr;
    let (status, _, _) = get(&server.url("/")).await;
    assert_eq!(status, StatusCode::OK);

    server.shutdown();

    // The listener should be gone shortly after shutdown.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Err(_) => break,
            Ok(_) if std::time::Instant::now() > deadline => {
                panic!("server still accepting connections after shutdown");
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn tls_connect(
    server: &HttpTestServer,
    alpn: &[&[u8]],
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let der = server.cert_der().expect("cert_der").to_vec();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(der))
        .expect("add root");
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = within(tokio::net::TcpStream::connect(server.addr))
        .await
        .expect("tcp connect");
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    within(connector.connect(name, tcp))
        .await
        .expect("tls handshake")
}

#[tokio::test]
async fn https_http1_with_trusted_self_signed_cert() {
    let server = HttpTestServer::spawn_tls().await.expect("spawn tls");
    assert!(server.base_url().starts_with("https://127.0.0.1:"));
    let pem = server.cert_pem().expect("cert_pem");
    assert!(pem.contains("BEGIN CERTIFICATE"));

    let tls = tls_connect(&server, &[b"http/1.1"]).await;
    {
        let (_, session) = tls.get_ref();
        assert_eq!(session.alpn_protocol(), Some(&b"http/1.1"[..]));
    }
    let (mut sender, connection) = within(hyper::client::conn::http1::handshake(TokioIo::new(tls)))
        .await
        .expect("h1 handshake");
    tokio::spawn(connection);
    let request = Request::builder()
        .uri("/json")
        .header(header::HOST, "localhost")
        .body(Full::<Bytes>::default())
        .expect("request build failed");
    let response = within(sender.send_request(request)).await.expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    let body = within(response.into_body().collect())
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(json(&body)["token"], "tok-123");
}

#[tokio::test]
async fn https_h2_via_alpn() {
    let server = HttpTestServer::spawn_tls().await.expect("spawn tls");
    let tls = tls_connect(&server, &[b"h2"]).await;
    {
        let (_, session) = tls.get_ref();
        assert_eq!(session.alpn_protocol(), Some(&b"h2"[..]));
    }
    let (mut sender, connection) = within(hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(tls),
    ))
    .await
    .expect("h2 handshake");
    tokio::spawn(connection);
    let request = Request::builder()
        .uri(format!("https://localhost{}", "/json"))
        .body(Full::<Bytes>::default())
        .expect("request build failed");
    let response = within(sender.send_request(request)).await.expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.version(), http::Version::HTTP_2);
    let body = within(response.into_body().collect())
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(json(&body)["nested"]["deep"]["value"], 42);
}

#[tokio::test]
async fn ws_echo_text_binary_and_close() {
    let server = WsEchoServer::spawn().await.expect("spawn");
    let (mut ws, _) = within(tokio_tungstenite::connect_async(server.url()))
        .await
        .expect("ws connect");

    ws.send(Message::text("hello ws")).await.expect("send text");
    let echoed = within(ws.next())
        .await
        .expect("stream ended")
        .expect("recv");
    assert_eq!(echoed, Message::text("hello ws"));

    ws.send(Message::Binary(Bytes::from_static(&[1, 2, 3, 255])))
        .await
        .expect("send binary");
    let echoed = within(ws.next())
        .await
        .expect("stream ended")
        .expect("recv");
    assert_eq!(echoed, Message::Binary(Bytes::from_static(&[1, 2, 3, 255])));

    ws.send(Message::text("ping-close"))
        .await
        .expect("send close trigger");
    let closed = within(ws.next()).await;
    match closed {
        Some(Ok(Message::Close(_))) | None => {}
        other => panic!("expected close frame, got {other:?}"),
    }
}

#[tokio::test]
async fn grpc_unary_echo() {
    let server = GrpcEchoServer::spawn().await.expect("spawn");
    let mut client = within(EchoClient::connect(server.url()))
        .await
        .expect("connect");
    let response = within(client.unary_echo(EchoRequest {
        message: "hello grpc".to_string(),
        repeat: 0,
        ..Default::default()
    }))
    .await
    .expect("unary")
    .into_inner();
    assert_eq!(response.message, "hello grpc");
    assert_eq!(response.index, 0);
}

#[tokio::test]
async fn grpc_tls_url_uses_https_and_connects() {
    let server = GrpcEchoServer::spawn_tls().await.expect("spawn tls");
    assert!(server.url().starts_with("https://127.0.0.1:"));
    assert_eq!(server.base_url(), server.url());

    let certificate =
        tonic::transport::Certificate::from_pem(server.cert_pem().expect("TLS server certificate"));
    let endpoint = tonic::transport::Endpoint::from_shared(server.url())
        .expect("endpoint")
        .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(certificate))
        .expect("TLS config");
    let channel = within(endpoint.connect()).await.expect("TLS connect");
    let mut client = EchoClient::new(channel);
    let response = within(client.unary_echo(EchoRequest {
        message: "hello TLS gRPC".to_string(),
        repeat: 0,
        ..Default::default()
    }))
    .await
    .expect("unary")
    .into_inner();
    assert_eq!(response.message, "hello TLS gRPC");
}

#[tokio::test]
async fn grpc_server_stream_echo() {
    let server = GrpcEchoServer::spawn().await.expect("spawn");
    let mut client = within(EchoClient::connect(server.url()))
        .await
        .expect("connect");

    // Explicit repeat
    let mut stream = within(client.server_stream_echo(EchoRequest {
        message: "s".to_string(),
        repeat: 4,
        ..Default::default()
    }))
    .await
    .expect("server stream")
    .into_inner();
    let mut received = Vec::new();
    while let Some(msg) = within(stream.message()).await.expect("recv") {
        received.push(msg);
    }
    assert_eq!(received.len(), 4);
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.message, "s");
        assert_eq!(msg.index, i as i32);
    }

    // Default repeat of 3 when repeat <= 0
    let mut stream = within(client.server_stream_echo(EchoRequest {
        message: "d".to_string(),
        repeat: 0,
        ..Default::default()
    }))
    .await
    .expect("server stream")
    .into_inner();
    let mut count = 0;
    while let Some(msg) = within(stream.message()).await.expect("recv") {
        assert_eq!(msg.message, "d");
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn grpc_client_stream_echo() {
    let server = GrpcEchoServer::spawn().await.expect("spawn");
    let mut client = within(EchoClient::connect(server.url()))
        .await
        .expect("connect");
    let requests = futures::stream::iter(vec![
        EchoRequest {
            message: "a".to_string(),
            repeat: 0,
            ..Default::default()
        },
        EchoRequest {
            message: "b".to_string(),
            repeat: 0,
            ..Default::default()
        },
        EchoRequest {
            message: "c".to_string(),
            repeat: 0,
            ..Default::default()
        },
    ]);
    let response = within(client.client_stream_echo(requests))
        .await
        .expect("client stream")
        .into_inner();
    assert_eq!(response.message, "abc");
    assert_eq!(response.index, 3);
}

#[tokio::test]
async fn grpc_bidi_echo() {
    let server = GrpcEchoServer::spawn().await.expect("spawn");
    let mut client = within(EchoClient::connect(server.url()))
        .await
        .expect("connect");
    let requests = futures::stream::iter(vec![
        EchoRequest {
            message: "x".to_string(),
            repeat: 0,
            ..Default::default()
        },
        EchoRequest {
            message: "y".to_string(),
            repeat: 0,
            ..Default::default()
        },
        EchoRequest {
            message: "z".to_string(),
            repeat: 0,
            ..Default::default()
        },
    ]);
    let mut inbound = within(client.bidi_echo(requests))
        .await
        .expect("bidi")
        .into_inner();
    let mut received = Vec::new();
    while let Some(msg) = within(inbound.message()).await.expect("recv") {
        received.push(msg);
    }
    let expected = ["x", "y", "z"];
    assert_eq!(received.len(), expected.len());
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.message, expected[i]);
        assert_eq!(msg.index, i as i32);
    }
}

#[tokio::test]
async fn grpc_file_descriptor_set_is_valid() {
    use prost::Message as _;
    let bytes = GrpcEchoServer::file_descriptor_set_bytes();
    assert!(!bytes.is_empty());
    let fds = prost_types::FileDescriptorSet::decode(bytes).expect("decode fds");
    let file = fds
        .file
        .iter()
        .find(|f| f.package() == "loadr.test")
        .expect("loadr.test file in descriptor set");
    assert!(file.service.iter().any(|s| s.name() == "Echo"));
}

#[tokio::test]
async fn grpc_reflection_lists_services() {
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
    use tonic_reflection::pb::v1::ServerReflectionRequest;

    let server = GrpcEchoServer::spawn().await.expect("spawn");
    let endpoint = tonic::transport::Endpoint::from_shared(server.url()).expect("endpoint");
    let channel = within(endpoint.connect()).await.expect("connect");
    let mut client = ServerReflectionClient::new(channel);
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut inbound = within(client.server_reflection_info(futures::stream::iter(vec![request])))
        .await
        .expect("reflection call")
        .into_inner();
    let response = within(inbound.message())
        .await
        .expect("recv")
        .expect("one reflection response");
    match response.message_response {
        Some(MessageResponse::ListServicesResponse(list)) => {
            assert!(
                list.service.iter().any(|s| s.name == "loadr.test.Echo"),
                "expected loadr.test.Echo in {list:?}"
            );
        }
        other => panic!("unexpected reflection response: {other:?}"),
    }
}

#[tokio::test]
async fn tcp_echo_roundtrip() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let server = TcpEchoServer::spawn().await.expect("spawn");
    let mut stream = within(tokio::net::TcpStream::connect(server.addr))
        .await
        .expect("connect");

    stream.write_all(b"hello tcp").await.expect("write");
    let mut buf = [0u8; 9];
    within(stream.read_exact(&mut buf)).await.expect("read");
    assert_eq!(&buf, b"hello tcp");

    stream.write_all(b"more").await.expect("write");
    let mut buf = [0u8; 4];
    within(stream.read_exact(&mut buf)).await.expect("read");
    assert_eq!(&buf, b"more");

    // Server echoes until EOF: after we shut down writes, the stream ends.
    stream.shutdown().await.expect("shutdown write");
    let mut rest = Vec::new();
    let n = within(stream.read_to_end(&mut rest))
        .await
        .expect("read to end");
    assert_eq!(n, 0);
}

#[tokio::test]
async fn udp_echo_roundtrip() {
    let server = UdpEchoServer::spawn().await.expect("spawn");
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind");
    socket.connect(server.addr).await.expect("connect");

    socket.send(b"ping udp").await.expect("send");
    let mut buf = [0u8; 64];
    let n = within(socket.recv(&mut buf)).await.expect("recv");
    assert_eq!(&buf[..n], b"ping udp");

    socket.send(&[0xde, 0xad, 0xbe, 0xef]).await.expect("send");
    let n = within(socket.recv(&mut buf)).await.expect("recv");
    assert_eq!(&buf[..n], [0xde, 0xad, 0xbe, 0xef]);
}
