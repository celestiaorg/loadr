//! Integration tests for loadr-protocols against the in-process test servers.

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loadr_config::{HttpDefaults, HttpVersionPref, TlsConfig};
use loadr_core::data::DataFeeds;
use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
use loadr_core::protocol::{
    GrpcRequest, PreparedRequest, ProtocolHandler, RequestOptions, SocketRequest, WsFrame,
    WsRequest,
};
use loadr_core::vu::{RunContext, VuContext};
use loadr_protocols::{
    builtin_registry, GraphqlHandler, GrpcHandler, HttpHandler, TcpHandler, UdpHandler, WsHandler,
};
use loadr_testserver::{
    GrpcEchoServer, HttpTestServer, TcpEchoServer, UdpEchoServer, WsEchoServer,
};

const ECHO_PROTO: &str = r#"syntax = "proto3";

package loadr.test;

service Echo {
  rpc UnaryEcho(EchoRequest) returns (EchoResponse);
  rpc ServerStreamEcho(EchoRequest) returns (stream EchoResponse);
  rpc ClientStreamEcho(stream EchoRequest) returns (EchoResponse);
  rpc BidiEcho(stream EchoRequest) returns (stream EchoResponse);
}

message EchoRequest {
  string message = 1;
  int32 repeat = 2;
}

message EchoResponse {
  string message = 1;
  int32 index = 2;
}
"#;

fn vu() -> VuContext {
    let data = DataFeeds::load(&Default::default(), Path::new(".")).expect("data feeds");
    let run = Arc::new(RunContext {
        variables: serde_json::Map::new(),
        secrets: Default::default(),
        env: Default::default(),
        data,
        registry: Arc::new(MetricRegistry::with_builtins()),
        base_dir: ".".into(),
        setup_data: parking_lot::RwLock::new(serde_json::Value::Null),
    });
    let (bus, _rx) = MetricsBus::new();
    VuContext::new(1, Arc::from("t"), Arc::new(Tags::new()), bus, run, true)
}

fn req(url: &str, protocol: &str) -> PreparedRequest {
    PreparedRequest {
        name: url.to_string(),
        protocol: protocol.to_string(),
        method: "GET".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
        timeout: Duration::from_secs(10),
        follow_redirects: true,
        max_redirects: 10,
        options: RequestOptions::default(),
    }
}

fn http_handler() -> HttpHandler {
    HttpHandler::new(&HttpDefaults::default(), Path::new(".")).expect("http handler")
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_get_json_with_timings_and_connection_reuse() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let first = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("first request");
    assert_eq!(first.status, 200);
    assert!(!first.failed());
    assert!(first.timings.duration_ms > 0.0, "duration should be > 0");
    assert!(first.timings.waiting_ms > 0.0, "waiting should be > 0");
    assert!(first.bytes_received > 0);
    assert!(first.bytes_sent > 0);
    let json: serde_json::Value = serde_json::from_slice(&first.body).expect("json body");
    assert_eq!(json["token"], "tok-123");

    // Second request on the same VU must reuse the pooled connection.
    let second = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("second request");
    assert_eq!(second.status, 200);
    assert_eq!(second.timings.blocked_ms, 0.0, "reused: no blocked time");
    assert_eq!(second.timings.dns_ms, 0.0, "reused: no dns time");
    assert_eq!(second.timings.connect_ms, 0.0, "reused: no connect time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_post_echo_body_and_headers() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let mut request = req(&server.url("/echo"), "http");
    request.method = "POST".to_string();
    request.body = Bytes::from_static(b"hello loadr");
    request.headers = vec![
        ("x-test".to_string(), "42".to_string()),
        ("content-type".to_string(), "text/plain".to_string()),
    ];

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 200);
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json");
    assert_eq!(json["method"], "POST");
    assert_eq!(json["body"], "hello loadr");
    assert_eq!(json["headers"]["x-test"], "42");
    assert_eq!(json["headers"]["user-agent"], "loadr/0.1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_status_404_is_failed() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/status/404"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 404);
    assert!(response.failed());
    assert!(response.error.is_none(), "4xx is not a transport error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delay_reflects_in_duration() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/delay/200"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200);
    assert!(
        response.timings.duration_ms >= 195.0,
        "duration {} should reflect the 200ms server delay",
        response.timings.duration_ms
    );
    assert!(response.timings.waiting_ms >= 195.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_timeout_returns_error_response() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let mut request = req(&server.url("/delay/2000"), "http");
    request.timeout = Duration::from_millis(300);
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0);
    let error = response.error.as_deref().expect("timeout error");
    assert!(error.contains("timed out"), "got: {error}");
    assert!(response.failed());
    assert!(response.timings.duration_ms >= 295.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_gzip_body_is_decompressed() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/gzip"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200);
    assert_eq!(response.body_text(), r#"{"compressed":true}"#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_redirects_followed_and_not_followed() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let followed = handler
        .execute(&mut vu, &req(&server.url("/redirect/3"), "http"))
        .await
        .expect("response");
    assert_eq!(followed.status, 200);
    assert!(
        followed.url.ends_with("/redirect/0"),
        "final url: {}",
        followed.url
    );
    assert_eq!(followed.body_text(), "done");

    let mut request = req(&server.url("/redirect/3"), "http");
    request.follow_redirects = false;
    let unfollowed = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(unfollowed.status, 302);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_cookies_stored_and_sent_automatically() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = http_handler();
    let mut vu = vu();

    let set = handler
        .execute(
            &mut vu,
            &req(&server.url("/cookies/set?session=abc"), "http"),
        )
        .await
        .expect("set response");
    assert_eq!(set.status, 200);

    let get = handler
        .execute(&mut vu, &req(&server.url("/cookies"), "http"))
        .await
        .expect("get response");
    assert_eq!(get.status, 200);
    let json: serde_json::Value = serde_json::from_slice(&get.body).expect("json");
    assert_eq!(json["session"], "abc", "cookie jar should send the cookie");
}

// ---------------------------------------------------------------------------
// TLS / HTTP2
// ---------------------------------------------------------------------------

fn write_ca(server: &HttpTestServer) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().expect("temp ca file");
    file.write_all(server.cert_pem().expect("cert pem").as_bytes())
        .expect("write ca");
    file
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_with_ca_file_negotiates_http2() {
    let server = HttpTestServer::spawn_tls().await.expect("tls server");
    let ca = write_ca(&server);

    let defaults = HttpDefaults {
        tls: TlsConfig {
            ca_file: Some(ca.path().to_path_buf()),
            ..Default::default()
        },
        ..Default::default()
    };
    let handler = HttpHandler::new(&defaults, Path::new(".")).expect("handler");
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200, "error: {:?}", response.error);
    assert!(
        response.protocol_version == "HTTP/2" || response.protocol_version == "HTTP/1.1",
        "unexpected version {}",
        response.protocol_version
    );
    // The test server advertises h2 via ALPN, so Auto must negotiate HTTP/2.
    assert_eq!(response.protocol_version, "HTTP/2");
    assert!(
        response.timings.tls_ms > 0.0,
        "tls handshake should be timed"
    );
    assert!(response.timings.blocked_ms > 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_insecure_skip_verify_works() {
    let server = HttpTestServer::spawn_tls().await.expect("tls server");
    let defaults = HttpDefaults {
        tls: TlsConfig {
            insecure_skip_verify: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let handler = HttpHandler::new(&defaults, Path::new(".")).expect("handler");
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200, "error: {:?}", response.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_forcing_http1_gives_http11() {
    let server = HttpTestServer::spawn_tls().await.expect("tls server");
    let ca = write_ca(&server);
    let defaults = HttpDefaults {
        version: HttpVersionPref::Http1,
        tls: TlsConfig {
            ca_file: Some(ca.path().to_path_buf()),
            ..Default::default()
        },
        ..Default::default()
    };
    let handler = HttpHandler::new(&defaults, Path::new(".")).expect("handler");
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200, "error: {:?}", response.error);
    assert_eq!(response.protocol_version, "HTTP/1.1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_untrusted_cert_is_transport_error_not_panic() {
    let server = HttpTestServer::spawn_tls().await.expect("tls server");
    let handler = http_handler(); // default roots: self-signed not trusted
    let mut vu = vu();

    let response = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 0);
    assert!(response.error.is_some());
    assert!(response.failed());
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_echo_two_messages() {
    let server = WsEchoServer::spawn().await.expect("ws server");
    let handler = WsHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let mut request = req(&server.url(), "ws");
    request.options.ws = Some(WsRequest {
        send: vec![
            WsFrame {
                payload: Bytes::from_static(b"first"),
                binary: false,
                delay: None,
            },
            WsFrame {
                payload: Bytes::from_static(b"second"),
                binary: false,
                delay: Some(Duration::from_millis(10)),
            },
        ],
        ..Default::default()
    });

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 101, "error: {:?}", response.error);
    assert!(response.error.is_none());
    assert_eq!(response.extras["msgs_sent"], 2);
    assert_eq!(response.extras["msgs_received"], 2);
    assert_eq!(response.extras["last_message"], "second");
    assert_eq!(response.body_text(), "second");
    assert_eq!(response.protocol_version, "ws");
    assert!(response.timings.duration_ms > 0.0);
}

// ---------------------------------------------------------------------------
// gRPC
// ---------------------------------------------------------------------------

fn grpc_request(url: &str, grpc: GrpcRequest) -> PreparedRequest {
    let mut request = req(url, "grpc");
    request.options.grpc = Some(grpc);
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_via_proto_files() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path],
            service: "loadr.test.Echo".to_string(),
            method: "UnaryEcho".to_string(),
            message: Some(Arc::new(serde_json::json!({"message": "hi grpc"}))),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    assert!(!response.failed());
    assert_eq!(response.protocol_version, "grpc");
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
    assert_eq!(json["message"], "hi grpc");
    assert_eq!(response.extras["message_count"], 1);
    assert!(response.timings.duration_ms > 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_literal_message_cache() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path],
            service: "loadr.test.Echo".to_string(),
            method: "UnaryEcho".to_string(),
            message: Some(Arc::new(serde_json::json!({"message": "cached"}))),
            message_literal: true,
            ..Default::default()
        },
    );

    // The first call resolves and encodes; later calls reuse both per-VU
    // caches while preserving the response.
    for _ in 0..3 {
        let response = handler.execute(&mut vu, &request).await.expect("response");
        assert_eq!(response.status, 0, "status_text: {}", response.status_text);
        assert!(!response.failed());
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "cached");
    }
}

/// The per-VU call cache keys on proto_includes too: the same VU executing
/// the same service/method with a different include set must not reuse the
/// previous entry (same files can resolve differently under other roots).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_call_cache_discriminates_proto_includes() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let base = GrpcRequest {
        proto_files: vec![proto_path],
        service: "loadr.test.Echo".to_string(),
        method: "UnaryEcho".to_string(),
        message: Some(Arc::new(serde_json::json!({"message": "includes"}))),
        ..Default::default()
    };
    let without_includes = grpc_request(&format!("grpc://{}", server.addr), base.clone());
    let with_includes = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_includes: vec![dir.path().to_path_buf()],
            ..base
        },
    );
    for request in [&without_includes, &with_includes, &without_includes] {
        let response = handler.execute(&mut vu, request).await.expect("response");
        assert_eq!(response.status, 0, "status_text: {}", response.status_text);
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "includes");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_server_streaming_collects_all_messages() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path],
            service: "loadr.test.Echo".to_string(),
            method: "ServerStreamEcho".to_string(),
            message: Some(Arc::new(
                serde_json::json!({"message": "stream", "repeat": 3}),
            )),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    assert_eq!(response.extras["message_count"], 3);
    assert_eq!(
        response.extras["messages"].as_array().map(|a| a.len()),
        Some(3)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_via_reflection() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            reflection: true,
            service: "loadr.test.Echo".to_string(),
            method: "UnaryEcho".to_string(),
            message: Some(Arc::new(serde_json::json!({"message": "reflected"}))),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(
        response.status, 0,
        "status_text: {} error: {:?}",
        response.status_text, response.error
    );
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
    assert_eq!(json["message"], "reflected");
}

// ---------------------------------------------------------------------------
// TCP / UDP
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_echo_round_trip() {
    let server = TcpEchoServer::spawn().await.expect("tcp server");
    let handler = TcpHandler::new();
    let mut vu = vu();

    let mut request = req(&format!("tcp://{}", server.addr), "tcp");
    request.options.socket = Some(SocketRequest {
        payload: Bytes::from_static(b"ping over tcp"),
        ..Default::default()
    });
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert!(response.error.is_none(), "error: {:?}", response.error);
    assert_eq!(response.status, 0);
    assert_eq!(response.protocol_version, "tcp");
    assert_eq!(response.body_text(), "ping over tcp");
    assert_eq!(response.bytes_sent, 13);
    assert_eq!(response.bytes_received, 13);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_echo_round_trip() {
    let server = UdpEchoServer::spawn().await.expect("udp server");
    let handler = UdpHandler::new();
    let mut vu = vu();

    let mut request = req(&format!("udp://{}", server.addr), "udp");
    request.options.socket = Some(SocketRequest {
        payload: Bytes::from_static(b"ping over udp"),
        ..Default::default()
    });
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert!(response.error.is_none(), "error: {:?}", response.error);
    assert_eq!(response.status, 0);
    assert_eq!(response.protocol_version, "udp");
    assert_eq!(response.body_text(), "ping over udp");
    assert_eq!(response.bytes_received, 13);
}

// ---------------------------------------------------------------------------
// GraphQL
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graphql_posts_envelope_over_http() {
    let server = HttpTestServer::spawn().await.expect("server");
    let handler = GraphqlHandler::new(Arc::new(http_handler()));
    let mut vu = vu();

    // The engine builds the {query,variables} envelope; simulate that here.
    let mut request = req(&server.url("/echo"), "graphql");
    request.method = "POST".to_string();
    request.headers = vec![("content-type".to_string(), "application/json".to_string())];
    request.body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "query": "{ hero { name } }",
            "variables": {"id": 7},
        }))
        .expect("envelope"),
    );

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 200);
    assert!(response.error.is_none());
    // /echo reflects the posted JSON back inside its own JSON document, which
    // has neither `errors` nor `data`, so post-processing leaves it alone.
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json");
    assert!(json["body"].as_str().expect("body").contains("hero"));
}

// ---------------------------------------------------------------------------
// Registry smoke test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_dispatches_http() {
    let server = HttpTestServer::spawn().await.expect("server");
    let registry = builtin_registry(&HttpDefaults::default(), Path::new(".")).expect("registry");
    let handler = registry.get("https").expect("https alias");
    let mut vu = vu();
    let response = handler
        .execute(&mut vu, &req(&server.url("/json"), "http"))
        .await
        .expect("response");
    assert_eq!(response.status, 200);
}
