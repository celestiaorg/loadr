//! Integration tests for loadr-protocols against the in-process test servers.

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use loadr_config::{GrpcTransport, HttpDefaults, HttpVersionPref, TlsConfig};
use loadr_core::data::DataFeeds;
use loadr_core::metrics::{MetricRegistry, MetricsBus, Tags};
use loadr_core::protocol::{
    GrpcProtobufFieldCheck, GrpcRequest, PreparedRequest, ProtocolHandler, RequestOptions,
    SocketRequest, WsFrame, WsRequest,
};
use loadr_core::vu::{RunContext, VuContext};
use loadr_protocols::{
    builtin_registry, GraphqlHandler, GrpcHandler, HttpHandler, NoopHandler, TcpHandler,
    UdpHandler, WsHandler,
};
use loadr_testserver::{
    GrpcEchoServer, HttpTestServer, TcpEchoServer, UdpEchoServer, WsEchoServer,
};
use prost::Message as _;

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
  bytes payload = 3;
  uint32 response_code = 4;
  optional uint32 owner_hint = 5;
  bool fail_transport = 6;
}

message EchoResponse {
  string message = 1;
  int32 index = 2;
  bytes payload = 3;
  uint32 code = 4;
  optional uint32 owner_hint = 5;
}
"#;

fn vu() -> VuContext {
    let data = DataFeeds::load(&Default::default(), Path::new("."), Default::default())
        .expect("data feeds");
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
        method: "GET".into(),
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
// No-op
// ---------------------------------------------------------------------------

#[tokio::test]
async fn noop_accepts_rendered_body_and_reports_success() {
    let handler = NoopHandler::new();
    let mut vu = vu();
    let mut request = req("noop://local", "noop");
    request.method = "POST".to_string();
    request.body = Bytes::from_static(b"generated-payload");

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 200);
    assert_eq!(response.status_text, "OK");
    assert_eq!(response.protocol_version, "noop");
    assert_eq!(response.url, "noop://local");
    assert_eq!(response.bytes_sent, 17);
    assert_eq!(response.bytes_received, 0);
    assert!(response.body.is_empty());
    assert!(response.headers.is_empty());
    assert!(response.error.is_none());
    assert_eq!(response.timings.duration_ms, 0.0);
    assert!(!response.failed());
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

fn protobuf_check(
    id: u32,
    field: &str,
    equals: Option<serde_json::Value>,
    exists: bool,
) -> GrpcProtobufFieldCheck {
    GrpcProtobufFieldCheck {
        id,
        field: Arc::from(field),
        equals,
        exists,
        group_failures: field == "code",
    }
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
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
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
async fn grpc_protobuf_field_uses_implicit_default_without_json() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "default"}))),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![protobuf_check(
                0,
                "code",
                Some(serde_json::json!(0)),
                true,
            )])),
            ..Default::default()
        },
    );

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0);
    assert!(
        response.body.is_empty(),
        "protobuf-only path serialized JSON"
    );
    assert_eq!(response.grpc_protobuf_outcomes.len(), 1);
    assert!(response.grpc_protobuf_outcomes[0].pass);
    assert!(!response.grpc_protobuf_outcomes[0].missing);
    assert_eq!(response.grpc_protobuf_outcomes[0].actual_code, Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_protobuf_field_reports_semantic_rejection_codes() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        for code in [18, 20] {
            let request = grpc_request(
                &format!("grpc://{}", server.addr),
                GrpcRequest {
                    proto_files: vec![proto_path.clone()].into(),
                    service: "loadr.test.Echo".into(),
                    method: "UnaryEcho".into(),
                    message: Some(Arc::new(serde_json::json!({"responseCode": code}))),
                    transport,
                    protobuf_only_response: true,
                    protobuf_checks: Some(Arc::new(vec![protobuf_check(
                        0,
                        "code",
                        Some(serde_json::json!(0)),
                        true,
                    )])),
                    ..Default::default()
                },
            );
            let response = handler.execute(&mut vu, &request).await.expect("response");
            assert_eq!(response.status, 0, "semantic rejection is transport OK");
            let outcome = &response.grpc_protobuf_outcomes[0];
            assert!(!outcome.pass);
            assert_eq!(outcome.actual_code, Some(code));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_protobuf_presence_distinguishes_optional_and_implicit_fields() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path.clone()].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({}))),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![
                protobuf_check(0, "code", None, true),
                protobuf_check(1, "owner_hint", None, false),
            ])),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert!(response.grpc_protobuf_outcomes[0].pass);
    assert!(!response.grpc_protobuf_outcomes[0].missing);
    assert!(response.grpc_protobuf_outcomes[1].pass);
    assert!(response.grpc_protobuf_outcomes[1].missing);

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({
                "responseCode": 18,
                "ownerHint": 7
            }))),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![
                protobuf_check(0, "code", Some(serde_json::json!(0)), true),
                protobuf_check(1, "owner_hint", Some(serde_json::json!(7)), true),
            ])),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.grpc_protobuf_outcomes[0].actual_code, Some(18));
    assert!(!response.grpc_protobuf_outcomes[0].pass);
    assert!(response.grpc_protobuf_outcomes[1].pass);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_protobuf_field_transport_failure_has_no_message_outcome() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"failTransport": true}))),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![protobuf_check(
                0,
                "code",
                Some(serde_json::json!(0)),
                true,
            )])),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, tonic::Code::ResourceExhausted as i64);
    assert!(response.grpc_protobuf_outcomes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_pooled_channels() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vus = [vu(), vu()];

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "pooled"}))),
            channel_pool_size: Some(2),
            ..Default::default()
        },
    );

    // Each VU pins its resolved call to one successive pool slot, then reuses
    // the cached client. This covers both round-robin slots and the hot path.
    for vu in &mut vus {
        for _ in 0..2 {
            let response = handler.execute(vu, &request).await.expect("response");
            assert_eq!(response.status, 0, "status_text: {}", response.status_text);
            assert!(!response.failed());
            assert_eq!(response.protocol_version, "grpc");
            let json: serde_json::Value =
                serde_json::from_slice(&response.body).expect("json body");
            assert_eq!(json["message"], "pooled");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_literal_message_cache() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
        let mut vu = vu();

        let request = grpc_request(
            &format!("grpc://{}", server.addr),
            GrpcRequest {
                proto_files: vec![proto_path.clone()].into(),
                service: "loadr.test.Echo".into(),
                method: "UnaryEcho".into(),
                message: Some(Arc::new(serde_json::json!({"message": "cached"}))),
                message_literal: true,
                transport,
                ..Default::default()
            },
        );

        // The first call resolves and encodes; later calls reuse the per-VU
        // client, call-state and encoded-body caches on both transports.
        for _ in 0..3 {
            let response = handler.execute(&mut vu, &request).await.expect("response");
            assert_eq!(
                response.status, 0,
                "{transport:?}: {}",
                response.status_text
            );
            assert!(!response.failed());
            let json: serde_json::Value =
                serde_json::from_slice(&response.body).expect("json body");
            assert_eq!(json["message"], "cached");
        }
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
        proto_files: vec![proto_path].into(),
        service: "loadr.test.Echo".into(),
        method: "UnaryEcho".into(),
        message: Some(Arc::new(serde_json::json!({"message": "includes"}))),
        ..Default::default()
    };
    let without_includes = grpc_request(&format!("grpc://{}", server.addr), base.clone());
    let with_includes = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_includes: vec![dir.path().to_path_buf()].into(),
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

/// Rendered metadata and request headers may change between iterations even
/// though the call identity stays the same. Alternating both sources exercises
/// the metadata memo's hit and rebuild paths through the full handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_call_cache_allows_alternating_metadata() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let base = GrpcRequest {
        proto_files: vec![proto_path].into(),
        service: "loadr.test.Echo".into(),
        method: "UnaryEcho".into(),
        message: Some(Arc::new(serde_json::json!({"message": "metadata"}))),
        ..Default::default()
    };

    let mut first = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            metadata: vec![("x-grpc-source".to_string(), "first".to_string())],
            ..base.clone()
        },
    );
    first.headers = vec![("x-header-source".to_string(), "alpha".to_string())];
    let mut second = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            metadata: vec![("x-grpc-source".to_string(), "second".to_string())],
            ..base
        },
    );
    second.headers = vec![("x-header-source".to_string(), "beta".to_string())];

    for request in [&first, &first, &second, &first] {
        let response = handler.execute(&mut vu, request).await.expect("response");
        assert_eq!(response.status, 0, "status_text: {}", response.status_text);
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "metadata");
    }
}

/// A rendered URL is part of the per-VU call identity. Two URLs resolving to
/// different servers must retain independent clients even when every other
/// request field is identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_call_cache_discriminates_rendered_urls() {
    let mut first_server = GrpcEchoServer::spawn().await.expect("first grpc server");
    let second_server = GrpcEchoServer::spawn().await.expect("second grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let base = GrpcRequest {
        proto_files: vec![proto_path].into(),
        service: "loadr.test.Echo".into(),
        method: "UnaryEcho".into(),
        message: Some(Arc::new(serde_json::json!({"message": "routed"}))),
        ..Default::default()
    };
    let first = grpc_request(&format!("grpc://{}", first_server.addr), base.clone());
    let second = grpc_request(&format!("grpc://{}", second_server.addr), base);

    for request in [&first, &second, &first, &second] {
        let response = handler.execute(&mut vu, request).await.expect("response");
        assert_eq!(response.status, 0, "status_text: {}", response.status_text);
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "routed");
    }

    // If the second URL had reused the first URL's call entry, it would fail
    // after the first server shuts down rather than continuing on its client.
    first_server.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let response = handler.execute(&mut vu, &second).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
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
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "ServerStreamEcho".into(),
            message: Some(Arc::new(
                serde_json::json!({"message": "stream", "repeat": 3}),
            )),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    assert_eq!(response.extras["message_count"], 3);
    // `extras.messages` was removed (zero consumers in the workspace; see
    // docs/src/protocols/grpc.md) — `message_count` is the only survivor.
    assert!(response.extras.get("messages").is_none());
}

/// Same VU, same (endpoint, service, method) twice — first decoding, then
/// discarding — proves discard is a per-call mode that doesn't fight the
/// shared `CachedCall` entry (the codec's `discard` flag lives on the
/// per-call clone, not the cached codec).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_discard_skips_body() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let base = GrpcRequest {
        proto_files: vec![proto_path].into(),
        service: "loadr.test.Echo".into(),
        method: "UnaryEcho".into(),
        message: Some(Arc::new(serde_json::json!({"message": "discard-me"}))),
        ..Default::default()
    };

    let decode_request = grpc_request(&format!("grpc://{}", server.addr), base.clone());
    let decode_response = handler
        .execute(&mut vu, &decode_request)
        .await
        .expect("response");
    assert_eq!(
        decode_response.status, 0,
        "status_text: {}",
        decode_response.status_text
    );
    let json: serde_json::Value = serde_json::from_slice(&decode_response.body).expect("json body");
    assert_eq!(json["message"], "discard-me");
    assert_eq!(decode_response.extras["message_count"], 1);
    assert!(decode_response.extras.get("messages").is_none());

    let discard_request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            discard_response_body: true,
            ..base
        },
    );
    let discard_response = handler
        .execute(&mut vu, &discard_request)
        .await
        .expect("response");
    assert_eq!(
        discard_response.status, 0,
        "status_text: {}",
        discard_response.status_text
    );
    assert!(discard_response.body.is_empty());
    assert_eq!(discard_response.extras["message_count"], 1);
    assert!(discard_response.extras.get("messages").is_none());
    // Parity: for a canonical encoder, discard's wire-length accounting
    // equals decode's re-encoded-length accounting.
    assert_eq!(
        discard_response.bytes_received,
        decode_response.bytes_received
    );
}

/// Server streaming in discard mode must still drain and count every frame
/// (status/timing parity with decode) while skipping the body entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_server_streaming_discard_counts_all_frames() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let base = GrpcRequest {
        proto_files: vec![proto_path].into(),
        service: "loadr.test.Echo".into(),
        method: "ServerStreamEcho".into(),
        message: Some(Arc::new(
            serde_json::json!({"message": "stream", "repeat": 3}),
        )),
        ..Default::default()
    };
    let decode_request = grpc_request(&format!("grpc://{}", server.addr), base.clone());
    let decode_response = handler
        .execute(&mut vu, &decode_request)
        .await
        .expect("response");
    assert_eq!(decode_response.extras["message_count"], 3);

    let discard_request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            discard_response_body: true,
            ..base
        },
    );
    let discard_response = handler
        .execute(&mut vu, &discard_request)
        .await
        .expect("response");
    assert_eq!(
        discard_response.status, 0,
        "status_text: {}",
        discard_response.status_text
    );
    assert_eq!(discard_response.extras["message_count"], 3);
    assert!(discard_response.body.is_empty());
    assert!(discard_response.extras.get("messages").is_none());
    assert_eq!(
        discard_response.bytes_received,
        decode_response.bytes_received
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
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
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

/// Run one call of `method` against a fresh echo server on the given
/// transport, with proto files on disk.
async fn run_grpc_shape(
    transport: GrpcTransport,
    method: &str,
    message: Option<serde_json::Value>,
    messages: Vec<serde_json::Value>,
) -> loadr_core::protocol::ProtocolResponse {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: method.into(),
            message: message.map(Arc::new),
            messages: Arc::new(messages),
            transport,
            ..Default::default()
        },
    );
    handler.execute(&mut vu, &request).await.expect("response")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_on_both_transports() {
    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        let response = run_grpc_shape(
            transport,
            "UnaryEcho",
            Some(serde_json::json!({"message": "hi grpc"})),
            Vec::new(),
        )
        .await;
        assert_eq!(
            response.status, 0,
            "{transport:?}: {}",
            response.status_text
        );
        assert!(!response.failed());
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "hi grpc");
        assert_eq!(response.extras["message_count"], 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_server_streaming_on_both_transports() {
    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        let response = run_grpc_shape(
            transport,
            "ServerStreamEcho",
            Some(serde_json::json!({"message": "stream", "repeat": 3})),
            Vec::new(),
        )
        .await;
        assert_eq!(
            response.status, 0,
            "{transport:?}: {}",
            response.status_text
        );
        assert_eq!(response.extras["message_count"], 3);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_client_streaming_on_both_transports() {
    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        let response = run_grpc_shape(
            transport,
            "ClientStreamEcho",
            None,
            vec![
                serde_json::json!({"message": "one"}),
                serde_json::json!({"message": "two"}),
                serde_json::json!({"message": "three"}),
            ],
        )
        .await;
        assert_eq!(
            response.status, 0,
            "{transport:?}: {}",
            response.status_text
        );
        // The echo server concatenates the messages; `index` is the count.
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "onetwothree");
        assert_eq!(json["index"], 3);
        assert_eq!(response.extras["message_count"], 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_bidi_on_both_transports() {
    for transport in [GrpcTransport::Channel, GrpcTransport::Raw] {
        let response = run_grpc_shape(
            transport,
            "BidiEcho",
            None,
            vec![
                serde_json::json!({"message": "a"}),
                serde_json::json!({"message": "b"}),
                serde_json::json!({"message": "c"}),
            ],
        )
        .await;
        assert_eq!(
            response.status, 0,
            "{transport:?}: {}",
            response.status_text
        );
        assert_eq!(response.extras["message_count"], 3);
        let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
        assert_eq!(json["message"], "c");
        assert_eq!(json["index"], 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_raw_via_reflection() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            reflection: true,
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "raw reflected"}))),
            transport: GrpcTransport::Raw,
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
    assert_eq!(json["message"], "raw reflected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_raw_unary_pooled_channels() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vus = [vu(), vu()];

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "raw pooled"}))),
            channel_pool_size: Some(2),
            transport: GrpcTransport::Raw,
            ..Default::default()
        },
    );

    // Each VU pins its resolved call to one successive raw pool slot, then
    // reuses the cached client.
    for vu in &mut vus {
        for _ in 0..2 {
            let response = handler.execute(vu, &request).await.expect("response");
            assert_eq!(response.status, 0, "status_text: {}", response.status_text);
            let json: serde_json::Value =
                serde_json::from_slice(&response.body).expect("json body");
            assert_eq!(json["message"], "raw pooled");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_raw_reconnects_after_server_restart() {
    let mut server = GrpcEchoServer::spawn().await.expect("grpc server");
    let addr = server.addr;
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{addr}"),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "before restart"}))),
            transport: GrpcTransport::Raw,
            ..Default::default()
        },
    );

    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);

    server.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The first post-shutdown call can race connection teardown; it must fail
    // either way.
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert!(
        response.failed(),
        "expected a failure right after shutdown, got: {}",
        response.status_text
    );

    // Once the closed connection has latched, failures are dial failures:
    // Unavailable (14) with `error: None`, exactly like the channel
    // transport's `connection failed` mapping.
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 14, "status_text: {}", response.status_text);
    assert!(response.error.is_none());
    assert!(
        response.status_text.contains("connection failed"),
        "status_text: {}",
        response.status_text
    );

    // Restart on the same port (the old listener may still be closing).
    let mut restarted = None;
    for _ in 0..50 {
        match GrpcEchoServer::spawn_on(addr).await {
            Ok(server) => {
                restarted = Some(server);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let _restarted = restarted.expect("respawn on the same address");

    // Calls succeed again once the dial cooldown lapses.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = handler.execute(&mut vu, &request).await.expect("response");
        if response.status == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "did not reconnect in time: {}",
            response.status_text
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_raw_tls_insecure_skip_verify() {
    let server = GrpcEchoServer::spawn_tls().await.expect("grpc tls server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let defaults = HttpDefaults {
        tls: TlsConfig {
            insecure_skip_verify: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let handler = GrpcHandler::new(&defaults, Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpcs://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "over tls"}))),
            transport: GrpcTransport::Raw,
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
    assert_eq!(json["message"], "over tls");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_raw_tls_with_ca_file() {
    let server = GrpcEchoServer::spawn_tls().await.expect("grpc tls server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, server.cert_pem().expect("cert pem")).expect("write ca");

    let defaults = HttpDefaults {
        tls: TlsConfig {
            ca_file: Some(ca_path),
            ..Default::default()
        },
        ..Default::default()
    };
    let handler = GrpcHandler::new(&defaults, Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpcs://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": "verified tls"}))),
            transport: GrpcTransport::Raw,
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
    assert_eq!(json["message"], "verified tls");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_rendered_unary_bytes_sent_and_payload_roundtrip() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let payload: Vec<u8> = (0..200u16).map(|i| (i % 251) as u8).collect();
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload);
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({
                "message": "parity",
                "payload": payload_b64.as_str(),
            }))),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);

    // Server-observed request parity: the echo copies the payload through.
    let json: serde_json::Value = serde_json::from_slice(&response.body).expect("json body");
    assert_eq!(json["message"], "parity");
    assert_eq!(json["payload"], payload_b64.as_str());

    // bytes_sent parity with the base `encoded_len() + 5` formula, using the
    // generated prost type as an independent oracle.
    let oracle = loadr_testserver::pb::EchoRequest {
        message: "parity".to_string(),
        payload,
        ..Default::default()
    };
    assert_eq!(response.bytes_sent, oracle.encoded_len() as u64 + 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_rendered_client_streaming_bytes_sent_parity() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let big = vec![7u8; 300];
    let big_b64 = base64::engine::general_purpose::STANDARD.encode(&big);
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path.clone()].into(),
            service: "loadr.test.Echo".into(),
            method: "ClientStreamEcho".into(),
            messages: Arc::new(vec![
                serde_json::json!({"message": "one"}),
                serde_json::json!({"message": "two", "payload": big_b64.as_str()}),
                serde_json::json!({}),
            ]),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);

    let expected: u64 = [
        loadr_testserver::pb::EchoRequest {
            message: "one".to_string(),
            ..Default::default()
        },
        loadr_testserver::pb::EchoRequest {
            message: "two".to_string(),
            payload: big,
            ..Default::default()
        },
        loadr_testserver::pb::EchoRequest::default(),
    ]
    .iter()
    .map(|m| m.encoded_len() as u64 + 5)
    .sum();
    assert_eq!(response.bytes_sent, expected);

    // No message at all still sends exactly one default (empty) frame.
    let empty = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            ..Default::default()
        },
    );
    let response = handler
        .execute(&mut vu, &empty)
        .await
        .expect("empty response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    assert_eq!(response.bytes_sent, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_rendered_message_schema_mismatch_error() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");

    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();

    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"message": 42}))),
            ..Default::default()
        },
    );
    let error = handler
        .execute(&mut vu, &request)
        .await
        .expect_err("schema mismatch");
    assert!(
        error
            .to_string()
            .contains("message does not match `loadr.test.EchoRequest`"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_protobuf_field_via_reflection() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            reflection: true,
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            message: Some(Arc::new(serde_json::json!({"responseCode": 20}))),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![protobuf_check(
                0,
                "code",
                Some(serde_json::json!(20)),
                true,
            )])),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.status, 0, "status_text: {}", response.status_text);
    assert!(response.grpc_protobuf_outcomes[0].pass);
    assert_eq!(response.grpc_protobuf_outcomes[0].actual_code, Some(20));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_protobuf_streaming_checks_last_response_message() {
    let server = GrpcEchoServer::spawn().await.expect("grpc server");
    let dir = tempfile::tempdir().expect("tempdir");
    let proto_path = dir.path().join("echo.proto");
    std::fs::write(&proto_path, ECHO_PROTO).expect("write proto");
    let handler = GrpcHandler::new(&HttpDefaults::default(), Path::new(".")).expect("handler");
    let mut vu = vu();
    let request = grpc_request(
        &format!("grpc://{}", server.addr),
        GrpcRequest {
            proto_files: vec![proto_path].into(),
            service: "loadr.test.Echo".into(),
            method: "BidiEcho".into(),
            messages: Arc::new(vec![
                serde_json::json!({"message": "first", "responseCode": 18}),
                serde_json::json!({"message": "last", "responseCode": 20}),
            ]),
            protobuf_only_response: true,
            protobuf_checks: Some(Arc::new(vec![protobuf_check(
                0,
                "code",
                Some(serde_json::json!(20)),
                true,
            )])),
            ..Default::default()
        },
    );
    let response = handler.execute(&mut vu, &request).await.expect("response");
    assert_eq!(response.extras["message_count"], 2);
    assert!(response.grpc_protobuf_outcomes[0].pass);
    assert_eq!(response.grpc_protobuf_outcomes[0].actual_code, Some(20));
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
