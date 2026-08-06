//! Dynamic gRPC handler: invokes any service/method without generated code.
//!
//! Message descriptors come either from `.proto` files compiled in-process
//! with protox, or from gRPC server reflection (v1). Calls go through
//! `tonic::client::Grpc` with a [`DynamicCodec`] that encodes/decodes
//! [`prost_reflect::DynamicMessage`] values, so all four call shapes (unary,
//! server-/client-streaming, bidi) work from plain JSON messages. When
//! nothing in the plan reads the response body, the codec discards each
//! frame instead of building a `DynamicMessage` for it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::{Buf as _, BufMut as _, Bytes};
use loadr_config::{GrpcTransport, HttpDefaults};
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{
    GrpcProtobufFieldCheck, GrpcProtobufFieldOutcome, GrpcRequest, PreparedRequest,
    ProtocolHandler, ProtocolResponse, Timings,
};
use loadr_core::vu::VuContext;
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MethodDescriptor, Value,
};
use tonic::client::GrpcService;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{Body as HttpBody, StdError};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
use tonic_reflection::pb::v1::ServerReflectionRequest;

use crate::grpc_transport::{RawChannel, TlsParams};
use crate::net::ms_since;

fn timeout_response(request: &PreparedRequest, start: Instant) -> ProtocolResponse {
    let elapsed = ms_since(start);
    ProtocolResponse {
        status: 0,
        error: Some(format!("request timed out after {:?}", request.timeout)),
        url: request.url.clone(),
        timings: Timings {
            duration_ms: elapsed,
            ..Timings::default()
        },
        ..ProtocolResponse::default()
    }
}

// ---------------------------------------------------------------------------
// Descriptor pool cache (global: compiling protos / reflection is expensive)
// ---------------------------------------------------------------------------

fn pool_cache() -> &'static Mutex<HashMap<String, DescriptorPool>> {
    static CACHE: OnceLock<Mutex<HashMap<String, DescriptorPool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// tower::buffer queue size per pooled channel (`LOADR_GRPC_BUFFER_SIZE`).
fn grpc_buffer_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("LOADR_GRPC_BUFFER_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4096)
            .max(1)
    })
}

fn cache_get(key: &str) -> Option<DescriptorPool> {
    pool_cache()
        .lock()
        .ok()
        .and_then(|map| map.get(key).cloned())
}

fn cache_put(key: String, pool: DescriptorPool) {
    if let Ok(mut map) = pool_cache().lock() {
        map.insert(key, pool);
    }
}

// ---------------------------------------------------------------------------
// Dynamic codec
// ---------------------------------------------------------------------------

/// tonic codec for [`DynamicMessage`] using the method's I/O descriptors.
#[derive(Clone)]
struct DynamicCodec {
    output: prost_reflect::MessageDescriptor,
    /// Skip decoding each response frame (still fully drained, so stream
    /// completion/status/timing parity holds). Set per-call from
    /// `GrpcRequest.discard_response_body` on the codec *clone* `execute`
    /// hands to `invoke` — never on the cached codec shared across calls to
    /// the same method, so a decode call and a discard call to the same
    /// (endpoint, service, method) share one `CachedCall` without fighting.
    discard: bool,
}

impl DynamicCodec {
    fn for_method(method: &MethodDescriptor) -> Self {
        DynamicCodec {
            output: method.output(),
            discard: false,
        }
    }
}

/// Encode one outbound message body. tonic adds the 5-byte gRPC frame header
/// (compression flag + length) outside the codec, hence the `+ 5` per frame
/// in `bytes_sent` accounting.
fn encode_message(message: &DynamicMessage) -> Bytes {
    Bytes::from(message.encode_to_vec())
}

/// Inbound message: fully decoded, or a discarded frame whose wire length is
/// kept for `bytes_received` accounting (nothing in the plan reads the
/// response body).
enum Inbound {
    Message(DynamicMessage),
    Skipped { encoded_len: usize },
}

impl Codec for DynamicCodec {
    // The encode side carries pre-encoded frames: `execute()` encodes each
    // message exactly once (rendered messages at request-build time, literal
    // ones via the per-VU cache); the decode side is unchanged.
    type Encode = Bytes;
    type Decode = Inbound;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.output.clone(),
            discard: self.discard,
        }
    }
}

struct DynamicEncoder;

impl Encoder for DynamicEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item);
        Ok(())
    }
}

struct DynamicDecoder {
    desc: prost_reflect::MessageDescriptor,
    discard: bool,
}

impl Decoder for DynamicDecoder {
    type Item = Inbound;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Inbound>, Status> {
        if self.discard {
            // `DecodeBuf` is exactly this frame's payload; the decoder must
            // fully consume it (its `advance` drives the shared stream
            // buffer's read cursor) or the next frame's header is misread.
            let encoded_len = src.remaining();
            src.advance(encoded_len);
            return Ok(Some(Inbound::Skipped { encoded_len }));
        }
        let mut message = DynamicMessage::new(self.desc.clone());
        message
            .merge(&mut *src)
            .map_err(|e| Status::internal(format!("failed to decode message: {e}")))?;
        Ok(Some(Inbound::Message(message)))
    }
}

// ---------------------------------------------------------------------------
// Channels: per-VU (default) and shared pool (opt-in), on either transport
// ---------------------------------------------------------------------------

/// A fixed set of lazily connected channels, handed out round-robin.
/// Cloning a `Channel` or `RawChannel` is cheap and shares the underlying
/// connection, so a small pool multiplexes arbitrarily many concurrent
/// streams.
struct RoundRobin<T: Clone> {
    items: Vec<T>,
    next: AtomicU64,
}

impl<T: Clone> RoundRobin<T> {
    fn next(&self) -> T {
        let i = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.items.len();
        self.items[i].clone()
    }

    fn len(&self) -> usize {
        self.items.len()
    }
}

/// Lazily connected gRPC channels per endpoint, stored per VU.
#[derive(Default)]
struct GrpcChannels {
    channels: HashMap<String, Channel>,
    /// VU-local memo of shared pools (the pools themselves are global).
    pools: HashMap<String, Arc<RoundRobin<Channel>>>,
    /// Raw hyper-h2 channels per endpoint (`transport: raw`).
    raws: HashMap<String, RawChannel>,
    /// VU-local memo of shared raw pools.
    raw_pools: HashMap<String, Arc<RoundRobin<RawChannel>>>,
    /// Resolved call state, one entry per distinct request shape this VU has
    /// executed (usually one). Linear scan on string keys — no per-request
    /// allocation. A VU runs one request at a time, so `&mut` access and the
    /// cached `Grpc` client's poll_ready discipline are safe; never share
    /// these across VUs.
    calls: Vec<CachedCall>,
}

/// Everything about a (endpoint, service, method) call that is invariant
/// across iterations: descriptors, path, codec, shape, a client pinned to the
/// VU's channel, and encoded bodies for literal messages.
struct CachedCall {
    identity: CachedCallIdentity,
    input_desc: prost_reflect::MessageDescriptor,
    path: http::uri::PathAndQuery,
    codec: DynamicCodec,
    shape: (bool, bool),
    client: CachedClient,
    /// Encoded literal message frames keyed by the message `Arc`'s pointer
    /// identity (stable for the run: the Arc is owned by the compiled plan).
    encoded: HashMap<usize, EncodedMessages>,
    metadata: CachedMetadata,
    /// Descriptor-resolved protobuf checks keyed by the compiled plan Arc's
    /// stable pointer identity. Resolution and expected-value validation are
    /// paid once per request shape and VU, never once per response.
    protobuf_check_sets: HashMap<usize, Vec<ResolvedProtobufFieldCheck>>,
}

struct CachedCallIdentity {
    /// The rendered URL is the lookup key. The parsed endpoint is retained so
    /// URL parsing is part of miss construction, never the cache-hit path.
    url: String,
    endpoint: String,
    tls: bool,
    service: Arc<str>,
    method_name: Arc<str>,
    reflection: bool,
    proto_files: Arc<[PathBuf]>,
    /// Part of the identity: the same files can resolve differently under
    /// different include roots.
    proto_includes: Arc<[PathBuf]>,
    pool_size: Option<usize>,
    transport: GrpcTransport,
}

struct EncodedMessages {
    frames: Vec<Bytes>,
    bytes_sent: u64,
}

#[derive(Default)]
struct CachedMetadata {
    /// Exact ordered inputs consumed by `build_metadata`: gRPC metadata first,
    /// followed by the request headers merged into the gRPC request.
    source: Vec<(String, String)>,
    map: MetadataMap,
}

#[derive(Debug, Clone)]
struct ResolvedProtobufFieldCheck {
    id: u32,
    field: FieldDescriptor,
    expected: Option<Value>,
    exists: bool,
}

impl CachedCall {
    fn matches(&self, url: &str, grpc: &GrpcRequest, transport: GrpcTransport) -> bool {
        self.identity.matches(url, grpc, transport)
    }
}

impl CachedCallIdentity {
    fn matches(&self, url: &str, grpc: &GrpcRequest, transport: GrpcTransport) -> bool {
        self.url == url
            && shared_eq(&self.service, &grpc.service)
            && shared_eq(&self.method_name, &grpc.method)
            && self.reflection == grpc.reflection
            && shared_eq(&self.proto_files, &grpc.proto_files)
            && shared_eq(&self.proto_includes, &grpc.proto_includes)
            && self.pool_size == grpc.channel_pool_size
            && self.transport == transport
    }
}

/// The client a request runs on, per its effective `transport`.
enum CallChannel {
    Buffered(Channel),
    Raw(RawChannel),
}

/// Per-VU client retained with the rest of the resolved call state. Keeping
/// both variants cached avoids rebuilding tonic's client facade on every call.
enum CachedClient {
    Buffered(tonic::client::Grpc<Channel>),
    Raw(tonic::client::Grpc<RawChannel>),
}

/// Global registry of shared pools, keyed by (endpoint, size).
type PoolRegistry<T> = RwLock<HashMap<(String, usize), Arc<RoundRobin<T>>>>;

/// Get-or-create the shared pool of `size` items for `endpoint` in `pools`.
/// Double-checked read→write; `build` does no I/O and no `.await` is held
/// across the lock, so building under the write lock is fine.
fn shared_pool<T: Clone>(
    pools: &PoolRegistry<T>,
    endpoint: &str,
    size: usize,
    build: impl Fn() -> Result<T, ProtocolError>,
) -> Result<Arc<RoundRobin<T>>, ProtocolError> {
    let key = (endpoint.to_string(), size);
    if let Some(pool) = pools
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
    {
        return Ok(pool.clone());
    }
    let mut map = pools.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(pool) = map.get(&key) {
        return Ok(pool.clone());
    }
    let mut items = Vec::with_capacity(size);
    for _ in 0..size {
        items.push(build()?);
    }
    let pool = Arc::new(RoundRobin {
        items,
        next: AtomicU64::new(0),
    });
    map.insert(key, pool.clone());
    Ok(pool)
}

/// Parse a `LOADR_GRPC_TRANSPORT` value (`channel` | `raw`, case-insensitive).
fn parse_transport(value: Option<&str>) -> Option<GrpcTransport> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    match value.to_ascii_lowercase().as_str() {
        "channel" => Some(GrpcTransport::Channel),
        "raw" => Some(GrpcTransport::Raw),
        other => {
            tracing::warn!("ignoring unknown LOADR_GRPC_TRANSPORT value `{other}`");
            None
        }
    }
}

fn shared_eq<T>(left: &Arc<T>, right: &Arc<T>) -> bool
where
    T: ?Sized + PartialEq,
{
    Arc::ptr_eq(left, right) || left.as_ref() == right.as_ref()
}

impl CachedMetadata {
    fn matches(&self, grpc: &GrpcRequest, headers: &[(String, String)]) -> bool {
        let source_len = grpc.metadata.len() + headers.len();
        self.source.len() == source_len
            && self
                .source
                .iter()
                .eq(grpc.metadata.iter().chain(headers.iter()))
    }

    fn for_request(
        &mut self,
        grpc: &GrpcRequest,
        headers: &[(String, String)],
    ) -> Result<MetadataMap, ProtocolError> {
        if !self.matches(grpc, headers) {
            let map = build_metadata(grpc, headers)?;
            self.source = grpc
                .metadata
                .iter()
                .chain(headers.iter())
                .cloned()
                .collect();
            self.map = map;
        }
        Ok(self.map.clone())
    }
}

fn resolve_protobuf_checks(
    output: &prost_reflect::MessageDescriptor,
    checks: &[GrpcProtobufFieldCheck],
) -> Result<Vec<ResolvedProtobufFieldCheck>, ProtocolError> {
    checks
        .iter()
        .map(|check| {
            let field = output.get_field_by_name(&check.field).ok_or_else(|| {
                ProtocolError::InvalidRequest(format!(
                    "protobuf field `{}` not found on `{}` (field names are exact and top-level)",
                    check.field,
                    output.full_name()
                ))
            })?;
            if field.is_list() || matches!(field.kind(), Kind::Message(_)) {
                return Err(ProtocolError::InvalidRequest(format!(
                    "protobuf field `{}` on `{}` must be a singular scalar or enum",
                    check.field,
                    output.full_name()
                )));
            }
            if check.group_failures && !protobuf_kind_is_groupable(&field.kind()) {
                return Err(ProtocolError::InvalidRequest(format!(
                    "failure_groups requires an integer or enum protobuf field; `{}` is {:?}",
                    check.field,
                    field.kind()
                )));
            }
            if !check.exists && !field.supports_presence() {
                return Err(ProtocolError::InvalidRequest(format!(
                    "protobuf field `{}` on `{}` always reads as its default (proto3 \
                     implicit presence), so `exists: false` can never pass",
                    check.field,
                    output.full_name()
                )));
            }
            let expected = check
                .equals
                .as_ref()
                .map(|value| protobuf_expected_value(&field, value))
                .transpose()?;
            Ok(ResolvedProtobufFieldCheck {
                id: check.id,
                field,
                expected,
                exists: check.exists,
            })
        })
        .collect()
}

fn protobuf_kind_is_groupable(kind: &Kind) -> bool {
    matches!(
        kind,
        Kind::Int32
            | Kind::Int64
            | Kind::Uint32
            | Kind::Uint64
            | Kind::Sint32
            | Kind::Sint64
            | Kind::Fixed32
            | Kind::Fixed64
            | Kind::Sfixed32
            | Kind::Sfixed64
            | Kind::Enum(_)
    )
}

fn protobuf_expected_value(
    field: &FieldDescriptor,
    value: &serde_json::Value,
) -> Result<Value, ProtocolError> {
    let invalid = || {
        ProtocolError::InvalidRequest(format!(
            "protobuf check value {value} does not match field `{}` ({:?})",
            field.name(),
            field.kind()
        ))
    };
    let result = match field.kind() {
        Kind::Double => value.as_f64().map(Value::F64),
        Kind::Float => value.as_f64().map(|v| Value::F32(v as f32)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => value
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(Value::I32),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => value.as_i64().map(Value::I64),
        Kind::Uint32 | Kind::Fixed32 => value
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .map(Value::U32),
        Kind::Uint64 | Kind::Fixed64 => value.as_u64().map(Value::U64),
        Kind::Bool => value.as_bool().map(Value::Bool),
        Kind::String => value.as_str().map(|v| Value::String(v.to_string())),
        Kind::Bytes => value.as_str().and_then(|v| {
            base64::engine::general_purpose::STANDARD
                .decode(v)
                .ok()
                .map(|bytes| Value::Bytes(bytes.into()))
        }),
        Kind::Enum(desc) => match value {
            serde_json::Value::String(name) => desc
                .get_value_by_name(name)
                .map(|v| Value::EnumNumber(v.number())),
            _ => value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Value::EnumNumber),
        },
        Kind::Message(_) => None,
    };
    result.ok_or_else(invalid)
}

fn evaluate_protobuf_checks(
    message: &DynamicMessage,
    checks: &[ResolvedProtobufFieldCheck],
) -> Vec<GrpcProtobufFieldOutcome> {
    checks
        .iter()
        .map(|check| {
            // Proto3 implicit-presence scalars are always semantically
            // present: an omitted wire value reads as its protobuf default.
            let present = !check.field.supports_presence() || message.has_field(&check.field);
            if !present {
                return GrpcProtobufFieldOutcome {
                    id: check.id,
                    pass: !check.exists,
                    detail: check
                        .exists
                        .then(|| format!("protobuf field `{}` is absent", check.field.name())),
                    actual_code: None,
                    missing: true,
                };
            }

            let actual = message.get_field(&check.field);
            let actual_code = protobuf_numeric_code(actual.as_ref());
            if !check.exists {
                return GrpcProtobufFieldOutcome {
                    id: check.id,
                    pass: false,
                    detail: Some(format!(
                        "protobuf field `{}` is present with value {}",
                        check.field.name(),
                        protobuf_value_brief(actual.as_ref())
                    )),
                    actual_code,
                    missing: false,
                };
            }
            let pass = check
                .expected
                .as_ref()
                .is_none_or(|expected| actual.as_ref() == expected);
            GrpcProtobufFieldOutcome {
                id: check.id,
                pass,
                detail: (!pass).then(|| {
                    format!(
                        "protobuf field `{}` expected {}, got {}",
                        check.field.name(),
                        protobuf_value_brief(
                            check
                                .expected
                                .as_ref()
                                .expect("failed equality has expected")
                        ),
                        protobuf_value_brief(actual.as_ref())
                    )
                }),
                actual_code,
                missing: false,
            }
        })
        .collect()
}

fn protobuf_numeric_code(value: &Value) -> Option<i64> {
    match value {
        Value::I32(v) => Some(i64::from(*v)),
        Value::I64(v) => Some(*v),
        Value::U32(v) => Some(i64::from(*v)),
        Value::U64(v) => i64::try_from(*v).ok(),
        Value::EnumNumber(v) => Some(i64::from(*v)),
        _ => None,
    }
}

/// Render a value for failure details. Bounded: a failing check must never
/// materialize an unbounded copy of a string/bytes payload.
fn protobuf_value_brief(value: &Value) -> String {
    const MAX_LEN: usize = 64;
    match value {
        Value::String(s) if s.len() > MAX_LEN => {
            let mut end = MAX_LEN;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            format!("{:?}… ({} bytes)", &s[..end], s.len())
        }
        Value::Bytes(b) if b.len() > MAX_LEN => format!("<{} bytes>", b.len()),
        other => format!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Dynamic gRPC protocol handler.
pub struct GrpcHandler {
    tls: Option<tonic::transport::ClientTlsConfig>,
    /// rustls config for `transport: raw` (ALPN `h2`). Unlike the tonic
    /// path it honors `insecure_skip_verify` and TLS version pinning.
    raw_tls: Arc<rustls::ClientConfig>,
    /// SNI override from `tls.server_name` (raw transport).
    server_name: Option<String>,
    /// Fleet-wide transport override from `LOADR_GRPC_TRANSPORT`.
    transport_override: Option<GrpcTransport>,
    base_dir: PathBuf,
    /// Shared channel pools. Consulted only on a VU's first pooled request
    /// per endpoint; hits are memoized per VU.
    channel_pools: PoolRegistry<Channel>,
    /// Same, for `transport: raw`.
    raw_pools: PoolRegistry<RawChannel>,
}

impl GrpcHandler {
    /// Build the handler; TLS material (for `grpcs://`) is loaded once.
    pub fn new(defaults: &HttpDefaults, base_dir: &Path) -> Result<Self, ProtocolError> {
        let tls_cfg = &defaults.tls;
        let mut tls = None;
        if tls_cfg.ca_file.is_some() || tls_cfg.cert_file.is_some() || tls_cfg.server_name.is_some()
        {
            let mut config = tonic::transport::ClientTlsConfig::new();
            if let Some(ca) = &tls_cfg.ca_file {
                let path = crate::tls::resolve_path(base_dir, ca);
                let pem = std::fs::read(&path).map_err(|e| {
                    ProtocolError::Tls(format!("cannot read ca_file {}: {e}", path.display()))
                })?;
                config = config.ca_certificate(tonic::transport::Certificate::from_pem(pem));
            }
            if let (Some(cert), Some(key)) = (&tls_cfg.cert_file, &tls_cfg.key_file) {
                let cert_pem = std::fs::read(crate::tls::resolve_path(base_dir, cert))
                    .map_err(|e| ProtocolError::Tls(format!("cannot read cert_file: {e}")))?;
                let key_pem = std::fs::read(crate::tls::resolve_path(base_dir, key))
                    .map_err(|e| ProtocolError::Tls(format!("cannot read key_file: {e}")))?;
                config = config.identity(tonic::transport::Identity::from_pem(cert_pem, key_pem));
            }
            if let Some(name) = &tls_cfg.server_name {
                config = config.domain_name(name.clone());
            }
            tls = Some(config);
        }
        if tls_cfg.insecure_skip_verify {
            tracing::warn!(
                "the grpc `channel` transport does not support insecure_skip_verify; \
                 ignored unless `transport: raw`"
            );
        }
        let raw_tls = Arc::new(crate::tls::client_config(
            tls_cfg,
            base_dir,
            vec![b"h2".to_vec()],
        )?);
        Ok(GrpcHandler {
            tls,
            raw_tls,
            server_name: tls_cfg.server_name.clone(),
            transport_override: parse_transport(
                std::env::var("LOADR_GRPC_TRANSPORT").ok().as_deref(),
            ),
            base_dir: base_dir.to_path_buf(),
            channel_pools: RwLock::new(HashMap::new()),
            raw_pools: RwLock::new(HashMap::new()),
        })
    }

    /// `grpc://` → `http://`, `grpcs://` → `https://`; strips any path.
    fn endpoint_uri(&self, raw: &str) -> Result<(String, bool), ProtocolError> {
        let url = url::Url::parse(raw)
            .map_err(|e| ProtocolError::InvalidRequest(format!("invalid url `{raw}`: {e}")))?;
        let (scheme, tls) = match url.scheme() {
            "grpc" | "http" => ("http", false),
            "grpcs" | "https" => ("https", true),
            other => {
                return Err(ProtocolError::InvalidRequest(format!(
                    "grpc handler cannot handle scheme `{other}`"
                )))
            }
        };
        let host = url
            .host_str()
            .ok_or_else(|| ProtocolError::InvalidRequest(format!("url `{raw}` has no host")))?;
        let port = url
            .port()
            .ok_or_else(|| ProtocolError::InvalidRequest(format!("url `{raw}` has no port")))?;
        Ok((format!("{scheme}://{host}:{port}"), tls))
    }

    /// Endpoint for a shared pooled channel: large fixed HTTP/2 windows (many
    /// concurrent streams share one connection) plus keepalive.
    fn pooled_endpoint(&self, endpoint: &str, tls: bool) -> Result<Endpoint, ProtocolError> {
        let mut ep = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| {
                ProtocolError::InvalidRequest(format!("invalid grpc endpoint `{endpoint}`: {e}"))
            })?
            .initial_stream_window_size(4 * 1024 * 1024)
            .initial_connection_window_size(8 * 1024 * 1024)
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .keep_alive_while_idle(true)
            // Many VUs share each pooled channel; widen the tower::buffer
            // request queue (tonic default 1024) so `ready()` doesn't stall
            // before the connection itself is the limit.
            .buffer_size(grpc_buffer_size());
        if tls {
            ep = ep
                .tls_config(self.tls.clone().unwrap_or_default())
                .map_err(|e| ProtocolError::Tls(format!("grpc tls config error: {e}")))?;
        }
        Ok(ep)
    }

    fn channel(
        &self,
        ctx: &mut VuContext,
        endpoint: &str,
        tls: bool,
        pool_size: Option<usize>,
    ) -> Result<Channel, ProtocolError> {
        if let Some(size) = pool_size {
            // Config validation rejects 0; clamp anyway so `% len` can never
            // divide by zero if a caller bypasses validation.
            let size = size.max(1);
            // Hot path: VU-local memo — no lock, no allocation.
            let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
            if let Some(pool) = state.pools.get(endpoint) {
                if pool.len() == size {
                    return Ok(pool.next());
                }
            }
            // First use (or size changed for this endpoint): global map.
            let pool = shared_pool(&self.channel_pools, endpoint, size, || {
                Ok(self.pooled_endpoint(endpoint, tls)?.connect_lazy())
            })?;
            let channel = pool.next();
            let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
            state.pools.insert(endpoint.to_string(), pool);
            return Ok(channel);
        }
        // ---- existing per-VU path: keep byte-for-byte as it is today ----
        let channels = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        if let Some(ch) = channels.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let mut ep = Endpoint::from_shared(endpoint.to_string()).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid grpc endpoint `{endpoint}`: {e}"))
        })?;
        if tls {
            let config = self.tls.clone().unwrap_or_default();
            ep = ep
                .tls_config(config)
                .map_err(|e| ProtocolError::Tls(format!("grpc tls config error: {e}")))?;
        }
        let channel = ep.connect_lazy();
        let channels = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        channels
            .channels
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// `transport: raw` counterpart of [`Self::channel`]: per-VU connection by
    /// default, shared round-robin pool when `pool_size` is set.
    fn raw(
        &self,
        ctx: &mut VuContext,
        endpoint: &str,
        tls: bool,
        pool_size: Option<usize>,
    ) -> Result<RawChannel, ProtocolError> {
        if let Some(size) = pool_size {
            let size = size.max(1);
            let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
            if let Some(pool) = state.raw_pools.get(endpoint) {
                if pool.len() == size {
                    return Ok(pool.next());
                }
            }
            let pool = shared_pool(&self.raw_pools, endpoint, size, || {
                self.raw_channel(endpoint, tls)
            })?;
            let raw = pool.next();
            let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
            state.raw_pools.insert(endpoint.to_string(), pool);
            return Ok(raw);
        }
        let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        if let Some(raw) = state.raws.get(endpoint) {
            return Ok(raw.clone());
        }
        let raw = self.raw_channel(endpoint, tls)?;
        let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        state.raws.insert(endpoint.to_string(), raw.clone());
        Ok(raw)
    }

    /// Build one raw channel handle for `endpoint` (no I/O; dials lazily).
    fn raw_channel(&self, endpoint: &str, tls: bool) -> Result<RawChannel, ProtocolError> {
        let tls_params = if tls {
            let url = url::Url::parse(endpoint).map_err(|e| {
                ProtocolError::InvalidRequest(format!("invalid grpc endpoint `{endpoint}`: {e}"))
            })?;
            Some(TlsParams {
                config: self.raw_tls.clone(),
                server_name: crate::tls::server_name(self.server_name.as_deref(), &url)?,
            })
        } else {
            None
        };
        RawChannel::new(endpoint, tls_params)
    }

    /// Compile `.proto` files in-process (cached globally per file set).
    fn pool_from_protos(&self, grpc: &GrpcRequest) -> Result<DescriptorPool, ProtocolError> {
        let files: Vec<PathBuf> = grpc
            .proto_files
            .iter()
            .map(|p| crate::tls::resolve_path(&self.base_dir, p))
            .collect();
        let mut includes: Vec<PathBuf> = grpc
            .proto_includes
            .iter()
            .map(|p| crate::tls::resolve_path(&self.base_dir, p))
            .collect();
        for file in &files {
            if let Some(parent) = file.parent() {
                if !includes.contains(&parent.to_path_buf()) {
                    includes.push(parent.to_path_buf());
                }
            }
        }
        if includes.is_empty() {
            includes.push(self.base_dir.clone());
        }

        let key = format!(
            "protos:{}::{}",
            files
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join("|"),
            includes
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join("|"),
        );
        if let Some(pool) = cache_get(&key) {
            return Ok(pool);
        }

        let fds = protox::compile(&files, &includes).map_err(|e| {
            ProtocolError::InvalidRequest(format!("failed to compile proto files: {e}"))
        })?;
        let pool = DescriptorPool::from_file_descriptor_set(fds).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid file descriptor set: {e}"))
        })?;
        cache_put(key, pool.clone());
        Ok(pool)
    }

    /// Fetch descriptors via gRPC server reflection v1 (cached per
    /// endpoint+symbol). The client is generic over the transport; on a
    /// cache hit it is dropped without any I/O.
    async fn pool_from_reflection<T>(
        &self,
        mut client: ServerReflectionClient<T>,
        endpoint: &str,
        symbol: &str,
    ) -> Result<DescriptorPool, String>
    where
        T: GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: HttpBody<Data = bytes::Bytes> + Send + 'static,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        let key = format!("reflection:{endpoint}::{symbol}");
        if let Some(pool) = cache_get(&key) {
            return Ok(pool);
        }

        let request = ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::FileContainingSymbol(symbol.to_string())),
        };
        let response = client
            .server_reflection_info(futures::stream::iter([request]))
            .await
            .map_err(|e| format!("server reflection call failed: {e}"))?;
        let mut stream = response.into_inner();
        let message = stream
            .message()
            .await
            .map_err(|e| format!("server reflection stream failed: {e}"))?
            .ok_or_else(|| "server reflection returned no response".to_string())?;

        let files = match message.message_response {
            Some(MessageResponse::FileDescriptorResponse(fd)) => fd.file_descriptor_proto,
            Some(MessageResponse::ErrorResponse(err)) => {
                return Err(format!(
                    "server reflection error {}: {}",
                    err.error_code, err.error_message
                ));
            }
            _ => return Err("unexpected server reflection response".to_string()),
        };

        let mut fds = prost_types::FileDescriptorSet::default();
        for bytes in files {
            let file = prost_types::FileDescriptorProto::decode(bytes.as_slice())
                .map_err(|e| format!("invalid file descriptor from reflection: {e}"))?;
            fds.file.push(file);
        }
        let pool = DescriptorPool::from_file_descriptor_set(fds)
            .map_err(|e| format!("invalid descriptor set from reflection: {e}"))?;
        cache_put(key, pool.clone());
        Ok(pool)
    }
}

#[async_trait]
impl ProtocolHandler for GrpcHandler {
    fn name(&self) -> &str {
        "grpc"
    }

    async fn execute(
        &self,
        ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let grpc = request.options.grpc.as_ref().ok_or_else(|| {
            ProtocolError::InvalidRequest("grpc request requires `grpc:` options".to_string())
        })?;
        let protobuf_checks = grpc
            .protobuf_checks
            .as_deref()
            .map_or(&[][..], Vec::as_slice);
        let transport = self.transport_override.unwrap_or(grpc.transport);

        // Look up by the raw rendered URL so repeated calls never parse it.
        // Templated URLs that render differently get distinct call entries.
        let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        let call_idx = state
            .calls
            .iter()
            .position(|c| c.matches(&request.url, grpc, transport));
        let parsed_endpoint = if call_idx.is_none() {
            Some(self.endpoint_uri(&request.url)?)
        } else {
            None
        };

        let start = Instant::now();

        // Resolve call state (descriptors, path, codec, client) — cached per
        // VU after the first iteration of each request shape.
        let call_idx = match call_idx {
            Some(idx) => idx,
            None => {
                let (endpoint, tls) = parsed_endpoint.expect("endpoint parsed on cache miss");
                let identity = CachedCallIdentity {
                    url: request.url.clone(),
                    endpoint,
                    tls,
                    service: grpc.service.clone(),
                    method_name: grpc.method.clone(),
                    reflection: grpc.reflection,
                    proto_files: grpc.proto_files.clone(),
                    proto_includes: grpc.proto_includes.clone(),
                    pool_size: grpc.channel_pool_size,
                    transport,
                };
                let channel = match transport {
                    GrpcTransport::Channel => CallChannel::Buffered(self.channel(
                        ctx,
                        &identity.endpoint,
                        identity.tls,
                        grpc.channel_pool_size,
                    )?),
                    GrpcTransport::Raw => CallChannel::Raw(self.raw(
                        ctx,
                        &identity.endpoint,
                        identity.tls,
                        grpc.channel_pool_size,
                    )?),
                };

                // Resolve the descriptor pool.
                let pool = if grpc.reflection {
                    let reflected = async {
                        match &channel {
                            CallChannel::Buffered(channel) => {
                                self.pool_from_reflection(
                                    ServerReflectionClient::new(channel.clone()),
                                    &identity.endpoint,
                                    &grpc.service,
                                )
                                .await
                            }
                            CallChannel::Raw(raw) => {
                                self.pool_from_reflection(
                                    ServerReflectionClient::with_origin(
                                        raw.clone(),
                                        raw.origin().clone(),
                                    ),
                                    &identity.endpoint,
                                    &grpc.service,
                                )
                                .await
                            }
                        }
                    };
                    match tokio::time::timeout(request.timeout, reflected).await {
                        Ok(Ok(pool)) => pool,
                        Ok(Err(message)) => {
                            return Ok(grpc_error_response(message, start, &request.url));
                        }
                        Err(_) => return Ok(timeout_response(request, start)),
                    }
                } else if !grpc.proto_files.is_empty() {
                    self.pool_from_protos(grpc)?
                } else {
                    return Err(ProtocolError::InvalidRequest(
                        "grpc request needs `proto_files` or `reflection: true`".to_string(),
                    ));
                };

                let service = pool.get_service_by_name(&grpc.service).ok_or_else(|| {
                    ProtocolError::InvalidRequest(format!("service `{}` not found", grpc.service))
                })?;
                let method = service
                    .methods()
                    .find(|m| m.name() == grpc.method.as_ref())
                    .ok_or_else(|| {
                        ProtocolError::InvalidRequest(format!(
                            "method `{}` not found on `{}`",
                            grpc.method, grpc.service
                        ))
                    })?;
                let path = http::uri::PathAndQuery::try_from(format!(
                    "/{}/{}",
                    service.full_name(),
                    method.name()
                ))
                .map_err(|e| ProtocolError::InvalidRequest(format!("invalid grpc path: {e}")))?;

                let client = match channel {
                    CallChannel::Buffered(channel) => {
                        CachedClient::Buffered(tonic::client::Grpc::new(channel))
                    }
                    CallChannel::Raw(raw) => {
                        let origin = raw.origin().clone();
                        CachedClient::Raw(tonic::client::Grpc::with_origin(raw, origin))
                    }
                };

                let cached = CachedCall {
                    identity,
                    input_desc: method.input(),
                    path,
                    codec: DynamicCodec::for_method(&method),
                    shape: (method.is_client_streaming(), method.is_server_streaming()),
                    client,
                    encoded: HashMap::new(),
                    metadata: CachedMetadata::default(),
                    protobuf_check_sets: HashMap::new(),
                };
                let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
                state.calls.push(cached);
                state.calls.len() - 1
            }
        };
        let state = ctx.extensions.get_or_insert_with(GrpcChannels::default);
        let cached = &mut state.calls[call_idx];

        let protobuf_checks_key = grpc
            .protobuf_checks
            .as_ref()
            .filter(|checks| !checks.is_empty())
            .map(|checks| Arc::as_ptr(checks) as usize);
        if let Some(key) = protobuf_checks_key {
            if !cached.protobuf_check_sets.contains_key(&key) {
                let checks = resolve_protobuf_checks(&cached.codec.output, protobuf_checks)?;
                cached.protobuf_check_sets.insert(key, checks);
            }
        }

        // Build outbound messages: pre-encoded bytes for literal messages
        // (cached by the message Arc's identity), dynamic otherwise.
        let raw: Vec<&serde_json::Value> = if !grpc.messages.is_empty() {
            grpc.messages.iter().collect()
        } else {
            grpc.message
                .iter()
                .map(|message| message.as_ref())
                .collect()
        };
        let literal_key = if grpc.message_literal {
            if !grpc.messages.is_empty() {
                Some(Arc::as_ptr(&grpc.messages) as usize)
            } else {
                grpc.message.as_ref().map(|m| Arc::as_ptr(m) as usize)
            }
        } else {
            None
        };
        let (outbound, bytes_sent) = match literal_key {
            Some(key) => {
                if !cached.encoded.contains_key(&key) {
                    let mut frames = Vec::with_capacity(raw.len());
                    let mut total: u64 = 0;
                    for json in raw {
                        let message = DynamicMessage::deserialize(cached.input_desc.clone(), json)
                            .map_err(|e| {
                                ProtocolError::InvalidRequest(format!(
                                    "message does not match `{}`: {e}",
                                    cached.input_desc.full_name()
                                ))
                            })?;
                        let bytes = encode_message(&message);
                        total += bytes.len() as u64 + 5;
                        frames.push(bytes);
                    }
                    cached.encoded.insert(
                        key,
                        EncodedMessages {
                            frames,
                            bytes_sent: total,
                        },
                    );
                }
                let enc = &cached.encoded[&key];
                (enc.frames.clone(), enc.bytes_sent)
            }
            None => {
                let mut outbound = Vec::with_capacity(raw.len().max(1));
                if raw.is_empty() {
                    outbound.push(encode_message(&DynamicMessage::new(
                        cached.input_desc.clone(),
                    )));
                } else {
                    for json in raw {
                        let message = DynamicMessage::deserialize(cached.input_desc.clone(), json)
                            .map_err(|e| {
                                ProtocolError::InvalidRequest(format!(
                                    "message does not match `{}`: {e}",
                                    cached.input_desc.full_name()
                                ))
                            })?;
                        outbound.push(encode_message(&message));
                    }
                }
                let bytes_sent: u64 = outbound.iter().map(|b| b.len() as u64 + 5).sum();
                (outbound, bytes_sent)
            }
        };

        let metadata = cached.metadata.for_request(grpc, &request.headers)?;
        let shape = cached.shape;
        let path = cached.path.clone();
        // Per-call flag on the clone, not the cached codec: a decode call and
        // a discard call to the same method share one `CachedCall` entry.
        let mut codec = cached.codec.clone();
        codec.discard = grpc.discard_response_body && protobuf_checks.is_empty();
        let outcome = match &mut cached.client {
            CachedClient::Buffered(client) => {
                let call = invoke(client, shape, outbound, path, codec, metadata);
                tokio::time::timeout(request.timeout, call).await
            }
            CachedClient::Raw(client) => {
                let call = invoke(client, shape, outbound, path, codec, metadata);
                tokio::time::timeout(request.timeout, call).await
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_) => return Ok(timeout_response(request, start)),
        };

        let elapsed = ms_since(start);
        let timings = Timings {
            waiting_ms: elapsed,
            duration_ms: elapsed,
            ..Timings::default()
        };

        match outcome {
            Ok(responses) => {
                let count = responses.len();
                // Parity with the pre-discard formula (`encoded_len + 5`
                // frame overhead per message); skip mode uses the wire
                // length seen by the decoder instead of a re-encoded length
                // (identical for canonical encoders, more accurate otherwise).
                let bytes_received: u64 = responses
                    .iter()
                    .map(|m| {
                        (match m {
                            Inbound::Message(m) => m.encoded_len(),
                            Inbound::Skipped { encoded_len } => *encoded_len,
                        }) as u64
                            + 5
                    })
                    .sum();
                let grpc_protobuf_outcomes = match (protobuf_checks_key, responses.last()) {
                    (Some(key), Some(Inbound::Message(message))) => evaluate_protobuf_checks(
                        message,
                        cached
                            .protobuf_check_sets
                            .get(&key)
                            .expect("protobuf check set resolved before invocation"),
                    ),
                    _ => Vec::new(),
                };
                let body: Bytes = if grpc.protobuf_only_response {
                    Bytes::new()
                } else {
                    match responses.last() {
                        Some(Inbound::Message(m)) => {
                            let json = serde_json::to_value(m).unwrap_or(serde_json::Value::Null);
                            serde_json::to_vec(&json).unwrap_or_default().into()
                        }
                        _ => Bytes::new(),
                    }
                };
                Ok(ProtocolResponse {
                    status: 0,
                    status_text: "OK".to_string(),
                    headers: Vec::new(),
                    body,
                    timings,
                    bytes_sent,
                    bytes_received,
                    protocol_version: "grpc".to_string(),
                    error: None,
                    url: request.url.clone(),
                    extras: serde_json::json!({
                        "message_count": count,
                    }),
                    grpc_protobuf_outcomes,
                })
            }
            Err(status) => {
                let code = status.code();
                Ok(ProtocolResponse {
                    status: code as i64,
                    status_text: format!("{:?}: {}", code, status.message()),
                    timings,
                    bytes_sent,
                    protocol_version: "grpc".to_string(),
                    url: request.url.clone(),
                    ..ProtocolResponse::default()
                })
            }
        }
    }
}

fn grpc_error_response(message: String, start: Instant, url: &str) -> ProtocolResponse {
    let elapsed = ms_since(start);
    ProtocolResponse {
        status: 0,
        error: Some(message),
        protocol_version: "grpc".to_string(),
        url: url.to_string(),
        timings: Timings {
            duration_ms: elapsed,
            ..Timings::default()
        },
        ..ProtocolResponse::default()
    }
}

fn build_metadata(
    grpc: &GrpcRequest,
    headers: &[(String, String)],
) -> Result<tonic::metadata::MetadataMap, ProtocolError> {
    let mut map = tonic::metadata::MetadataMap::new();
    for (name, value) in grpc.metadata.iter().chain(headers.iter()) {
        let key = MetadataKey::from_bytes(name.to_ascii_lowercase().as_bytes()).map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid metadata key `{name}`: {e}"))
        })?;
        let value: MetadataValue<_> = value.parse().map_err(|e| {
            ProtocolError::InvalidRequest(format!("invalid metadata value for `{name}`: {e}"))
        })?;
        map.append(key, value);
    }
    Ok(map)
}

/// Run the call in the right shape and collect every response message.
/// Generic over the transport: `Grpc<Channel>` and `Grpc<RawChannel>`
/// monomorphize to the same code paths.
async fn invoke<T>(
    client: &mut tonic::client::Grpc<T>,
    shape: (bool, bool),
    messages: Vec<Bytes>,
    path: http::uri::PathAndQuery,
    codec: DynamicCodec,
    metadata: tonic::metadata::MetadataMap,
) -> Result<Vec<Inbound>, Status>
where
    T: GrpcService<tonic::body::Body>,
    T::ResponseBody: HttpBody + Send + 'static,
    <T::ResponseBody as HttpBody>::Error: Into<StdError>,
{
    client.ready().await.map_err(|e| {
        let e: StdError = e.into();
        Status::unavailable(format!("connection failed: {e}"))
    })?;

    match shape {
        // Unary
        (false, false) => {
            let message = messages
                .into_iter()
                .next()
                .ok_or_else(|| Status::internal("missing request message"))?;
            let mut request = tonic::Request::new(message);
            *request.metadata_mut() = metadata;
            let response = client.unary(request, path, codec).await?;
            Ok(vec![response.into_inner()])
        }
        // Server streaming
        (false, true) => {
            let message = messages
                .into_iter()
                .next()
                .ok_or_else(|| Status::internal("missing request message"))?;
            let mut request = tonic::Request::new(message);
            *request.metadata_mut() = metadata;
            let response = client.server_streaming(request, path, codec).await?;
            collect_stream(response.into_inner()).await
        }
        // Client streaming
        (true, false) => {
            let mut request = tonic::Request::new(futures::stream::iter(messages));
            *request.metadata_mut() = metadata;
            let response = client.client_streaming(request, path, codec).await?;
            Ok(vec![response.into_inner()])
        }
        // Bidi streaming
        (true, true) => {
            let mut request = tonic::Request::new(futures::stream::iter(messages));
            *request.metadata_mut() = metadata;
            let response = client.streaming(request, path, codec).await?;
            collect_stream(response.into_inner()).await
        }
    }
}

async fn collect_stream(mut stream: tonic::Streaming<Inbound>) -> Result<Vec<Inbound>, Status> {
    let mut out = Vec::new();
    while let Some(message) = stream.message().await? {
        out.push(message);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadr_core::conditions::CompiledCondition;

    #[test]
    fn parse_transport_values() {
        assert_eq!(parse_transport(None), None);
        assert_eq!(parse_transport(Some("")), None);
        assert_eq!(parse_transport(Some("  ")), None);
        assert_eq!(parse_transport(Some("raw")), Some(GrpcTransport::Raw));
        assert_eq!(parse_transport(Some(" RAW ")), Some(GrpcTransport::Raw));
        assert_eq!(
            parse_transport(Some("Channel")),
            Some(GrpcTransport::Channel)
        );
        assert_eq!(parse_transport(Some("bogus")), None);
    }

    #[test]
    fn call_cache_identity_discriminates_raw_url_transport_and_pool_size() {
        let request = GrpcRequest {
            proto_files: vec![PathBuf::from("echo.proto")].into(),
            proto_includes: vec![PathBuf::from("protos")].into(),
            reflection: false,
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            channel_pool_size: Some(2),
            transport: GrpcTransport::Channel,
            ..GrpcRequest::default()
        };
        let identity = CachedCallIdentity {
            url: "grpc://127.0.0.1:50051".to_string(),
            endpoint: "http://127.0.0.1:50051".to_string(),
            tls: false,
            service: request.service.clone(),
            method_name: request.method.clone(),
            reflection: request.reflection,
            proto_files: request.proto_files.clone(),
            proto_includes: request.proto_includes.clone(),
            pool_size: request.channel_pool_size,
            transport: GrpcTransport::Channel,
        };

        assert!(identity.matches("grpc://127.0.0.1:50051", &request, GrpcTransport::Channel));
        assert!(!identity.matches(
            "grpc://127.0.0.1:50051/other",
            &request,
            GrpcTransport::Channel
        ));
        assert!(!identity.matches("grpc://127.0.0.1:50051", &request, GrpcTransport::Raw));

        let equivalent = GrpcRequest {
            proto_files: vec![PathBuf::from("echo.proto")].into(),
            proto_includes: vec![PathBuf::from("protos")].into(),
            service: "loadr.test.Echo".into(),
            method: "UnaryEcho".into(),
            channel_pool_size: Some(2),
            ..GrpcRequest::default()
        };
        assert!(!Arc::ptr_eq(&request.service, &equivalent.service));
        assert!(identity.matches(
            "grpc://127.0.0.1:50051",
            &equivalent,
            GrpcTransport::Channel
        ));

        let different_method = GrpcRequest {
            method: "OtherMethod".into(),
            ..equivalent
        };
        assert!(!identity.matches(
            "grpc://127.0.0.1:50051",
            &different_method,
            GrpcTransport::Channel
        ));

        let different_pool = GrpcRequest {
            channel_pool_size: Some(4),
            ..request
        };
        assert!(!identity.matches(
            "grpc://127.0.0.1:50051",
            &different_pool,
            GrpcTransport::Channel
        ));
    }

    #[test]
    fn metadata_cache_hits_and_rebuilds_for_alternating_inputs() {
        let first = GrpcRequest {
            metadata: vec![("X-Grpc".to_string(), "one".to_string())],
            ..GrpcRequest::default()
        };
        let first_headers = vec![("X-Header".to_string(), "alpha".to_string())];
        let second = GrpcRequest {
            metadata: vec![("X-Grpc".to_string(), "two".to_string())],
            ..GrpcRequest::default()
        };
        let second_headers = vec![("X-Header".to_string(), "beta".to_string())];
        let mut cache = CachedMetadata::default();

        assert!(!cache.matches(&first, &first_headers));
        let first_map = cache
            .for_request(&first, &first_headers)
            .expect("first metadata build");
        assert!(cache.matches(&first, &first_headers));
        assert_eq!(first_map.get("x-grpc").unwrap().to_str().unwrap(), "one");
        assert_eq!(
            first_map.get("x-header").unwrap().to_str().unwrap(),
            "alpha"
        );

        let first_source_ptr = cache.source.as_ptr();
        let hit = cache
            .for_request(&first, &first_headers)
            .expect("metadata cache hit");
        assert!(cache.matches(&first, &first_headers));
        assert_eq!(cache.source.as_ptr(), first_source_ptr);
        assert_eq!(hit.get("x-grpc").unwrap().to_str().unwrap(), "one");

        assert!(!cache.matches(&second, &second_headers));
        let second_map = cache
            .for_request(&second, &second_headers)
            .expect("second metadata build");
        assert!(cache.matches(&second, &second_headers));
        assert_ne!(cache.source.as_ptr(), first_source_ptr);
        assert_eq!(second_map.get("x-grpc").unwrap().to_str().unwrap(), "two");
        assert_eq!(
            second_map.get("x-header").unwrap().to_str().unwrap(),
            "beta"
        );

        let first_again = cache
            .for_request(&first, &first_headers)
            .expect("alternating metadata rebuild");
        assert!(cache.matches(&first, &first_headers));
        assert_eq!(first_again.get("x-grpc").unwrap().to_str().unwrap(), "one");
    }

    #[test]
    fn metadata_cache_keeps_valid_entry_after_parse_error() {
        let valid = GrpcRequest {
            metadata: vec![("x-valid".to_string(), "value".to_string())],
            ..GrpcRequest::default()
        };
        let invalid = GrpcRequest {
            metadata: vec![("bad key".to_string(), "value".to_string())],
            ..GrpcRequest::default()
        };
        let mut cache = CachedMetadata::default();

        cache.for_request(&valid, &[]).expect("valid metadata");
        let error = cache
            .for_request(&invalid, &[])
            .expect_err("invalid metadata key");
        assert!(error.to_string().contains("invalid metadata key `bad key`"));
        assert!(cache.matches(&valid, &[]));
        assert_eq!(cache.map.get("x-valid").unwrap().to_str().unwrap(), "value");
    }

    #[test]
    fn deserialize_by_ref_error_matches_by_value() {
        // execute() deserializes from &Value; the error surface must be
        // identical to the owned-Value deserialization it replaced.
        let pool = DescriptorPool::decode(loadr_testserver::FILE_DESCRIPTOR_SET)
            .expect("testserver descriptor pool");
        let desc = pool
            .get_message_by_name("loadr.test.EchoRequest")
            .expect("EchoRequest descriptor");
        let bad_by_ref = serde_json::json!({"message": 42});
        let bad_by_value = serde_json::json!({"message": 42});
        let by_ref =
            DynamicMessage::deserialize(desc.clone(), &bad_by_ref).expect_err("type mismatch");
        let by_value = DynamicMessage::deserialize(desc, bad_by_value).expect_err("type mismatch");
        assert_eq!(by_ref.to_string(), by_value.to_string());
    }

    const CORPUS_PROTO: &str = r#"syntax = "proto3";

package corpus;

enum Kind {
  KIND_UNSPECIFIED = 0;
  KIND_A = 1;
  KIND_B = 2;
}

message Inner {
  string name = 1;
  repeated int64 values = 2;
}

message Everything {
  double f_double = 1;
  float f_float = 2;
  int32 f_int32 = 3;
  int64 f_int64 = 4;
  uint32 f_uint32 = 5;
  uint64 f_uint64 = 6;
  sint32 f_sint32 = 7;
  sint64 f_sint64 = 8;
  fixed32 f_fixed32 = 9;
  fixed64 f_fixed64 = 10;
  sfixed32 f_sfixed32 = 11;
  sfixed64 f_sfixed64 = 12;
  bool f_bool = 13;
  string f_string = 14;
  bytes f_bytes = 15;
  Kind f_enum = 16;
  Inner f_message = 17;
  repeated string r_strings = 18;
  repeated Inner r_messages = 19;
  map<string, int32> m_counts = 20;
}
"#;

    fn everything_descriptor() -> prost_reflect::MessageDescriptor {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("corpus.proto");
        std::fs::write(&path, CORPUS_PROTO).expect("write corpus proto");
        let fds = protox::compile([path.as_path()], [dir.path()]).expect("compile corpus");
        let pool = DescriptorPool::from_file_descriptor_set(fds).expect("corpus pool");
        pool.get_message_by_name("corpus.Everything")
            .expect("Everything descriptor")
    }

    #[test]
    fn rendered_encode_parity_with_prost_message_encode() {
        // encode_message must yield byte-for-byte what the retired codec-time
        // prost::Message::encode produced for the same DynamicMessage.
        let desc = everything_descriptor();

        let json = serde_json::json!({
            "f_double": 1.25,
            "f_float": -0.5,
            "f_int32": -42,
            "f_int64": "-9007199254740993",
            "f_uint32": 4294967295u32,
            "f_uint64": "18446744073709551615",
            "f_sint32": -1,
            "f_sint64": "-2",
            "f_fixed32": 7,
            "f_fixed64": "8",
            "f_sfixed32": -7,
            "f_sfixed64": "-8",
            "f_bool": true,
            "f_string": "héllo ünicode",
            "f_bytes": "3q2+7w==",
            "f_enum": "KIND_B",
            "f_message": {"name": "inner", "values": ["1", "2", "3"]},
            "r_strings": ["a", "b", ""],
            "r_messages": [{"name": "x"}, {"name": "y", "values": ["9"]}],
            "m_counts": {"alpha": 1, "beta": 2},
        });
        let message = DynamicMessage::deserialize(desc, &json).expect("corpus deserialize");

        let mut oracle = Vec::new();
        prost::Message::encode(&message, &mut oracle).expect("oracle encode");
        assert!(!oracle.is_empty());
        assert_eq!(encode_message(&message), oracle);
    }

    #[test]
    fn rendered_encode_matches_generated_prost_type() {
        // Cross-implementation oracle: encoding a JSON-built DynamicMessage
        // must equal the prost-generated type's encoding of the same values.
        let pool = DescriptorPool::decode(loadr_testserver::FILE_DESCRIPTOR_SET)
            .expect("testserver descriptor pool");
        let desc = pool
            .get_message_by_name("loadr.test.EchoRequest")
            .expect("EchoRequest descriptor");
        let json = serde_json::json!({
            "message": "hi",
            "repeat": 3,
            "payload": "3q2+7w==",
        });
        let message = DynamicMessage::deserialize(desc, &json).expect("deserialize");
        let expected = loadr_testserver::pb::EchoRequest {
            message: "hi".to_string(),
            repeat: 3,
            payload: vec![0xde, 0xad, 0xbe, 0xef],
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(encode_message(&message), expected);
    }

    #[test]
    fn metadata_cache_rebuilds_when_only_headers_change() {
        // Literal gRPC metadata must not pin the cache: ordinary request
        // headers (templated, or replaced by beforeRequest) arrive through
        // `request.headers` and have to rebuild the merged MetadataMap.
        let grpc = GrpcRequest {
            metadata: vec![("x-static".to_string(), "1".to_string())],
            ..GrpcRequest::default()
        };
        let mut cache = CachedMetadata::default();

        let first = cache
            .for_request(&grpc, &[("x-h".to_string(), "alpha".to_string())])
            .expect("first build");
        assert_eq!(first.get("x-h").unwrap().to_str().unwrap(), "alpha");

        let second = cache
            .for_request(&grpc, &[("x-h".to_string(), "beta".to_string())])
            .expect("rebuild for changed header");
        assert_eq!(second.get("x-h").unwrap().to_str().unwrap(), "beta");
        assert_eq!(second.get("x-static").unwrap().to_str().unwrap(), "1");
    }

    fn echo_output_descriptor() -> prost_reflect::MessageDescriptor {
        let pool = DescriptorPool::decode(loadr_testserver::FILE_DESCRIPTOR_SET)
            .expect("test descriptor set");
        pool.get_message_by_name("loadr.test.EchoResponse")
            .expect("EchoResponse descriptor")
    }

    #[test]
    fn protobuf_check_rejects_non_scalar_fields_and_typed_mismatches() {
        let output = echo_output_descriptor();
        let bad_type = GrpcProtobufFieldCheck {
            id: 0,
            field: Arc::from("code"),
            equals: Some(serde_json::json!("not-a-number")),
            exists: true,
            group_failures: false,
        };
        let error = resolve_protobuf_checks(&output, &[bad_type]).expect_err("typed mismatch");
        assert!(error.to_string().contains("does not match field `code`"));

        let missing = GrpcProtobufFieldCheck {
            id: 0,
            field: Arc::from("nested.code"),
            equals: Some(serde_json::json!(0)),
            exists: true,
            group_failures: false,
        };
        let error = resolve_protobuf_checks(&output, &[missing]).expect_err("exact field name");
        assert!(error.to_string().contains("not found"));

        // Messages, maps, and repeated fields are rejected outright.
        let everything = everything_descriptor();
        for field in ["f_message", "m_counts", "r_strings"] {
            let non_scalar = GrpcProtobufFieldCheck {
                id: 0,
                field: Arc::from(field),
                equals: None,
                exists: true,
                group_failures: false,
            };
            let error =
                resolve_protobuf_checks(&everything, &[non_scalar]).expect_err("non-scalar");
            assert!(
                error
                    .to_string()
                    .contains("must be a singular scalar or enum"),
                "`{field}`: {error}"
            );
        }
    }

    #[test]
    fn protobuf_check_rejects_exists_false_on_implicit_presence_fields() {
        let output = echo_output_descriptor();
        let implicit = GrpcProtobufFieldCheck {
            id: 0,
            field: Arc::from("code"),
            equals: None,
            exists: false,
            group_failures: false,
        };
        let error = resolve_protobuf_checks(&output, &[implicit]).expect_err("implicit presence");
        assert!(error.to_string().contains("can never pass"));

        // Explicit presence (proto3 `optional`) keeps `exists: false` meaningful.
        let optional = GrpcProtobufFieldCheck {
            id: 0,
            field: Arc::from("owner_hint"),
            equals: None,
            exists: false,
            group_failures: false,
        };
        resolve_protobuf_checks(&output, &[optional]).expect("explicit presence resolves");
    }

    /// Focused response-path microbenchmark. Run explicitly with:
    ///
    /// `cargo test -p loadr-grpc grpc_response_path_benchmark --release -- --ignored --nocapture`
    ///
    /// The standard test harness does not expose allocator counters, so this
    /// reports latency and throughput. The cases share the same small message.
    /// No assertion — wall-clock comparisons are inherently noisy; eyeball the
    /// numbers.
    #[test]
    #[ignore = "manual response-path benchmark"]
    fn grpc_response_path_benchmark() {
        use std::hint::black_box;
        use std::time::Instant;

        const ITERATIONS: u64 = 200_000;
        let output = echo_output_descriptor();
        let code = output.get_field_by_name("code").expect("code field");
        let mut message = DynamicMessage::new(output.clone());
        message.set_field(&code, Value::U32(18));
        let wire = Bytes::from(message.encode_to_vec());

        let json_spec = loadr_config::Condition::Jsonpath {
            name: None,
            expression: "$.code".to_string(),
            equals: Some(serde_json::json!(0)),
            exists: None,
            on_failure: None,
        };
        let json_condition = CompiledCondition::compile(&json_spec).expect("compile JSONPath");

        let protobuf_spec = loadr_config::Condition::ProtobufField {
            name: None,
            field: "code".to_string(),
            equals: Some(serde_json::json!(0)),
            exists: None,
            failure_groups: None,
            on_failure: None,
        };
        let mut protobuf_condition =
            CompiledCondition::compile(&protobuf_spec).expect("compile protobuf condition");
        let check = protobuf_condition
            .bind_protobuf_check(0)
            .expect("bind protobuf condition");
        let resolved = resolve_protobuf_checks(&output, &[check]).expect("resolve check");

        fn measure(mut operation: impl FnMut(), iterations: u64) -> (f64, f64) {
            let started = Instant::now();
            for _ in 0..iterations {
                operation();
            }
            let elapsed = started.elapsed().as_secs_f64();
            (
                elapsed * 1e9 / iterations as f64,
                iterations as f64 / elapsed,
            )
        }

        let (discard_ns, discard_ops) = measure(
            || {
                let mut frame = wire.clone();
                let len = frame.remaining();
                frame.advance(len);
                black_box(len);
            },
            ITERATIONS,
        );
        let (json_ns, json_ops) = measure(
            || {
                let json = serde_json::to_value(&message).expect("dynamic JSON");
                let response = ProtocolResponse {
                    body: serde_json::to_vec(&json).expect("JSON bytes").into(),
                    ..Default::default()
                };
                black_box(json_condition.evaluate(&response).pass);
            },
            ITERATIONS,
        );
        let (protobuf_ns, protobuf_ops) = measure(
            || {
                let response = ProtocolResponse {
                    grpc_protobuf_outcomes: evaluate_protobuf_checks(&message, &resolved),
                    ..Default::default()
                };
                black_box(protobuf_condition.evaluate(&response).pass);
            },
            ITERATIONS,
        );

        println!("status-only discard: {discard_ns:.1} ns/op, {discard_ops:.0} ops/s");
        println!("JSONPath decode/check: {json_ns:.1} ns/op, {json_ops:.0} ops/s");
        println!("protobuf field check: {protobuf_ns:.1} ns/op, {protobuf_ops:.0} ops/s");
    }
}
