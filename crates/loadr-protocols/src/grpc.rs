//! Dynamic gRPC handler: invokes any service/method without generated code.
//!
//! Message descriptors come either from `.proto` files compiled in-process
//! with protox, or from gRPC server reflection (v1). Calls go through
//! `tonic::client::Grpc` with a [`DynamicCodec`] that encodes/decodes
//! [`prost_reflect::DynamicMessage`] values, so all four call shapes (unary,
//! server-/client-streaming, bidi) work from plain JSON messages.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use loadr_config::HttpDefaults;
use loadr_core::error::ProtocolError;
use loadr_core::protocol::{
    GrpcRequest, PreparedRequest, ProtocolHandler, ProtocolResponse, Timings,
};
use loadr_core::vu::VuContext;
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MethodDescriptor, ReflectMessage, Value,
};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
use tonic_reflection::pb::v1::ServerReflectionRequest;

use crate::net::ms_since;

// ---------------------------------------------------------------------------
// Descriptor pool cache (global: compiling protos / reflection is expensive)
// ---------------------------------------------------------------------------

fn pool_cache() -> &'static Mutex<HashMap<String, DescriptorPool>> {
    static CACHE: OnceLock<Mutex<HashMap<String, DescriptorPool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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
}

impl DynamicCodec {
    fn for_method(method: &MethodDescriptor) -> Self {
        DynamicCodec {
            output: method.output(),
        }
    }
}

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.output.clone(),
        }
    }
}

struct DynamicEncoder;

impl Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(&mut self, item: DynamicMessage, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst)
            .map_err(|e| Status::internal(format!("failed to encode message: {e}")))
    }
}

struct DynamicDecoder {
    desc: prost_reflect::MessageDescriptor,
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<DynamicMessage>, Status> {
        let mut message = DynamicMessage::new(self.desc.clone());
        message
            .merge(&mut *src)
            .map_err(|e| Status::internal(format!("failed to decode message: {e}")))?;
        Ok(Some(message))
    }
}

// ---------------------------------------------------------------------------
// Per-VU channels
// ---------------------------------------------------------------------------

/// Lazily connected tonic channels per endpoint, stored per VU.
#[derive(Default)]
struct GrpcChannels {
    channels: HashMap<String, Channel>,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Dynamic gRPC protocol handler.
pub struct GrpcHandler {
    tls: Option<tonic::transport::ClientTlsConfig>,
    base_dir: PathBuf,
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
            tracing::warn!("grpc transport does not support insecure_skip_verify; ignoring");
        }
        Ok(GrpcHandler {
            tls,
            base_dir: base_dir.to_path_buf(),
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

    fn channel(
        &self,
        ctx: &mut VuContext,
        endpoint: &str,
        tls: bool,
    ) -> Result<Channel, ProtocolError> {
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
    /// endpoint+symbol).
    async fn pool_from_reflection(
        &self,
        channel: Channel,
        endpoint: &str,
        symbol: &str,
    ) -> Result<DescriptorPool, String> {
        let key = format!("reflection:{endpoint}::{symbol}");
        if let Some(pool) = cache_get(&key) {
            return Ok(pool);
        }

        let mut client = ServerReflectionClient::new(channel);
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
        let (endpoint, tls) = self.endpoint_uri(&request.url)?;
        let channel = self.channel(ctx, &endpoint, tls)?;

        let start = Instant::now();

        // Resolve the descriptor pool.
        let pool = if grpc.reflection {
            match tokio::time::timeout(
                request.timeout,
                self.pool_from_reflection(channel.clone(), &endpoint, &grpc.service),
            )
            .await
            {
                Ok(Ok(pool)) => pool,
                Ok(Err(message)) => {
                    return Ok(grpc_error_response(message, start, &request.url));
                }
                Err(_) => return Ok(crate::http::timeout_response(request, start)),
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
            .find(|m| m.name() == grpc.method)
            .ok_or_else(|| {
                ProtocolError::InvalidRequest(format!(
                    "method `{}` not found on `{}`",
                    grpc.method, grpc.service
                ))
            })?;

        // Build input messages from JSON.
        let input_desc = method.input();
        let raw_messages: Vec<&serde_json::Value> = if !grpc.messages.is_empty() {
            grpc.messages.iter().collect()
        } else {
            grpc.message.iter().collect()
        };
        let mut messages = Vec::with_capacity(raw_messages.len().max(1));
        if raw_messages.is_empty() {
            messages.push(DynamicMessage::new(input_desc.clone()));
        } else {
            for json in raw_messages {
                let message = DynamicMessage::deserialize(input_desc.clone(), json.clone())
                    .map_err(|e| {
                        ProtocolError::InvalidRequest(format!(
                            "message does not match `{}`: {e}",
                            input_desc.full_name()
                        ))
                    })?;
                messages.push(message);
            }
        }
        for field in &grpc.binary_fields {
            let message = messages.get_mut(field.message_index).ok_or_else(|| {
                ProtocolError::InvalidRequest(format!(
                    "binary field targets missing gRPC message {}",
                    field.message_index
                ))
            })?;
            set_binary_field(message, &field.name, field.value.clone())?;
        }
        let bytes_sent: u64 = messages.iter().map(|m| m.encoded_len() as u64 + 5).sum();

        let path = http::uri::PathAndQuery::try_from(format!(
            "/{}/{}",
            service.full_name(),
            method.name()
        ))
        .map_err(|e| ProtocolError::InvalidRequest(format!("invalid grpc path: {e}")))?;
        let codec = DynamicCodec::for_method(&method);
        let metadata = build_metadata(grpc, &request.headers)?;

        let mut client = tonic::client::Grpc::new(channel);
        let shape = (method.is_client_streaming(), method.is_server_streaming());
        let call = invoke(&mut client, shape, messages, path, codec, metadata);
        let outcome = match tokio::time::timeout(request.timeout, call).await {
            Ok(outcome) => outcome,
            Err(_) => return Ok(crate::http::timeout_response(request, start)),
        };

        let elapsed = ms_since(start);
        let timings = Timings {
            waiting_ms: elapsed,
            duration_ms: elapsed,
            ..Timings::default()
        };

        match outcome {
            Ok(responses) => {
                let json: Vec<serde_json::Value> = responses
                    .iter()
                    .map(|m| serde_json::to_value(m).unwrap_or(serde_json::Value::Null))
                    .collect();
                let body = json
                    .last()
                    .map(|v| serde_json::to_vec(v).unwrap_or_default())
                    .unwrap_or_default();
                let bytes_received: u64 =
                    responses.iter().map(|m| m.encoded_len() as u64 + 5).sum();
                let count = json.len();
                Ok(ProtocolResponse {
                    status: 0,
                    status_text: "OK".to_string(),
                    headers: Vec::new(),
                    body: body.into(),
                    timings,
                    bytes_sent,
                    bytes_received,
                    protocol_version: "grpc".to_string(),
                    error: None,
                    url: request.url.clone(),
                    extras: serde_json::json!({
                        "messages": json,
                        "message_count": count,
                    }),
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

fn set_binary_field(
    message: &mut DynamicMessage,
    name: &str,
    value: bytes::Bytes,
) -> Result<(), ProtocolError> {
    let descriptor = message.descriptor();
    let field = descriptor
        .get_field_by_json_name(name)
        .or_else(|| descriptor.get_field_by_name(name))
        .ok_or_else(|| {
            ProtocolError::InvalidRequest(format!(
                "field `{name}` does not exist on `{}`",
                descriptor.full_name()
            ))
        })?;
    if field.is_list() || field.is_map() {
        return Err(ProtocolError::InvalidRequest(format!(
            "binary field `{name}` must be singular"
        )));
    }
    if !matches!(field.kind(), Kind::Bytes) {
        return Err(ProtocolError::InvalidRequest(format!(
            "binary placeholder targets non-bytes field `{name}`"
        )));
    }
    message.set_field(&field, Value::Bytes(value));
    Ok(())
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
async fn invoke(
    client: &mut tonic::client::Grpc<Channel>,
    shape: (bool, bool),
    messages: Vec<DynamicMessage>,
    path: http::uri::PathAndQuery,
    codec: DynamicCodec,
    metadata: tonic::metadata::MetadataMap,
) -> Result<Vec<DynamicMessage>, Status> {
    client
        .ready()
        .await
        .map_err(|e| Status::unavailable(format!("connection failed: {e}")))?;

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

async fn collect_stream(
    mut stream: tonic::Streaming<DynamicMessage>,
) -> Result<Vec<DynamicMessage>, Status> {
    let mut out = Vec::new();
    while let Some(message) = stream.message().await? {
        out.push(message);
    }
    Ok(out)
}
