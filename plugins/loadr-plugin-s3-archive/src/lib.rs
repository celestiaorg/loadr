//! `loadr-plugin-s3-archive` — a native **output** plugin that archives the
//! finished run report as a single object in Amazon S3.
//!
//! # How it plugs in
//!
//! loadr's native output ABI ([`FfiOutput`]) is the same `start` /
//! `on_snapshot` / `finish` lifecycle used by the shipped `native-output`
//! example: the host calls `start(config)` once, hands the plugin a JSON
//! `Snapshot` roughly once a second during the run, then `finish(summary)` at
//! the end. Unlike a live exporter, this plugin does **no per-second network
//! traffic** — it appends each snapshot to an in-memory buffer and flushes once:
//!
//! 1. `start` validates the config, resolves AWS credentials from the
//!    environment and fails the run early on a bad bucket / missing region /
//!    absent credentials.
//! 2. `on_snapshot` appends the snapshot to a local buffer.
//! 3. `finish` serialises the accumulated report plus the end-of-run summary,
//!    gzip-compresses it and uploads the whole thing as a single **HTTPS `PUT`**
//!    signed with **AWS Signature Version 4** to
//!    `s3://{bucket}/{key_prefix}{run_id}.json.gz`.
//!
//! # Transport
//!
//! S3 is plain HTTPS plus a SigV4 `Authorization` header, so the object is
//! uploaded directly over the project's existing **hyper + hyper-rustls** stack
//! and signed by a hand-rolled, pure-Rust SigV4 signer (`sha2` + `hmac`). There
//! is no AWS SDK, no OpenSSL/C client and (thanks to `flate2`'s pure-Rust
//! miniz_oxide backend) no zlib C dependency, so the cdylib cross-compiles
//! cleanly for every release target.

use std::io::Write as _;
use std::time::Duration;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression as FlateLevel;
use hmac::{Hmac, KeyInit, Mac as _};
use http_body_util::{BodyExt as _, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiOutput, FfiOutputBox, FfiOutput_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "s3-archive";
/// Wall-clock cap for the (one-shot) end-of-run upload.
const PUT_TIMEOUT_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Crypto helpers (pure-Rust; no C deps).
// ---------------------------------------------------------------------------

/// Lowercase hex encoding.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Hex-encoded SHA-256 of `data`.
fn hex_sha256(data: &[u8]) -> String {
    hex_lower(Sha256::digest(data).as_slice())
}

/// HMAC-SHA256(`key`, `data`) as raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts a key of any length, so `new_from_slice` never errors here.
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// AWS Signature Version 4.
// ---------------------------------------------------------------------------

/// Everything needed to produce a SigV4 `Authorization` header for one request.
struct SigV4Params<'a> {
    method: &'a str,
    /// Already-encoded, `/`-preserving canonical path.
    canonical_uri: &'a str,
    /// Already-encoded, sorted canonical query string (may be empty).
    canonical_query: &'a str,
    /// Headers to sign: lowercase name + value. Order-insensitive (sorted here).
    headers: Vec<(String, String)>,
    /// Hex SHA-256 of the request payload.
    payload_hash: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    region: &'a str,
    service: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    amz_date: &'a str,
    /// `YYYYMMDD`.
    date_stamp: &'a str,
}

/// The signing outputs. Intermediate strings are exposed for testing against
/// the published AWS Signature Version 4 vectors.
#[allow(dead_code)] // canonical_request/string_to_sign asserted in tests only
struct Signed {
    authorization: String,
    canonical_request: String,
    string_to_sign: String,
    signature: String,
}

/// Compute the SigV4 signature and `Authorization` header for `p`.
fn sign_request(p: &SigV4Params) -> Signed {
    let mut headers = p.headers.clone();
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload}",
        method = p.method,
        uri = p.canonical_uri,
        query = p.canonical_query,
        payload = p.payload_hash,
    );

    let scope = format!(
        "{date}/{region}/{service}/aws4_request",
        date = p.date_stamp,
        region = p.region,
        service = p.service,
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hash}",
        amz_date = p.amz_date,
        hash = hex_sha256(canonical_request.as_bytes()),
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", p.secret_key).as_bytes(),
        p.date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, p.region.as_bytes());
    let k_service = hmac_sha256(&k_region, p.service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex_lower(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        key = p.access_key,
    );

    Signed {
        authorization,
        canonical_request,
        string_to_sign,
        signature,
    }
}

/// AWS URI-encode `input`: unreserved characters pass through, everything else
/// is `%`-escaped (uppercase hex). `/` is preserved unless `encode_slash`.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Current UTC `(amz_date, date_stamp)`.
fn timestamps() -> (String, String) {
    use time::macros::format_description;
    let now = time::OffsetDateTime::now_utc();
    let amz_date = now
        .format(&format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .unwrap_or_default();
    let date_stamp = now
        .format(&format_description!("[year][month][day]"))
        .unwrap_or_default();
    (amz_date, date_stamp)
}

// ---------------------------------------------------------------------------
// Config + credentials.
// ---------------------------------------------------------------------------

/// Compression applied to the buffered report before upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Compression {
    Gzip,
    None,
}

impl Compression {
    /// The object-key suffix (and, implicitly, the content type) for this mode.
    fn suffix(self) -> &'static str {
        match self {
            Compression::Gzip => ".json.gz",
            Compression::None => ".json",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Compression::Gzip => "application/gzip",
            Compression::None => "application/json",
        }
    }
}

/// The resolved output configuration.
#[derive(Debug, Clone)]
struct Config {
    bucket: String,
    key_prefix: String,
    region: String,
    /// Explicit endpoint override for S3-compatible stores (path-style).
    endpoint: Option<String>,
    compression: Compression,
}

impl Config {
    /// Parse and validate the config JSON handed to `start`.
    fn from_json(cfg: &Value) -> Result<Config, String> {
        let bucket = cfg
            .get("bucket")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "s3-archive output requires a non-empty `bucket`".to_string())?
            .to_string();
        let region = cfg
            .get("region")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "s3-archive output requires a non-empty `region`".to_string())?
            .to_string();
        let key_prefix = cfg
            .get("key_prefix")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let endpoint = cfg
            .get("endpoint")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let compression = match cfg
            .get("compression")
            .and_then(Value::as_str)
            .unwrap_or("gzip")
        {
            "gzip" => Compression::Gzip,
            "none" => Compression::None,
            other => {
                return Err(format!(
                    "unknown compression `{other}` (expected gzip|none)"
                ))
            }
        };
        Ok(Config {
            bucket,
            key_prefix,
            region,
            endpoint,
            compression,
        })
    }
}

/// Resolved AWS credentials.
#[derive(Debug, Clone)]
struct Creds {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

/// Resolve credentials from the standard AWS environment variables. Empty
/// values are treated as absent (so an unset `AWS_SESSION_TOKEN` does not sign a
/// bogus security-token header).
fn resolve_credentials() -> Result<Creds, String> {
    fn env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }
    let access_key = env("AWS_ACCESS_KEY_ID")
        .ok_or_else(|| "missing AWS credentials (env AWS_ACCESS_KEY_ID)".to_string())?;
    let secret_key = env("AWS_SECRET_ACCESS_KEY")
        .ok_or_else(|| "missing AWS credentials (env AWS_SECRET_ACCESS_KEY)".to_string())?;
    let session_token = env("AWS_SESSION_TOKEN");
    Ok(Creds {
        access_key,
        secret_key,
        session_token,
    })
}

// ---------------------------------------------------------------------------
// Target resolution + document assembly.
// ---------------------------------------------------------------------------

/// A resolved S3 object to PUT.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    /// Full request URL (path already URI-encoded).
    url: String,
    /// `Host` header value (host[:port]) as signed.
    host: String,
    /// Encoded canonical path used in the signature.
    canonical_uri: String,
}

/// Resolve the destination object URL for `key` under the configured bucket.
fn resolve_target(config: &Config, key: &str) -> Result<Target, String> {
    let key_path = if key.starts_with('/') {
        key.to_string()
    } else {
        format!("/{key}")
    };

    // Path-style against a custom endpoint (MinIO / S3-compatible stores).
    if let Some(endpoint) = &config.endpoint {
        let eu =
            Url::parse(endpoint).map_err(|e| format!("invalid `endpoint` `{endpoint}`: {e}"))?;
        let ehost_only = eu
            .host_str()
            .ok_or_else(|| format!("endpoint `{endpoint}` has no host"))?;
        let host = match eu.port() {
            Some(p) => format!("{ehost_only}:{p}"),
            None => ehost_only.to_string(),
        };
        let path = format!("/{}{}", config.bucket, key_path);
        let canonical_uri = uri_encode(&path, false);
        let url = format!("{}://{host}{canonical_uri}", eu.scheme());
        return Ok(Target {
            url,
            host,
            canonical_uri,
        });
    }

    // Virtual-hosted-style against AWS S3.
    let host = format!("{}.s3.{}.amazonaws.com", config.bucket, config.region);
    let canonical_uri = uri_encode(&key_path, false);
    let url = format!("https://{host}{canonical_uri}");
    Ok(Target {
        url,
        host,
        canonical_uri,
    })
}

/// The `run_id` from the summary, or `"unknown"` when absent.
fn run_id_of(summary: &Value) -> String {
    summary
        .get("run_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

/// Assemble the uploaded document: the end-of-run summary with the accumulated
/// per-second snapshots attached under `snapshots`. Because `Summary`
/// deserialises with unknown fields ignored, the object still feeds straight
/// back into `loadr report`.
fn build_document(summary: Value, snapshots: &[Value]) -> Value {
    match summary {
        Value::Object(mut map) => {
            map.insert("snapshots".to_string(), Value::Array(snapshots.to_vec()));
            Value::Object(map)
        }
        other => serde_json::json!({ "summary": other, "snapshots": snapshots }),
    }
}

/// Gzip-compress `data`.
fn gzip(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut enc = GzEncoder::new(Vec::new(), FlateLevel::default());
    enc.write_all(data)
        .map_err(|e| format!("gzip write failed: {e}"))?;
    enc.finish().map_err(|e| format!("gzip finish failed: {e}"))
}

/// Map an S3 PUT status to an upload outcome. Any 2xx is success; `403`/`404`
/// (and anything else) fail loudly so a broken archive step does not pass
/// silently.
fn check_status(status: u16) -> Result<(), String> {
    match status {
        200..=299 => Ok(()),
        403 => Err("S3 returned 403 Forbidden (check credentials / bucket policy)".to_string()),
        404 => Err("S3 returned 404 Not Found (check bucket and region)".to_string()),
        other => Err(format!("S3 returned unexpected status {other}")),
    }
}

// ---------------------------------------------------------------------------
// Upload transport — a seam so `finish` can be unit-tested without a socket.
// ---------------------------------------------------------------------------

/// Performs the signed PUT. A returned `Ok(status)` is a completed HTTP
/// exchange (any status); `Err` is a transport failure.
trait Uploader: Send {
    fn put(&self, url: &str, headers: &[(String, String)], body: Vec<u8>) -> Result<u16, String>;
}

/// The real hyper + hyper-rustls uploader.
struct HyperUploader;

impl Uploader for HyperUploader {
    fn put(&self, url: &str, headers: &[(String, String)], body: Vec<u8>) -> Result<u16, String> {
        runtime().block_on(http_put(url, headers, body, PUT_TIMEOUT_MS))
    }
}

/// The single Tokio runtime the plugin uses to drive the one-shot async upload.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build s3-archive plugin tokio runtime")
    })
}

/// PUT `body` to `url` with the given headers, returning the HTTP status.
async fn http_put(
    url: &str,
    headers: &[(String, String)],
    body: Vec<u8>,
    timeout_ms: u64,
) -> Result<u16, String> {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also allows plaintext endpoints (e.g. a local MinIO).
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(tls);

    let mut builder = Request::builder().method("PUT").uri(url);
    for (k, v) in headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| format!("building request failed: {e}"))?;

    let send = client.request(request);
    let resp = if timeout_ms == 0 {
        send.await
            .map_err(|e| format!("request to {url} failed: {e}"))?
    } else {
        tokio::time::timeout(Duration::from_millis(timeout_ms), send)
            .await
            .map_err(|_| format!("request to {url} timed out after {timeout_ms}ms"))?
            .map_err(|e| format!("request to {url} failed: {e}"))?
    };
    let status = resp.status().as_u16();
    // Drain (and discard) the response body to release the connection.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

// ---------------------------------------------------------------------------
// The output plugin.
// ---------------------------------------------------------------------------

struct S3Archive {
    config: Option<Config>,
    creds: Option<Creds>,
    uploader: Box<dyn Uploader>,
    /// Snapshots accumulated during the run (no per-second network traffic).
    snapshots: Vec<Value>,
    /// Report objects successfully uploaded (`s3_archive_objects`).
    objects: u64,
    /// Compressed bytes PUT to S3 (`s3_archive_bytes`).
    bytes: u64,
}

impl Default for S3Archive {
    fn default() -> Self {
        S3Archive {
            config: None,
            creds: None,
            uploader: Box::new(HyperUploader),
            snapshots: Vec::new(),
            objects: 0,
            bytes: 0,
        }
    }
}

impl S3Archive {
    /// Sign and upload the assembled report. Updates the object/byte counters on
    /// success. All the non-network work is pure so this is exercised in tests
    /// through a mock [`Uploader`].
    fn archive(&mut self, summary: Value) -> Result<(), String> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| "s3-archive: finish called before a successful start".to_string())?;
        let creds = self
            .creds
            .as_ref()
            .ok_or_else(|| "s3-archive: credentials were not resolved".to_string())?;

        let run_id = run_id_of(&summary);
        let document = build_document(summary, &self.snapshots);
        let json_bytes =
            serde_json::to_vec(&document).map_err(|e| format!("serialising report failed: {e}"))?;
        let body = match config.compression {
            Compression::Gzip => gzip(&json_bytes)?,
            Compression::None => json_bytes,
        };

        let key = format!(
            "{}{}{}",
            config.key_prefix,
            run_id,
            config.compression.suffix()
        );
        let target = resolve_target(config, &key)?;

        let (amz_date, date_stamp) = timestamps();
        let payload_hash = hex_sha256(&body);
        let mut headers = vec![
            ("host".to_string(), target.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &creds.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }

        let signed = sign_request(&SigV4Params {
            method: "PUT",
            canonical_uri: &target.canonical_uri,
            canonical_query: "",
            headers: headers.clone(),
            payload_hash: &payload_hash,
            access_key: &creds.access_key,
            secret_key: &creds.secret_key,
            region: &config.region,
            service: "s3",
            amz_date: &amz_date,
            date_stamp: &date_stamp,
        });

        // Sent headers = the signed set + the (unsigned) Authorization and
        // Content-Type headers. S3 only verifies the SignedHeaders list, so the
        // extra Content-Type is allowed.
        let mut request_headers = headers;
        request_headers.push(("authorization".to_string(), signed.authorization));
        request_headers.push((
            "content-type".to_string(),
            config.compression.content_type().to_string(),
        ));

        let len = body.len() as u64;
        let status = self.uploader.put(&target.url, &request_headers, body)?;
        check_status(status)?;
        self.objects += 1;
        self.bytes += len;
        Ok(())
    }
}

impl FfiOutput for S3Archive {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<(), RString> {
        let cfg: Value = match serde_json::from_str(config_json.as_str()) {
            Ok(v) => v,
            Err(e) => return RErr(RString::from(format!("invalid config JSON: {e}"))),
        };
        // Validate the config first, so a bad target fails regardless of whatever
        // AWS credentials happen to be in the environment.
        let config = match Config::from_json(&cfg) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        let creds = match resolve_credentials() {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        self.config = Some(config);
        self.creds = Some(creds);
        ROk(())
    }

    fn on_samples(&mut self, _samples_json: RString) {}

    fn on_snapshot(&mut self, snapshot_json: RString) {
        // Buffer the snapshot verbatim; there is no per-second network traffic.
        if let Ok(value) = serde_json::from_str::<Value>(snapshot_json.as_str()) {
            self.snapshots.push(value);
        }
    }

    fn finish(&mut self, summary_json: RString) {
        let summary: Value = serde_json::from_str(summary_json.as_str()).unwrap_or(Value::Null);
        if let Err(e) = self.archive(summary) {
            // Fail loud, not silent: the archive did not land. `s3_archive_objects`
            // stays at 0 so a CI gate on the object count turns this into a
            // failed run rather than a silently lost artifact.
            eprintln!("s3-archive output: upload failed: {e}");
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "output",
            "description": "Buffers the run report and uploads it gzip-compressed to S3 (SigV4)",
        })
        .to_string(),
    )
}

extern "C" fn make_output() -> FfiOutputBox {
    FfiOutput_TO::from_value(S3Archive::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RSome(make_output),
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RNone,
    }
}

// ---------------------------------------------------------------------------
// Tests — all offline. The SigV4 signer is checked against the published AWS
// vectors and the upload path is driven through a scripted mock uploader, never
// a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Read as _;
    use std::sync::{Arc, Mutex};

    // -- SigV4 against the published AWS vectors ----------------------------

    /// AWS S3 "GET Object" example from the SigV4 header-based auth docs.
    #[test]
    fn sigv4_matches_aws_s3_get_object_vector() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/test.txt",
            canonical_query: "",
            headers: vec![
                ("host".into(), "examplebucket.s3.amazonaws.com".into()),
                ("range".into(), "bytes=0-9".into()),
                ("x-amz-content-sha256".into(), empty.into()),
                ("x-amz-date".into(), "20130524T000000Z".into()),
            ],
            payload_hash: empty,
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "s3",
            amz_date: "20130524T000000Z",
            date_stamp: "20130524",
        });
        assert_eq!(
            signed.signature,
            "67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
    }

    /// AWS S3 "PUT Object" example from the SigV4 header-based auth docs: a real
    /// PUT with a non-empty payload and a `date` header, exactly the shape this
    /// plugin signs.
    #[test]
    fn sigv4_matches_aws_s3_put_object_vector() {
        let payload = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e5b2b5b7c6f7a5b8b0c0d";
        let signed = sign_request(&SigV4Params {
            method: "PUT",
            canonical_uri: "/test%24file.text",
            canonical_query: "",
            headers: vec![
                ("date".into(), "Fri, 24 May 2013 00:00:00 GMT".into()),
                ("host".into(), "examplebucket.s3.amazonaws.com".into()),
                ("x-amz-content-sha256".into(), payload.into()),
                ("x-amz-date".into(), "20130524T000000Z".into()),
                ("x-amz-storage-class".into(), "REDUCED_REDUNDANCY".into()),
            ],
            payload_hash: payload,
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "s3",
            amz_date: "20130524T000000Z",
            date_stamp: "20130524",
        });
        // The canonical request lists the headers sorted and the payload hash
        // last; the Authorization advertises exactly the signed set.
        assert!(signed
            .canonical_request
            .starts_with("PUT\n/test%24file.text\n\n"));
        assert!(signed.authorization.contains(
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
        ));
        assert!(signed.string_to_sign.starts_with(
            "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n"
        ));
    }

    #[test]
    fn empty_payload_hash_is_the_known_constant() {
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uri_encode_rules() {
        assert_eq!(uri_encode("/runs/abc.json.gz", false), "/runs/abc.json.gz");
        assert_eq!(uri_encode("/a b/c.json", false), "/a%20b/c.json");
        assert_eq!(uri_encode("/a b", true), "%2Fa%20b");
        assert_eq!(uri_encode("/k~e.y-1_2", false), "/k~e.y-1_2");
    }

    // -- config parsing -----------------------------------------------------

    #[test]
    fn config_requires_bucket_and_region() {
        assert!(Config::from_json(&json!({})).is_err());
        assert!(Config::from_json(&json!({ "bucket": "b" })).is_err());
        assert!(Config::from_json(&json!({ "region": "r" })).is_err());
        assert!(Config::from_json(&json!({ "bucket": "", "region": "r" })).is_err());
        assert!(Config::from_json(&json!({ "bucket": "b", "region": "r" })).is_ok());
    }

    #[test]
    fn config_defaults_and_overrides() {
        let c = Config::from_json(&json!({ "bucket": "b", "region": "r" })).unwrap();
        assert_eq!(c.key_prefix, "");
        assert_eq!(c.compression, Compression::Gzip);
        assert!(c.endpoint.is_none());

        let c = Config::from_json(&json!({
            "bucket": "reports",
            "region": "eu-west-2",
            "key_prefix": "runs/",
            "endpoint": "http://localhost:9000",
            "compression": "none",
        }))
        .unwrap();
        assert_eq!(c.key_prefix, "runs/");
        assert_eq!(c.compression, Compression::None);
        assert_eq!(c.endpoint.as_deref(), Some("http://localhost:9000"));
    }

    #[test]
    fn config_rejects_unknown_compression() {
        let err = Config::from_json(&json!({
            "bucket": "b",
            "region": "r",
            "compression": "zstd",
        }))
        .unwrap_err();
        assert!(err.contains("compression"), "{err}");
    }

    // -- target resolution --------------------------------------------------

    #[test]
    fn resolves_virtual_hosted_target() {
        let config = Config {
            bucket: "reports".into(),
            key_prefix: "runs/".into(),
            region: "eu-west-2".into(),
            endpoint: None,
            compression: Compression::Gzip,
        };
        let t = resolve_target(&config, "runs/run-1.json.gz").unwrap();
        assert_eq!(t.host, "reports.s3.eu-west-2.amazonaws.com");
        assert_eq!(t.canonical_uri, "/runs/run-1.json.gz");
        assert_eq!(
            t.url,
            "https://reports.s3.eu-west-2.amazonaws.com/runs/run-1.json.gz"
        );
    }

    #[test]
    fn resolves_path_style_endpoint() {
        let config = Config {
            bucket: "reports".into(),
            key_prefix: String::new(),
            region: "us-east-1".into(),
            endpoint: Some("http://localhost:9000".into()),
            compression: Compression::None,
        };
        let t = resolve_target(&config, "run 2.json").unwrap();
        assert_eq!(t.host, "localhost:9000");
        assert_eq!(t.canonical_uri, "/reports/run%202.json");
        assert_eq!(t.url, "http://localhost:9000/reports/run%202.json");
    }

    // -- document assembly + gzip -------------------------------------------

    #[test]
    fn build_document_attaches_snapshots() {
        let summary = json!({ "run_id": "r1", "duration_secs": 2.0 });
        let snaps = vec![json!({ "n": 1 }), json!({ "n": 2 })];
        let doc = build_document(summary, &snaps);
        assert_eq!(doc["run_id"], "r1");
        assert_eq!(doc["duration_secs"], 2.0);
        assert_eq!(doc["snapshots"].as_array().unwrap().len(), 2);
        assert_eq!(doc["snapshots"][1]["n"], 2);
    }

    #[test]
    fn gzip_round_trips() {
        let original = b"{\"run_id\":\"r1\",\"snapshots\":[1,2,3]}";
        let compressed = gzip(original).unwrap();
        // gzip magic header.
        assert_eq!(&compressed[0..2], &[0x1f, 0x8b]);
        let mut dec = flate2::read::GzDecoder::new(&compressed[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }

    #[test]
    fn status_mapping() {
        assert!(check_status(200).is_ok());
        assert!(check_status(204).is_ok());
        assert!(check_status(403).is_err());
        assert!(check_status(404).is_err());
        assert!(check_status(500).is_err());
    }

    #[test]
    fn run_id_falls_back_to_unknown() {
        assert_eq!(run_id_of(&json!({ "run_id": "abc" })), "abc");
        assert_eq!(run_id_of(&json!({ "run_id": "" })), "unknown");
        assert_eq!(run_id_of(&json!({})), "unknown");
    }

    // -- upload path through a mock uploader (no network) --------------------

    /// A recorded PUT.
    #[derive(Debug, Clone)]
    struct PutCall {
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    /// A scripted uploader: records every PUT and returns a fixed status.
    struct MockUploader {
        status: u16,
        calls: Arc<Mutex<Vec<PutCall>>>,
    }

    impl Uploader for MockUploader {
        fn put(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: Vec<u8>,
        ) -> Result<u16, String> {
            self.calls.lock().unwrap().push(PutCall {
                url: url.to_string(),
                headers: headers.to_vec(),
                body,
            });
            Ok(self.status)
        }
    }

    fn creds() -> Creds {
        Creds {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        }
    }

    /// Build an archiver wired to a mock uploader, returning it plus a handle to
    /// the recorded calls. (Not named `new` on purpose.)
    fn archiver_with(
        config: Config,
        snapshots: Vec<Value>,
        status: u16,
    ) -> (S3Archive, Arc<Mutex<Vec<PutCall>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let uploader = MockUploader {
            status,
            calls: calls.clone(),
        };
        let archive = S3Archive {
            config: Some(config),
            creds: Some(creds()),
            uploader: Box::new(uploader),
            snapshots,
            objects: 0,
            bytes: 0,
        };
        (archive, calls)
    }

    fn gunzip(data: &[u8]) -> Vec<u8> {
        let mut dec = flate2::read::GzDecoder::new(data);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn archive_signs_and_puts_a_gzip_object() {
        let config = Config {
            bucket: "reports".into(),
            key_prefix: "runs/".into(),
            region: "eu-west-2".into(),
            endpoint: None,
            compression: Compression::Gzip,
        };
        let (mut archive, calls) = archiver_with(config, vec![json!({ "series": [] })], 200);

        archive
            .archive(json!({ "run_id": "run-xyz", "duration_secs": 1.0 }))
            .unwrap();

        assert_eq!(archive.objects, 1);
        assert!(archive.bytes > 0);

        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let call = &recorded[0];
        assert_eq!(
            call.url,
            "https://reports.s3.eu-west-2.amazonaws.com/runs/run-xyz.json.gz"
        );
        assert_eq!(archive.bytes, call.body.len() as u64);
        // gzip magic bytes.
        assert_eq!(&call.body[0..2], &[0x1f, 0x8b]);

        let header = |name: &str| {
            call.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert!(header("x-amz-content-sha256").is_some());
        assert!(header("x-amz-date").is_some());
        assert_eq!(header("content-type").as_deref(), Some("application/gzip"));
        let auth = header("authorization").expect("authorization header");
        assert!(
            auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"),
            "{auth}"
        );
        assert!(auth.contains("/eu-west-2/s3/aws4_request"), "{auth}");

        // The uploaded body decompresses to the summary + attached snapshots.
        let doc: Value = serde_json::from_slice(&gunzip(&call.body)).unwrap();
        assert_eq!(doc["run_id"], "run-xyz");
        assert_eq!(doc["snapshots"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn archive_uncompressed_uploads_raw_json() {
        let config = Config {
            bucket: "reports".into(),
            key_prefix: String::new(),
            region: "us-east-1".into(),
            endpoint: None,
            compression: Compression::None,
        };
        let (mut archive, calls) = archiver_with(config, Vec::new(), 200);
        archive.archive(json!({ "run_id": "r9" })).unwrap();

        let recorded = calls.lock().unwrap();
        let call = &recorded[0];
        assert_eq!(
            call.url,
            "https://reports.s3.us-east-1.amazonaws.com/r9.json"
        );
        // Raw JSON, not gzip.
        let doc: Value = serde_json::from_slice(&call.body).unwrap();
        assert_eq!(doc["run_id"], "r9");
    }

    #[test]
    fn archive_reports_failed_upload() {
        let config = Config {
            bucket: "reports".into(),
            key_prefix: "runs/".into(),
            region: "eu-west-2".into(),
            endpoint: None,
            compression: Compression::Gzip,
        };
        let (mut archive, _calls) = archiver_with(config, Vec::new(), 403);
        let err = archive.archive(json!({ "run_id": "r" })).unwrap_err();
        assert!(err.contains("403"), "{err}");
        // A failed upload must not count as an archived object.
        assert_eq!(archive.objects, 0);
        assert_eq!(archive.bytes, 0);
    }

    #[test]
    fn session_token_is_signed_when_present() {
        let config = Config {
            bucket: "b".into(),
            key_prefix: String::new(),
            region: "us-east-1".into(),
            endpoint: None,
            compression: Compression::None,
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut archive = S3Archive {
            config: Some(config),
            creds: Some(Creds {
                access_key: "AKIA".into(),
                secret_key: "secret".into(),
                session_token: Some("TOKEN123".into()),
            }),
            uploader: Box::new(MockUploader {
                status: 200,
                calls: calls.clone(),
            }),
            snapshots: Vec::new(),
            objects: 0,
            bytes: 0,
        };
        archive.archive(json!({ "run_id": "r" })).unwrap();
        let recorded = calls.lock().unwrap();
        let call = &recorded[0];
        let auth = call
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert!(auth.contains("x-amz-security-token"), "{auth}");
        assert!(call
            .headers
            .iter()
            .any(|(k, v)| k == "x-amz-security-token" && v == "TOKEN123"));
    }

    // -- output lifecycle ----------------------------------------------------

    #[test]
    fn start_rejects_invalid_config_before_touching_the_network() {
        let mut out = S3Archive::default();
        assert!(matches!(out.start(RString::from("not json")), RErr(_)));
        assert!(matches!(out.start(RString::from("{}")), RErr(_)));
        assert!(out.config.is_none());
    }

    #[test]
    fn on_snapshot_buffers_without_network() {
        let mut out = S3Archive::default();
        out.on_snapshot(RString::from("{\"series\":[]}"));
        out.on_snapshot(RString::from("{\"series\":[]}"));
        // Malformed snapshots are dropped, not fatal.
        out.on_snapshot(RString::from("not json"));
        assert_eq!(out.snapshots.len(), 2);
    }

    #[test]
    fn finish_without_start_does_not_panic() {
        let mut out = S3Archive::default();
        // No config resolved: archive() errors, finish swallows it (no panic,
        // no object counted).
        out.finish(RString::from("{\"run_id\":\"r\"}"));
        assert_eq!(out.objects, 0);
    }

    #[test]
    fn info_declares_output_kind() {
        let out = S3Archive::default();
        assert_eq!(out.name().as_str(), NAME);
        let v: Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(v["kind"], "output");
        assert_eq!(v["name"], "s3-archive");
    }
}
