//! `loadr-plugin-s3-dataset` — a native **service** plugin in the *data sources
//! & feeders* role.
//!
//! Instead of driving a target, it acts as a **data source**: at the start of a
//! run it fetches a single object from S3 over HTTPS, parses it (CSV or JSONL)
//! into rows and returns them, so a table/file living in a bucket becomes a
//! `data:` feeder — the same shape as a local `type: csv` file, but sourced from
//! S3 so the fixture stays next to the system under test.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> RString` is called once, before any VU runs, and
//! `stop()` once at the end. This plugin does all of its work in `start`:
//!
//! 1. Resolve the target object (a full `url`, or `bucket` + `key` + `region`,
//!    or a path-style `endpoint` for S3-compatible stores like MinIO).
//! 2. Resolve credentials from the `config:` block or the standard
//!    `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
//!    environment variables.
//! 3. Sign a `GET` with **AWS Signature Version 4** using a hand-rolled,
//!    pure-Rust signer (sha2 + hmac) — no AWS SDK, no OpenSSL.
//! 4. Fetch the object over the project's existing hyper + hyper-rustls stack
//!    (ring + webpki roots: pure-Rust TLS).
//! 5. Parse the body into rows and return `{ name, source, format, count, rows }`
//!    as JSON for the host feeder to materialise.
//!
//! A `403`/`404` (or any other non-success status) fails `start`, so a
//! misconfigured dataset stops the run before it begins rather than silently
//! feeding empty rows.

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac as _};
use http_body_util::{BodyExt as _, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use once_cell::sync::OnceCell;
use sha2::{Digest as _, Sha256};
use std::time::Duration;
use tokio::runtime::Runtime;
use url::Url;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "s3-dataset";
/// Wall-clock cap for the (one-shot) object fetch.
const FETCH_TIMEOUT_MS: u64 = 30_000;

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
#[allow(dead_code)] // fields asserted in tests
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

// ---------------------------------------------------------------------------
// Target + credential resolution.
// ---------------------------------------------------------------------------

/// A resolved S3 object to fetch.
struct Target {
    /// Full request URL (path already URI-encoded).
    url: String,
    /// `Host` header value (host[:port]) as signed.
    host: String,
    /// Encoded canonical path used in the signature.
    canonical_uri: String,
    /// Object key/path, used for format auto-detection.
    key: String,
    region: String,
}

/// Resolve the target object from the feeder config.
fn resolve_target(cfg: &serde_json::Value) -> Result<Target, String> {
    let region = cfg
        .get("region")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("us-east-1")
        .to_string();

    // 1. Explicit full URL wins.
    if let Some(raw) = cfg
        .get("url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        let u = Url::parse(raw).map_err(|e| format!("invalid `url` `{raw}`: {e}"))?;
        match u.scheme() {
            "https" | "http" => {}
            other => return Err(format!("unsupported url scheme `{other}` (expected https)")),
        }
        let host_only = u
            .host_str()
            .ok_or_else(|| format!("url `{raw}` has no host"))?;
        let host = match u.port() {
            Some(p) => format!("{host_only}:{p}"),
            None => host_only.to_string(),
        };
        // `Url::path()` is already percent-encoded, so it *is* the canonical URI
        // — re-encoding here would double-escape any `%xx` the caller wrote.
        let path = if u.path().is_empty() { "/" } else { u.path() };
        return Ok(Target {
            url: raw.to_string(),
            host,
            canonical_uri: path.to_string(),
            key: path.to_string(),
            region,
        });
    }

    // 2. bucket + key (+ optional endpoint).
    let bucket = cfg
        .get("bucket")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "config requires `url`, or `bucket` + `key`".to_string())?;
    let key = cfg
        .get("key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "config requires `key` when `bucket` is set".to_string())?;
    let key_path = if key.starts_with('/') {
        key.to_string()
    } else {
        format!("/{key}")
    };

    // Path-style against a custom endpoint (e.g. MinIO / S3-compatible stores).
    if let Some(endpoint) = cfg
        .get("endpoint")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        let eu =
            Url::parse(endpoint).map_err(|e| format!("invalid `endpoint` `{endpoint}`: {e}"))?;
        let ehost_only = eu
            .host_str()
            .ok_or_else(|| format!("endpoint `{endpoint}` has no host"))?;
        let host = match eu.port() {
            Some(p) => format!("{ehost_only}:{p}"),
            None => ehost_only.to_string(),
        };
        let path = format!("/{bucket}{key_path}");
        let canonical_uri = uri_encode(&path, false);
        let url = format!("{}://{host}{canonical_uri}", eu.scheme());
        return Ok(Target {
            url,
            host,
            canonical_uri,
            key: key_path,
            region,
        });
    }

    // Virtual-hosted-style against AWS S3.
    let host = format!("{bucket}.s3.{region}.amazonaws.com");
    let canonical_uri = uri_encode(&key_path, false);
    let url = format!("https://{host}{canonical_uri}");
    Ok(Target {
        url,
        host,
        canonical_uri,
        key: key_path,
        region,
    })
}

/// Resolved AWS credentials.
struct Creds {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

/// Read credentials from the config block, falling back to the standard AWS
/// environment variables. Empty strings are treated as absent (so
/// `"${env.AWS_SESSION_TOKEN}"` interpolated to `""` does not sign a bogus
/// security-token header).
fn resolve_credentials(cfg: &serde_json::Value) -> Result<Creds, String> {
    fn from_cfg_or_env(cfg: &serde_json::Value, key: &str, env: &str) -> Option<String> {
        cfg.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| std::env::var(env).ok())
            .filter(|s| !s.is_empty())
    }

    let access_key =
        from_cfg_or_env(cfg, "access_key_id", "AWS_ACCESS_KEY_ID").ok_or_else(|| {
            "missing AWS access key (config `access_key_id` or env AWS_ACCESS_KEY_ID)".to_string()
        })?;
    let secret_key = from_cfg_or_env(cfg, "secret_access_key", "AWS_SECRET_ACCESS_KEY")
        .ok_or_else(|| {
            "missing AWS secret key (config `secret_access_key` or env AWS_SECRET_ACCESS_KEY)"
                .to_string()
        })?;
    let session_token = from_cfg_or_env(cfg, "session_token", "AWS_SESSION_TOKEN");
    Ok(Creds {
        access_key,
        secret_key,
        session_token,
    })
}

// ---------------------------------------------------------------------------
// Row parsing.
// ---------------------------------------------------------------------------

/// The dataset serialisation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Csv,
    Jsonl,
}

impl Kind {
    fn as_str(&self) -> &'static str {
        match self {
            Kind::Csv => "csv",
            Kind::Jsonl => "jsonl",
        }
    }
}

/// Decide the dataset format from an explicit `format` (or auto-detect by the
/// object key's extension).
fn detect_format(explicit: Option<&str>, key: &str) -> Result<Kind, String> {
    match explicit.map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("csv") | Some("tsv") => Ok(Kind::Csv),
        Some("jsonl") | Some("ndjson") | Some("json") => Ok(Kind::Jsonl),
        Some("auto") | None => {
            let k = key.to_ascii_lowercase();
            if k.ends_with(".jsonl") || k.ends_with(".ndjson") || k.ends_with(".json") {
                Ok(Kind::Jsonl)
            } else {
                Ok(Kind::Csv)
            }
        }
        Some(other) => Err(format!(
            "unknown format `{other}` (expected csv|jsonl|auto)"
        )),
    }
}

/// The CSV field delimiter from config (`delimiter`, or a tab for `format: tsv`).
fn csv_delimiter(cfg: &serde_json::Value) -> u8 {
    if let Some(d) = cfg.get("delimiter").and_then(|v| v.as_str()) {
        if d == "\t" || d == "\\t" {
            return b'\t';
        }
        if let Some(b) = d.bytes().next() {
            return b;
        }
    }
    if cfg.get("format").and_then(|v| v.as_str()) == Some("tsv") {
        return b'\t';
    }
    b','
}

/// Parse CSV `body` into a row per record. With `has_header`, columns are named
/// by the header row; otherwise they are `c0`, `c1`, … Every value is a string.
fn parse_csv(
    body: &[u8],
    delimiter: u8,
    has_header: bool,
) -> Result<Vec<serde_json::Value>, String> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(true)
        .from_reader(body);

    let headers: Vec<String> = if has_header {
        rdr.headers()
            .map_err(|e| format!("reading CSV header: {e}"))?
            .iter()
            .map(str::to_string)
            .collect()
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| format!("reading CSV row: {e}"))?;
        let mut obj = serde_json::Map::new();
        for (i, field) in rec.iter().enumerate() {
            let col = headers.get(i).cloned().unwrap_or_else(|| format!("c{i}"));
            obj.insert(col, serde_json::Value::String(field.to_string()));
        }
        rows.push(serde_json::Value::Object(obj));
    }
    Ok(rows)
}

/// Parse JSONL `body`: either a single top-level JSON array (each element a
/// row) or one JSON value per non-empty line.
fn parse_jsonl(body: &[u8]) -> Result<Vec<serde_json::Value>, String> {
    let text = std::str::from_utf8(body).map_err(|e| format!("dataset is not valid UTF-8: {e}"))?;
    let trimmed = text.trim_start();
    if trimmed.starts_with('[') {
        return match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(serde_json::Value::Array(a)) => Ok(a),
            Ok(_) => Err("top-level JSON is not an array".to_string()),
            Err(e) => Err(format!("invalid JSON array: {e}")),
        };
    }
    let mut rows = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_str::<serde_json::Value>(line)
            .map_err(|e| format!("invalid JSON on line {}: {e}", n + 1))?;
        rows.push(value);
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// HTTP fetch.
// ---------------------------------------------------------------------------

/// The single Tokio runtime the plugin uses to drive the one-shot async fetch.
fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build s3-dataset plugin tokio runtime")
    })
}

/// GET `url` with the given headers, returning `(status, body)`.
async fn fetch_object(
    url: &str,
    headers: &[(String, String)],
    timeout_ms: u64,
) -> Result<(u16, Vec<u8>), String> {
    // webpki roots + ring: pure-Rust TLS, no system OpenSSL. `https_or_http`
    // also allows plaintext endpoints (e.g. a local MinIO used in tests).
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(tls);

    let mut builder = Request::builder().method("GET").uri(url);
    for (k, v) in headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(Bytes::new()))
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
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("reading response body failed: {e}"))?
        .to_bytes()
        .to_vec();
    Ok((status, body))
}

/// Map an S3 GET status to a fetch outcome. Success is `200`/`206`; `403`/`404`
/// (and anything else non-2xx) fail the run with an explanatory message.
fn check_status(status: u16) -> Result<(), String> {
    match status {
        200 | 206 => Ok(()),
        403 => Err("S3 returned 403 Forbidden (check credentials / bucket policy)".to_string()),
        404 => Err("S3 returned 404 Not Found (check bucket and key)".to_string()),
        other => Err(format!("S3 returned unexpected status {other}")),
    }
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
// Service plugin.
// ---------------------------------------------------------------------------

/// The whole `start` flow: resolve → sign → fetch → parse. Returns the feeder
/// payload JSON on success.
fn run_start(config_json: &str) -> Result<String, String> {
    let cfg: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    // Resolve the object first, so a config missing a target fails regardless of
    // whatever AWS credentials happen to be in the environment.
    let target = resolve_target(&cfg)?;
    let creds = resolve_credentials(&cfg)?;

    let (amz_date, date_stamp) = timestamps();
    let payload_hash = hex_sha256(b"");

    let mut headers = vec![
        ("host".to_string(), target.host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(token) = &creds.session_token {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }

    let signed = sign_request(&SigV4Params {
        method: "GET",
        canonical_uri: &target.canonical_uri,
        canonical_query: "",
        headers: headers.clone(),
        payload_hash: &payload_hash,
        access_key: &creds.access_key,
        secret_key: &creds.secret_key,
        region: &target.region,
        service: "s3",
        amz_date: &amz_date,
        date_stamp: &date_stamp,
    });

    // Sent headers = the signed set + the (unsigned) Authorization header.
    let mut request_headers = headers;
    request_headers.push(("authorization".to_string(), signed.authorization));

    let (status, body) = runtime().block_on(fetch_object(
        &target.url,
        &request_headers,
        FETCH_TIMEOUT_MS,
    ))?;
    check_status(status)?;

    let kind = detect_format(cfg.get("format").and_then(|v| v.as_str()), &target.key)?;
    let rows = match kind {
        Kind::Csv => {
            let has_header = cfg
                .get("has_header")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            parse_csv(&body, csv_delimiter(&cfg), has_header)?
        }
        Kind::Jsonl => parse_jsonl(&body)?,
    };

    let result = serde_json::json!({
        "name": NAME,
        "source": target.url,
        "format": kind.as_str(),
        "count": rows.len(),
        "rows": rows,
    });
    Ok(result.to_string())
}

#[derive(Default)]
struct S3Dataset {
    started: bool,
}

impl FfiService for S3Dataset {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        match run_start(config_json.as_str()) {
            Ok(json) => {
                self.started = true;
                ROk(RString::from(json))
            }
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // The dataset is fetched once in `start`; there is nothing to tear down.
        // Idempotent: repeated calls are a no-op once already stopped.
        if self.started {
            self.started = false;
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "S3 dataset feeder: fetches an S3 object (SigV4) and parses CSV/JSONL into feeder rows",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(S3Dataset::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RNone,
        make_service: RSome(make_service),
        make_data_source: RNone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // SigV4 — asserted against the published AWS test vectors. No network.
    // -----------------------------------------------------------------------

    /// AWS S3 "GET Object" example from the SigV4 header-based auth docs.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
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

        let expected_cr = "GET\n\
             /test.txt\n\
             \n\
             host:examplebucket.s3.amazonaws.com\n\
             range:bytes=0-9\n\
             x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
             x-amz-date:20130524T000000Z\n\
             \n\
             host;range;x-amz-content-sha256;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(signed.canonical_request, expected_cr);

        let expected_sts = "AWS4-HMAC-SHA256\n\
             20130524T000000Z\n\
             20130524/us-east-1/s3/aws4_request\n\
             7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";
        assert_eq!(signed.string_to_sign, expected_sts);

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

    /// The `get-vanilla` case from the awslabs `aws-sig-v4-test-suite`.
    #[test]
    fn sigv4_matches_get_vanilla_vector() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/",
            canonical_query: "",
            headers: vec![
                ("host".into(), "example.amazonaws.com".into()),
                ("x-amz-date".into(), "20150830T123600Z".into()),
            ],
            payload_hash: empty,
            access_key: "AKIDEXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "service",
            amz_date: "20150830T123600Z",
            date_stamp: "20150830",
        });

        let expected_cr = "GET\n\
             /\n\
             \n\
             host:example.amazonaws.com\n\
             x-amz-date:20150830T123600Z\n\
             \n\
             host;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(signed.canonical_request, expected_cr);
        assert_eq!(
            signed.signature,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
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
        assert_eq!(uri_encode("/data/users.csv", false), "/data/users.csv");
        assert_eq!(uri_encode("/a b/c.csv", false), "/a%20b/c.csv");
        assert_eq!(uri_encode("/a b", true), "%2Fa%20b");
        assert_eq!(uri_encode("/k~e.y-1_2", false), "/k~e.y-1_2");
    }

    // -----------------------------------------------------------------------
    // Parsing.
    // -----------------------------------------------------------------------

    #[test]
    fn parses_csv_with_header() {
        let rows = parse_csv(b"id,email\n1,a@b.com\n2,c@d.com\n", b',', true).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], "1");
        assert_eq!(rows[0]["email"], "a@b.com");
        assert_eq!(rows[1]["email"], "c@d.com");
    }

    #[test]
    fn parses_csv_without_header() {
        let rows = parse_csv(b"x,y\nz,w\n", b',', false).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["c0"], "x");
        assert_eq!(rows[0]["c1"], "y");
        assert_eq!(rows[1]["c0"], "z");
    }

    #[test]
    fn parses_tsv() {
        let rows = parse_csv(b"id\tcity\n1\tLeeds\n", b'\t', true).unwrap();
        assert_eq!(rows[0]["city"], "Leeds");
    }

    #[test]
    fn parses_jsonl_lines() {
        let rows =
            parse_jsonl(b"{\"id\":1,\"name\":\"a\"}\n\n{\"id\":2,\"name\":\"b\"}\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[1]["name"], "b");
    }

    #[test]
    fn parses_json_array() {
        let rows = parse_jsonl(b"[{\"a\":1},{\"a\":2}]").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["a"], 2);
    }

    #[test]
    fn rejects_bad_jsonl() {
        assert!(parse_jsonl(b"{not json}").is_err());
    }

    #[test]
    fn detects_format() {
        assert_eq!(detect_format(None, "x.csv").unwrap(), Kind::Csv);
        assert_eq!(detect_format(None, "x.jsonl").unwrap(), Kind::Jsonl);
        assert_eq!(detect_format(None, "x.ndjson").unwrap(), Kind::Jsonl);
        assert_eq!(detect_format(None, "x.dat").unwrap(), Kind::Csv);
        assert_eq!(detect_format(Some("jsonl"), "x.csv").unwrap(), Kind::Jsonl);
        assert_eq!(detect_format(Some("csv"), "x.jsonl").unwrap(), Kind::Csv);
        assert!(detect_format(Some("parquet"), "x").is_err());
    }

    #[test]
    fn csv_delimiter_selection() {
        assert_eq!(csv_delimiter(&serde_json::json!({})), b',');
        assert_eq!(csv_delimiter(&serde_json::json!({"delimiter": ";"})), b';');
        assert_eq!(
            csv_delimiter(&serde_json::json!({"delimiter": "\t"})),
            b'\t'
        );
        assert_eq!(csv_delimiter(&serde_json::json!({"format": "tsv"})), b'\t');
    }

    // -----------------------------------------------------------------------
    // Status mapping / target resolution.
    // -----------------------------------------------------------------------

    #[test]
    fn status_403_and_404_are_errors() {
        assert!(check_status(200).is_ok());
        assert!(check_status(206).is_ok());
        assert!(check_status(403).is_err());
        assert!(check_status(404).is_err());
        assert!(check_status(500).is_err());
    }

    #[test]
    fn resolves_virtual_hosted_target() {
        let cfg = serde_json::json!({
            "bucket": "examplebucket",
            "key": "data/users.csv",
            "region": "us-east-1",
        });
        let t = resolve_target(&cfg).unwrap();
        assert_eq!(t.host, "examplebucket.s3.us-east-1.amazonaws.com");
        assert_eq!(t.canonical_uri, "/data/users.csv");
        assert_eq!(
            t.url,
            "https://examplebucket.s3.us-east-1.amazonaws.com/data/users.csv"
        );
        assert_eq!(t.region, "us-east-1");
    }

    #[test]
    fn resolves_path_style_endpoint() {
        let cfg = serde_json::json!({
            "bucket": "fixtures",
            "key": "users.jsonl",
            "endpoint": "http://localhost:9000",
        });
        let t = resolve_target(&cfg).unwrap();
        assert_eq!(t.host, "localhost:9000");
        assert_eq!(t.canonical_uri, "/fixtures/users.jsonl");
        assert_eq!(t.url, "http://localhost:9000/fixtures/users.jsonl");
    }

    #[test]
    fn resolves_explicit_url() {
        let cfg = serde_json::json!({
            "url": "https://examplebucket.s3.amazonaws.com/a%20b.csv",
        });
        let t = resolve_target(&cfg).unwrap();
        assert_eq!(t.host, "examplebucket.s3.amazonaws.com");
        // The URL path is already percent-encoded; we pass it through unchanged.
        assert_eq!(t.canonical_uri, "/a%20b.csv");
    }

    #[test]
    fn empty_config_has_no_target() {
        assert!(resolve_target(&serde_json::json!({})).is_err());
        assert!(resolve_target(&serde_json::json!({"bucket": "b"})).is_err());
    }

    // -----------------------------------------------------------------------
    // Service lifecycle. No network.
    // -----------------------------------------------------------------------

    #[test]
    fn start_rejects_invalid_config_before_touching_network() {
        let mut svc = S3Dataset::default();
        assert!(matches!(
            svc.start(RString::from("not json")),
            RResult::RErr(_)
        ));
        assert!(matches!(svc.start(RString::from("{}")), RResult::RErr(_)));
        assert!(!svc.started);
    }

    #[test]
    fn stop_is_idempotent() {
        let mut svc = S3Dataset::default();
        svc.stop();
        svc.stop();
        assert!(!svc.started);
    }

    #[test]
    fn service_name_and_info() {
        let svc = S3Dataset::default();
        assert_eq!(svc.name().as_str(), NAME);
        let v: serde_json::Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "s3-dataset");
    }
}
