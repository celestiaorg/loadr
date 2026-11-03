//! `loadr-plugin-aws-sigv4` — a native **service** plugin in the *auth &
//! signers* role.
//!
//! It is a small, in-process **AWS Signature Version 4 signer**. Given a request
//! (method + url [+ headers/body]) and a `region` + `service` scope, it builds
//! the SigV4 **canonical request**, derives the signing key and returns the
//! `Authorization` header — plus `X-Amz-Date`, `X-Amz-Content-Sha256`, and, for
//! temporary credentials, `X-Amz-Security-Token` — that AWS expects. A request
//! that names it as its signer gets those headers stamped on just before it goes
//! out.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> RString` and `stop()`. This signer reuses `start` as
//! its per-request hook: the host hands it a JSON request descriptor and it
//! returns the JSON set of headers to stamp:
//!
//! ```json
//! // in  (config_json)
//! { "region": "eu-west-2", "service": "s3", "method": "GET",
//!   "url": "https://b.s3.eu-west-2.amazonaws.com/k", "headers": {"range": "bytes=0-9"} }
//! // out
//! { "headers": { "Authorization": "AWS4-HMAC-SHA256 …", "X-Amz-Date": "…",
//!                "X-Amz-Content-Sha256": "…" }, "signatures": 1 }
//! ```
//!
//! It is **pure Rust** — SHA-256 and HMAC via `sha2` + `hmac`, with no AWS SDK,
//! no `aws-*` crate and no C dependency — and does **no** network or disk I/O of
//! its own: it only transforms request headers. Credentials come from the
//! standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
//! environment variables (never the plan), so supply them via `aws-vault exec`.

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use hmac::{Hmac, KeyInit, Mac as _};
use sha2::{Digest as _, Sha256};
use url::Url;

use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};

const NAME: &str = "aws-sigv4";
/// The content hash used when the caller opts out of hashing the body.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

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
// AWS Signature Version 4 core.
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
    /// Hex SHA-256 of the request payload (or `UNSIGNED-PAYLOAD`).
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
#[derive(Debug)]
struct Signed {
    authorization: String,
    canonical_request: String,
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
        signature,
    }
}

// ---------------------------------------------------------------------------
// Clock seam — a small injection point so the full `sign` path can be unit
// tested against the published AWS vectors with a fixed timestamp, without
// touching the wall clock.
// ---------------------------------------------------------------------------

/// Supplies the `(amz_date, date_stamp)` pair a signature is stamped with.
trait Clock: Send {
    /// `("YYYYMMDDTHHMMSSZ", "YYYYMMDD")` in UTC.
    fn now(&self) -> (String, String);
}

/// The real clock: current UTC via the `time` crate.
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> (String, String) {
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
}

// ---------------------------------------------------------------------------
// Request + credential resolution.
// ---------------------------------------------------------------------------

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

/// Header names the signer manages itself; a caller-supplied duplicate is
/// dropped so it is signed exactly once with the value we compute.
fn is_managed_header(name: &str) -> bool {
    matches!(
        name,
        "authorization" | "host" | "x-amz-date" | "x-amz-content-sha256" | "x-amz-security-token"
    )
}

/// The `Host` header value (host[:port]) as it must be signed.
fn host_header(url: &Url) -> Result<String, String> {
    let host = url
        .host_str()
        .ok_or_else(|| format!("url `{url}` has no host"))?;
    Ok(match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    })
}

/// AWS-canonical, sorted query string built from the url's query pairs. Each
/// key and value is re-encoded (AWS rules) and the pairs are sorted by encoded
/// key then value. Empty when the url has no query.
fn canonical_query(url: &Url) -> String {
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (uri_encode(&k, true), uri_encode(&v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The headers this signer stamps on the outgoing request, as canonical
/// (`X-Amz-…`) casing → value.
#[derive(Debug)]
#[allow(dead_code)] // `canonical_request` / `signature` asserted in tests
struct SignedRequest {
    authorization: String,
    amz_date: String,
    content_sha256: String,
    security_token: Option<String>,
    /// Intermediate, exposed for the AWS-vector tests.
    canonical_request: String,
    signature: String,
}

impl SignedRequest {
    /// The headers to stamp, in stable canonical casing.
    fn headers(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert(
            "Authorization".into(),
            serde_json::Value::String(self.authorization.clone()),
        );
        m.insert(
            "X-Amz-Date".into(),
            serde_json::Value::String(self.amz_date.clone()),
        );
        m.insert(
            "X-Amz-Content-Sha256".into(),
            serde_json::Value::String(self.content_sha256.clone()),
        );
        if let Some(token) = &self.security_token {
            m.insert(
                "X-Amz-Security-Token".into(),
                serde_json::Value::String(token.clone()),
            );
        }
        m
    }
}

/// The whole signer: resolve scope + credentials, build the canonical request,
/// and return the headers to stamp. Pure — no I/O beyond reading credentials
/// from the environment. `clock` supplies the timestamp so tests are
/// deterministic.
fn sign(cfg: &serde_json::Value, clock: &dyn Clock) -> Result<SignedRequest, String> {
    let region = cfg
        .get("region")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_REGION").ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "missing `region` (config `region` or env AWS_REGION)".to_string())?;

    let service = cfg
        .get("service")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "missing `service` (e.g. s3, execute-api, dynamodb)".to_string())?
        .to_string();

    let method = cfg
        .get("method")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("GET")
        .to_ascii_uppercase();

    let raw_url = cfg
        .get("url")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "missing `url` to sign".to_string())?;
    let url = Url::parse(raw_url).map_err(|e| format!("invalid `url` `{raw_url}`: {e}"))?;
    match url.scheme() {
        "https" | "http" => {}
        other => return Err(format!("unsupported url scheme `{other}` (expected https)")),
    }

    let creds = resolve_credentials(cfg)?;

    let unsigned_payload = cfg
        .get("unsigned_payload")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let body = cfg
        .get("body")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .as_bytes();
    let payload_hash = if unsigned_payload {
        UNSIGNED_PAYLOAD.to_string()
    } else {
        hex_sha256(body)
    };

    let (amz_date, date_stamp) = clock.now();

    // `Url::path()` is already percent-encoded, so it *is* the canonical URI —
    // re-encoding would double-escape any `%xx` the caller wrote.
    let canonical_uri = if url.path().is_empty() {
        "/".to_string()
    } else {
        url.path().to_string()
    };
    let canonical_query_str = canonical_query(&url);
    let host = host_header(&url)?;

    // Headers the signer always contributes.
    let mut headers = vec![
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(token) = &creds.session_token {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }
    // Caller-supplied additional headers to include in the signature (e.g.
    // `range`, `content-type`). Managed names are ignored so they are not
    // double-signed with a stale value.
    if let Some(extra) = cfg.get("headers").and_then(|v| v.as_object()) {
        for (name, value) in extra {
            let lname = name.to_ascii_lowercase();
            if is_managed_header(&lname) {
                continue;
            }
            if let Some(v) = value.as_str() {
                headers.push((lname, v.to_string()));
            }
        }
    }

    let signed = sign_request(&SigV4Params {
        method: &method,
        canonical_uri: &canonical_uri,
        canonical_query: &canonical_query_str,
        headers,
        payload_hash: &payload_hash,
        access_key: &creds.access_key,
        secret_key: &creds.secret_key,
        region: &region,
        service: &service,
        amz_date: &amz_date,
        date_stamp: &date_stamp,
    });

    Ok(SignedRequest {
        authorization: signed.authorization,
        amz_date,
        content_sha256: payload_hash,
        security_token: creds.session_token,
        canonical_request: signed.canonical_request,
        signature: signed.signature,
    })
}

// ---------------------------------------------------------------------------
// Service plugin.
// ---------------------------------------------------------------------------

/// The pure-Rust SigV4 signer service. Holds the clock seam and a running count
/// of signatures produced (surfaced in each response for the host's
/// `sigv4_signatures` metric).
struct Signer {
    clock: Box<dyn Clock>,
    signatures: u64,
}

impl Default for Signer {
    fn default() -> Self {
        Signer {
            clock: Box::new(SystemClock),
            signatures: 0,
        }
    }
}

impl Signer {
    /// Sign one request (`config_json`) and return the headers-to-stamp JSON.
    fn run_start(&mut self, config_json: &str) -> Result<String, String> {
        let cfg: serde_json::Value =
            serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;
        let signed = sign(&cfg, self.clock.as_ref())?;
        self.signatures += 1;
        let result = serde_json::json!({
            "headers": serde_json::Value::Object(signed.headers()),
            "signatures": self.signatures,
        });
        Ok(result.to_string())
    }
}

impl FfiService for Signer {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        match self.run_start(config_json.as_str()) {
            Ok(json) => ROk(RString::from(json)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // The signer holds no sockets, files or timers — signing is a pure
        // transform — so there is nothing to tear down. Idempotent by nature.
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "Pure-Rust AWS SigV4 request signer (canonical request + Authorization header)",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(Signer::default(), abi_stable::erased_types::TD_Opaque)
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

    /// The SHA-256 of an empty body — the payload hash for a GET.
    const EMPTY_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    /// A clock pinned to a fixed instant, so the full `sign` path is
    /// deterministic and can be checked against the published AWS vectors —
    /// the same seam role `ConnFactory` plays in the redis-loader plugin.
    struct FixedClock {
        amz_date: &'static str,
        date_stamp: &'static str,
    }

    impl Clock for FixedClock {
        fn now(&self) -> (String, String) {
            (self.amz_date.to_string(), self.date_stamp.to_string())
        }
    }

    fn s3_vector_clock() -> FixedClock {
        FixedClock {
            amz_date: "20130524T000000Z",
            date_stamp: "20130524",
        }
    }

    // -----------------------------------------------------------------------
    // Low-level SigV4 core — asserted against the published AWS test vectors.
    // -----------------------------------------------------------------------

    /// AWS S3 "GET Object" example from the SigV4 header-based auth docs.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    #[test]
    fn sign_request_matches_aws_s3_get_object_vector() {
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/test.txt",
            canonical_query: "",
            headers: vec![
                ("host".into(), "examplebucket.s3.amazonaws.com".into()),
                ("range".into(), "bytes=0-9".into()),
                ("x-amz-content-sha256".into(), EMPTY_SHA.into()),
                ("x-amz-date".into(), "20130524T000000Z".into()),
            ],
            payload_hash: EMPTY_SHA,
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
    }

    /// The `get-vanilla` case from the awslabs `aws-sig-v4-test-suite`. Signed
    /// through the low-level core because it does NOT carry an
    /// `x-amz-content-sha256` header (the higher-level `sign` always adds one).
    #[test]
    fn sign_request_matches_get_vanilla_vector() {
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/",
            canonical_query: "",
            headers: vec![
                ("host".into(), "example.amazonaws.com".into()),
                ("x-amz-date".into(), "20150830T123600Z".into()),
            ],
            payload_hash: EMPTY_SHA,
            access_key: "AKIDEXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            service: "service",
            amz_date: "20150830T123600Z",
            date_stamp: "20150830",
        });
        assert_eq!(
            signed.signature,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn empty_payload_hash_is_the_known_constant() {
        assert_eq!(hex_sha256(b""), EMPTY_SHA);
    }

    #[test]
    fn uri_encode_rules() {
        assert_eq!(uri_encode("/data/users.csv", false), "/data/users.csv");
        assert_eq!(uri_encode("/a b/c.csv", false), "/a%20b/c.csv");
        assert_eq!(uri_encode("/a b", true), "%2Fa%20b");
        assert_eq!(uri_encode("/k~e.y-1_2", false), "/k~e.y-1_2");
    }

    // -----------------------------------------------------------------------
    // Full `sign` path — deterministic via the FixedClock seam. No network.
    // -----------------------------------------------------------------------

    /// The full signer (auto-adding host + `x-amz-content-sha256` + `x-amz-date`)
    /// reproduces the AWS S3 GET-Object vector end to end.
    #[test]
    fn sign_reproduces_s3_vector_end_to_end() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "method": "GET",
            "url": "https://examplebucket.s3.amazonaws.com/test.txt",
            "headers": { "range": "bytes=0-9" },
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        });
        let signed = sign(&cfg, &s3_vector_clock()).unwrap();

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
        assert_eq!(
            signed.signature,
            "67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
        assert_eq!(signed.content_sha256, EMPTY_SHA);
        assert_eq!(signed.amz_date, "20130524T000000Z");
        assert!(signed.security_token.is_none());
        assert!(signed.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"
        ));
    }

    #[test]
    fn sign_stamps_security_token_header_for_temporary_creds() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "https://examplebucket.s3.amazonaws.com/test.txt",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
            "session_token": "FQoGZ-token",
        });
        let signed = sign(&cfg, &s3_vector_clock()).unwrap();
        assert_eq!(signed.security_token.as_deref(), Some("FQoGZ-token"));
        // The token is part of the signed set.
        assert!(signed
            .canonical_request
            .contains("x-amz-security-token:FQoGZ-token"));
        let headers = signed.headers();
        assert_eq!(headers["X-Amz-Security-Token"], "FQoGZ-token");
    }

    #[test]
    fn empty_session_token_does_not_add_header() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "https://b.s3.amazonaws.com/k",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
            "session_token": "",
        });
        let signed = sign(&cfg, &s3_vector_clock()).unwrap();
        assert!(signed.security_token.is_none());
        assert!(!signed.canonical_request.contains("x-amz-security-token"));
    }

    #[test]
    fn unsigned_payload_uses_the_sentinel_hash() {
        let cfg = serde_json::json!({
            "region": "eu-west-2",
            "service": "s3",
            "url": "https://b.s3.eu-west-2.amazonaws.com/big.bin",
            "unsigned_payload": true,
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
        });
        let signed = sign(&cfg, &s3_vector_clock()).unwrap();
        assert_eq!(signed.content_sha256, UNSIGNED_PAYLOAD);
        assert!(signed
            .canonical_request
            .contains("x-amz-content-sha256:UNSIGNED-PAYLOAD"));
    }

    #[test]
    fn body_changes_the_payload_hash() {
        let base = serde_json::json!({
            "region": "us-east-1",
            "service": "execute-api",
            "method": "POST",
            "url": "https://api.example.com/things",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
        });
        let empty = sign(&base, &s3_vector_clock()).unwrap();
        assert_eq!(empty.content_sha256, EMPTY_SHA);

        let mut with_body = base.clone();
        with_body["body"] = serde_json::json!("{\"name\":\"widget\"}");
        let signed = sign(&with_body, &s3_vector_clock()).unwrap();
        assert_ne!(signed.content_sha256, EMPTY_SHA);
        assert_eq!(signed.content_sha256, hex_sha256(b"{\"name\":\"widget\"}"));
    }

    #[test]
    fn canonical_query_is_sorted_and_encoded() {
        let url = Url::parse("https://h/p?b=2&a=1&c=a%20b").unwrap();
        assert_eq!(canonical_query(&url), "a=1&b=2&c=a%20b");
        let none = Url::parse("https://h/p").unwrap();
        assert_eq!(canonical_query(&none), "");
    }

    #[test]
    fn host_header_includes_non_default_port() {
        let url = Url::parse("http://localhost:9000/bucket/key").unwrap();
        assert_eq!(host_header(&url).unwrap(), "localhost:9000");
        let url = Url::parse("https://h.example.com/k").unwrap();
        assert_eq!(host_header(&url).unwrap(), "h.example.com");
    }

    #[test]
    fn caller_supplied_managed_header_is_not_double_signed() {
        // A caller passing `host` / `x-amz-date` must not add a second copy.
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "https://examplebucket.s3.amazonaws.com/test.txt",
            "headers": { "Host": "attacker", "X-Amz-Date": "19700101T000000Z", "range": "bytes=0-9" },
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        });
        let signed = sign(&cfg, &s3_vector_clock()).unwrap();
        // Still the canonical S3 vector — the bogus overrides were dropped.
        assert_eq!(
            signed.signature,
            "67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
    }

    // -----------------------------------------------------------------------
    // Config validation errors. No network, no env dependence.
    // -----------------------------------------------------------------------

    #[test]
    fn missing_service_is_an_error() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "url": "https://b.s3.amazonaws.com/k",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
        });
        let err = sign(&cfg, &s3_vector_clock()).unwrap_err();
        assert!(err.contains("service"), "got: {err}");
    }

    #[test]
    fn missing_url_is_an_error() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
        });
        let err = sign(&cfg, &s3_vector_clock()).unwrap_err();
        assert!(err.contains("url"), "got: {err}");
    }

    #[test]
    fn missing_credentials_is_an_error() {
        // Config carries no creds; rely on config-only resolution being empty.
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "https://b.s3.amazonaws.com/k",
            "access_key_id": "",
            "secret_access_key": "",
        });
        let err = sign(&cfg, &s3_vector_clock()).unwrap_err();
        assert!(
            err.contains("access key") || err.contains("secret key"),
            "got: {err}"
        );
    }

    #[test]
    fn non_http_scheme_is_rejected() {
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "ftp://b.example.com/k",
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
        });
        let err = sign(&cfg, &s3_vector_clock()).unwrap_err();
        assert!(err.contains("scheme"), "got: {err}");
    }

    // -----------------------------------------------------------------------
    // Service lifecycle. No network.
    // -----------------------------------------------------------------------

    fn signer_with_fixed_clock() -> Signer {
        Signer {
            clock: Box::new(s3_vector_clock()),
            signatures: 0,
        }
    }

    #[test]
    fn start_returns_headers_and_counts_signatures() {
        let mut svc = signer_with_fixed_clock();
        let cfg = serde_json::json!({
            "region": "us-east-1",
            "service": "s3",
            "url": "https://examplebucket.s3.amazonaws.com/test.txt",
            "headers": { "range": "bytes=0-9" },
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        })
        .to_string();

        let out = match svc.start(RString::from(cfg.clone())) {
            RResult::ROk(s) => s,
            RResult::RErr(e) => panic!("start failed: {e}"),
        };
        let v: serde_json::Value = serde_json::from_str(out.as_str()).unwrap();
        assert_eq!(v["signatures"], 1);
        let auth = v["headers"]["Authorization"].as_str().unwrap();
        assert!(auth.contains(
            "Signature=67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        ));
        assert_eq!(v["headers"]["X-Amz-Date"], "20130524T000000Z");

        // A second sign bumps the counter — real per-request state.
        let out2 = match svc.start(RString::from(cfg)) {
            RResult::ROk(s) => s,
            RResult::RErr(e) => panic!("start failed: {e}"),
        };
        let v2: serde_json::Value = serde_json::from_str(out2.as_str()).unwrap();
        assert_eq!(v2["signatures"], 2);
    }

    #[test]
    fn start_rejects_invalid_config() {
        let mut svc = signer_with_fixed_clock();
        assert!(matches!(
            svc.start(RString::from("not json")),
            RResult::RErr(_)
        ));
        assert!(matches!(svc.start(RString::from("{}")), RResult::RErr(_)));
    }

    #[test]
    fn stop_is_idempotent() {
        let mut svc = signer_with_fixed_clock();
        svc.stop();
        svc.stop();
    }

    #[test]
    fn service_name_and_info() {
        let svc = Signer::default();
        assert_eq!(svc.name().as_str(), NAME);
        let v: serde_json::Value = serde_json::from_str(plugin_info().as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "aws-sigv4");
    }
}
