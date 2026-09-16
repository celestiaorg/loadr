//! `loadr-plugin-hmac-signer` — a native **service** plugin that serves
//! HMAC-SHA256/512 request signatures over a small local endpoint, for
//! signed-request auth against partner and webhook APIs.
//!
//! # Why a service plugin
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> bound_addr` and an idempotent `stop()`. On `start`
//! this plugin parses the signer config (shared secret, algorithm, canonical
//! `template`, header, encoding, prefix), binds a tiny local line endpoint
//! (`127.0.0.1:0` by default) and returns its bound address.
//!
//! Every request that names it as its signer connects to that endpoint and
//! sends one request line — either a JSON object of request fields
//! (`{"method":…,"path":…,"url":…,"body":…}`) which the plugin substitutes into
//! the configured `template`, or an already-rendered canonical string which is
//! signed verbatim. The plugin computes the keyed MAC over that canonical
//! string and replies with a single JSON line:
//!
//! ```text
//! {"header":"x-signature","value":"sha256=9f86d081…"}
//! ```
//!
//! loadr stamps that header/value pair onto the request just before it goes out.
//!
//! # Pure Rust
//!
//! The keyed MAC and hash are computed with the [`hmac`] and [`sha2`] crates and
//! encoded with [`base64`]/hex — **no OpenSSL and no C dependency** — so the
//! cdylib cross-compiles cleanly for every release target and installs by name
//! with no build toolchain.
//!
//! [`hmac`]: https://docs.rs/hmac
//! [`sha2`]: https://docs.rs/sha2

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac as _};
use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use sha2::{Sha256, Sha512};

const NAME: &str = "hmac-signer";

const DEFAULT_HEADER: &str = "x-signature";
const DEFAULT_TEMPLATE: &str = "{method}{path}{body}";
const DEFAULT_BIND: &str = "127.0.0.1:0";

// ---------------------------------------------------------------------------
// Crypto core — pure Rust HMAC + encoding, no external I/O.
// ---------------------------------------------------------------------------

/// The HMAC hash to key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Algo {
    Sha256,
    Sha512,
}

/// How the signature bytes are rendered into the header value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Hex,
    Base64,
}

/// HMAC(`key`, `data`) raw bytes for the chosen hash. HMAC accepts a key of any
/// length, so `new_from_slice` never errors here.
fn hmac_bytes(algo: Algo, key: &[u8], data: &[u8]) -> Vec<u8> {
    match algo {
        Algo::Sha256 => {
            let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac key");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algo::Sha512 => {
            let mut mac = <Hmac<Sha512> as KeyInit>::new_from_slice(key).expect("hmac key");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    }
}

/// Lowercase hex of `bytes`.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn encode(encoding: Encoding, bytes: &[u8]) -> String {
    match encoding {
        Encoding::Hex => hex_lower(bytes),
        Encoding::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

// ---------------------------------------------------------------------------
// Config.
// ---------------------------------------------------------------------------

/// The parsed, validated signer configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    secret: Vec<u8>,
    algo: Algo,
    encoding: Encoding,
    prefix: String,
    header: String,
    template: String,
    bind: String,
}

fn parse_config(config_json: &str) -> Result<Config, String> {
    let cfg: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let secret = cfg
        .get("secret")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "config requires a non-empty `secret` string".to_string())?
        .as_bytes()
        .to_vec();

    let algo = match cfg.get("algo").and_then(|v| v.as_str()).unwrap_or("sha256") {
        "sha256" => Algo::Sha256,
        "sha512" => Algo::Sha512,
        other => return Err(format!("unknown algo `{other}` (use `sha256` or `sha512`)")),
    };

    let encoding = match cfg
        .get("encoding")
        .and_then(|v| v.as_str())
        .unwrap_or("hex")
    {
        "hex" => Encoding::Hex,
        "base64" => Encoding::Base64,
        other => {
            return Err(format!(
                "unknown encoding `{other}` (use `hex` or `base64`)"
            ))
        }
    };

    let prefix = cfg
        .get("prefix")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let header = cfg
        .get("header")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_HEADER)
        .to_string();

    let template = cfg
        .get("template")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_TEMPLATE)
        .to_string();

    let bind = cfg
        .get("bind")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_BIND)
        .to_string();

    Ok(Config {
        secret,
        algo,
        encoding,
        prefix,
        header,
        template,
        bind,
    })
}

// ---------------------------------------------------------------------------
// The signer — renders the canonical string and produces the header value.
// ---------------------------------------------------------------------------

/// A configured signer plus a count of the signatures it has produced.
struct Signer {
    cfg: Config,
    signed: AtomicU64,
}

impl Signer {
    fn new(cfg: Config) -> Self {
        Signer {
            cfg,
            signed: AtomicU64::new(0),
        }
    }

    /// Substitute request placeholders into the configured template.
    fn render_template(
        &self,
        fields: &serde_json::Map<String, serde_json::Value>,
        now: u64,
    ) -> String {
        let get = |k: &str| fields.get(k).and_then(|v| v.as_str()).unwrap_or("");
        self.cfg
            .template
            .replace("{method}", &get("method").to_uppercase())
            .replace("{path}", get("path"))
            .replace("{url}", get("url"))
            .replace("{body}", get("body"))
            .replace("{timestamp}", &now.to_string())
    }

    /// The exact byte string to sign for one request line. A JSON object is
    /// rendered through the template; anything else is treated as an
    /// already-rendered canonical string and signed verbatim.
    fn canonical_for(&self, line: &str, now: u64) -> String {
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(serde_json::Value::Object(fields)) => self.render_template(&fields, now),
            _ => line.to_string(),
        }
    }

    /// The encoded, prefixed signature for a canonical string.
    fn sign_canonical(&self, canonical: &str) -> String {
        let mac = hmac_bytes(self.cfg.algo, &self.cfg.secret, canonical.as_bytes());
        format!("{}{}", self.cfg.prefix, encode(self.cfg.encoding, &mac))
    }

    /// Handle one request line: render + sign, count it, and return the JSON
    /// reply line (`{"header":…,"value":…}\n`). Pure — no network I/O — so it
    /// is exercised directly in the tests.
    fn handle_request_line(&self, line: &str, now: u64) -> String {
        let canonical = self.canonical_for(line, now);
        let value = self.sign_canonical(&canonical);
        self.signed.fetch_add(1, Ordering::Relaxed);
        let reply = serde_json::json!({ "header": self.cfg.header, "value": value });
        format!("{reply}\n")
    }
}

// ---------------------------------------------------------------------------
// Endpoint — a local line service in front of the signer. Generic over the
// stream so it is unit-tested against an in-memory duplex, never a real socket.
// ---------------------------------------------------------------------------

/// Read one `\n`-terminated line (including the newline) into `buf`. Returns the
/// number of bytes read; `Ok(0)` means the peer closed the connection.
fn read_line_bytes<R: Read>(reader: &mut R, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    let mut byte = [0u8; 1];
    let mut n = 0usize;
    loop {
        match reader.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {
                n += 1;
                buf.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// Serve one client connection: for every request line, write back the signer's
/// JSON reply line. Loops until the peer closes or `stop()` flips `shutdown`.
fn serve_client<S: Read + Write>(stream: &mut S, signer: &Signer, shutdown: &AtomicBool) {
    let mut line = Vec::new();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        line.clear();
        match read_line_bytes(stream, &mut line) {
            Ok(0) => return, // peer closed
            Ok(_) => {}
            Err(_) => return,
        }
        let text = String::from_utf8_lossy(&line);
        let request = text.trim_end_matches(['\n', '\r']);
        let reply = signer.handle_request_line(request, unix_now());
        if stream.write_all(reply.as_bytes()).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Service lifecycle.
// ---------------------------------------------------------------------------

/// A running signer endpoint. Handed to `stop()` for teardown.
struct ServerHandle {
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    addr: String,
    signer: Arc<Signer>,
}

/// The service plugin instance.
#[derive(Default)]
struct HmacSigner {
    handle: Option<ServerHandle>,
}

impl HmacSigner {
    fn start_config(&mut self, cfg: Config) -> Result<String, String> {
        let bind = cfg.bind.clone();
        let signer = Arc::new(Signer::new(cfg));

        let listener = TcpListener::bind(&bind)
            .map_err(|e| format!("cannot bind signer endpoint {bind}: {e}"))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .to_string();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let accept = spawn_accept_loop(listener, signer.clone(), shutdown.clone());

        self.handle = Some(ServerHandle {
            shutdown,
            accept: Some(accept),
            addr: addr.clone(),
            signer,
        });
        Ok(addr)
    }
}

/// Spawn the accept loop. Each accepted connection is served on its own thread,
/// sharing the one [`Signer`].
fn spawn_accept_loop(
    listener: TcpListener,
    signer: Arc<Signer>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nonblocking(false);
                    let signer = signer.clone();
                    let shutdown = shutdown.clone();
                    std::thread::spawn(move || {
                        let mut stream: TcpStream = stream;
                        serve_client(&mut stream, &signer, &shutdown);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    })
}

impl FfiService for HmacSigner {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if let Some(h) = self.handle.as_ref() {
            // Already running: return the existing address rather than rebind.
            return ROk(RString::from(h.addr.clone()));
        }
        let cfg = match parse_config(config_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(e)),
        };
        match self.start_config(cfg) {
            Ok(addr) => ROk(RString::from(addr)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // Idempotent: a no-op when never started or already stopped.
        if let Some(mut handle) = self.handle.take() {
            handle.shutdown.store(true, Ordering::Relaxed);
            // The accept loop polls a nonblocking listener, so it observes the
            // shutdown flag within its 50ms poll interval and returns; join it.
            if let Some(join) = handle.accept.take() {
                let _ = join.join();
            }
            let signed = handle.signer.signed.load(Ordering::Relaxed);
            eprintln!("[hmac-signer] signed {signed} request(s)");
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description":
                "Pure-Rust HMAC (SHA-256/512) request signer over a configurable canonical string",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(HmacSigner::default(), abi_stable::erased_types::TD_Opaque)
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

// ---------------------------------------------------------------------------
// Tests — all offline; the endpoint is exercised through an in-memory duplex,
// never a real socket.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// An in-memory bidirectional stream: reads drain `input`, writes append to
    /// `output`. Stands in for an accepted `TcpStream` in [`serve_client`].
    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl MockStream {
        fn feeding(input: &str) -> Self {
            MockStream {
                input: Cursor::new(input.as_bytes().to_vec()),
                output: Vec::new(),
            }
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn sample_config() -> Config {
        Config {
            secret: b"key".to_vec(),
            algo: Algo::Sha256,
            encoding: Encoding::Hex,
            prefix: String::new(),
            header: DEFAULT_HEADER.to_string(),
            template: DEFAULT_TEMPLATE.to_string(),
            bind: DEFAULT_BIND.to_string(),
        }
    }

    // -- config --------------------------------------------------------------

    #[test]
    fn config_requires_secret() {
        let err = parse_config(r#"{"algo":"sha256"}"#).unwrap_err();
        assert!(err.contains("secret"), "{err}");
        let err = parse_config(r#"{"secret":""}"#).unwrap_err();
        assert!(err.contains("secret"), "{err}");
    }

    #[test]
    fn config_defaults() {
        let cfg = parse_config(r#"{"secret":"s3cr3t"}"#).unwrap();
        assert_eq!(cfg.secret, b"s3cr3t");
        assert_eq!(cfg.algo, Algo::Sha256);
        assert_eq!(cfg.encoding, Encoding::Hex);
        assert_eq!(cfg.prefix, "");
        assert_eq!(cfg.header, "x-signature");
        assert_eq!(cfg.template, "{method}{path}{body}");
        assert_eq!(cfg.bind, "127.0.0.1:0");
    }

    #[test]
    fn config_overrides() {
        let cfg = parse_config(
            r#"{"secret":"k","algo":"sha512","encoding":"base64","header":"x-hub-signature-256","prefix":"sha256=","template":"{body}","bind":"127.0.0.1:9999"}"#,
        )
        .unwrap();
        assert_eq!(cfg.algo, Algo::Sha512);
        assert_eq!(cfg.encoding, Encoding::Base64);
        assert_eq!(cfg.header, "x-hub-signature-256");
        assert_eq!(cfg.prefix, "sha256=");
        assert_eq!(cfg.template, "{body}");
        assert_eq!(cfg.bind, "127.0.0.1:9999");
    }

    #[test]
    fn config_rejects_unknown_algo() {
        let err = parse_config(r#"{"secret":"k","algo":"md5"}"#).unwrap_err();
        assert!(err.contains("algo"), "{err}");
    }

    #[test]
    fn config_rejects_unknown_encoding() {
        let err = parse_config(r#"{"secret":"k","encoding":"base32"}"#).unwrap_err();
        assert!(err.contains("encoding"), "{err}");
    }

    // -- crypto --------------------------------------------------------------

    #[test]
    fn hmac_sha256_known_vector() {
        // RFC-style vector: HMAC-SHA256(key="key", msg="The quick brown fox
        // jumps over the lazy dog").
        let mac = hmac_bytes(
            Algo::Sha256,
            b"key",
            b"The quick brown fox jumps over the lazy dog",
        );
        assert_eq!(
            hex_lower(&mac),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn hmac_sha512_has_expected_width() {
        // 64-byte digest -> 128 hex chars / distinct from sha256.
        let mac = hmac_bytes(Algo::Sha512, b"key", b"data");
        assert_eq!(mac.len(), 64);
        assert_eq!(hex_lower(&mac).len(), 128);
    }

    #[test]
    fn encoding_hex_vs_base64() {
        let bytes = [0x9f, 0x86, 0xd0, 0x81];
        assert_eq!(encode(Encoding::Hex, &bytes), "9f86d081");
        assert_eq!(encode(Encoding::Base64, &bytes), "n4bQgQ==");
    }

    // -- signer --------------------------------------------------------------

    #[test]
    fn render_template_substitutes_and_uppercases_method() {
        let signer = Signer::new(Config {
            template: "{method}\n{path}\n{url}\n{body}\n{timestamp}".to_string(),
            ..sample_config()
        });
        let fields = serde_json::json!({
            "method": "post",
            "path": "/v1/orders?x=1",
            "url": "https://p.example.com/v1/orders?x=1",
            "body": "{\"sku\":\"abc\"}",
        });
        let serde_json::Value::Object(map) = fields else {
            unreachable!()
        };
        let rendered = signer.render_template(&map, 1700000000);
        assert_eq!(
            rendered,
            "POST\n/v1/orders?x=1\nhttps://p.example.com/v1/orders?x=1\n{\"sku\":\"abc\"}\n1700000000"
        );
    }

    #[test]
    fn canonical_passes_through_non_json_verbatim() {
        let signer = Signer::new(sample_config());
        // A plain (non-JSON) line is signed exactly as sent.
        assert_eq!(
            signer.canonical_for("POST/orders{body}", 42),
            "POST/orders{body}"
        );
    }

    #[test]
    fn sign_applies_prefix_and_encoding() {
        let signer = Signer::new(Config {
            prefix: "sha256=".to_string(),
            ..sample_config()
        });
        let value = signer.sign_canonical("The quick brown fox jumps over the lazy dog");
        assert_eq!(
            value,
            "sha256=f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn handle_request_line_returns_header_and_counts() {
        let signer = Signer::new(sample_config());
        // Raw canonical line -> deterministic signature; counter advances.
        let reply = signer.handle_request_line("The quick brown fox jumps over the lazy dog", 0);
        let v: serde_json::Value = serde_json::from_str(reply.trim_end()).unwrap();
        assert_eq!(v["header"], "x-signature");
        assert_eq!(
            v["value"],
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        assert!(reply.ends_with('\n'));
        assert_eq!(signer.signed.load(Ordering::Relaxed), 1);
    }

    // -- endpoint (in-memory, no socket) ------------------------------------

    #[test]
    fn serve_client_answers_each_line() {
        let signer = Signer::new(sample_config());
        let shutdown = AtomicBool::new(false);
        // Two request lines: a JSON request object and a raw canonical string.
        let mut stream = MockStream::feeding(
            "{\"method\":\"get\",\"path\":\"/a\"}\nThe quick brown fox jumps over the lazy dog\n",
        );
        serve_client(&mut stream, &signer, &shutdown);

        let out = String::from_utf8(stream.output).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);

        // Line 1: template "{method}{path}{body}" -> "GET/a".
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["header"], "x-signature");
        assert_eq!(
            first["value"],
            hex_lower(&hmac_bytes(Algo::Sha256, b"key", b"GET/a"))
        );

        // Line 2: raw canonical, known vector.
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(
            second["value"],
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        assert_eq!(signer.signed.load(Ordering::Relaxed), 2);
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn stop_is_idempotent_without_start() {
        let mut svc = HmacSigner::default();
        svc.stop();
        svc.stop(); // second stop must not panic
        assert!(svc.handle.is_none());
    }

    #[test]
    fn start_rejects_bad_config() {
        let mut svc = HmacSigner::default();
        // Missing `secret` -> start fails, nothing bound, stop still a no-op.
        let res = svc.start(RString::from(r#"{"algo":"sha256"}"#));
        assert!(matches!(res, RErr(_)));
        assert!(svc.handle.is_none());
        svc.stop();
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: serde_json::Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "hmac-signer");
    }
}
