//! `loadr-plugin-testcontainers` — a native **service** plugin in the *fixtures
//! & lifecycle* role. It stands up the throwaway backing services a test needs
//! — a Postgres, a Redis, a mock API — **before the run and tears them down
//! after**, so a plan is self-contained and a CI job needs no `docker compose`
//! sidecar step.
//!
//! # How it plugs in
//!
//! loadr's native service ABI ([`FfiService`]) has an explicit lifecycle:
//! `start(config_json) -> summary` and an idempotent `stop()`. On `start` this
//! plugin, for every declared container:
//!
//! 1. `POST /containers/create` (pulling the image first if the create 404s),
//! 2. `POST /containers/{id}/start`,
//! 3. reads the ephemeral host port Docker bound via `GET /containers/{id}/json`,
//! 4. blocks until the container's `wait` condition is satisfied.
//!
//! It then exports each published port as `LOADR_TC_<IMAGE>_<CONTAINER_PORT>`
//! and returns a JSON summary of the started containers. On `stop` it
//! `DELETE`s every container it created; `stop` is idempotent and a `start`
//! that fails partway rolls back the containers it already created.
//!
//! # Transport
//!
//! The plugin talks to the **Docker Engine directly over its HTTP API** on the
//! local `/var/run/docker.sock` (or `$DOCKER_HOST`). There is no Docker client
//! library, no CLI shell-out and no `testcontainers` SDK: HTTP/1.1 is
//! hand-rolled over a blocking `std::os::unix::net::UnixStream`, keeping this a
//! small, pure-Rust, std-only cdylib that cross-compiles cleanly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult::{self, RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiService, FfiServiceBox, FfiService_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use serde_json::{json, Value};

const NAME: &str = "testcontainers";
const DEFAULT_SOCKET: &str = "/var/run/docker.sock";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Per-request socket timeout. Generous: an image pull can take a while.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Minimal hand-rolled HTTP/1.1 over the Docker socket.
// ---------------------------------------------------------------------------

/// One parsed HTTP response from the Docker Engine.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl HttpResponse {
    fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).trim().to_string()
    }

    fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// The transport seam: performs one HTTP/1.1 request/response against the Docker
/// Engine. The real implementation opens a fresh Unix-socket connection per
/// call; tests substitute a scripted mock so no socket is ever touched.
trait DockerTransport: Send {
    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse, String>;
}

/// Build a raw HTTP/1.1 request. `Connection: close` lets the response be read
/// to EOF, so no keep-alive framing is needed on the read side.
fn build_request(method: &str, path: &str, body: Option<&[u8]>) -> Vec<u8> {
    let mut req = Vec::new();
    req.extend_from_slice(format!("{method} {path} HTTP/1.1\r\n").as_bytes());
    req.extend_from_slice(b"Host: localhost\r\n");
    req.extend_from_slice(b"Accept: application/json\r\n");
    req.extend_from_slice(b"Connection: close\r\n");
    match body {
        Some(b) => {
            req.extend_from_slice(b"Content-Type: application/json\r\n");
            req.extend_from_slice(format!("Content-Length: {}\r\n\r\n", b.len()).as_bytes());
            req.extend_from_slice(b);
        }
        None => req.extend_from_slice(b"\r\n"),
    }
    req
}

/// Find the first index of `needle` within `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse a complete (read-to-EOF) HTTP response: status line + headers + body,
/// decoding a `Transfer-Encoding: chunked` body when present.
fn parse_http_response(raw: &[u8]) -> Result<HttpResponse, String> {
    let sep = find_subsequence(raw, b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response: no header terminator".to_string())?;
    let head = String::from_utf8_lossy(&raw[..sep]);
    let mut body = raw[sep + 4..].to_vec();

    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| "empty HTTP response".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("bad HTTP status line: {status_line}"))?;

    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.trim().eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            }
        }
    }
    if chunked {
        body = dechunk(&body)?;
    }
    Ok(HttpResponse { status, body })
}

/// Decode an HTTP/1.1 chunked body.
fn dechunk(raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let rel = find_subsequence(&raw[i..], b"\r\n")
            .ok_or_else(|| "chunked body: missing size CRLF".to_string())?;
        let size_str = String::from_utf8_lossy(&raw[i..i + rel]);
        let size_hex = size_str.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|e| format!("chunked body: bad chunk size `{size_hex}`: {e}"))?;
        i += rel + 2;
        if size == 0 {
            break;
        }
        if i + size > raw.len() {
            return Err("chunked body: truncated chunk".to_string());
        }
        out.extend_from_slice(&raw[i..i + size]);
        i += size;
        if raw[i..].starts_with(b"\r\n") {
            i += 2;
        }
    }
    Ok(out)
}

/// Strip Docker's 8-byte stream-multiplexing frame headers from a log stream,
/// returning the concatenated payload. A non-framed (TTY) stream is returned
/// as-is.
fn demux_docker_logs(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 8 <= raw.len() {
        // Frame header: [stream(1)][0][0][0][size: u32 big-endian].
        let framed = raw[i] <= 2 && raw[i + 1] == 0 && raw[i + 2] == 0 && raw[i + 3] == 0;
        if !framed {
            break;
        }
        let size = u32::from_be_bytes([raw[i + 4], raw[i + 5], raw[i + 6], raw[i + 7]]) as usize;
        i += 8;
        let end = (i + size).min(raw.len());
        out.extend_from_slice(&raw[i..end]);
        i = end;
    }
    // Append any unframed remainder (TTY logs, or a partial trailing frame).
    if out.is_empty() || i < raw.len() {
        out.extend_from_slice(&raw[i..]);
    }
    out
}

/// The real transport: one blocking Unix-socket round trip per request.
#[cfg(unix)]
struct UnixSocketTransport {
    socket_path: String,
    timeout: Duration,
}

#[cfg(unix)]
impl DockerTransport for UnixSocketTransport {
    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse, String> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(&self.socket_path)
            .map_err(|e| format!("cannot connect to Docker socket {}: {e}", self.socket_path))?;
        let _ = stream.set_read_timeout(Some(self.timeout));
        let _ = stream.set_write_timeout(Some(self.timeout));

        let req = build_request(method, path, body);
        stream
            .write_all(&req)
            .map_err(|e| format!("write to Docker socket failed: {e}"))?;
        let _ = stream.flush();

        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .map_err(|e| format!("read from Docker socket failed: {e}"))?;
        parse_http_response(&buf)
    }
}

// ---------------------------------------------------------------------------
// Config parsing.
// ---------------------------------------------------------------------------

/// The readiness gate for a container.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WaitSpec {
    /// No gate: the container merely needs to be running.
    Running,
    /// Wait for a substring to appear in the container's log stream.
    Log(String),
    /// Wait for a mapped host port to accept a TCP connection. `None` = the
    /// first published port; `Some(p)` = the host port for container port `p`.
    Port(Option<u16>),
    /// Wait for the image's Docker `HEALTHCHECK` to report `healthy`.
    Healthy,
}

/// One container to bring up.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerSpec {
    image: String,
    ports: Vec<u16>,
    wait: WaitSpec,
    env: Vec<String>,
    cmd: Option<Vec<String>>,
    name: Option<String>,
}

/// The parsed `start()` configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    containers: Vec<ContainerSpec>,
    startup_timeout: Duration,
    socket: Option<String>,
}

fn parse_plan(config_json: &str) -> Result<Plan, String> {
    let cfg: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config JSON: {e}"))?;

    let entries = cfg
        .get("containers")
        .and_then(Value::as_array)
        .ok_or_else(|| "config requires a `containers` array".to_string())?;
    if entries.is_empty() {
        return Err("config `containers` must not be empty".to_string());
    }
    let mut containers = Vec::with_capacity(entries.len());
    for entry in entries {
        containers.push(parse_container(entry)?);
    }

    let startup_timeout = match cfg.get("startup_timeout") {
        Some(v) => parse_duration(v)?,
        None => Duration::from_secs(60),
    };
    let socket = cfg
        .get("socket")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    Ok(Plan {
        containers,
        startup_timeout,
        socket,
    })
}

fn parse_container(v: &Value) -> Result<ContainerSpec, String> {
    let image = v
        .get("image")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "each container requires a non-empty `image`".to_string())?
        .to_string();
    let ports = parse_ports(v.get("port"))?;
    let wait = parse_wait(v.get("wait").and_then(Value::as_str))?;
    let env = parse_env(v.get("env"));
    let cmd = v.get("cmd").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect()
    });
    let name = v
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(ContainerSpec {
        image,
        ports,
        wait,
        env,
        cmd,
        name,
    })
}

fn parse_ports(v: Option<&Value>) -> Result<Vec<u16>, String> {
    fn as_port(v: &Value) -> Result<u16, String> {
        v.as_u64()
            .filter(|n| *n > 0 && *n <= u16::MAX as u64)
            .map(|n| n as u16)
            .ok_or_else(|| "`port` must be in 1..=65535".to_string())
    }
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(n @ Value::Number(_)) => Ok(vec![as_port(n)?]),
        Some(Value::Array(arr)) => arr.iter().map(as_port).collect(),
        Some(_) => Err("`port` must be a number or an array of numbers".to_string()),
    }
}

fn parse_wait(s: Option<&str>) -> Result<WaitSpec, String> {
    match s.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(WaitSpec::Running),
        Some("healthy") => Ok(WaitSpec::Healthy),
        Some("port") => Ok(WaitSpec::Port(None)),
        Some(other) => {
            if let Some(sub) = other.strip_prefix("log:") {
                if sub.is_empty() {
                    return Err("`wait: log:` requires a substring".to_string());
                }
                Ok(WaitSpec::Log(sub.to_string()))
            } else if let Some(p) = other.strip_prefix("port:") {
                let n = p
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| format!("invalid wait port `{p}`"))?;
                Ok(WaitSpec::Port(Some(n)))
            } else {
                Err(format!(
                    "unknown wait condition `{other}` (use `log:<s>`, `port`, `port:<n>` or `healthy`)"
                ))
            }
        }
    }
}

fn parse_env(v: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(Value::Object(map)) = v {
        for (k, val) in map {
            if let Some(s) = val.as_str() {
                out.push(format!("{k}={s}"));
            } else if val.is_number() || val.is_boolean() {
                out.push(format!("{k}={val}"));
            }
        }
    }
    out
}

/// Parse a duration from a JSON number (seconds) or a string (`"60s"`,
/// `"500ms"`, `"2m"`, `"1h"`).
fn parse_duration(v: &Value) -> Result<Duration, String> {
    if let Some(n) = v.as_u64() {
        return Ok(Duration::from_secs(n));
    }
    if let Some(f) = v.as_f64() {
        if f >= 0.0 {
            return Ok(Duration::from_secs_f64(f));
        }
    }
    let s = v
        .as_str()
        .ok_or_else(|| "`startup_timeout` must be seconds or a duration string".to_string())?;
    parse_duration_str(s)
}

fn parse_duration_str(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let val: f64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid duration `{s}`"))?;
    let secs = match unit.trim() {
        "ms" => val / 1000.0,
        "" | "s" => val,
        "m" => val * 60.0,
        "h" => val * 3600.0,
        other => return Err(format!("unknown duration unit `{other}` in `{s}`")),
    };
    Ok(Duration::from_secs_f64(secs))
}

// ---------------------------------------------------------------------------
// Image name -> env var name, image splitting.
// ---------------------------------------------------------------------------

/// The image basename with its registry, path and tag/digest stripped, upper-
/// cased, with any non-alphanumeric byte replaced by `_`. `postgres:16` ->
/// `POSTGRES`, `ghcr.io/foo/bar-baz:1.2` -> `BAR_BAZ`.
fn image_basename(image: &str) -> String {
    let no_digest = image.split('@').next().unwrap_or(image);
    let after_slash = no_digest.rsplit('/').next().unwrap_or(no_digest);
    let name = after_slash.split(':').next().unwrap_or(after_slash);
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// The env var a published port is exported as: `LOADR_TC_<IMAGE>_<PORT>`.
fn env_var_name(image: &str, container_port: u16) -> String {
    format!("LOADR_TC_{}_{}", image_basename(image), container_port)
}

/// Split an image reference into `(repository, tag)` for the pull endpoint.
/// The tag is only the part after a `:` that follows the final `/`, so a
/// registry port (`localhost:5000/img`) is not mistaken for a tag.
fn split_image(image: &str) -> (String, String) {
    let no_digest = image.split('@').next().unwrap_or(image);
    let (prefix, rest) = match no_digest.rfind('/') {
        Some(i) => no_digest.split_at(i + 1),
        None => ("", no_digest),
    };
    match rest.find(':') {
        Some(i) => (format!("{prefix}{}", &rest[..i]), rest[i + 1..].to_string()),
        None => (no_digest.to_string(), "latest".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Docker Engine API payloads.
// ---------------------------------------------------------------------------

/// The `POST /containers/create` request body for a spec.
fn create_body(spec: &ContainerSpec) -> Value {
    let mut exposed = serde_json::Map::new();
    let mut bindings = serde_json::Map::new();
    for &p in &spec.ports {
        let key = format!("{p}/tcp");
        exposed.insert(key.clone(), json!({}));
        // Empty HostPort => Docker assigns an ephemeral host port.
        bindings.insert(key, json!([{ "HostIp": "127.0.0.1", "HostPort": "" }]));
    }
    let mut body = json!({
        "Image": spec.image,
        "Env": spec.env,
        "HostConfig": { "PortBindings": bindings },
    });
    if !spec.ports.is_empty() {
        body["ExposedPorts"] = Value::Object(exposed);
    }
    if let Some(cmd) = &spec.cmd {
        body["Cmd"] = json!(cmd);
    }
    body
}

/// The subset of `GET /containers/{id}/json` this plugin reads.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Inspect {
    running: bool,
    health: Option<String>,
    /// container port -> mapped host port.
    ports: BTreeMap<u16, u16>,
}

fn parse_inspect(v: &Value) -> Inspect {
    let running = v
        .pointer("/State/Running")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let health = v
        .pointer("/State/Health/Status")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut ports = BTreeMap::new();
    if let Some(obj) = v
        .pointer("/NetworkSettings/Ports")
        .and_then(Value::as_object)
    {
        for (k, val) in obj {
            let cp = k.split('/').next().and_then(|s| s.parse::<u16>().ok());
            let hp = val
                .as_array()
                .and_then(|a| a.first())
                .and_then(|b| b.get("HostPort"))
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<u16>().ok());
            if let (Some(cp), Some(hp)) = (cp, hp) {
                ports.insert(cp, hp);
            }
        }
    }
    Inspect {
        running,
        health,
        ports,
    }
}

fn parse_create(resp: &HttpResponse) -> Result<String, String> {
    if !resp.is_success() {
        return Err(format!(
            "container create failed: HTTP {} {}",
            resp.status,
            resp.body_text()
        ));
    }
    let v: Value =
        serde_json::from_slice(&resp.body).map_err(|e| format!("bad create response: {e}"))?;
    v.get("Id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "create response missing `Id`".to_string())
}

/// Map each declared container port to the host port Docker bound it to.
fn map_ports(
    declared: &[u16],
    inspected: &BTreeMap<u16, u16>,
) -> Result<BTreeMap<u16, u16>, String> {
    let mut out = BTreeMap::new();
    for &cp in declared {
        match inspected.get(&cp) {
            Some(&hp) => {
                out.insert(cp, hp);
            }
            None => {
                return Err(format!(
                    "container port {cp}/tcp was not published by Docker"
                ))
            }
        }
    }
    Ok(out)
}

/// Pick the host port a `port` wait should probe.
fn pick_wait_port(which: Option<u16>, host_ports: &BTreeMap<u16, u16>) -> Result<u16, String> {
    match which {
        Some(cp) => host_ports
            .get(&cp)
            .copied()
            .ok_or_else(|| format!("`wait: port:{cp}` but that port is not published")),
        None => host_ports
            .values()
            .next()
            .copied()
            .ok_or_else(|| "`wait: port` requires the container to publish a port".to_string()),
    }
}

/// A container this plugin created and started.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Started {
    id: String,
    image: String,
    ports: BTreeMap<u16, u16>,
}

/// Generate a unique container name so parallel runs never collide.
fn generate_name(index: usize) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("loadr-tc-{nanos:x}-{index}-{c}")
}

/// A short, log-friendly container id.
fn short_id(id: &str) -> String {
    id.chars().take(12).collect()
}

// ---------------------------------------------------------------------------
// Engine: the Docker orchestration built on the transport seam.
// ---------------------------------------------------------------------------

type TcpProbe = Box<dyn Fn(u16) -> bool + Send>;

/// Drives the Docker Engine over a [`DockerTransport`]. The `tcp_probe` seam
/// lets `port` waits be tested without a real socket.
struct Engine {
    transport: Box<dyn DockerTransport>,
    tcp_probe: TcpProbe,
}

impl Engine {
    fn create(&self, spec: &ContainerSpec, index: usize) -> Result<String, String> {
        let body = create_body(spec).to_string();
        let name = spec.name.clone().unwrap_or_else(|| generate_name(index));
        let path = format!("/containers/create?name={name}");

        let resp = self
            .transport
            .request("POST", &path, Some(body.as_bytes()))?;
        if resp.status == 404 {
            // Image not present locally: pull it, then retry the create.
            self.pull(&spec.image)?;
            let retry = self
                .transport
                .request("POST", &path, Some(body.as_bytes()))?;
            return parse_create(&retry);
        }
        parse_create(&resp)
    }

    fn pull(&self, image: &str) -> Result<(), String> {
        let (repo, tag) = split_image(image);
        let path = format!("/images/create?fromImage={repo}&tag={tag}");
        let resp = self.transport.request("POST", &path, Some(b""))?;
        if resp.is_success() {
            Ok(())
        } else {
            Err(format!(
                "pull of `{image}` failed: HTTP {} {}",
                resp.status,
                resp.body_text()
            ))
        }
    }

    fn start_container(&self, id: &str) -> Result<(), String> {
        let resp = self
            .transport
            .request("POST", &format!("/containers/{id}/start"), Some(b""))?;
        // 204 = started, 304 = already running.
        if resp.is_success() || resp.status == 304 {
            Ok(())
        } else {
            Err(format!(
                "starting container {} failed: HTTP {} {}",
                short_id(id),
                resp.status,
                resp.body_text()
            ))
        }
    }

    fn inspect(&self, id: &str) -> Result<Inspect, String> {
        let resp = self
            .transport
            .request("GET", &format!("/containers/{id}/json"), None)?;
        if !resp.is_success() {
            return Err(format!(
                "inspecting container {} failed: HTTP {} {}",
                short_id(id),
                resp.status,
                resp.body_text()
            ));
        }
        let v: Value =
            serde_json::from_slice(&resp.body).map_err(|e| format!("bad inspect response: {e}"))?;
        Ok(parse_inspect(&v))
    }

    fn logs(&self, id: &str) -> Result<Vec<u8>, String> {
        let resp = self.transport.request(
            "GET",
            &format!("/containers/{id}/logs?stdout=1&stderr=1&tail=200"),
            None,
        )?;
        if !resp.is_success() {
            return Err(format!(
                "reading logs for container {} failed: HTTP {} {}",
                short_id(id),
                resp.status,
                resp.body_text()
            ));
        }
        Ok(demux_docker_logs(&resp.body))
    }

    fn remove(&self, id: &str) -> Result<(), String> {
        let resp =
            self.transport
                .request("DELETE", &format!("/containers/{id}?force=1&v=1"), None)?;
        // 404 => already gone: teardown stays idempotent.
        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            Err(format!(
                "removing container {} failed: HTTP {} {}",
                short_id(id),
                resp.status,
                resp.body_text()
            ))
        }
    }

    /// Block until `wait` is satisfied or `deadline` passes.
    fn wait_ready(
        &self,
        id: &str,
        wait: &WaitSpec,
        host_ports: &BTreeMap<u16, u16>,
        deadline: Instant,
    ) -> Result<(), String> {
        loop {
            let ready = match wait {
                WaitSpec::Running => self.inspect(id)?.running,
                WaitSpec::Healthy => self.inspect(id)?.health.as_deref() == Some("healthy"),
                WaitSpec::Log(substr) => {
                    let logs = self.logs(id)?;
                    String::from_utf8_lossy(&logs).contains(substr.as_str())
                }
                WaitSpec::Port(which) => {
                    let port = pick_wait_port(*which, host_ports)?;
                    (self.tcp_probe)(port)
                }
            };
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "container {} did not become ready before startup_timeout",
                    short_id(id)
                ));
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// Create + start + wait for one container. Cleans up its own partially
    /// created container if any step after `create` fails.
    fn launch_one(
        &self,
        spec: &ContainerSpec,
        index: usize,
        deadline: Instant,
    ) -> Result<Started, String> {
        let id = self.create(spec, index)?;
        let outcome = self.bring_running(&id, spec, deadline);
        match outcome {
            Ok(host_ports) => Ok(Started {
                id,
                image: spec.image.clone(),
                ports: host_ports,
            }),
            Err(e) => {
                let _ = self.remove(&id);
                Err(e)
            }
        }
    }

    fn bring_running(
        &self,
        id: &str,
        spec: &ContainerSpec,
        deadline: Instant,
    ) -> Result<BTreeMap<u16, u16>, String> {
        self.start_container(id)?;
        let inspect = self.inspect(id)?;
        let host_ports = map_ports(&spec.ports, &inspect.ports)?;
        self.wait_ready(id, &spec.wait, &host_ports, deadline)?;
        Ok(host_ports)
    }

    /// Bring up every container in the plan, rolling back the ones already
    /// created if any fails.
    fn bring_up(&self, plan: &Plan) -> Result<Vec<Started>, String> {
        let deadline = Instant::now() + plan.startup_timeout;
        let mut started: Vec<Started> = Vec::new();
        for (index, spec) in plan.containers.iter().enumerate() {
            match self.launch_one(spec, index, deadline) {
                Ok(s) => started.push(s),
                Err(e) => {
                    for prior in &started {
                        let _ = self.remove(&prior.id);
                    }
                    return Err(e);
                }
            }
        }
        Ok(started)
    }
}

/// The default real probe: try to open a TCP connection to `127.0.0.1:port`.
fn default_tcp_probe(port: u16) -> bool {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

/// Build an [`Engine`] backed by the real Unix-socket transport.
#[cfg(unix)]
fn connect_engine(socket: Option<&str>) -> Result<Engine, String> {
    let socket_path = resolve_socket_path(socket);
    Ok(Engine {
        transport: Box::new(UnixSocketTransport {
            socket_path,
            timeout: REQUEST_TIMEOUT,
        }),
        tcp_probe: Box::new(default_tcp_probe),
    })
}

#[cfg(not(unix))]
fn connect_engine(_socket: Option<&str>) -> Result<Engine, String> {
    Err("the testcontainers plugin requires a Unix Docker socket".to_string())
}

/// Resolve the Docker socket path: explicit `socket`, else `$DOCKER_HOST`, else
/// `/var/run/docker.sock`. A `unix://` scheme prefix is stripped.
fn resolve_socket_path(explicit: Option<&str>) -> String {
    if let Some(p) = explicit.filter(|s| !s.is_empty()) {
        return strip_unix_scheme(p);
    }
    if let Ok(h) = std::env::var("DOCKER_HOST") {
        if !h.is_empty() {
            return strip_unix_scheme(&h);
        }
    }
    DEFAULT_SOCKET.to_string()
}

fn strip_unix_scheme(s: &str) -> String {
    s.strip_prefix("unix://").unwrap_or(s).to_string()
}

/// The JSON summary `start()` returns: the started containers and their
/// `image -> host port` mappings.
fn summary_json(started: &[Started]) -> String {
    let containers: Vec<Value> = started
        .iter()
        .map(|s| {
            let ports: serde_json::Map<String, Value> = s
                .ports
                .iter()
                .map(|(cp, hp)| (cp.to_string(), json!(hp)))
                .collect();
            json!({
                "id": short_id(&s.id),
                "image": s.image,
                "ports": Value::Object(ports),
            })
        })
        .collect();
    json!({
        "containers": containers,
        "containers_started": started.len(),
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// The service plugin.
// ---------------------------------------------------------------------------

/// The service plugin instance.
#[derive(Default)]
struct Testcontainers {
    engine: Option<Engine>,
    started: Vec<Started>,
    removed: u64,
}

impl Testcontainers {
    /// Bring the plan up on `engine`, exporting the mapped ports as env vars.
    fn start_with(&mut self, engine: Engine, plan: &Plan) -> Result<String, String> {
        let started = engine.bring_up(plan)?;
        for s in &started {
            for (&cp, &hp) in &s.ports {
                std::env::set_var(env_var_name(&s.image, cp), hp.to_string());
            }
        }
        let summary = summary_json(&started);
        self.started = started;
        self.engine = Some(engine);
        Ok(summary)
    }
}

impl FfiService for Testcontainers {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn start(&mut self, config_json: RString) -> RResult<RString, RString> {
        if self.engine.is_some() {
            // Already started: return the existing summary rather than re-run.
            return ROk(RString::from(summary_json(&self.started)));
        }
        let plan = match parse_plan(config_json.as_str()) {
            Ok(p) => p,
            Err(e) => return RErr(RString::from(e)),
        };
        let engine = match connect_engine(plan.socket.as_deref()) {
            Ok(e) => e,
            Err(e) => return RErr(RString::from(e)),
        };
        match self.start_with(engine, &plan) {
            Ok(summary) => ROk(RString::from(summary)),
            Err(e) => RErr(RString::from(e)),
        }
    }

    fn stop(&mut self) {
        // Idempotent: a no-op once the engine has been taken.
        if let Some(engine) = self.engine.take() {
            for s in &self.started {
                if engine.remove(&s.id).is_ok() {
                    self.removed += 1;
                }
            }
        }
        self.started.clear();
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description":
                "Starts declared containers for the run (Docker Engine API over the local socket) and removes them afterwards",
        })
        .to_string(),
    )
}

extern "C" fn make_service() -> FfiServiceBox {
    FfiService_TO::from_value(
        Testcontainers::default(),
        abi_stable::erased_types::TD_Opaque,
    )
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
// Tests — all offline: the Docker Engine is a scripted in-memory mock, and the
// `port` wait is a fake probe, so no socket is ever opened.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // -- HTTP wire format ----------------------------------------------------

    #[test]
    fn builds_request_with_body() {
        let req = build_request("POST", "/containers/create", Some(b"hello"));
        let text = String::from_utf8_lossy(&req);
        assert!(
            text.starts_with("POST /containers/create HTTP/1.1\r\n"),
            "{text}"
        );
        assert!(text.contains("Content-Length: 5\r\n"), "{text}");
        assert!(text.contains("Connection: close\r\n"), "{text}");
        assert!(text.ends_with("\r\n\r\nhello"), "{text}");
    }

    #[test]
    fn builds_request_without_body() {
        let req = build_request("GET", "/containers/x/json", None);
        let text = String::from_utf8_lossy(&req);
        assert!(
            text.starts_with("GET /containers/x/json HTTP/1.1\r\n"),
            "{text}"
        );
        assert!(!text.contains("Content-Length"), "{text}");
        assert!(text.ends_with("\r\n\r\n"), "{text}");
    }

    #[test]
    fn parses_content_length_response() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n\r\n{\"Id\":\"abc\"}";
        let resp = parse_http_response(raw).unwrap();
        assert_eq!(resp.status, 201);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["Id"], "abc");
    }

    #[test]
    fn parses_chunked_response() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        let resp = parse_http_response(raw).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn dechunk_concatenates_chunks() {
        let raw = b"3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n";
        assert_eq!(dechunk(raw).unwrap(), b"foobar");
    }

    #[test]
    fn rejects_malformed_response() {
        assert!(parse_http_response(b"not http at all").is_err());
    }

    #[test]
    fn demuxes_framed_logs() {
        // stdout frame (stream=1), size=5, payload "ready".
        let mut raw = vec![1u8, 0, 0, 0, 0, 0, 0, 5];
        raw.extend_from_slice(b"ready");
        assert_eq!(demux_docker_logs(&raw), b"ready");
    }

    #[test]
    fn demux_passes_through_unframed_logs() {
        assert_eq!(
            demux_docker_logs(b"plain tty log line"),
            b"plain tty log line"
        );
    }

    #[test]
    fn demuxes_multiple_frames() {
        let mut raw = vec![1u8, 0, 0, 0, 0, 0, 0, 3];
        raw.extend_from_slice(b"abc");
        raw.extend_from_slice(&[2u8, 0, 0, 0, 0, 0, 0, 3]);
        raw.extend_from_slice(b"def");
        assert_eq!(demux_docker_logs(&raw), b"abcdef");
    }

    // -- image naming --------------------------------------------------------

    #[test]
    fn image_basename_strips_registry_and_tag() {
        assert_eq!(image_basename("postgres:16"), "POSTGRES");
        assert_eq!(image_basename("redis:7-alpine"), "REDIS");
        assert_eq!(image_basename("ghcr.io/foo/bar-baz:1.2"), "BAR_BAZ");
        assert_eq!(image_basename("localhost:5000/img"), "IMG");
    }

    #[test]
    fn env_var_name_composes() {
        assert_eq!(env_var_name("postgres:16", 5432), "LOADR_TC_POSTGRES_5432");
    }

    #[test]
    fn split_image_separates_repo_and_tag() {
        assert_eq!(split_image("postgres:16"), ("postgres".into(), "16".into()));
        assert_eq!(split_image("redis"), ("redis".into(), "latest".into()));
        assert_eq!(
            split_image("ghcr.io/foo/bar:2"),
            ("ghcr.io/foo/bar".into(), "2".into())
        );
        // Registry port must not be read as a tag.
        assert_eq!(
            split_image("localhost:5000/img"),
            ("localhost:5000/img".into(), "latest".into())
        );
    }

    // -- config parsing ------------------------------------------------------

    #[test]
    fn parse_plan_requires_non_empty_containers() {
        assert!(parse_plan("{}").is_err());
        assert!(parse_plan(r#"{"containers":[]}"#).is_err());
        assert!(parse_plan("not json").is_err());
    }

    #[test]
    fn parse_plan_reads_full_container() {
        let plan = parse_plan(
            r#"{
                "startup_timeout": "30s",
                "socket": "/run/docker.sock",
                "containers": [
                    {
                        "image": "postgres:16",
                        "port": 5432,
                        "wait": "log:ready to accept connections",
                        "env": {"POSTGRES_PASSWORD": "test", "N": 3},
                        "cmd": ["postgres", "-c", "max_connections=50"],
                        "name": "db"
                    }
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(plan.startup_timeout, Duration::from_secs(30));
        assert_eq!(plan.socket.as_deref(), Some("/run/docker.sock"));
        let c = &plan.containers[0];
        assert_eq!(c.image, "postgres:16");
        assert_eq!(c.ports, vec![5432]);
        assert_eq!(c.wait, WaitSpec::Log("ready to accept connections".into()));
        assert!(c.env.contains(&"POSTGRES_PASSWORD=test".to_string()));
        assert!(c.env.contains(&"N=3".to_string()));
        assert_eq!(c.name.as_deref(), Some("db"));
        assert_eq!(c.cmd.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn parse_ports_accepts_number_or_array() {
        assert_eq!(parse_ports(Some(&json!(5432))).unwrap(), vec![5432]);
        assert_eq!(
            parse_ports(Some(&json!([5432, 8080]))).unwrap(),
            vec![5432, 8080]
        );
        assert!(parse_ports(None).unwrap().is_empty());
        assert!(parse_ports(Some(&json!(0))).is_err());
        assert!(parse_ports(Some(&json!(70000))).is_err());
        assert!(parse_ports(Some(&json!("nope"))).is_err());
    }

    #[test]
    fn parse_wait_covers_all_forms() {
        assert_eq!(parse_wait(None).unwrap(), WaitSpec::Running);
        assert_eq!(parse_wait(Some("")).unwrap(), WaitSpec::Running);
        assert_eq!(parse_wait(Some("healthy")).unwrap(), WaitSpec::Healthy);
        assert_eq!(parse_wait(Some("port")).unwrap(), WaitSpec::Port(None));
        assert_eq!(
            parse_wait(Some("port:5432")).unwrap(),
            WaitSpec::Port(Some(5432))
        );
        assert_eq!(
            parse_wait(Some("log:up")).unwrap(),
            WaitSpec::Log("up".into())
        );
        assert!(parse_wait(Some("log:")).is_err());
        assert!(parse_wait(Some("port:xyz")).is_err());
        assert!(parse_wait(Some("bogus")).is_err());
    }

    #[test]
    fn parse_duration_forms() {
        assert_eq!(parse_duration(&json!(45)).unwrap(), Duration::from_secs(45));
        assert_eq!(
            parse_duration(&json!("500ms")).unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration(&json!("2m")).unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            parse_duration(&json!("1h")).unwrap(),
            Duration::from_secs(3600)
        );
        assert!(parse_duration(&json!("10x")).is_err());
    }

    // -- payload building ----------------------------------------------------

    #[test]
    fn create_body_publishes_ports_and_env() {
        let spec = ContainerSpec {
            image: "postgres:16".into(),
            ports: vec![5432],
            wait: WaitSpec::Running,
            env: vec!["POSTGRES_PASSWORD=test".into()],
            cmd: Some(vec!["postgres".into()]),
            name: None,
        };
        let body = create_body(&spec);
        assert_eq!(body["Image"], "postgres:16");
        assert_eq!(body["Env"][0], "POSTGRES_PASSWORD=test");
        assert_eq!(body["Cmd"][0], "postgres");
        assert!(body["ExposedPorts"].get("5432/tcp").is_some());
        assert_eq!(
            body["HostConfig"]["PortBindings"]["5432/tcp"][0]["HostPort"],
            ""
        );
    }

    #[test]
    fn parse_inspect_reads_state_and_ports() {
        let v = json!({
            "State": {"Running": true, "Health": {"Status": "healthy"}},
            "NetworkSettings": {"Ports": {
                "5432/tcp": [{"HostIp": "0.0.0.0", "HostPort": "49153"}]
            }}
        });
        let insp = parse_inspect(&v);
        assert!(insp.running);
        assert_eq!(insp.health.as_deref(), Some("healthy"));
        assert_eq!(insp.ports.get(&5432), Some(&49153));
    }

    #[test]
    fn map_ports_errors_on_unpublished() {
        let mut inspected = BTreeMap::new();
        inspected.insert(5432u16, 49153u16);
        assert_eq!(
            map_ports(&[5432], &inspected).unwrap().get(&5432),
            Some(&49153)
        );
        assert!(map_ports(&[6379], &inspected).is_err());
    }

    #[test]
    fn pick_wait_port_selects() {
        let mut ports = BTreeMap::new();
        ports.insert(5432u16, 49153u16);
        assert_eq!(pick_wait_port(None, &ports).unwrap(), 49153);
        assert_eq!(pick_wait_port(Some(5432), &ports).unwrap(), 49153);
        assert!(pick_wait_port(Some(6379), &ports).is_err());
        assert!(pick_wait_port(None, &BTreeMap::new()).is_err());
    }

    // -- scripted Docker Engine mock ----------------------------------------

    type Handler = Box<dyn Fn(&str, &str, Option<&[u8]>) -> Result<HttpResponse, String> + Send>;

    struct MockTransport {
        handler: Handler,
        calls: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl DockerTransport for MockTransport {
        fn request(
            &self,
            method: &str,
            path: &str,
            body: Option<&[u8]>,
        ) -> Result<HttpResponse, String> {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), path.to_string()));
            (self.handler)(method, path, body)
        }
    }

    fn resp(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            body: body.as_bytes().to_vec(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn test_engine<H, P>(handler: H, probe: P) -> (Engine, Arc<Mutex<Vec<(String, String)>>>)
    where
        H: Fn(&str, &str, Option<&[u8]>) -> Result<HttpResponse, String> + Send + 'static,
        P: Fn(u16) -> bool + Send + 'static,
    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let engine = Engine {
            transport: Box::new(MockTransport {
                handler: Box::new(handler),
                calls: calls.clone(),
            }),
            tcp_probe: Box::new(probe),
        };
        (engine, calls)
    }

    fn container(image: &str, wait: WaitSpec) -> ContainerSpec {
        ContainerSpec {
            image: image.into(),
            ports: vec![5432],
            wait,
            env: Vec::new(),
            cmd: None,
            name: Some("fixed".into()),
        }
    }

    const INSPECT_BODY: &str = r#"{"State":{"Running":true},"NetworkSettings":{"Ports":{"5432/tcp":[{"HostIp":"0.0.0.0","HostPort":"49153"}]}}}"#;

    // Short create-response bodies keep the mock match arms within rustfmt's
    // line width. Ids only matter where a test asserts on a rollback DELETE.
    const CREATE_OK: &str = r#"{"Id":"c1"}"#;
    const CREATE_CBAD: &str = r#"{"Id":"cbad"}"#;
    const CREATE_GOOD: &str = r#"{"Id":"good1"}"#;
    const CREATE_CTO: &str = r#"{"Id":"cto"}"#;
    const NO_IMAGE: &str = r#"{"message":"No such image"}"#;

    #[test]
    fn bring_up_creates_starts_waits_and_maps_ports() {
        let (engine, calls) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_OK),
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    // framed stdout log carrying "ready".
                    ("GET", p) if p.contains("/logs") => {
                        let mut b = vec![1u8, 0, 0, 0, 0, 0, 0, 5];
                        b.extend_from_slice(b"ready");
                        HttpResponse {
                            status: 200,
                            body: b,
                        }
                    }
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        let plan = Plan {
            containers: vec![container("postgres:16", WaitSpec::Log("ready".into()))],
            startup_timeout: Duration::from_secs(5),
            socket: None,
        };
        let started = engine.bring_up(&plan).unwrap();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].ports.get(&5432), Some(&49153));

        let recorded = calls.lock().unwrap();
        assert!(recorded
            .iter()
            .any(|(m, p)| m == "POST" && p.contains("/create")));
        assert!(recorded
            .iter()
            .any(|(m, p)| m == "POST" && p.ends_with("/start")));
    }

    #[test]
    fn create_retries_after_pull_on_404() {
        let creates = Arc::new(Mutex::new(0u32));
        let creates_h = creates.clone();
        let (engine, calls) = test_engine(
            move |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => {
                        let mut n = creates_h.lock().unwrap();
                        *n += 1;
                        if *n == 1 {
                            resp(404, NO_IMAGE)
                        } else {
                            resp(201, CREATE_OK)
                        }
                    }
                    ("POST", p) if p.contains("/images/create") => resp(200, "{}"),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        let id = engine
            .create(&container("postgres:16", WaitSpec::Running), 0)
            .unwrap();
        assert_eq!(id, "c1");
        assert_eq!(
            *creates.lock().unwrap(),
            2,
            "created twice: before + after pull"
        );
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .any(|(m, p)| m == "POST" && p.contains("/images/create")));
    }

    #[test]
    fn launch_one_removes_partial_container_on_start_failure() {
        let (engine, calls) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_CBAD),
                    ("POST", p) if p.ends_with("/start") => resp(500, "boom"),
                    ("DELETE", _) => resp(204, ""),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        let err = engine
            .launch_one(&container("postgres:16", WaitSpec::Running), 0, deadline)
            .unwrap_err();
        assert!(err.contains("start"), "{err}");
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .any(|(m, p)| m == "DELETE" && p.contains("cbad")),
            "partial container must be removed"
        );
    }

    #[test]
    fn bring_up_rolls_back_earlier_containers_on_later_failure() {
        let (engine, calls) = test_engine(
            |method, path, body| {
                // First container (name c-good) fully succeeds; second create
                // fails, so the first must be rolled back.
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => {
                        let b = body.map(String::from_utf8_lossy).unwrap_or_default();
                        if b.contains("good-image") {
                            resp(201, CREATE_GOOD)
                        } else {
                            resp(500, "create refused")
                        }
                    }
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    ("DELETE", _) => resp(204, ""),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        let good = ContainerSpec {
            image: "good-image:1".into(),
            ports: vec![5432],
            wait: WaitSpec::Running,
            env: Vec::new(),
            cmd: None,
            name: Some("c-good".into()),
        };
        let bad = ContainerSpec {
            image: "bad-image:1".into(),
            ports: vec![],
            wait: WaitSpec::Running,
            env: Vec::new(),
            cmd: None,
            name: Some("c-bad".into()),
        };
        let plan = Plan {
            containers: vec![good, bad],
            startup_timeout: Duration::from_secs(5),
            socket: None,
        };
        assert!(engine.bring_up(&plan).is_err());
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .any(|(m, p)| m == "DELETE" && p.contains("good1")),
            "first container must be rolled back"
        );
    }

    #[test]
    fn wait_ready_times_out_and_launch_cleans_up() {
        let (engine, calls) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_CTO),
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    // logs never contain the awaited substring.
                    ("GET", p) if p.contains("/logs") => resp(200, "still booting"),
                    ("DELETE", _) => resp(204, ""),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        // Zero timeout => one readiness check, then immediate timeout (no sleep).
        let deadline = Instant::now();
        let err = engine
            .launch_one(
                &container("postgres:16", WaitSpec::Log("ready".into())),
                0,
                deadline,
            )
            .unwrap_err();
        assert!(err.contains("ready"), "{err}");
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .any(|(m, p)| m == "DELETE" && p.contains("cto")));
    }

    #[test]
    fn wait_ready_port_uses_probe() {
        let (engine, _) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_OK),
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    _ => resp(500, "unexpected"),
                })
            },
            // Probe reports the mapped host port open immediately.
            |port| port == 49153,
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        let started = engine
            .launch_one(&container("postgres:16", WaitSpec::Port(None)), 0, deadline)
            .unwrap();
        assert_eq!(started.ports.get(&5432), Some(&49153));
    }

    // -- service lifecycle ---------------------------------------------------

    #[test]
    fn start_with_sets_env_and_returns_summary() {
        let (engine, _) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_OK),
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        let plan = Plan {
            containers: vec![container("postgres:16", WaitSpec::Running)],
            startup_timeout: Duration::from_secs(5),
            socket: None,
        };
        let mut svc = Testcontainers::default();
        let summary = svc.start_with(engine, &plan).unwrap();
        let v: Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(v["containers_started"], 1);
        assert_eq!(v["containers"][0]["ports"]["5432"], 49153);
        assert_eq!(
            std::env::var("LOADR_TC_POSTGRES_5432").ok(),
            Some("49153".to_string())
        );
        std::env::remove_var("LOADR_TC_POSTGRES_5432");
    }

    #[test]
    fn stop_removes_started_containers_and_is_idempotent() {
        let (engine, calls) = test_engine(
            |method, path, _| {
                Ok(match (method, path) {
                    ("POST", p) if p.contains("/containers/create") => resp(201, CREATE_OK),
                    ("POST", p) if p.ends_with("/start") => resp(204, ""),
                    ("GET", p) if p.ends_with("/json") => resp(200, INSPECT_BODY),
                    ("DELETE", _) => resp(204, ""),
                    _ => resp(500, "unexpected"),
                })
            },
            |_| true,
        );
        // A distinct image => distinct env var, so this test never races the
        // `LOADR_TC_POSTGRES_5432` assertion in another test.
        let plan = Plan {
            containers: vec![container("stopimg:1", WaitSpec::Running)],
            startup_timeout: Duration::from_secs(5),
            socket: None,
        };
        let mut svc = Testcontainers::default();
        svc.start_with(engine, &plan).unwrap();
        svc.stop();
        assert_eq!(svc.removed, 1);
        assert!(svc.engine.is_none());
        assert!(svc.started.is_empty());
        assert!(calls.lock().unwrap().iter().any(|(m, _)| m == "DELETE"));
        // Second stop is a no-op and must not panic.
        svc.stop();
        assert_eq!(svc.removed, 1);
        std::env::remove_var("LOADR_TC_STOPIMG_5432");
    }

    #[test]
    fn ffi_start_rejects_bad_config() {
        let mut svc = Testcontainers::default();
        let res = svc.start(RString::from(r#"{"containers":[]}"#));
        assert!(matches!(res, RErr(_)));
        assert!(svc.engine.is_none());
        svc.stop(); // still a no-op
    }

    #[test]
    fn stop_without_start_is_noop() {
        let mut svc = Testcontainers::default();
        svc.stop();
        svc.stop();
        assert!(svc.engine.is_none());
        assert_eq!(svc.removed, 0);
    }

    // -- socket resolution ---------------------------------------------------

    #[test]
    fn resolve_socket_path_prefers_explicit_and_strips_scheme() {
        assert_eq!(
            resolve_socket_path(Some("unix:///custom.sock")),
            "/custom.sock"
        );
        assert_eq!(resolve_socket_path(Some("/plain.sock")), "/plain.sock");
    }

    #[test]
    fn info_declares_service_kind() {
        let info = plugin_info();
        let v: Value = serde_json::from_str(info.as_str()).unwrap();
        assert_eq!(v["kind"], "service");
        assert_eq!(v["name"], "testcontainers");
    }
}
