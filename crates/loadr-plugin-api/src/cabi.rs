//! Plain C-ABI loader for protocol plugins authored in **non-Rust** languages
//! (C, Go, Zig, ...).
//!
//! The existing [`crate::native`] path is Rust-to-Rust: it relies on
//! `abi_stable`'s compile-time layout handshake, which no other language can
//! reproduce. This module defines a *frozen, minimal C ABI* — pointers,
//! lengths, and a plugin-owned allocator contract — so any toolchain that can
//! emit a C shared library can implement a loadr **protocol** plugin.
//!
//! # The C symbol contract (version [`LOADR_C_ABI_VERSION`])
//!
//! A plugin cdylib MUST export these `extern "C"` symbols:
//!
//! ```c
//! // ABI version this plugin targets. The host refuses to load a plugin
//! // whose version it does not understand.
//! uint32_t loadr_plugin_abi_version(void);
//!
//! // PluginInfo as UTF-8 JSON. `*out_len` receives the byte length.
//! // The returned buffer is owned by the plugin; the host copies it and
//! // then hands it back to `loadr_plugin_free`.
//! uint8_t *loadr_plugin_info(size_t *out_len);
//!
//! // Execute one request. `req`/`req_len` is a UTF-8 JSON `FfiRequest`.
//! // Returns a UTF-8 JSON `FfiResponse` of length `*out_len`, owned by the
//! // plugin (freed via `loadr_plugin_free`). MUST NOT panic / unwind across
//! // the boundary; report failures in the response `error` field.
//! //
//! // THREADING: the host calls this concurrently from many worker threads.
//! // Implementations MUST be thread-safe (the Rust equivalent is
//! // `FfiProtocol: Send + Sync`).
//! uint8_t *loadr_plugin_execute(const uint8_t *req, size_t req_len, size_t *out_len);
//!
//! // Free a buffer previously returned by info()/execute(). Pairing the
//! // allocator with the deallocator on the *plugin* side keeps allocation
//! // symmetric across the FFI boundary (the host never frees plugin memory
//! // with its own allocator).
//! void loadr_plugin_free(uint8_t *ptr, size_t len);
//! ```
//!
//! All buffers are plugin-owned: the host copies the bytes it needs, then
//! calls `loadr_plugin_free(ptr, len)` with the exact `ptr`/`len` the plugin
//! returned. A null return with `*out_len == 0` is treated as an empty buffer
//! (and is **not** passed to `free`).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use libloading::{Library, Symbol};

use loadr_core::error::ProtocolError;
use loadr_core::{PreparedRequest, ProtocolHandler, ProtocolResponse, VuContext};

use crate::error::PluginError;
use crate::native::{
    cache_protocol_config, ffi_request_to_vec, response_from_ffi, CachedProtocolConfig,
    FfiResponse, OwnedFfiRequest,
};
use crate::PluginInfo;

/// The frozen C-ABI version. Bumped only on an incompatible change to the
/// C symbol contract above. Independent of [`crate::abi::LOADR_PLUGIN_ABI_VERSION`]
/// (the abi_stable surface), which evolves separately.
pub const LOADR_C_ABI_VERSION: u32 = 1;

/// The exported-symbol names, kept in one place so the loader and docs agree.
mod sym {
    pub const ABI_VERSION: &[u8] = b"loadr_plugin_abi_version";
    pub const INFO: &[u8] = b"loadr_plugin_info";
    pub const EXECUTE: &[u8] = b"loadr_plugin_execute";
    pub const FREE: &[u8] = b"loadr_plugin_free";
}

type AbiVersionFn = unsafe extern "C" fn() -> u32;
type InfoFn = unsafe extern "C" fn(out_len: *mut usize) -> *mut u8;
type ExecuteFn =
    unsafe extern "C" fn(req: *const u8, req_len: usize, out_len: *mut usize) -> *mut u8;
type FreeFn = unsafe extern "C" fn(ptr: *mut u8, len: usize);

/// Cheaply probe whether `path` is a C-ABI plugin (exports
/// `loadr_plugin_abi_version`) rather than an `abi_stable` [`crate::native`]
/// plugin. Used by the unified loader to pick a path without committing to a
/// full load. Returns `false` (rather than erroring) when the library cannot
/// be opened or the symbol is absent.
pub fn is_c_abi_plugin(path: &Path) -> bool {
    // SAFETY: opening a library and looking up a symbol runs no plugin code;
    // any constructors are the plugin author's responsibility, same as the
    // abi_stable path. We immediately drop the handle.
    unsafe {
        match Library::new(as_os(path)) {
            Ok(lib) => lib.get::<AbiVersionFn>(sym::ABI_VERSION).is_ok(),
            Err(_) => false,
        }
    }
}

fn as_os(path: &Path) -> &OsStr {
    path.as_os_str()
}

/// A loaded C-ABI plugin library.
///
/// Like the abi_stable [`crate::native::NativePlugin`], the underlying
/// [`Library`] is leaked for the process lifetime so that the raw function
/// pointers we hand out stay valid (a plugin may keep state in process-global
/// storage that must not be torn down mid-run).
pub struct CAbiPlugin {
    info: PluginInfo,
    path: PathBuf,
    /// Resolved at load time; valid for the process lifetime (see above).
    execute: ExecuteFn,
    free: FreeFn,
}

impl std::fmt::Debug for CAbiPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CAbiPlugin")
            .field("info", &self.info)
            .field("path", &self.path)
            .finish()
    }
}

impl CAbiPlugin {
    /// Load a C-ABI plugin dynamic library, validate its ABI version, and read
    /// its [`PluginInfo`].
    pub fn load(path: &Path) -> Result<CAbiPlugin, PluginError> {
        // SAFETY: see module docs. We never unload the library.
        let lib = unsafe { Library::new(as_os(path)) }.map_err(|e| PluginError::Load {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;

        // SAFETY: the symbols are looked up by the names in `sym`; the function
        // signatures match the documented C contract. Calling them runs plugin
        // code that the contract requires not to unwind.
        unsafe {
            let abi_version: Symbol<AbiVersionFn> =
                lib.get(sym::ABI_VERSION).map_err(|e| PluginError::Load {
                    path: path.display().to_string(),
                    message: format!("missing `loadr_plugin_abi_version`: {e}"),
                })?;
            let version = abi_version();
            if version != LOADR_C_ABI_VERSION {
                return Err(PluginError::AbiVersion {
                    host: LOADR_C_ABI_VERSION,
                    plugin: version,
                });
            }

            let info_fn: Symbol<InfoFn> = lib.get(sym::INFO).map_err(|e| PluginError::Load {
                path: path.display().to_string(),
                message: format!("missing `loadr_plugin_info`: {e}"),
            })?;
            let execute: Symbol<ExecuteFn> =
                lib.get(sym::EXECUTE).map_err(|e| PluginError::Load {
                    path: path.display().to_string(),
                    message: format!("missing `loadr_plugin_execute`: {e}"),
                })?;
            let free: Symbol<FreeFn> = lib.get(sym::FREE).map_err(|e| PluginError::Load {
                path: path.display().to_string(),
                message: format!("missing `loadr_plugin_free`: {e}"),
            })?;

            // Resolve the raw pointers we keep, then leak the library so they
            // remain valid for the process lifetime.
            let execute = *execute;
            let free = *free;
            let info_fn = *info_fn;

            let info_bytes = call_alloc(info_fn, free, path)?;
            let info: PluginInfo =
                serde_json::from_slice(&info_bytes).map_err(|e| PluginError::Load {
                    path: path.display().to_string(),
                    message: format!("invalid plugin info JSON: {e}"),
                })?;

            std::mem::forget(lib);

            tracing::debug!(
                name = %info.name,
                kind = %info.kind,
                path = %path.display(),
                "loaded C-ABI plugin",
            );

            Ok(CAbiPlugin {
                info,
                path: path.to_path_buf(),
                execute,
                free,
            })
        }
    }

    pub fn info(&self) -> &PluginInfo {
        &self.info
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Instantiate this plugin's protocol handler. Unlike the abi_stable path
    /// there is no separate constructor: a C-ABI plugin is a single shared,
    /// thread-safe `execute` function, so the adapter just borrows the resolved
    /// pointers.
    pub fn make_protocol(
        &self,
        config: serde_json::Value,
    ) -> Result<CAbiProtocolAdapter, PluginError> {
        if crate::manifest::PluginKind::parse(&self.info.kind)
            != Some(crate::manifest::PluginKind::Protocol)
        {
            return Err(PluginError::KindMismatch {
                name: self.info.name.clone(),
                expected: "protocol".to_string(),
                actual: self.info.kind.clone(),
            });
        }
        let config = cache_protocol_config(config)?;
        Ok(CAbiProtocolAdapter {
            name: self.info.name.clone(),
            config,
            execute: self.execute,
            free: self.free,
        })
    }
}

/// Call an allocating C function `(out_len*) -> *mut u8`, copy the bytes into a
/// Rust `Vec`, then hand the buffer back to the plugin's `free`. A null pointer
/// with `len == 0` is an empty buffer (and is not freed).
///
/// SAFETY: `alloc`/`free` must be the plugin's matching allocator pair, the
/// returned pointer valid for `*out_len` bytes.
unsafe fn call_alloc(alloc: InfoFn, free: FreeFn, path: &Path) -> Result<Vec<u8>, PluginError> {
    let mut len: usize = 0;
    let ptr = alloc(&mut len as *mut usize);
    copy_and_free(ptr, len, free, path)
}

/// Shared "copy the plugin buffer, then free it" helper for the raw pointer
/// returned by `info`/`execute`.
unsafe fn copy_and_free(
    ptr: *mut u8,
    len: usize,
    free: FreeFn,
    path: &Path,
) -> Result<Vec<u8>, PluginError> {
    if ptr.is_null() {
        if len != 0 {
            return Err(PluginError::Call(format!(
                "plugin `{}` returned a null buffer with non-zero length {len}",
                path.display()
            )));
        }
        return Ok(Vec::new());
    }
    let bytes = std::slice::from_raw_parts(ptr, len).to_vec();
    free(ptr, len);
    Ok(bytes)
}

/// Bridges a C-ABI protocol plugin to [`loadr_core::ProtocolHandler`], the same
/// engine-facing trait the abi_stable [`crate::native::NativeProtocolAdapter`]
/// satisfies — so scheme routing, metrics, and the registry treat both kinds
/// of plugin identically.
pub struct CAbiProtocolAdapter {
    name: String,
    config: CachedProtocolConfig,
    execute: ExecuteFn,
    free: FreeFn,
}

impl std::fmt::Debug for CAbiProtocolAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CAbiProtocolAdapter")
            .field("name", &self.name)
            .finish()
    }
}

// SAFETY: the C contract requires `loadr_plugin_execute` to be thread-safe
// (documented: the host calls it concurrently). The raw function pointers are
// plain `Copy` values; sharing them across threads is sound given that
// requirement.
unsafe impl Send for CAbiProtocolAdapter {}
unsafe impl Sync for CAbiProtocolAdapter {}

fn call_execute(
    name: &str,
    execute: ExecuteFn,
    free: FreeFn,
    request_json: &[u8],
) -> Result<FfiResponse, PluginError> {
    // SAFETY: contract-conformant call; we copy then free via the plugin's
    // own deallocator (see `copy_and_free`).
    let response_bytes = unsafe {
        let mut len: usize = 0;
        let ptr = execute(
            request_json.as_ptr(),
            request_json.len(),
            &mut len as *mut usize,
        );
        copy_and_free(ptr, len, free, Path::new(name))?
    };
    serde_json::from_slice(&response_bytes).map_err(|e| {
        PluginError::Call(format!(
            "plugin `{name}` returned invalid response JSON: {e}"
        ))
    })
}

fn call_c_abi_protocol(
    plugin_name: &str,
    execute: ExecuteFn,
    free: FreeFn,
    request: OwnedFfiRequest,
    config: CachedProtocolConfig,
) -> Result<ProtocolResponse, ProtocolError> {
    let request_json = ffi_request_to_vec(&request, &config)
        .map_err(|e| ProtocolError::InvalidRequest(format!("cannot encode request: {e}")))?;
    let (url, bytes_sent) = request.into_response_metadata();
    let ffi = call_execute(plugin_name, execute, free, &request_json)
        .map_err(|e| ProtocolError::Transport(e.to_string()))?;
    response_from_ffi(plugin_name, ffi, url, bytes_sent)
}

#[async_trait]
impl ProtocolHandler for CAbiProtocolAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(
        &self,
        _ctx: &mut VuContext,
        request: &PreparedRequest,
    ) -> Result<ProtocolResponse, ProtocolError> {
        let owned = OwnedFfiRequest::from_prepared(request);
        let name = self.name.clone();
        let execute = self.execute;
        let free = self.free;
        let config = Arc::clone(&self.config);
        tokio::task::spawn_blocking(move || {
            call_c_abi_protocol(&name, execute, free, owned, config)
        })
        .await
        .map_err(|e| {
            ProtocolError::Transport(format!("plugin `{}` blocking task failed: {e}", self.name))
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use bytes::Bytes;
    use loadr_core::RequestOptions;

    /// Compile a C source string into a cdylib in a fresh temp dir, returning
    /// the dir (kept alive) and the artifact path. Uses the system `cc`.
    fn compile_cdylib(src: &str) -> (tempfile::TempDir, PathBuf) {
        // Unique stem so repeated builds don't collide / get cached oddly.
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = tempfile::tempdir().expect("tempdir");
        let c_path = dir.path().join(format!("fixture_{n}.c"));
        let lib_path = dir.path().join(format!("libfixture_{n}.so"));
        std::fs::write(&c_path, src).expect("write c source");
        let status = Command::new("cc")
            .args(["-O0", "-fPIC", "-shared", "-o"])
            .arg(&lib_path)
            .arg(&c_path)
            .status()
            .expect("run cc");
        assert!(status.success(), "cc failed to build fixture");
        (dir, lib_path)
    }

    /// A complete, contract-conformant C plugin: abi v1, echoes a fixed
    /// response that copies back the request's `body_b64` value when present.
    const VALID_C: &str = r#"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
uint32_t loadr_plugin_abi_version(void) { return 1u; }
void loadr_plugin_free(uint8_t *p, size_t n) { (void)n; free(p); }
static uint8_t *dup_str(const char *s, size_t *out) {
    size_t m = strlen(s); uint8_t *b = (uint8_t*)malloc(m);
    memcpy(b, s, m); *out = m; return b;
}
uint8_t *loadr_plugin_info(size_t *out) {
    return dup_str("{\"name\":\"fx\",\"version\":\"0.1.0\",\"kind\":\"protocol\",\"description\":\"d\",\"schemes\":[\"fx\"]}", out);
}
uint8_t *loadr_plugin_execute(const uint8_t *req, size_t n, size_t *out) {
    (void)req; (void)n;
    return dup_str("{\"status\":200,\"status_text\":\"OK\",\"body_b64\":\"aGk=\",\"duration_ms\":1.5,\"extras\":{\"k\":\"v\"}}", out);
}
"#;

    /// Same surface but advertises an incompatible ABI version.
    const BAD_VERSION_C: &str = r#"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
uint32_t loadr_plugin_abi_version(void) { return 999u; }
void loadr_plugin_free(uint8_t *p, size_t n) { (void)n; free(p); }
uint8_t *loadr_plugin_info(size_t *out) { *out = 0; return NULL; }
uint8_t *loadr_plugin_execute(const uint8_t *r, size_t n, size_t *out) { (void)r;(void)n;*out=0;return NULL; }
"#;

    /// A library that is NOT a C-ABI plugin (no entry symbol).
    const NOT_A_PLUGIN_C: &str = r#"
int unrelated_symbol(void) { return 42; }
"#;

    #[test]
    fn detects_c_abi_plugin() {
        let (_d, path) = compile_cdylib(VALID_C);
        assert!(is_c_abi_plugin(&path), "should detect the C entry symbol");
    }

    #[test]
    fn does_not_detect_non_c_abi_library() {
        let (_d, path) = compile_cdylib(NOT_A_PLUGIN_C);
        assert!(
            !is_c_abi_plugin(&path),
            "library without loadr_plugin_abi_version is not a C-ABI plugin"
        );
    }

    #[test]
    fn detection_false_for_missing_file() {
        assert!(!is_c_abi_plugin(Path::new("/no/such/library.so")));
    }

    #[test]
    fn loads_and_reads_info() {
        let (_d, path) = compile_cdylib(VALID_C);
        let plugin = CAbiPlugin::load(&path).expect("load");
        assert_eq!(plugin.info().name, "fx");
        assert_eq!(plugin.info().kind, "protocol");
        assert_eq!(plugin.info().schemes, vec!["fx".to_string()]);
    }

    #[test]
    fn rejects_incompatible_abi_version() {
        let (_d, path) = compile_cdylib(BAD_VERSION_C);
        let err = CAbiPlugin::load(&path).expect_err("version mismatch");
        match err {
            PluginError::AbiVersion { host, plugin } => {
                assert_eq!(host, LOADR_C_ABI_VERSION);
                assert_eq!(plugin, 999);
            }
            other => panic!("expected AbiVersion, got {other:?}"),
        }
    }

    #[test]
    fn execute_json_round_trip() {
        let (_d, path) = compile_cdylib(VALID_C);
        let plugin = CAbiPlugin::load(&path).expect("load");
        let adapter = plugin
            .make_protocol(serde_json::json!({"some": "config"}))
            .expect("make protocol");
        assert_eq!(ProtocolHandler::name(&adapter), "fx");
        let resp = call_execute(
            &adapter.name,
            adapter.execute,
            adapter.free,
            b"{\"name\":\"t\"}",
        )
        .expect("execute");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.status_text, "OK");
        // "aGk=" is base64 for "hi".
        assert_eq!(resp.body_b64, "aGk=");
        assert_eq!(resp.duration_ms, 1.5);
        assert_eq!(resp.extras["k"], "v");
        assert!(resp.error.is_none());
    }

    #[test]
    fn concurrent_adapters_keep_cached_configs_isolated() {
        let (_d, path) = compile_cdylib(VALID_C);
        let plugin = CAbiPlugin::load(&path).expect("load");
        let left_config = serde_json::json!({"prefix": "LEFT"});
        let right_config = serde_json::json!({"prefix": "RIGHT"});
        let left = Arc::new(
            plugin
                .make_protocol(left_config.clone())
                .expect("make left protocol"),
        );
        let right = Arc::new(
            plugin
                .make_protocol(right_config.clone())
                .expect("make right protocol"),
        );
        let start = Arc::new(Barrier::new(2));

        let request = || PreparedRequest {
            name: "config isolation".into(),
            protocol: "fx".into(),
            method: "GET".into(),
            url: "fx://local".into(),
            headers: Vec::new(),
            body: Bytes::new(),
            timeout: Duration::from_secs(1),
            follow_redirects: false,
            max_redirects: 0,
            options: RequestOptions::default(),
        };

        std::thread::scope(|scope| {
            for (adapter, expected) in [(left, left_config), (right, right_config)] {
                let start = Arc::clone(&start);
                let request = request();
                scope.spawn(move || {
                    start.wait();
                    for iteration in 0..256 {
                        // This is the same Arc-clone + borrowing serializer
                        // seam used by CAbiProtocolAdapter::execute.
                        let config = Arc::clone(&adapter.config);
                        let owned = OwnedFfiRequest::from_prepared(&request);
                        let wire = ffi_request_to_vec(&owned, &config)
                            .expect("serialize cached C-ABI request");
                        let value: serde_json::Value =
                            serde_json::from_slice(&wire).expect("parse C-ABI request");
                        assert_eq!(
                            value.get("config"),
                            Some(&expected),
                            "adapter config mixed on iteration {iteration}",
                        );
                        std::thread::yield_now();
                    }
                });
            }
        });
    }

    #[test]
    fn make_protocol_rejects_non_protocol_kind() {
        // A C plugin whose info reports a non-protocol kind is rejected.
        let src = r#"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
uint32_t loadr_plugin_abi_version(void) { return 1u; }
void loadr_plugin_free(uint8_t *p, size_t n) { (void)n; free(p); }
static uint8_t *d(const char *s, size_t *o){size_t m=strlen(s);uint8_t*b=(uint8_t*)malloc(m);memcpy(b,s,m);*o=m;return b;}
uint8_t *loadr_plugin_info(size_t *o){return d("{\"name\":\"fx\",\"version\":\"0\",\"kind\":\"output\",\"description\":\"d\"}",o);}
uint8_t *loadr_plugin_execute(const uint8_t *r, size_t n, size_t *o){(void)r;(void)n;*o=0;return NULL;}
"#;
        let (_d, path) = compile_cdylib(src);
        let plugin = CAbiPlugin::load(&path).expect("load");
        let err = plugin
            .make_protocol(serde_json::Value::Null)
            .expect_err("non-protocol kind rejected");
        assert!(matches!(err, PluginError::KindMismatch { .. }), "{err:?}");
    }
}
