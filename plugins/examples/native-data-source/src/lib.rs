//! Example native data-source plugin (`tx-signer`).
//!
//! Generates a fresh Ed25519-signed "transaction" per `next_row` call, for
//! use as `data.<name>.type: plugin`. Demonstrates the motivating use case:
//! a gRPC request body field that must contain a freshly signed,
//! time-sensitive payload.
//!
//! Wire payload layout (signed message, then the 64-byte signature appended):
//! `chain_id bytes || vu (u64 LE) || seq (u64 LE) || ts_ms (u64 LE) || signature`,
//! base64-encoded as `tx_b64`. Uniqueness comes from the core-supplied
//! `(vu, seq)` pair -- no locks needed on the hot path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult,
    RResult::{RErr, ROk},
    RString,
};
use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey};
use loadr_plugin_api::abi::{
    FfiDataSource, FfiDataSourceBox, FfiDataSource_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use serde::Deserialize;

const NAME: &str = "tx-signer";

/// Per-`data.<name>` settings captured at `init`.
struct SourceState {
    chain_id: String,
    /// Total rows to generate before reporting exhaustion (unset = unbounded).
    limit: Option<u64>,
    /// Global (cross-VU) count of rows generated so far, for `limit`.
    generated: AtomicU64,
}

#[derive(Default)]
struct TxSigner {
    signing_key: Option<SigningKey>,
    sources: HashMap<String, SourceState>,
}

/// `{"plugin_config": <merged [config] + PluginRef.config>, "sources": {...}}`.
#[derive(Deserialize)]
struct InitPayload {
    plugin_config: serde_json::Value,
    sources: HashMap<String, serde_json::Value>,
}

/// `{"source","vu","iteration","seq","scenario","request"?,"ts_ms"}`.
#[derive(Deserialize)]
struct RowCtx {
    source: String,
    vu: u64,
    seq: u64,
    ts_ms: u64,
}

/// Decode an even-length hex string. No external `hex` crate needed for
/// this small, example-only helper.
fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("key_hex must have an even number of hex digits".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| format!("invalid hex digit in key_hex: {e}"))
        })
        .collect()
}

/// `key_hex` (64 hex chars = 32 bytes) wins; otherwise derive a deterministic
/// key from a `seed` integer. Good enough for examples/tests -- production
/// signer plugins should load real key material (e.g. from an env var).
fn derive_signing_key(plugin_config: &serde_json::Value) -> Result<SigningKey, String> {
    if let Some(hex_str) = plugin_config.get("key_hex").and_then(|v| v.as_str()) {
        let bytes = hex_decode(hex_str)?;
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "key_hex must decode to exactly 32 bytes".to_string())?;
        return Ok(SigningKey::from_bytes(&key));
    }
    if let Some(seed) = plugin_config.get("seed").and_then(|v| v.as_u64()) {
        let seed_bytes = seed.to_le_bytes();
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = seed_bytes[i % seed_bytes.len()] ^ (i as u8);
        }
        return Ok(SigningKey::from_bytes(&key));
    }
    Err("plugin config needs `key_hex` (64 hex chars) or `seed` (integer)".to_string())
}

impl FfiDataSource for TxSigner {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn init(&mut self, init_json: RString) -> RResult<(), RString> {
        let payload: InitPayload = match serde_json::from_str(init_json.as_str()) {
            Ok(p) => p,
            Err(e) => return RErr(RString::from(format!("invalid init JSON: {e}"))),
        };
        let signing_key = match derive_signing_key(&payload.plugin_config) {
            Ok(k) => k,
            Err(e) => return RErr(RString::from(e)),
        };
        self.signing_key = Some(signing_key);
        self.sources.clear();
        for (name, config) in payload.sources {
            let chain_id = config
                .get("chain_id")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let limit = config.get("limit").and_then(|v| v.as_u64());
            self.sources.insert(
                name,
                SourceState {
                    chain_id,
                    limit,
                    generated: AtomicU64::new(0),
                },
            );
        }
        ROk(())
    }

    fn next_row(&self, ctx_json: RString) -> RResult<RString, RString> {
        let ctx: RowCtx = match serde_json::from_str(ctx_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(format!("invalid row context JSON: {e}"))),
        };
        let Some(signing_key) = &self.signing_key else {
            return RErr(RString::from("plugin not initialized"));
        };
        let Some(state) = self.sources.get(&ctx.source) else {
            return RErr(RString::from(format!(
                "unknown data source `{}`",
                ctx.source
            )));
        };

        if let Some(limit) = state.limit {
            let generated = state.generated.fetch_add(1, Ordering::SeqCst);
            if generated >= limit {
                return ROk(RString::from(r#"{"exhausted":true}"#));
            }
        }

        let mut message = Vec::with_capacity(state.chain_id.len() + 24);
        message.extend_from_slice(state.chain_id.as_bytes());
        message.extend_from_slice(&ctx.vu.to_le_bytes());
        message.extend_from_slice(&ctx.seq.to_le_bytes());
        message.extend_from_slice(&ctx.ts_ms.to_le_bytes());

        let signature = signing_key.sign(&message);
        let mut tx = message;
        tx.extend_from_slice(&signature.to_bytes());
        let tx_b64 = base64::engine::general_purpose::STANDARD.encode(&tx);
        let nonce = format!("{}:{}", ctx.vu, ctx.seq);

        let row = serde_json::json!({"row": {"tx_b64": tx_b64, "nonce": nonce}});
        ROk(RString::from(row.to_string()))
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "Generates Ed25519-signed transaction payloads for gRPC SubmitRequest",
        })
        .to_string(),
    )
}

extern "C" fn make_data_source() -> FfiDataSourceBox {
    FfiDataSource_TO::from_value(TxSigner::default(), abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RSome(make_data_source),
    }
}
