//! Example native protocol plugin (`echo-proto`).
//!
//! `execute()` returns a 200 response whose body is the request body
//! reversed. A `prefix` (from `options.plugin` per request, falling back to
//! the plugin config) is prepended to the body first — demonstrating
//! `RequestOptions::plugin` passthrough.

use std::time::Instant;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RString,
};
use base64::Engine as _;
use loadr_plugin_api::abi::{
    FfiProtocol, FfiProtocolBox, FfiProtocol_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use loadr_plugin_api::{FfiRequest, FfiResponse};

const NAME: &str = "echo-proto";

struct EchoProto;

fn error_response(message: String, duration_ms: f64) -> FfiResponse {
    FfiResponse {
        status: 0,
        status_text: String::new(),
        headers: Vec::new(),
        body_b64: String::new(),
        duration_ms,
        error: Some(message),
        extras: serde_json::Value::Null,
    }
}

fn handle(request_json: &str) -> FfiResponse {
    let started = Instant::now();
    let request: FfiRequest = match serde_json::from_str(request_json) {
        Ok(r) => r,
        Err(e) => return error_response(format!("invalid request JSON: {e}"), 0.0),
    };
    let body = match base64::engine::general_purpose::STANDARD.decode(&request.body_b64) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                format!("invalid body base64: {e}"),
                started.elapsed().as_secs_f64() * 1000.0,
            )
        }
    };
    // Per-request options.plugin wins over plugin-level config.
    let prefix = request
        .options
        .as_ref()
        .and_then(|o| o.get("prefix"))
        .or_else(|| request.config.get("prefix"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let mut echoed: Vec<u8> = prefix.clone().into_bytes();
    echoed.extend(body.iter().rev());
    FfiResponse {
        status: 200,
        status_text: "OK".to_string(),
        headers: vec![
            ("x-echo-proto".to_string(), "1".to_string()),
            ("x-echo-method".to_string(), request.method.clone()),
        ],
        body_b64: base64::engine::general_purpose::STANDARD.encode(&echoed),
        duration_ms: started.elapsed().as_secs_f64() * 1000.0,
        error: None,
        extras: serde_json::json!({
            "prefix_applied": !prefix.is_empty(),
            "url": request.url,
        }),
    }
}

impl FfiProtocol for EchoProto {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn execute(&self, request_json: RString) -> RString {
        let response = handle(request_json.as_str());
        match serde_json::to_string(&response) {
            Ok(json) => RString::from(json),
            Err(e) => RString::from(format!(
                "{{\"status\":0,\"error\":\"cannot encode response: {e}\"}}"
            )),
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "protocol",
            "description": "Echo protocol: responds with the request body reversed",
        })
        .to_string(),
    )
}

extern "C" fn make_protocol() -> FfiProtocolBox {
    FfiProtocol_TO::from_value(EchoProto, abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RSome(make_protocol),
        make_service: RNone,
        make_data_source: RNone,
    }
}
