//! Example native data-source plugin (`nonce-feeder`).
//!
//! Hands out the next transaction nonce for a blockchain account, and only
//! advances it when the request that used it succeeded. Each VU owns one
//! account, so no two in-flight requests ever hold the same nonce. Accounts
//! are spread over N shards; each shard has its own lock, so VUs touching
//! different shards never contend.
//!
//! Rows look like `{"account": "acct-3", "nonce": "17"}`. The result sink
//! reads the row back off the payload, so no pending-request bookkeeping is
//! needed.

use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::Mutex;

use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RResult,
    RResult::{RErr, ROk},
    RString,
};
use loadr_plugin_api::abi::{
    FfiDataSource, FfiDataSourceBox, FfiDataSource_TO, FfiResultSink, FfiResultSinkBox,
    FfiResultSink_TO, PluginMod, LOADR_PLUGIN_ABI_VERSION,
};
use serde::Deserialize;
use std::hash::{DefaultHasher, Hash, Hasher};

const NAME: &str = "nonce-feeder";
const DEFAULT_SHARDS: usize = 256;
const DEFAULT_ACCOUNTS: u64 = 1000;

/// Account name → next nonce, split across independently locked shards.
struct NonceMap {
    shards: Vec<Mutex<HashMap<String, u64>>>,
    accounts: u64,
}

impl NonceMap {
    fn new(shards: usize, accounts: u64) -> Self {
        NonceMap {
            shards: (0..shards.max(1))
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            accounts: accounts.max(1),
        }
    }

    fn shard_of(&self, account: &str) -> &Mutex<HashMap<String, u64>> {
        let mut hasher = DefaultHasher::new();
        account.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % self.shards.len()]
    }

    /// One account per VU: a nonce is a per-account sequence, so two VUs
    /// sharing an account would race for the same value and one of their
    /// transactions would be rejected by the chain.
    fn account_for(&self, vu: u64) -> String {
        format!("acct-{}", vu % self.accounts)
    }

    /// Current nonce, without advancing it: an unconfirmed transaction does
    /// not consume one.
    fn peek(&self, account: &str) -> u64 {
        self.shard_of(account)
            .lock()
            .get(account)
            .copied()
            .unwrap_or(0)
    }

    fn advance(&self, account: &str) {
        let mut shard = self.shard_of(account).lock();
        match shard.get_mut(account) {
            Some(nonce) => *nonce += 1,
            None => {
                shard.insert(account.to_string(), 1);
            }
        }
    }
}

/// Both capabilities are separate trait objects, so they share one instance.
static STATE: OnceLock<NonceMap> = OnceLock::new();

fn state() -> &'static NonceMap {
    STATE.get_or_init(|| NonceMap::new(DEFAULT_SHARDS, DEFAULT_ACCOUNTS))
}

#[derive(Deserialize)]
struct InitPayload {
    plugin_config: serde_json::Value,
}

#[derive(Deserialize)]
struct RowCtx {
    vu: u64,
}

#[derive(Deserialize)]
struct ResultPayload {
    row: HashMap<String, String>,
    response: Response,
}

#[derive(Deserialize)]
struct Response {
    status: i64,
    #[serde(default)]
    error: Option<String>,
}

struct Feeder;

impl FfiDataSource for Feeder {
    fn name(&self) -> RString {
        RString::from(NAME)
    }

    fn init(&mut self, init_json: RString) -> RResult<(), RString> {
        let payload: InitPayload = match serde_json::from_str(init_json.as_str()) {
            Ok(p) => p,
            Err(e) => return RErr(RString::from(format!("invalid init JSON: {e}"))),
        };
        let shards = payload
            .plugin_config
            .get("shards")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_SHARDS as u64) as usize;
        let accounts = payload
            .plugin_config
            .get("accounts")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_ACCOUNTS);
        // First writer wins; a second `init` (one plugin backing several
        // sources) keeps the map that is already handing out nonces.
        let _ = STATE.set(NonceMap::new(shards, accounts));
        ROk(())
    }

    fn next_row(&self, ctx_json: RString) -> RResult<RString, RString> {
        let ctx: RowCtx = match serde_json::from_str(ctx_json.as_str()) {
            Ok(c) => c,
            Err(e) => return RErr(RString::from(format!("invalid row context JSON: {e}"))),
        };
        let map = state();
        let account = map.account_for(ctx.vu);
        let nonce = map.peek(&account);
        let row = serde_json::json!({"row": {"account": account, "nonce": nonce.to_string()}});
        ROk(RString::from(row.to_string()))
    }
}

struct Sink;

impl FfiResultSink for Sink {
    fn on_result(&self, result_json: RString) {
        let Ok(payload) = serde_json::from_str::<ResultPayload>(result_json.as_str()) else {
            return;
        };
        let Some(account) = payload.row.get("account") else {
            return;
        };
        let ok = payload.response.error.is_none() && (200..300).contains(&payload.response.status);
        if ok {
            state().advance(account);
        }
    }
}

extern "C" fn plugin_info() -> RString {
    RString::from(
        serde_json::json!({
            "name": NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "kind": "service",
            "description": "Per-account transaction nonces, advanced only on success",
        })
        .to_string(),
    )
}

extern "C" fn make_data_source() -> FfiDataSourceBox {
    FfiDataSource_TO::from_value(Feeder, abi_stable::erased_types::TD_Opaque)
}

extern "C" fn make_result_sink() -> FfiResultSinkBox {
    FfiResultSink_TO::from_value(Sink, abi_stable::erased_types::TD_Opaque)
}

loadr_plugin_api::export_loadr_plugin! {
    PluginMod {
        abi_version: LOADR_PLUGIN_ABI_VERSION,
        info: plugin_info,
        make_output: RNone,
        make_protocol: RNone,
        make_service: RNone,
        make_data_source: RSome(make_data_source),
        make_result_sink: RSome(make_result_sink),
    }
}
