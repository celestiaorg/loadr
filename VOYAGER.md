# Voyager handoff

## Final summary

Voyager narrows the product to distributed gRPC load generation, native feeder plugins, Prometheus output, and the embedded Web UI. The CLI advertises that scope and exposes only run, validate, controller, agent, plugin, schema, completions, and version commands ([crates/loadr-cli/src/main.rs](crates/loadr-cli/src/main.rs#L9)).

- The workspace contains the core/config/gRPC/Prometheus/feeder/agent/Web UI/CLI crates, the signed-transaction feeder example, and the gRPC test server ([Cargo.toml](Cargo.toml#L1)).
- The production protocol registry registers only the gRPC handler, and the gRPC crate covers unary, client-streaming, server-streaming, and bidirectional-streaming calls ([crates/loadr-grpc/src/lib.rs](crates/loadr-grpc/src/lib.rs#L1)).
- Configuration validation rejects non-gRPC protocols and URLs, requires a `grpc:` block, and rejects request fields belonging to removed protocol families ([crates/loadr-config/src/validate.rs](crates/loadr-config/src/validate.rs#L512)).
- Feeder plugins use native ABI version 2 with a single data-source interface ([crates/loadr-feeder-api/src/abi.rs](crates/loadr-feeder-api/src/abi.rs#L14)). Feeder row context includes run, instance, and partition identity ([crates/loadr-core/src/data.rs](crates/loadr-core/src/data.rs#L30)), and distributed agents populate those values when constructing the engine ([crates/loadr-agent/src/agent.rs](crates/loadr-agent/src/agent.rs#L501)).
- Feeder discovery, loading, enable/disable, and installation are local filesystem operations; explicit relative feeder paths resolve against the plan directory ([crates/loadr-feeder-api/src/registry.rs](crates/loadr-feeder-api/src/registry.rs#L14)).
- Output construction accepts only Prometheus scrape and remote-write configuration ([crates/loadr-prometheus/src/lib.rs](crates/loadr-prometheus/src/lib.rs#L25)).
- CI runs formatting, all-target clippy with warnings denied, and locked workspace tests on Linux, macOS, and Windows ([.github/workflows/ci.yml](.github/workflows/ci.yml#L20)).

## Verification

The following commands passed before this commit:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --no-fail-fast`
- `loadr validate` for `examples/10-grpc.yaml`, `examples/15-distributed.yaml`, and `examples/17-feeders-and-throttle.yaml`
- `git diff --check HEAD`

## Known leftovers

The public validator is gRPC-only, but the configuration model still contains HTTP body fields plus WebSocket, GraphQL, socket, SSE, SQL, and generic protocol-plugin option types ([crates/loadr-config/src/plan.rs](crates/loadr-config/src/plan.rs#L1060)). The core metrics path also retains dispatch branches and metric names for removed protocols ([crates/loadr-core/src/flow.rs](crates/loadr-core/src/flow.rs#L1954)). This is compatibility residue, not a supported runtime surface.

The gRPC registry still accepts a type named `HttpDefaults` ([crates/loadr-grpc/src/lib.rs](crates/loadr-grpc/src/lib.rs#L15)), and the generic request-failure metric is still named `http_req_failed` ([crates/loadr-config/src/validate.rs](crates/loadr-config/src/validate.rs#L19)). Renaming either is a breaking schema/metrics change and should be deliberate.

## Next steps

1. Remove the rejected legacy request fields and their option types from `loadr-config`, then delete the corresponding unreachable flow compilation and metrics branches. Start from the validator's explicit rejection list ([crates/loadr-config/src/validate.rs](crates/loadr-config/src/validate.rs#L532)).
2. Decide whether to rename `HttpDefaults` and `http_req_failed`. If compatibility is not required on this branch, rename both before publishing 1.0 artifacts; otherwise document them as frozen legacy names.
3. Add platform-specific feeder packaging. The current example manifest hard-codes a Linux `.so` entry name ([plugins/examples/signed-tx-feeder/plugin.toml](plugins/examples/signed-tx-feeder/plugin.toml#L1)), while CI tests all three desktop operating systems ([.github/workflows/ci.yml](.github/workflows/ci.yml#L34)).
4. Run a multi-host soak test for both gRPC transports with controller restarts and agent reconnects. The configuration exposes `channel` and `raw` transport modes ([crates/loadr-config/src/plan.rs](crates/loadr-config/src/plan.rs#L1244)), and agent assignments propagate stable run/instance/partition identity ([crates/loadr-agent/src/agent.rs](crates/loadr-agent/src/agent.rs#L501)).
5. Push `voyager`, open a review focused first on scope/deletions, and only then split further cleanup into follow-up commits. Do not restore removed protocol or plugin families merely to reduce the size of this commit.
