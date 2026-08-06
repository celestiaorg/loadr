# Installation

## Release binaries

Download the archive for your platform from the
[GitHub releases](https://github.com/levantar-ai/loadr/releases/latest), unpack
it and put `loadr` on your `PATH`:

```bash
curl -sSL https://github.com/levantar-ai/loadr/releases/latest/download/loadr-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv loadr-*/loadr /usr/local/bin/
loadr version
```

Builds are published for Linux (x86_64, aarch64), macOS (Intel & Apple
Silicon) and Windows — each with a SHA256 checksum and SLSA build provenance
you can verify with `gh attestation verify`.

## From source (Cargo)

```bash
cargo install --git https://github.com/levantar-ai/loadr loadr-cli
```

Rust 1.85+ is required. Protobuf compilation happens in-process with protox
and TLS uses rustls, so no system `protoc` or OpenSSL installation is needed.

## Shell completions

```bash
loadr completions bash | sudo tee /etc/bash_completion.d/loadr
loadr completions zsh > "${fpath[1]}/_loadr"
loadr completions fish > ~/.config/fish/completions/loadr.fish
```

## Editor support for test files

Generate the JSON Schema once and point your editor at it for autocomplete
and inline validation — see [JSON Schema & editor setup](../reference/json-schema.md):

```bash
loadr schema > loadr.schema.json
```
