# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`rathole` is a reverse proxy for NAT traversal (like frp/ngrok) written in Rust on tokio. This fork uses a **user model**: `[server.users.<name>]` in the server config defines each user's key, an exclusive remote `port_block` and the local `tcp`/`udp` ports of its client to expose; a client only authenticates as one user (`remote_addr`, `user`, `key`), and the server allocates remote ports inside the block (persisted in `alloc_file`, stable across restarts). The client keeps one **control channel** to the server; when a visitor connects to an allocated remote port, the server asks the client to open a **data channel**, and traffic is copied between them. After registration the server pushes the global mapping **directory** (user / local port / remote port / online) to every client, which prints it and opens `alias_bind:<remote_port>` alias listeners forwarding to the server for other users' online TCP mappings. `docs/internals.md` has the (pre-fork) concept glossary.

Toolchain is pinned by `rust-toolchain` (1.71.0). `.rustfmt.toml` sets `imports_granularity = "module"`.

## Commands

```sh
cargo build                     # dev build, default features
cargo build --release
cargo clippy -- -D warnings     # CI fails on any warning
cargo fmt

cargo test                      # unit + integration (native-tls)
cargo test --no-default-features --features server,client,rustls,noise,websocket-rustls,hot-reload   # CI also runs this
cargo test --test integration_test tcp     # one integration test (tcp | udp)
cargo test test_determine_run_mode         # one unit test by name
RUST_LOG=debug cargo test -- --nocapture   # logs use tracing + RUST_LOG

# Check every feature combination compiles (what CI runs)
cargo install cargo-hack
cargo hack check --feature-powerset --no-dev-deps \
  --mutually-exclusive-features default,native-tls,websocket-native-tls,rustls,websocket-rustls

# Smallest binary
cargo build --profile minimal --no-default-features --features client

# Run
./target/debug/rathole examples/minimal/server.toml     # mode inferred from which block exists
./target/debug/rathole --client config.toml             # explicit mode when both blocks present
./target/debug/rathole --genkey                         # X25519 keypair for noise
```

Integration tests bind fixed localhost ports (8080, 8081, 8082, 2331–2337, `[::1]:2336`). They will fail if those are in use. They write allocation tables to `target/alloc_*.toml`. TLS integration tests are skipped on macOS with `native-tls` (self-signed cert issue); with `rustls` they currently fail because `examples/tls/server.crt` has expired; the `websocket_tls` test is skipped on macOS entirely.

## Feature flags

Almost every module is feature-gated, so a change must compile across combinations (run `cargo hack` above).

| Feature | Effect |
|---|---|
| `server`, `client` | Compile `src/server.rs` / `src/client.rs`. Integration tests are no-ops unless both are on. |
| `native-tls` / `rustls` | Mutually exclusive TLS backends (`compile_error!` if both). Both export `TlsTransport`. |
| `websocket-native-tls` / `websocket-rustls` | Websocket transport, tied to the matching TLS backend. |
| `noise` | Noise protocol transport and `--genkey`. |
| `hot-reload` | Enables the `notify` file watcher for the config file; without it `config_watcher::watch_file` returns a receiver that never fires. |
| `embedded` | `server,client,hot-reload,noise` (no TLS; cross-compiling TLS is painful). |
| `console` | tokio-console instrumentation, debug only. |

Code paths for missing features call `helper::feature_not_compile()`, which panics at runtime with a "re-compile rathole" message.

## Architecture

**Entry and lifecycle** (`src/main.rs` → `src/lib.rs::run`):
`main` installs tracing and a ctrl-c → `broadcast<bool>` shutdown channel. `run` spawns a `ConfigWatcherHandle` and restarts the whole instance (`run_instance`) on every config change. `determine_run_mode` picks server vs client from CLI flags, then from which of `[server]`/`[client]` exists.

**Config** (`src/config.rs`): serde structs with `deny_unknown_fields`. `ClientConfig` = `remote_addr`, `user`, `key`, `alias_bind` (no port declarations: the server decides). `ServerConfig` = `bind_addr`, `users` (`HashMap<String, UserConfig>` from `[server.users.<name>]`), `alloc_file` (resolved relative to the config file in `from_file`). `UserConfig` = `key`, `port_block`, `tcp`/`udp` port specs like `"3000"` / `"8000-8010"`; `validate_users` expands them into `block` and `tcp_ports`/`udp_ports`, checks the ports fit in the block and that blocks are disjoint. `parse_port_range` is shared. Unit tests parse every TOML under `examples/` and `tests/config_test/{valid,invalid}_config`, so new example configs must be valid. `MaskedString` hides keys in `Debug` output.

**Hot reload** (`src/config_watcher.rs`): `watch_file(path)` watches the parent dir (to catch editor rename-replace) and yields `()` on modification. The config watcher restarts the whole instance on any change (users included); clients reconnect and re-register, allocations survive via `alloc_file`.

**Transport** (`src/transport/`): unchanged. The `Transport` trait (`bind`/`accept`/`handshake`/`connect`) abstracts the wire; `TcpTransport` is the base; `TlsTransport`, `NoiseTransport`, `WebsocketTransport` wrap it. `SocketOpts::nodelay` carries the client/server-level `nodelay` hint for data channels; the control channel always gets nodelay.

**Protocol** (`src/protocol.rs`, version 3): `Hello`/`Auth`/`Ack` are fixed-size bincode frames as before, but the control-channel digest is `sha256(user)` and auth is `sha256(key ++ nonce)`. Everything after `Ack::Ok` is a length-prefixed frame (`write_frame`/`read_frame`, u32 BE + bincode, 1 MiB cap), all server → client: `RegisterAck` (the server allocated remote ports for the user's configured local ports, or an error string), then `ControlChannelCmd::{CreateDataChannel, HeartBeat, Directory(Vec<Mapping>)}`. Data channels receive `DataChannelCmd::StartForwardTcp(local_port)` / `StartForwardUdp(local_port)` so the client knows which local port to connect to. UDP is framed as `UdpHeader{from, len}` + payload.

**Registry** (`src/registry.rs`, server only): the persisted allocation table (`[[alloc]]` entries in `alloc_file`), the online map (user → session nonce + registered ports) and the directory. `register` reuses existing allocations inside the user's block, otherwise takes the lowest free port; `unregister` ignores stale nonces. The directory is broadcast through a `tokio::sync::watch`.

**Server** (`src/server.rs`): one `ControlChannelHandle` per authenticated user in `MultiMap<UserDigest, Nonce, _>` (`src/multi_map.rs`). All mappings of a user share one data-channel pool (`Arc<Mutex<mpsc::Receiver>>`): one TCP listener per TCP mapping on `<host of bind_addr>:<remote_port>` feeding `(visitor, local_port)` into a single `run_tcp_connection_pool`, and one `run_udp_connection_pool` per UDP mapping. The control channel task pushes directory updates, sends heartbeats, detects client disconnect by reading, and on exit removes itself by nonce and unregisters.

**Client** (`src/client.rs`): a single `ControlChannel`, reconnecting with `constants::run_control_chan_backoff`. On `CreateDataChannel` it connects, reads the `DataChannelCmd` and forwards to `127.0.0.1:<local_port>` (TCP `copy_bidirectional`, UDP via `UdpPortMap`). On `Directory` it prints the table and reconciles `Aliases` (`alias_bind:<remote_port>` → `<server host>:<remote_port>` for other users' online TCP mappings; each alias is a task holding a `JoinSet`, aborted when the mapping goes offline or the control channel drops). `heartbeat_timeout` drops the control channel if the server goes silent.

## Docs and examples

- `docs/transport.md`: TLS (PKCS#12, `-legacy` needed for rustls) and Noise setup.
- `docs/build-guide.md`: feature/profile combinations for small binaries.
- `examples/*`: paired `client.toml`/`server.toml` per scenario; these double as config parse tests. `examples/minimal` also has `client_bob.toml` (an alias-only client).
- `flake.nix`, `Dockerfile`, `.github/workflows/release.yml`: release builds cross-compile with `cross` for musl/arm/mips targets.
