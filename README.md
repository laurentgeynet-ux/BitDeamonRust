# BitDaemon

A BitTorrent client library ported from
[libtorrent](https://github.com/arvidn/libtorrent) (Rasterbar, C++) to
**100% safe Rust** — no C++ bindings, no FFI, no `unsafe`.

> **Status: early scaffolding.** The crate skeleton, architecture
> decisions and porting plan are in place; protocol code has not been
> ported yet. See [`docs/PORTING.md`](docs/PORTING.md) for live status.

## Reference implementation

This project tracks libtorrent `master` at `v2.1.2-45-gc019e80ef`
(~170 000 lines of C++). The goal is a **semantically faithful** port:
same wire behavior, same resume-data format, same `.session` state
format — but expressed with idiomatic Rust APIs, `tokio` async I/O and
strong types instead of `boost::asio`, inheritance and `error_code`
out-parameters.

## Workspace layout

| Crate | Ports (libtorrent) | Purpose |
|---|---|---|
| `bitdaemon-common` | `sha1_hash`, `bitfield`, `peer_id`, `units`, `flags`, `span` | Shared primitives |
| `bitdaemon-bencode` | `bdecode`, `entry`, `bencode` | Bencode codec (zero-copy decode) |
| `bitdaemon-core` | `torrent_info`, `file_storage`, `create_torrent`, `magnet_uri`, resume data | Torrent metainfo |
| `bitdaemon-disk` | `disk_interface`, `*_storage`, `part_file`, `file_pool`, `merkle` | Disk I/O |
| `bitdaemon-net` | `peer_connection`, trackers, `utp`, `upnp`, `natpmp`, `lsd`, proxies, `pe_crypto` | Networking |
| `bitdaemon-dht` | `kademlia/*` | Mainline DHT (BEP-5/42/44/51) |
| `bitdaemon-session` | `session_impl`, `torrent`, `alert_*`, `settings_pack`, `choker`, `piece_picker` | Orchestration |
| `bitdaemon` | `libtorrent.hpp` | Facade re-exporting the public API |
| `bitdaemon-cli` | `examples/client_test` | Example client binary |

## Building

```sh
cargo build            # whole workspace
cargo test             # all tests
cargo clippy -- -D warnings
cargo run -p bitdaemon-cli -- --help
```

Requires Rust ≥ 1.85 (edition 2024). Developed and CI-tested on
Windows and Linux.

## Documentation

- [`ROADMAP.md`](ROADMAP.md) — phased port plan
- [`docs/PORTING.md`](docs/PORTING.md) — C++ file → Rust module mapping and status
- [`docs/architecture.md`](docs/architecture.md) — crate graph and threading model
- [`docs/adr/`](docs/adr/) — architecture decision records
- [`AGENTS.md`](AGENTS.md) — conventions for AI agents and contributors
- [`CHANGELOG.md`](CHANGELOG.md) — notable changes
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — how to contribute

## License

BSD-2-Clause, same spirit as upstream libtorrent — see [LICENSE](LICENSE).
Derived from libtorrent © 2003-2020 Arvid Norberg.
