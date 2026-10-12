# BitDaemon

A BitTorrent client library ported from
[libtorrent](https://github.com/arvidn/libtorrent) (Rasterbar, C++) to
**100% safe Rust** — no C++ bindings, no FFI, no `unsafe`.

> **Status: protocol port complete — every upstream C++ file is
> `ported`, `verified` or explicitly `won't port`** in
> [`docs/PORTING.md`](docs/PORTING.md). Interoperability verified
> live: BitDaemon downloads from and seeds to official libtorrent
> C++ peers (byte-identical output), does the same over uTP and
> mutual-TLS SSL torrents, boots a real Mainline DHT table, and
> downloads real public torrents from the global swarm (Ubuntu
> 24.04.4 ISO, 3.4 GB, ~117 MB/s peak, official SHA-256 verified).
>
> **Remaining for 1.0** is *surface* work, not protocol work:
> expand the `Alert` event surface to upstream parity, deliver DHT
> query results to API consumers, finish BEP-17 web seeds and a
> WebRTC backend — see
> [`docs/adr/0023`](docs/adr/0023-alert-surface-and-api-completion.md)
> and ROADMAP phases K–N.

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
| `bitdaemon-mmap` | `mmap_storage`, `file_view_pool` | `memmap2` storage backend (feature-gated `unsafe`, ADR-0010) |
| `bitdaemon-sim` | `simulation/*` | Deterministic network simulator for tests |

## Using the CLI

```sh
cargo run --release -p bitdaemon-cli -- \
    -d <download-dir> [-p 6881] [--limit <bytes/s>] <TORRENT>
```

Supports `.torrent` files and magnets, `--seed`, `--peer host:port`,
HTTPS/UDP trackers, DHT, uTP, UPnP/NAT-PMP port mapping, SOCKS5/HTTP
proxies (`--proxy`, `--proxy-auth`, peer + tracker + DHT UDP lanes)
and OS-cache control (`--no-os-cache`, `--no-os-read-cache`,
`--no-upnp`).

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
- [`tools/`](tools/) — golden-vector generator + real-swarm test client
  built against C++ libtorrent
- [`fuzz/`](fuzz/) — cargo-fuzz targets (bdecode, peer wire, resume data)

## License

BSD-2-Clause, same spirit as upstream libtorrent — see [LICENSE](LICENSE).
Derived from libtorrent © 2003-2020 Arvid Norberg.
