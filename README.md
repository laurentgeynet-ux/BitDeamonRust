# BitDaemon

A BitTorrent client library ported from
[libtorrent](https://github.com/arvidn/libtorrent) (Rasterbar, C++) to
**100% safe Rust** — no C++ bindings, no FFI, no `unsafe`.

> **Status: port complete — every upstream C++ file is `ported`,
> `verified` or explicitly `won't port`** in
> [`docs/PORTING.md`](docs/PORTING.md). The API surface is at
> upstream parity: the full `Alert` type set (101 types, category/
> type/priority conformant to upstream), async DHT query results,
> port-mapping handles, extension/plugin hooks
> ([ADR-0023](docs/adr/0023-alert-surface-and-api-completion.md)) —
> everything a GUI client needs is exposed; see
> [`docs/gui-parity.md`](docs/gui-parity.md) for the
> widget ↔ handle/alert matrix.
>
> Interoperability verified live: BitDaemon downloads from and seeds
> to official libtorrent C++ peers (byte-identical output), does the
> same over uTP and mutual-TLS SSL torrents, boots a real Mainline
> DHT table, and downloads real public torrents from the global
> swarm (Ubuntu 24.04.4 ISO, 3.4 GB, ~117 MB/s peak, official
> SHA-256 verified). Conformance is pinned by golden fixtures
> generated *from the C++ code itself* and a real-swarm differential
> bench — see [Conformance testing](#conformance-testing).
>
> **Remaining for 1.0** is hardening, not features: promote `ported`
> → `verified` across the remaining modules, then release
> engineering (API freeze, `cargo deny`, crates.io) — see
> [`ROADMAP.md`](ROADMAP.md) Phase G.

## Reference implementation

This project tracks libtorrent `master` at `v2.1.2-62-ge7381c7bc`
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

Supports multiple `.torrent` files and magnets in one session,
`--seed`, `--super-seed`, `--upload-only`, `--file-priority`,
`--peer host:port`, IPv6 (`--port6`), HTTPS/UDP trackers, DHT, uTP,
UPnP/NAT-PMP port mapping, SOCKS5/HTTP proxies (`--proxy`,
`--proxy-auth`, peer + tracker + DHT UDP lanes) and OS-cache
control (`--no-os-cache`, `--no-os-read-cache`, `--no-upnp`) and
uTP/TCP balance policy (`--mixed-mode`). It
loads upstream-format fastresume files (`--resume`), writes
`<infohash>.resume` on shutdown — both directions round-trip with
libtorrent — and restores a full session from `--session <file>`
(torrents, embedded peers, DHT routing, settings).

## Conformance testing

Two harnesses pin BitDaemon against upstream itself, not against
re-implementations:

- **Golden vectors** (`tools/golden_vectors/`) — a generator built
  against the C++ v2.1.2 sources emits reference bytes, committed as
  fixtures under `crates/*/tests/data/`. The `test_golden` suites in
  `bitdaemon-{bencode,core,dht,session}` byte-compare them: bencode
  dict order + int64-overflow quirks, `write_resume_data` output,
  `.session` params dicts, DHT state serialization,
  `create_torrent` output (v1 + v2), `-XXNNNN-` fingerprints and
  magnet URIs.
- **Real-swarm bench** (`tools/real_swarm/`) — a C++ client built
  on libtorrent itself exchanges data with BitDaemon: TCP, uTP and
  mutual-TLS SSL torrents in both directions, magnet `ut_metadata`
  retrieval + download, `.resume` files round-tripped both ways
  (upstream detects and repairs a truncated tail), `ut_pex` driving
  upstream's dialer, and boundary file layouts (0-byte, tiny, deep
  paths, UTF-8/space names, pad files). All transfers verified
  byte-identical.

  The extended campaign additionally covered, live against
  libtorrent v2.1.2 and local harnesses: MSE/PE encrypted
  handshakes (forced, both directions), SSL over uTP, UDP+HTTP
  tracker announces, DHT-only peer discovery, LSD multicast both
  directions, IPv6 `[::1]` both directions, BEP-19 web-seed
  `Range` GETs, SOCKS5-tunneled peers/trackers, UPnP port mapping
  on a real IGD, multi-peer swarms and multi-torrent soak,
  super-seeding and BEP-21 `upload_only` (observed on the wire),
  file priorities, and `.session` save/restore continuing a
  partial download to completion. Throughput check: 128 MiB in
  ~40 s seeding to C++ leeches, ~14 s leeching from a C++ seed.
- **Decode-surface fuzzing** (`fuzz/`) — `cargo-fuzz` targets for
  `bdecode`, `peer_wire` and `resume_data`; on Windows (no sancov
  link) a deterministic mutation driver hammers the same decoders
  — 92.7 M inputs, zero panics.

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
- [`docs/gui-parity.md`](docs/gui-parity.md) — GUI integration matrix: every widget ↔ handle/alert
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
