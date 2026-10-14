# AGENTS.md — Guide for coding agents and contributors

BitDaemon is a **semantically faithful port of *all of* libtorrent
(Rasterbar, C++) to 100% safe Rust** (ADR-0010: every behavioral
`src/*.cpp` file gets a Rust module or a reasoned `won't port` row).
This file is the working contract for anyone —
human or AI — contributing code. Read it fully before touching anything.

## 1. Mission and ground truth

- **Reference implementation**: libtorrent `master` @ `v2.1.2-45-gc019e80ef`
  (~170 000 LOC: ~114k `src/*.cpp`, ~55k `include/**/*.hpp`).
- **Local C++ checkout**: `D:\Projet\libtorrent\libtorrent`
  (read-only reference — never modify, never copy wholesale).
- **Fidelity rule**: match *wire behavior, file formats and state-machine
  semantics* of the C++ code, not its line-by-line structure. We write
  idiomatic Rust, we do not transliterate C++.
- **Interoperability is the spec**: the port is correct when real
  BitTorrent clients (libtorrent, qBittorrent, Transmission…) cannot
  distinguish it on the wire, and when it round-trips libtorrent's
  resume data and `.session` files.
- **Complete-port goal** (ADR-0010): "deferred" is a phase position,
  never a permanent status. The port is done when `PORTING.md` is 100%
  resolved rows (`ported`/`verified`/`won't port`+reason).
- **First downstream consumer**: OnionBit (`D:\Projet\Tribler-Rust-Torrent`)
  — replaces its vendored/patched librqbit. Its anonymous lanes require
  injectable transports and a storage factory; that shaped ADR-0008.
  It remains a consumer, **not** the ordering driver — phase order is
  upstream dependency depth.

## 2. Repository layout

```
crates/
  bitdaemon-common    shared primitives (hashes, bitfield, ids, time)
  bitdaemon-bencode   bdecode/entry/bencode codec
  bitdaemon-core      torrent_info, file_storage, create_torrent, magnet
  bitdaemon-disk      DiskIo trait, storage backends, part_file, merkle
  bitdaemon-net       peer wire, trackers, uTP, proxies, UPnP/NAT-PMP/LSD
  bitdaemon-dht       Mainline DHT (kademlia)
  bitdaemon-session   session/torrent actors, alerts, settings, choker
  bitdaemon-mmap      mmap storage backend (feature-gated;
                      unsafe allowed for memmap2 — ADR-0010 §3)
  bitdaemon-sim       upstream simulation/ harness — deterministic
                      swarm tests over injected transports
  bitdaemon           facade: re-exports the public API
  bitdaemon-cli       example client (port of examples/client_test.cpp)
docs/
  PORTING.md          C++→Rust file mapping + status — ALWAYS up to date
  architecture.md     crate graph, threading/concurrency model
  adr/                architecture decision records
  upstream/           « sync libtorrent » workflow + delta fiches —
                      local only (.git/info/exclude, never pushed; §5)
```

Dependency direction is strictly downward:
`common ← bencode ← core ← disk / net ← dht ← session ← bitdaemon`.
Never introduce a dependency that goes upward or sideways across this
graph; if two crates need shared code, it belongs in `common` (or a new
lower layer — open an ADR).

## 3. Commands (run before every commit)

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps   # must be warning-free
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy, tests and an MSRV
(1.85) check on ubuntu + windows. A red CI blocks merge.

## 4. Hard rules

1. **`#![forbid(unsafe_code)]` everywhere.** No `unsafe` — *two
   reviewed exceptions*: `bitdaemon-mmap` (ADR-0010 §3) may use
   `unsafe` for `memmap2` inside its feature-gated boundary, and
   `bitdaemon-disk` uses `#![deny]` with scoped `#[allow]` on
   `file::{win32,posix}` for OS file controls (`FSCTL_SET_SPARSE`,
   `SetFileValidData`, `posix_fallocate`, `posix_fadvise`,
   `FS_NOCOW_FL`) that have no safe-std equivalent (ADR-0022 §3).
   Nothing unsafe leaks into the public API. Dependencies using
   unsafe internally are fine — *our* other crates do not.
2. **No new dependency without an ADR note** in `docs/adr/0007-*` or a
   commit-message justification. Prefer crates ≥ 7 days old, pinned to
   minor versions in `[workspace.dependencies]`. See ADR-0007.
3. **No `unwrap()`/`expect()`/`panic!`/`todo!()` in shipped code paths.**
   Allowed in tests and in `bitdaemon-cli`. Library code returns `Result`.
4. **Update `docs/PORTING.md` in the same commit** as the port it
   describes. A module is not "Ported" until its ported unit tests pass;
   see §7.
5. **Update `CHANGELOG.md`** (Keep a Changelog) for every user-visible
   change.
6. **Do not copy C++ comments verbatim into Rust code.** Re-express the
   *why* in rustdoc where it carries real information; skip license
   boilerplate (the project LICENSE already credits upstream).
7. **Naming**: `snake_case` modules/functions, `CamelCase` types,
   `SCREAMING` consts. Keep C++ names recognizable (`torrent_info` →
   `TorrentInfo`) so `PORTING.md` stays greppable.
8. **Bitfields/hashes/buffers**: `bitvec` for `bitfield`, fixed `[u8; N]`
   newtypes for hashes, `bytes::Bytes`/`BytesMut` for wire buffers.
   No `Vec<u8>` in public APIs where a `Bytes` or sized array works.
9. **Error handling**: `thiserror` enums per crate (`BdecodeError`,
   `NetError`…), `Result<T>` aliases. Never `Box<dyn Error>` in public
   APIs. Error variants must name the C++ `error_code` they replace in
   a doc comment when non-obvious. See ADR-0004.
10. **Async**: all I/O on `tokio`. One actor task owns all session
    state (ADR-0001); public handles are `Clone` + `mpsc`. Never hold
    a `Mutex` across `.await`; prefer message passing over shared
    `Arc<Mutex>` state inside the session. Disk jobs run on
    `tokio::task::spawn_blocking` (or a dedicated pool) — see
    `docs/architecture.md`.
11. **`unsafe`-free crypto**: use `sha1`, `sha2`, `crc32c`,
    `ed25519-dalek`, `chacha20poly1305` etc. — do not port the bundled
    C code (`src/ed25519`, `puff.cpp`, `win_cng`). See ADR-0007.
12. **Tracing**: instrument with `tracing` (`debug!`/`info!`/`error!`),
    no `println!`/`eprintln!` outside `bitdaemon-cli` and tests.
13. **No subsystem binds OS sockets** (ADR-0008): DHT, uTP, UDP
    trackers, LSD all take `Arc<dyn transport::DatagramSocket>`;
    peer connections come from `PeerConnector`/`PeerAcceptor`. The
    session hands them down from `SessionParams`. A direct
    `UdpSocket::bind`/`TcpListener::bind` outside the default
    `TokioUdpSocket`/`TcpConnector` impls is a review-blocking bug —
    it breaks anonymous-tunnel lanes *and* the future simulator.
14. **Storage is a factory** (`disk::StorageFactory`, upstream
    `storage_constructor`) — downstream can inject custom storage
    (OnionBit's `PrivateStorageFactory` pattern).

## 5. Porting workflow (follow it for every module)

1. **Pick a module** in `docs/PORTING.md` marked `not started`, in
   dependency order (a phase in `ROADMAP.md` unlocks the next).
2. **Read the C++**: header first (`include/libtorrent/**`), then
   `src/*.cpp`. Note invariants, error paths, wire formats, defaults
   from `session_settings`.
3. **Design the Rust API** matching §4 conventions. Where C++ uses
   inheritance (`peer_connection` → `bt_peer_connection`), use trait
   objects or enums. Where C++ uses `error_code&` out-params, return
   `Result`. Where C++ uses `boost::optional`, use `Option`. Where C++
   uses flags bitsets, use `bitflags`-style newtypes.
4. **Implement** in the target crate, one C++ file ≈ one Rust module.
   Name the mapping explicit: a `//! Port of aux/foo.hpp + src/foo.cpp`
   line at the top of every ported module.
5. **Port the tests**: nearly every `src/foo.cpp` has a matching
   `test/test_foo.cpp`. Port those tests — they are the spec. Test data
   files live in `test/` of the C++ checkout; reference them by copying
   *data files only* (`.torrent`, `.bencode`, hex vectors) into
   `crates/<crate>/tests/data/`.
6. **Mark status** in `docs/PORTING.md`: `in progress` → `ported`
   (code compiles, unit tests pass) → `verified` (integration/wire
   test against C++ or a live swarm).
7. **Commit** with a message `port(<crate>): <module>` and update the
   changelog.

### Upstream sync (« sync libtorrent »)

Tracking new upstream commits on `RC_2_1` is a separate, repeatable
workflow living in `docs/upstream/` — **local only**: the directory is
excluded via `.git/info/exclude` and must never be committed or pushed
(add the same rule on a fresh clone before recreating it). Inside,
`WORKFLOW.md` is the protocol, `sync-state.toml` the machine-readable
state (baseline commit + per-commit verdicts), and each sync writes a
`deltas/<date>_<range>/` folder (index + per-commit fiches + action
list). Git operations run in the working checkout named by
`sync-state.toml`, never in the read-only reference.

## 6. C++ → Rust translation cheatsheet

| C++ (libtorrent) | Rust (BitDaemon) |
|---|---|
| `boost::asio` io_service + handlers | `tokio` tasks, channels, `async fn` |
| `error_code` / `lt::errors` | `thiserror` enums, `Result<T>` |
| `boost::optional` / `boost::variant` | `Option` / `enum` |
| `span<T>` / `string_view` | `&[T]` / `&[u8]` / `Bytes` |
| `shared_ptr`/`weak_ptr` cycles | `Arc`/`Weak`, or actor ownership |
| `std::function<...>` callbacks | `impl Fn` generics, `oneshot`/`mpsc` |
| `flags::bitfield_flag` | `bitflags` newtype |
| `alert` class hierarchy | single `Alert` enum + per-type structs |
| `settings_pack` int/bool/string bags | typed `Settings` struct + builder |
| `TORRENT_ASSERT` (debug only) | `debug_assert!` / `debug_assert_eq!` |
| `TORRENT_ASSERT_FAILS` in release | `unreachable!()` only if truly unreachable |
| `aux::session_impl` (one network thread) | one `tokio` task + handle/command channel |
| `disk_io_thread` + job queue | `spawn_blocking` pool + `mpsc` results |
| `deadline_timer` | `tokio::time::{sleep, Sleep}` |
| `socket_type` variant (TCP/SSL/SOCKS5/uTP) | `PeerConnector`/`PeerAcceptor`/`DatagramSocket` traits (`net::transport`, ADR-0008) |

## 7. Definition of done for a ported module

- [ ] Compiles with `clippy -D warnings`, `#![forbid(unsafe_code)]`.
- [ ] `//! Port of …` header cites the C++ files.
- [ ] Public items have rustdoc; non-trivial invariants documented.
- [ ] Corresponding `test/test_*.cpp` tests ported and passing.
- [ ] Wire-format compatibility validated where applicable (golden
      vectors generated *from* the C++ implementation).
- [ ] `docs/PORTING.md` status updated; `CHANGELOG.md` entry added.
- [ ] No `unwrap`/`expect`/`todo!`/`unimplemented!` outside `#[cfg(test)]`.

## 8. What NOT to port (or port differently)

- `src/ed25519/*`, `src/puff.cpp`, `src/sha1.cpp`, `src/sha256.cpp`,
  `win_cng`, `win_crypto_provider` → use vetted RustCrypto crates (ADR-0007).
- `posix_*`/`pread_*` storage → `PreadDiskIo` (std::fs positioned I/O)
  is the default backend; `mmap_*` (`file_view_pool`, `mmap.cpp`,
  `mmap_storage`, `mmap_disk_io`) → `bitdaemon-mmap` crate
  (feature-gated, sole `unsafe` carve-out — ADR-0010 §3).
- `bindings/` (python), `Jamfile`/`CMake` build system, deprecated-API
  shims, `debug.hpp` invariant-check scaffolding → `won't port`.
- Vendored C crypto (`src/ed25519/*`, `puff.cpp`, `sha1.cpp`,
  `sha256.cpp`, `win_cng`) → RustCrypto crates (ADR-0007), never ported.
- `rtc_stream`/`rtc_signaling` (WebRTC) **are in scope** (ADR-0010 §5)
  — `webrtc` crate vs. bespoke decided in a follow-up ADR.
- Everything else deferred → a ROADMAP phase, not a skip. A `won't
  port` row in PORTING.md requires an explicit reason.

## 9. Known traps (read before porting!)

- **bencode**: dict keys are sorted byte-lexicographically and stored
  *once*; `bdecode` is lazy — `data_section()` must return the exact
  original bytes of the `info` dict for info-hash computation. A
  re-encoded `info` dict is **not** hash-stable if it reorders keys.
- **Peer wire**: 4-byte big-endian length *includes* the 1-byte message
  id but not itself; keepalive = length 0. Block size cap: 16 KiB
  requests (`tor2`/default), hard limit enforced.
- **Info hash**: v1 = SHA-1 over raw bencoded `info` dict; v2 = SHA-256.
  Hybrid torrents carry both. Never re-serialize `info` to hash it.
- **Resume data** format is bencoded and must stay
  forward/backward-compatible with libtorrent's `read_resume_data.cpp`.
- **uTP** has subtle loss/delay heuristics — port `utp_stream.cpp`
  constants verbatim before tuning anything.
- **`settings_pack` defaults** are the behavioral spec: when in doubt,
  copy the default from `src/settings_pack.cpp`, not intuition.
- **Windows paths**: storage must sanitize `..`, drive letters, ADS
  (`file:`), reserved device names — `file_storage`/`path.cpp` logic is
  security-critical.
- **Disk fidelity is the product** (ADR-0009): never "simplify" the
  job queue, per-torrent `disk_job_fence` ordering, bounded
  `disk_buffer_pool`, lazy `file_pool`, or `part_file` — those are the
  fixes for real production failures downstream (349 s restores,
  unbounded memory, per-piece-lock corruption on chunk-straddling
  formats).
- **Lazy open = deferred errors**: files open on first access; invalid
  paths/permissions surface as alerts at first I/O, not at
  `add_torrent`. Do not "fix" this back to eager init — upstream
  behaves this way and OnionBit standardized on it.
- **Resume round-trip both ways**: `.resume`/`.session` must load
  upstream-written files *and* be readable by upstream. Test both
  directions with golden vectors.
- **Write coalescing + ordering**: piece-complete flush boundaries and
  fence semantics (e.g. `clear_piece`, `move_storage`, `delete_files`
  vs. in-flight writes) are correctness-critical — port
  `disk_job_fence` before any perf tuning.

## 10. Session state & alerts (the model)

C++ `session_impl` ≈ single-threaded actor: all mutation happens on the
network thread; `session_handle` forwards calls via `session_call`
(functor dispatch). Mirror this: a `Session` actor task + a
`SessionHandle` holding an `mpsc::Sender<Command>`; `pop_alerts` becomes
a `broadcast`/`mpsc` receiver. Timeouts (`deadline_timer`) map to
`tokio::time`. Torrent objects are owned by the actor, never `Arc`-shared.

When in doubt: **read the C++ first, then re-read it**. The answer to
"how should this behave" is always upstream, not intuition.
