# 0010. Complete-fidelity port — everything upstream compiles, we port

- Status: accepted
- Date: 2026-10-11
- Deciders: project owner
- Extends: ADR-0001 (semantic port), ADR-0009 (disk fidelity)
- Amends: ROADMAP "explicit non-goals" — the OnionBit-driven deferrals are lifted

## Context

The roadmap was shaped by OnionBit's needs: BitTorrent v1 first, uTP
pulled forward, and a tail of "deferred / won't port" items (SSL
torrents, WebRTC signaling, `ut_holepunch`, `i2p_pex`, `mmap_*`,
`peer_class`, `simulation`, RSS, `mmap_disk_io`, …).

The project goal has changed: **port all of
`D:\Projet\libtorrent` @ `v2.1.2-45-gc019e80ef`**, the full ~170k LOC
surface, so that BitDaemon is a drop-in semantic equivalent of upstream
— not a feature subset.

"Everything" still excludes what is definitionally non-Rust or
non-portable: C build system (`Jamfile`, `CMake`), Python `bindings/`,
vendored C crypto (`src/ed25519/*`, `puff.cpp`, `sha1.cpp`,
`sha256.cpp` — replaced by RustCrypto crates per ADR-0007), C++
container/boost shims, and OS-specific `win_*`/posix shims. Those stay
`won't port` in `PORTING.md` but every *behavioral* file gets a Rust
module.

## Decision

1. **Every `src/*.cpp` and `src/kademlia/*.cpp` file maps to a Rust
   module or an explicit, reasoned `won't port` row in PORTING.md.**
   "Deferred" is no longer a permanent status — it means "later
   phase", never "never".
2. **Phase order is re-driven by dependency depth, not by OnionBit**:
   session completeness → net completeness → DHT completeness →
   advanced features (SSL, WebRTC, holepunch, i2p_pex) → simulation
   harness → conformance/hardening.
3. **`#![forbid(unsafe_code)]` stays on all existing crates.** `mmap`
   storage (`file_view_pool`, `mmap_disk_io`, `mmap_storage`) gets its
   own crate `bitdaemon-mmap` behind a cargo feature, which is the
   single workspace member allowed `unsafe` (review-scoped). This
   un-defers the mmap backend instead of dropping it.
4. **SSL**: `ssl.cpp`/`ssl_stream` port to rustls (ADR-0007) — SSL
   torrents, HTTPS trackers, cert pinning. `win_cng` remains replaced
   by RustCrypto.
5. **WebRTC**: `rtc_stream`/`rtc_signaling` are ported (evaluate
   `webrtc` crate vs. bespoke SDP/signaling; decision recorded in a
   follow-up ADR when reached).
6. **`simulation/` harness is ported** as `crates/bitdaemon-sim`
   (injected `DatagramSocket`/`PeerConnector` seams from ADR-0008 make
   this natural) — it becomes the differential-test backbone.
7. **Coverage accounting**: `PORTING.md` gains a completion metric —
   every row is `ported`/`verified`/`won't port`+reason; "in progress"
   and "started" rows must drain to zero. Full-port target =
   `PORTING.md` 100% resolved rows.
8. **Testing floor per module stays as defined** (AGENTS.md §7):
   ported `test_*.cpp` cases, `//! Port of …` headers, PORTING.md +
   CHANGELOG updated in the same commit.

## Consequences

- Roadmap rewritten (see `ROADMAP.md`) — OnionBit milestone becomes a
  downstream consumer note, no longer the ordering driver.
- New crate `bitdaemon-mmap` (feature-gated, sole unsafe carve-out)
  and `bitdaemon-sim` added to the workspace graph.
- `won't port` rows must each carry a reason ("build system",
  "replaced by crate X", "platform shim") — no silent gaps.
- Larger review surface: every ported module follows the §5 porting
  workflow (C++ read → design → implement → port tests → mark status).
- `rust-toolchain.toml`, `cargo deny` move from "hardening leftover"
  to a phase prerequisite (dependency surface grows: rustls, webrtc).
