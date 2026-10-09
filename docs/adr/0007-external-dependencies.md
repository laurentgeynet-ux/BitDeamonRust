# 0007. External dependency policy

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

Upstream bundles or depends on: boost (asio, system, optional, variant,
multi_index…), OpenSSL (SHA-1/SHA-256, RC4, SSL), zlib (gzip),
a bundled ed25519 implementation (ref10-derived), bundled puff (zlib
inflate). Rust has a mature crate ecosystem; re-porting vetted crypto
is wasted effort *and* a security risk.

## Decision

1. **Use established crates for crypto/compression** — do not port the
   bundled C code: `sha1`, `sha2`, `crc32c`, `ed25519-dalek` (DHT
   BEP-44), `rc4`/RustCrypto for MSE/PE, `flate2` for gzip,
   `rustls` (not OpenSSL) for SSL torrents/HTTPS trackers.
2. **Pin minor versions** in `[workspace.dependencies]` only; crates
   declare `x.y` ranges. Bump deliberately via `cargo update -p` in a
   dedicated commit.
3. **Maturity gate**: prefer versions published ≥ 7 days ago, ≥ 1.0
   where possible, maintained within the last year. Exceptions
   documented in this file's table below.
4. **`cargo deny` + `cargo vet`** (Phase 0 exit criteria): licenses
   checked per ADR-0005 allow-list; advisories checked on CI.
5. **Feature minimality**: default features off where practical
   (`default-features = false`) to keep the dep tree small — audit when
   adding.
6. **Adding a dependency** requires a commit-message justification;
   adding a *category* (new async runtime, new TLS stack, new database)
   requires a new ADR.

### Pre-approved / chosen stack

| Need | Crate | Notes |
|---|---|---|
| async runtime | tokio | ADR-0002 |
| byte buffers | bytes | |
| framing | tokio-util::codec | |
| errors | thiserror | |
| hashes | sha1, sha2, crc32c | RustCrypto |
| ed25519 | ed25519-dalek | BEP-44 (decide feature set when DHT lands) |
| gzip | flate2 | trackers + compressed .torrent |
| TLS | rustls | replaces OpenSSL — new ADR when SSL torrents port |
| bitfield | bitvec | |
| random | rand | `generate_peer_id`, transaction ids |
| logging | tracing | |
| CLI | clap | bitdaemon-cli only |
| url | url | tracker/magnet parsing |
| (Phase 3 eval) | memmap2 | optional mmap backend |
| (Phase 8 eval) | webrtc or bespoke | rtc_stream — decide then |

## Consequences

- Smaller port surface (~15k C++ lines avoided: ed25519, puff, sha1,
  sha256, cpuid paths).
- Supply-chain risk accepted and mitigated via deny/vet + pin policy.
- `rustls` changes certificate handling vs upstream OpenSSL paths —
  record deviations in PORTING.md when SSL lands (Phase 8).
