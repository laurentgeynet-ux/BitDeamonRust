# 0002. tokio as the async runtime

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

libtorrent's networking is built on boost::asio: one io_service thread
for all sockets and timers, plus a dedicated disk-I/O thread pool.
The Rust port needs an equivalent executor + reactor for TCP, UDP,
uTP-over-UDP, timers, DNS, and a blocking pool for disk work.

Candidates:

| Runtime | Pros | Cons |
|---|---|---|
| **tokio** | de-facto standard; `tokio-util` codecs, `Bytes`, timers, `spawn_blocking`, biggest ecosystem (rustls, quinn, hickory-dns) | heavy dependency tree |
| smol | small, runtime-agnostic style | thin ecosystem, fewer helpers, niche |
| async-std | std-like API | maintenance slowed, ecosystem consolidating on tokio |
| std threads + polling | zero-dep, full control | re-implements what tokio already solved; high risk |

## Decision

Use **tokio** (with `tokio-util` for framing, `bytes` for buffers,
`futures` for utilities) as the sole async runtime of the workspace.

Concrete mappings:

- `boost::asio::io_context` + `post`/`dispatch` → tokio tasks + `mpsc`
  command channels.
- `deadline_timer` → `tokio::time::sleep` / `Interval`.
- `tcp::socket`/`udp::socket` → `tokio::net::*`.
- `disk_io_thread` job queue → `tokio::task::spawn_blocking` (evaluate
  a dedicated `rayon`/custom pool in Phase 3 if throughput requires —
  record outcome here or in a superseding ADR).
- The runtime is an **implementation detail**: public API types must
  not leak `tokio` types where avoidable (accept `AsyncRead`/`AsyncWrite`
  generics for transport injection in tests), so a future runtime swap
  remains possible without a breaking API change.

## Consequences

- All crates pull tokio via `[workspace.dependencies]`; features are
  centralized (currently `full`, to be slimmed when the port matures).
- Tests use `#[tokio::test]`; deterministic timing tests use
  `tokio::time::pause`/advance.
- The runtime-detail rule means session/disk traits are defined over
  futures and channels, not concrete tokio sockets — important for the
  simulation harness port (Phase 8) which will inject virtual sockets.
