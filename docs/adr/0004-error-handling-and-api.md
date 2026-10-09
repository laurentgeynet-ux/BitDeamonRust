# 0004. Error handling and API conventions

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

Upstream error model: `boost::system::error_code` via out-params
(`void f(error_code& ec)`), throwing overloads for the "exceptional"
API, `lt::errors` categories, plus `TORRENT_ASSERT` for invariants.
Rust needs a coherent translation that stays debuggable against the
C++ spec.

## Decision

1. **Per-crate `thiserror` enums.** Each crate defines `XxxError`
   (e.g. `BdecodeError`, `TorrentInfoError`, `NetError`, `DiskError`,
   `SessionError`, `DhtError`) with `std::error::Error` impls. Crates
   re-export a `Result<T>` alias. A wrapper `bitdaemon::Error` (enum of
   crate errors via `#[from]`) lives in the facade.
2. **Variant granularity follows upstream `lt::errors`.** When porting
   a code path that sets a specific `error_code`, create a
   corresponding variant carrying context (`#[error("invalid piece
   index {0}")]`), not a generic `Other`. Doc-comment the C++ error it
   replaces when non-obvious.
3. **No panics in library paths** (AGENTS.md §4.3). Internal
   invariants use `debug_assert!` (mirrors `TORRENT_ASSERT`, which is
   debug-only upstream too). Truly-impossible states may use
   `unreachable!()` with a comment justifying impossibility.
4. **`Option` for absence, `Result` for failure** — never collapse to
   `Option<T>` where upstream distinguishes error kinds.
5. **Typed settings**: `settings_pack`'s untyped int/bool/string bags
   become a `Settings` struct with named fields and upstream defaults;
   unknown-key extensibility is kept via a reserved `ext` map rather
   than open-ended enum indexing. `Alert` hierarchy → one enum.
6. **Naming**: keep upstream nouns (`TorrentInfo`, `AddTorrentParams`,
   `AnnounceEntry`) so docs/PORTING.md stays greppable; rename only
   where Rust idiom demands (`torrent_handle::is_valid` →
   `TorrentHandle::is_valid` — method on the type, etc.).

## Consequences

- Error enums will grow large (upstream has ~250 `lt::errors` values);
  that's acceptable — exhaustiveness aids debugging against upstream.
- Adding an error variant is not a breaking change until 1.0
  (`#[non_exhaustive]` on public error enums from day one).
- The `Settings` struct becomes the compile-time documentation of
  defaults — a correctness improvement over the runtime bag.
