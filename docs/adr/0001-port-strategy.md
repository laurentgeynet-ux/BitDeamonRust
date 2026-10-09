# 0001. Semantic port, not transliteration nor FFI

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

The goal is a production-grade BitTorrent library in Rust, derived
from libtorrent (~170k LOC C++, boost::asio, heavy use of inheritance,
`error_code` out-params, `shared_ptr` graphs).

Three strategies were possible:

1. **FFI binding** (`libtorrent-sys` + safe wrappers): fastest to
   ship, but keeps C++ UB, boost dependency, ABI pain on Windows, and
   adds no Rust value. Rejected — the project explicitly targets a
   100% Rust codebase.
2. **Mechanical transliteration** (line-by-line C++→Rust, e.g. via
   c2rust then cleanup): preserves structure including the bad parts —
   callback spaghetti, `shared_ptr` cycles, `void*`-style type erasure.
   Produces un-idiomatic, hard-to-maintain Rust.
3. **Semantic port**: port each subsystem so that *observable behavior*
   matches — wire protocols, file formats (`.torrent`, resume data,
   `.session`), state-machine transitions, timing/retry policy, default
   settings — while APIs, ownership and concurrency are designed
   idiomatically in Rust.

## Decision

Adopt **semantic port**:

- Fidelity is defined at the boundary: wire bytes, disk formats,
  alert/state semantics, and upstream test-suite outcomes.
- Internal structure follows Rust idioms: traits over inheritance,
  enums over class hierarchies, `Result` over `error_code&`,
  ownership/actors over shared pointers.
- The C++ single-network-thread model (`session_impl` on one
  io_service thread) maps to a **single session actor task** owning all
  torrent state; handles communicate via channels. This preserves the
  upstream no-shared-mutable-state guarantee without `Arc<Mutex>`
  sprawl.
- Port order follows the dependency graph and `ROADMAP.md` phases —
  never port a consumer before its dependencies exist.
- Every ported module names its C++ origin in a `//! Port of …`
  doc-line so the mapping stays greppable via `docs/PORTING.md`.

## Consequences

- Slower per-file progress than transliteration, but each merged
  module is idiomatic and reviewable.
- We inherit upstream's *behavioral* test suite as the spec: porting
  `test/test_*.cpp` cases is mandatory (ADR-0006).
- Deviations must be explicit: any place where we intentionally
  differ from upstream is recorded in `docs/PORTING.md`'s deviation
  log — silently "improving" behavior is forbidden because it breaks
  interoperability in non-obvious ways.
- Downstream users get a Rust-native API (typed `Settings`, enum
  `Alert`, `async` handles) rather than a C++ API wearing Rust syntax.
