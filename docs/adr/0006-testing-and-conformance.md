# 0006. Testing and conformance strategy

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

A BitTorrent library is only correct if it interoperates. Unit tests
that only verify self-consistency can pass while the wire format is
wrong. Upstream has ~150 `test/test_*.cpp` files, a `simulation/`
network simulator, `fuzzers/`, and test data (`.torrent`, corrupt,
gzip) — collectively the executable spec of the port.

## Decision

Four-layer conformance strategy, in order of adoption:

1. **Ported unit tests** (mandatory per module, ADR-0001): translate
   the corresponding `test/test_<module>.cpp` assertions. Copy test
   *data* (never code) into `crates/<crate>/tests/data/`.
2. **Golden vectors** for wire formats: generate fixtures *from the C++
   build* — bencode encodings, `bt` handshake bytes, tracker requests,
   resume-data output, `.session` files — stored under
   `tests/vectors/`. A small C++ dumper (Phase 0 exit criterion)
   produces them once; Rust tests assert byte-equality. This catches
   "looks right" bugs that ported tests can miss when both sides were
   ported with the same misunderstanding.
3. **Differential / integration tests**: loopback scenarios between
   BitDaemon instances (Phase 6), then BitDaemon↔C++ libtorrent
   pairs, then real clients (Phase 9).
4. **Fuzzing** (`cargo-fuzz`): bdecode, peer-wire codec, DHT `msg`,
   tracker responses, resume parser — mirroring `fuzzers/` upstream.
   Deferred until the parsers exist; harness structure decided then.

Skipped/deferred: upstream `simulation/` (fake network for
deterministic swarm tests) is a large port of its own — Phase 8. Until
then, `tokio::time::pause` covers deterministic-timing needs.

## Consequences

- The golden-vector generator must run before Phase 1 merges (it needs
  only a C++ compiler + the upstream checkout).
- `verified` status in PORTING.md is only granted when a module passes
  layers 1+2 (formats) or 1+3 (protocols).
- CI stays fast: layers 1–2 in `cargo test`; layers 3–4 behind a
  `conformance` cargo alias / separate CI job.
