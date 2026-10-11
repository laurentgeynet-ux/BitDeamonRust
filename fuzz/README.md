# BitDaemon fuzz targets

cargo-fuzz scaffold mirroring upstream `fuzzers/` (ADR-0010, Phase F).

Targets:

- `bdecode` — `bdecode` parser + `bencode` re-encode round-trip
- `peer_wire` — peer-wire message decode
- `resume_data` — `read_resume_data` entry points

## Running

libFuzzer requires the coverage/sanitizer runtime shipped only on
Linux toolchains. On Windows host: run under WSL or CI.

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
cargo +nightly fuzz run bdecode
cargo +nightly fuzz run peer_wire
cargo +nightly fuzz run resume_data
```

Corpus seeds: the golden fixtures under `crates/*/tests/data/` are
good initial inputs, e.g.

```sh
cargo +nightly fuzz run bdecode fuzz/corpus/bdecode ../crates/bitdaemon-bencode/tests/data
```
