# golden-vectors

Fixture generator run **against the C++ upstream checkout**
(`D:\Projet\libtorrent\libtorrent`) — it produces the reference bytes
BitDaemon must match. This is the Phase A/F conformance pipeline
(ADR-0006, ADR-0010).

## Build

Requires a compiled libtorrent (CMake or b2 — see upstream docs).
With an installed/built libtorrent:

```sh
c++ -std=c++17 generate.cpp \
    -I<upstream>/include \
    -L<upstream>/build -ltorrent \
    -o generate
./generate <out-dir>
```

### Windows / MSVC (procedure used for the v2.1.2 fixtures)

The shipped fixtures were generated against the **v2.1.2 release**
(extracted via `git archive v2.1.2` into `D:\Projet\libtorrent-v2.1.2`
so the read-only reference checkout stays untouched).

```bat
REM 1. upstream release build (Boost headers only, VS 2026 / MSVC)
cmake -B build -G "Visual Studio 18 2026" -A x64 ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DBOOST_ROOT=<boost-root> -DBoost_NO_SYSTEM_PATHS=ON ^
    -Dbuild_tests=OFF -Dbuild_examples=OFF -Dbuild_tools=OFF ^
    -Dwebtorrent=OFF
cmake --build build --config Release --parallel

REM 2. generator: build_generate.bat
cl /std:c++17 /EHsc /MD /O2 /DTORRENT_LINKING_SHARED /DTORRENT_USE_RTC=0 ^
   /DTORRENT_ABI_VERSION=2 /DBOOST_ASIO_NO_DEPRECATED ^
   /DBOOST_SYSTEM_USE_UTF8 /DBOOST_ALL_NO_LIB ^
   /I <release>\include /I <boost-root> generate.cpp ^
   /link /LIBPATH:<release>\build\Release torrent-rasterbar.lib

REM 3. run (DLL must be on PATH)
set PATH=<release>\build\Release;%PATH%
generate.exe <out-dir>
```

Notes: `git archive` does not fetch submodules — `deps/try_signal`
must be cloned from github.com/arvidn/try_signal, and `-Dwebtorrent=OFF`
avoids the `deps/libdatachannel` dependency entirely.

Each generator writes `<name>.bin` (usually a bencoded blob) into
`<out-dir>`. Commit the produced fixtures under
`crates/<crate>/tests/data/` when a conformance test consumes them —
fixture names are a contract, don't rename.

## Contract

- Bytes must come from the C++ implementation, never from BitDaemon
  (a self-generated "golden" proves nothing).
- When upstream output is intentionally non-deterministic (random
  peer-ids, timestamps), either fix the entropy source in the
  generator or record the variable fields in the fixture manifest.
- Rust-side loader: `tests/data/<name>.bin` + `bdecode` compare, or
  byte-compare for byte-exact formats (resume data, .torrent info
  dict).

## Current generators

| fixture | what it pins |
|---|---|
| `bencode_sorted_dict` | dict key order, nesting, lazy bdecode offsets |
| `bencode_edge_ints` | int64 min (decodes to 0 — upstream overflow quirk), empty strings |
| `resume_empty` / `resume_buf_empty` | `write_resume_data` wire format |
| `resume_rich` | resume flags/counters/limits on populated ATP |
| `fingerprint_*` | `generate_fingerprint` `-XXNNNN-` encoding |
| `magnet_hybrid` | `make_magnet_uri` param order + escaping |
| `session_default` / `session_custom` | `write_session_params` dict (`.session` params) |
| `dht_state_nodes` | `dht::save_dht_state` nids/nodes serialization |
| `torrent_v1_synthetic` | `create_torrent` + `set_piece_hashes` output |
| `torrent_v2_synthetic` | v2-only torrent (file tree, pieces root, pad file) |

Consumed by `tests/test_golden.rs` in `bitdaemon-bencode`,
`bitdaemon-core`, `bitdaemon-dht` and `bitdaemon-session` — all
byte-compared against upstream v2.1.2 output.
