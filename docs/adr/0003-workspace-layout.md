# 0003. Multi-crate workspace layout

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

libtorrent ships as one library (`libtorrent-rasterbar`) whose ~290
headers intermix foundations (`bdecode`, `sha1_hash`) with orchestration
(`session_impl`). A single-crate port would build fine but offers no
enforced boundaries, slow full builds during the multi-year port, and
forces downstream users to compile the DHT/uTP/disk stack even if they
only want bencode.

Options: single crate with modules vs. multi-crate workspace.

## Decision

A Cargo workspace of **9 crates** with a strict downward dependency
graph (enforced by review — Cargo does not check it):

```
bitdaemon-common      (no internal deps — primitives)
bitdaemon-bencode     (no internal deps — codec)
bitdaemon-core        → common, bencode      (metainfo)
bitdaemon-disk        → common, bencode, core        (storage)
bitdaemon-net         → common, bencode, core        (wire+trackers)
bitdaemon-dht         → common, bencode, net         (kademlia)
bitdaemon-session     → all above                    (orchestration)
bitdaemon             → all above                    (facade)
bitdaemon-cli         → bitdaemon                    (binary)
```

Rationale for the cut lines:

- **common/bencode/core** are reusable standalone (any tool parsing
  torrents needs only these).
- **disk vs net** are independent branches off `core` — neither depends
  on the other, matching upstream where `disk_interface` and
  `peer_connection` are decoupled and both plug into `torrent`.
- **bandwidth** code lives in `net` (it wraps sockets), and session
  consumes it via trait — the C++ `bandwidth_manager` sits closer to
  sockets than to torrent logic.
- **dht** depends on `net` for the UDP socket abstraction.
- **session** is the only crate allowed to depend on all others —
  mirroring `session_impl` owning everything.
- **facade `bitdaemon`** re-exports the public API so casual users get
  one dependency; granular crates remain available for tools.

New crates require an ADR (or an explicit superseding note here).

## Consequences

- Compile times during the port stay low for the crates actually being
  edited; `cargo test -p` scopes work.
- The boundary is a forcing function: helpers needed across two crates
  must be demoted to `common`, which prevents the `aux_` "grab-bag"
  problem where C++ let everything reach into everything else.
- Publishing order is fixed bottom-up (Phase 10 in ROADMAP).
- Risk: over-eager abstraction at wrong seams. Mitigation: seams follow
  upstream's own subsystem split, which has 20 years of evolution.
