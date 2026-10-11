# ADR-0021: DHT `dos_blocker` instantiation

- Status: accepted
- Date: 2026-10-11
- Deciders: port team

## Context

`dht::dos_blocker` (port of `kademlia/dos_blocker.cpp`) is complete —
per-source rate limiting, `incoming()` banning of flooders, ban
timeouts — but is **never instantiated**: `dht_tracker`/`node` accepts
every inbound datagram regardless of rate. Public Mainline nodes get
scanned/flooded constantly; upstream drops abusive sources, we process
them all (CPU + log noise + ban-less abuse surface).

## Decision

- Instantiate `DosBlocker` in `dht::node` (one per bound socket,
  upstream attaches it to `dht_tracker`/`node` per listen socket).
- Call `dos_blocker.incoming(addr, now, node_id_log)` at the top of
  the UDP demux path before parsing; drop the datagram when it
  returns false. This complements (not replaces) the existing
  RPC-side `pongs` rate limiting — the blocker runs *pre-parse*.
- Keep upstream constants (`block_timeout`, message-rate window);
  add a `dht_debug`/`log` line on ban (upstream logs via dht_logger).
- No new settings: upstream exposes no toggle for the blocker.

## Consequences

- Cheap: one `Mutex`-free struct check per datagram.
- Closes the last un-wired `src/kademlia/*` file — the DHT module
  becomes fully instantiated, matching `PORTING.md` claims.
