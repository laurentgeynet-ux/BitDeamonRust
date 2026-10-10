# 0008. Injectable transports — no subsystem binds OS sockets directly

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner
- Supersedes: nothing — extends ADR-0002 (runtime detail rule)

## Context

The first downstream consumer of BitDaemon is **OnionBit**
(`D:\Projet\Tribler-Rust-Torrent`), a GPL-3.0 anonymous P2P ecosystem
porting Tribler. Its anonymous download lanes route **all** BitTorrent
traffic — peer uTP, DHT, UDP trackers — through IPv8 onion circuits as
`data` cells; no real UDP socket may be bound (anti-leak by
construction), and TCP peer connections are disabled entirely
(Tribler `enable_outgoing_tcp=False`, `anonymous_mode` semantics).

OnionBit currently embeds **vendored + patched librqbit** crates
(`vendor/librqbit-*`) precisely because rqbit hard-binds OS sockets —
the patch added a `DatagramSocket` trait, `UtpConnector`/`UtpAcceptor`,
and injectable DHT/tracker sockets (see their ADR-0007). That retrofit
cost is the concrete evidence for this ADR: a BitTorrent engine that
binds sockets internally cannot be tunnelled without surgery.

The same seam independently serves our own **simulation harness**
(ADR-0006 layer 4, ROADMAP Phase 8) — deterministic swarm tests need
virtual sockets.

## Decision

1. **`bitdaemon-net::transport`** (already scaffolded) defines the
   object-safe injection seams:
   - `DatagramSocket` — `send_to`/`recv_from`/`local_addr` over boxed
     futures (the rqbit patch's exact shape, deliberately).
   - `PeerConnector` / `PeerAcceptor` — outbound/inbound peer streams
     yielding `Box<dyn PeerStream>` (`AsyncRead+AsyncWrite`).
   - `PeerStream` — blanket-impl'd stream abstraction.
2. **Nothing below `bitdaemon-session` binds a socket.** DHT
   (`bitdaemon-dht`), UDP trackers, uTP socket manager, LSD announce —
   all consume `Arc<dyn DatagramSocket>` handed down by session config.
   Peer connections are established via `PeerConnector`/`PeerAcceptor`.
   The only place real `tokio::net::UdpSocket`/`TcpListener` appear is
   the default `TokioUdpSocket`/`TcpConnector` impls in `net`.
3. **Anonymous-lane session flags are first-class**, mirroring
   libtorrent's semantics (not rqbit's): `enable_outgoing_tcp`,
   `enable_outgoing_utp`, `enable_incoming_tcp`, `enable_incoming_utp`,
   `anonymous_mode`, `force_proxy` exist in typed `Settings`
   (ADR-0004). `enable_outgoing_tcp=false` means **zero TCP for
   peers** — even through a proxy — matching Tribler's lane config.
4. **Storage is also injectable**: `bitdaemon-disk` exposes a
   `StorageFactory` trait (upstream `storage_constructor` /
   `default_storage_constructor`), so OnionBit's `PrivateStorageFactory`
   pattern ports without patching.
5. HTTP(S) trackers keep using a real (proxied) HTTP stack — tunnels
   can't carry them upstream either (one-shot `http-request` cells,
   no CONNECT semantics); that's a documented limitation, not a gap.

## Consequences

- `uTP` is no longer "Phase 8 optional" — for OnionBit's anonymous
  lane it is *the* peer transport. ROADMAP pulls uTP forward to
  Phase 5 alongside trackers.
- `EngineConfig`-equivalent fields (`utp_socket`, `utp_listen_socket`,
  `dht_socket`, `udp_tracker_socket`, `enable_tcp`, `utp_only`,
  storage factory) map to typed `SessionParams`/`Settings` fields —
  see milestone ONIONBIT-1 in ROADMAP.
- Every UDP-based module ported gets a "consumes `DatagramSocket`"
  note in `docs/PORTING.md`.
- `Transport`/`DatagramSocket` must stay `Debug + Send + Sync + 'static`
  object-safe — `async_trait`-free boxed futures, matching the vendored
  rqbit shape so downstream traits port 1:1.
- Testing benefits immediately: the Phase-8 simulator registers as a
  `DatagramSocket`/`PeerConnector` impl, no extra seams needed.
