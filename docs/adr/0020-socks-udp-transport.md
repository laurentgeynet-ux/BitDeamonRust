# ADR-0020: SOCKS5 UDP-associate as injected `DatagramSocket`

- Status: accepted
- Date: 2026-10-11
- Deciders: port team

## Context

`net::socks_udp::Socks5UdpSocket` (port of the UDP-associate half of
`socks5_stream.cpp`) is implemented and green in end-to-end mock
tests — but only in tests. `SessionParams`/`Engine` never receive it
as the session `DatagramSocket`, so even with a SOCKS5 proxy
configured, UDP trackers, DHT and uTP keep using the direct UDP
socket. OnionBit's anonymous lanes need exactly this seam.

## Decision

- `SessionParams`/`Session` proxy config (`settings_pack::proxy_*`)
  gains the wiring: when `proxy_type = socks5` (or `socks5_pw`) and
  `proxy_udp`/`proxy_peer_connections` allow it, the engine builds
  `Socks5UdpSocket::associate(proxy, auth)` and hands it to DHT/uTP/
  UDP-tracker demux instead of the direct socket.
- `Socks5UdpSocket` keeps implementing `transport::DatagramSocket`;
  wrap/unwrap of the SOCKS5 UDP header stays inside it (already
  done). DNS names may be handed to the proxy unresolved (upstream
  `proxy_hostnames`) — flag-respecting.
- Failure policy follows upstream: if the associate fails at session
  start, UDP features stay down (no silent fallback to direct — that
  would leak; upstream `proxy_udp` semantics).
- CLI: `--proxy-host/--proxy-port` already exist? If not, add
  `--proxy <host:port>` + `--proxy-user` for manual testing.

## Consequences

- DHT/uTP/UDP trackers can run through SOCKS5 or through OnionBit's
  tunnel transport — the last un-proxied socket path closes.
- `DatagramSocket` trait is already public; only the session wiring
  is new — no dependency change.
