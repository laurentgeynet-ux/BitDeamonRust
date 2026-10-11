# ADR-0019: NAT-PMP — gateway discovery + `start_natpmp` wiring

- Status: accepted
- Date: 2026-10-11
- Deciders: port team

## Context

`net::natpmp` (port of `natpmp.cpp`) is ported end-to-end: request
format, response parsing, renewal logic (`natpmp_renew` runs in the
engine tick). `Engine::start_natpmp(gateway, local_port, ttl)` exists
— **nothing calls it**, because nothing discovers the default
gateway. Upstream gets it from `enum_net_interfaces`/`enum_routes`
(`enum_net.cpp`); our `net::enum_net` keeps the pure helpers and
injects the OS enumeration — but no default implementation or wiring
exists, so `natpmp` stays `None` forever.

## Decision

- Add a `NetInterfaces`/`RouteEnumerator` injectable trait in
  `net::enum_net` (default impl: `net2`/`if-addrs`-style OS
  enumeration + route table read on Windows `GetBestRoute`,
  Linux `/proc/net/route`) producing `Vec<NetIf>` + default gateway
  v4 address.
- Session start: if `SessionServices::natpmp` and a default gateway
  exists → `engine.start_natpmp(gateway, listen_port, ttl)`; the
  existing `natpmp_renew` tick drives retries/renewals.
- Responses: already demuxed by gateway address match
  (`engine.rs` `from == n.gateway`) — no change needed.
- Downstream injects the trait for anonymous lanes (gateway lookup
  happens inside the tunnel namespace or is declined outright).

## Consequences

- Completes the NAT traversal pair with ADR-0018; on gateways
  speaking both, upstream prefers NAT-PMP — keep the same precedence.
- The trait is a new public seam in `bitdaemon-net` — semver-visible
  before the 1.0 freeze (Phase G).
