# ADR-0018: UPnP — SSDP discovery wiring (M-SEARCH → mapping)

- Status: accepted
- Date: 2026-10-11
- Deciders: port team

## Context

`net::upnp` (port of `upnp.cpp`) is complete: `UpnpDriver::discover()`
emits the `GetExternalIPAddress` → `AddPortMapping` chain, the engine
owns `upnp_devices`/`upnp_rx`/`drive_upnp_outs`, and
`register_upnp_device` exists. **Nothing emits the SSDP M-SEARCH.**
Upstream's `upnp` object owns an SSDP socket bound to
239.255.255.250:1900 that multicasts `M-SEARCH` for
`urn:schemas-upnp-org:device:InternetGatewayDevice:1`/`2` and parses
the `LOCATION:` headers into `RootDevice`s pushed to the device list.
Without it no inbound port mapping ever gets requested — the client
only accepts peers on an already-open port, degrading NAT'd swarms.

## Decision

- SSDP transport stays injectable (ADR-0008): the engine drives
  discovery through the session's `DatagramSocket` (multicast send to
  239.255.255.250:1900; replies arrive on the UDP demux, same path as
  NAT-PMP responses). No OS-specific multicast join in the first pass —
  unicast responses to M-SEARCH arrive at the bound socket; passive
  NOTIFY listening is a follow-up.
- `start_upnp()` triggers an M-SEARCH burst (upstream sends 3 spaced
  ~250 ms); `stop_upnp()` sends `DeletePortMapping` for live mappings
  then drops the device list.
- HTTP `LOCATION` fetch (device description XML) reuses the injected
  `Resolver` + `PeerConnector`/plain TCP — no direct socket bind.
- Mapping lifecycle follows upstream `upnp.cpp`: map listen port
  TCP+UDP on every discovered IGD, refresh at `mapping.expires/2`,
  `Portmap` alerts surfaced through the alert bus.

## Consequences

- Inbound connectivity improves on NAT'd hosts — user-visible.
- Requires a periodic engine task (renew + re-discover), same pattern
  as `natpmp_renew`.
- OnionBit's anonymous lanes must be able to *disable* UPnP entirely
  (mapping a real port deanonymizes) — the setting gate already
  exists (`SessionServices::upnp`); the discovery burst must respect
  it and injected transports may decline multicast sends.
