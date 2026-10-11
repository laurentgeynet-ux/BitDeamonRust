# ADR-0011: WebRTC stack — injected `rtc::PeerConnection` seam

Status: Accepted
Date: 2026-03-08

## Context

Upstream `rtc_stream.cpp`/`rtc_signaling.cpp` (WebTorrent/BEP-XX peer
transport) are built directly on libdatachannel (`rtc::PeerConnection`,
`rtc::DataChannel`). No mature pure-safe-Rust equivalent exists:
`webrtc` (webrtc-rs) is async and heavy but featureful; `str0m` is
sync/Sans-IO and lighter. Picking one at the library level would bake a
large dependency into `bitdaemon-net` and violate ADR-0008's "no
subsystem owns its transport" rule — OnionBit, in particular, needs
RTC over its own lanes.

## Decision

Port `rtc_signaling` and `rtc_stream` as **stack-agnostic state
machines** over injected traits, mirroring how upstream talks to
libdatachannel:

- `RtcPeerConnectionFactory` / `RtcPeerConnection` — the
  `rtc::PeerConnection` seam (`create_data_channel`,
  `set_remote_description`, events `GatheringComplete`/`Failed`/
  `DataChannel` replacing libdatachannel's callbacks).
- `RtcChannel` — the `rtc::DataChannel` seam
  (`send`/`available`/`is_open`/`close`); `RtcStream` adapts it to
  `AsyncRead`/`AsyncWrite`.

All upstream signaling semantics are ported verbatim: 20-byte random
offer ids, offer batches (`offer_batch` completion incl. failed-offer
decrement), `max_webtorrent_offers` incoming cap, duplicate-offer and
duplicate-answer dedup, `webtorrent_connection_timeout` negotiation
deadline, `webtorrent_stun_server` ICE config.

## Consequences

- A concrete binding (e.g. `bitdaemon-webrtc` on `str0m`, or OnionBit's
  own stack) is a downstream/optional crate — no WebRTC dependency is
  forced on `bitdaemon-net` consumers.
- The state machine is fully testable with a mock factory (7 tests in
  `rtc_signaling.rs`).
- When a default stack is chosen for `bitdaemon-cli`, the choice is a
  leaf decision and needs no further ADR; swapping `str0m` ↔ `webrtc`
  only touches the factory impl.
