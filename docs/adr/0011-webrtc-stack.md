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

## Phase N assessment (2026-10-12)

Binding analysis for `str0m` against the ported seam:

- `RtcPeerConnection::create_data_channel` →
  `str0m::Rtc::direct_api().create_data_channel(...)`; events feed
  `RtcEvent::{GatheringComplete,Failed,DataChannel}` from
  `rtc.poll_output()` `Output::Event`s.
- `set_remote_description` → `rtc.sdp_api()` offer/answer accept;
  the local SDP at `GatheringComplete` is str0m's generated
  description post-ICE-gathering.
- The blocker is **not** the seam (it maps cleanly) but the driver:
  str0m is Sans-IO — each `RtcPeerConnection` needs a tokio task
  pumping `handle_input`/`poll_output`/`poll_transmit` against a
  UDP transport *plus* the `Instant` timer loop. Per ADR-0008 that
  socket must be an injected `Arc<dyn DatagramSocket>`, not a bound
  `UdpSocket` — the factory signature must gain a transport
  parameter, which is a real API change, not a shim.
- Verdict: the leaf stays **optional-deferred** — roadmap Phase N
  marks it non-blocking (`no functional blocker for TCP/uTP/SSL
  swarms`), no current consumer (OnionBit routes over its own lanes,
  WebTorrent peers are out of its threat model), and an untested
  driver can't be wire-verified without a real WebTorrent peer.
  Landing criteria: a consumer asks for it, or interop validation
  against libtorrent's WebTorrent peers is scheduled.
