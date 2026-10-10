# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

### Added

- Workspace scaffolding: 9 crates (`common`, `bencode`, `core`, `disk`,
  `net`, `dht`, `session`, facade `bitdaemon`, `bitdaemon-cli`).
- Project governance docs: `AGENTS.md`, `ROADMAP.md`, `CONTRIBUTING.md`,
  `docs/PORTING.md` (C++→Rust module mapping), `docs/architecture.md`.
- Architecture decision records `docs/adr/0001`–`0007` covering port
  strategy, tokio runtime, workspace layout, error handling, license,
  testing/conformance and dependency policy.
- CI workflow: `fmt`, `clippy -D warnings`, `cargo test`, MSRV 1.85,
  on Ubuntu and Windows.
- Reference pin: libtorrent `master` @ `v2.1.2-45-gc019e80ef`.
- ADR-0008 injectable transports: `bitdaemon_net::transport` defines
  `DatagramSocket`, `PeerConnector`, `PeerAcceptor`, `PeerStream` —
  no subsystem binds OS sockets; anonymous/tunnel lanes and the future
  simulator plug in without patching (replaces rqbit vendoring need).
- ROADMAP: downstream driver section (OnionBit rqbit replacement),
  milestone ONIONBIT-1, uTP moved to Phase 5.
- `bitdaemon_net::NetError`/`Result` foundation for transport errors.

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
