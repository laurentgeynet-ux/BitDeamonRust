//! # BitDaemon
//!
//! A BitTorrent client library ported from
//! [libtorrent](https://github.com/arvidn/libtorrent) (Rasterbar, C++)
//! to safe, idiomatic Rust.
//!
//! This facade crate re-exports the public API of the workspace crates.
//! During early development, prefer depending on the granular crates
//! (`bitdaemon-session`, `bitdaemon-core`, …) — the facade surface will
//! stabilize at 1.0.
//!
//! Upstream reference: libtorrent `master` @ `v2.1.2-45-gc019e80ef`.
//! Port status: see `docs/PORTING.md`. Design decisions: `docs/adr/`.

#![forbid(unsafe_code)]

pub use bitdaemon_bencode as bencode;
pub use bitdaemon_common as common;
pub use bitdaemon_core as core;
pub use bitdaemon_dht as dht;
pub use bitdaemon_disk as disk;
pub use bitdaemon_net as net;
pub use bitdaemon_session as session;

/// `Engine` — the async `session_impl` driver over injected transports.
pub mod engine;
