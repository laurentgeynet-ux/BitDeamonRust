//! Session orchestration — the heart of the port.
//!
//! Ports `session` / `session_impl` / `session_handle` (public API and
//! the single-threaded network thread), `torrent` (per-torrent state
//! machine), `torrent_handle`, `torrent_status`, `alert` /
//! `alert_manager` / `alert_types`, `settings_pack` / `session_settings`
//! / `session_params`, `add_torrent_params`, `choker`,
//! `bandwidth_manager` / `bandwidth_limit` / `bandwidth_queue_entry`,
//! `peer_class` / `peer_class_set`, `piece_picker`, `hash_picker`
//! coordination, `smart_ban`, `stat` / `session_stats` /
//! `performance_counters`, `ip_filter` application, `resolver`
//! scheduling, `proxy_settings`, extensions (`ut_metadata`, `ut_pex`,
//! `smart_ban`, `i2p_pex`), `read_resume_data` / `write_resume_data`
//! integration, `listen_socket_handle`, `session_udp_sockets`.
//!
//! Concurrency model (ADR-0001/0002): C++ `session_impl` runs almost
//! entirely on one io_service thread. The Rust port keeps a single
//! actor task owning all torrent state; public handles communicate via
//! `mpsc` commands and `mpsc`/`watch` alert channels.
//!
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// Client session (`libtorrent::session` / `session_handle`).
///
/// TODO: implement — spawn the actor, `add_torrent`, `pause`/`resume`,
/// `post_torrent_updates`, `pop_alerts`, `apply_settings`, `save_state`
/// / `load_state` (`.session` file, bencoded), `is_listening`, ports
/// mapping, DHT/LSD/UPnP/NAT-PMP toggles.
/// `aux_/piece_picker` — piece availability, priorities, picking.
pub mod piece_picker;

/// Client session (`libtorrent::session` / `session_handle`).
pub struct Session;

/// Per-torrent handle (`libtorrent::torrent_handle`).
///
/// TODO: implement — cheap `Clone`able handle sending commands into the
/// session actor; mirrors the ~90 methods of `torrent_handle`.
pub struct TorrentHandle;
