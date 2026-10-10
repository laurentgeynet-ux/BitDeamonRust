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

/// `peer_connection` state machine (bitfield, choke, picker drive).
pub mod peer_conn;
/// `peer_info.hpp` — per-peer status snapshot + flag constants.
pub mod peer_info;
/// `peer_connection` request pipeline + `merge_block_requests`.
pub mod peer_pipeline;
/// Client session (`libtorrent::session` / `session_handle`).
///
/// TODO: implement — spawn the actor, `add_torrent`, `pause`/`resume`,
/// `post_torrent_updates`, `pop_alerts`, `apply_settings`, `save_state`
/// / `load_state` (`.session` file, bencoded), `is_listening`, ports
/// mapping, DHT/LSD/UPnP/NAT-PMP toggles.
/// `aux_/piece_picker` — piece availability, priorities, picking.
pub mod piece_picker;
/// `aux_/request_blocks` — `request_a_block` glue picker↔peer.
pub mod request_blocks;

/// `alert`/`alert_manager` — event bus.
pub mod alerts;
/// `choker` — unchoke sort.
pub mod choker;
/// `performance_counters`/`session_stats`.
pub mod counters;
/// `session`/`session_impl` — orchestrator core.
pub mod session;
/// `session_handle`/`torrent_handle` — public API.
pub mod session_handle;
/// `.session` state file.
pub mod session_state;
/// `settings_pack`/`session_params`.
pub mod settings;
/// `extensions/smart_ban` — corrupt-block peer banning.
pub mod smart_ban;
pub mod status;
/// `torrent` — per-torrent state machine.
pub mod torrent;
/// `announce_entry`/`tracker_list` — retry & failover.
pub mod tracker_list;

/// Client session (`libtorrent::session` / `session_handle`).
pub struct Session;

/// Per-torrent handle (`libtorrent::torrent_handle`).
///
/// TODO: implement — cheap `Clone`able handle sending commands into the
/// session actor; mirrors the ~90 methods of `torrent_handle`.
pub struct TorrentHandle;
pub mod ut_metadata;
pub mod ut_pex;
