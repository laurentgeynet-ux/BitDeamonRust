# GUI parity matrix — what a full client can build on BitDaemon

Acceptance doc for the "GUI complet" goal (ROADMAP Phase M). Every
row is a feature a qBittorrent-class client ships, mapped to the
BitDaemon surface that implements it. A cell marked ✅ exists and is
observable end-to-end; ⚠️ exists with noted limits; — is out of
scope upstream too.

The two pillars a GUI consumes are:

- **`SessionHandle` / `TorrentHandle`** — synchronous commands +
  snapshots (~220 methods, mirrors `session_handle`/`torrent_handle`).
- **`Alert` stream** — every async result upstream delivers, via
  `SessionHandle::pop_alerts` / `set_alert_notify` /
  `wait_for_alert` + `alert_mask` filtering (default upstream
  `error`; a GUI typically opts into the categories it renders).

## Torrent lifecycle

| GUI feature | BitDaemon surface | Status |
|---|---|---|
| Add magnet / .torrent / atp | `SessionHandle::add_torrent`, `add_torrent_params`, `AddTorrentParams` (magnet URI, `ti`, save_path, flags, trackerid, renamed files) | ✅ |
| Add-result feedback | `Alert::AddTorrent` (params + error), `Alert::TorrentConflict` (duplicate info-hash), `Alert::FastresumeRejected` | ✅ |
| Remove (keep/delete files) | `SessionHandle::remove_torrent`; `Alert::TorrentDeleted` / `TorrentDeleteFailed` | ✅ |
| Pause / resume (global + per-torrent) | `SessionHandle::{pause,resume,is_paused}`, `TorrentHandle::{pause,resume,is_paused}`, `set_paused_graceful`; `Alert::{TorrentPaused,TorrentResumed}` | ✅ |
| Recheck | `TorrentHandle::force_recheck`; `Alert::TorrentChecked` / `CheckIntegrity` progress | ✅ |
| Reannounce / scrape | `TorrentHandle::force_reannounce`, `scrape_tracker`; `Alert::{TrackerAnnounce,TrackerReply,ScrapeReply,ScrapeFailed}` | ✅ |
| Move storage | `TorrentHandle::move_storage`; `Alert::{StorageMoved,StorageMovedFailed}` | ✅ |
| Rename file | `TorrentHandle::rename_file`; `Alert::{FileRenamed,FileRenameFailed}` | ✅ |
| Delete progress | disk `delete_files` job → `Alert::TorrentDeleteFailed` on error | ✅ |
| Metadata resolve (magnet) | ut_metadata pieces; `Alert::{MetadataReceived,MetadataFailed}` (hash-mismatch retry / parse-fail → `TorrentError` + pause) | ✅ |
| Save path display | `TorrentStatus::save_path` via `post_status`/`post_torrent_updates` | ✅ |

## Torrent list & per-torrent detail

| GUI feature | BitDaemon surface | Status |
|---|---|---|
| Torrent table (state, progress, speed, size, ETA, ratio) | `TorrentStatus` snapshot — `TorrentHandle::status`, `SessionHandle::post_torrent_updates` → `Alert::StateUpdate` | ✅ |
| State column | `TorrentStatus::state` (upstream `torrent_status::state_t`) + `Alert::StateChanged` transitions | ✅ |
| Error column | `Alert::TorrentError`, `TorrentStatus::error`; per-file `Alert::{FileError,FileCompleted}` | ✅ |
| Piece-priority grid | `TorrentHandle::{piece_priority, piece_priorities, prioritize_pieces}`; `Alert::PieceInfo` on request | ✅ |
| File list + per-file priority/progress | `TorrentHandle::{file_priorities, file_progress, prioritize_files, file_priority}`; `Alert::{FilePrio,FilePriorities,FileProgress}` (post_* replies) | ✅ |
| Files-completed badge | `Alert::FileCompleted` per file | ✅ |
| Piece availability map | `TorrentHandle::post_piece_availability` → `Alert::PieceAvailability` | ✅ |
| Trackers tab (add/edit/remove/reannounce) | `TorrentHandle::{trackers, add_tracker, remove_tracker, replace_trackers, force_reannounce}`; `Alert::{TrackerList,TrackerAnnounce,TrackerReply,TrackerWarning,Trackerid}` | ✅ |
| Web seeds tab | `TorrentHandle::{url_seeds, add_url_seed, remove_url_seed}`; `Alert::UrlSeed` | ✅ |
| Peers tab (ip, client, flags, speeds, progress) | `TorrentHandle::post_peer_info` → `Alert::PeerInfo` (full `peer_info` snapshot); `Alert::{PeerConnect,PeerDisconnected,PeerBlocked,PeerError}` | ✅ |
| Pieces progress bars | `TorrentStatus` progress/`num_pieces`; `Alert::PieceFinished` | ✅ |
| Speed graphs (down/up, session + per-torrent) | `SessionHandle::post_session_stats` → `Alert::SessionStats` (category-0, unconditional) + `SessionStatsHeader` (once); per-torrent `Alert::Stats`; `Alert::DhtStats` for DHT graphs | ✅ |
| Upload/download limits | `SessionSettings` rate limits + `TorrentHandle::{set_upload_limit, set_download_limit}`, per-peer `set_peer_*_limit` | ✅ |
| Torrent properties (flags, seeds/peers counts, share ratio) | `TorrentStatus` + `TorrentHandle` flags setters (sequential, upload-mode, super-seeding, share-mode, auto-managed) | ✅ |
| Queue positions (top/bottom/up/down) | `TorrentHandle::{queue_position, queue_position_up, queue_position_down, queue_position_top, queue_position_bottom}`; `Alert::TorrentMoved` | ✅ |
| Deadlines | `TorrentHandle::{reset_piece_deadline, set_piece_deadline, clear_piece_deadlines}`; `Alert::ReadPiece`/`ReadPieceFailed` for preview fetches | ✅ |
| Sequential / stream preview | `TorrentHandle::{set_sequential_download, set_sequential_range}`, `post_status` | ✅ |

## Session-level panels

| GUI feature | BitDaemon surface | Status |
|---|---|---|
| Status bar (DHT on, listen port, external IP, conn count) | `SessionHandle::{is_dht_running, is_listening, num_connections, num_uploads}`; `Alert::{ListenSucceeded,ListenFailed,ExternalIp}` | ✅ |
| DHT controls (nodes, lookups) | `SessionHandle::{dht_get_peers, dht_announce, dht_get_item(+mut), dht_put_item(+mut), dht_sample_infohashes, dht_live_nodes, dht_direct_request, post_dht_stats}` → `Alert::{DhtGetPeersReply,DhtImmutableItem,DhtMutableItem,DhtPut,DhtAnnounce,DhtDirectResponse,DhtBootstrap,DhtLiveNodes,DhtError,DhtStats,DhtPacket,DhtLog}` | ✅ |
| Port mapping status | `SessionHandle::{add_port_mapping, delete_port_mapping}` → `Alert::{Portmap,PortmapError,PortmapLog}` | ✅ |
| Listen interface config | `SessionHandle::{listen_on, use_interfaces, ssl_listen_port, reopen_network_sockets}`; `Alert::{ListenSucceeded,ListenFailed}` | ✅ |
| IP filter / ban list | `SessionHandle::{set_ip_filter, get_ip_filter, set_peer_class_filter, set_peer_class_type_filter}`; `Alert::{PeerBlocked,IpBan}` | ✅ |
| Proxy config | `SessionHandle::{set_proxy, set_peer_proxy, set_tracker_proxy, set_dht_proxy, set_web_seed_proxy, set_lsd_proxy, set_alert_proxy, set_ssl_proxy, set_i2p_proxy}` | ✅ |
| Peer classes | `SessionHandle::{create_peer_class, delete_peer_class}` + filters | ✅ |
| Resume/session persistence | `SessionHandle::{save_state_buf, load_state, write_session_params, session_state}`, `TorrentHandle::write_resume_data`/`get_resume_data`; `Alert::{SaveResumeData,SaveResumeDataFailed,SessionError,SessionStatsHeader}` | ✅ |
| Connection/type counters | `TorrentStatus`/`SessionStats` counters (`session_stats_metrics`) | ✅ |
| Log view | `Alert::{Log,TorrentLog,PeerLog,PickerLog,DhtLog,DhtPacket,I2p,Socks5,UdpError,LsdError,PortmapLog}` gated via `alert_mask` | ✅ |

## Alert coverage per upstream category

`AlertCategory` = upstream `alert_category_t` verbatim (bits 0–24).
A GUI sets `alert_mask` to the categories it renders:

| Category | Representative alerts | Typical widget |
|---|---|---|
| `ERROR` (default) | `TorrentError`, `SessionError`, `TorrentConflict`, `FileError` | error banner/log |
| `STATUS` | `StateChanged`, `TorrentPaused`/`Resumed`, `AddTorrent`, `TorrentFinished` | torrent list refresh |
| `STATS` | `SessionStats`, `SessionStatsHeader`, `Stats` (cat-0 unconditional for session/dht) | graphs |
| `CONNECT`/`PEER_LOG` | `PeerConnect`, `PeerDisconnected`, `PeerLog` | peers tab/log |
| `TRACKER` | `TrackerAnnounce`, `TrackerReply`, `TrackerWarning`, `Trackerid` | trackers tab |
| `DHT`/`DHT_LOG`/`DHT_OPERATION` | `DhtGetPeersReply`, `DhtMutableItem`, `DhtBootstrap`, `DhtLiveNodes`, `DhtPacket`, `DhtLog`, `DhtStats` | DHT panel |
| `PORT_MAPPING`/`PORT_MAPPING_LOG` | `Portmap`, `PortmapError`, `PortmapLog` | network prefs |
| `IP_BLOCK` | `PeerBlocked`, `IpBan` | ban list |
| `UPLOAD`/`BLOCK_LOG` | `BlockUploaded`, `BlockFinished`, `BlockDownloading`, `BlockTimeout`, `UnwantedBlock`, `InvalidRequest`, `IncomingRequest` | block-level view |
| `FILE_PROGRESS` | `FileProgress`, `FileCompleted`, `FilePrio`, `FilePriorities` | files tab |
| `STORAGE` | `StorageMoved`, `StorageMovedFailed`, `FastresumeRejected`, `OversizedFile`, `ReadPiece`, `CacheFlushed` | transfer details |
| `PICKER_LOG` | `PickerLog` | debug log |
| `PERFORMANCE` | `Performance` | status bar |
| `TORRENT_LOG`/`SESSION_LOG` | `TorrentLog`, `Log` | log view |

## Known upstream-parity caveats

- `Alert::RequestDropped` and `Alert::AnonymousMode` exist for
  table parity but are **dead upstream** — libtorrent never posts
  them either. Nothing to render.
- `Alert::IncomingRequest` fires at *accept* time (request queued
  for service), matching upstream `incoming_request` — it is not a
  per-wire-message counter.
- `Alert::ExternalIp` fires only on *elected-address change*, not
  per vote — a status-bar field should latch, not flash.
- `Alert::Stats` (per-torrent) is `STATS`-gated; `SessionStats`/
  `SessionStatsHeader`/`DhtStats` are category-0 and always post.
- `AlertsDropped` carries the dropped `alert_type` ids — surface it
  as a counter, not a list, when the UI floods the queue.
- `TorrentHandle::{add_http_seed,remove_http_seed,http_seeds}` are
  upstream-deprecated no-ops (BEP-17 fetch removed upstream);
  `atp.http_seeds` still merges into the BEP-19 web-seed path.

## Extension points for GUI-side custom behavior

- `SessionPlugin` (session-level): `on_alert` fan-out +
  `torrent_plugin` role on a torrent — `new_peer_connection`,
  `on_piece_pass`/`on_piece_failed`, `on_files_checked`, `on_pause`
  veto, `tick`.
- `PeerPlugin` (per-connection): `on_message`, `on_tick`.
- `TorrentHandle::add_extension(factory)` — factory invoked
  immediately; late adds replay `on_files_checked`.
