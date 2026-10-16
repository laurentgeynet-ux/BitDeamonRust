//! Port of `peer_connection.cpp`/`peer_connection.hpp` (state machine
//! half — the wire codec is `net::peer_wire`, the async driver
//! `net::bt_peer_conn`).
//!
//! `peer_connection` assembled as a Rust state machine: the remote's
//! bitfield, choke/interest flags both directions, the request
//! pipeline (`PeerPipeline`), BEP-10 extension ids, suggest pieces,
//! and the score/statistics used by the choker.
//!
//! Sockets and the torrent object stay outside: methods take the
//! `PiecePicker` and produce `PeerMessage`s (from `peer_wire`) for the
//! caller to serialize onto a `PeerStream`. This keeps every
//! wire-visible decision identical to upstream while remaining
//! transport-agnostic — the anonymous OnionBit path plugs in at the
//! stream layer.

use bitdaemon_common::{Bitfield, PeerId, PeerRequest, PieceBlock, PieceIndex};
use bitdaemon_net::peer_wire::PeerMessage;
use sha1::Digest;

use crate::peer_pipeline::{PeerPipeline, PendingBlock};
use crate::piece_picker::{PickerStats, PiecePicker};
use crate::request_blocks::{PeerRequestCtx, TorrentRequestCtx};

/// `peer_connection::upload_channel`/`download_channel` — bandwidth
/// channel indices (match `peer_class::{UPLOAD,DOWNLOAD}`).
pub const UPLOAD_CHANNEL: usize = 0;
/// `peer_connection::download_channel`.
pub const DOWNLOAD_CHANNEL: usize = 1;

/// `peer_connection`'s `m_quota[]` + `m_channel_state[]` — shared
/// between the session (requests, grants, status reads — all under
/// the session lock) and the engine's per-connection socket task
/// (actual byte consumption + waiting on grants). Upstream keeps the
/// counters inside `peer_connection` on the network thread; the tokio
/// split moves them into an `Arc` so the socket task can consume
/// quota without taking the session lock per block.
///
/// Implements [`bitdaemon_net::bandwidth::BandwidthSocket`] — a
/// queued request stores a clone of this `Arc` inside the session's
/// `BandwidthManager`s and `assign_bandwidth` lands under the lock.
#[derive(Debug)]
pub struct PeerQuota {
    /// `m_quota[channel]` — bytes this conn may transfer right now.
    pub quota: [std::sync::atomic::AtomicI64; 2],
    /// `m_channel_state[channel]` — `peer_info::bw_*` bits
    /// (`BandwidthState` values).
    pub channel_state: [std::sync::atomic::AtomicU8; 2],
    /// `stat::upload_ip_overhead`/`download_ip_overhead` — the
    /// socket task accumulates per-packet IP/TCP header bytes here;
    /// the per-second tick drains them into the class channels via
    /// `use_quota_overhead` like upstream's `second_tick`.
    pub ip_overhead: [std::sync::atomic::AtomicI64; 2],
    /// grant dispatch — `assign_bandwidth` notifies the parked task.
    grant: [tokio::sync::Notify; 2],
    /// `is_disconnecting` — queued requests of a dead conn are
    /// dropped and their assigned quota returned on the next
    /// `update_quotas` pass.
    pub disconnecting: std::sync::atomic::AtomicBool,
    /// `m_recv_buffer.pos()` — staged-but-undecoded bytes, for
    /// `peer_info::used_receive_buffer`. Written by the socket task.
    pub recv_pos: std::sync::atomic::AtomicI64,
    /// `m_recv_buffer.capacity()` — the staging buffer's size, for
    /// `peer_info::receive_buffer_size`.
    pub recv_capacity: std::sync::atomic::AtomicI64,
    /// `downloading_piece_progress().bytes_downloaded` — staged
    /// payload bytes of the in-flight `piece` message at the head
    /// of the receive buffer (0 when the head isn't a piece
    /// message). Written by the socket task next to `recv_pos`;
    /// `build_tracker_request` folds it into `downloaded`/`redundant`
    /// on seed torrents.
    pub piece_recv_staged: std::sync::atomic::AtomicI64,
    /// `on_read_counter` — completed socket reads, bumped by the
    /// socket task (upstream incs inside `on_receive_data` under
    /// the session lock; we accumulate per-conn and drain into the
    /// session counters at second_tick/`remove_peer`).
    pub socket_reads: std::sync::atomic::AtomicI64,
    /// `socket_recv_size3..20` — per-read bucket counts, index
    /// `min(log2p1(bytes >> 3), 17)` like `received_buffer`.
    pub recv_size_hist: [std::sync::atomic::AtomicI64; 18],
}

impl PeerQuota {
    /// `m_channel_state` starts at `bw_idle` upstream.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            quota: [
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
            ],
            channel_state: [
                std::sync::atomic::AtomicU8::new(crate::peer_info::BandwidthState::IDLE.0),
                std::sync::atomic::AtomicU8::new(crate::peer_info::BandwidthState::IDLE.0),
            ],
            ip_overhead: [
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
            ],
            grant: [tokio::sync::Notify::new(), tokio::sync::Notify::new()],
            disconnecting: std::sync::atomic::AtomicBool::new(false),
            recv_pos: std::sync::atomic::AtomicI64::new(0),
            recv_capacity: std::sync::atomic::AtomicI64::new(0),
            piece_recv_staged: std::sync::atomic::AtomicI64::new(0),
            socket_reads: std::sync::atomic::AtomicI64::new(0),
            recv_size_hist: [
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
                std::sync::atomic::AtomicI64::new(0),
            ],
        })
    }

    /// `session_impl::received_buffer` + the `on_read_counter` leg
    /// of `on_receive_data` — one completed socket read.
    pub fn record_read(&self, bytes: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        self.socket_reads.fetch_add(1, Relaxed);
        let idx = bitdaemon_common::ffs::log2p1((bytes >> 3) as u32).min(17);
        self.recv_size_hist[usize::try_from(idx).unwrap_or(0)].fetch_add(1, Relaxed);
    }

    /// Move the conn's accumulated read accounting into the session
    /// counters — `second_tick` and `remove_peer` call this.
    pub fn drain_read_counters(&self, counters: &crate::counters::Counters) {
        use std::sync::atomic::Ordering::Relaxed;
        let reads = self.socket_reads.swap(0, Relaxed);
        if reads != 0 {
            counters.add(crate::counters::Counter::OnReadCounter, reads);
        }
        for (i, b) in self.recv_size_hist.iter().enumerate() {
            let n = b.swap(0, Relaxed);
            if n != 0 {
                counters.add_idx(crate::counters::Counter::SocketRecvSize3 as usize + i, n);
            }
        }
    }

    /// Park until `assign_bandwidth` dispatches quota on `channel`
    /// (a `Notify` permit survives pre-registration, so a grant
    /// landing before the wait begins is not lost).
    pub async fn wait_grant(&self, channel: usize) {
        self.grant[channel].notified().await;
    }
}

impl bitdaemon_net::bandwidth::BandwidthSocket for PeerQuota {
    /// `peer_connection::assign_bandwidth` — quota lands, `bw_limit`
    /// clears, the parked socket task wakes (upstream re-arms
    /// `setup_send`/`setup_receive`; ours resumes at the gate).
    fn assign_bandwidth(&self, channel: i32, amount: i64) {
        let c = usize::try_from(channel).unwrap_or(0).min(1);
        self.quota[c].fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
        self.channel_state[c].fetch_and(
            !crate::peer_info::BandwidthState::LIMIT.0,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.grant[c].notify_one();
    }

    fn is_disconnecting(&self) -> bool {
        self.disconnecting
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The direction-independent flags of a peer connection.
#[derive(Debug)]
pub struct PeerConn {
    /// `m_peer_id` — remote's peer_id once the handshake completed.
    pub peer_id: Option<PeerId>,
    /// `m_have_piece` — pieces the remote has (have-bitfield).
    pub peer_pieces: Bitfield,
    /// `m_interesting` — they have something we want.
    pub interesting: bool,
    /// `m_need_interest_update` — a have-family event dirtied the
    /// interest evaluation; `Torrent::do_update_interest` drains it
    /// once per message batch (upstream posts a deferred callback).
    pub need_interest_update: bool,
    /// `m_choked` — we are choking their download.
    pub choked: bool,
    /// `m_peer_choked` — they are choking our download.
    pub peer_choked: bool,
    /// `m_peer_interested` — they want pieces from us.
    pub peer_interested: bool,
    /// `m_bitfield_received` — at least one bitfield message seen.
    pub bitfield_received: bool,
    /// `m_allowed_fast` — BEP-6 pieces we may fetch while choked.
    pub allowed_fast: Vec<PieceIndex>,
    /// `m_accept_fast` — pieces WE offered this peer while it was
    /// choked (the `send_allowed_set` set).
    pub accept_fast: Vec<PieceIndex>,
    /// `m_accept_fast_piece_cnt` — per-entry completion counters
    /// upstream keeps for accept-fast accounting.
    pub accept_fast_piece_cnt: Vec<i32>,
    /// `m_suggested_pieces` — BEP-6 suggested pieces.
    pub suggested_pieces: Vec<PieceIndex>,
    /// `m_suggest_pieces` — pieces WE suggested to this peer
    /// (upstream keeps them on the conn; cleared on choke).
    pub suggest_sent: Vec<PieceIndex>,
    /// `m_request_queue`/`m_download_queue` + pipeline state.
    pub pipeline: PeerPipeline,
    /// `m_upload_only_id` etc. — BEP-10 remote extension ids.
    pub ext_upload_only: u8,
    /// `ut_holepunch` id.
    pub ext_holepunch: u8,
    /// `lt_donthave` id.
    pub ext_dont_have: u8,
    /// `ut_metadata` id (BEP-9) — the id the REMOTE expects when we
    /// send ut_metadata messages.
    pub ext_ut_metadata: u8,
    /// `ut_pex` id (BEP-11).
    pub ext_ut_pex: u8,
    /// `metadata_size` the remote advertised (its info dict size).
    pub metadata_size: Option<i64>,
    /// `m_sent_requests` — `ut_metadata` pieces we have asked this
    /// peer for and not yet gotten a reply to (upstream caps the
    /// list at 2 outstanding; a `piece` or `dont_have` reply erases
    /// its entry).
    pub metadata_sent_requests: Vec<i32>,
    /// `m_request_limit` — don't ask this peer for metadata before
    /// this instant unless it advertised `metadata_size` (upstream
    /// `has_metadata()` = advertised || limit expired). `dont_have`
    /// pushes it +1 min; a failed hash check pushes it +20–70s
    /// (with a +5 min base when the metadata is a single piece).
    /// `None` = `min_time()` (always expired).
    pub metadata_request_limit: Option<std::time::Instant>,
    /// `m_incoming_requests` — `ut_metadata` pieces this peer asked
    /// for while our send buffer was at/above the 160 KiB serve
    /// limit (upstream caps it at 1024; overflow → `dont_have`).
    pub metadata_incoming_requests: std::collections::VecDeque<i32>,
    /// `m_sent_allowed_fast` — the allowed-fast set goes out once,
    /// on the peer's first `Interested` (upstream defers it to save
    /// bandwidth and to skip pieces the peer already has).
    pub sent_allowed_fast: bool,
    /// `m_holepunch_mode` — set by `set_holepunch_mode` when a
    /// `ut_holepunch` `connect` targets an already-connected peer:
    /// the conn retries faster and sticks to uTP.
    pub holepunch_mode: bool,
    /// `m_has_metadata` — upstream ctor `true`; the ext handshake
    /// clears it when `ut_metadata` is advertised without
    /// `metadata_size` (the peer is still fetching the info dict);
    /// any piece-level message (`have`/`have_none`/`dont_have`)
    /// proves the peer has metadata and sets it back.
    pub has_metadata: bool,
    /// OUR local ext ids — the ids we advertise in our own BEP-10
    /// handshake; inbound messages carry these (the remote's ids
    /// above are for our sends). The values are upstream's fixed
    /// constants (`bt_peer_connection.hpp`): `ut_pex`=1,
    /// `ut_metadata`=2, `upload_only`=3, `ut_holepunch`=4,
    /// `lt_donthave`=7, `share_mode`=8, `i2p_pex`=9.
    pub local_ut_pex: u8,
    /// our local `ut_metadata` id.
    pub local_ut_metadata: u8,
    /// our local `upload_only` id — remote sends a 1-byte flag.
    pub local_upload_only: u8,
    /// our local `ut_holepunch` id (BEP-55).
    pub local_ut_holepunch: u8,
    /// our local `lt_donthave` id — remote sends extended messages
    /// on this id to retract a piece.
    pub local_dont_have: u8,
    /// our local `share_mode` id — remote sends a 1-byte flag.
    pub local_share_mode: u8,
    /// `i2p_pex` — remote's message id from its handshake (`m`).
    pub ext_i2p_pex: u8,
    /// `local_i2p_pex` — our advertised id (upstream fixes it at 9).
    pub local_i2p_pex: u8,
    /// I2P destination (base64 string) when this conn rides an I2P
    /// socket — `is_i2p(pc.get_socket())` upstream.
    pub i2p: Option<Vec<u8>>,
    /// `is_outgoing()` — we dialed this peer.
    pub outgoing: bool,
    /// `is_connecting()` — handshake not yet complete.
    pub connecting: bool,
    /// `m_disconnecting` — a disconnect has been issued; upstream
    /// keeps the object alive until the send buffer drains and gates
    /// the request pipeline on it. Our actor tears the conn down the
    /// same tick, so this only flips in tests / drain paths.
    pub disconnecting: bool,
    /// `on_parole` — pieces from this peer get piece-at-a-time hash
    /// checks; a parole peer keeps its request queue on choke and is
    /// never snubbed (upstream `peer_connection::on_parole`).
    pub on_parole: bool,
    /// `!in_handshake()` — the peer's BitTorrent handshake landed.
    pub handshake_done: bool,
    /// `received_listen_port` — peer shared its listen port.
    pub received_listen_port: bool,
    /// the listen port the peer advertised via ext-handshake `p`
    /// (upstream `peer_info::port`) — ut_pex rewrites inbound
    /// endpoints to this port, the source port being ephemeral.
    pub peer_listen_port: Option<u16>,
    /// `i2p_pex_peer_plugin` per-connection state.
    pub i2p_pex: crate::i2p_pex::I2pPexPeerState,
    /// `m_superseed_piece[2]` — the piece(s) we're currently
    /// super-seeding to this peer (BEP initial-seed); requests for
    /// either slot are honored.
    pub superseed_pieces: [Option<PieceIndex>; 2],
    /// `m_became_uninterested` — conn-clock ms of the last
    /// interested → not-interested transition; upstream's
    /// `relative_time` default is the connect instant, so a
    /// never-interested peer still ages into the super-seed
    /// recovery window.
    pub became_uninterested_ms: i64,
    /// `m_became_uninteresting` — conn-clock ms of the last
    /// `NotInterested` we sent (connect-time default, like
    /// upstream). Feeds `timed_out_no_interest`.
    pub became_uninteresting_ms: i64,
    /// `m_connect` — conn-clock ms at connect; the epoch the
    /// handshake timeout measures against.
    pub connect_ms: i64,
    /// `m_last_sent` — conn-clock ms of the last message we wrote
    /// (any kind); drives `keep_alive` (`peer_timeout / 2`).
    pub last_sent_ms: i64,
    /// `m_last_incoming_request` — conn-clock ms of the last
    /// `Request` the peer sent us.
    pub last_incoming_request_ms: i64,
    /// `m_last_sent_payload` — conn-clock ms of the last piece
    /// payload we sent.
    pub last_sent_payload_ms: i64,
    /// `m_last_unchoke` (ms) — conn-clock of our last unchoke of
    /// this peer; part of the `timed_out_no_request` max.
    pub last_unchoke_ms: i64,
    /// `m_last_unchoked` (ms) — conn-clock of the last time the peer
    /// unchoked *us* (`download_queue_time` rate leg).
    pub last_unchoked_ms: i64,
    /// `m_last_choke` (ms) — conn-clock of the last `Choke` we
    /// sent. Requests from a still-choked peer stop being tolerated
    /// 2 s after this stamp (upstream disconnects them).
    pub last_choke_ms: i64,
    /// `m_last_receive` (ms) — conn-clock of last inbound data
    /// (complements `last_active`, which is session seconds).
    pub last_receive_ms: i64,
    /// `peer_info::protocol_v2` — the connection was established
    /// with the v2 info-hash. Hash messages (`hash_request`/
    /// `hashes`/`hash_reject`) are a protocol violation without it.
    pub protocol_v2: bool,
    /// `m_upload_only` — remote told us it's upload-only.
    pub remote_upload_only: bool,
    /// `share_mode` handshake bit — the remote runs share-mode and
    /// doesn't count as a downloader in `recalc_share_mode`.
    pub remote_share_mode: bool,
    /// `m_client_version` — remote's `v` string.
    pub client_version: Vec<u8>,
    /// `m_upload_queue` — peer's requests to us.
    pub upload_queue: Vec<PeerRequest>,
    /// `m_send_buffer.size()` — bytes queued for the socket that the
    /// writer pump hasn't sent yet. Drives `fill_send_buffer`'s
    /// watermark gate (`send_buffer + reading_bytes < watermark`).
    pub send_buffer: usize,
    /// `m_reading_bytes` — payload bytes of disk reads in flight for
    /// this peer (issued, completion not yet drained).
    pub reading_bytes: usize,
    /// `m_outstanding_writing_bytes` — payload bytes of blocks this
    /// peer sent us that are queued for a disk write, not yet
    /// confirmed landed. Drives the per-peer `bw_disk` read gate
    /// (`peer_conn::PeerWriteGates`): unlike the global disk-queue
    /// watermark, a peer is only held back by *its own* writes.
    pub outstanding_writing_bytes: i64,
    /// `m_channel_state[download_channel] & peer_info::bw_disk` —
    /// this conn is counted in `num_peers_down_disk` while its own
    /// write queue keeps the read gate closed.
    pub disk_blocked: bool,
    /// The `disk_observer` upstream passes as `self()` to every
    /// `async_write`: when the global write queue drains below the
    /// low watermark the observer releases this conn's `bw_disk`
    /// gate — the second release path alongside the own-write
    /// completion (which can lag by a full `cache_expiry` interval).
    pub disk_observer: Option<std::sync::Arc<dyn bitdaemon_disk::back_pressure::DiskObserver>>,
    /// `m_channel_state[upload_channel] & peer_info::bw_disk` —
    /// counted in `num_peers_up_disk` while the send buffer is
    /// drained with disk reads still in flight.
    pub up_disk_blocked: bool,
    /// `m_failed` — set when the disconnect ran at
    /// `disconnect_severity_t > normal` (`failure`/`peer_error`);
    /// `connection_closed` counts it against `failcount`.
    pub failed: bool,
    /// `m_num_invalid_requests`.
    pub num_invalid_requests: i32,
    /// `m_disk_read_failures` — consecutive failed disk reads serving
    /// this peer; upstream disconnects at >100 and resets on success.
    pub disk_read_failures: u32,
    /// `m_outstanding_piece_verification` — in-flight seed-mode lazy
    /// hash checks; upstream caps a peer at 3.
    pub outstanding_piece_verification: i32,
    /// `m_hash_requests` — v2 `hash_request`s we sent and still await
    /// `hashes`/`hash_reject` for; upstream caps the set at 2 and
    /// ignores replies it never asked for.
    pub hash_requests: Vec<bitdaemon_net::peer_wire::HashRequestWire>,
    /// `m_num_pieces` — total pieces the peer announced.
    pub num_pieces: i32,
    /// `m_last_seen_complete` — epoch seconds the peer reported via
    /// BEP-21 `complete_ago` (0 = never/didn't say). Feeds
    /// `torrent`'s `m_swarm_last_seen_complete`.
    pub last_seen_complete: i64,
    /// `stat::upload_rate()` — all-channel EMA B/s (payload +
    /// protocol + ip-overhead); `peer_info.up_speed`.
    pub rate_up_speed: i32,
    /// `stat::download_rate()`.
    pub rate_down_speed: i32,
    /// `stat::upload_payload_rate()` — payload-channel EMA only;
    /// `peer_info.payload_up_speed`, the choker's sort key and the
    /// rate peaks.
    pub rate_up_payload: i32,
    /// `stat::download_payload_rate()`.
    pub rate_down_payload: i32,
    /// last sampled `stat_up_total` for payload-rate computation.
    pub last_up_total: i64,
    /// `m_uploaded_last_second` — payload bytes uploaded during the
    /// last `second_tick` interval (`stat::last_payload_uploaded()`);
    /// the instantaneous quantity `fill_send_buffer`'s dynamic
    /// watermark scales with — *not* the EMA `rate_up_speed`.
    pub uploaded_last_second: i64,
    /// last sampled `stat_down_total` for rate computation.
    pub last_down_total: i64,
    /// last sampled all-channel totals (`stat_*_total +
    /// stat_*_protocol + stat_*_ip`) driving `rate_*_speed`.
    pub last_up_wire: i64,
    /// last sampled all-channel download total.
    pub last_down_wire: i64,
    /// `m_download_rate_peak` — best payload download rate ever
    /// measured on this conn (`peer_info` exposes it).
    pub download_rate_peak: i32,
    /// `m_upload_rate_peak`.
    pub upload_rate_peak: i32,
    /// `m_supports_fast` — remote negotiated the FAST extension
    /// (reserved byte 7 bit 0x04). Rejects/allowed-fast only exist
    /// when set.
    pub supports_fast: bool,
    /// `m_supports_dht_port` — remote advertised a DHT port bit
    /// (handshake) *or* sent us a `DhtPort` message (upstream flips it
    /// in `on_dht_port`).
    pub supports_dht: bool,
    /// `m_sent_bitfield` — our post-handshake `write_bitfield` ran
    /// (set for *every* upstream branch, including the no-send
    /// have-none/super-seed legs — the flag means "handshake init
    /// done with metadata", not "bitfield bytes on the wire"). Gates
    /// the `DhtPort` reply in `on_dht_port`.
    pub sent_bitfield: bool,
    /// Our own DHT listen port (`m_ses.dht_port()`) — needed to build
    /// the `DhtPort` reply when a remote's first announcement arrives
    /// after our bitfield went out.
    pub local_dht_port: Option<u16>,
    /// `supports_encryption()` — the connection went through the
    /// MSE/PE handshake (`m_encrypted`), regardless of the negotiated
    /// `crypto_select` (plaintext-select still counts). Feeds the
    /// `pex_encryption` flag in ut_pex.
    pub supports_encryption: bool,
    /// `m_rc4_encrypted` — PE4 selected RC4 (`crypto_select == rc4`).
    /// Drives `peer_info::rc4_encrypted` vs `plaintext_encrypted`
    /// (upstream reports one of them whenever `m_encrypted`) and the
    /// `error_rc4_peers` disconnect counter.
    pub rc4_encrypted: bool,
    /// `HaveAll`/`Have` semantics arrived while the torrent had no
    /// metadata (magnet): the peer claims every piece but
    /// `peer_pieces` could not represent it yet — replayed into a
    /// full bitfield when `num_pieces` becomes known.
    pub pre_metadata_all: bool,
    /// `m_interested` — WE sent `interested` (upstream tracks the
    /// wire state to avoid duplicate sends).
    pub we_sent_interested: bool,
    /// remote endpoint (`m_remote`) — needed for `ut_pex` diffs.
    pub remote: Option<std::net::SocketAddr>,
    /// `local_endpoint()` — our socket's local endpoint; `None`
    /// when the transport doesn't report one. Drives the
    /// `peer_list::new_connection` self-connection legs
    /// (`i->connection->remote() == c.local_endpoint()`).
    pub local: Option<std::net::SocketAddr>,
    /// `m_our_pid` — the peer-id WE sent in *this* conn's handshake.
    /// Upstream generates one per connection (`generate_peer_id`
    /// at `create_peer_connection`) and tracks outgoing ones in
    /// `m_outgoing_pids` for `is_self_connection`.
    pub our_peer_id: Option<PeerId>,
    /// `m_fast_reconnect` — this conn is an MSE/plaintext retry
    /// (`fast_reconnect(true)` at the `pe_enabled` toggle leg):
    /// `connection_closed` keeps the dial-time `last_connected`
    /// stamp so the fallback isn't cooled out.
    pub fast_reconnect: bool,
    /// pex update bookkeeping — the set we last sent this peer.
    pub pex_sent: std::collections::BTreeSet<std::net::SocketAddr>,
    /// `m_peers`/`m_peers6` — endpoints this peer has told us about
    /// and not yet dropped (upstream dedups `added` against this and
    /// erases on `dropped`). Upstream bounds each family separately
    /// at `max_pex_peers`.
    pub pex_known: std::collections::BTreeSet<std::net::SocketAddr>,
    /// `m_peers6` — the IPv6 half of `pex_known`.
    pub pex_known6: std::collections::BTreeSet<std::net::SocketAddr>,
    /// `m_last_pex[6]` — receive timestamps of the last ≤6 pex
    /// messages from this peer; a 7th inside 60s is a flood and
    /// upstream disconnects (`too_frequent_pex`).
    pub pex_rx_times: std::collections::VecDeque<std::time::Instant>,
    /// `m_first_time` — this peer's first pex tick sends the full
    /// peer list; later ticks send the diff (skipped when empty).
    pub pex_first_time: bool,
    /// `m_last_msg` (peer plugin) — when this conn last sent a ut_pex
    /// update. `None` is upstream's `min_time()` init: the first
    /// plugin tick (~1 s after attach) sends the full list once
    /// `num_peers() > 1`, instead of waiting out a torrent-global
    /// 60 s cadence.
    pub pex_last_msg: Option<std::time::Instant>,
    /// transport is uTP — feeds the `pex_utp` flag on advertise.
    pub is_utp: bool,
    /// `socket_type_idx(m_socket)` — the conn's `socket_type_t`
    /// index (tcp 0, socks5 1, http 2, utp 3, i2p 4, rtc 5,
    /// tcp_ssl 6, socks5_ssl 7, http_ssl 8, utp_ssl 9); the
    /// transports flatten into `peer_class_type_filter` classes via
    /// `set_peer_classes`.
    pub sock_type: u8,
    /// inbound SSL conns: the info-hash the ClientHello SNI resolved
    /// to — the TLS ctx owner (`ssl::has_context` leg of
    /// `torrent::attach_peer`: a conn pinned to a different torrent
    /// is `invalid_ssl_cert`).
    pub ssl_sni: Option<bitdaemon_common::Sha1Hash>,
    /// `m_peer_class` — the conn's `peer_class_set`, assigned once at
    /// attach from `peer_class_filter` + `peer_class_type_filter`
    /// (`session_impl::set_peer_classes`). Refcounted in the session
    /// pool on add/drop.
    pub peer_classes: bitdaemon_net::peer_class::PeerClassSet,

    /// `m_exceeded_limit` — admitted past the session
    /// `connections_limit` via `connections_slack`; once the
    /// handshake lands it must evict the disconnect-candidate
    /// torrent's lowest-ranking conn or drop
    /// (`peer_exceeds_limit`/`peer_disconnected_other`).
    pub exceeded_limit: bool,
    /// `m_stat` payload accounting for the choker — lifetime bytes
    /// and bytes since the last unchoke round.
    pub stat_up_total: i64,
    /// download payload lifetime bytes.
    pub stat_down_total: i64,
    /// `m_stat[upload_protocol]` — lifetime BT message bytes sent
    /// that aren't piece payload (headers, bitfield, ext msgs…).
    pub stat_up_protocol: i64,
    /// `m_stat[download_protocol]`.
    pub stat_down_protocol: i64,
    /// `m_stat[upload_ip_protocol]` — estimated IP+TCP overhead
    /// (`trancieve_ip_packet`: a packet's header *and* its ACK —
    /// every event lands on both directions' channels).
    pub stat_up_ip: i64,
    /// `m_stat[download_ip_protocol]`.
    pub stat_down_ip: i64,
    /// upload payload bytes since the last unchoke round.
    pub stat_up_round: i64,
    /// download payload bytes since the last unchoke round.
    pub stat_down_round: i64,
    /// download bytes at the previous tick (drives the upstream
    /// slow-start exit: stalled growth ⇒ AIMD takes over).
    pub down_delta_prev: i64,
    /// `time_of_last_unchoke` (round-robin quota bookkeeping).
    pub last_unchoke: std::time::Instant,
    /// `m_uploaded_at_last_unchoke` — payload total at the last
    /// unchoke transition; `uploaded_since_unchoked` = `stat_up_total`
    /// minus this.
    pub uploaded_at_last_unchoke: i64,
    /// monotonic ms clock refreshed each `second_tick` — drives the
    /// pipeline's RTT samples and request-timeout bookkeeping.
    pub now_ms: i64,
    /// `peer_connection::m_extensions` — per-connection plugins
    /// produced by `SessionPlugin::new_peer_connection`.
    pub plugins: Vec<std::sync::Arc<dyn crate::alerts::PeerPlugin>>,
    /// `m_quota[]`/`m_channel_state[]` — bandwidth state shared with
    /// the socket task (`BandwidthSocket` impl for the manager's
    /// queued requests).
    pub bw: std::sync::Arc<PeerQuota>,
}

impl PeerConn {
    /// New connection for a torrent of `num_pieces` pieces.
    #[must_use]
    pub fn new(num_pieces: usize, mut pipeline: PeerPipeline, now_ms: i64) -> Self {
        // upstream `m_last_piece` defaults to the connect time —
        // `piece_timeout` measures progress *since connect*, not
        // since epoch, or a fresh conn would look stalled.
        pipeline.last_piece_ms = now_ms;
        Self {
            peer_id: None,
            peer_pieces: Bitfield::with_size(num_pieces),
            interesting: false,
            need_interest_update: false,
            choked: true,
            peer_choked: true,
            peer_interested: false,
            bitfield_received: false,
            allowed_fast: Vec::new(),
            accept_fast: Vec::new(),
            accept_fast_piece_cnt: Vec::new(),
            suggested_pieces: Vec::new(),
            suggest_sent: Vec::new(),
            pipeline,
            ext_upload_only: 0,
            ext_holepunch: 0,
            ext_dont_have: 0,
            ext_ut_metadata: 0,
            ext_ut_pex: 0,
            metadata_size: None,
            metadata_sent_requests: Vec::new(),
            metadata_request_limit: None,
            metadata_incoming_requests: std::collections::VecDeque::new(),
            sent_allowed_fast: false,
            holepunch_mode: false,
            has_metadata: true,
            local_ut_pex: 1,
            local_ut_metadata: 2,
            local_upload_only: 3,
            local_ut_holepunch: 4,
            superseed_pieces: [None, None],
            became_uninterested_ms: now_ms,
            became_uninteresting_ms: now_ms,
            connect_ms: now_ms,
            last_sent_ms: now_ms,
            last_incoming_request_ms: now_ms,
            last_sent_payload_ms: now_ms,
            last_unchoke_ms: now_ms,
            last_unchoked_ms: now_ms,
            last_choke_ms: now_ms,
            last_receive_ms: now_ms,
            remote_upload_only: false,
            remote_share_mode: false,
            client_version: Vec::new(),
            upload_queue: Vec::new(),
            send_buffer: 0,
            reading_bytes: 0,
            outstanding_writing_bytes: 0,
            disk_blocked: false,
            disk_observer: None,
            up_disk_blocked: false,
            failed: false,
            num_invalid_requests: 0,
            outstanding_piece_verification: 0,
            hash_requests: Vec::new(),
            disk_read_failures: 0,
            num_pieces: 0,
            last_seen_complete: 0,

            remote: None,
            local: None,
            our_peer_id: None,
            fast_reconnect: false,
            pex_sent: std::collections::BTreeSet::new(),
            pex_known: std::collections::BTreeSet::new(),
            pex_known6: std::collections::BTreeSet::new(),
            pex_rx_times: std::collections::VecDeque::new(),
            pex_first_time: true,
            pex_last_msg: None,
            is_utp: false,
            sock_type: 0,
            ssl_sni: None,
            peer_classes: bitdaemon_net::peer_class::PeerClassSet::default(),

            exceeded_limit: false,
            stat_up_total: 0,
            stat_down_total: 0,
            stat_up_protocol: 0,
            stat_down_protocol: 0,
            stat_up_ip: 0,
            stat_down_ip: 0,
            rate_up_speed: 0,
            rate_down_speed: 0,
            rate_up_payload: 0,
            rate_down_payload: 0,
            download_rate_peak: 0,
            upload_rate_peak: 0,
            last_up_total: 0,
            uploaded_last_second: 0,
            last_down_total: 0,
            last_up_wire: 0,
            last_down_wire: 0,
            stat_up_round: 0,
            stat_down_round: 0,
            down_delta_prev: -1,
            supports_fast: false,
            pre_metadata_all: false,
            supports_dht: false,
            sent_bitfield: false,
            local_dht_port: None,
            supports_encryption: false,
            rc4_encrypted: false,
            local_dont_have: 7,
            local_share_mode: 8,
            ext_i2p_pex: 0,
            local_i2p_pex: crate::i2p_pex::I2P_PEX_ID,
            i2p: None,
            outgoing: false,
            connecting: true,
            disconnecting: false,
            on_parole: false,
            handshake_done: false,
            received_listen_port: false,
            peer_listen_port: None,
            i2p_pex: crate::i2p_pex::I2pPexPeerState::new(),
            protocol_v2: false,
            we_sent_interested: false,
            last_unchoke: std::time::Instant::now(),
            uploaded_at_last_unchoke: 0,
            now_ms,
            plugins: Vec::new(),
            bw: PeerQuota::new(),
        }
    }

    /// `peer_connection::has_piece(index)`.
    #[must_use]
    pub fn has_piece(&self, index: PieceIndex) -> bool {
        (index.0 as usize) < self.peer_pieces.size() && self.peer_pieces.get_bit(index.0 as usize)
    }

    /// `incoming_bitfield` — store the peer's have-set. Marks the
    /// interest dirty like upstream's tail `update_interest()`
    /// (`peer_connection.cpp:2317`); the drain re-scans once.
    pub fn on_bitfield(&mut self, bits: &Bitfield) {
        self.peer_pieces.clone_from(bits);
        self.bitfield_received = true;
        self.num_pieces = bits.iter().filter(|&b| b).count() as i32;
        self.need_interest_update = true;
    }

    /// `incoming_have(index)` — update the bitfield; returns whether
    /// the peer became interesting (we lack that piece). A new bit
    /// also marks the interest dirty — the drained rescan stands in
    /// for upstream's eager `peer_is_interesting` leg.
    pub fn on_have(&mut self, index: PieceIndex, have: impl Fn(PieceIndex) -> bool) -> bool {
        if (index.0 as usize) >= self.peer_pieces.size() {
            return false;
        }
        if self.peer_pieces.get_bit(index.0 as usize) {
            return false;
        }
        self.peer_pieces.set_bit(index.0 as usize);
        self.num_pieces += 1;
        self.need_interest_update = true;
        if !have(index) {
            self.interesting = true;
            return true;
        }
        false
    }

    /// `is_seed()` — the peer claims every piece it knows the size
    /// of (`m_num_pieces == m_have_piece.size()` upstream).
    #[must_use]
    pub fn is_seed(&self) -> bool {
        self.peer_pieces.size() > 0 && self.num_pieces as usize == self.peer_pieces.size()
    }

    /// `peer_connection::ignore_unchoke_slots` — the *connection*
    /// legs: an empty class set exempts, otherwise any member class
    /// flagged `ignore_unchoke_slots` does. Upstream also ORs the
    /// *torrent's* class set — callers fold that in via
    /// `Torrent::ignores_unchoke_slots`. Evaluated live against the
    /// pool so `set_peer_class` flag changes apply immediately.
    /// Exempt conns don't count `num_peers_up_unchoked` or occupy
    /// upload slots.
    #[must_use]
    pub fn ignores_unchoke_slots(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> bool {
        if self.peer_classes.num_classes() == 0 {
            return true;
        }
        let mut ignore = false;
        self.peer_classes.for_each(|c| {
            if let Some(pc) = pool.at(c) {
                ignore |= pc.ignore_unchoke_slots;
            }
        });
        ignore
    }

    /// `peer_connection::get_priority(upload_channel)` — the conn
    /// leg: max of `priority[UPLOAD]` over the conn's class set,
    /// base 1. The torrent leg lives on `Torrent::upload_priority`;
    /// upstream takes the max of both.
    #[must_use]
    pub fn upload_priority(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> i32 {
        let mut prio = 1;
        self.peer_classes.for_each(|c| {
            if let Some(pc) = pool.at(c) {
                prio = prio.max(pc.priority[bitdaemon_net::peer_class::UPLOAD]);
            }
        });
        prio
    }

    /// `incoming_have_none` — the peer sent no bitfield (means
    /// empty). Upstream also drops its refcount share
    /// (`peer_lost(m_have_piece)`) and unflags the seed — the caller
    /// emits those effects from the old bitfield.
    pub fn on_have_none(&mut self) {
        self.bitfield_received = true;
        self.peer_pieces.clear_all();
        self.num_pieces = 0;
        // upstream answers with the eager `send_not_interested`
        // leg — the drain emits it after re-evaluation.
        self.need_interest_update = true;
    }

    /// `incoming_have_all` — a seed.
    pub fn on_have_all(&mut self) {
        self.peer_pieces.set_all();
        self.num_pieces = self.peer_pieces.size() as i32;
        self.bitfield_received = true;
        // upstream fires the eager `peer_is_interesting`/
        // `send_not_interested` legs — the drain emits whichever
        // the re-scan computes.
        self.need_interest_update = true;
    }

    /// `incoming_choke` / `incoming_unchoke`.
    pub fn on_choke(&mut self) {
        self.peer_choked = true;
        // upstream abandons unsent requests on choke
    }
    /// unchoke.
    pub fn on_unchoke(&mut self) {
        self.peer_choked = false;
        // `m_last_unchoked` — the peer unchoked *us*; feeds
        // `download_queue_time`'s short-unchoke rate leg.
        self.last_unchoked_ms = self.now_ms;
    }
    /// `incoming_interested` / `incoming_not_interested`.
    pub fn on_interested(&mut self) {
        self.peer_interested = true;
    }
    /// `incoming_not_interested` — `m_became_uninterested` stamps
    /// only the interested → not-interested edge.
    pub fn on_not_interested(&mut self) {
        if self.peer_interested {
            self.became_uninterested_ms = self.now_ms;
        }
        self.peer_interested = false;
    }

    /// `incoming_suggest` (peer-conn half) — the torrent-side
    /// checks run in `Torrent::apply_peer_effects`; this only keeps
    /// the bounded front-insert (later suggestions sort first —
    /// the picker ascribes them higher priority).
    pub fn on_suggest(&mut self, index: PieceIndex, max_suggest_pieces: i32) {
        if self.suggested_pieces.len() as i32 > max_suggest_pieces {
            self.suggested_pieces
                .truncate((max_suggest_pieces - 1).max(0) as usize);
        }
        self.suggested_pieces.insert(0, index);
        // upstream `incoming_suggest` fires the eager
        // `peer_is_interesting` leg — the drain emits `Interested`
        // if the suggestion is one we want.
        self.need_interest_update = true;
    }

    /// `incoming_allowed_fast` — BEP-6 allowed-fast.
    pub fn on_allowed_fast(&mut self, index: PieceIndex) {
        self.allowed_fast.push(index);
        // same eager-interest upstream leg as `incoming_suggest`.
        self.need_interest_update = true;
    }

    /// `send_allowed_set` — deterministic BEP-6 allowed-fast set.
    /// Upstream hashes `remote-addr-bytes ‖ info-hash` with SHA-1 and
    /// consumes the digest 32 bits at a time modulo `num_pieces`,
    /// rehashing on exhaustion; pieces the peer already has are
    /// skipped (no point offering them). Returns the `AllowedFast`
    /// messages to write. Callers pass the same gating upstream does:
    /// `valid_metadata`, `!super_seeding`, `!upload_only`.
    pub fn send_allowed_set(
        &mut self,
        info_hash: &bitdaemon_common::Sha1Hash,
        num_pieces: i32,
        set_size: i32,
        valid_metadata: bool,
        super_seeding: bool,
        upload_only: bool,
    ) -> Vec<PeerMessage> {
        let mut out = Vec::new();
        if !valid_metadata || super_seeding || upload_only || set_size <= 0 || num_pieces <= 0 {
            return out;
        }
        if i64::from(set_size) >= i64::from(num_pieces) {
            // more fast slots than pieces — offer everything the
            // peer lacks.
            for i in 0..num_pieces {
                let idx = PieceIndex(i);
                if self.has_piece(idx) || self.accept_fast.contains(&idx) {
                    continue;
                }
                self.accept_fast.push(idx);
                self.accept_fast_piece_cnt.push(0);
                out.push(PeerMessage::AllowedFast(idx));
            }
            return out;
        }

        let mut x: Vec<u8> = match self.remote {
            Some(std::net::SocketAddr::V4(a)) => a.ip().octets().to_vec(),
            Some(std::net::SocketAddr::V6(a)) => a.ip().octets().to_vec(),
            None => Vec::new(),
        };
        x.extend_from_slice(info_hash.as_bytes());

        let mut hash = sha1::Sha1::new();
        hash.update(&x);
        let mut digest: [u8; 20] = hash.finalize().into();

        let mut attempts = 0i32;
        let mut loops = 0i32;
        loop {
            let mut off = 0usize;
            while off + 4 <= digest.len() {
                loops += 1;
                let w = u32::from_be_bytes([
                    digest[off],
                    digest[off + 1],
                    digest[off + 2],
                    digest[off + 3],
                ]);
                off += 4;
                let piece = PieceIndex((w % num_pieces as u32) as i32);
                if self.accept_fast.contains(&piece) {
                    // safety-net to terminate under the worst hash
                    // collisions (upstream `++loops > 500`).
                    if loops > 500 {
                        return out;
                    }
                    continue;
                }
                if !self.has_piece(piece) {
                    self.accept_fast.push(piece);
                    self.accept_fast_piece_cnt.push(0);
                    out.push(PeerMessage::AllowedFast(piece));
                }
                attempts += 1;
                if attempts >= set_size {
                    return out;
                }
            }
            let mut h = sha1::Sha1::new();
            h.update(digest);
            digest = h.finalize().into();
        }
    }

    /// `incoming_dont_have`.
    pub fn on_dont_have(&mut self, index: PieceIndex) -> bool {
        if (index.0 as usize) >= self.peer_pieces.size()
            || !self.peer_pieces.get_bit(index.0 as usize)
        {
            return false;
        }
        self.peer_pieces.clear_bit(index.0 as usize);
        self.num_pieces -= 1;
        true
    }

    /// `incoming_choke` — also abandons unsent requests
    /// (`clear_request_queue`); the blocks are returned so the
    /// picker can `abort_download` them. Upstream keeps the queue
    /// intact for parole peers (`clear_request_queue` early-outs on
    /// `peer_info_struct() == nullptr || !on_parole` — only parole
    /// peers with a picker entry skip the wipe).
    /// `peer_connection::set_endgame` — transition-counted
    /// `num_peers_end_game` gauge.
    pub fn set_endgame(&mut self, b: bool, counters: &crate::counters::Counters) {
        if self.pipeline.endgame == b {
            return;
        }
        self.pipeline.endgame = b;
        counters.add(
            crate::counters::Counter::NumPeersEndGame,
            if b { 1 } else { -1 },
        );
    }

    pub fn on_choke_with_aborts(
        &mut self,
        counters: &crate::counters::Counters,
    ) -> Vec<PieceBlock> {
        self.peer_choked = true;
        self.set_endgame(false, counters);
        if self.on_parole {
            return Vec::new();
        }
        self.pipeline.queued_time_critical = 0;
        self.pipeline
            .request_queue
            .drain(..)
            .map(|b| b.block)
            .collect()
    }

    /// `incoming_request`'s shape check — in-range piece,
    /// `0 <= start < piece_size`, `0 < length <= block_size`, and
    /// `start + length <= piece_size` (sub-block requests are legal;
    /// `validate_piece_request`'s strict `to_req` match is for
    /// *incoming pieces*, not requests). The `user_have_piece ||
    /// predictive || seed_mode` leg runs torrent-side.
    /// `num_pieces` == 0 when metadata is unknown → all reject.
    pub fn validate_request(
        &mut self,
        r: &PeerRequest,
        num_pieces: i32,
        block_size: i32,
        piece_size: i64,
    ) -> bool {
        let ok = r.piece.0 >= 0
            && r.piece.0 < num_pieces
            && r.start >= 0
            && i64::from(r.start) < piece_size
            && r.length > 0
            && i64::from(r.length) + i64::from(r.start) <= piece_size
            && r.length <= block_size;
        if !ok {
            self.num_invalid_requests += 1;
        }
        ok
    }

    /// `incoming_reject_request` — validates the reject against the
    /// request shape upstream accepts (in-range piece, aligned start,
    /// exact block length) and returns `Some(block)` only when the
    /// picker mark should be aborted: a *non*-parole conn on a
    /// non-seed torrent whose dlq entry wasn't already timed out or
    /// cancelled. `piece_sz` is `t->piece_size_for_req(r.piece)` (0
    /// when out of range — validation fails the same way upstream).
    /// Returns `(valid, abort)`: `valid` = the reject passed the
    /// shape checks (callers run the graceful-pause/re-pick tail only
    /// then — malformed rejects are a log line upstream, not a state
    /// change); `abort` = the picker mark to free.
    pub fn on_reject(
        &mut self,
        r: &PeerRequest,
        block_size: i32,
        piece_sz: i32,
        is_seed: bool,
    ) -> (bool, Option<PieceBlock>) {
        // upstream validates the reject shape first — a malformed
        // reject is a log line, not a state change. This also makes
        // BitComet's zero-length `piece` quirk a pure `peer_error`
        // alert: `length != min(piece_sz - start, block_size)` always
        // holds for length 0.
        if piece_sz <= 0
            || r.start < 0
            || r.start >= piece_sz
            || (r.start % block_size) != 0
            || r.length != (piece_sz - r.start).min(block_size)
        {
            return (false, None);
        }
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / block_size,
        };
        let mut abort = None;
        if let Some(pos) = self
            .pipeline
            .download_queue
            .iter()
            .position(|b| b.block == block)
        {
            let entry = self.pipeline.download_queue.remove(pos);
            self.pipeline.outstanding_bytes =
                (self.pipeline.outstanding_bytes - i64::from(r.length)).max(0);
            // `remove_from_picker` — a block already timed out or
            // cancelled keeps its picker state; a parole peer keeps
            // the request (requeued at the head) instead of freeing
            // the block.
            let remove_from_picker = !entry.timed_out && !entry.not_wanted;
            if self.on_parole {
                if remove_from_picker {
                    self.pipeline.request_queue.insert(0, entry);
                }
            } else if !is_seed && remove_from_picker {
                abort = Some(block);
            }
        }
        // upstream `incoming_reject`: a rejected allowed-fast piece
        // (while choked) leaves that set; unchoked, the rejected
        // piece leaves our suggested set instead.
        if self.peer_choked {
            self.allowed_fast.retain(|&p| p != r.piece);
        } else {
            self.suggested_pieces.retain(|&p| p != r.piece);
        }
        (true, abort)
    }

    /// `incoming_piece` bookkeeping for a *requested* block — queue
    /// removal, RTT sample, timeout re-arm. The slow-start/clamp and
    /// un-snub parts live in `PeerPipeline::on_piece_received` /
    /// `maybe_unsnub`, which dispatch runs earlier in upstream's
    /// order. `block_size` is the torrent's `t->block_size()`.
    pub fn on_piece(&mut self, r: &PeerRequest, block_size: i32) -> PieceBlock {
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / block_size,
        };
        let _ = self.cancel_request(block);
        // `incoming_piece` — suggested/allowed-fast entries for the
        // piece we just got drop out of both sets.
        self.suggested_pieces.retain(|&p| p != r.piece);
        self.allowed_fast.retain(|&p| p != r.piece);
        self.pipeline.outstanding_bytes =
            (self.pipeline.outstanding_bytes - i64::from(r.length)).max(0);
        // upstream `incoming_piece`: sample `now - m_requested`
        // *before* re-arming it below.
        self.pipeline.add_request_time_sample(self.now_ms);
        // while requests remain in flight, the next expected block
        // gets a fresh timeout window — the re-arm keys off the
        // download queue only (`m_requested` is set when the queue
        // went non-empty and here on every completion with
        // requests still outstanding).
        if !self.pipeline.download_queue.is_empty() {
            self.pipeline.requested_ms = self.now_ms;
        }
        block
    }

    /// `start_receive_piece` — the header-time bookkeeping upstream
    /// runs when the 9-byte `piece` header completes, before the
    /// payload lands: a block found in `request_queue` migrates into
    /// `download_queue` (its wire request never went out — the entry
    /// carries `not_wanted` = false so the payload is treated as a
    /// normal arrival), and a block unknown to *both* queues is
    /// inserted at the head with `not_wanted` — the payload is
    /// consumed and written anyway, `unwanted_block_alert` marks the
    /// waste. Both inserts grow `outstanding_bytes`; a conn that is
    /// disconnecting skips the whole step.
    ///
    /// Returns `true` when the block was unsolicited (drives the
    /// `unwanted_block_alert` at apply time).
    fn start_receive_piece(&mut self, block: PieceBlock, len: i32) -> bool {
        if self
            .pipeline
            .download_queue
            .iter()
            .any(|b| b.block == block)
        {
            return false;
        }
        if self.disconnecting {
            return false;
        }
        let rq_pos = self
            .pipeline
            .request_queue
            .iter()
            .position(|b| b.block == block);
        if let Some(pos) = rq_pos {
            // migrated entry — if it sat inside the time-critical
            // prefix, the prefix shrinks with it.
            if pos < self.pipeline.queued_time_critical as usize {
                self.pipeline.queued_time_critical -= 1;
            }
            self.pipeline.request_queue.remove(pos);
        }
        let mut pb = PendingBlock::new(block);
        let unsolicited = rq_pos.is_none();
        pb.not_wanted = unsolicited;
        self.pipeline.download_queue.insert(0, pb);
        self.pipeline.outstanding_bytes += i64::from(len);
        unsolicited
    }

    /// `peer_connection::incoming_piece` — was this block actually
    /// requested? Runs after `start_receive_piece`, so the lookup sees
    /// the migrated `request_queue` entry and the `not_wanted` insert
    /// — `m_download_queue` only, matching upstream.
    /// `block_size` is `t->block_size()` — `min(piece_length, 0x4000)`,
    /// so a hardcoded 16 KiB divisor mis-indexes small-piece torrents.
    #[must_use]
    pub fn was_requested(&self, r: &PeerRequest, block_size: i32) -> bool {
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / block_size.max(1),
        };
        self.pipeline
            .download_queue
            .iter()
            .any(|b| b.block == block)
    }

    /// `dispatch` — route a decoded wire message. Returns the
    /// side-effects the torrent layer must run (picker updates,
    /// disk writes, uploads). `block_size` is the torrent's
    /// `t->block_size()` for mapping byte offsets to block indices;
    /// `is_seed` is the *torrent's* `t->is_seed()` (we hold every
    /// piece — incoming payload is `piece_seed` waste).
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch(
        &mut self,
        msg: &bitdaemon_net::peer_wire::PeerMessage,
        num_pieces: i32,
        block_size: i32,
        piece_length: i64,
        files: Option<&bitdaemon_core::FileStorage>,
        is_seed: bool,
        super_seeding: bool,
        max_piece_count: i32,
        max_in_queue: i32,
        counters: &crate::counters::Counters,
        eff: &mut Vec<PeerEffect>,
    ) {
        use bitdaemon_net::peer_wire::PeerMessage as M;
        match *msg {
            M::Keepalive => {}
            M::Choke => {
                // `num_peers_down_unchoked` — transition gauge
                // (`incoming_choke` decs on the unchoked→choked edge).
                if !self.peer_choked {
                    counters.add(crate::counters::Counter::NumPeersDownUnchoked, -1);
                }
                for b in self.on_choke_with_aborts(counters) {
                    eff.push(PeerEffect::AbortDownload(b));
                }
                // upstream `on_choke`: without the FAST extension a
                // choke rejects every outstanding block request —
                // `incoming_reject_request(to_req(pb.block))` runs per
                // download_queue entry, tail included (graceful
                // check, gated re-pick — allowed-fast pieces only
                // while choked — and an unconditional
                // `send_block_requests`; the batch applies them in
                // order and a mid-batch graceful disconnect makes the
                // later sends no-op, as upstream's `is_disconnecting`
                // return does).
                if !self.supports_fast && !self.disconnecting {
                    let blocks: Vec<PieceBlock> = self
                        .pipeline
                        .download_queue
                        .iter()
                        .map(|b| b.block)
                        .collect();
                    let had_in_flight = !blocks.is_empty();
                    for block in blocks {
                        // `to_req` — start is block-aligned, length is
                        // the block's tail-clamped size.
                        let piece_sz =
                            if block.piece_index.0 >= 0 && block.piece_index.0 < num_pieces {
                                files.map_or_else(
                                    || i32::try_from(piece_length).unwrap_or(i32::MAX),
                                    |f| f.piece_size(block.piece_index),
                                )
                            } else {
                                0
                            };
                        let start = block.block_index * block_size;
                        let r = bitdaemon_common::PeerRequest {
                            piece: block.piece_index,
                            start,
                            length: (piece_sz - start).min(block_size),
                        };
                        let (valid, abort) = self.on_reject(&r, block_size, piece_sz, is_seed);
                        if valid {
                            if let Some(b) = abort {
                                eff.push(PeerEffect::AbortDownload(b));
                            }
                            eff.push(PeerEffect::GracefulCheck);
                            if self.pipeline.request_queue.is_empty()
                                && self.pipeline.download_queue.len() < 2
                            {
                                eff.push(PeerEffect::MaybePick(Some(
                                    crate::counters::Counter::RejectPiecePicks,
                                )));
                            }
                        }
                    }
                    if had_in_flight {
                        eff.push(PeerEffect::FlushRequests);
                    }
                }
            }
            M::Unchoke => {
                if self.peer_choked {
                    counters.add(crate::counters::Counter::NumPeersDownUnchoked, 1);
                }
                self.on_unchoke();
                // `incoming_unchoke` — pick only when the peer is
                // interesting; bumps `unchoke_piece_picks` on success.
                if self.interesting {
                    eff.push(PeerEffect::MaybePick(Some(
                        crate::counters::Counter::UnchokePiecePicks,
                    )));
                }
            }
            M::Interested => {
                // upstream `on_interested` — the allowed-fast set is
                // deferred to the first `Interested` (saves bandwidth
                // and skips pieces the peer already has); the flag
                // stamps before `send_allowed_set` runs.
                if !self.sent_allowed_fast && self.supports_fast {
                    self.sent_allowed_fast = true;
                    eff.push(PeerEffect::SendAllowedSet);
                }
                // `incoming_interested` — `num_peers_up_interested`
                // on the not-interested→interested edge.
                if !self.peer_interested {
                    counters.add(crate::counters::Counter::NumPeersUpInterested, 1);
                }
                self.on_interested();
                // `incoming_interested` — a conn mid-teardown stops
                // before the metadata/redundancy legs.
                if self.disconnecting {
                    return;
                }
                // "if the peer is ready to download stuff, it must
                // have metadata" — upstream sets `m_has_metadata`
                // here, before `disconnect_if_redundant`.
                self.has_metadata = true;
                // `incoming_interested` tail — `disconnect_if_redundant`
                // first, then `maybe_unchoke_this_peer` (or the
                // redundant `Unchoke` re-send) only while still
                // alive. Routed to the torrent: the graceful-pause
                // gate and the choke-state branch need its state.
                eff.push(PeerEffect::MaybeUnchoke);
            }
            M::NotInterested => {
                if self.peer_interested {
                    counters.add(crate::counters::Counter::NumPeersUpInterested, -1);
                }
                self.on_not_interested();
                // `choke_this_peer` — an uninterested peer can't
                // take payload; its upload slot is reclaimed
                // (`choke_peer` no-ops when already choked).
                eff.push(PeerEffect::PeerNotInterested);
            }
            M::Have(i) => {
                // upstream: a `have` before any bitfield implies an
                // omitted (empty) bitfield — the real
                // `incoming_have_none` runs (`m_has_metadata` on; the
                // refcount drop can't trigger: `m_bitfield_received`
                // was false ⇒ `m_have_piece` is empty).
                if !self.bitfield_received {
                    self.on_have_none();
                    self.has_metadata = true;
                }
                // `suggest_read_cache`: the peer took a piece we
                // suggested — refill it (checked before the bit
                // lands; upstream reads `m_suggest_pieces`, the
                // per-conn sent list).
                if !self.choked && self.suggest_sent.contains(&i) {
                    eff.push(PeerEffect::SuggestAck(i));
                }
                // `!valid_metadata && index >= end` — grow the
                // bitfield to fit (capped at `max_piece_count`);
                // a larger index is just ignored.
                if num_pieces <= 0 && i.0 as usize >= self.peer_pieces.size() {
                    if i.0 <= max_piece_count {
                        self.peer_pieces.resize_with(i.0 as usize + 1, false);
                    } else {
                        return;
                    }
                }
                // `invalid_have` — out-of-range indexes are a
                // protocol violation upstream (disconnect).
                if i.0 < 0 || i.0 as usize >= self.peer_pieces.size() {
                    eff.push(PeerEffect::Disconnect(
                        "invalid have index",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // a peer sending piece-level messages has metadata
                // (upstream `incoming_have` → `m_has_metadata = true`).
                self.has_metadata = true;
                // upstream returns early on a redundant `have` —
                // no refcount, no seed re-check. No pick attempt
                // here either (`incoming_have` only re-evaluates
                // interest; `request_a_block` fires on unchoke/
                // piece/reject/suggest/snub paths).
                if !self.has_piece(i) {
                    self.on_have(i, |_| false);
                    eff.push(PeerEffect::RefcountInc(i));
                    eff.push(PeerEffect::PeerSeed(self.is_seed()));
                }
            }
            M::HaveAll => {
                // upstream `on_have_all` — FAST messages on a conn
                // that never negotiated the extension disconnect
                // (`invalid_have_all`, peer_error).
                if !self.supports_fast {
                    eff.push(PeerEffect::Disconnect(
                        "invalid have_all",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // `incoming_have_all` — a re-announced all-set drops
                // the previous share first (`peer_lost`).
                if self.bitfield_received && !self.peer_pieces.none_set() {
                    eff.push(PeerEffect::RefcountBulk(self.peer_pieces.clone(), false));
                }
                self.on_have_all();
                if num_pieces <= 0 {
                    // magnet with no metadata yet: a full bitfield
                    // can't be represented — flag it for the replay
                    // in `Torrent::set_metadata`
                    self.pre_metadata_all = true;
                }
                // `peer_has_all` → `inc_refcount_all`: the peer is a
                // seed; skip it in the refcount or `dec_refcount`
                // underflows `m_seeds` on disconnect. Upstream's
                // `incoming_have_all` doesn't kick `request_a_block`
                // — interest recompute is enough.
                let mut bf = bitdaemon_common::Bitfield::with_size(num_pieces.max(0) as usize);
                bf.set_all();
                eff.push(PeerEffect::RefcountBulk(bf, true));
                eff.push(PeerEffect::PeerSeed(true));
            }
            M::HaveNone => {
                // upstream `on_have_none` — FAST without negotiation
                // is `invalid_have_none` (peer_error disconnect).
                if !self.supports_fast {
                    eff.push(PeerEffect::Disconnect(
                        "invalid have_none",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // `peer_lost` on the old bitfield — the peer's
                // previous pieces leave the refcount. have_none
                // proves the peer has metadata (upstream sets
                // `m_has_metadata` here).
                let old = self.peer_pieces.clone();
                self.on_have_none();
                self.has_metadata = true;
                eff.push(PeerEffect::RefcountBulk(old, false));
                eff.push(PeerEffect::PeerSeed(false));
            }
            M::DontHave { piece: i, .. } => {
                // `incoming_dont_have` — on the wire this only ever
                // arrives as an `lt_donthave` extended message (the
                // `Extended` arm below routes it here-equivalent
                // logic inline; this arm stays for the semantic
                // variant). Beyond the torrent's piece range is just
                // logged upstream (magnets have `end_piece()==0` →
                // every index returns), beyond the peer's own
                // bitfield is a protocol violation.
                if i.0 < 0 || i.0 >= num_pieces.max(0) {
                    return;
                }
                // upstream `is_disconnecting` early-out — a conn
                // mid-teardown ignores the message before the
                // bitfield-bound check.
                if self.disconnecting {
                    return;
                }
                if i.0 as usize >= self.peer_pieces.size() {
                    eff.push(PeerEffect::Disconnect(
                        "invalid dont_have index",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                if self.on_dont_have(i) {
                    eff.push(PeerEffect::RefcountDec(i));
                    eff.push(PeerEffect::PeerSeed(false));
                }
            }
            M::Bitfield(ref bits) => {
                // metadata still missing (magnet): `num_pieces` is 0
                // and upstream has nowhere to put the bits — we store
                // them raw and reconcile sizes in `set_metadata`,
                // which is strictly better than disconnecting a
                // well-formed peer.
                let expected = if num_pieces > 0 {
                    num_pieces as usize
                } else {
                    bits.len() * 8
                };
                // upstream `on_bitfield` — with valid metadata the
                // frame must be `ceil(num_pieces/8)` bytes *exactly*;
                // over- or under-sized is `invalid_bitfield_size`
                // (peer_error disconnect). The metadata-less magnet
                // leg skips the check upstream too.
                if num_pieces > 0 && bits.len() != expected.div_ceil(8) {
                    eff.push(PeerEffect::Disconnect(
                        "invalid bitfield size",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                match bitdaemon_common::Bitfield::from_bytes(bits, expected) {
                    Some(bf) => {
                        // repeat bitfield — `peer_lost(m_have_piece)`
                        // decrements the old share before the new one
                        // is counted (`peer_has` upstream).
                        if self.bitfield_received && !self.peer_pieces.none_set() {
                            eff.push(PeerEffect::RefcountBulk(self.peer_pieces.clone(), false));
                        }
                        self.on_bitfield(&bf);
                        let seed = self.is_seed();
                        // upstream marks `seen_complete` only once a
                        // seed-flagged peer has delivered payload —
                        // a bare claim can't move the timestamp.
                        if seed && self.stat_down_total > 0 {
                            eff.push(PeerEffect::SeenComplete);
                        }
                        eff.push(PeerEffect::RefcountBulk(bf, true));
                        eff.push(PeerEffect::PeerSeed(seed));
                    }
                    None => eff.push(PeerEffect::Disconnect(
                        "invalid bitfield size",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    )),
                }
            }
            M::Request(r) => {
                // `incoming_request` — `piece_requests` counts every
                // request wire message, valid or not.
                counters.inc(crate::counters::Counter::PieceRequests);
                // upstream order: the super-seed gate runs *before*
                // `incoming_have_none` — a request outside the leaked
                // set is rejected without touching `m_has_metadata`
                // (peer_connection.cpp:2408-2447).
                if super_seeding && !self.superseed_pieces.contains(&Some(r.piece)) {
                    counters.inc(crate::counters::Counter::InvalidPieceRequests);
                    self.num_invalid_requests += 1;
                    eff.push(PeerEffect::RejectRequest(r));
                    eff.push(PeerEffect::InvalidRequest(format!(
                        "piece not super-seeded: piece {}",
                        r.piece.0
                    )));
                    return;
                }
                // an omitted bitfield reads as `have_none` before the
                // first request; upstream runs the real
                // `incoming_have_none` (`m_has_metadata` on; the
                // refcount drop can't trigger: `m_bitfield_received`
                // was false ⇒ `m_have_piece` is empty).
                if !self.bitfield_received {
                    self.on_have_none();
                    self.has_metadata = true;
                }
                // `!valid_metadata` — requests can't be honored before
                // the layout exists; upstream rejects without the
                // interested/invalid machinery.
                if num_pieces <= 0 {
                    counters.inc(crate::counters::Counter::InvalidPieceRequests);
                    eff.push(PeerEffect::RejectRequest(r));
                    return;
                }
                // `piece_size` is only safe to resolve in-range —
                // upstream's validity check short-circuits on
                // `piece >= end_piece` before touching the layout.
                let piece_size = if r.piece.0 >= 0 && r.piece.0 < num_pieces {
                    files.map_or(piece_length, |f| i64::from(f.piece_size(r.piece)))
                } else {
                    i64::from(block_size)
                };
                // `max_allowed_in_request_queue` — reject floods before
                // they grow the upload queue (`max_piece_requests`).
                // Upstream only *logs* this reject — no
                // `invalid_request_alert`.
                if self.upload_queue.len() as i64 > i64::from(max_in_queue) {
                    counters.inc(crate::counters::Counter::MaxPieceRequests);
                    eff.push(PeerEffect::RejectRequest(r));
                    return;
                }
                // a request implies interest — upstream logs an
                // `invalid_request_alert` then fakes `incoming_interested`.
                if !self.peer_interested {
                    eff.push(PeerEffect::InvalidRequest(format!(
                        "request from not-interested peer: piece {} start {} length {}",
                        r.piece.0, r.start, r.length
                    )));
                    self.on_interested();
                }
                if !self.validate_request(&r, num_pieces, block_size, piece_size) {
                    // upstream `incoming_request` rejects + posts
                    // `invalid_request_alert` for out-of-range
                    // piece/start/length — it does not disconnect
                    // (only >300 invalid requests while choked do).
                    counters.inc(crate::counters::Counter::InvalidPieceRequests);
                    eff.push(PeerEffect::RejectRequest(r));
                    eff.push(PeerEffect::InvalidRequest(format!(
                        "out of range: piece {} start {} length {}",
                        r.piece.0, r.start, r.length
                    )));
                    // every tenth invalid request while we still choke
                    // the peer gets a reminder `Choke`; beyond 300 the
                    // peer is dropped (`too_many_requests_when_choked`
                    // — only while they haven't choked us back).
                    if !self.peer_interested && self.num_invalid_requests % 10 == 0 && self.choked {
                        if self.num_invalid_requests > 300 && !self.peer_choked {
                            eff.push(PeerEffect::Disconnect(
                                "too many requests when choked",
                                bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                            ));
                            return;
                        }
                        self.last_choke_ms = self.now_ms;
                        eff.push(PeerEffect::SendChoke);
                    }
                    return;
                }
                // choked peers may only fetch `accept_fast` pieces; a
                // fast piece stops being free after `3 * blocks_per_piece`
                // requests, and non-fast requests are tolerated for 2 s
                // past the last choke before the peer is dropped.
                let fast_idx = self.accept_fast.iter().position(|&p| p == r.piece);
                let blocks_per_piece = ((piece_length + i64::from(block_size) - 1)
                    / i64::from(block_size).max(1)) as i32;
                if self.choked {
                    if let Some(idx) = fast_idx {
                        if self.accept_fast_piece_cnt[idx] >= 3 * blocks_per_piece {
                            eff.push(PeerEffect::Disconnect(
                                "too many requests when choked",
                                bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                            ));
                            return;
                        }
                    } else {
                        // `choked && fast_idx == -1` — upstream
                        // rejects + counts (`choked_piece_requests`).
                        counters.inc(crate::counters::Counter::ChokedPieceRequests);
                        eff.push(PeerEffect::RejectRequest(r));
                        if self.now_ms - self.last_choke_ms > 2000 {
                            eff.push(PeerEffect::Disconnect(
                                "too many requests when choked",
                                bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                            ));
                            return;
                        }
                        return;
                    }
                }
                if let Some(idx) = fast_idx {
                    self.accept_fast_piece_cnt[idx] += 1;
                }
                // `m_last_incoming_request` stamps only on accepted
                // requests (upstream sets it inside the accept branch).
                self.last_incoming_request_ms = self.now_ms;
                self.upload_queue.push(r);
                eff.push(PeerEffect::ReadRequest(r));
            }
            M::Cancel(r) => {
                if let Some(pos) = self
                    .upload_queue
                    .iter()
                    .position(|u| u.piece == r.piece && u.start == r.start && u.length == r.length)
                {
                    // `incoming_cancel` — a hit counts
                    // (`cancelled_piece_requests`), a miss is ignored.
                    counters.inc(crate::counters::Counter::CancelledPieceRequests);
                    self.upload_queue.remove(pos);
                    eff.push(PeerEffect::CancelRequest(r));
                    // upstream `incoming_cancel` — the peer learns the
                    // request was dropped via `write_reject_request`
                    // (piece_rejects counts even when `supports_fast`
                    // suppresses the actual message — the torrent
                    // side gates the send).
                    eff.push(PeerEffect::RejectRequest(r));
                }
            }
            M::Piece {
                piece,
                offset,
                ref data,
            } => {
                let r = bitdaemon_common::PeerRequest {
                    piece,
                    start: offset,
                    length: data.len() as i32,
                };
                // `bt_peer_connection::on_piece` — a payload larger
                // than the torrent's block size is a protocol
                // violation (`errors::packet_too_large`); upstream
                // disconnects on the first fragment, before any
                // `incoming_piece` state moves.
                if i64::from(r.length) > i64::from(block_size) {
                    eff.push(PeerEffect::Disconnect(
                        "packet_too_large",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // `start_receive_piece` → `validate_piece_request` —
                // in-range piece index, aligned `start < piece_size`,
                // and an exact `to_req` length match. Merged responses
                // can't exist here (`request_large_blocks` is
                // web-seed-only upstream). Anything else disconnects
                // `invalid_piece` — including a zero-length payload
                // (the BitComet reject-like quirk inside
                // `incoming_piece` is only reachable from
                // `web_peer_connection`, which bypasses
                // `start_receive_piece`).
                let piece_sz = if r.piece.0 >= 0 && r.piece.0 < num_pieces {
                    files.map_or_else(
                        || i32::try_from(piece_length).unwrap_or(i32::MAX),
                        |f| f.piece_size(r.piece),
                    )
                } else {
                    0
                };
                if piece_sz <= 0
                    || r.start < 0
                    || r.start >= piece_sz
                    || r.start % block_size.max(1) != 0
                    || r.length != (piece_sz - r.start).min(block_size)
                {
                    eff.push(PeerEffect::Disconnect(
                        "invalid_piece",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                let block = PieceBlock {
                    piece_index: r.piece,
                    block_index: r.start / block_size.max(1),
                };
                // `start_receive_piece` — a block still sitting in
                // `request_queue` migrates into `download_queue` (the
                // request never hit the wire); a block unknown to
                // both queues is inserted `not_wanted` — its payload
                // is written anyway, `unwanted_block_alert` marking
                // the waste. A disconnecting conn skips the insert.
                if self.start_receive_piece(block, r.length) {
                    eff.push(PeerEffect::UnwantedBlockAlert(block));
                }
                // `incoming_piece` — an omitted bitfield reads as
                // `have_none`; upstream runs the real
                // `incoming_have_none` here (`m_has_metadata` on; the
                // refcount drop can't trigger: `m_bitfield_received`
                // was false ⇒ `m_have_piece` is empty).
                if !self.bitfield_received {
                    self.on_have_none();
                    self.has_metadata = true;
                }
                // `incoming_piece` — a conn mid-teardown ignores the
                // rest of the message (upstream's `is_disconnecting`
                // return after `have_none`).
                if self.disconnecting {
                    return;
                }
                // upstream `incoming_piece` runs the slow-start bump
                // + `update_desired_queue_size` before the
                // seed/requested checks.
                self.pipeline.on_piece_received(
                    self.now_ms,
                    i64::from(self.rate_down_speed),
                    block_size,
                );
                // `incoming_piece` is_seed leg — a `piece` received
                // while we are seeding is dropped outright: one stale
                // `m_download_queue` front entry self-heals per
                // message (the block we may have just inserted is
                // the one it takes), the payload counts as
                // `piece_seed` waste, and nothing reaches unsnub,
                // the picker, or disk. `num_peers_down_requests`
                // reconciles via `sync_down_requests`; the payload's
                // `outstanding_bytes` are consumed regardless of the
                // seed state (upstream decrements per fragment).
                if is_seed {
                    if !self.pipeline.download_queue.is_empty() {
                        self.pipeline.download_queue.remove(0);
                    }
                    self.pipeline.outstanding_bytes =
                        (self.pipeline.outstanding_bytes - i64::from(r.length)).max(0);
                    eff.push(PeerEffect::SeedWaste(r.length));
                    return;
                }
                // Defensive `b == end` arm — unreachable once
                // `start_receive_piece` ran (the block was either
                // found or inserted), kept for parity with upstream:
                // `unwanted_block_alert` + `piece_unknown` waste.
                if !self.was_requested(&r, block_size) {
                    eff.push(PeerEffect::UnwantedBlock(block, r.length));
                    return;
                }
                // `incoming_piece` — the queue entry's flags pick the
                // waste bucket when the block turns out already
                // downloaded upstream reads them off the
                // `m_download_queue` iterator before erasing.
                let waste = self
                    .pipeline
                    .download_queue
                    .iter()
                    .find(|b| b.block == block)
                    .map_or(WasteReason::Unknown, |e| {
                        if e.timed_out {
                            WasteReason::TimedOut
                        } else if e.not_wanted {
                            WasteReason::Cancelled
                        } else if e.busy {
                            WasteReason::EndGame
                        } else {
                            WasteReason::Unknown
                        }
                    });
                // `incoming_piece` clears snub only when the block
                // arrived inside the request-timeout window — and
                // only on the *write* path (the `is_downloaded` arm
                // returns before the check). The window is evaluated
                // here, before `on_piece` re-arms `requested_ms`;
                // the flag clears torrent-side when `DiskWrite`
                // actually reaches disk.
                if self.pipeline.unsnub_window_open(self.now_ms) {
                    eff.push(PeerEffect::Unsnub);
                }
                let block = self.on_piece(&r, block_size);
                // upstream `incoming_piece` order: the redundant
                // `is_downloaded` consult and the `async_write`
                // dispatch run *before* `mark_as_writing` — emit
                // `DiskWrite` first so the torrent-side gate reads
                // the pre-mark picker state. `block_finished_alert`
                // posts on write completion (session side), never at
                // receipt.
                eff.push(PeerEffect::DiskWrite(r, data.clone(), waste));
                eff.push(PeerEffect::MarkDownloaded(block));
                // `check_graceful_pause` runs before the
                // re-pick upstream (`is_disconnecting` then skips it).
                eff.push(PeerEffect::GracefulCheck);
                // `incoming_piece` tail — `request_a_block` bumps
                // `incoming_piece_picks` (torrent-side swaps in
                // `incoming_redundant_piece_picks` when the block
                // turned out waste).
                eff.push(PeerEffect::MaybePick(Some(
                    crate::counters::Counter::IncomingPiecePicks,
                )));
            }
            M::RejectRequest(r) => {
                // upstream `on_reject_request` — FAST without
                // negotiation is `invalid_reject` (peer_error).
                if !self.supports_fast {
                    eff.push(PeerEffect::Disconnect(
                        "invalid reject",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // `incoming_reject_request` — a conn mid-teardown
                // ignores the message outright (upstream's
                // `is_disconnecting` return before validation).
                if self.disconnecting {
                    return;
                }
                // `piece_size_for_req` — 0 when the piece is out of
                // range, which fails validation upstream-style.
                let piece_sz = if r.piece.0 >= 0 && r.piece.0 < num_pieces {
                    files.map_or_else(
                        || i32::try_from(piece_length).unwrap_or(i32::MAX),
                        |f| f.piece_size(r.piece),
                    )
                } else {
                    0
                };
                let (valid, abort) = self.on_reject(&r, block_size, piece_sz, is_seed);
                if !valid {
                    return;
                }
                if let Some(b) = abort {
                    eff.push(PeerEffect::AbortDownload(b));
                }
                // `incoming_reject` — `check_graceful_pause` before
                // the retry pick (upstream order).
                eff.push(PeerEffect::GracefulCheck);
                // `incoming_reject` tail — pick only when nearly
                // drained (`request_queue.empty() && download_queue
                // .size() < 2` upstream), `reject_piece_picks`.
                if self.pipeline.request_queue.is_empty() && self.pipeline.download_queue.len() < 2
                {
                    eff.push(PeerEffect::MaybePick(Some(
                        crate::counters::Counter::RejectPiecePicks,
                    )));
                }
                // upstream ends `incoming_reject_request` with an
                // unconditional `send_block_requests` — a parole
                // block requeued at the head (and any surviving
                // `request_queue` entries) go on the wire now, not
                // at the next pick.
                eff.push(PeerEffect::FlushRequests);
            }
            M::AllowedFast(i) => {
                // upstream `on_allowed_fast` — `invalid_allow_fast`
                // when FAST was never negotiated.
                if !self.supports_fast {
                    eff.push(PeerEffect::Disconnect(
                        "invalid allowed_fast",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                // `incoming_allowed_fast`'s bounds/have checks and
                // the `peer_is_interesting` leg need torrent state —
                // routed as an effect.
                eff.push(PeerEffect::AllowedFast(i));
            }
            M::SuggestPiece(i) => {
                // upstream `on_suggest_piece` — `invalid_suggest`
                // when FAST was never negotiated.
                if !self.supports_fast {
                    eff.push(PeerEffect::Disconnect(
                        "invalid suggest",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                    return;
                }
                eff.push(PeerEffect::SuggestPiece(i));
            }
            // upstream `on_piece` swallows a sub-header frame silently
            // (`packet_finished`, no effect) — never disconnects.
            M::Malformed { .. } => {}
            M::DhtPort(p) => eff.push(PeerEffect::DhtPort(p)),
            M::Extended { id, ref data } => {
                if id == 0 {
                    // BEP-10 handshake — learn the remote's ext ids
                    if let Some(hs) = bitdaemon_net::bep10::ExtendedHandshake::parse(data) {
                        let ids = hs.remote_ids();
                        self.ext_upload_only = ids.upload_only;
                        self.ext_holepunch = ids.holepunch;
                        self.ext_dont_have = ids.dont_have;
                        self.ext_ut_metadata =
                            hs.messages.get("ut_metadata").copied().unwrap_or(0) as u8;
                        self.ext_ut_pex = hs.messages.get("ut_pex").copied().unwrap_or(0) as u8;
                        self.ext_i2p_pex = hs.messages.get("i2p_pex").copied().unwrap_or(0) as u8;
                        self.metadata_size = hs.metadata_size;
                        // `set_has_metadata(false)` — a peer that
                        // advertised ut_metadata but no size is still
                        // fetching the info dict itself.
                        if self.ext_ut_metadata != 0 {
                            self.has_metadata = hs.metadata_size.is_some_and(|sz| sz > 0);
                        }
                        // upstream `on_extended_handshake`: an
                        // `upload_only` key calls `set_upload_only`
                        // (peer-list sync + `disconnect_if_redundant`
                        // — the torrent arm handles all three); a
                        // `share_mode` key is honored only under
                        // `settings.support_share_mode` — also
                        // torrent-side.
                        if hs.upload_only {
                            eff.push(PeerEffect::RemoteUploadOnly(true));
                        }
                        if hs.share_mode {
                            eff.push(PeerEffect::RemoteShareMode);
                        }
                        // BEP-21 `complete_ago` — when this peer last
                        // saw a complete copy (upstream stores it as
                        // `m_last_seen_complete` on the conn; the
                        // torrent's `m_swarm_last_seen_complete`
                        // maxes over conns).
                        if hs.complete_ago >= 0 {
                            self.last_seen_complete = std::time::SystemTime::now()
                                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                                .map_or(0, |d| d.as_secs() as i64)
                                - hs.complete_ago;
                        }
                        // ext-handshake `p` — the peer's listen port
                        // (upstream `update_peer_port` +
                        // `received_listen_port`); ut_pex uses it for
                        // inbound conns whose source port is ephemeral.
                        if let Some(p) = hs.listen_port
                            && let Ok(p) = u16::try_from(p)
                        {
                            self.received_listen_port = true;
                            self.peer_listen_port = Some(p);
                            eff.push(PeerEffect::ListenPort(p));
                        }
                        // `reqq` — the peer's max tolerated request
                        // queue (upstream `max_out_request_queue(reqq)`);
                        // exceeding it floods the peer → disconnect.
                        if hs.reqq > 0 {
                            self.pipeline.max_out_request_queue =
                                hs.reqq.min(i64::from(u16::MAX)) as u16;
                        }
                        if let Some(v) = hs.client_version {
                            self.client_version = v;
                        }
                        // `yourip` — the peer's view of our address
                        // (upstream feeds `ip_voter::cast_vote` with
                        // `source_peer`).
                        match hs.your_ip.as_deref() {
                            Some(ip) if ip.len() == 4 => {
                                if let Ok(a) = <[u8; 4]>::try_from(ip) {
                                    eff.push(PeerEffect::YourIp(std::net::IpAddr::from(a)));
                                }
                            }
                            Some(ip) if ip.len() == 16 => {
                                if let Ok(a) = <[u8; 16]>::try_from(ip) {
                                    eff.push(PeerEffect::YourIp(std::net::IpAddr::from(a)));
                                }
                            }
                            _ => {}
                        }
                        eff.push(PeerEffect::ExtHandshake);
                        // `num_incoming_ext_handshake` bumps at the
                        // tail of upstream's `on_extended_handshake`
                        // — a bdecode-invalid handshake never counts.
                        counters.inc(crate::counters::Counter::NumIncomingExtHandshake);
                    }
                } else if id == self.local_ut_metadata && self.local_ut_metadata != 0 {
                    // `ut_metadata_plugin::on_extended` — a peer that
                    // never advertised `ut_metadata` (m_message_index
                    // == 0 upstream) makes the plugin refuse the
                    // message → it falls through to `invalid_message`
                    // like any unconsumed id.
                    if self.ext_ut_metadata == 0 {
                        eff.push(PeerEffect::Disconnect(
                            "invalid message",
                            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                        ));
                        return;
                    }
                    // `num_incoming_metadata` bumps at the END of the
                    // upstream handler — only messages that reach the
                    // switch are counted (not oversized/malformed
                    // disconnects, not unwanted pieces/dont_haves);
                    // the torrent arm owns it.
                    eff.push(PeerEffect::MetadataPacket(data.to_vec()));
                } else if id == self.local_ut_pex && self.local_ut_pex != 0 {
                    // same upstream gate for `ut_pex`
                    // (`m_message_index == 0` → refuse).
                    if self.ext_ut_pex == 0 {
                        eff.push(PeerEffect::Disconnect(
                            "invalid message",
                            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                        ));
                        return;
                    }
                    // `num_incoming_pex` counts at the handler tail —
                    // after disable/size/flood/parse verdicts.
                    eff.push(PeerEffect::PexPacket(data.to_vec()));
                } else if id == self.local_i2p_pex && self.local_i2p_pex != 0 {
                    if self.ext_i2p_pex == 0 {
                        eff.push(PeerEffect::Disconnect(
                            "invalid message",
                            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                        ));
                        return;
                    }
                    eff.push(PeerEffect::I2pPexPacket(data.to_vec()));
                } else if id == self.local_upload_only {
                    // `upload_only` — 1-byte flag (upstream checks
                    // `packet_size() == 3` — ext id + 1 payload byte).
                    if data.len() != 1 {
                        return;
                    }
                    eff.push(PeerEffect::RemoteUploadOnly(data[0] != 0));
                } else if id == self.local_share_mode {
                    // `share_mode` — 1-byte flag; ignored when the
                    // peer is a seed (upstream `set_share_mode`).
                    if data.len() != 1 {
                        return;
                    }
                    if !self.is_seed() {
                        self.remote_share_mode = data[0] != 0;
                    }
                } else if id == self.local_ut_holepunch {
                    // `on_holepunch` line 1508 — `m_holepunch_id == 0`
                    // (the peer never advertised `ut_holepunch`) drops
                    // the message silently; malformed bodies ignored.
                    if self.ext_holepunch != 0
                        && let Some(hp) = crate::ut_holepunch::decode_hp(data)
                    {
                        eff.push(PeerEffect::Holepunch(hp));
                    }
                } else if id == self.local_dont_have {
                    // `on_extended` upstream: a negotiated id with the
                    // wrong size is logged and swallowed, NOT a
                    // disconnect — only unknown ids are fatal.
                    if data.len() != 4 {
                        return;
                    }
                    // `incoming_dont_have` — peer retracted a piece
                    // (same validation as the wire `dont_have` arm).
                    let i = PieceIndex(i32::from_be_bytes([data[0], data[1], data[2], data[3]]));
                    if i.0 < 0 || i.0 >= num_pieces.max(0) {
                        return;
                    }
                    // upstream `is_disconnecting` early-out — before
                    // the `invalid_dont_have` bound check.
                    if self.disconnecting {
                        return;
                    }
                    if i.0 as usize >= self.peer_pieces.size() {
                        eff.push(PeerEffect::Disconnect(
                            "invalid dont_have index",
                            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                        ));
                        return;
                    }
                    if self.on_dont_have(i) {
                        eff.push(PeerEffect::RefcountDec(i));
                        eff.push(PeerEffect::PeerSeed(false));
                    }
                } else {
                    // upstream `on_extended` tail — every extension got
                    // a look (`peer_plugin::on_extended`); unclaimed
                    // ids are `invalid_message` (peer_error).
                    eff.push(PeerEffect::Disconnect(
                        "invalid message",
                        bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    ));
                }
            }
            _ => {}
        }
    }

    /// `add_request(pb, flags)` — queue a picked block. Returns
    /// false when a *busy* (already requested elsewhere) block would
    /// join a pipeline that already has a busy request — upstream
    /// allows at most one busy request per peer, except in
    /// time-critical mode. `m_requested`/`m_last_request` are stamped
    /// when the request hits the wire (`send_block_requests`), not
    /// here.
    pub fn add_request(&mut self, block: PieceBlock, busy: bool, time_critical: bool) -> bool {
        if busy && !time_critical {
            let busy_in_flight = self.pipeline.download_queue.iter().any(|b| b.busy)
                || self.pipeline.request_queue.iter().any(|b| b.busy);
            if busy_in_flight {
                return false;
            }
        }
        let mut pb = PendingBlock::new(block);
        pb.busy = busy;
        pb.time_critical = time_critical;
        if time_critical {
            // critical requests jump ahead of everything already
            // queued, but stay behind earlier criticals.
            let pos = (self.pipeline.queued_time_critical as usize)
                .min(self.pipeline.request_queue.len());
            self.pipeline.request_queue.insert(pos, pb);
            self.pipeline.queued_time_critical += 1;
        } else {
            self.pipeline.request_queue.push(pb);
        }
        true
    }

    /// Cancel an outstanding request (`peer_connection::cancel_block`
    /// shape — timed out or rejected).
    pub fn cancel_request(&mut self, block: PieceBlock) -> bool {
        if let Some(pos) = self
            .pipeline
            .download_queue
            .iter()
            .position(|b| b.block == block)
        {
            self.pipeline.download_queue.remove(pos);
        } else if let Some(pos) = self
            .pipeline
            .request_queue
            .iter()
            .position(|b| b.block == block)
        {
            self.pipeline.request_queue.remove(pos);
            // the time-critical prefix shrinks when a block inside
            // it is removed (upstream `cancel_block` decrements
            // `m_queued_time_critical` the same way).
            if pos < self.pipeline.queued_time_critical as usize {
                self.pipeline.queued_time_critical -= 1;
            }
        } else {
            return false;
        }
        // `m_requested` — re-arm the timeout on the remaining head.
        if self.pipeline.download_queue.is_empty() && self.pipeline.request_queue.is_empty() {
            self.pipeline.requested_ms = 0;
            self.pipeline.outstanding_bytes = 0;
        }
        true
    }

    /// `peer_connection::can_request_time_critical` — eligibility for
    /// deadline picks (`peer_connection.cpp:3572-3588`): unchoked and
    /// interesting, shallow combined queues (≤ 2× the raw
    /// `m_desired_queue_size` member — the snub/endgame collapse
    /// doesn't apply here), not on parole, not disconnecting, torrent
    /// not in upload mode, not snubbed.
    #[must_use]
    pub fn can_request_time_critical(&self, torrent_upload_mode: bool) -> bool {
        if self.peer_choked || !self.interesting {
            return false;
        }
        if self.pipeline.download_queue.len() + self.pipeline.request_queue.len()
            > i32::from(self.pipeline.desired_queue_size) as usize * 2
        {
            return false;
        }
        if self.on_parole || self.disconnecting || torrent_upload_mode {
            return false;
        }
        !self.pipeline.snubbed
    }

    /// `peer_connection::make_time_critical` — move an unsent
    /// request-queue block into the time-critical prefix. Returns
    /// false when the block isn't queued or is already critical.
    pub fn make_time_critical(&mut self, block: PieceBlock) -> bool {
        let Some(pos) = self
            .pipeline
            .request_queue
            .iter()
            .position(|b| b.block == block)
        else {
            return false;
        };
        if pos < self.pipeline.queued_time_critical as usize {
            return false;
        }
        let b = self.pipeline.request_queue.remove(pos);
        self.pipeline
            .request_queue
            .insert(self.pipeline.queued_time_critical as usize, b);
        self.pipeline.queued_time_critical = self.pipeline.queued_time_critical.saturating_add(1);
        true
    }

    /// Build the `PeerRequestCtx` the picker glue needs from this
    /// connection's state.
    pub fn request_ctx(
        &self,
        no_download: bool,
        disconnecting: bool,
        rate: i64,
    ) -> PeerRequestCtx<'_> {
        // outstanding = download_queue ∪ request_queue blocks
        PeerRequestCtx {
            no_download,
            disconnecting,
            desired_queue_size: self.pipeline.desired_queue_size(),
            outstanding: &[], // filled by caller — see `outstanding()`
            prefer_contiguous_blocks: 0,
            download_payload_rate: rate,
            peer_choked: self.peer_choked,
            allowed_fast: &self.allowed_fast,
            peer_pieces: &self.peer_pieces,
            peer_key: self.peer_key(),
            picker_options: crate::piece_picker::PickerOptions::RAREST_FIRST,
            suggested_pieces: &self.suggested_pieces,
        }
    }

    /// All blocks outstanding (download + request queues).
    #[must_use]
    pub fn outstanding(&self) -> Vec<PieceBlock> {
        self.pipeline
            .download_queue
            .iter()
            .chain(self.pipeline.request_queue.iter())
            .map(|b| b.block)
            .collect()
    }

    /// The queued entries themselves (download + request queues) —
    /// `remove_peer`'s abort loop needs `timed_out`/`not_wanted`
    /// because upstream only releases picker state for entries that
    /// are neither (`!b.timed_out && !b.not_wanted` — a timed-out or
    /// unwanted block already lost its picker refcount).
    #[must_use]
    pub fn outstanding_entries(&self) -> Vec<crate::peer_pipeline::PendingBlock> {
        self.pipeline
            .download_queue
            .iter()
            .chain(self.pipeline.request_queue.iter())
            .copied()
            .collect()
    }

    /// `peer_info_struct()` — picker's stable peer key (use the peer
    /// id's first 8 bytes when known, else the connect slot).
    #[must_use]
    pub fn peer_key(&self) -> Option<u64> {
        self.peer_id.map(|id| {
            let b = id.as_bytes();
            u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
        })
    }

    /// `peer_connection::picker_options()` — the per-peer legs on
    /// top of the torrent-level `base` (see
    /// `piece_picker::picker_options`): parole peers pick
    /// `on_parole | prioritize_partials` (only pieces they touched
    /// first) on any branch; inside `rarest_first`, snubbed peers
    /// pick `reverse` so they converge on the same common pieces —
    /// else `piece_extent_affinity` when the setting is on and no
    /// time-critical pieces are open.
    #[must_use]
    pub fn picker_options(
        &self,
        base: crate::piece_picker::PickerOptions,
        piece_extent_affinity: bool,
        time_critical_mode: bool,
    ) -> crate::piece_picker::PickerOptions {
        let mut opts = base;
        if self.on_parole {
            opts = opts
                | crate::piece_picker::PickerOptions::ON_PAROLE
                | crate::piece_picker::PickerOptions::PRIORITIZE_PARTIALS;
        }
        if opts.contains(crate::piece_picker::PickerOptions::RAREST_FIRST) {
            if self.pipeline.snubbed {
                opts = opts | crate::piece_picker::PickerOptions::REVERSE;
            } else if piece_extent_affinity && !time_critical_mode {
                opts = opts | crate::piece_picker::PickerOptions::PIECE_EXTENT_AFFINITY;
            }
        }
        opts
    }

    /// Drive the picker → request queue → wire for this peer
    /// (`request_a_block` + `send_block_requests` in upstream order).
    /// `opts` is `picker_options()` for `mark_as_downloading`.
    /// Returns `(invoked, endgame, wire_requests, newly_queued)` —
    /// callers post `block_downloading_alert` for each queued block.
    #[allow(clippy::too_many_arguments)]
    pub fn drive_requests<T: crate::peer_pipeline::PipelineTorrent>(
        &mut self,
        picker: &mut PiecePicker,
        t: &TorrentRequestCtx,
        torrent: &T,
        opts: crate::piece_picker::PickerOptions,
        rate: i64,
        send_buffer_size: usize,
        now_ms: i64,
        counters: &crate::counters::Counters,
        pc: &mut dyn PickerStats,
    ) -> (
        bool,
        Option<bool>,
        Vec<bitdaemon_common::PeerRequest>,
        Vec<PieceBlock>,
    ) {
        let peer_key = self.peer_key();
        let outstanding = self.outstanding();
        let disconnecting = self.disconnecting;
        let time_critical_mode = t.time_critical_mode;
        // `picker_options()` — per-peer legs on the caller's base.
        let opts = self.picker_options(opts, t.piece_extent_affinity, t.time_critical_mode);
        let mut ctx = self.request_ctx(false, disconnecting, rate);
        ctx.outstanding = &outstanding;
        ctx.picker_options = opts;
        // The busy cap runs inside the pick loop like upstream's
        // `add_request` does; `picked` stands in for the request-queue
        // prefix since marks land only after the call returns.
        let mut picked: Vec<(PieceBlock, bool)> = Vec::new();
        let (invoked, endgame) =
            crate::request_blocks::request_a_block_out(picker, t, &ctx, pc, |b, busy| {
                if busy && !time_critical_mode {
                    let busy_in_flight = picked.iter().any(|&(_, pbusy)| pbusy)
                        || self.pipeline.download_queue.iter().any(|p| p.busy)
                        || self.pipeline.request_queue.iter().any(|p| p.busy);
                    if busy_in_flight {
                        return false;
                    }
                }
                picked.push((b, busy));
                true
            });
        if let Some(e) = endgame {
            // `m_endgame_mode` — upstream sets it inside
            // request_a_block, before the marks land.
            self.set_endgame(e, counters);
        }
        let mut queued = Vec::new();
        for (b, busy) in picked {
            if picker.mark_as_downloading(b, peer_key, opts) {
                if self.add_request(b, busy, false) {
                    queued.push(b);
                } else {
                    // lost the busy-cap race vs blocks marked above
                    picker.abort_download(b, peer_key);
                }
            }
        }
        let reqs = self.pipeline.send_block_requests(
            torrent,
            send_buffer_size,
            now_ms,
            disconnecting,
            |b| {
                if picker.is_downloaded(b) {
                    picker.abort_download(b, peer_key);
                    true
                } else {
                    false
                }
            },
        );
        (invoked, endgame, reqs, queued)
    }

    /// `write_handshake` — the 68-byte BitTorrent handshake with the
    /// BEP-10 extension bit set (`write_handshake` upstream sets it
    /// when `m_supports_extensions`).
    #[must_use]
    pub fn handshake(info_hash: &[u8; 20], our_peer_id: &PeerId) -> Vec<u8> {
        let mut out = Vec::with_capacity(68);
        out.push(19u8);
        out.extend(b"BitTorrent protocol");
        let mut reserved = [0u8; 8];
        // extension protocol (BEP-10) — byte 5, bit 0x10
        reserved[5] |= 0x10;
        out.extend(reserved);
        out.extend(info_hash);
        out.extend(our_peer_id.as_bytes());
        out
    }

    /// Message to send when we decide the peer is interesting.
    #[must_use]
    pub fn interested_msg() -> PeerMessage {
        PeerMessage::Interested
    }

    /// `peer_connection::wanted_transfer` — how much quota a request
    /// should cover for `channel`. `recv_spare` is the engine's read
    /// buffer room, upstream's `m_recv_buffer.packet_bytes_remaining()`.
    /// `tick_ms` is `settings_pack::tick_interval` in milliseconds.
    #[must_use]
    pub fn wanted_transfer(&self, channel: usize, recv_spare: i64, tick_ms: i64) -> i64 {
        let tick = tick_ms.max(1);
        if channel == DOWNLOAD_CHANNEL {
            let download_rate = i64::from(self.rate_down_speed) * 3 / 2;
            (self.pipeline.outstanding_bytes + 30)
                .max(recv_spare + 30)
                .max(download_rate * tick / 1000)
        } else {
            let upload_rate = i64::from(self.rate_up_speed) * 2;
            (self.reading_bytes as i64)
                .max(self.send_buffer as i64)
                .max(upload_rate * tick / 1000)
        }
    }

    /// `stat::trancieve_ip_packet` — IP/TCP overhead estimate for one
    /// socket op moving `bytes`: one packet header per `mtu`-sized
    /// run *plus* the ACK going back the other way (upstream folds
    /// both into each direction's `ip_protocol` channel).
    #[must_use]
    pub fn ip_overhead_bytes(bytes: i64, ipv6: bool) -> i64 {
        let header = (if ipv6 { 40 } else { 20 }) + 20;
        let packet_size = 1500 - header;
        // `max(1, ceil(bytes / packet_size)) * header` — `ceil` for
        // non-negative operands is `(a + b - 1) / b` (0 for a=0, which
        // the floor covers).
        ((bytes + packet_size - 1) / packet_size).max(1) * header
    }

    /// `stat::total_upload()` — payload + protocol + ip-overhead.
    #[must_use]
    pub fn total_upload(&self) -> i64 {
        self.stat_up_total + self.stat_up_protocol + self.stat_up_ip
    }

    /// `stat::total_download()`.
    #[must_use]
    pub fn total_download(&self) -> i64 {
        self.stat_down_total + self.stat_down_protocol + self.stat_down_ip
    }

    /// `peer_connection::download_queue_time` — estimated ms to
    /// drain the request queue. `torrent_payload_rate` is the
    /// torrent's payload download rate and `peers_with_requests` the
    /// session's `num_peers_down_requests` gauge (upstream's global
    /// download-rate leg approximates a fair share).
    #[must_use]
    pub fn download_queue_time(
        &self,
        extra_bytes: i64,
        torrent_payload_rate: i64,
        peers_with_requests: i64,
        block_size: i64,
    ) -> i64 {
        let mut rate: i64 = if self.now_ms - self.pipeline.last_piece_ms > 30_000
            && self.download_rate_peak > 0
        {
            i64::from(self.download_rate_peak)
        } else if self.now_ms - self.last_unchoked_ms < 5_000 && self.stat_up_total < 2 * 0x4000 {
            // freshly unchoked — no measured rate yet, assume a
            // fair share of the torrent's payload download rate.
            torrent_payload_rate / peers_with_requests.max(1)
        } else {
            i64::from(self.rate_down_payload)
        };
        if rate < 50 {
            rate = 50;
        }
        (self.pipeline.outstanding_bytes
            + extra_bytes
            + i64::from(self.pipeline.queued_time_critical) * block_size)
            * 1000
            / rate
    }
}

/// `aux::waste_reason` — why a received block's payload was thrown
/// away; indexes the `waste_piece_*` stats counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasteReason {
    /// `piece_timed_out` — a timed-out request's payload arrived late.
    TimedOut,
    /// `piece_cancelled` — the request was cancelled while in flight.
    Cancelled,
    /// `piece_unknown` — unrequested / already-picked-elsewhere.
    Unknown,
    /// `piece_seed` — we're a seed; all incoming payload is waste.
    Seed,
    /// `piece_end_game` — busy-block duplicate (end-game requester).
    EndGame,
    /// `piece_closing` — conn was disconnecting when it arrived.
    Closing,
}

/// `peer_connection` incoming-side effects — the session runs
/// these (picker/disk/wire/alerts).
#[derive(Debug)]
pub enum PeerEffect {
    /// `picker.inc_refcount(index)`.
    RefcountInc(PieceIndex),
    /// `picker.dec_refcount(index)`.
    RefcountDec(PieceIndex),
    /// bulk refcount from a bitfield (`true` = inc).
    RefcountBulk(bitdaemon_common::Bitfield, bool),
    /// write a received block to disk — the `WasteReason` carries the
    /// queue-entry flags (`timed_out`/`not_wanted`/`busy`) upstream
    /// reads off the `m_download_queue` iterator to bucket the block
    /// when it turns out already-downloaded. The payload rides as
    /// `Bytes` — a refcount clone, not a `to_vec` copy.
    DiskWrite(bitdaemon_common::PeerRequest, bytes::Bytes, WasteReason),
    /// serve a peer's request (upload path).
    ReadRequest(bitdaemon_common::PeerRequest),
    /// peer cancelled a request.
    CancelRequest(bitdaemon_common::PeerRequest),
    /// `picker.mark_as_downloaded`.
    MarkDownloaded(PieceBlock),
    /// `picker.abort_download` (reject/choke clearing).
    AbortDownload(PieceBlock),
    /// drive `request_a_block` again — the counter tags the pick
    /// context (`incoming_piece_picks`, `reject_piece_picks`,
    /// `unchoke_piece_picks`…) and bumps only when a pick lands.
    /// `None` = uncounted context.
    MaybePick(Option<crate::counters::Counter>),
    /// `check_graceful_pause` — under `graceful_pause` a conn is
    /// dropped once its `m_outstanding_bytes` drains to 0. Emitted
    /// at upstream's check sites (`incoming_piece`/`incoming_reject`
    /// tails; write completion is handled session-side).
    GracefulCheck,
    /// BEP-5 port announcement.
    DhtPort(u16),
    /// BEP-10 extended handshake received.
    ExtHandshake,
    /// a `ut_metadata` message body (dict + optional payload).
    MetadataPacket(Vec<u8>),
    /// a `ut_pex` message body.
    PexPacket(Vec<u8>),
    /// an `i2p_pex` message body (32-byte destination hashes).
    I2pPexPacket(Vec<u8>),
    /// `ut_holepunch` message (BEP-55) — caller routes it through
    /// `ut_holepunch::{rendezvous, connect_allowed}`.
    Holepunch(crate::ut_holepunch::Hp),
    /// `yourip` from the BEP-10 handshake — the peer's view of our
    /// external address (`ip_voter` `source_peer`).
    YourIp(std::net::IpAddr),
    /// `p` port from the BEP-10 handshake — the peer's real listen
    /// port (upstream `peer_list::update_peer_port`; an inbound
    /// conn's source port is ephemeral, so this makes the entry
    /// dialable). `false` back from `update_peer_port` drops the
    /// conn — the endpoint now collides with another live peer.
    ListenPort(u16),
    /// `torrent::seen_complete` — a peer proved a complete copy
    /// exists (upstream marks it on `have_all` with prior payload,
    /// and on every `piece` a seed delivers).
    SeenComplete,
    /// `incoming_suggest` — BEP-6 SUGGEST payload; the checks live
    /// on the torrent side (it owns `have_piece`/`valid_metadata`).
    SuggestPiece(PieceIndex),
    /// `incoming_allowed_fast` — BEP-6 allowed-fast payload; bounds
    /// checks, `have_piece` skip and the `peer_is_interesting` leg
    /// all need torrent state (`valid_metadata`/`picker`).
    AllowedFast(PieceIndex),
    /// `on_interested` — first `Interested` on a FAST conn triggers
    /// `send_allowed_set` (BEP-6); torrent-side because the set
    /// needs the piece count and the peer's bitfield.
    SendAllowedSet,
    /// `on_extended_handshake` `share_mode` key — honored only under
    /// `settings_pack::support_share_mode` upstream, so the gate
    /// lives on the torrent side.
    RemoteShareMode,
    /// `incoming_have` — the peer took a suggested piece; under
    /// `suggest_read_cache` refill the suggestion slot.
    SuggestAck(PieceIndex),
    /// `unwanted_block_alert` — posted at `start_receive_piece` time
    /// when an unsolicited block is inserted `not_wanted` into the
    /// download queue; the payload still goes through the normal
    /// write path (no waste is counted here — the `is_downloaded`
    /// consult downstream decides `piece_cancelled`).
    UnwantedBlockAlert(PieceBlock),
    /// `unwanted_block_alert` + `piece_unknown` waste — the
    /// `b == m_download_queue.end()` arm of `incoming_piece`
    /// (unreachable once `start_receive_piece` ran; kept for parity).
    UnwantedBlock(PieceBlock, i32),
    /// `send_block_requests` alone — upstream flushes the
    /// `request_queue` after every valid `incoming_reject_request`
    /// (and once at the tail of the non-FAST choke rejection loop)
    /// regardless of the pick gates; nothing is picked, queued
    /// entries just go on the wire.
    FlushRequests,
    /// `incoming_piece` seed leg — a `piece` message received while
    /// we hold every piece: `piece_seed` waste, no alert, no write.
    SeedWaste(i32),
    /// `block_finished_alert` — a `piece` message completed a
    /// requested block.
    BlockFinished(PieceBlock),
    /// `peer_unsnubbed_alert` — data arrived from a snubbed peer
    /// (upstream clears `m_snubbed` in `incoming_piece`).
    Unsnub,
    /// `write_reject_request` — send a BEP-6 `reject_request` for a
    /// malformed/unservable request. The torrent layer gates the
    /// actual send on `supports_fast`.
    RejectRequest(bitdaemon_common::PeerRequest),
    /// `invalid_request_alert` — a request we rejected (malformed
    /// range, super-seed mismatch, choked + not allowed-fast). The
    /// `msg` mirrors upstream's peer_log reason text.
    InvalidRequest(String),
    /// the peer's completeness flipped (`set_seed` upstream —
    /// bitfield-full, `have_all`, or `have` reaching full → `true`;
    /// `have_none`/shrinking bitfield → `false`).
    PeerSeed(bool),
    /// `set_upload_only` on the inbound `upload_only` message — the
    /// remote toggled BEP-21 upload-only mode. The torrent side syncs
    /// the peer-list `upload_only` bit and runs
    /// `disconnect_if_redundant`.
    RemoteUploadOnly(bool),
    /// `write_choke` — the bare `Choke` re-send upstream issues as a
    /// choked-state reminder every tenth invalid request; unlike
    /// `choke_peer` it emits unconditionally and only updates
    /// `m_last_choke` + the outgoing-choke counter.
    SendChoke,
    /// `incoming_not_interested` → `choke_this_peer` — a peer that
    /// wants nothing from us forfeits its upload slot; the torrent
    /// runs `choke_peer` (which clears optimistic state and re-runs
    /// the unchoke pick on the next choker pass).
    PeerNotInterested,
    /// `incoming_interested` tail — `maybe_unchoke_this_peer` on a
    /// choked conn, or the redundant `Unchoke` re-send upstream emits
    /// on an already-unchoked one (peers that missed the handshake
    /// unchoke). The `graceful_pause` gate lives on the torrent.
    MaybeUnchoke,
    /// `peer_error_alert` — a non-fatal peer protocol complaint
    /// (e.g. `peer_sent_empty_piece`); posted, no disconnect.
    PeerError(&'static str),
    /// protocol violation — drop the connection. The severity mirrors
    /// upstream `disconnect_severity_t` (`peer_error` for `invalid_*`
    /// violations): it picks the teardown alert
    /// (`peer_error_alert` vs `peer_disconnected_alert`) and drives
    /// the swarm-entry `failcount` bump in `connection_closed`.
    Disconnect(
        &'static str,
        bitdaemon_net::bt_peer_conn::DisconnectSeverity,
    ),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitdaemon_common::Sha1Hash;
    use bitdaemon_net::peer_wire::PeerMessage;

    fn conn(remote: &str) -> PeerConn {
        let mut c = PeerConn::new(100, PeerPipeline::new(1000, 30, 250), 0);
        c.remote = Some(remote.parse().unwrap());
        c
    }

    #[test]
    fn allowed_set_deterministic_and_bounded() {
        let ih = Sha1Hash::from_bytes(&[7u8; 20]).unwrap();
        let mut a = conn("1.2.3.4:6881");
        let mut b = conn("1.2.3.4:6881");
        let sa = a.send_allowed_set(&ih, 100, 5, true, false, false);
        let sb = b.send_allowed_set(&ih, 100, 5, true, false, false);
        assert_eq!(sa, sb);
        assert!(!sa.is_empty());
        assert!(sa.len() <= 5);
        // different addr → different set
        let mut c = conn("5.6.7.8:6881");
        let sc = c.send_allowed_set(&ih, 100, 5, true, false, false);
        assert_ne!(sa, sc);
    }

    #[test]
    fn allowed_set_skips_peer_pieces_and_gates() {
        let ih = Sha1Hash::from_bytes(&[9u8; 20]).unwrap();
        let mut c = conn("1.2.3.4:6881");
        // no metadata → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, false, false, false)
                .is_empty()
        );
        // super-seeding → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, true, false)
                .is_empty()
        );
        // upload-only → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, true)
                .is_empty()
        );
        // set_size <= 0 → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, true)
                .is_empty()
        );
        // peer has every piece → nothing offered
        c.peer_pieces.set_all();
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, false)
                .is_empty()
        );
    }

    #[test]
    fn allowed_set_larger_than_torrent() {
        let ih = Sha1Hash::from_bytes(&[1u8; 20]).unwrap();
        let mut c = conn("9.9.9.9:1");
        let msgs = c.send_allowed_set(&ih, 3, 10, true, false, false);
        assert_eq!(msgs.len(), 3);
        assert!(
            msgs.iter()
                .all(|m| matches!(m, PeerMessage::AllowedFast(_)))
        );
    }
}
