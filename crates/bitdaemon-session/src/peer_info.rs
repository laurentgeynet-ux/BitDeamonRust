// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_info.hpp` — the public per-peer status snapshot.
//! Flag bit values are verbatim upstream; fields are the subset
//! our engine tracks today (rate/counter fields we don't have yet
//! are documented, not faked).

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::{Bitfield, PeerId};
use std::net::SocketAddr;

/// `peer_flags_t` — connection state bits (`peer_info.hpp`,
/// verbatim values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerFlags(pub u32);

impl PeerFlags {
    /// `interesting` — **we** want pieces this peer has.
    pub const INTERESTING: Self = Self(1 << 0);
    /// `choked` — we are choking this peer.
    pub const CHOKED: Self = Self(1 << 1);
    /// `remote_interested` — the peer wants pieces from us.
    pub const REMOTE_INTERESTED: Self = Self(1 << 2);
    /// `remote_choked` — the peer is choking us.
    pub const REMOTE_CHOKED: Self = Self(1 << 3);
    /// `supports_extensions` — BEP-10 seen.
    pub const SUPPORTS_EXTENSIONS: Self = Self(1 << 4);
    /// `outgoing_connection`.
    pub const OUTGOING_CONNECTION: Self = Self(1 << 5);
    /// `handshake` — handshake received, awaiting metadata.
    pub const HANDSHAKE: Self = Self(1 << 6);
    /// `connecting` — connection in progress.
    pub const CONNECTING: Self = Self(1 << 7);
    /// `on_parole` — piece-at-a-time after a hash fail.
    pub const ON_PAROLE: Self = Self(1 << 9);
    /// `seed` — peer has all pieces.
    pub const SEED: Self = Self(1 << 10);
    /// `optimistic_unchoke`.
    pub const OPTIMISTIC_UNCHOKE: Self = Self(1 << 11);
    /// `snubbed` — timed out a block request.
    pub const SNUBBED: Self = Self(1 << 12);
    /// `upload_only` — peer won't download more.
    pub const UPLOAD_ONLY: Self = Self(1 << 13);
    /// `endgame_mode`.
    pub const ENDGAME_MODE: Self = Self(1 << 14);
    /// `holepunched`.
    pub const HOLEPUNCHED: Self = Self(1 << 15);
    /// `i2p_socket`.
    pub const I2P_SOCKET: Self = Self(1 << 16);
    /// `utp_socket`.
    pub const UTP_SOCKET: Self = Self(1 << 17);
    /// `ssl_socket`.
    pub const SSL_SOCKET: Self = Self(1 << 18);
    /// `rc4_encrypted`.
    pub const RC4_ENCRYPTED: Self = Self(1 << 19);
    /// `plaintext_encrypted` — DH handshake done.
    pub const PLAINTEXT_ENCRYPTED: Self = Self(1 << 20);

    /// bit set?
    #[must_use]
    pub fn has(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }

    /// set `f`.
    #[must_use]
    pub fn with(self, f: Self) -> Self {
        Self(self.0 | f.0)
    }
}

/// `connection_type_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionType(pub u8);

impl ConnectionType {
    /// `standard_bittorrent`.
    pub const STANDARD_BITTORRENT: Self = Self(1 << 0);
    /// `web_seed` — BEP-19.
    pub const WEB_SEED: Self = Self(1 << 1);
    /// `http_seed` — BEP-17.
    pub const HTTP_SEED: Self = Self(1 << 2);
}

/// `bandwidth_state_flags_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandwidthState(pub u8);

impl BandwidthState {
    /// `bw_idle`.
    pub const IDLE: Self = Self(0);
    /// `bw_limit` — waiting on the rate limiter.
    pub const LIMIT: Self = Self(1 << 0);
    /// `bw_network` — waiting on socket i/o.
    pub const NETWORK: Self = Self(1 << 1);
    /// `bw_disk` — waiting on the disk queue.
    pub const DISK: Self = Self(1 << 2);
}

/// `peer_info` — snapshot for API/reporting (Tribler REST). Fields
/// we don't track yet (`*_speed`, quotas, rtt) are kept for wire
/// shape parity and populated as 0 — documented.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// `flags` — `PeerFlags` bitmask.
    pub flags: PeerFlags,
    /// `source` — where the peer came from.
    pub source: PeerSourceFlags,
    /// `up_speed`/`down_speed` (B/s) — not tracked yet.
    pub up_speed: i32,
    /// download speed.
    pub down_speed: i32,
    /// payload-only speeds.
    pub payload_up_speed: i32,
    /// payload-only download speed.
    pub payload_down_speed: i32,
    /// `pid` — remote peer id (all-zero until handshake).
    pub pid: PeerId,
    /// `pieces` — the peer's have-bitfield.
    pub pieces: Bitfield,
    /// `queue_bytes` — requested, not yet received.
    pub queue_bytes: i32,
    /// `request_timeout` (s, −1 = none).
    pub request_timeout: i32,
    /// `send_buffer_size`/`used_send_buffer`.
    pub send_buffer_size: i32,
    /// bytes of send buffer in use.
    pub used_send_buffer: i32,
    /// `receive_buffer_size`/`used_receive_buffer`.
    pub receive_buffer_size: i32,
    /// receive buffer in use.
    pub used_receive_buffer: i32,
    /// `receive_buffer_watermark`.
    pub receive_buffer_watermark: i32,
    /// `num_hashfails`.
    pub num_hashfails: i32,
    /// `download_queue_length` — outstanding requests.
    pub download_queue_length: i32,
    /// `timed_out_requests`.
    pub timed_out_requests: i32,
    /// `busy_requests` — also requested elsewhere.
    pub busy_requests: i32,
    /// `requests_in_buffer` — queued for send.
    pub requests_in_buffer: i32,
    /// `target_dl_queue_length` — AIMD queue goal.
    pub target_dl_queue_length: i32,
    /// `upload_queue_length` — unanswered peer requests.
    pub upload_queue_length: i32,
    /// `failcount`.
    pub failcount: i32,
    /// `downloading_piece_index` (−1 = none).
    pub downloading_piece_index: i32,
    /// `downloading_block_index`.
    pub downloading_block_index: i32,
    /// `downloading_progress` (bytes into block).
    pub downloading_progress: i32,
    /// `downloading_total` (block size).
    pub downloading_total: i32,
    /// `connection_type`.
    pub connection_type: ConnectionType,
    /// `pending_disk_bytes` — queued for write.
    pub pending_disk_bytes: i32,
    /// `pending_disk_read_bytes`.
    pub pending_disk_read_bytes: i32,
    /// `send_quota`/`receive_quota`.
    pub send_quota: i32,
    /// receive quota.
    pub receive_quota: i32,
    /// `rtt` (ms, 0 for incoming).
    pub rtt: i32,
    /// `num_pieces`.
    pub num_pieces: i32,
    /// `download_rate_peak`/`upload_rate_peak`.
    pub download_rate_peak: i32,
    /// upload peak.
    pub upload_rate_peak: i32,
    /// `progress_ppm` [0,1_000_000].
    pub progress_ppm: i32,
    /// `ip` — remote endpoint (`None` for i2p).
    pub remote_endpoint: Option<SocketAddr>,
    /// `local_endpoint`.
    pub local_endpoint: Option<SocketAddr>,
    /// `read_state`/`write_state`.
    pub read_state: BandwidthState,
    /// write bw state.
    pub write_state: BandwidthState,
    /// `client` — `v` from ext-handshake or fingerprint.
    pub client: Vec<u8>,
}
