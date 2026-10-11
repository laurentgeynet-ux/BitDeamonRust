// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `alert` + `alert_manager` — the event bus. Upstream
//! `alert_types.hpp` declares ~100 structs; this ports the alert
//! *categories* verbatim and the subset of alerts the ported
//! machinery emits, behind one `Alert` enum (struct-of-variants
//! becomes enum payload). New alert payloads add variants, not
//! new files.

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::PieceIndex;

/// `alert_category_t` — bitmask (upstream: every alert carries a
/// `category()`; the manager filters on the mask).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AlertCategory(pub u32);

impl AlertCategory {
    /// `error_notification`.
    pub const ERROR: Self = Self(0x01);
    /// `peer_notification`.
    pub const PEER: Self = Self(0x02);
    /// `port_mapping_notification`.
    pub const PORT_MAPPING: Self = Self(0x04);
    /// `storage_notification`.
    pub const STORAGE: Self = Self(0x08);
    /// `tracker_notification`.
    pub const TRACKER: Self = Self(0x10);
    /// `status_notification`.
    pub const STATUS: Self = Self(0x20);
    /// `progress_notification`.
    pub const PROGRESS: Self = Self(0x40);
    /// `piece_progress_notification`.
    pub const PIECE_PROGRESS: Self = Self(0x80);
    /// `dht_notification`.
    pub const DHT: Self = Self(0x100);
    /// `stats_notification`.
    pub const STATS: Self = Self(0x200);
    /// `connect_notification`.
    pub const CONNECT: Self = Self(0x400);
    /// `file_progress_notification`.
    pub const FILE_PROGRESS: Self = Self(0x800);
    /// `ip_block_notification`.
    pub const IP_BLOCK: Self = Self(0x1000);
    /// `performance_warning`.
    pub const PERFORMANCE: Self = Self(0x2000);
    /// `upload_notification`.
    pub const UPLOAD: Self = Self(0x4000);
    /// `all_categories`.
    pub const ALL: Self = Self(0xffff_ffff);
}

/// `alert` — payload enum (each variant is one `*_alert` struct).
#[derive(Clone, Debug)]
pub enum Alert {
    /// `torrent_added_alert`.
    TorrentAdded(Sha1Hash),
    /// `torrent_removed_alert`.
    TorrentRemoved(Sha1Hash),
    /// `torrent_finished_alert`.
    TorrentFinished(Sha1Hash),
    /// `torrent_checked_alert` — files_checked/recheck done.
    TorrentChecked(Sha1Hash),
    /// `state_changed_alert` — paused→downloading etc.
    StateChanged {
        ih: Sha1Hash,
        prev: String,
        next: String,
    },
    /// `piece_finished_alert`.
    PieceFinished { ih: Sha1Hash, piece: PieceIndex },
    /// `block_finished_alert`.
    BlockFinished {
        ih: Sha1Hash,
        piece: PieceIndex,
        block: u32,
    },
    /// `peer_disconnected_alert`.
    PeerDisconnected { ih: Sha1Hash, reason: String },
    /// `peer_connect_alert`.
    PeerConnect { ih: Sha1Hash, remote: String },
    /// `peer_ban_alert` — `torrent_handle::ban_peer` /
    /// `ip_filter` dropped a peer.
    PeerBan {
        /// `peer_ban_alert::ih`.
        ih: Sha1Hash,
        /// `peer_ban_alert::ip` — the banned endpoint.
        ip: std::net::SocketAddr,
    },
    /// `hash_failed_alert` — a piece failed verification.
    HashFailed { ih: Sha1Hash, piece: PieceIndex },
    /// `tracker_error_alert` / `tracker_reply_alert`.
    TrackerError {
        ih: Sha1Hash,
        url: String,
        error: String,
    },
    /// `tracker_reply_alert`.
    TrackerReply {
        ih: Sha1Hash,
        url: String,
        num_peers: usize,
    },
    /// `scrape_reply_alert`.
    ScrapeReply {
        ih: Sha1Hash,
        url: String,
        complete: i64,
        incomplete: i64,
    },
    /// `scrape_failed_alert`.
    ScrapeFailed {
        ih: Sha1Hash,
        url: String,
        reason: String,
    },
    /// `file_error_alert` — disk error on a file.
    FileError {
        ih: Sha1Hash,
        path: String,
        error: String,
    },
    /// `dht_pkt_alert` — a DHT datagram (direction+payload len).
    DhtPacket { direction: u8, size: usize },
    /// `performance_alert` — disk queue / buffer pressure.
    Performance { warning: String },
    /// `save_resume_data_alert` — blob ready to persist.
    SaveResumeData { ih: Sha1Hash, data: Vec<u8> },
    /// `state_update_alert` — `post_torrent_updates` payload:
    /// a `torrent_status` snapshot per torrent.
    StateUpdate {
        /// statuses.
        statuses: Vec<crate::status::TorrentStatus>,
    },
    /// `storage_moved_alert`.
    StorageMoved { ih: Sha1Hash, path: String },
    /// `storage_moved_failed_alert`.
    StorageMovedFailed {
        ih: Sha1Hash,
        path: String,
        error: String,
    },
    /// `file_renamed_alert`.
    FileRenamed {
        ih: Sha1Hash,
        index: i32,
        name: String,
    },
    /// `file_rename_failed_alert`.
    FileRenameFailed {
        ih: Sha1Hash,
        index: i32,
        error: String,
    },
    /// `torrent_deleted_alert`.
    TorrentDeleted(Sha1Hash),
    /// `torrent_delete_failed_alert`.
    TorrentDeleteFailed { ih: Sha1Hash, error: String },
    /// `read_piece_alert` — the piece payload (deadline/
    /// `alert_when_available` reads).
    ReadPiece {
        ih: Sha1Hash,
        piece: i32,
        buffer: Vec<u8>,
    },
    /// `read_piece_alert` failure form (`ec` set upstream).
    ReadPieceFailed {
        ih: Sha1Hash,
        piece: i32,
        error: String,
    },
    /// `cache_flushed_alert`.
    CacheFlushed(Sha1Hash),
    /// `session_error_alert`.
    SessionError(String),
    /// `dht_stats_alert` — routing-table snapshot on
    /// `session_handle::post_dht_stats`.
    DhtStats {
        /// v4 table `(nodes, replacements, confirmed)`.
        v4: (usize, usize, usize),
        /// v6 table `(nodes, replacements, confirmed)`.
        v6: (usize, usize, usize),
    },
}

impl Alert {
    /// `alert::category()`.
    #[must_use]
    pub fn category(&self) -> AlertCategory {
        match self {
            Self::TorrentAdded(_)
            | Self::TorrentRemoved(_)
            | Self::StateChanged { .. }
            | Self::StateUpdate { .. } => AlertCategory::STATUS,
            Self::TorrentFinished(_) | Self::PieceFinished { .. } | Self::BlockFinished { .. } => {
                AlertCategory::PROGRESS
            }
            Self::TorrentChecked(_) => AlertCategory::STATUS, // upstream: status_notification
            Self::PeerDisconnected { .. } | Self::PeerConnect { .. } => AlertCategory::PEER,
            Self::PeerBan { .. } => AlertCategory::PEER,
            Self::HashFailed { .. }
            | Self::FileError { .. }
            | Self::StorageMovedFailed { .. }
            | Self::FileRenameFailed { .. }
            | Self::TorrentDeleteFailed { .. } => AlertCategory::ERROR.or(AlertCategory::STORAGE),
            Self::StorageMoved { .. } | Self::FileRenamed { .. } => AlertCategory::STORAGE,
            Self::ReadPiece { .. } => AlertCategory::PROGRESS,
            Self::ReadPieceFailed { .. } => AlertCategory::ERROR,
            Self::TorrentDeleted(_) | Self::CacheFlushed(_) => {
                AlertCategory::STATUS.or(AlertCategory::STORAGE)
            }
            Self::TrackerError { .. }
            | Self::TrackerReply { .. }
            | Self::ScrapeReply { .. }
            | Self::ScrapeFailed { .. } => AlertCategory::TRACKER,
            Self::DhtPacket { .. } | Self::DhtStats { .. } => AlertCategory::DHT,
            Self::Performance { .. } => AlertCategory::PERFORMANCE,
            Self::SaveResumeData { .. } => AlertCategory::STORAGE,
            Self::SessionError(_) => AlertCategory::ERROR,
        }
    }
}

impl AlertCategory {
    /// `category & category` bit-or (upstream categories compose).
    #[must_use]
    pub const fn or(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
    /// `should_notify` — mask test.
    #[must_use]
    pub const fn matches(self, mask: Self) -> bool {
        self.0 & mask.0 != 0
    }
}

/// `alert_manager` — the double-buffered queue + mask + notify
/// callback (upstream uses a condition variable; the posted
/// `notify` hook wakes the owning actor instead).
pub struct AlertManager {
    /// `m_alert_mask`.
    mask: AlertCategory,
    /// `m_queue_size_limit`.
    queue_limit: usize,
    /// `m_alerts[m_generation & 1]` — the pending queue.
    pending: Vec<Alert>,
    /// `m_notify` — invoked when an empty queue becomes non-empty.
    notify: Option<Box<dyn Fn() + Send>>,
    /// dropped alerts when the queue overflows (`m_num_dropped`).
    dropped: usize,
}

impl Default for AlertManager {
    /// Upstream defaults: `m_alert_mask = all_categories`,
    /// `alert_queue_size = 2000`.
    fn default() -> Self {
        Self {
            mask: AlertCategory::ALL,
            queue_limit: 2000,
            pending: Vec::new(),
            notify: None,
            dropped: 0,
        }
    }
}

impl AlertManager {
    /// `alert_manager(queue_limit, mask)`.
    #[must_use]
    pub fn new(queue_limit: usize, mask: AlertCategory) -> Self {
        Self {
            mask,
            queue_limit,
            ..Default::default()
        }
    }

    /// `settings_pack::alert_queue_size` — hot-applied by
    /// `apply_settings` (upstream `update_alert_queue_size`).
    pub fn set_queue_limit(&mut self, limit: usize) {
        self.queue_limit = limit;
    }

    /// `set_alert_mask` — which categories get queued.
    pub fn set_mask(&mut self, mask: AlertCategory) {
        self.mask = mask;
    }

    /// `alert_manager::alert_mask`.
    #[must_use]
    pub fn mask(&self) -> AlertCategory {
        self.mask
    }

    /// `set_notify_callback`.
    pub fn set_notify(&mut self, cb: impl Fn() + Send + 'static) {
        self.notify = Some(Box::new(cb));
    }

    /// `emplace_alert` / `should_post_alert` + queue-limit drop.
    pub fn post(&mut self, a: Alert) {
        if !a.category().matches(self.mask) {
            return;
        }
        if self.pending.len() >= self.queue_limit {
            self.dropped += 1;
            return;
        }
        let was_empty = self.pending.is_empty();
        self.pending.push(a);
        if was_empty {
            if let Some(cb) = &self.notify {
                cb();
            }
        }
    }

    /// `get_all` — swap the queue out for the caller.
    pub fn get_all(&mut self) -> Vec<Alert> {
        std::mem::take(&mut self.pending)
    }

    /// `alerts_dropped`.
    #[must_use]
    pub fn num_dropped(&self) -> usize {
        self.dropped
    }

    /// `pending()`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// `plugin`/`ses_extension` — session-level extension point.
/// OnionBit attaches IPv8/peer-class plugins here.
pub trait SessionPlugin: Send + Sync {
    /// `plugin::on_alert` — every posted alert fans out here.
    fn on_alert(&self, _a: &Alert) {}
}
