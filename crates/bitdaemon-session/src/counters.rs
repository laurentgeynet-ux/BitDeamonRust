// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `performance_counters`/`session_stats` — session metrics.
//! Upstream keeps one `std::array<std::atomic<int64>>` indexed by
//! `stats_counter_t`; we port the counter enum subset the ported
//! machinery emits + a typed `SessionStats` snapshot.

use std::sync::atomic::{AtomicI64, Ordering};

/// `stats_counter_t` — the indices (subset of the ported layers).
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Counter {
    /// `num_peers_connected`.
    PeersConnected = 0,
    /// `num_peers_half_open`.
    PeersHalfOpen,
    /// `num_incoming_bitfield`.
    IncomingBitfield,
    /// `num_incoming_have`.
    IncomingHave,
    /// `num_incoming_request`.
    IncomingRequest,
    /// `num_incoming_piece`.
    IncomingPiece,
    /// `num_incoming_cancel`.
    IncomingCancel,
    /// `num_incoming_choke`/`unchoke`/`interested`/`not`.
    IncomingChoke,
    /// see above.
    IncomingUnchoke,
    /// see above.
    IncomingInterested,
    /// see above.
    IncomingNotInterested,
    /// `num_outgoing_bitfield`/have/request/piece.
    OutgoingBitfield,
    /// see above.
    OutgoingHave,
    /// see above.
    OutgoingRequest,
    /// see above.
    OutgoingPiece,
    /// `recv_piece_bytes`/`sent_piece_bytes`.
    PieceBytesRecv,
    /// see above.
    PieceBytesSent,
    /// `recv_failed_bytes` — payload wasted (bad pieces).
    FailedBytes,
    /// `recv_redundant_bytes` — already-had blocks.
    RedundantBytes,
    /// `disk.write_queue_size`/`read_queue_size`.
    DiskWriteQueue,
    /// see above.
    DiskReadQueue,
    /// `disk.num_write_ops`/`num_read_ops`/`num_hash_jobs`.
    DiskWriteOps,
    /// see above.
    DiskReadOps,
    /// see above.
    DiskHashOps,
    /// `disk.block_write`/`block_read`/`block_hash` counters.
    DiskBlocksWrite,
    /// see above.
    DiskBlocksRead,
    /// `ses.num_checking_torrents`/`num_downloading`/`num_seeding`.
    CheckingTorrents,
    /// see above.
    DownloadingTorrents,
    /// see above.
    SeedingTorrents,
    /// `dht.dht_bytes_in`/`dht_bytes_out`/`dht_msgs_in`/`out`.
    DhtBytesIn,
    /// see above.
    DhtBytesOut,
    /// see above.
    DhtMsgsIn,
    /// see above.
    DhtMsgsOut,
    /// `num_error_peers`/`num_banned_peers`.
    ErrorPeers,
    /// see above.
    BannedPeers,
}

/// `num_counters`.
pub const NUM_COUNTERS: usize = 37;

/// `counters` — the atomic bank the session owns.
pub struct Counters {
    v: [AtomicI64; NUM_COUNTERS],
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            v: std::array::from_fn(|_| AtomicI64::new(0)),
        }
    }
}

impl Counters {
    /// `inc_stats_counter(c)` (+1).
    pub fn inc(&self, c: Counter) {
        self.add(c, 1);
    }
    /// `inc_stats_counter(c, n)`.
    pub fn add(&self, c: Counter, n: i64) {
        self.v[c as usize].fetch_add(n, Ordering::Relaxed);
    }
    /// `set_value(c, n)` — gauge counters (queue sizes).
    pub fn set(&self, c: Counter, n: i64) {
        self.v[c as usize].store(n, Ordering::Relaxed);
    }
    /// `counters[c]`.
    #[must_use]
    pub fn value(&self, c: Counter) -> i64 {
        self.v[c as usize].load(Ordering::Relaxed)
    }
}

/// `session_stats` — the snapshot posted by `post_session_stats`.
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    /// `values[num_counters]` snapshot, index = `Counter as usize`.
    pub values: Vec<i64>,
}

impl SessionStats {
    /// `session_stats` — snapshot the bank.
    #[must_use]
    pub fn snapshot(c: &Counters) -> Self {
        Self {
            values: (0..NUM_COUNTERS)
                .map(|i| c.v[i].load(Ordering::Relaxed))
                .collect(),
        }
    }

    /// `stats_metric` lookup.
    #[must_use]
    pub fn get(&self, c: Counter) -> i64 {
        self.values.get(c as usize).copied().unwrap_or(0)
    }
}
