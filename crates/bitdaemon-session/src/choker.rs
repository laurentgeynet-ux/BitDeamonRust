// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `choker` — the unchoke sort. `unchoke_sort` upstream returns
//! the number of upload slots after reordering `peers` so the top
//! `slots` should be unchoked. Peers are described by
//! `ChokePeer` — a value snapshot, so the sort is pure and
//! testable (upstream sorts `peer_connection*`).

use std::time::{Duration, Instant};

/// `settings_pack::seed_choking_algorithm`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeedChokingAlgorithm {
    /// `round_robin` — quota per peer, de-prioritize when served.
    RoundRobin,
    /// `fastest_upload` — prefer the fastest uploader.
    FastestUpload,
    /// `anti_leech` — score by pieces the peer is missing.
    AntiLeech,
}

/// `settings_pack::choking_algorithm`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChokingAlgorithm {
    /// `fixed_slots` — `unchoke_slots_limit` slots.
    FixedSlots,
    /// `rate_based` — grow slots while rates exceed threshold.
    RateBased,
}

/// One peer's choker-visible state (`peer_connection` snapshot).
#[derive(Clone, Debug)]
pub struct ChokePeer {
    /// peer id (caller's key).
    pub id: u64,
    /// `is_choked`.
    pub choked: bool,
    /// `is_interesting` — peer wants pieces from us.
    pub interested: bool,
    /// `uploaded_in_last_round`.
    pub uploaded_last: i64,
    /// `downloaded_in_last_round`.
    pub downloaded_last: i64,
    /// `time_of_last_unchoke`.
    pub last_unchoke: Instant,
    /// `associated_torrent` seeding + piece length.
    pub seeding: bool,
    /// `num_have_pieces` — peer's piece count (anti-leech).
    pub num_pieces: i32,
    /// `total_payload_upload` — lifetime bytes sent (anti-leech).
    pub uploaded_total: i64,
    /// `torrent_file().total_size()`.
    pub total_size: i64,
    /// `piece_length` of its torrent.
    pub piece_length: i64,
    /// `get_priority(upload_channel)` — 1.0 scale.
    pub priority: f64,
}

/// `seeding_piece_quota` default.
pub const DEFAULT_SEEDING_PIECE_QUOTA: i64 = 3;
/// `rate_choker_initial_threshold` default (2 KiB/s).
pub const DEFAULT_RATE_THRESHOLD: i64 = 2048;

fn quota_complete(p: &ChokePeer, pieces: i64, now: Instant) -> bool {
    !p.choked
        && p.uploaded_last > p.piece_length * pieces
        && now.duration_since(p.last_unchoke) > Duration::from_secs(60)
}

/// `unchoke_compare_rr` — round-robin seed comparator.
fn rr_less(a: &ChokePeer, b: &ChokePeer, pieces: i64, now: Instant) -> std::cmp::Ordering {
    let c1 = quota_complete(a, pieces, now);
    let c2 = quota_complete(b, pieces, now);
    if c1 != c2 {
        return c1.cmp(&c2);
    }
    // choked peers' rate counts as 0 (residual in-flight ignored)
    let u1 = if a.choked { 0 } else { a.uploaded_last };
    let u2 = if b.choked { 0 } else { b.uploaded_last };
    if u1 != u2 {
        return u2.cmp(&u1);
    }
    // longest-waiting first (round-robin relies on this)
    a.last_unchoke.cmp(&b.last_unchoke)
}

/// `unchoke_compare_fastest_upload`.
fn fastest_less(a: &ChokePeer, b: &ChokePeer) -> std::cmp::Ordering {
    if a.uploaded_last != b.uploaded_last {
        return b.uploaded_last.cmp(&a.uploaded_last);
    }
    a.last_unchoke.cmp(&b.last_unchoke)
}

/// `anti_leech_score` — Chow et al. V-shape: high at both ends
/// (`|have_size - total/2| · 2000 / total`). `total_size` must be
/// carried on `ChokePeer` (upstream reads the torrent_file).
fn anti_leech_score(p: &ChokePeer) -> i64 {
    if p.total_size == 0 {
        return 0;
    }
    let given = p.uploaded_total.min(p.total_size / 2);
    let have = given.max(p.piece_length * i64::from(p.num_pieces));
    (have - p.total_size / 2).abs() * 2000 / p.total_size
}

/// `unchoke_compare_anti_leech`.
fn anti_leech_less(a: &ChokePeer, b: &ChokePeer) -> std::cmp::Ordering {
    let s1 = anti_leech_score(a) as f64 * a.priority;
    let s2 = anti_leech_score(b) as f64 * b.priority;
    if (s1 - s2).abs() > f64::EPSILON {
        return s2.partial_cmp(&s1).unwrap();
    }
    a.last_unchoke.cmp(&b.last_unchoke)
}

/// `unchoke_sort` — reorder `peers` so the first N should be
/// unchoked; returns N (`upload_slots`). `peers` must already be
/// filtered to interested, connectable peers.
pub fn unchoke_sort(
    peers: &mut [ChokePeer],
    slots_limit: i32,
    algorithm: ChokingAlgorithm,
    seed_algorithm: SeedChokingAlgorithm,
    unchoke_interval: Duration,
    seeding_piece_quota: i64,
    now: Instant,
) -> usize {
    let mut upload_slots = slots_limit;
    if algorithm == ChokingAlgorithm::RateBased {
        upload_slots = 0;
        let mut threshold = DEFAULT_RATE_THRESHOLD;
        peers.sort_by_key(|a| std::cmp::Reverse(a.uploaded_last));
        for p in peers.iter() {
            let rate = p.uploaded_last * 1000 / unchoke_interval.as_millis().max(1) as i64;
            if rate < threshold {
                break;
            }
            upload_slots += 1;
            threshold += 2048;
        }
        upload_slots += 1;
    }
    if upload_slots <= 0 {
        return 0;
    }
    let slots = (upload_slots as usize).min(peers.len());
    let cmp = |a: &ChokePeer, b: &ChokePeer| match seed_algorithm {
        SeedChokingAlgorithm::FastestUpload => fastest_less(a, b),
        SeedChokingAlgorithm::AntiLeech => anti_leech_less(a, b),
        SeedChokingAlgorithm::RoundRobin => rr_less(a, b, seeding_piece_quota, now),
    };
    // `nth_element` — only the top `slots` must be ordered
    peers.select_nth_unstable_by(slots.saturating_sub(1), cmp);
    peers[..slots].sort_by(cmp);
    slots
}
