// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `choker.cpp`/`choker.hpp`.
//!
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
    /// `uploaded_since_unchoked` — payload bytes since the last
    /// unchoke transition (`stat_up_total - uploaded_at_last_unchoke`).
    pub uploaded_since_unchoked: i64,
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
    /// `get_priority(upload_channel)` — peer-class priority, int.
    pub priority: i32,
}

/// `compare_peers` — the shared head of every `unchoke_compare_*`:
/// upload-channel priority first, then `downloaded_in_last_round`
/// (higher is preferred in both).
fn compare_peers(a: &ChokePeer, b: &ChokePeer) -> std::cmp::Ordering {
    if a.priority != b.priority {
        return b.priority.cmp(&a.priority);
    }
    if a.downloaded_last != b.downloaded_last {
        return b.downloaded_last.cmp(&a.downloaded_last);
    }
    std::cmp::Ordering::Equal
}

/// quota check from `unchoke_compare_rr` — an unchoked peer that
/// already served `seeding_piece_quota` pieces worth since its
/// unchoke, and stayed unchoked over a minute, yields its slot.
fn quota_complete(p: &ChokePeer, pieces: i64, now: Instant) -> bool {
    !p.choked
        && p.uploaded_since_unchoked > p.piece_length * pieces
        && now.duration_since(p.last_unchoke) > Duration::from_secs(60)
}

/// `unchoke_compare_rr` — round-robin seed comparator.
fn rr_less(a: &ChokePeer, b: &ChokePeer, pieces: i64, now: Instant) -> std::cmp::Ordering {
    let cmp = compare_peers(a, b);
    if cmp != std::cmp::Ordering::Equal {
        return cmp;
    }
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
    let cmp = compare_peers(a, b);
    if cmp != std::cmp::Ordering::Equal {
        return cmp;
    }
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
    let cmp = compare_peers(a, b);
    if cmp != std::cmp::Ordering::Equal {
        return cmp;
    }
    let s1 = anti_leech_score(a);
    let s2 = anti_leech_score(b);
    if s1 != s2 {
        return s2.cmp(&s1);
    }
    a.last_unchoke.cmp(&b.last_unchoke)
}

/// The choker's view of `session_settings` (upstream passes
/// `aux::session_settings const&` into `unchoke_sort`).
#[derive(Clone, Copy, Debug)]
pub struct ChokerSettings {
    /// `unchoke_slots_limit`.
    pub slots_limit: i32,
    /// `choking_algorithm`.
    pub algorithm: ChokingAlgorithm,
    /// `seed_choking_algorithm`.
    pub seed_algorithm: SeedChokingAlgorithm,
    /// `seeding_piece_quota`.
    pub seeding_piece_quota: i64,
    /// `rate_choker_initial_threshold`.
    pub rate_threshold: i64,
}

/// `unchoke_sort` — reorder `peers` so the first N should be
/// unchoked; returns `upload_slots` — the *uncapped* slot count
/// (upstream `return upload_slots`, not `slots`): the caller
/// derives `num_opt_unchoke` and `unchoke_set_size` from it, so
/// capping at `peers.len()` would starve real unchokes when the
/// eligible set is smaller than the quota.
/// `peers` must already be filtered to interested, connectable peers.
pub fn unchoke_sort(
    peers: &mut [ChokePeer],
    unchoke_interval: Duration,
    sett: &ChokerSettings,
    now: Instant,
) -> usize {
    let mut upload_slots = i64::from(sett.slots_limit);
    if upload_slots < 0 {
        // `unchoke_slots_limit < 0` — unlimited upstream
        upload_slots = i64::from(i32::MAX);
    }
    if sett.algorithm == ChokingAlgorithm::RateBased {
        // the rate-based choker grows slots while the sorted
        // upload rates clear a threshold rising by 2 kiB/s each
        upload_slots = 0;
        let mut threshold = sett.rate_threshold;
        // `upload_rate_compare` — rate × priority, descending
        peers.sort_by_key(|p| std::cmp::Reverse(p.uploaded_last * i64::from(p.priority)));
        for p in peers.iter() {
            let rate = p.uploaded_last * 1000 / unchoke_interval.as_millis().max(1) as i64;
            // always have at least 1 unchoke slot
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
    if slots == 0 {
        return 0;
    }
    // upstream returns `upload_slots` — `slots` only bounds the
    // `nth_element` partition below.
    let ret = upload_slots as usize;
    let cmp = |a: &ChokePeer, b: &ChokePeer| match sett.seed_algorithm {
        SeedChokingAlgorithm::FastestUpload => fastest_less(a, b),
        SeedChokingAlgorithm::AntiLeech => anti_leech_less(a, b),
        SeedChokingAlgorithm::RoundRobin => rr_less(a, b, sett.seeding_piece_quota, now),
    };
    // `nth_element` — only the top `slots` must be ordered
    peers.select_nth_unstable_by(slots.saturating_sub(1), cmp);
    ret
}
