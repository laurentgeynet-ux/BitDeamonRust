// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `announce_entry.cpp`/`announce_entry.hpp` +
//! `tracker_list.cpp`/`tracker_list.hpp`.
//!
//! `announce_entry` + `tracker_list` — tracker retry/failover.
//! Ports `announce_infohash::failed`'s quadratic backoff
//! (`fail² × 5s × backoff/100`, capped 60 min) and the
//! tier-ordered endpoint selection of `tracker_list`.

use std::time::{Duration, Instant};

/// `tracker_retry_delay_min`.
pub const RETRY_MIN: Duration = Duration::from_secs(5);
/// `tracker_retry_delay_max`.
pub const RETRY_MAX: Duration = Duration::from_secs(60 * 60);

/// `is_tracker_protocol_supported` — can this build dispatch the URL
/// scheme of a tracker announce? Upstream gates it on build flags
/// (`http` always, `https` under TORRENT_USE_SSL, `udp`, `ws`/`wss`
/// under TORRENT_USE_RTC); this port always has ssl+ws. Shared by the
/// announce selection (`due`), the timer (`next_announce_time`),
/// `force_announce` and `scrape` so they can never disagree about
/// which trackers are dispatchable — a tracker that is always skipped
/// must also stay pristine (`fails == 0`, no `last_error`) and must
/// not pull the announce timer back to "now" (upstream `e0716cf24`).
#[must_use]
pub fn tracker_protocol_supported(url: &str) -> bool {
    let proto = url.split(':').next().unwrap_or("");
    matches!(proto, "http" | "https" | "udp" | "ws" | "wss")
}

/// `tracker_source::source_torrent` — came from the .torrent file.
pub const SOURCE_TORRENT: u8 = 1;
/// `tracker_source::source_client` — added via `add_tracker`.
pub const SOURCE_CLIENT: u8 = 2;
/// `tracker_source::source_magnet_link` — a magnet `tr=` param.
pub const SOURCE_MAGNET: u8 = 4;
/// `tracker_source::source_tex` — tracker exchange.
pub const SOURCE_TEX: u8 = 8;

/// `announce_infohash` — per-info-hash announce state on one
/// endpoint. `fails` saturates at 7 bits like upstream.
#[derive(Clone, Debug, Default)]
pub struct AnnounceInfohash {
    /// `next_announce` — earliest next attempt.
    pub next_announce: Option<Instant>,
    /// `min_announce` — tracker-provided minimum.
    pub min_announce: Option<Instant>,
    /// `fails` — consecutive failures (7-bit saturate).
    pub fails: u8,
    /// `updating` — a request is in flight.
    pub updating: bool,
    /// `complete_sent` — sent the `completed` event.
    pub complete_sent: bool,
    /// `start_sent`.
    pub start_sent: bool,
    /// the `event` value of the in-flight request — carried to the
    /// response so `sent()` can mark `start_sent`/`complete_sent`
    /// (upstream `tracker_connection` remembers its request).
    pub sent_event: i32,
    /// `triggered_manually` — set by `trigger_announce`
    /// (`force_reannounce`); consumed into `sent_manual` when the
    /// request goes out.
    pub triggered_manually: bool,
    /// `tracker_request::triggered_manually` — the in-flight copy;
    /// makes `tracker_reply`/`scrape_reply` alerts unconditional.
    pub sent_manual: bool,
}

impl AnnounceInfohash {
    /// `is_working`.
    #[must_use]
    pub fn is_working(&self) -> bool {
        self.fails == 0
    }

    /// `announce_infohash::failed` — quadratic backoff:
    /// `max(retry_interval, min(60min, 5s + fails²·5s·ratio/100))`.
    /// The 60min cap applies to the backoff term only — a larger
    /// `retry_interval` is honored verbatim.
    pub fn failed(&mut self, backoff_ratio: i32, retry_interval: Duration, now: Instant) {
        if self.fails < 127 {
            self.fails += 1;
        }
        let sq = i64::from(self.fails) * i64::from(self.fails);
        let backoff_secs =
            (5 + sq * 5 * i64::from(backoff_ratio) / 100).min(RETRY_MAX.as_secs() as i64);
        let delay = retry_interval.max(Duration::from_secs(backoff_secs.max(0) as u64));
        if !self.is_working() {
            self.next_announce = Some(now + delay);
        }
        self.updating = false;
    }

    /// `can_announce` — due + not min-blocked + under fail limit.
    #[must_use]
    pub fn can_announce(&self, now: Instant, is_seed: bool, fail_limit: u8) -> bool {
        let need_complete = is_seed && !self.complete_sent;
        let due = self
            .next_announce
            .map(|t| now + Duration::from_secs(1) >= t)
            .unwrap_or(true);
        let min_ok = self.min_announce.map(|t| now >= t).unwrap_or(true) || need_complete;
        due && min_ok && (self.fails < fail_limit || fail_limit == 0) && !self.updating
    }

    /// `announce_infohash::reset` — clears `start_sent` and both
    /// timers; `fails`/`updating`/`complete_sent` are preserved.
    pub fn reset(&mut self) {
        self.start_sent = false;
        self.next_announce = None;
        self.min_announce = None;
    }

    /// `announce_endpoint::sent` — successful response bookkeeping:
    /// clears `updating`/`fails`, schedules `next_announce` at
    /// `now + interval`, `min_announce` at `now + min_interval`
    /// (upstream assigns `min_announce` unconditionally — an absent
    /// `min interval` parses as 30s/60s, clearing any earlier one),
    /// and records the lifecycle event actually delivered
    /// (`started`/`completed`) so it is never sent twice.
    pub fn sent(&mut self, interval: Duration, min_interval: Duration, now: Instant) {
        self.updating = false;
        self.fails = 0;
        self.next_announce = Some(now + interval);
        self.min_announce = Some(now + min_interval);
        match self.sent_event {
            1 => self.complete_sent = true,
            2 => self.start_sent = true,
            _ => {}
        }
        self.sent_event = 0;
    }
}

/// `announce_endpoint` — one listen-side endpoint of a tracker.
#[derive(Clone, Debug, Default)]
pub struct AnnounceEndpoint {
    /// `info_hashes` (v1 only in this port — v2 is a second slot).
    pub v1: AnnounceInfohash,
    /// `v2` — hybrid torrents announce both.
    pub v2: AnnounceInfohash,
    /// `enabled` — `enabled` flag per endpoint.
    pub enabled: bool,
    /// `message` — upstream `announce_endpoint::last_error.message()`:
    /// texte de la derniere erreur d'announce (vide au succes). La
    /// `torrent_status::error` ne porte que les erreurs fatales
    /// (disque, cert) — une erreur tracker transitoire ne doit pas
    /// y remonter sinon le torrent apparait `stopped_on_error`.
    pub message: String,
}

impl AnnounceEndpoint {
    /// `can_announce` — either version slot is due.
    #[must_use]
    pub fn can_announce(&self, now: Instant, is_seed: bool, fail_limit: u8) -> bool {
        self.enabled
            && (self.v1.can_announce(now, is_seed, fail_limit)
                || self.v2.can_announce(now, is_seed, fail_limit))
    }
    /// `next_announce` — earliest slot.
    #[must_use]
    pub fn next_announce(&self) -> Option<Instant> {
        [self.v1.next_announce, self.v2.next_announce]
            .into_iter()
            .flatten()
            .min()
    }

    /// `trigger_announce` — a client `force_reannounce` schedules
    /// the entry at `t` (clamped to `min_announce`, +1s like
    /// upstream) and marks the request manual so its reply posts
    /// unconditionally.
    /// `trigger_announce` per endpoint — `next_announce` becomes
    /// `max(t, min_announce) + 1s`, or `t + 1s` under
    /// `ignore_min_interval` (which also clears `min_announce`
    /// like upstream's `time_point32::min`).
    fn trigger(&mut self, t: Instant, ignore_min: bool) {
        for slot in [&mut self.v1, &mut self.v2] {
            slot.next_announce = Some(
                if ignore_min {
                    t
                } else {
                    slot.min_announce.map_or(t, |m| m.max(t))
                } + Duration::from_secs(1),
            );
            if ignore_min {
                slot.min_announce = None;
            }
            slot.triggered_manually = true;
        }
    }
}

/// `trigger_announce` entry-point helper — schedules every
/// enabled endpoint of an `AnnounceEntry` at `t`.
fn trigger_entry(e: &mut AnnounceEntry, t: Instant, ignore_min: bool) {
    for ep in e.endpoints.iter_mut().filter(|ep| ep.enabled) {
        ep.trigger(t, ignore_min);
    }
}

/// `announce_entry` — one tracker URL and its endpoints.
#[derive(Clone, Debug)]
pub struct AnnounceEntry {
    /// `url`.
    pub url: String,
    /// `trackerid` — the `trackerid` from the last response.
    pub tracker_id: String,
    /// `tier` — lower = tried first.
    pub tier: i32,
    /// `fail_limit` — per-tracker max retries (0 = infinite).
    pub fail_limit: u8,
    /// `endpoints` — one per listen socket.
    pub endpoints: Vec<AnnounceEndpoint>,
    /// `verified` — a working response came back once.
    pub verified: bool,
    /// `source` bitmask — where we heard about this tracker
    /// (`SOURCE_*`; dedup merges bits, `0` normalizes to
    /// `SOURCE_CLIENT` like upstream's `announce_entry` ctor).
    pub source: u8,
    /// scrape results (`complete`/`incomplete`/`downloaded` from
    /// the last scrape response; -1 = never scraped).
    pub scrape_complete: i64,
    /// leechers from the last scrape.
    pub scrape_incomplete: i64,
    /// completed downloads from the last scrape.
    pub scrape_downloaded: i64,
    /// `last_scrape` timestamp (upstream `torrent_status::last_scrape`
    /// = now - this).
    pub scrape_time: Option<Instant>,
}

impl AnnounceEntry {
    /// `announce_entry(url)`.
    #[must_use]
    pub fn new(url: impl Into<String>, tier: i32) -> Self {
        Self {
            url: url.into(),
            tracker_id: String::new(),
            tier,
            fail_limit: 0,
            endpoints: vec![AnnounceEndpoint {
                enabled: true,
                ..Default::default()
            }],
            verified: false,
            source: 0,
            scrape_complete: -1,
            scrape_incomplete: -1,
            scrape_downloaded: -1,
            scrape_time: None,
        }
    }

    /// `can_announce` — any endpoint due.
    #[must_use]
    pub fn can_announce(&self, now: Instant, is_seed: bool) -> bool {
        self.endpoints
            .iter()
            .any(|e| e.can_announce(now, is_seed, self.fail_limit))
    }

    /// `next_announce` — earliest endpoint time.
    #[must_use]
    pub fn next_announce(&self) -> Option<Instant> {
        self.endpoints
            .iter()
            .filter_map(|e| e.next_announce())
            .min()
    }

    /// `announce_entry::failed` — mark all endpoint slots failed.
    pub fn failed(&mut self, backoff_ratio: i32, retry_interval: Duration, now: Instant) {
        for e in &mut self.endpoints {
            e.v1.failed(backoff_ratio, retry_interval, now);
            e.v2.failed(backoff_ratio, retry_interval, now);
        }
    }

    /// `sent` on every endpoint slot — the entry's announce covers
    /// all slots in this port (one exchange per tracker URL).
    pub fn sent(&mut self, interval: Duration, min_interval: Duration, now: Instant) {
        for e in &mut self.endpoints {
            let ev = e.v1.sent_event;
            e.v1.sent(interval, min_interval, now);
            // the v2 slot was held in-flight too — it shares the
            // schedule and the delivered-event flags (upstream
            // announces each info-hash separately; our single
            // exchange stands in for both).
            e.v2.updating = false;
            e.v2.fails = 0;
            e.v2.next_announce = Some(now + interval);
            e.v2.min_announce = Some(now + min_interval);
            if ev == 1 {
                e.v2.complete_sent = true;
            }
            if ev == 2 {
                e.v2.start_sent = true;
            }
        }
    }

    /// `announce_endpoint::last_error` — mémorise/efface le message
    /// d'erreur de l'endpoint (mirroir des champs `v1`/`v2`).
    pub fn set_message(&mut self, msg: &str) {
        for e in &mut self.endpoints {
            e.message = msg.to_string();
        }
    }
}

/// `tracker_list` — the tier-ordered tracker set for a torrent.
#[derive(Clone, Debug, Default)]
pub struct TrackerList {
    /// `m_trackers` — always kept sorted by tier.
    pub trackers: Vec<AnnounceEntry>,
    /// `m_last_working_tracker` — url of the most recent tracker to
    /// answer an announce (`record_working`; feeds
    /// `torrent_status::current_tracker`). Empty = none yet.
    pub last_working_url: String,
}

impl TrackerList {
    /// `add_tracker` — insert keeping tier order, dedup by url;
    /// a dup merges `source` bits into the existing entry.
    pub fn add(&mut self, e: AnnounceEntry) {
        let mut e = e;
        if e.source == 0 {
            e.source = SOURCE_CLIENT;
        }
        if let Some(k) = self.trackers.iter_mut().find(|t| t.url == e.url) {
            k.source |= e.source;
            return;
        }
        let pos = self
            .trackers
            .iter()
            .position(|t| t.tier > e.tier)
            .unwrap_or(self.trackers.len());
        self.trackers.insert(pos, e);
    }

    /// `next_announce` — the first tracker (lowest tier) that is
    /// due gets announced; upstream iterates and picks the min.
    ///
    /// Upstream `update_tracker_timer` skips trackers
    /// `announce_with_tracker` will skip too — an always-skipped
    /// tracker's `next_announce` must not drive the timer to "now",
    /// or every fire selects nothing and the pump spins (`e0716cf24`).
    #[must_use]
    pub fn next_announce_time(&self) -> Option<Instant> {
        self.trackers
            .iter()
            .filter(|t| tracker_protocol_supported(&t.url))
            .filter_map(|t| t.next_announce())
            .min()
    }

    /// `find_tracker`.
    #[must_use]
    pub fn find(&self, url: &str) -> Option<usize> {
        self.trackers.iter().position(|t| t.url == url)
    }

    /// `announce_with_tracker`'s selection — the upstream
    /// `announce_state` machine per listen socket (we have one
    /// logical endpoint, driven by the v1 info-hash slot):
    ///
    /// - a *working* entry lowers `state.tier` and resets
    ///   `sent_announce` — lowest-tier working tracker always wins;
    /// - a working-but-not-due entry counts as covered
    ///   (`sent_announce = true`);
    /// - `announce_to_all_tiers` → at most one announce per tier;
    /// - neither flag → stop after the first working announce
    ///   (`state.done`).
    #[must_use]
    pub fn due(
        &self,
        now: Instant,
        is_seed: bool,
        to_all_tiers: bool,
        to_all_trackers: bool,
    ) -> Vec<usize> {
        let mut picked = Vec::new();
        let mut state_tier = i32::MAX;
        let mut sent_announce = false;
        let mut done = false;
        for (i, ae) in self.trackers.iter().enumerate() {
            if done {
                break;
            }
            // upstream `announce_with_tracker`: schemes we cannot
            // dispatch are skipped, never attempted — they stay
            // pristine (`fails == 0`, no error) (`e0716cf24`).
            if !tracker_protocol_supported(&ae.url) {
                continue;
            }
            for ep in ae.endpoints.iter().filter(|e| e.enabled) {
                let a = &ep.v1;
                if to_all_tiers
                    && !to_all_trackers
                    && sent_announce
                    && ae.tier <= state_tier
                    && state_tier != i32::MAX
                {
                    continue;
                }
                if ae.tier > state_tier && sent_announce && !to_all_tiers {
                    continue;
                }
                if a.is_working() {
                    state_tier = ae.tier;
                    sent_announce = false;
                }
                if !a.can_announce(now, is_seed, ae.fail_limit) {
                    if a.is_working() {
                        sent_announce = true;
                        if !to_all_trackers && !to_all_tiers {
                            done = true;
                        }
                    }
                    continue;
                }
                picked.push(i);
                sent_announce = true;
                if a.is_working() && !to_all_trackers && !to_all_tiers {
                    done = true;
                }
                // one exchange covers the whole entry (v2 slot
                // is held in-flight alongside v1)
                break;
            }
        }
        picked
    }

    /// `tracker_list::reset` — per-slot `reset()` on every entry:
    /// clears `start_sent`/`next_announce`/`min_announce` so the
    /// next announce re-sends `started` (used by
    /// `torrent::start_announcing` on resume).
    pub fn reset(&mut self) {
        for t in &mut self.trackers {
            for e in &mut t.endpoints {
                e.v1.reset();
                e.v2.reset();
            }
        }
    }

    /// `torrent::force_tracker_request` — schedule a manual
    /// announce for tracker `idx` (or all trackers for `None`) at
    /// time `t`; `min_announce` still applies (upstream only
    /// bypasses it under `ignore_min_interval`).
    /// Returns whether any entry was actually scheduled.
    pub fn trigger(&mut self, idx: Option<usize>, t: Instant, ignore_min: bool) -> bool {
        let mut found = false;
        match idx {
            Some(i) => {
                if let Some(e) = self.trackers.get_mut(i) {
                    trigger_entry(e, t, ignore_min);
                    found = true;
                }
            }
            None => {
                for e in &mut self.trackers {
                    trigger_entry(e, t, ignore_min);
                }
                found = !self.trackers.is_empty();
            }
        }
        found
    }

    /// `tracker_list::set_complete_sent` — latch `complete_sent` on
    /// every slot. `files_checked` does this for torrents that check
    /// out as seeds: the `completed` event is assumed sent, so
    /// `finished()`→`completed()` must not announce it again.
    pub fn set_complete_sent(&mut self) {
        for t in &mut self.trackers {
            for e in &mut t.endpoints {
                e.v1.complete_sent = true;
                e.v2.complete_sent = true;
            }
        }
    }

    /// `tracker_list::stop_announcing` — arms every slot at `now`
    /// (the `stopped` announce sent right after goes out
    /// immediately), and drops `updating` — upstream's in-flight
    /// announces are aborted by `session_impl::pause`'s
    /// `tracker_manager::abort_all_requests`.
    pub fn stop_announcing(&mut self, now: Instant) {
        for t in &mut self.trackers {
            for e in &mut t.endpoints {
                for a in [&mut e.v1, &mut e.v2] {
                    a.next_announce = Some(now);
                    a.min_announce = Some(now);
                    a.updating = false;
                }
            }
        }
    }
}
