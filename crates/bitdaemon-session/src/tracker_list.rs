// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `announce_entry` + `tracker_list` — tracker retry/failover.
//! Ports `announce_infohash::failed`'s quadratic backoff
//! (`fail² × 5s × backoff/100`, capped 60 min) and the
//! tier-ordered endpoint selection of `tracker_list`.

use std::time::{Duration, Instant};

/// `tracker_retry_delay_min`.
pub const RETRY_MIN: Duration = Duration::from_secs(5);
/// `tracker_retry_delay_max`.
pub const RETRY_MAX: Duration = Duration::from_secs(60 * 60);

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
}

impl AnnounceInfohash {
    /// `is_working`.
    #[must_use]
    pub fn is_working(&self) -> bool {
        self.fails == 0
    }

    /// `announce_infohash::failed` — quadratic backoff:
    /// `max(retry_interval, min(60min, 5s + fails²·5s·ratio/100))`.
    pub fn failed(&mut self, backoff_ratio: i32, retry_interval: Duration, now: Instant) {
        if self.fails < 127 {
            self.fails += 1;
        }
        let sq = i64::from(self.fails) * i64::from(self.fails);
        let delay = retry_interval
            .max(RETRY_MIN.mul_f64(1.0 + (sq * i64::from(backoff_ratio)) as f64 / 100.0));
        let delay = delay.min(RETRY_MAX);
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

    /// `announce_infohash::reset`.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// `announce_endpoint::sent` — successful response bookkeeping:
    /// clears `updating`/`fails`, schedules `next_announce` at
    /// `now + interval`, `min_announce` at `now + min_interval`,
    /// and records the lifecycle event actually delivered
    /// (`started`/`completed`) so it is never sent twice.
    pub fn sent(&mut self, interval: Duration, min_interval: Option<Duration>, now: Instant) {
        self.updating = false;
        self.fails = 0;
        self.next_announce = Some(now + interval);
        if let Some(m) = min_interval {
            self.min_announce = Some(now + m);
        }
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
    pub fn sent(&mut self, interval: Duration, min_interval: Option<Duration>, now: Instant) {
        for e in &mut self.endpoints {
            e.v1.sent(interval, min_interval, now);
            // the v2 slot was held in-flight too — it shares the
            // schedule (upstream announces each info-hash
            // separately; our single exchange stands in for both).
            e.v2.updating = false;
            e.v2.fails = 0;
            e.v2.next_announce = Some(now + interval);
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
}

impl TrackerList {
    /// `add_tracker` — insert keeping tier order, dedup by url.
    pub fn add(&mut self, e: AnnounceEntry) {
        if self.trackers.iter().any(|t| t.url == e.url) {
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
    #[must_use]
    pub fn next_announce_time(&self) -> Option<Instant> {
        self.trackers.iter().filter_map(|t| t.next_announce()).min()
    }

    /// `find_tracker`.
    #[must_use]
    pub fn find(&self, url: &str) -> Option<usize> {
        self.trackers.iter().position(|t| t.url == url)
    }

    /// `due_trackers` — entries whose `can_announce` fires, in
    /// tier order (failover = first working tracker wins).
    #[must_use]
    pub fn due(&self, now: Instant, is_seed: bool) -> Vec<usize> {
        self.trackers
            .iter()
            .enumerate()
            .filter(|(_, t)| t.can_announce(now, is_seed))
            .map(|(i, _)| i)
            .collect()
    }
}
