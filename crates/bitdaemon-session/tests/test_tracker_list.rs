// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `tracker_list` — quadratic backoff (`5 + f²·5·r/100`, max 60m)
//! and tier-ordered failover; `alerts` — mask + queue limit.

use std::time::{Duration, Instant};

use bitdaemon_session::alerts::{Alert, AlertCategory, AlertManager};
use bitdaemon_session::tracker_list::{AnnounceEntry, RETRY_MAX, TrackerList};

#[test]
fn quadratic_backoff_matches_upstream() {
    let mut e = AnnounceEntry::new("udp://t:80", 0);
    let now = Instant::now();
    // upstream comment: fails=1..  delay = 5 + f²·5·250/100 → 17.5, 55, ...
    // (ratio 250 → multiplier f²·12.5; upstream values 7,15,27 use ratio 50)
    e.failed(50, Duration::ZERO, now); // f=1 → 5+5*0.5 = 7.5s
    let t = e.next_announce().unwrap() - now;
    assert_eq!(t, Duration::from_millis(7500));
    e.failed(50, Duration::ZERO, now); // f=2 → 5+4*5*0.5 = 15s
    let t = e.next_announce().unwrap() - now;
    assert_eq!(t, Duration::from_secs(15));
    e.failed(50, Duration::ZERO, now); // f=3 → 5+9*5*0.5 = 27.5s
    let t = e.next_announce().unwrap() - now;
    assert_eq!(t, Duration::from_millis(27_500));
}

#[test]
fn backoff_caps_at_60_minutes() {
    let mut e = AnnounceEntry::new("udp://t:80", 0);
    let now = Instant::now();
    for _ in 0..20 {
        e.failed(250, Duration::ZERO, now);
    }
    assert!(e.next_announce().unwrap() - now <= RETRY_MAX);
}

#[test]
fn tier_ordering_and_failover() {
    let mut l = TrackerList::default();
    l.add(AnnounceEntry::new("udp://b:80", 2));
    l.add(AnnounceEntry::new("udp://a:80", 0));
    l.add(AnnounceEntry::new("udp://a:80", 0)); // dedup
    assert_eq!(l.trackers.len(), 2);
    assert_eq!(l.trackers[0].url, "udp://a:80");
    // exhaust tier-0's fail_limit
    l.trackers[0].fail_limit = 1;
    l.trackers[0].failed(250, Duration::ZERO, Instant::now());
    let now = Instant::now() + Duration::from_secs(3600);
    let due = l.due(now, false);
    assert_eq!(due, vec![1]); // only tier-2 remains
}

#[test]
fn alert_mask_and_queue_limit() {
    // `peer_disconnected_alert` is CONNECT category upstream.
    let mut m = AlertManager::new(2, AlertCategory::CONNECT);
    m.post(Alert::SessionError("x".into())); // ERROR — masked off
    assert!(m.is_empty());
    for i in 0..4 {
        m.post(Alert::PeerDisconnected {
            ih: Default::default(),
            reason: i.to_string(),
        });
    }
    // 2 queued; the overflow drops are recorded per `alert_type` and
    // a synthesized `alerts_dropped_alert` rides `get_all`.
    let all = m.get_all();
    assert_eq!(all.len(), 3);
    assert!(matches!(
        all.last(),
        Some(Alert::AlertsDropped { types }) if types == &[24]
    ));
}

#[test]
fn category_zero_alerts_are_unconditional() {
    // upstream `emplace_alert` is unconditional — `SessionStats`/
    // `DhtStats`/`SessionStatsHeader` post even under `mask = 0`.
    let mut m = AlertManager::new(10, AlertCategory(0));
    m.post(Alert::SessionStatsHeader {});
    m.post(Alert::DhtStats {
        v4: (1, 2, 3),
        v6: (4, 5, 6),
    });
    m.post(Alert::PeerDisconnected {
        ih: Default::default(),
        reason: "x".into(),
    }); // CONNECT — masked
    assert_eq!(m.get_all().len(), 2);
}

#[test]
fn priority_gives_queue_headroom() {
    // upstream: `queue.size() / (1 + priority) >= limit` — a
    // critical alert has 3× the headroom of a normal one.
    let mut m = AlertManager::new(2, AlertCategory::ALL);
    // high (factor 2): posts 1–4 land (len/2 < 2), post 5 drops.
    for _ in 0..5 {
        m.post(Alert::TorrentFinished(Default::default()));
    }
    let all = m.get_all();
    assert_eq!(all.len(), 5); // 4 queued + synthesized AlertsDropped
    assert!(matches!(
        all.last(),
        Some(Alert::AlertsDropped { types }) if types == &[26]
    ));

    let mut m = AlertManager::new(1, AlertCategory::ALL);
    m.post(Alert::PieceFinished {
        ih: Default::default(),
        piece: Default::default(),
    }); // normal — len=1/1 >= 1 → next drops
    m.post(Alert::SaveResumeData {
        ih: Default::default(),
        data: vec![],
    }); // critical — 1/3 < 1 → lands
    assert_eq!(m.get_all().len(), 2);
}
