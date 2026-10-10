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
    let mut m = AlertManager::new(2, AlertCategory::PEER);
    m.post(Alert::SessionError("x".into())); // filtered out
    assert!(m.is_empty());
    for i in 0..4 {
        m.post(Alert::PeerDisconnected {
            ih: Default::default(),
            reason: i.to_string(),
        });
    }
    assert_eq!(m.get_all().len(), 2);
    assert_eq!(m.num_dropped(), 2);
}
