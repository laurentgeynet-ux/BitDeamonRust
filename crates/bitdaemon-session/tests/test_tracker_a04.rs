// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! A04 apply regressions — per-(endpoint, info-hash version) announce
//! selection (`announce_with_tracker`), structured failure semantics
//! (`retry_interval`, 410 → `dont_try_again`, deprioritize), the
//! `completed` burst latch, `stopped` gating, `trackerid` echo and
//! real byte counters in `announce_params`.

use std::time::{Duration, Instant};

use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash, Sha256Hash};
use bitdaemon_net::http_conn::HttpError;
use bitdaemon_net::http_tracker::TrackerResponse;
use bitdaemon_net::tracker_client::AnnounceError;
use bitdaemon_session::alerts::{Alert, AlertCategory, TrackerOp};
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::tracker_list::{AnnounceEntry, SOURCE_CLIENT, TrackerList};
use bitdaemon_session::torrent::Torrent;

fn session() -> Session {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s
}

fn torrent(byte: u8) -> Torrent {
    let ih = Sha1Hash::from_bytes(&[byte; 20]).unwrap();
    Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0))
}

fn hybrid(byte: u8) -> Torrent {
    let mut t = torrent(byte);
    let v2 = Sha256Hash::from_bytes(&[byte.wrapping_add(1); 32]).unwrap();
    t.info_hashes = InfoHash::new(t.info_hash, v2);
    t
}

fn ok_reply() -> TrackerResponse {
    TrackerResponse {
        interval: 1800,
        min_interval: 60,
        ..Default::default()
    }
}

/// hybrid torrent: `due()` picks one announce per supported
/// info-hash version — upstream runs `announce_with_tracker` per
/// (socket, version) and hybrid torrents announce both hashes.
#[test]
fn hybrid_announces_v1_and_v2() {
    let mut t = hybrid(1);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let picks: Vec<(PV, [u8; 20])> = out
        .iter()
        .filter_map(|o| match o {
            SessionOut::TrackerAnnounce { v, params, .. } => Some((*v, params.info_hash)),
            _ => None,
        })
        .collect();
    assert_eq!(picks.len(), 2);
    assert_eq!(picks[0].0, PV::V1);
    assert_eq!(picks[1].0, PV::V2);
    // the v2 wire hash is the truncated 20-byte SHA-256 (`sha1_hash(v2)`).
    assert_eq!(picks[0].1, t_hash_v1(1));
    assert_eq!(picks[1].1, [2u8; 20]);
    // both slots are in-flight — a second pump dispatches nothing.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty());
}

fn t_hash_v1(byte: u8) -> [u8; 20] {
    [byte; 20]
}

/// a failed exchange only fails the announced version's slot —
/// the sibling version keeps its working state (upstream writes
/// `aep->info_hashes[ih].failed`).
#[test]
fn failed_version_only_touches_announced_slot() {
    let mut t = hybrid(2);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert_eq!(out.len(), 2);
    // v2 answers, v1 fails.
    s.on_announce_response(
        &t_ih(2),
        "http://tr/announce",
        PV::V2,
        Ok(ok_reply()),
        None,
        &mut Vec::new(),
    );
    s.on_announce_response(
        &t_ih(2),
        "http://tr/announce",
        PV::V1,
        Err(AnnounceError::TimedOut),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&t_ih(2)).unwrap();
    let e = &t.trackers.trackers[0].endpoints[0];
    assert_eq!(e.v1.fails, 1);
    assert_eq!(e.v2.fails, 0);
    assert!(e.v2.is_working());
    assert!(!e.v1.is_working());
    // v1 carries its own backoff; v2 is scheduled at the reply
    // interval — neither re-fires inside the window.
    assert!(e.v1.next_announce.is_some());
    assert!(e.v2.next_announce.is_some());
}

fn t_ih(byte: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[byte; 20]).unwrap()
}

/// HTTP 410 Gone → `dont_try_again` (`fail_limit = 1`): the very
/// first failure retires the entry — no announce is ever due again.
#[test]
fn http_410_dont_try_again() {
    let mut t = torrent(3);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert_eq!(out.len(), 1);
    s.on_announce_response(
        &t_ih(3),
        "http://tr/announce",
        PV::V1,
        Err(AnnounceError::Http(HttpError::Status(410))),
        None,
        &mut Vec::new(),
    );
    // far past any backoff — still never due.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now() + Duration::from_secs(86400), 0, &mut out);
    assert!(out.is_empty(), "410 must retire the tracker");
    let t = s.torrents.get(&t_ih(3)).unwrap();
    assert_eq!(t.trackers.trackers[0].fail_limit, 1);
    assert!(s
        .pop_alerts()
        .iter()
        .any(|a| matches!(a, Alert::TrackerError { status_code: 410, .. })));
}

/// `tracker_error_alert` carries `times_in_row`, `status_code` and
/// the `operation_t` string (upstream payload fields).
#[test]
fn tracker_error_alert_fields() {
    let mut t = torrent(4);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    s.on_announce_response(
        &t_ih(4),
        "http://tr/announce",
        PV::V1,
        Err(AnnounceError::Http(HttpError::Status(503))),
        None,
        &mut Vec::new(),
    );
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(
        a,
        Alert::TrackerError {
            times_in_row: 1,
            status_code: 503,
            op,
            ..
        } if *op == TrackerOp::SockRead
    )));
}

/// the failure response's `interval` is `retry_interval` — honored
/// verbatim even above the quadratic term (`announce_infohash::
/// failed`: `max(retry_interval, backoff)`).
#[test]
fn failure_retry_interval_overrides_backoff() {
    let mut t = torrent(5);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let before = Instant::now();
    s.on_announce_response(
        &t_ih(5),
        "http://tr/announce",
        PV::V1,
        Err(AnnounceError::Failure {
            message: "too popular".into(),
            interval: 7200,
            min_interval: 0,
        }),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&t_ih(5)).unwrap();
    let next = t.trackers.trackers[0].endpoints[0]
        .v1
        .next_announce
        .unwrap();
    let delay = next.duration_since(before);
    // quadratic would be ~7-17s; the tracker's hint must win
    // (upper bound generous — the value is `now + 7200`).
    assert!(delay >= Duration::from_secs(7199) && delay <= Duration::from_secs(7300));
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now() + Duration::from_secs(3600), 0, &mut out);
    assert!(out.is_empty());
}

/// `event == stopped` skips slots that never sent `started`
/// (upstream `send_tracker_request`: `!a.start_sent && stopped →
/// continue`). A fresh slot gets no stopped; the announced one does.
#[test]
fn stopped_only_reaches_started_slots() {
    let mut l = TrackerList::default();
    l.add(AnnounceEntry::new("http://a/announce", 0));
    let now = Instant::now();
    let supports = [true, true];
    // nothing ever announced → no stopped goes out.
    assert!(l.due(now, false, false, false, supports, true).is_empty());
    // mark v1 as having announced `started` — only that slot stops.
    l.trackers[0].endpoints[0].v1.start_sent = true;
    let due = l.due(now, false, false, false, supports, true);
    assert_eq!(due, vec![(0, PV::V1)]);
}

/// `deprioritize_tracker` — an all-failed entry sinks to the tail
/// of its tier so same-tier siblings are tried first.
#[test]
fn deprioritize_moves_to_tier_tail() {
    let mut l = TrackerList::default();
    l.add(AnnounceEntry::new("http://a", 0));
    l.add(AnnounceEntry::new("http://b", 0));
    l.add(AnnounceEntry::new("http://c", 0));
    l.deprioritize(0);
    assert_eq!(
        l.trackers.iter().map(|t| t.url.as_str()).collect::<Vec<_>>(),
        ["http://b", "http://c", "http://a"]
    );
}

/// a failure on the last working tracker runs the
/// `announce_with_tracker` cascade — the next due tracker of a
/// *lower* tier is dispatched immediately, not after the failed
/// tracker's backoff.
#[test]
fn failure_cascades_to_next_tier() {
    let mut t = torrent(6);
    t.add_tracker("http://dead/announce".into(), 0, SOURCE_CLIENT);
    t.add_tracker("http://backup/announce".into(), 1, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert_eq!(out.len(), 1);
    let mut out = Vec::new();
    s.on_announce_response(
        &t_ih(6),
        "http://dead/announce",
        PV::V1,
        Err(AnnounceError::TimedOut),
        None,
        &mut out,
    );
    assert!(out.iter().any(|o| matches!(
        o,
        SessionOut::TrackerAnnounce { url, .. } if url == "http://backup/announce"
    )));
}

/// `tracker_list::completed` + the regular selection: the burst
/// emits `completed` exactly once — the ack latches both the slot's
/// `complete_sent` and the torrent's `m_complete_sent`.
#[test]
fn completed_burst_emits_completed_once() {
    let mut t = torrent(7);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    // a checked seed upstream: picker exists (valid_metadata) and
    // `seed_mode` is on — `is_seed` short-circuits to true.
    t.picker = Some(bitdaemon_session::piece_picker::PiecePicker::new(
        4 * 128 * 1024,
        128 * 1024,
    ));
    t.seed_mode = true;
    let mut s = session();
    s.add_torrent(t);

    // `started` announce first (fresh slot → ev=2).
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let SessionOut::TrackerAnnounce { params, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(params.event, 2);
    s.on_announce_response(
        &t_ih(7),
        "http://tr/announce",
        PV::V1,
        Ok(ok_reply()),
        None,
        &mut Vec::new(),
    );

    // `torrent::completed` → arm + selection → `completed` (ev=1).
    {
        let t = s.torrents.get_mut(&t_ih(7)).unwrap();
        t.trackers.completed(Instant::now());
    }
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert_eq!(out.len(), 1);
    let SessionOut::TrackerAnnounce { params, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(params.event, 1, "burst must carry completed");
    s.on_announce_response(
        &t_ih(7),
        "http://tr/announce",
        PV::V1,
        Ok(ok_reply()),
        None,
        &mut Vec::new(),
    );

    // re-armed burst → nothing due (complete_sent latched) — the
    // `completed` event is never re-sent.
    {
        let t = s.torrents.get_mut(&t_ih(7)).unwrap();
        assert!(t.complete_sent);
        t.trackers.completed(Instant::now());
    }
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty());
}

/// `req.trackerid`: the tracker-assigned id is echoed on later
/// announces; `m_trackerid` (add-torrent param) is the fallback
/// when the entry has none.
#[test]
fn trackerid_echo_and_fallback() {
    let mut t = torrent(8);
    t.trackerid = b"from-params".to_vec();
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    // fallback — the entry has no trackerid yet, so the request
    // carries `m_trackerid` (the effect's `tracker_id` field, which
    // the driver moves into the announce params).
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let SessionOut::TrackerAnnounce { tracker_id, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(tracker_id, "from-params");

    // the tracker assigns its own id → entry wins on the next one,
    // `trackerid_alert` posts.
    let mut resp = ok_reply();
    resp.tracker_id = Some(b"from-tracker".to_vec());
    s.on_announce_response(
        &t_ih(8),
        "http://tr/announce",
        PV::V1,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert_eq!(
        s.torrents.get(&t_ih(8)).unwrap().trackers.trackers[0].tracker_id,
        "from-tracker"
    );
    assert!(s
        .pop_alerts()
        .iter()
        .any(|a| matches!(a, Alert::Trackerid { trackerid, .. } if trackerid == "from-tracker")));
    // and the entry's id wins over `m_trackerid` on later requests.
    let mut out = Vec::new();
    s.tracker_announce_due(
        Instant::now() + Duration::from_secs(3601),
        0,
        &mut out,
    );
    let SessionOut::TrackerAnnounce { tracker_id, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(tracker_id, "from-tracker");
}

/// `announce_params` carries real byte counters — `left` is
/// `bytes_left` (total − verified have, 16 KiB fallback with no
/// metadata like upstream's `req.left < 0 → 16*1024`),
/// `uploaded`/`downloaded` come from the transfer counters,
/// `num_want` from settings, and `private` marks the request.
#[test]
fn announce_params_real_counters() {
    let mut t = torrent(9);
    t.private = true;
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.settings.num_want = 42;
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let SessionOut::TrackerAnnounce { params, .. } = &out[0] else {
        panic!("announce effect")
    };
    // no file storage → metadata-less → upstream 16 KiB fallback.
    assert_eq!(params.left, 16 * 1024);
    assert_eq!(params.downloaded, 0);
    assert_eq!(params.uploaded, 0);
    assert_eq!(params.num_want, 42);
    assert!(params.private_torrent);
    // `req.key` is the per-torrent `tracker_key` (upstream hashes
    // the torrent/session/storage pointers — ours is drawn once at
    // construction and stable for the session).
    let key = s.torrents.get(&t_ih(9)).unwrap().tracker_key;
    assert_eq!(params.key, key);
}

/// hybrid `scrape_tracker` — one `scrape_request` per supported
/// version (`sreq.scrape_ih`), each flagged `user_triggered`.
#[test]
fn scrape_emits_per_version_for_hybrid() {
    let mut t = hybrid(10);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.scrape_tracker(&t_ih(10), 0, &mut out);
    let scrapes: Vec<(PV, [u8; 20])> = out
        .iter()
        .filter_map(|o| match o {
            SessionOut::TrackerScrape { v, info_hash, .. } => Some((*v, *info_hash)),
            _ => None,
        })
        .collect();
    assert_eq!(scrapes.len(), 2);
    assert_eq!(scrapes[0].0, PV::V1);
    assert_eq!(scrapes[1].0, PV::V2);
    assert_eq!(scrapes[0].1, [10u8; 20]);
    assert_eq!(scrapes[1].1, [11u8; 20]);
}

/// the scrape reply lands on the entry (`scrape_*` fields) and
/// `verified` — a working scrape marks the tracker verified.
#[test]
fn scrape_reply_marks_entry_verified() {
    let mut t = torrent(11);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut resp = ok_reply();
    resp.complete = 7;
    resp.incomplete = 3;
    s.on_scrape_response(&t_ih(11), "http://tr/announce", PV::V1, Ok(resp));
    let t = s.torrents.get(&t_ih(11)).unwrap();
    assert!(t.trackers.trackers[0].verified);
    assert_eq!(t.trackers.trackers[0].scrape_complete, 7);
    assert_eq!(t.trackers.trackers[0].scrape_incomplete, 3);
}
