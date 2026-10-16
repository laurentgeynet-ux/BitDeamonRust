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
use bitdaemon_session::torrent::Torrent;
use bitdaemon_session::tracker_list::{AnnounceEntry, SOURCE_CLIENT, TrackerList};

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

/// the `sent_seq` a dispatched `TrackerAnnounce` carries — response
/// handlers echo it so stale completions (aborted, re-dispatched)
/// are dropped like upstream's destroyed `tracker_connection`s.
fn seq_of(out: &[SessionOut], url: &str, v: PV) -> u64 {
    out.iter()
        .find_map(|o| match o {
            SessionOut::TrackerAnnounce {
                url: u, v: vv, seq, ..
            } if u == url && *vv == v => Some(*seq),
            _ => None,
        })
        .expect("announce effect")
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
    let seq_v1 = seq_of(&out, "http://tr/announce", PV::V1);
    let seq_v2 = seq_of(&out, "http://tr/announce", PV::V2);
    // v2 answers, v1 fails.
    s.on_announce_response(
        &t_ih(2),
        "http://tr/announce",
        PV::V2,
        seq_v2,
        Ok(ok_reply()),
        None,
        &mut Vec::new(),
    );
    s.on_announce_response(
        &t_ih(2),
        "http://tr/announce",
        PV::V1,
        seq_v1,
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
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    s.on_announce_response(
        &t_ih(3),
        "http://tr/announce",
        PV::V1,
        seq,
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
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        Alert::TrackerError {
            status_code: 410,
            ..
        }
    )));
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

    // arm a request — a completion only counts against an in-flight
    // announce (upstream destroys aborted `tracker_connection`s).
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    s.on_announce_response(
        &t_ih(4),
        "http://tr/announce",
        PV::V1,
        seq,
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
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let before = Instant::now();
    s.on_announce_response(
        &t_ih(5),
        "http://tr/announce",
        PV::V1,
        seq,
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
        l.trackers
            .iter()
            .map(|t| t.url.as_str())
            .collect::<Vec<_>>(),
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
    let seq = seq_of(&out, "http://dead/announce", PV::V1);
    let mut out = Vec::new();
    s.on_announce_response(
        &t_ih(6),
        "http://dead/announce",
        PV::V1,
        seq,
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
    let SessionOut::TrackerAnnounce { params, seq, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(params.event, 2);
    let seq1 = *seq;
    s.on_announce_response(
        &t_ih(7),
        "http://tr/announce",
        PV::V1,
        seq1,
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
    let SessionOut::TrackerAnnounce { params, seq, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(params.event, 1, "burst must carry completed");
    let seq2 = *seq;
    s.on_announce_response(
        &t_ih(7),
        "http://tr/announce",
        PV::V1,
        seq2,
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
    // the driver moves into the announce params). `trackerid` is
    // binary upstream — carried as bytes end-to-end.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let SessionOut::TrackerAnnounce {
        tracker_id, seq, ..
    } = &out[0]
    else {
        panic!("announce effect")
    };
    assert_eq!(tracker_id, b"from-params");
    let seq = *seq;

    // the tracker assigns its own id → entry wins on the next one,
    // `trackerid_alert` posts.
    let mut resp = ok_reply();
    resp.tracker_id = Some(b"from-tracker".to_vec());
    s.on_announce_response(
        &t_ih(8),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert_eq!(
        s.torrents.get(&t_ih(8)).unwrap().trackers.trackers[0].tracker_id,
        b"from-tracker"
    );
    assert!(
        s.pop_alerts().iter().any(
            |a| matches!(a, Alert::Trackerid { trackerid, .. } if trackerid == "from-tracker")
        )
    );
    // and the entry's id wins over `m_trackerid` on later requests.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now() + Duration::from_secs(3601), 0, &mut out);
    let SessionOut::TrackerAnnounce { tracker_id, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(tracker_id, b"from-tracker");
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

/// the scrape reply lands on the entry (`scrape_*` fields) but does
/// NOT mark it `verified` — upstream `torrent::scrape_response`
/// never touches `ae->verified`; only announce replies do
/// (`tracker_response` sets `verified = true`).
#[test]
fn scrape_reply_does_not_verify() {
    let mut t = torrent(11);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut resp = ok_reply();
    resp.complete = 7;
    resp.incomplete = 3;
    s.on_scrape_response(&t_ih(11), "http://tr/announce", PV::V1, Ok(resp));
    let t = s.torrents.get(&t_ih(11)).unwrap();
    assert!(!t.trackers.trackers[0].verified);
    assert_eq!(t.trackers.trackers[0].scrape_complete, 7);
    assert_eq!(t.trackers.trackers[0].scrape_incomplete, 3);
}

/// `stop_tracker_timeout <= 0` upstream suppresses the whole
/// stopped-announce leg (`announce_with_tracker` returns before any
/// work): no request, no bookkeeping, no error alert. With a real
/// timeout the `stopped` announce goes out — the gate is the
/// timeout, not the pause path.
#[test]
fn stopped_suppressed_when_timeout_zero() {
    for timeout in [Duration::ZERO, Duration::from_secs(10)] {
        let mut t = torrent(20);
        t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
        let mut s = session();
        s.settings.stop_tracker_timeout = timeout;
        s.add_torrent(t);
        s.pop_alerts();

        // deliver `started` so the slot owes a `stopped` on pause.
        let mut out = Vec::new();
        s.tracker_announce_due(Instant::now(), 0, &mut out);
        let seq = seq_of(&out, "http://tr/announce", PV::V1);
        s.on_announce_response(
            &t_ih(20),
            "http://tr/announce",
            PV::V1,
            seq,
            Ok(ok_reply()),
            None,
            &mut Vec::new(),
        );
        s.pop_alerts();

        let mut out = Vec::new();
        s.set_paused(&t_ih(20), true, &mut out);
        let stopped = out.iter().any(|o| {
            matches!(
                o,
                SessionOut::TrackerAnnounce { params, .. } if params.event == 3
            )
        });
        let t = s.torrents.get(&t_ih(20)).unwrap();
        if timeout.is_zero() {
            assert!(!stopped, "zero timeout must suppress the stopped announce");
            // no bookkeeping at all — the slot stays pristine.
            assert_eq!(t.trackers.trackers[0].endpoints[0].v1.fails, 0);
            assert!(
                !s.pop_alerts()
                    .iter()
                    .any(|a| matches!(a, Alert::TrackerError { .. }))
            );
        } else {
            assert!(stopped, "nonzero timeout must send the stopped announce");
        }
    }
}

/// `session_impl::pause` → `abort_all_requests`: in-flight
/// non-stopped requests are aborted BEFORE torrents get
/// session-paused — each abort runs `fail(announce_skipped)`
/// bookkeeping (fails++, message, alert) and the driver is told to
/// drop queued requests. A late completion is dropped stale.
#[test]
fn session_pause_aborts_in_flight() {
    let mut t = torrent(21);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    assert!(
        s.torrents.get(&t_ih(21)).unwrap().trackers.trackers[0].endpoints[0]
            .v1
            .updating
    );

    let mut out = Vec::new();
    s.set_session_paused(true, &mut out);
    assert!(
        out.iter()
            .any(|o| matches!(o, SessionOut::AbortTrackerRequests))
    );
    let t = s.torrents.get(&t_ih(21)).unwrap();
    let ep = &t.trackers.trackers[0].endpoints[0];
    assert_eq!(ep.v1.fails, 1);
    assert!(!ep.v1.updating);
    assert_eq!(ep.message, "announce skipped");
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        Alert::TrackerError { error, .. } if error == "announce skipped"
    )));

    // the aborted exchange's late reply must not relatch the slot.
    let mut out = Vec::new();
    s.on_announce_response(
        &t_ih(21),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(ok_reply()),
        None,
        &mut out,
    );
    let t = s.torrents.get(&t_ih(21)).unwrap();
    assert!(!t.trackers.trackers[0].verified);
    assert_eq!(t.trackers.trackers[0].endpoints[0].v1.fails, 1);
}

/// a completion whose `sent_seq` doesn't match the live request is
/// dropped wholesale — no state latches, no alerts (upstream's
/// aborted `tracker_connection` object is simply gone).
#[test]
fn stale_completion_dropped() {
    let mut t = torrent(22);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);

    let mut out = Vec::new();
    s.on_announce_response(
        &t_ih(22),
        "http://tr/announce",
        PV::V1,
        seq + 1,
        Ok(ok_reply()),
        None,
        &mut out,
    );
    let t = s.torrents.get(&t_ih(22)).unwrap();
    let ep = &t.trackers.trackers[0].endpoints[0];
    // the live request is still in flight; nothing was applied.
    assert!(ep.v1.updating);
    assert_eq!(ep.v1.fails, 0);
    assert!(!t.trackers.trackers[0].verified);
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::TrackerReply { .. } | Alert::TrackerError { .. }))
    );
}

/// `send_tracker_request` stamps `next_announce = min_announce =
/// now` at dispatch — a stale tracker `min interval` must not delay
/// the failure backoff of the request just sent.
#[test]
fn dispatch_resets_stale_min_announce() {
    let mut t = torrent(23);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    // the tracker pins a huge min interval on us.
    let mut resp = ok_reply();
    resp.min_interval = 7200;
    let t0 = Instant::now();
    s.on_announce_response(
        &t_ih(23),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    {
        let t = s.torrents.get(&t_ih(23)).unwrap();
        let a = &t.trackers.trackers[0].endpoints[0].v1;
        assert!(a.min_announce.unwrap() >= t0 + Duration::from_secs(7199));
    }
    // a manual reannounce bypasses `min` (`ignore_min_interval`) —
    // dispatch must reset the slot's timers so the failure below
    // schedules from `now`, not from the stale min.
    use bitdaemon_session::session::ReannounceFlags;
    let mut out = Vec::new();
    s.force_announce(
        &t_ih(23),
        None,
        0,
        ReannounceFlags::IGNORE_MIN_INTERVAL | ReannounceFlags::HIGH_PRIORITY,
        &mut out,
    );
    let seq2 = seq_of(&out, "http://tr/announce", PV::V1);
    let t1 = Instant::now();
    {
        let t = s.torrents.get(&t_ih(23)).unwrap();
        let a = &t.trackers.trackers[0].endpoints[0].v1;
        assert!(a.min_announce.unwrap() < t1 + Duration::from_secs(60));
    }
    s.on_announce_response(
        &t_ih(23),
        "http://tr/announce",
        PV::V1,
        seq2,
        Err(AnnounceError::TimedOut),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&t_ih(23)).unwrap();
    let a = &t.trackers.trackers[0].endpoints[0].v1;
    let next = a.next_announce.unwrap();
    assert!(
        next < t1 + Duration::from_secs(7200),
        "stale min must not delay retry"
    );
    assert!(next >= t1 + Duration::from_secs(5));
}

/// `torrent::add_tracker` — a real insertion marks
/// `need_save_resume(if_metadata_changed)` and, when the torrent is
/// announcing, triggers an immediate `announce_with_tracker`
/// (routed via `pending_out` for handle-API calls).
#[test]
fn add_tracker_marks_resume_and_announces() {
    let mut t = torrent(24);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    t.announcing = true;
    // the test's seeding add counts as a change — clear it so the
    // assertions below isolate the `Session::add_tracker` leg.
    t.need_save_resume = bitdaemon_common::flags::ResumeDataFlags::NONE;
    let mut s = session();
    s.add_torrent(t);

    // ctor-seeded tracker is NOT a user change — no metadata dirty.
    let t = s.torrents.get(&t_ih(24)).unwrap();
    assert!(
        !t.need_save_resume
            .contains(bitdaemon_common::flags::ResumeDataFlags::IF_METADATA_CHANGED)
    );

    // retire the seeded tracker's announce first — a fresh
    // `announce_state` scan picks the first due entry, so the
    // existing tracker must not be due for the new one to win.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    s.on_announce_response(
        &t_ih(24),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(ok_reply()),
        None,
        &mut Vec::new(),
    );

    // tier -1 inserts ahead of the covering tier-0 entry — the
    // `announce_state` scan picks it and the announce goes out
    // synchronously (upstream `m_announcing && added →
    // announce_with_tracker()`).
    s.add_tracker(&t_ih(24), "http://new/announce".into(), -1, SOURCE_CLIENT);
    let t = s.torrents.get(&t_ih(24)).unwrap();
    assert!(
        t.need_save_resume
            .contains(bitdaemon_common::flags::ResumeDataFlags::IF_METADATA_CHANGED)
    );
    assert!(s.pending_out.iter().any(|o| matches!(
        o,
        SessionOut::TrackerAnnounce { url, .. } if url == "http://new/announce"
    )));

    // a duplicate URL is a no-op — no extra dirty marking beyond
    // what the first add already set, no second announce.
    s.pending_out.clear();
    let t = s.torrents.get_mut(&t_ih(24)).unwrap();
    t.need_save_resume = bitdaemon_common::flags::ResumeDataFlags::NONE;
    s.add_tracker(&t_ih(24), "http://new/announce".into(), 1, SOURCE_CLIENT);
    let t = s.torrents.get(&t_ih(24)).unwrap();
    assert!(
        !t.need_save_resume
            .contains(bitdaemon_common::flags::ResumeDataFlags::IF_METADATA_CHANGED)
    );
    assert!(s.pending_out.is_empty());
}

/// `torrent::replace_trackers` — rebuild + `need_save_resume`, then
/// an immediate announce when the torrent is announcing and the
/// list is non-empty.
#[test]
fn replace_trackers_announces_when_announcing() {
    let mut t = torrent(25);
    t.add_tracker("http://old/announce".into(), 0, SOURCE_CLIENT);
    t.announcing = true;
    let mut s = session();
    s.add_torrent(t);

    s.replace_trackers(&t_ih(25), &[("http://fresh/announce".into(), 0)]);
    let t = s.torrents.get(&t_ih(25)).unwrap();
    assert_eq!(t.trackers.trackers.len(), 1);
    assert_eq!(t.trackers.trackers[0].url, "http://fresh/announce");
    assert!(
        t.need_save_resume
            .contains(bitdaemon_common::flags::ResumeDataFlags::IF_METADATA_CHANGED)
    );
    assert!(s.pending_out.iter().any(|o| matches!(
        o,
        SessionOut::TrackerAnnounce { url, .. } if url == "http://fresh/announce"
    )));
}

/// `if (i.pid == m_peer_id) continue` — a dict-form peer entry
/// carrying our own peer_id is never added to the swarm.
#[test]
fn self_peer_id_skipped() {
    let mut t = torrent(26);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);

    let us = s.peer_id;
    let mut resp = ok_reply();
    resp.peers = vec![
        std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1)),
            6881,
        ),
        std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2)),
            6881,
        ),
    ];
    resp.peer_ids = vec![Some(us.as_bytes().to_vec()), Some(vec![9u8; 20])];
    s.on_announce_response(
        &t_ih(26),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&t_ih(26)).unwrap();
    assert_eq!(t.peer_list.num_peers(), 1);
}

/// `trackerid_alert` only fires when the tracker-assigned id
/// *changes* — an identical or empty `tracker id` is silent.
#[test]
fn trackerid_alert_only_on_change() {
    let mut t = torrent(27);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    // announce → tracker assigns "tid-x" → alert.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let mut resp = ok_reply();
    resp.interval = 0;
    resp.min_interval = 0;
    resp.tracker_id = Some(b"tid-x".to_vec());
    s.on_announce_response(
        &t_ih(27),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Trackerid { .. }))
    );

    // same id again → no alert.
    s.pop_alerts();
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let mut resp = ok_reply();
    resp.interval = 0;
    resp.min_interval = 0;
    resp.tracker_id = Some(b"tid-x".to_vec());
    s.on_announce_response(
        &t_ih(27),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Trackerid { .. }))
    );

    // absent trackerid → no alert, stored id kept.
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let mut resp = ok_reply();
    resp.interval = 0;
    resp.min_interval = 0;
    resp.tracker_id = None;
    s.on_announce_response(
        &t_ih(27),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Trackerid { .. }))
    );
    assert_eq!(
        s.torrents.get(&t_ih(27)).unwrap().trackers.trackers[0].tracker_id,
        b"tid-x"
    );
}

/// a `warning message` in the announce reply lands on the endpoint's
/// `message` slot (upstream `aep->message = warning_message`).
#[test]
fn warning_message_lands_on_entry() {
    let mut t = torrent(28);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let mut resp = ok_reply();
    resp.warning_message = Some("careful now".into());
    s.on_announce_response(
        &t_ih(28),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&t_ih(28)).unwrap();
    assert_eq!(t.trackers.trackers[0].endpoints[0].message, "careful now");
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        Alert::TrackerWarning { msg, .. } if msg == "careful now"
    )));
}

/// `trackerid` is binary upstream (`std::string`, not utf8) — a
/// non-UTF-8 id must round-trip losslessly: stored verbatim on the
/// entry and echoed verbatim on the next announce.
#[test]
fn trackerid_binary_round_trip() {
    let mut t = torrent(29);
    t.add_tracker("http://tr/announce".into(), 0, SOURCE_CLIENT);
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    let seq = seq_of(&out, "http://tr/announce", PV::V1);
    let binary_tid = vec![0xff, 0x00, 0xfe, 0x80];
    let mut resp = ok_reply();
    resp.tracker_id = Some(binary_tid.clone());
    s.on_announce_response(
        &t_ih(29),
        "http://tr/announce",
        PV::V1,
        seq,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    assert_eq!(
        s.torrents.get(&t_ih(29)).unwrap().trackers.trackers[0].tracker_id,
        binary_tid
    );
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now() + Duration::from_secs(3601), 0, &mut out);
    let SessionOut::TrackerAnnounce { tracker_id, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(tracker_id, &binary_tid);
}
