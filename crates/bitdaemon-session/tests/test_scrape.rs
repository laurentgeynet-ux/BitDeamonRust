// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `scrape_tracker` → `TrackerScrape` effect → `on_scrape_response`
//! updates seed/leech counts + `scrape_*` fields and posts
//! `scrape_reply`/`scrape_failed` alerts.

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::http_tracker::TrackerResponse;
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::Torrent;

/// upstream tests opt into alert categories — default mask is error-only.
fn session() -> Session {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s
}

#[test]
fn scrape_cycle() {
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker(
        "http://tr/announce".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    t.add_tracker(
        "http://other/announce".into(),
        1,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts(); // drain TorrentAdded

    let mut out = Vec::new();
    s.scrape_tracker(&ih, 0, &mut out);
    assert_eq!(out.len(), 1);
    let SessionOut::TrackerScrape { ih: ai, url } = &out[0] else {
        panic!("scrape effect")
    };
    assert_eq!(*ai, ih);
    assert_eq!(url, "http://tr/announce");

    let resp = TrackerResponse {
        interval: 0,
        min_interval: -1,
        complete: 42,
        incomplete: 17,
        downloaded: 500,
        downloaders: -1,
        tracker_id: None,
        failure_reason: None,
        warning_message: None,
        peers: vec![],
        peer_ids: vec![],
        external_ip: None,
        ..Default::default()
    };
    s.on_scrape_response(&ih, "http://tr/announce", Ok(resp));

    let t = s.torrents.get(&ih).unwrap();
    // upstream: scrape `complete` lands on `m_complete`; `m_num_seeds`
    // is the connected-seed count (none here).
    assert_eq!(t.complete, 42);
    assert_eq!(t.num_seeds, 0);
    let i = t.trackers.find("http://tr/announce").unwrap();
    let e = &t.trackers.trackers[i];
    assert_eq!(e.scrape_complete, 42);
    assert_eq!(e.scrape_incomplete, 17);
    assert_eq!(e.scrape_downloaded, 500);
    assert!(e.verified);
    // other entry untouched
    let j = t.trackers.find("http://other/announce").unwrap();
    assert_eq!(t.trackers.trackers[j].scrape_complete, -1);

    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(
        a,
        Alert::ScrapeReply {
            complete: 42,
            incomplete: 17,
            ..
        }
    )));
}

#[test]
fn scrape_failure_posts_alert() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker(
        "http://tr/announce".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    s.on_scrape_response(
        &ih,
        "http://tr/announce",
        Err("scrape not available".into()),
    );
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(
        |a| matches!(a, Alert::ScrapeFailed { reason, .. } if reason.contains("not available"))
    ));
}
