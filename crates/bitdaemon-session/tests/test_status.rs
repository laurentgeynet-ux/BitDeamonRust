// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `Torrent::status` / `post_torrent_updates` — the enriched
//! `torrent_status` snapshot (state enum, counters, scrape bests,
//! rates, `state_update_alert`).

use std::time::Instant;

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::http_tracker::TrackerResponse;
use bitdaemon_session::alerts::Alert;
use bitdaemon_session::session::Session;
use bitdaemon_session::status::TorrentState;
use bitdaemon_session::torrent::Torrent;

#[test]
fn status_snapshot() {
    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut t = Torrent::new(ih, 8, 128 * 1024, Bitfield::with_size(8), StorageIndex(0));
    t.add_tracker("http://tr/announce".into(), 0);
    t.name = "swarm-x".into();
    t.save_path = "/dl".into();
    let mut s = Session::new();
    s.add_torrent(t);

    // scrape feeds the status counters
    let resp = TrackerResponse {
        interval: 0,
        min_interval: -1,
        complete: 12,
        incomplete: 5,
        downloaded: 99,
        downloaders: -1,
        tracker_id: None,
        failure_reason: None,
        warning_message: None,
        peers: vec![],
        peer_ids: vec![],
        external_ip: None,
    };
    s.on_scrape_response(&ih, "http://tr/announce", Ok(resp));

    let st = s.torrents.get(&ih).unwrap().status();
    assert_eq!(st.state, TorrentState::Downloading);
    assert_eq!(st.name, "swarm-x");
    assert_eq!(st.save_path, "/dl");
    assert_eq!(st.num_complete, 12);
    assert_eq!(st.num_incomplete, 5);
    assert_eq!(st.downloaded, 99);
    assert_eq!(st.num_seeds, 12); // scrape complete seeds num_seeds
    assert_eq!(st.total_wanted, 8 * 128 * 1024);
    assert_eq!(st.total_done, 0);
    assert_eq!(st.pieces.count(), 0);
    assert!(st.added_time > 0);

    // rate sampling — one tick between samples
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .update_rates(Instant::now());
    let mut out = Vec::new();
    s.tick(&mut out);
    // no peers → zero rates, but sampler ran
    assert_eq!(s.torrents.get(&ih).unwrap().down_rate, 0);

    // post_torrent_updates → state_update_alert
    s.pop_alerts();
    s.post_torrent_updates();
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(
        a,
        Alert::StateUpdate { statuses } if statuses.len() == 1
            && statuses[0].info_hash == ih
    )));
}
