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
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::Session;
use bitdaemon_session::status::TorrentState;
use bitdaemon_session::torrent::Torrent;

#[test]
fn status_snapshot() {
    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut t = Torrent::new(ih, 8, 128 * 1024, Bitfield::with_size(8), StorageIndex(0));
    t.add_tracker(
        "http://tr/announce".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    t.name = "swarm-x".into();
    t.save_path = "/dl".into();
    // `update_subscribe` — upstream only reports subscribed torrents
    // that marked `state_updated()`.
    t.set_flags(
        bitdaemon_common::flags::TorrentFlags::UPDATE_SUBSCRIBE,
        bitdaemon_common::flags::TorrentFlags::UPDATE_SUBSCRIBE,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut Vec::new(),
    );
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
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
        ..Default::default()
    };
    s.on_scrape_response(&ih, "http://tr/announce", Ok(resp));

    let st = s.torrents.get(&ih).unwrap().status(
        &s.settings,
        s.queue_position(&ih),
        bitdaemon_session::status::StatusFlags::ALL,
        &s.peer_class_pool,
    );
    assert_eq!(st.state, TorrentState::Downloading);
    assert_eq!(st.name, "swarm-x");
    assert_eq!(st.save_path, "/dl");
    assert_eq!(st.num_complete, 12);
    assert_eq!(st.num_incomplete, 5);
    assert_eq!(st.downloaded, 99);
    // scrape `complete` feeds `num_complete`; `num_seeds` counts
    // *connected* seeds only (upstream `m_num_seeds`).
    assert_eq!(st.num_seeds, 0);
    assert_eq!(st.total_wanted, 8 * 128 * 1024);
    assert_eq!(st.total_done, 0);
    assert_eq!(st.pieces.count(), 0);
    assert!(st.added_time > 0);

    // rate sampling — one tick between samples
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .update_rates(Instant::now(), &mut Vec::new());
    let mut out = Vec::new();
    s.tick(&mut out);
    // no peers → zero rates, but sampler ran
    assert_eq!(s.torrents.get(&ih).unwrap().down_rate, 0);

    // post_torrent_updates → state_update_alert
    s.pop_alerts();
    s.post_torrent_updates(bitdaemon_session::status::StatusFlags::ALL);
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(
        a,
        Alert::StateUpdate { statuses } if statuses.len() == 1
            && statuses[0].info_hash == ih
    )));
}
