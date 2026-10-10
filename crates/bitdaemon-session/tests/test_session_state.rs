// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session_state` — `.session` file round-trip.

use bitdaemon_common::{InfoHash, Sha1Hash};
use bitdaemon_core::AddTorrentParams;
use bitdaemon_session::session_state::{SessionState, load_session_state, save_session_state_buf};

#[test]
fn session_state_round_trip() {
    let mut st = SessionState::default();
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let atp = AddTorrentParams {
        info_hashes: InfoHash::from_v1(ih),
        name: b"test-torrent".to_vec(),
        ..Default::default()
    };
    st.torrents.insert(ih, atp);
    st.dht_state = b"dhtblob".to_vec();

    let buf = save_session_state_buf(&st);
    let back = load_session_state(&buf).unwrap();
    assert_eq!(back.torrents.len(), 1);
    assert_eq!(back.torrents[&ih].name, b"test-torrent");
    assert_eq!(back.dht_state, b"dhtblob");
}

#[test]
fn session_state_tolerates_garbage() {
    assert!(load_session_state(b"i42e").is_none());
    // dict without torrents → empty state, not error
    let st = load_session_state(b"d4:junk1:xe").unwrap();
    assert!(st.torrents.is_empty());
}
