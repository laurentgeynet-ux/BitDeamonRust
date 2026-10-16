// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_info` — snapshot + flags.

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_info::PeerFlags;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::Torrent;

#[test]
fn snapshot_flags_and_queues() {
    let mut t = Torrent::new(
        Sha1Hash::from_bytes(&[7; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    );
    let mut c = PeerConn::new(4, PeerPipeline::new(5, 60, 500), 0);
    c.interesting = true;
    c.peer_choked = false;
    c.bitfield_received = true;
    c.num_pieces = 4;
    t.add_peer(1, c, &bitdaemon_session::counters::Counters::default());
    let s = SessionSettings::default();
    let pi = t.peer_info(1, None, &s).expect("peer 1");
    assert!(pi.flags.has(PeerFlags::INTERESTING));
    assert!(pi.flags.has(PeerFlags::CHOKED));
    assert!(pi.flags.has(PeerFlags::SEED));
    assert!(!pi.flags.has(PeerFlags::REMOTE_CHOKED));
    assert_eq!(pi.progress_ppm, 1_000_000);
    assert_eq!(pi.num_pieces, 4);
    assert_eq!(pi.request_timeout, -1);
    assert!(t.peer_info(99, None, &s).is_none());
}
