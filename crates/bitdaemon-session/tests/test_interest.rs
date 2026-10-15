// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `update_interest` — Interested/NotInterested emission on
//! bitfield/have + our own piece_verified.

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{PeerPacket, Session, SessionOut};
use bitdaemon_session::torrent::Torrent;

fn mk() -> (Session, Sha1Hash) {
    let mut s = Session::new();
    let mut have = Bitfield::new();
    have.resize_with(4, false);
    let mut t = Torrent::new(Sha1Hash::from([7u8; 20]), 4, 0x40000, have, StorageIndex(0));
    t.init_picker(true);
    let ih = s.add_torrent(t);
    (s, ih)
}

fn connect(s: &mut Session, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
    s.peer_connected(
        ih,
        1,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "127.0.0.1:6881".parse().unwrap(),
        out,
    );
}

#[test]
fn bitfield_sends_interested() {
    let (mut s, ih) = mk();
    let mut out = Vec::new();
    connect(&mut s, &ih, &mut out);
    out.clear();
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Bitfield(bytes::Bytes::from(vec![0b0010_0000u8])),
    };
    s.on_peer_message(&p, 0, &mut out);
    assert!(
        out.iter().any(|e| matches!(
            e,
            SessionOut::ToPeer {
                msg: PeerMessage::Interested,
                ..
            }
        )),
        "{out:?}"
    );
}

#[test]
fn piece_verified_sends_not_interested() {
    let (mut s, ih) = mk();
    let mut out = Vec::new();
    connect(&mut s, &ih, &mut out);
    out.clear();
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Bitfield(bytes::Bytes::from(vec![0b1000_0000u8])),
    };
    s.on_peer_message(&p, 0, &mut out);
    out.clear();
    // upstream `on_piece_verified` ignores pieces the picker doesn't
    // consider finished — simulate the download completing first.
    if let Some(t) = s.torrents.get_mut(&ih)
        && let Some(p) = t.picker.as_mut()
    {
        for b in 0..p.blocks_in_piece(PieceIndex(0)) {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: PieceIndex(0),
                    block_index: b,
                },
                None,
            );
        }
    }
    // we gain the only piece they had → NotInterested
    s.on_piece_verified(&ih, PieceIndex(0), true, &[], &mut out);
    assert!(
        out.iter().any(|o| matches!(
            o,
            SessionOut::ToPeer {
                msg: PeerMessage::NotInterested,
                ..
            }
        )),
        "{out:?}"
    );
}

#[test]
fn allowed_fast_set_on_fast_peer() {
    let (mut s, ih) = mk();
    // give ourselves a piece so there is something to allow
    s.torrents.get_mut(&ih).unwrap().have.set_bit(1);
    let mut out = Vec::new();
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.supports_fast = true;
    s.peer_connected(&ih, 1, c, "127.0.0.1:6881".parse().unwrap(), &mut out);
    // upstream defers the allowed-fast set to the FIRST inbound
    // `Interested` (`m_sent_allowed_fast` in `on_interested`) — not
    // at attach.
    assert!(
        !out.iter().any(|e| matches!(
            e,
            SessionOut::ToPeer {
                msg: PeerMessage::AllowedFast(_),
                ..
            }
        )),
        "{out:?}"
    );
    let t = s.torrents.get_mut(&ih).unwrap();
    let mut eff = Vec::new();
    t.on_peer_message(
        1,
        &PeerMessage::Interested,
        0,
        &s.settings,
        &s.counters,
        &mut eff,
    );
    assert!(
        eff.iter().any(|e| matches!(
            e,
            bitdaemon_session::torrent::TorrentEffect::SendTo {
                msg: PeerMessage::AllowedFast(_),
                ..
            }
        )),
        "{eff:?}"
    );
}
