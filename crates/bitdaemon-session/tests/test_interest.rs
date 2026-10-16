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

#[test]
fn piece_does_not_mark_interest() {
    // upstream never calls `update_interest` from `incoming_piece` —
    // a `Piece` payload must leave the dirty flag alone (the eager
    // per-message scan was A08-D12). Conn-level dispatch proves the
    // flag is never set; session-level proves no re-emission.
    let mut c = PeerConn::new(10, PeerPipeline::new(1000, 30, 250), 0);
    // a pre-bitfield `Piece` legitimately implies `incoming_have_none`
    // upstream (which re-evaluates) — settle the have-set first so the
    // flag check isolates the Piece leg.
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Bitfield(bytes::Bytes::from(vec![0xff, 0xc0])),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.need_interest_update);
    c.need_interest_update = false;
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(
            bitdaemon_common::types::PieceBlock {
                piece_index: PieceIndex(3),
                block_index: 0,
            },
        ));
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from(vec![7u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        !c.need_interest_update,
        "Piece must not dirty the interest flag"
    );
    // session-level: a solicited piece produces no Interested /
    // NotInterested re-emission.
    let (mut s, ih) = mk();
    let mut out = Vec::new();
    connect(&mut s, &ih, &mut out);
    out.clear();
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Bitfield(bytes::Bytes::from(vec![0b0100_0000u8])),
    };
    s.on_peer_message(&p, 0, &mut out);
    out.clear();
    // queue a request for piece 1 block 0 so the piece is solicited
    // (a full block — `validate_piece_request` checks the exact
    // `to_req` length).
    let t = s.torrents.get_mut(&ih).unwrap();
    t.peers.get_mut(&1).unwrap().pipeline.download_queue.push(
        bitdaemon_session::peer_pipeline::PendingBlock::new(bitdaemon_common::types::PieceBlock {
            piece_index: PieceIndex(1),
            block_index: 0,
        }),
    );
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Piece {
            piece: PieceIndex(1),
            offset: 0,
            data: bytes::Bytes::from(vec![7u8; 0x4000]),
        },
    };
    s.on_peer_message(&p, 0, &mut out);
    if let Some(c) = s.torrents[&ih].peers.get(&1) {
        assert!(
            !c.need_interest_update,
            "Piece must not dirty the interest flag"
        );
    }
    assert!(
        !out.iter().any(|e| matches!(
            e,
            SessionOut::ToPeer {
                msg: PeerMessage::Interested | PeerMessage::NotInterested,
                ..
            }
        )),
        "{out:?}"
    );
}

#[test]
fn dirty_flag_gates_disconnect_if_redundant() {
    // upstream `disconnect_if_redundant` early-outs while
    // `m_need_interest_update` is pending (`peer_connection.cpp:2323`)
    // — the conn keeps its stale bit until the deferred re-eval runs.
    let (mut s, ih) = mk();
    let mut out = Vec::new();
    connect(&mut s, &ih, &mut out);
    let t = s.torrents.get_mut(&ih).unwrap();
    {
        let c = t.peers.get_mut(&1).unwrap();
        // make the conn a disconnect candidate: upload-only remote
        // on an upload-only torrent with a settled (empty) have-set.
        c.remote_upload_only = true;
        c.bitfield_received = true;
        t.upload_mode = true;
        c.need_interest_update = true;
    }
    let mut eff = Vec::new();
    assert!(
        !t.disconnect_if_redundant(1, &mut eff),
        "dirty conn must survive disconnect_if_redundant"
    );
    assert!(eff.is_empty());
    // the drain clears the flag, re-evaluates, and drops the conn
    // through `do_update_interest`'s own disconnect tail.
    t.drain_interest_updates(&s.counters, &mut eff);
    assert!(
        !t.peers[&1].need_interest_update && !t.peers[&1].interesting,
        "drain must clear the flag and the interest"
    );
    assert!(
        eff.iter().any(|e| matches!(
            e,
            bitdaemon_session::torrent::TorrentEffect::DisconnectPeer { peer: 1, .. }
        )),
        "{eff:?}"
    );
}

#[test]
fn have_burst_sends_interested_once() {
    // upstream collapses N dirty events into one deferred
    // `do_update_interest`; several `have` messages in a row must
    // still yield a single `Interested` on the wire.
    let (mut s, ih) = mk();
    let mut out = Vec::new();
    connect(&mut s, &ih, &mut out);
    out.clear();
    for i in 0..3 {
        let p = PeerPacket {
            info_hash: ih,
            peer: 1,
            remote: "127.0.0.1:6881".parse().unwrap(),
            msg: PeerMessage::Have(PieceIndex(i)),
        };
        s.on_peer_message(&p, 0, &mut out);
    }
    let n = out
        .iter()
        .filter(|e| {
            matches!(
                e,
                SessionOut::ToPeer {
                    msg: PeerMessage::Interested,
                    ..
                }
            )
        })
        .count();
    assert_eq!(n, 1, "{out:?}");
}
