// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! A20 — torrent state-machine parity: `stop_when_ready` edges,
//! per-transition `state_changed` alerts, `close_redundant_
//! connections` gating, the `need_save_resume` dirty bitmask and
//! the demotion-vs-pause save-marking split.

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_session::alerts::AlertCategory;
use bitdaemon_session::session::Session;
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::status::TorrentState;
use bitdaemon_session::torrent::{Torrent, TorrentEffect};

fn torrent(pieces: i32) -> Torrent {
    Torrent::new(
        Sha1Hash::from_bytes(&[7u8; 20]).unwrap(),
        pieces,
        0x40000,
        Bitfield::with_size(pieces as usize),
        StorageIndex(0),
    )
}

/// a torrent parked in `checking_resume_data`, like a fresh
/// fastresume add before the verdict lands.
fn checking_resume_torrent(pieces: i32) -> Torrent {
    let mut t = torrent(pieces);
    t.init_picker(false);
    t.checking = true;
    t.checking_resume = true;
    t.state_prev = Some(t.state());
    t
}

#[test]
fn stop_when_ready_fires_on_checking_to_downloading_edge() {
    let mut t = checking_resume_torrent(2);
    t.stop_when_ready = true;
    t.auto_managed = true;
    let mut out = Vec::new();
    t.on_files_checked(
        true,
        &SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    // upstream `set_state`'s `trigger_stop` — leaving the checking
    // states into a downloading state is THE edge.
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::StopWhenReadyCheck)),
        "checking→downloading must surface the stop_when_ready edge"
    );
    // the state_changed edge lands the same way.
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::StateChangedEdge {
            prev: TorrentState::CheckingResumeData,
            next: TorrentState::Downloading,
        }
    )));
}

#[test]
fn stop_when_ready_check_pauses_and_disarms() {
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 0x40000, Bitfield::with_size(2), StorageIndex(0));
    t.init_picker(false);
    // already downloading, flag armed (e.g. via `atp.flags`).
    t.stop_when_ready = true;
    t.auto_managed = true;
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s.add_torrent(t);
    s.route_into_pending(ih, vec![TorrentEffect::StopWhenReadyCheck]);
    let t = s.torrents.get(&ih).unwrap();
    // upstream trigger: `auto_managed(false); pause();
    // m_stop_when_ready = false;`
    assert!(!t.stop_when_ready);
    assert!(!t.auto_managed);
    assert!(t.paused);
    // `pause()` marks `if_state_changed` resume-dirty.
    assert!(
        t.need_save_resume
            .contains(bitdaemon_common::flags::ResumeDataFlags::IF_STATE_CHANGED)
    );
}

#[test]
fn stop_when_ready_setter_triggers_when_already_downloading() {
    let mut t = checking_resume_torrent(2);
    let settings = SessionSettings::default();
    let counters = bitdaemon_session::counters::Counters::default();
    let mut out = Vec::new();
    // still checking — the setter only arms, no immediate trigger.
    t.set_stop_when_ready(true, &mut out);
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::StopWhenReadyCheck))
    );
    // resolve the check → downloading; then a second arming while
    // already in a downloading state fires immediately upstream
    // (`stop_when_ready` race-avoidance comment).
    t.on_files_checked(true, &settings, &counters, &mut out);
    assert!(t.is_downloading_state());
    out.clear();
    t.set_stop_when_ready(true, &mut out);
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::StopWhenReadyCheck)),
        "setting the flag on an already-downloading torrent must \
         trigger the stop sequence"
    );
}

#[test]
fn stop_when_ready_no_edge_between_downloading_states() {
    let mut t = checking_resume_torrent(2);
    let settings = SessionSettings::default();
    let counters = bitdaemon_session::counters::Counters::default();
    let mut out = Vec::new();
    t.on_files_checked(true, &settings, &counters, &mut out);
    assert!(t.is_downloading_state());
    // armed mid-download: `downloading`→`finished` is *not* the
    // non-downloading→downloading edge upstream triggers on
    // (torrent.cpp:12364).
    t.stop_when_ready = true;
    out.clear();
    t.set_state(TorrentState::Finished, &mut out);
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::StopWhenReadyCheck))
    );
    // but the state_changed edge itself posts.
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::StateChangedEdge {
            prev: TorrentState::Downloading,
            next: TorrentState::Finished,
        }
    )));
}

#[test]
fn state_changed_emits_once_per_transition() {
    let mut t = checking_resume_torrent(2);
    let settings = SessionSettings::default();
    let counters = bitdaemon_session::counters::Counters::default();
    let mut out = Vec::new();
    // a complete seed on resume-verify: upstream walks
    // `checking_resume_data`→`finished`→`seeding` — two
    // `state_changed_alert`s, not one coalesced tick diff.
    for i in 0..2 {
        t.have.set_bit(i);
    }
    if let Some(p) = t.picker.as_mut() {
        for i in 0..2 {
            for b in 0..p.blocks_in_piece(PieceIndex(i)) {
                p.mark_as_finished(
                    bitdaemon_common::types::PieceBlock {
                        piece_index: PieceIndex(i),
                        block_index: b,
                    },
                    None,
                );
            }
        }
    }
    t.on_files_checked(true, &settings, &counters, &mut out);
    let edges: Vec<_> = out
        .iter()
        .filter_map(|e| {
            if let TorrentEffect::StateChangedEdge { prev, next } = e {
                Some((*prev, *next))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        edges,
        [
            (TorrentState::CheckingResumeData, TorrentState::Finished),
            (TorrentState::Finished, TorrentState::Seeding),
        ],
        "each set_state transition posts its own alert"
    );
    // `torrent_checked_alert` posts *after* the transitions.
    let checked_pos = out
        .iter()
        .position(|e| {
            matches!(
                e,
                TorrentEffect::Alert(bitdaemon_session::alerts::Alert::TorrentChecked(_))
            )
        })
        .unwrap();
    let last_edge_pos = out
        .iter()
        .rposition(|e| matches!(e, TorrentEffect::StateChangedEdge { .. }))
        .unwrap();
    assert!(checked_pos > last_edge_pos);
}

#[test]
fn finished_disconnects_upload_only_only_when_close_redundant() {
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    for close in [false, true] {
        let mut t = torrent(2);
        t.init_picker(false);
        t.close_redundant_connections = close;
        // one piece already had, one incoming: peer is upload-only
        // (it has everything we want).
        t.have.set_bit(0);
        if let Some(p) = t.picker.as_mut() {
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
        let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
        c.remote_upload_only = true;
        t.add_peer(1, c, &bitdaemon_session::counters::Counters::default());
        let mut out = Vec::new();
        // verify the last piece → `finished()` runs.
        if let Some(p) = t.picker.as_mut() {
            for b in 0..p.blocks_in_piece(PieceIndex(1)) {
                p.mark_as_finished(
                    bitdaemon_common::types::PieceBlock {
                        piece_index: PieceIndex(1),
                        block_index: b,
                    },
                    None,
                );
            }
        }
        t.piece_verified(
            PieceIndex(1),
            true,
            &[],
            &SessionSettings::default(),
            &bitdaemon_session::counters::Counters::default(),
            &mut out,
        );
        let disconnected = out
            .iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { peer: 1, .. }));
        assert_eq!(
            disconnected, close,
            "upload-only disconnect is gated on close_redundant_connections"
        );
    }
}

#[test]
fn resume_dirty_bitmask_filters_save_categories() {
    use bitdaemon_common::flags::ResumeDataFlags as RF;
    let ih = Sha1Hash::from_bytes(&[5; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 0x40000, Bitfield::with_size(2), StorageIndex(0));
    t.init_picker(false);
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    // dirty ONLY the config category — a state/category mismatch
    // must post `resume_data_not_modified` upstream.
    t.need_save_resume = RF::IF_CONFIG_CHANGED;
    s.add_torrent(t);
    s.pop_alerts();
    s.save_resume_data(&ih, RF::IF_DOWNLOAD_PROGRESS);
    assert!(
        s.pop_alerts().iter().any(|a| matches!(
            a,
            bitdaemon_session::alerts::Alert::SaveResumeDataFailed { .. }
        )),
        "a dirty mask that misses the requested category must fail"
    );
    // mask is NOT consumed by the failed save.
    assert!(s.torrents[&ih].need_save_resume.is_set());
    // matching category → save succeeds and clears the mask.
    s.save_resume_data(&ih, RF::IF_CONFIG_CHANGED);
    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, bitdaemon_session::alerts::Alert::SaveResumeData { .. }))
    );
    assert!(!s.torrents[&ih].need_save_resume.is_set());
}

#[test]
fn demotion_pause_does_not_mark_resume_dirty() {
    use bitdaemon_common::flags::ResumeDataFlags as RF;
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 0x40000, Bitfield::with_size(2), StorageIndex(0));
    t.init_picker(false);
    t.auto_managed = true;
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s.add_torrent(t);
    // `pause()` (API) marks `if_state_changed`; upstream
    // `set_paused` — the auto-manage demotion path — does not.
    s.set_paused(&ih, true, &mut Vec::new());
    assert!(
        s.torrents[&ih]
            .need_save_resume
            .contains(RF::IF_STATE_CHANGED),
        "torrent::pause() marks if_state_changed"
    );
    // clean state → demote through the unmarked path: `manage_queue`
    // with a zero download limit graceful-pauses via `set_paused`,
    // upstream's non-marking call.
    s.torrents.get_mut(&ih).unwrap().need_save_resume = RF::NONE;
    s.torrents.get_mut(&ih).unwrap().paused = false;
    s.torrents.get_mut(&ih).unwrap().announce_dht = true;
    s.torrents.get_mut(&ih).unwrap().announce_trackers = true;
    s.torrents.get_mut(&ih).unwrap().announce_lsd = true;
    s.settings.active_limit = 0;
    s.settings.active_downloads = 0;
    let mut out = Vec::new();
    s.manage_queue(&mut out);
    assert!(
        s.torrents[&ih].paused,
        "a zero active limit must demote the downloader"
    );
    assert!(
        !s.torrents[&ih].need_save_resume.is_set(),
        "set_paused (demotion) must not mark resume-dirty"
    );
}

#[test]
fn resume_download_requeues_at_tail() {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    let ih_a = Sha1Hash::from_bytes(&[1; 20]).unwrap();
    let ih_b = Sha1Hash::from_bytes(&[2; 20]).unwrap();
    for ih in [ih_a, ih_b] {
        let mut t = Torrent::new(ih, 2, 0x40000, Bitfield::with_size(2), StorageIndex(0));
        t.init_picker(false);
        s.add_torrent(t);
    }
    // A finishes → leaves the download queue (upstream
    // `finished()` → `set_queue_position(no_pos)`).
    s.torrents.get_mut(&ih_a).unwrap().finished = true;
    assert_eq!(s.queue_position(&ih_a), -1);
    // upstream `resume_download()` → `set_queue_position(last_pos)`
    // — A re-joins *behind* B.
    s.route_into_pending(ih_a, vec![TorrentEffect::QueueTail]);
    s.torrents.get_mut(&ih_a).unwrap().finished = false;
    assert_eq!(s.queue_position(&ih_b), 0);
    assert_eq!(s.queue_position(&ih_a), 1);
}
