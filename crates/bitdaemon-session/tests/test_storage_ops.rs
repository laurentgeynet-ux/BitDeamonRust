// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! storage ops — `move_storage`/`rename_file`/`delete_files`
//! completion plumbing → `storage_moved`/`file_renamed`/
//! `torrent_deleted` alerts.

use std::path::PathBuf;

use bitdaemon_common::types::{DownloadPriority, FileIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::{FileFlags, FileStorage};
use bitdaemon_disk::storage::MoveFlags;
use bitdaemon_disk::storage_error::{Operation, StorageError};
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::{DiskCompleted, Session};
use bitdaemon_session::session_handle::SessionHandle;
use bitdaemon_session::torrent::Torrent;

fn mk(ih: Sha1Hash) -> Session {
    let mut s = Session::new();
    // upstream tests opt into alert categories — default is error-only.
    s.alerts.set_mask(AlertCategory::ALL);
    s.add_torrent(Torrent::new(
        ih,
        2,
        128 * 1024,
        Bitfield::with_size(2),
        StorageIndex(0),
    ));
    s.pop_alerts();
    s
}

#[test]
fn move_storage_done_updates_path_and_alerts() {
    let ih = Sha1Hash::from_bytes(&[1; 20]).unwrap();
    let mut s = mk(ih);
    s.disk_completions()
        .send(DiskCompleted::MoveStorage {
            ih,
            path: PathBuf::from("/new/spot"),
            status: 0,
            err: StorageError::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert_eq!(s.torrents.get(&ih).unwrap().save_path, "/new/spot");
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::StorageMoved { path, .. } if path == "/new/spot"))
    );
}

#[test]
fn rename_and_delete_failures_alert() {
    let ih = Sha1Hash::from_bytes(&[2; 20]).unwrap();
    let mut s = mk(ih);
    let boom = || {
        StorageError::new(
            std::io::Error::from_raw_os_error(5),
            FileIndex(0),
            bitdaemon_disk::storage_error::Operation::FileRename,
        )
    };
    s.disk_completions()
        .send(DiskCompleted::RenameFile {
            ih,
            index: FileIndex(1),
            name: PathBuf::from("x.bin"),
            err: boom(),
        })
        .unwrap();
    s.disk_completions()
        .send(DiskCompleted::DeleteFiles { ih, err: boom() })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::FileRenameFailed { index: 1, .. }))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::TorrentDeleteFailed { .. }))
    );
}

#[test]
fn delete_files_without_disk_just_removes() {
    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut s = mk(ih);
    s.delete_files(&ih, 0, &mut Vec::new());
    assert!(!s.torrents.contains_key(&ih));
    let alerts = s.pop_alerts();
    // upstream `remove_torrent_impl`: `!m_storage` makes
    // `delete_files` return false → `torrent_delete_failed` (empty
    // error) posts *before* the removal alert.
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::TorrentDeleteFailed { error, .. } if error.is_empty()))
    );
    assert!(alerts.iter().any(|a| matches!(a, Alert::TorrentRemoved(_))));
}

/// `on_file_priority` failure — upstream runs `set_error` + `pause()`
/// (full `do_pause`: announce gates off, disconnect_all,
/// `torrent_paused_alert`), not a bare `m_paused` flip.
#[test]
fn file_priority_error_runs_full_pause() {
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut s = mk(ih);
    s.disk_completions()
        .send(DiskCompleted::SetFilePriority {
            ih,
            prios: vec![DownloadPriority::DEFAULT, DownloadPriority::DEFAULT],
            err: StorageError::new(
                std::io::Error::from_raw_os_error(5),
                FileIndex(0),
                Operation::FileOpen,
            ),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let t = s.torrents.get(&ih).unwrap();
    assert!(t.paused);
    assert!(!t.announce_trackers && !t.announce_dht && !t.announce_lsd);
    assert_eq!(t.last_error.as_str(), StorageError::new(
        std::io::Error::from_raw_os_error(5),
        FileIndex(0),
        Operation::FileOpen,
    ).to_string());
    let alerts = s.pop_alerts();
    // `file_error_alert` carries the operation name; `pause()` posts
    // `torrent_paused` (non-graceful leg — no peers to drain).
    assert!(alerts.iter().any(
        |a| matches!(a, Alert::FileError { op, .. } if op == "file_open")
    ));
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::TorrentPaused { ih: h } if *h == ih))
    );
}

/// metadata-less `move_storage` — the alert carries the *requested*
/// path while `save_path` stores the `complete()`'d form
/// (upstream `torrent::move_storage`, `storage_moved_alert(handle,
/// save_path, m_save_path)`).
#[test]
fn move_storage_without_metadata_posts_requested_path() {
    let ih = Sha1Hash::from_bytes(&[5; 20]).unwrap();
    let mut s = mk(ih);
    // `mk()`'s torrent has no `files` — the `!valid_metadata()` leg.
    s.move_storage(&ih, PathBuf::from("rel/spot"), MoveFlags::AlwaysReplaceFiles);
    let t = s.torrents.get(&ih).unwrap();
    // complete() absolutizes — the stored path is no longer the raw arg.
    assert_ne!(t.save_path, "rel/spot");
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::StorageMoved { path, old_path, .. } if path == "rel/spot" && old_path.is_empty()))
    );
}

/// `file_priorities` getter resizes to `num_files` with
/// `default_priority` once metadata is valid; `file_priority(index)`
/// guards `dont_download` for pads/OOB and `default_priority` for
/// unallocated slots (upstream `torrent::file_priorities` /
/// `torrent::file_priority`).
#[test]
fn file_priority_getters() {
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let mut fs = FileStorage::new();
    fs.add_file(
        Some(b"a"),
        b"root/a",
        0x18000,
        FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.add_file(
        Some(b"pad"),
        b"root/.pad",
        0x8000,
        FileFlags::PAD_FILE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.set_piece_length(0x10000);
    fs.set_num_pieces(2);
    let mut t = Torrent::new(ih, 2, 0x10000, Bitfield::with_size(2), StorageIndex(0));
    t.files = Some(std::sync::Arc::new(fs));
    // stored vector deliberately short — a metadata-less
    // `set_file_priority` may only cover index 0.
    t.file_priorities = vec![DownloadPriority::TOP];

    let h = SessionHandle::wrap(std::sync::Arc::new(std::sync::Mutex::new(
        Session::new(),
    )));
    let th = h.add_torrent(t);
    assert_eq!(
        th.file_priorities(),
        vec![DownloadPriority::TOP, DownloadPriority::DEFAULT]
    );
    assert_eq!(
        th.get_file_priority(FileIndex(0)),
        DownloadPriority::TOP
    );
    // pad file always reports dont_download
    assert_eq!(
        th.get_file_priority(FileIndex(1)),
        DownloadPriority::DONT_DOWNLOAD
    );
    // out-of-range and negative → dont_download
    assert_eq!(
        th.get_file_priority(FileIndex(2)),
        DownloadPriority::DONT_DOWNLOAD
    );
    assert_eq!(
        th.get_file_priority(FileIndex(-1)),
        DownloadPriority::DONT_DOWNLOAD
    );
}
