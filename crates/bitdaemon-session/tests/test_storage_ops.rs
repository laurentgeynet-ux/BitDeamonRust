// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! storage ops — `move_storage`/`rename_file`/`delete_files`
//! completion plumbing → `storage_moved`/`file_renamed`/
//! `torrent_deleted` alerts.

use std::path::PathBuf;

use bitdaemon_common::types::{FileIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_disk::storage_error::StorageError;
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::{DiskCompleted, Session};
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
    let boom = || StorageError {
        io: std::io::Error::from_raw_os_error(5),
        file: FileIndex(0),
        operation: bitdaemon_disk::storage_error::Operation::FileRename,
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
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::TorrentRemoved(_)))
    );
}
