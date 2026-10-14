// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! BitDaemon `.session` container — **extension, not an upstream
//! format**. Upstream persists the session as `session_params`
//! (`write_session_params`: settings/dht/dht-state/extensions/
//! ip-filter) and each torrent separately via `save_resume_data`;
//! there is no upstream single-file "session + torrents" blob.
//! This module is that convenience container for downstream users:
//! `{"version": 1, "torrents": {info_hash: resume-blob}, "dht": blob,
//! "settings": sparse-settings-dict, …}`. Each torrent's resume
//! blob is `write_resume_data_buf` output verbatim, so it stays
//! byte-compatible with libtorrent's per-torrent `.resume` files.

use std::collections::BTreeMap;

use bitdaemon_bencode::{BdecodeNode, Entry, bdecode, bencode};
use bitdaemon_common::Sha1Hash;
use bitdaemon_core::AddTorrentParams;
use bitdaemon_core::resume_data::{read_resume_data, write_resume_data_buf};
use bitdaemon_core::torrent_info::LoadTorrentLimits;

/// `torrents` map entry — resume blob per info-hash.
#[derive(Debug, Default)]
pub struct SessionState {
    /// `torrents` — `write_resume_data` output per swarm.
    pub torrents: BTreeMap<Sha1Hash, AddTorrentParams>,
    /// `dht` — opaque dht-state blob (`dht_tracker::save`).
    pub dht_state: Vec<u8>,
    /// `peer-class` counts etc. — opaque extension map.
    pub extra: BTreeMap<Vec<u8>, Vec<u8>>,
    /// `settings` — `settings_pack` persisted by upstream
    /// (`session_state::settings`).
    pub settings: Option<crate::settings::SessionSettings>,
}

/// `save_state` — `Entry` for the whole session.
#[must_use]
pub fn save_session_state(st: &SessionState) -> Entry {
    let mut d = Entry::Dict(std::collections::BTreeMap::new());
    if let Entry::Dict(m) = &mut d {
        m.insert(b"version".to_vec(), Entry::Int(1));
        let mut torrents = Entry::Dict(std::collections::BTreeMap::new());
        if let Entry::Dict(tm) = &mut torrents {
            for (ih, atp) in &st.torrents {
                tm.insert(
                    ih.as_bytes().to_vec(),
                    Entry::String(write_resume_data_buf(atp)),
                );
            }
        }
        m.insert(b"torrents".to_vec(), torrents);
        if !st.dht_state.is_empty() {
            m.insert(b"dht".to_vec(), Entry::String(st.dht_state.clone()));
        }
        if let Some(set) = &st.settings {
            m.insert(b"settings".to_vec(), save_settings(set));
        }
        for (k, v) in &st.extra {
            m.insert(k.clone(), Entry::String(v.clone()));
        }
    }
    d
}

/// `save_state` — the `.session` file bytes.
#[must_use]
pub fn save_session_state_buf(st: &SessionState) -> Vec<u8> {
    bencode(&save_session_state(st))
}

/// `load_state` — parse a `.session` file. Tolerant: unknown keys
/// land in `extra`, bad blobs are skipped.
pub fn load_session_state(buf: &[u8]) -> Option<SessionState> {
    let root = bdecode(buf);
    load_session_state_node(&root)
}

/// `load_session_state` on an already-decoded root node (lets the
/// caller keep `root` for a sparse settings overlay — upstream's
/// `settings_pack` only contains the keys the file carries).
#[must_use]
pub fn load_session_state_node(root: &BdecodeNode<'_>) -> Option<SessionState> {
    if root.node_type() != bitdaemon_bencode::NodeType::Dict {
        return None;
    }
    let mut st = SessionState::default();
    let t = root.dict_find_dict(b"torrents");
    if t.node_type() == bitdaemon_bencode::NodeType::Dict {
        for i in 0..t.dict_size() {
            let (k, v) = t.dict_at(i);
            if let (Some(ih), Some(blob)) =
                (Sha1Hash::from_bytes(k), Some(v.string_value().to_vec()))
            {
                if let Ok(atp) = read_resume_data(&blob, &LoadTorrentLimits::default()) {
                    st.torrents.insert(ih, atp);
                }
            }
        }
    }
    let dht = root.dict_find_string_value(b"dht", &[]);
    if !dht.is_empty() {
        st.dht_state = dht.to_vec();
    }
    let set = root.dict_find_dict(b"settings");
    if set.is_valid() {
        st.settings = Some(read_settings(&set));
    }
    Some(st)
}

/// `session_state::settings` — upstream stores the `settings_pack`
/// dict verbatim (same `save_settings_to_dict` format as
/// `write_session_params`), so we delegate: upstream key names,
/// upstream units, sparse (non-default) entries.
#[must_use]
pub fn save_settings(s: &crate::settings::SessionSettings) -> Entry {
    crate::session_params::save_settings_to_dict(s)
}

/// read back a `settings` dict — missing keys keep defaults
/// (forward/backward compatible like upstream).
#[must_use]
pub fn read_settings(n: &BdecodeNode<'_>) -> crate::settings::SessionSettings {
    crate::session_params::read_settings_dict(n)
}
