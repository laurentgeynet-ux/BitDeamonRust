// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session_params::session_state` — the `.session` file. Upstream
//! v2 serializes the whole session into one bencoded dict:
//! `{"torrents": {info_hash: resume-data}, "dht": dht-state,
//! "settings": …}`. We port the container; each torrent's resume
//! blob is `write_resume_data_buf` output verbatim.

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

/// `session_state::settings` — int/bool fields persist as ints,
/// durations as milliseconds.
#[must_use]
pub fn save_settings(s: &crate::settings::SessionSettings) -> Entry {
    let mut d = Entry::Dict(std::collections::BTreeMap::new());
    if let Entry::Dict(m) = &mut d {
        let ms = |x: std::time::Duration| x.as_millis() as i64;
        for (k, v) in [
            ("connections_limit", i64::from(s.connections_limit)),
            ("active_limit", i64::from(s.active_limit)),
            ("active_downloads", i64::from(s.active_downloads)),
            ("active_seeds", i64::from(s.active_seeds)),
            ("unchoke_slots", i64::from(s.unchoke_slots)),
            (
                "optimistic_unchoke_slots",
                i64::from(s.optimistic_unchoke_slots),
            ),
            ("request_timeout", ms(s.request_timeout)),
            ("peer_timeout", ms(s.peer_timeout)),
            ("request_queue_time", ms(s.request_queue_time)),
            ("max_allowed_in_queue", i64::from(s.max_allowed_in_queue)),
            ("upload_rate_limit", s.upload_rate_limit),
            ("download_rate_limit", s.download_rate_limit),
            ("piece_timeout", ms(s.piece_timeout)),
            ("max_disk_write_queue", s.max_disk_write_queue),
            ("send_buffer_watermark", s.send_buffer_watermark as i64),
            ("alert_queue_size", s.alert_queue_size as i64),
            ("dht_enabled", i64::from(s.dht_enabled)),
            ("prefer_rc4", i64::from(s.prefer_rc4)),
            ("connection_speed", i64::from(s.connection_speed)),
            ("tick_interval", ms(s.tick_interval)),
            (
                "web_seed_blocks_per_tick",
                i64::from(s.web_seed_blocks_per_tick),
            ),
            ("min_reconnect_time", i64::from(s.min_reconnect_time)),
            ("max_peerlist_size", i64::from(s.max_peerlist_size)),
            ("max_failcount", i64::from(s.max_failcount)),
            ("anonymous_mode", i64::from(s.anonymous_mode)),
        ] {
            m.insert(k.as_bytes().to_vec(), Entry::Int(v));
        }
    }
    d
}

/// read back a `settings` dict — missing keys keep defaults
/// (forward/backward compatible like upstream).
#[must_use]
pub fn read_settings(n: &BdecodeNode<'_>) -> crate::settings::SessionSettings {
    use std::time::Duration;
    let mut s = crate::settings::SessionSettings::default();
    let ms = |v: i64| Duration::from_millis(v.max(0) as u64);
    let g = |k: &str| n.dict_find_int_value(k.as_bytes(), -1);
    let b = |k: &str, cur: &mut bool| {
        let v = g(k);
        if v >= 0 {
            *cur = v != 0;
        }
    };
    let i = |k: &str, cur: &mut i32| {
        let v = g(k);
        if v >= 0 {
            *cur = v as i32;
        }
    };
    i("connections_limit", &mut s.connections_limit);
    i("active_limit", &mut s.active_limit);
    i("active_downloads", &mut s.active_downloads);
    i("active_seeds", &mut s.active_seeds);
    i("unchoke_slots", &mut s.unchoke_slots);
    i("optimistic_unchoke_slots", &mut s.optimistic_unchoke_slots);
    let v = g("request_timeout");
    if v >= 0 {
        s.request_timeout = ms(v);
    }
    let v = g("peer_timeout");
    if v >= 0 {
        s.peer_timeout = ms(v);
    }
    let v = g("request_queue_time");
    if v >= 0 {
        s.request_queue_time = ms(v);
    }
    i("max_allowed_in_queue", &mut s.max_allowed_in_queue);
    let v = g("upload_rate_limit");
    if v >= 0 {
        s.upload_rate_limit = v;
    }
    let v = g("download_rate_limit");
    if v >= 0 {
        s.download_rate_limit = v;
    }
    let v = g("piece_timeout");
    if v >= 0 {
        s.piece_timeout = ms(v);
    }
    let v = g("max_disk_write_queue");
    if v >= 0 {
        s.max_disk_write_queue = v;
    }
    let v = g("send_buffer_watermark");
    if v >= 0 {
        s.send_buffer_watermark = v as usize;
    }
    let v = g("alert_queue_size");
    if v >= 0 {
        s.alert_queue_size = v as usize;
    }
    b("dht_enabled", &mut s.dht_enabled);
    b("prefer_rc4", &mut s.prefer_rc4);
    i("connection_speed", &mut s.connection_speed);
    let v = g("tick_interval");
    if v >= 0 {
        s.tick_interval = ms(v);
    }
    i("web_seed_blocks_per_tick", &mut s.web_seed_blocks_per_tick);
    i("min_reconnect_time", &mut s.min_reconnect_time);
    i("max_peerlist_size", &mut s.max_peerlist_size);
    i("max_failcount", &mut s.max_failcount);
    b("anonymous_mode", &mut s.anonymous_mode);
    s
}

#[allow(dead_code)]
fn _node_ty(_: &BdecodeNode<'_>) {}
