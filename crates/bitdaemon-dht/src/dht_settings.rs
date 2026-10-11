// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/dht_settings.cpp` — `dht_settings` struct with
//! upstream defaults, plus `dht_load_state`/`dht_save_state`
//! (the `dht` sub-dict of the session file).

use bitdaemon_bencode::{BdecodeNode, Entry, NodeType};

/// `dht_settings` — verbatim field list and defaults from
/// `kademlia/dht_settings.hpp`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DhtSettings {
    /// `max_peers_reply = 100`.
    pub max_peers_reply: i32,
    /// `search_branching = 5`.
    pub search_branching: i32,
    /// `max_fail_count = 20`.
    pub max_fail_count: i32,
    /// `max_torrents = 2000`.
    pub max_torrents: i32,
    /// `max_dht_items = 700`.
    pub max_dht_items: i32,
    /// `max_peers = 500`.
    pub max_peers: i32,
    /// `max_torrent_search_reply = 20`.
    pub max_torrent_search_reply: i32,
    /// `restrict_routing_ips = true`.
    pub restrict_routing_ips: bool,
    /// `restrict_search_ips = true`.
    pub restrict_search_ips: bool,
    /// `extended_routing_table = true`.
    pub extended_routing_table: bool,
    /// `aggressive_lookups = true`.
    pub aggressive_lookups: bool,
    /// `privacy_lookups = false`.
    pub privacy_lookups: bool,
    /// `enforce_node_id = false`.
    pub enforce_node_id: bool,
    /// `ignore_dark_internet = true`.
    pub ignore_dark_internet: bool,
    /// `block_timeout = 5 * 60`.
    pub block_timeout: i32,
    /// `block_ratelimit = 5`.
    pub block_ratelimit: i32,
    /// `read_only = false`.
    pub read_only: bool,
    /// `item_lifetime = 0`.
    pub item_lifetime: i32,
    /// `upload_rate_limit = 8000`.
    pub upload_rate_limit: i32,
    /// `sample_infohashes_interval = 21600` (BEP-51).
    pub sample_infohashes_interval: i32,
    /// `max_infohashes_sample_count = 20`.
    pub max_infohashes_sample_count: i32,
    /// `prefer_verified_node_ids = true`.
    pub prefer_verified_node_ids: bool,
}

impl Default for DhtSettings {
    fn default() -> Self {
        Self {
            max_peers_reply: 100,
            search_branching: 5,
            max_fail_count: 20,
            max_torrents: 2000,
            max_dht_items: 700,
            max_peers: 500,
            max_torrent_search_reply: 20,
            restrict_routing_ips: true,
            restrict_search_ips: true,
            extended_routing_table: true,
            aggressive_lookups: true,
            privacy_lookups: false,
            enforce_node_id: false,
            ignore_dark_internet: true,
            block_timeout: 5 * 60,
            block_ratelimit: 5,
            read_only: false,
            item_lifetime: 0,
            upload_rate_limit: 8000,
            sample_infohashes_interval: 21600,
            max_infohashes_sample_count: 20,
            prefer_verified_node_ids: true,
        }
    }
}

/// `dht_load_state(e, sett)` — reads the `dht` dict.
pub fn dht_load_state(e: &BdecodeNode<'_>, sett: &mut DhtSettings) {
    if e.node_type() != NodeType::Dict {
        return;
    }
    macro_rules! int_field {
        ($key:expr, $field:ident) => {
            let v = e.dict_find_int($key);
            if v.node_type() == NodeType::Int {
                sett.$field = v.int_value() as i32;
            }
        };
    }
    macro_rules! bool_field {
        ($key:expr, $field:ident) => {
            let v = e.dict_find_int($key);
            if v.node_type() == NodeType::Int {
                sett.$field = v.int_value() != 0;
            }
        };
    }
    int_field!(b"max_peers_reply", max_peers_reply);
    int_field!(b"search_branching", search_branching);
    int_field!(b"max_fail_count", max_fail_count);
    int_field!(b"max_torrents", max_torrents);
    int_field!(b"max_dht_items", max_dht_items);
    int_field!(b"max_peers", max_peers);
    int_field!(b"max_torrent_search_reply", max_torrent_search_reply);
    bool_field!(b"restrict_routing_ips", restrict_routing_ips);
    bool_field!(b"restrict_search_ips", restrict_search_ips);
    bool_field!(b"extended_routing_table", extended_routing_table);
    bool_field!(b"aggressive_lookups", aggressive_lookups);
    bool_field!(b"privacy_lookups", privacy_lookups);
    bool_field!(b"enforce_node_id", enforce_node_id);
    bool_field!(b"ignore_dark_internet", ignore_dark_internet);
    int_field!(b"block_timeout", block_timeout);
    int_field!(b"block_ratelimit", block_ratelimit);
    bool_field!(b"read_only", read_only);
    int_field!(b"item_lifetime", item_lifetime);
    // ABI-3 fields (upstream gates load on TORRENT_ABI_VERSION)
    int_field!(b"upload_rate_limit", upload_rate_limit);
    int_field!(b"sample_infohashes_interval", sample_infohashes_interval);
    int_field!(b"max_infohashes_sample_count", max_infohashes_sample_count);
    bool_field!(b"prefer_verified_node_ids", prefer_verified_node_ids);
}

/// `dht_save_state` — writes the `dht` dict.
#[must_use]
pub fn dht_save_state(settings: &DhtSettings) -> Entry {
    let mut e = Entry::Dict(Default::default());
    let d = e.dict_mut().expect("dict");
    let mut i = |k: &[u8], v: i64| {
        d.insert(k.to_vec(), Entry::Int(v));
    };
    i(b"max_peers_reply", i64::from(settings.max_peers_reply));
    i(b"search_branching", i64::from(settings.search_branching));
    i(b"max_fail_count", i64::from(settings.max_fail_count));
    i(b"max_torrents", i64::from(settings.max_torrents));
    i(b"max_dht_items", i64::from(settings.max_dht_items));
    i(b"max_peers", i64::from(settings.max_peers));
    i(
        b"max_torrent_search_reply",
        i64::from(settings.max_torrent_search_reply),
    );
    i(
        b"restrict_routing_ips",
        settings.restrict_routing_ips as i64,
    );
    i(b"restrict_search_ips", settings.restrict_search_ips as i64);
    i(
        b"extended_routing_table",
        settings.extended_routing_table as i64,
    );
    i(b"aggressive_lookups", settings.aggressive_lookups as i64);
    i(b"privacy_lookups", settings.privacy_lookups as i64);
    i(b"enforce_node_id", settings.enforce_node_id as i64);
    i(
        b"ignore_dark_internet",
        settings.ignore_dark_internet as i64,
    );
    i(b"block_timeout", i64::from(settings.block_timeout));
    i(b"block_ratelimit", i64::from(settings.block_ratelimit));
    i(b"read_only", settings.read_only as i64);
    i(b"item_lifetime", i64::from(settings.item_lifetime));
    // upstream `save_dht_settings` writes only the 18 keys above;
    // the ABI-3 fields (upload_rate_limit, sample_infohashes_interval,
    // max_infohashes_sample_count, prefer_verified_node_ids) are read
    // but never written — `dht_save_state` here is that function, so
    // keep the output set identical for .session byte-compat.
    e
}
