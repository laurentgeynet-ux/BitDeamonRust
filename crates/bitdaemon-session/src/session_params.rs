//! Port of `session_params.cpp` — `write_session_params` /
//! `read_session_params`: the serialized `session_params` dict
//! (`"dht"`, `"dht state"`, `"settings"`, `"extensions"`,
//! `"ip_filter4"`, `"ip_filter6"`). This is the format upstream uses
//! for `session::save_state`/`.session` params — distinct from our
//! `session_state` torrent-list container.
//!
//! Upstream `settings` is a sparse bag: only *explicitly set* values
//! are serialized. Since `SessionSettings` is a plain struct, we
//! serialize fields that differ from `SessionSettings::default()`.

use std::net::IpAddr;

use bitdaemon_bencode::{BdecodeNode, Entry, NodeType, bdecode, bencode};
use bitdaemon_dht::dht_settings::{dht_load_state, dht_save_state};
use bitdaemon_dht::dht_state::{read_dht_state, save_dht_state};
use bitdaemon_net::ip_filter::IpFilter;
use bitdaemon_net::socket_io::{Cursor, read_address, write_address, write_u32};

use crate::settings::{SessionParams, SessionSettings};

/// `write_session_params` — the params dict, upstream key layout.
#[must_use]
pub fn write_session_params(sp: &SessionParams) -> Entry {
    let mut d = bitdaemon_bencode::Dictionary::new();

    d.insert(b"dht".to_vec(), dht_save_state(&sp.dht_settings));
    d.insert(b"dht state".to_vec(), save_dht_state(&sp.dht_state));
    d.insert(b"settings".to_vec(), save_settings_to_dict(&sp.settings));

    let mut m = bitdaemon_bencode::Dictionary::new();
    for (k, v) in &sp.ext_state {
        m.insert(k.clone(), Entry::String(v.clone()));
    }
    d.insert(b"extensions".to_vec(), Entry::Dict(m));

    let (v4, v6) = sp.ip_filter.export_filter();
    if !v4.is_empty() {
        let mut l = Vec::with_capacity(v4.len());
        for r in v4 {
            let mut s = Vec::with_capacity(12);
            write_address(IpAddr::V4(r.first), &mut s);
            write_address(IpAddr::V4(r.last), &mut s);
            write_u32(r.flags, &mut s);
            l.push(Entry::String(s));
        }
        d.insert(b"ip_filter4".to_vec(), Entry::List(l));
    }
    if !v6.is_empty() {
        let mut l = Vec::with_capacity(v6.len());
        for r in v6 {
            let mut s = Vec::with_capacity(36);
            write_address(IpAddr::V6(r.first), &mut s);
            write_address(IpAddr::V6(r.last), &mut s);
            write_u32(r.flags, &mut s);
            l.push(Entry::String(s));
        }
        d.insert(b"ip_filter6".to_vec(), Entry::List(l));
    }

    Entry::Dict(d)
}

/// `write_session_params_buf`.
#[must_use]
pub fn write_session_params_buf(sp: &SessionParams) -> Vec<u8> {
    bencode(&write_session_params(sp))
}

/// `read_session_params` — tolerant: missing keys keep defaults;
/// unknown settings keys are ignored (forward compat).
#[must_use]
pub fn read_session_params(buf: &[u8]) -> SessionParams {
    let e = bdecode(buf);
    read_session_params_node(&e)
}

/// `read_session_params` on an already-decoded node.
#[must_use]
pub fn read_session_params_node(e: &BdecodeNode<'_>) -> SessionParams {
    let mut params = SessionParams::default();

    let dht = e.dict_find_dict(b"dht");
    dht_load_state(&dht, &mut params.dht_settings);
    // upstream `apply_deprecated_dht_settings`: the legacy "dht"
    // dict's keys also land in the flat `dht_*` settings —
    // applied *before* the "settings" dict, which then overrides
    // per-key.
    apply_dht_settings_dict(&mut params.settings, &dht);

    let st = e.dict_find_dict(b"dht state");
    if st.node_type() == NodeType::Dict {
        params.dht_state = read_dht_state(&st);
    }

    let sett = e.dict_find_dict(b"settings");
    if sett.node_type() == NodeType::Dict {
        // sparse overlay on top of the dht-derived keys —
        // `read_settings_dict` would reset to defaults and lose
        // them.
        apply_settings_dict(&mut params.settings, &sett);
    }

    let ext = e.dict_find_dict(b"extensions");
    if ext.node_type() == NodeType::Dict {
        for i in 0..ext.dict_size() {
            let (k, v) = ext.dict_at(i);
            if v.node_type() == NodeType::String {
                params
                    .ext_state
                    .insert(k.to_vec(), v.string_value().to_vec());
            }
        }
    }

    load_ip_filter(e, &mut params.ip_filter);
    params
}

fn load_ip_filter(e: &BdecodeNode<'_>, filter: &mut IpFilter) {
    for (key, v6) in [(b"ip_filter4".as_slice(), false), (b"ip_filter6", true)] {
        let list = e.dict_find_list(key);
        if list.node_type() != NodeType::List {
            continue;
        }
        let stride = if v6 { 36 } else { 12 };
        for i in 0..list.list_size() {
            let s = list.list_string_value_at(i, &[]);
            if s.len() < stride {
                continue;
            }
            let mut cur = Cursor::new(s);
            let (Ok(first), Ok(last), Ok(flags)) = (
                read_address(&mut cur, v6),
                read_address(&mut cur, v6),
                cur.read_u32(),
            ) else {
                continue;
            };
            // upstream: ignore invalid entries
            if first > last {
                continue;
            }
            filter.add_rule(first, last, flags);
        }
    }
}

/// Int-codec for enum-typed settings: the `settings_pack` wire
/// values map onto typed Rust enums (unknown ints fall back to the
/// upstream default, like upstream's unvalidated enum slots).
trait SettingInt: Sized {
    fn to_int(self) -> i64;
    fn from_int(v: i64) -> Self;
}

impl SettingInt for crate::settings::SuggestMode {
    fn to_int(self) -> i64 {
        match self {
            Self::NoPieceSuggestions => 0,
            Self::SuggestReadCache => 1,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            1 => Self::SuggestReadCache,
            _ => Self::NoPieceSuggestions,
        }
    }
}

impl SettingInt for crate::settings::DiskIoMode {
    fn to_int(self) -> i64 {
        match self {
            Self::EnableOsCache => 0,
            Self::DisableOsCacheForAlignedFiles => 1,
            Self::DisableOsCache => 2,
            Self::WriteThrough => 3,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            1 => Self::DisableOsCacheForAlignedFiles,
            2 => Self::DisableOsCache,
            3 => Self::WriteThrough,
            _ => Self::EnableOsCache,
        }
    }
}

impl SettingInt for crate::settings::BandwidthMixedAlgo {
    fn to_int(self) -> i64 {
        match self {
            Self::PreferTcp => 0,
            Self::PeerProportional => 1,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            0 => Self::PreferTcp,
            _ => Self::PeerProportional,
        }
    }
}

impl SettingInt for crate::settings::MmapWriteMode {
    fn to_int(self) -> i64 {
        match self {
            Self::AlwaysPwrite => 0,
            Self::AlwaysMmapWrite => 1,
            Self::AutoMmapWrite => 2,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            0 => Self::AlwaysPwrite,
            2 => Self::AutoMmapWrite,
            _ => Self::AlwaysMmapWrite,
        }
    }
}

impl SettingInt for crate::choker::ChokingAlgorithm {
    fn to_int(self) -> i64 {
        match self {
            // upstream: fixed_slots_choker = 0, rate_based_choker = 2
            Self::FixedSlots => 0,
            Self::RateBased => 2,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            2 => Self::RateBased,
            _ => Self::FixedSlots,
        }
    }
}

impl SettingInt for crate::choker::SeedChokingAlgorithm {
    fn to_int(self) -> i64 {
        match self {
            Self::RoundRobin => 0,
            Self::FastestUpload => 1,
            Self::AntiLeech => 2,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            1 => Self::FastestUpload,
            2 => Self::AntiLeech,
            _ => Self::RoundRobin,
        }
    }
}

impl SettingInt for bitdaemon_net::pe_crypto::EncPolicy {
    fn to_int(self) -> i64 {
        match self {
            Self::Forced => 0,
            Self::Enabled => 1,
            Self::Disabled => 2,
        }
    }
    fn from_int(v: i64) -> Self {
        // `sanitize_settings` — an out-of-range enc_policy
        // deserializes to `pe_disabled` upstream.
        match v {
            0 => Self::Forced,
            1 => Self::Enabled,
            _ => Self::Disabled,
        }
    }
}

impl SettingInt for bitdaemon_net::proxy::ProxyType {
    fn to_int(self) -> i64 {
        match self {
            Self::None => 0,
            Self::Socks4 => 1,
            Self::Socks5 => 2,
            Self::Socks5Password => 3,
            Self::Http => 4,
            Self::HttpPassword => 5,
            Self::I2p => 6,
        }
    }
    fn from_int(v: i64) -> Self {
        match v {
            1 => Self::Socks4,
            2 => Self::Socks5,
            3 => Self::Socks5Password,
            4 => Self::Http,
            5 => Self::HttpPassword,
            6 => Self::I2p,
            _ => Self::None,
        }
    }
}

/// `save_settings_to_dict` — sparse: only non-default fields are
/// written, under upstream `settings_pack` key names and units.
#[must_use]
pub fn save_settings_to_dict(s: &SessionSettings) -> Entry {
    let dflt = SessionSettings::default();
    let mut d = bitdaemon_bencode::Dictionary::new();

    macro_rules! put {
        ($f:ident, $k:literal, str) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::String(s.$f.clone()));
            }
        };
        ($f:ident, $k:literal, boolean) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(i64::from(s.$f)));
            }
        };
        ($f:ident, $k:literal, i32) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(i64::from(s.$f)));
            }
        };
        ($f:ident, $k:literal, u8i) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(i64::from(s.$f)));
            }
        };
        ($f:ident, $k:literal, i64) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(s.$f));
            }
        };
        ($f:ident, $k:literal, usize) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(s.$f as i64));
            }
        };
        ($f:ident, $k:literal, sec) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(s.$f.as_secs() as i64));
            }
        };
        ($f:ident, $k:literal, ms) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(s.$f.as_millis() as i64));
            }
        };
        ($f:ident, $k:literal, en) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(SettingInt::to_int(s.$f)));
            }
        };
        ($f:ident, $k:literal, amask) => {
            if s.$f != dflt.$f {
                d.insert($k.to_vec(), Entry::Int(i64::from(s.$f.bits())));
            }
        };
    }

    // string settings
    put!(user_agent, b"user_agent", str);
    put!(announce_ip, b"announce_ip", str);
    put!(mmap_cache, b"mmap_cache", str);
    put!(handshake_client_version, b"handshake_client_version", str);
    put!(outgoing_interfaces, b"outgoing_interfaces", str);
    put!(listen_interfaces, b"listen_interfaces", str);
    put!(proxy_hostname, b"proxy_hostname", str);
    put!(proxy_username, b"proxy_username", str);
    put!(proxy_password, b"proxy_password", str);
    put!(i2p_hostname, b"i2p_hostname", str);
    put!(peer_fingerprint, b"peer_fingerprint", str);
    put!(dht_bootstrap_nodes, b"dht_bootstrap_nodes", str);
    put!(natpmp_gateway, b"natpmp_gateway", str);
    put!(webtorrent_stun_server, b"webtorrent_stun_server", str);

    // bool settings
    put!(
        allow_multiple_connections_per_ip,
        b"allow_multiple_connections_per_ip",
        boolean
    );
    put!(
        ignore_limits_on_local_network,
        b"ignore_limits_on_local_network",
        boolean
    );
    put!(send_redundant_have, b"send_redundant_have", boolean);
    put!(lazy_bitfields, b"lazy_bitfields", boolean);
    put!(use_dht_as_fallback, b"use_dht_as_fallback", boolean);
    put!(upnp_ignore_nonrouters, b"upnp_ignore_nonrouters", boolean);
    put!(use_parole_mode, b"use_parole_mode", boolean);
    put!(use_read_cache, b"use_read_cache", boolean);
    put!(use_write_cache, b"use_write_cache", boolean);
    put!(dont_flush_write_cache, b"dont_flush_write_cache", boolean);
    put!(coalesce_reads, b"coalesce_reads", boolean);
    put!(coalesce_writes, b"coalesce_writes", boolean);
    put!(
        auto_manage_prefer_seeds,
        b"auto_manage_prefer_seeds",
        boolean
    );
    put!(
        dont_count_slow_torrents,
        b"dont_count_slow_torrents",
        boolean
    );
    put!(
        close_redundant_connections,
        b"close_redundant_connections",
        boolean
    );
    put!(
        prioritize_partial_pieces,
        b"prioritize_partial_pieces",
        boolean
    );
    put!(rate_limit_ip_overhead, b"rate_limit_ip_overhead", boolean);
    put!(
        announce_to_all_trackers,
        b"announce_to_all_trackers",
        boolean
    );
    put!(announce_to_all_tiers, b"announce_to_all_tiers", boolean);
    put!(prefer_udp_trackers, b"prefer_udp_trackers", boolean);
    put!(strict_super_seeding, b"strict_super_seeding", boolean);
    put!(lock_disk_cache, b"lock_disk_cache", boolean);
    put!(disable_hash_checks, b"disable_hash_checks", boolean);
    put!(allow_i2p_mixed, b"allow_i2p_mixed", boolean);
    put!(low_prio_disk, b"low_prio_disk", boolean);
    put!(volatile_read_cache, b"volatile_read_cache", boolean);
    put!(guided_read_cache, b"guided_read_cache", boolean);
    put!(no_atime_storage, b"no_atime_storage", boolean);
    put!(
        incoming_starts_queued_torrents,
        b"incoming_starts_queued_torrents",
        boolean
    );
    put!(report_true_downloaded, b"report_true_downloaded", boolean);
    put!(strict_end_game_mode, b"strict_end_game_mode", boolean);
    put!(broadcast_lsd, b"broadcast_lsd", boolean);
    put!(enable_outgoing_utp, b"enable_outgoing_utp", boolean);
    put!(enable_incoming_utp, b"enable_incoming_utp", boolean);
    put!(enable_outgoing_tcp, b"enable_outgoing_tcp", boolean);
    put!(enable_incoming_tcp, b"enable_incoming_tcp", boolean);
    put!(
        ignore_resume_timestamps,
        b"ignore_resume_timestamps",
        boolean
    );
    put!(
        no_recheck_incomplete_resume,
        b"no_recheck_incomplete_resume",
        boolean
    );
    put!(anonymous_mode, b"anonymous_mode", boolean);
    put!(
        report_web_seed_downloads,
        b"report_web_seed_downloads",
        boolean
    );
    put!(rate_limit_utp, b"rate_limit_utp", boolean);
    put!(announce_double_nat, b"announce_double_nat", boolean);
    put!(
        seeding_outgoing_connections,
        b"seeding_outgoing_connections",
        boolean
    );
    put!(
        no_connect_privileged_ports,
        b"no_connect_privileged_ports",
        boolean
    );
    put!(smooth_connects, b"smooth_connects", boolean);
    put!(always_send_user_agent, b"always_send_user_agent", boolean);
    put!(
        apply_ip_filter_to_trackers,
        b"apply_ip_filter_to_trackers",
        boolean
    );
    put!(use_disk_read_ahead, b"use_disk_read_ahead", boolean);
    put!(lock_files, b"lock_files", boolean);
    put!(contiguous_recv_buffer, b"contiguous_recv_buffer", boolean);
    put!(ban_web_seeds, b"ban_web_seeds", boolean);
    put!(
        allow_partial_disk_writes,
        b"allow_partial_disk_writes",
        boolean
    );
    put!(force_proxy, b"force_proxy", boolean);
    put!(support_share_mode, b"support_share_mode", boolean);
    put!(support_merkle_torrents, b"support_merkle_torrents", boolean);
    put!(report_redundant_bytes, b"report_redundant_bytes", boolean);
    put!(
        listen_system_port_fallback,
        b"listen_system_port_fallback",
        boolean
    );
    put!(use_disk_cache_pool, b"use_disk_cache_pool", boolean);
    put!(announce_crypto_support, b"announce_crypto_support", boolean);
    put!(enable_upnp, b"enable_upnp", boolean);
    put!(enable_natpmp, b"enable_natpmp", boolean);
    put!(enable_lsd, b"enable_lsd", boolean);
    put!(enable_dht, b"enable_dht", boolean);
    put!(prefer_rc4, b"prefer_rc4", boolean);
    put!(proxy_hostnames, b"proxy_hostnames", boolean);
    put!(proxy_peer_connections, b"proxy_peer_connections", boolean);
    put!(auto_sequential, b"auto_sequential", boolean);
    put!(
        proxy_tracker_connections,
        b"proxy_tracker_connections",
        boolean
    );
    put!(enable_ip_notifier, b"enable_ip_notifier", boolean);
    put!(
        dht_prefer_verified_node_ids,
        b"dht_prefer_verified_node_ids",
        boolean
    );
    put!(
        dht_restrict_routing_ips,
        b"dht_restrict_routing_ips",
        boolean
    );
    put!(dht_restrict_search_ips, b"dht_restrict_search_ips", boolean);
    put!(
        dht_extended_routing_table,
        b"dht_extended_routing_table",
        boolean
    );
    put!(dht_aggressive_lookups, b"dht_aggressive_lookups", boolean);
    put!(dht_privacy_lookups, b"dht_privacy_lookups", boolean);
    put!(dht_enforce_node_id, b"dht_enforce_node_id", boolean);
    put!(
        dht_ignore_dark_internet,
        b"dht_ignore_dark_internet",
        boolean
    );
    put!(dht_read_only, b"dht_read_only", boolean);
    put!(piece_extent_affinity, b"piece_extent_affinity", boolean);
    put!(validate_https_trackers, b"validate_https_trackers", boolean);
    put!(ssrf_mitigation, b"ssrf_mitigation", boolean);
    put!(allow_idna, b"allow_idna", boolean);
    put!(
        enable_set_file_valid_data,
        b"enable_set_file_valid_data",
        boolean
    );
    put!(
        socks5_udp_send_local_ep,
        b"socks5_udp_send_local_ep",
        boolean
    );
    put!(
        proxy_send_host_in_connect,
        b"proxy_send_host_in_connect",
        boolean
    );
    put!(
        disk_disable_copy_on_write,
        b"disk_disable_copy_on_write",
        boolean
    );
    put!(
        allow_multiple_connections_per_pid,
        b"allow_multiple_connections_per_pid",
        boolean
    );
    put!(apply_filter_to_dht, b"apply_filter_to_dht", boolean);

    // int settings
    put!(
        tracker_completion_timeout,
        b"tracker_completion_timeout",
        sec
    );
    put!(tracker_receive_timeout, b"tracker_receive_timeout", sec);
    put!(stop_tracker_timeout, b"stop_tracker_timeout", sec);
    put!(
        tracker_maximum_response_length,
        b"tracker_maximum_response_length",
        i64
    );
    put!(piece_timeout, b"piece_timeout", sec);
    put!(request_timeout, b"request_timeout", sec);
    put!(request_queue_time, b"request_queue_time", sec);
    put!(
        max_allowed_in_request_queue,
        b"max_allowed_in_request_queue",
        i32
    );
    put!(max_out_request_queue, b"max_out_request_queue", i32);
    put!(whole_pieces_threshold, b"whole_pieces_threshold", i32);
    put!(peer_timeout, b"peer_timeout", sec);
    put!(urlseed_timeout, b"urlseed_timeout", sec);
    put!(urlseed_pipeline_size, b"urlseed_pipeline_size", i32);
    put!(urlseed_wait_retry, b"urlseed_wait_retry", sec);
    put!(file_pool_size, b"file_pool_size", i32);
    put!(max_failcount, b"max_failcount", i32);
    put!(min_reconnect_time, b"min_reconnect_time", i32);
    put!(peer_connect_timeout, b"peer_connect_timeout", sec);
    put!(connection_speed, b"connection_speed", i32);
    put!(inactivity_timeout, b"inactivity_timeout", sec);
    put!(unchoke_interval, b"unchoke_interval", sec);
    put!(
        optimistic_unchoke_interval,
        b"optimistic_unchoke_interval",
        sec
    );
    put!(num_want, b"num_want", i32);
    put!(initial_picker_threshold, b"initial_picker_threshold", i32);
    put!(allowed_fast_set_size, b"allowed_fast_set_size", i32);
    put!(suggest_mode, b"suggest_mode", en);
    put!(max_queued_disk_bytes, b"max_queued_disk_bytes", i64);
    put!(handshake_timeout, b"handshake_timeout", sec);
    put!(send_buffer_low_watermark, b"send_buffer_low_watermark", i64);
    put!(send_buffer_watermark, b"send_buffer_watermark", usize);
    put!(
        send_buffer_watermark_factor,
        b"send_buffer_watermark_factor",
        i32
    );
    put!(choking_algorithm, b"choking_algorithm", en);
    put!(seed_choking_algorithm, b"seed_choking_algorithm", en);
    put!(cache_size, b"cache_size", i32);
    put!(cache_buffer_chunk_size, b"cache_buffer_chunk_size", i32);
    put!(cache_expiry, b"cache_expiry", sec);
    put!(disk_io_write_mode, b"disk_io_write_mode", en);
    put!(disk_io_read_mode, b"disk_io_read_mode", en);
    put!(outgoing_port, b"outgoing_port", i32);
    put!(num_outgoing_ports, b"num_outgoing_ports", i32);
    put!(peer_dscp, b"peer_dscp", i32);
    put!(active_downloads, b"active_downloads", i32);
    put!(active_seeds, b"active_seeds", i32);
    put!(active_checking, b"active_checking", i32);
    put!(active_dht_limit, b"active_dht_limit", i32);
    put!(active_tracker_limit, b"active_tracker_limit", i32);
    put!(active_lsd_limit, b"active_lsd_limit", i32);
    put!(active_limit, b"active_limit", i32);
    put!(active_loaded_limit, b"active_loaded_limit", i32);
    put!(auto_manage_interval, b"auto_manage_interval", i32);
    put!(seed_time_limit, b"seed_time_limit", i32);
    put!(auto_scrape_interval, b"auto_scrape_interval", sec);
    put!(auto_scrape_min_interval, b"auto_scrape_min_interval", sec);
    put!(max_peerlist_size, b"max_peerlist_size", i32);
    put!(max_paused_peerlist_size, b"max_paused_peerlist_size", i32);
    put!(min_announce_interval, b"min_announce_interval", sec);
    put!(auto_manage_startup, b"auto_manage_startup", sec);
    put!(seeding_piece_quota, b"seeding_piece_quota", i32);
    put!(max_rejects, b"max_rejects", i32);
    put!(recv_socket_buffer_size, b"recv_socket_buffer_size", i64);
    put!(send_socket_buffer_size, b"send_socket_buffer_size", i64);
    put!(max_peer_recv_buffer_size, b"max_peer_recv_buffer_size", i64);
    put!(
        file_checks_delay_per_block,
        b"file_checks_delay_per_block",
        i32
    );
    put!(read_cache_line_size, b"read_cache_line_size", i32);
    put!(write_cache_line_size, b"write_cache_line_size", i32);
    put!(optimistic_disk_retry, b"optimistic_disk_retry", sec);
    put!(max_suggest_pieces, b"max_suggest_pieces", i32);
    put!(
        local_service_announce_interval,
        b"local_service_announce_interval",
        sec
    );
    put!(dht_announce_interval, b"dht_announce_interval", sec);
    put!(udp_tracker_token_expiry, b"udp_tracker_token_expiry", sec);
    put!(default_cache_min_age, b"default_cache_min_age", i32);
    put!(
        num_optimistic_unchoke_slots,
        b"num_optimistic_unchoke_slots",
        i32
    );
    put!(
        default_est_reciprocation_rate,
        b"default_est_reciprocation_rate",
        i32
    );
    put!(
        increase_est_reciprocation_rate,
        b"increase_est_reciprocation_rate",
        i32
    );
    put!(
        decrease_est_reciprocation_rate,
        b"decrease_est_reciprocation_rate",
        i32
    );
    put!(max_pex_peers, b"max_pex_peers", i32);
    put!(tick_interval, b"tick_interval", ms);
    put!(share_mode_target, b"share_mode_target", i32);
    put!(upload_rate_limit, b"upload_rate_limit", i64);
    put!(download_rate_limit, b"download_rate_limit", i64);
    put!(local_upload_rate_limit, b"local_upload_rate_limit", i64);
    put!(local_download_rate_limit, b"local_download_rate_limit", i64);
    put!(dht_upload_rate_limit, b"dht_upload_rate_limit", i64);
    put!(unchoke_slots_limit, b"unchoke_slots_limit", i32);
    put!(half_open_limit, b"half_open_limit", i32);
    put!(connections_limit, b"connections_limit", i32);
    put!(connections_slack, b"connections_slack", i32);
    put!(utp_target_delay, b"utp_target_delay", ms);
    put!(utp_gain_factor, b"utp_gain_factor", i32);
    put!(utp_min_timeout, b"utp_min_timeout", ms);
    put!(utp_syn_resends, b"utp_syn_resends", i32);
    put!(utp_fin_resends, b"utp_fin_resends", i32);
    put!(utp_num_resends, b"utp_num_resends", i32);
    put!(utp_connect_timeout, b"utp_connect_timeout", ms);
    put!(utp_delayed_ack, b"utp_delayed_ack", i32);
    put!(utp_loss_multiplier, b"utp_loss_multiplier", i32);
    put!(mixed_mode_algorithm, b"mixed_mode_algorithm", en);
    put!(listen_queue_size, b"listen_queue_size", i32);
    put!(torrent_connect_boost, b"torrent_connect_boost", i32);
    put!(alert_queue_size, b"alert_queue_size", usize);
    put!(max_metadata_size, b"max_metadata_size", i64);
    put!(hashing_threads, b"hashing_threads", i64);
    put!(checking_mem_usage, b"checking_mem_usage", i32);
    put!(predictive_piece_announce, b"predictive_piece_announce", i32);
    put!(aio_threads, b"aio_threads", i64);
    put!(aio_max, b"aio_max", i32);
    put!(network_threads, b"network_threads", i32);
    put!(ssl_listen, b"ssl_listen", i32);
    put!(tracker_backoff, b"tracker_backoff", i32);
    put!(share_ratio_limit, b"share_ratio_limit", i32);
    put!(seed_time_ratio_limit, b"seed_time_ratio_limit", i32);
    put!(peer_turnover, b"peer_turnover", i32);
    put!(peer_turnover_cutoff, b"peer_turnover_cutoff", i32);
    put!(peer_turnover_interval, b"peer_turnover_interval", sec);
    put!(
        connect_seed_every_n_download,
        b"connect_seed_every_n_download",
        i32
    );
    put!(max_http_recv_buffer_size, b"max_http_recv_buffer_size", i64);
    put!(max_retry_port_bind, b"max_retry_port_bind", i32);
    put!(alert_mask, b"alert_mask", amask);
    put!(out_enc_policy, b"out_enc_policy", en);
    put!(in_enc_policy, b"in_enc_policy", en);
    put!(allowed_enc_level, b"allowed_enc_level", u8i);
    put!(inactive_down_rate, b"inactive_down_rate", i64);
    put!(inactive_up_rate, b"inactive_up_rate", i64);
    put!(proxy_type, b"proxy_type", en);
    put!(proxy_port, b"proxy_port", i32);
    put!(i2p_port, b"i2p_port", i32);
    put!(cache_size_volatile, b"cache_size_volatile", i32);
    put!(urlseed_max_request_bytes, b"urlseed_max_request_bytes", i64);
    put!(
        web_seed_name_lookup_retry,
        b"web_seed_name_lookup_retry",
        sec
    );
    put!(close_file_interval, b"close_file_interval", sec);
    put!(utp_cwnd_reduce_timer, b"utp_cwnd_reduce_timer", ms);
    put!(max_web_seed_connections, b"max_web_seed_connections", i32);
    put!(resolver_cache_timeout, b"resolver_cache_timeout", sec);
    put!(
        send_not_sent_low_watermark,
        b"send_not_sent_low_watermark",
        i64
    );
    put!(
        rate_choker_initial_threshold,
        b"rate_choker_initial_threshold",
        i64
    );
    put!(upnp_lease_duration, b"upnp_lease_duration", sec);
    put!(
        max_concurrent_http_announces,
        b"max_concurrent_http_announces",
        i32
    );
    put!(dht_max_peers_reply, b"dht_max_peers_reply", i32);
    put!(dht_search_branching, b"dht_search_branching", i32);
    put!(dht_max_fail_count, b"dht_max_fail_count", i32);
    put!(dht_max_torrents, b"dht_max_torrents", i32);
    put!(dht_max_dht_items, b"dht_max_dht_items", i32);
    put!(dht_max_peers, b"dht_max_peers", i32);
    put!(
        dht_max_torrent_search_reply,
        b"dht_max_torrent_search_reply",
        i32
    );
    put!(dht_block_timeout, b"dht_block_timeout", sec);
    put!(dht_block_ratelimit, b"dht_block_ratelimit", i32);
    put!(dht_item_lifetime, b"dht_item_lifetime", sec);
    put!(
        dht_sample_infohashes_interval,
        b"dht_sample_infohashes_interval",
        sec
    );
    put!(
        dht_max_infohashes_sample_count,
        b"dht_max_infohashes_sample_count",
        i32
    );
    put!(max_piece_count, b"max_piece_count", i32);
    put!(metadata_token_limit, b"metadata_token_limit", i64);
    put!(disk_write_mode, b"disk_write_mode", en);
    put!(mmap_file_size_cutoff, b"mmap_file_size_cutoff", i32);
    put!(i2p_inbound_quantity, b"i2p_inbound_quantity", i32);
    put!(i2p_outbound_quantity, b"i2p_outbound_quantity", i32);
    put!(i2p_inbound_length, b"i2p_inbound_length", i32);
    put!(i2p_outbound_length, b"i2p_outbound_length", i32);
    put!(announce_port, b"announce_port", i32);
    put!(
        i2p_inbound_length_variance,
        b"i2p_inbound_length_variance",
        i32
    );
    put!(
        i2p_outbound_length_variance,
        b"i2p_outbound_length_variance",
        i32
    );
    put!(natpmp_lease_duration, b"natpmp_lease_duration", sec);
    put!(
        min_websocket_announce_interval,
        b"min_websocket_announce_interval",
        sec
    );
    put!(
        webtorrent_connection_timeout,
        b"webtorrent_connection_timeout",
        i32
    );
    put!(max_webtorrent_offers, b"max_webtorrent_offers", i32);

    // BitDaemon extensions
    put!(web_seed_blocks_per_tick, b"web_seed_blocks_per_tick", i32);
    put!(save_resume_data_interval, b"save_resume_data_interval", sec);
    Entry::Dict(d)
}

/// `load_pack_from_dict` — applies known keys; unknown keys are
/// skipped (upstream ignores them too).
#[must_use]
pub fn read_settings_dict(n: &BdecodeNode<'_>) -> SessionSettings {
    let mut s = SessionSettings::default();
    apply_settings_dict(&mut s, n);
    s
}

/// upstream `apply_deprecated_dht_settings` — sparse overlay of
/// the legacy `"dht"` dict's keys onto the flat `dht_*` settings
/// fields; only keys the dict carries are touched.
pub fn apply_dht_settings_dict(s: &mut SessionSettings, d: &BdecodeNode<'_>) {
    use std::time::Duration;
    if d.node_type() != NodeType::Dict {
        return;
    }
    macro_rules! int_field {
        ($k:literal, $f:ident) => {
            let v = d.dict_find_int($k);
            if v.node_type() == NodeType::Int {
                s.$f = v.int_value() as _;
            }
        };
        ($k:literal, $f:ident, dur) => {
            let v = d.dict_find_int($k);
            if v.node_type() == NodeType::Int {
                s.$f = Duration::from_secs(v.int_value().max(0) as u64);
            }
        };
        ($k:literal, $f:ident, bool) => {
            let v = d.dict_find_int($k);
            if v.node_type() == NodeType::Int {
                s.$f = v.int_value() != 0;
            }
        };
    }
    int_field!(b"max_peers_reply", dht_max_peers_reply);
    int_field!(b"search_branching", dht_search_branching);
    int_field!(b"max_fail_count", dht_max_fail_count);
    int_field!(b"max_torrents", dht_max_torrents);
    int_field!(b"max_dht_items", dht_max_dht_items);
    int_field!(b"max_peers", dht_max_peers);
    int_field!(b"max_torrent_search_reply", dht_max_torrent_search_reply);
    int_field!(b"restrict_routing_ips", dht_restrict_routing_ips, bool);
    int_field!(b"restrict_search_ips", dht_restrict_search_ips, bool);
    int_field!(b"extended_routing_table", dht_extended_routing_table, bool);
    int_field!(b"aggressive_lookups", dht_aggressive_lookups, bool);
    int_field!(b"privacy_lookups", dht_privacy_lookups, bool);
    int_field!(b"enforce_node_id", dht_enforce_node_id, bool);
    int_field!(b"ignore_dark_internet", dht_ignore_dark_internet, bool);
    int_field!(b"block_timeout", dht_block_timeout, dur);
    int_field!(b"block_ratelimit", dht_block_ratelimit);
    int_field!(b"read_only", dht_read_only, bool);
    int_field!(b"item_lifetime", dht_item_lifetime, dur);
    int_field!(b"upload_rate_limit", dht_upload_rate_limit);
    int_field!(
        b"sample_infohashes_interval",
        dht_sample_infohashes_interval,
        dur
    );
    int_field!(
        b"max_infohashes_sample_count",
        dht_max_infohashes_sample_count
    );
    int_field!(
        b"prefer_verified_node_ids",
        dht_prefer_verified_node_ids,
        bool
    );
}

/// `apply_settings_pack_impl` — sparse overlay: only keys present
/// in the dict are written into `s` (upstream's `settings_pack`
/// carries just the serialized keys, so runtime values for absent
/// keys survive `load_state`).
pub fn apply_settings_dict(s: &mut SessionSettings, n: &BdecodeNode<'_>) {
    use std::time::Duration;
    macro_rules! get {
        ($f:ident, $k:literal, str) => {
            let v = n.dict_find_string_value($k, &[]);
            if !v.is_empty() {
                s.$f = v.to_vec();
            }
        };
        ($f:ident, $k:literal, boolean) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = v != 0;
            }
        };
        ($f:ident, $k:literal, i32) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = v as i32;
            }
        };
        ($f:ident, $k:literal, u8i) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = v.clamp(0, 255) as u8;
            }
        };
        ($f:ident, $k:literal, i64) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = v;
            }
        };
        ($f:ident, $k:literal, usize) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = v.max(0) as usize;
            }
        };
        ($f:ident, $k:literal, sec) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = Duration::from_secs(v.max(0) as u64);
            }
        };
        ($f:ident, $k:literal, ms) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = Duration::from_millis(v.max(0) as u64);
            }
        };
        ($f:ident, $k:literal, en) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = SettingInt::from_int(v);
            }
        };
        ($f:ident, $k:literal, amask) => {
            let v = n.dict_find_int_value($k, i64::MIN);
            if v != i64::MIN {
                s.$f = crate::alerts::AlertCategory::from_bits(v.max(0) as u32);
            }
        };
    }

    // string settings
    get!(user_agent, b"user_agent", str);
    get!(announce_ip, b"announce_ip", str);
    get!(mmap_cache, b"mmap_cache", str);
    get!(handshake_client_version, b"handshake_client_version", str);
    get!(outgoing_interfaces, b"outgoing_interfaces", str);
    get!(listen_interfaces, b"listen_interfaces", str);
    get!(proxy_hostname, b"proxy_hostname", str);
    get!(proxy_username, b"proxy_username", str);
    get!(proxy_password, b"proxy_password", str);
    get!(i2p_hostname, b"i2p_hostname", str);
    get!(peer_fingerprint, b"peer_fingerprint", str);
    get!(dht_bootstrap_nodes, b"dht_bootstrap_nodes", str);
    get!(natpmp_gateway, b"natpmp_gateway", str);
    get!(webtorrent_stun_server, b"webtorrent_stun_server", str);

    // bool settings
    get!(
        allow_multiple_connections_per_ip,
        b"allow_multiple_connections_per_ip",
        boolean
    );
    get!(
        ignore_limits_on_local_network,
        b"ignore_limits_on_local_network",
        boolean
    );
    get!(send_redundant_have, b"send_redundant_have", boolean);
    get!(lazy_bitfields, b"lazy_bitfields", boolean);
    get!(use_dht_as_fallback, b"use_dht_as_fallback", boolean);
    get!(upnp_ignore_nonrouters, b"upnp_ignore_nonrouters", boolean);
    get!(use_parole_mode, b"use_parole_mode", boolean);
    get!(use_read_cache, b"use_read_cache", boolean);
    get!(use_write_cache, b"use_write_cache", boolean);
    get!(dont_flush_write_cache, b"dont_flush_write_cache", boolean);
    get!(coalesce_reads, b"coalesce_reads", boolean);
    get!(coalesce_writes, b"coalesce_writes", boolean);
    get!(
        auto_manage_prefer_seeds,
        b"auto_manage_prefer_seeds",
        boolean
    );
    get!(
        dont_count_slow_torrents,
        b"dont_count_slow_torrents",
        boolean
    );
    get!(
        close_redundant_connections,
        b"close_redundant_connections",
        boolean
    );
    get!(
        prioritize_partial_pieces,
        b"prioritize_partial_pieces",
        boolean
    );
    get!(rate_limit_ip_overhead, b"rate_limit_ip_overhead", boolean);
    get!(
        announce_to_all_trackers,
        b"announce_to_all_trackers",
        boolean
    );
    get!(announce_to_all_tiers, b"announce_to_all_tiers", boolean);
    get!(prefer_udp_trackers, b"prefer_udp_trackers", boolean);
    get!(strict_super_seeding, b"strict_super_seeding", boolean);
    get!(lock_disk_cache, b"lock_disk_cache", boolean);
    get!(disable_hash_checks, b"disable_hash_checks", boolean);
    get!(allow_i2p_mixed, b"allow_i2p_mixed", boolean);
    get!(low_prio_disk, b"low_prio_disk", boolean);
    get!(volatile_read_cache, b"volatile_read_cache", boolean);
    get!(guided_read_cache, b"guided_read_cache", boolean);
    get!(no_atime_storage, b"no_atime_storage", boolean);
    get!(
        incoming_starts_queued_torrents,
        b"incoming_starts_queued_torrents",
        boolean
    );
    get!(report_true_downloaded, b"report_true_downloaded", boolean);
    get!(strict_end_game_mode, b"strict_end_game_mode", boolean);
    get!(broadcast_lsd, b"broadcast_lsd", boolean);
    get!(enable_outgoing_utp, b"enable_outgoing_utp", boolean);
    get!(enable_incoming_utp, b"enable_incoming_utp", boolean);
    get!(enable_outgoing_tcp, b"enable_outgoing_tcp", boolean);
    get!(enable_incoming_tcp, b"enable_incoming_tcp", boolean);
    get!(
        ignore_resume_timestamps,
        b"ignore_resume_timestamps",
        boolean
    );
    get!(
        no_recheck_incomplete_resume,
        b"no_recheck_incomplete_resume",
        boolean
    );
    get!(anonymous_mode, b"anonymous_mode", boolean);
    get!(
        report_web_seed_downloads,
        b"report_web_seed_downloads",
        boolean
    );
    get!(rate_limit_utp, b"rate_limit_utp", boolean);
    get!(announce_double_nat, b"announce_double_nat", boolean);
    get!(
        seeding_outgoing_connections,
        b"seeding_outgoing_connections",
        boolean
    );
    get!(
        no_connect_privileged_ports,
        b"no_connect_privileged_ports",
        boolean
    );
    get!(smooth_connects, b"smooth_connects", boolean);
    get!(always_send_user_agent, b"always_send_user_agent", boolean);
    get!(
        apply_ip_filter_to_trackers,
        b"apply_ip_filter_to_trackers",
        boolean
    );
    get!(use_disk_read_ahead, b"use_disk_read_ahead", boolean);
    get!(lock_files, b"lock_files", boolean);
    get!(contiguous_recv_buffer, b"contiguous_recv_buffer", boolean);
    get!(ban_web_seeds, b"ban_web_seeds", boolean);
    get!(
        allow_partial_disk_writes,
        b"allow_partial_disk_writes",
        boolean
    );
    get!(force_proxy, b"force_proxy", boolean);
    get!(support_share_mode, b"support_share_mode", boolean);
    get!(support_merkle_torrents, b"support_merkle_torrents", boolean);
    get!(report_redundant_bytes, b"report_redundant_bytes", boolean);
    get!(
        listen_system_port_fallback,
        b"listen_system_port_fallback",
        boolean
    );
    get!(use_disk_cache_pool, b"use_disk_cache_pool", boolean);
    get!(announce_crypto_support, b"announce_crypto_support", boolean);
    get!(enable_upnp, b"enable_upnp", boolean);
    get!(enable_natpmp, b"enable_natpmp", boolean);
    get!(enable_lsd, b"enable_lsd", boolean);
    get!(enable_dht, b"enable_dht", boolean);
    get!(prefer_rc4, b"prefer_rc4", boolean);
    get!(proxy_hostnames, b"proxy_hostnames", boolean);
    get!(proxy_peer_connections, b"proxy_peer_connections", boolean);
    get!(auto_sequential, b"auto_sequential", boolean);
    get!(
        proxy_tracker_connections,
        b"proxy_tracker_connections",
        boolean
    );
    get!(enable_ip_notifier, b"enable_ip_notifier", boolean);
    get!(
        dht_prefer_verified_node_ids,
        b"dht_prefer_verified_node_ids",
        boolean
    );
    get!(
        dht_restrict_routing_ips,
        b"dht_restrict_routing_ips",
        boolean
    );
    get!(dht_restrict_search_ips, b"dht_restrict_search_ips", boolean);
    get!(
        dht_extended_routing_table,
        b"dht_extended_routing_table",
        boolean
    );
    get!(dht_aggressive_lookups, b"dht_aggressive_lookups", boolean);
    get!(dht_privacy_lookups, b"dht_privacy_lookups", boolean);
    get!(dht_enforce_node_id, b"dht_enforce_node_id", boolean);
    get!(
        dht_ignore_dark_internet,
        b"dht_ignore_dark_internet",
        boolean
    );
    get!(dht_read_only, b"dht_read_only", boolean);
    get!(piece_extent_affinity, b"piece_extent_affinity", boolean);
    get!(validate_https_trackers, b"validate_https_trackers", boolean);
    get!(ssrf_mitigation, b"ssrf_mitigation", boolean);
    get!(allow_idna, b"allow_idna", boolean);
    get!(
        enable_set_file_valid_data,
        b"enable_set_file_valid_data",
        boolean
    );
    get!(
        socks5_udp_send_local_ep,
        b"socks5_udp_send_local_ep",
        boolean
    );
    get!(
        proxy_send_host_in_connect,
        b"proxy_send_host_in_connect",
        boolean
    );
    get!(
        disk_disable_copy_on_write,
        b"disk_disable_copy_on_write",
        boolean
    );
    get!(
        allow_multiple_connections_per_pid,
        b"allow_multiple_connections_per_pid",
        boolean
    );
    get!(apply_filter_to_dht, b"apply_filter_to_dht", boolean);

    // int settings
    get!(
        tracker_completion_timeout,
        b"tracker_completion_timeout",
        sec
    );
    get!(tracker_receive_timeout, b"tracker_receive_timeout", sec);
    get!(stop_tracker_timeout, b"stop_tracker_timeout", sec);
    get!(
        tracker_maximum_response_length,
        b"tracker_maximum_response_length",
        i64
    );
    get!(piece_timeout, b"piece_timeout", sec);
    get!(request_timeout, b"request_timeout", sec);
    get!(request_queue_time, b"request_queue_time", sec);
    get!(
        max_allowed_in_request_queue,
        b"max_allowed_in_request_queue",
        i32
    );
    get!(max_out_request_queue, b"max_out_request_queue", i32);
    get!(whole_pieces_threshold, b"whole_pieces_threshold", i32);
    get!(peer_timeout, b"peer_timeout", sec);
    get!(urlseed_timeout, b"urlseed_timeout", sec);
    get!(urlseed_pipeline_size, b"urlseed_pipeline_size", i32);
    get!(urlseed_wait_retry, b"urlseed_wait_retry", sec);
    get!(file_pool_size, b"file_pool_size", i32);
    get!(max_failcount, b"max_failcount", i32);
    get!(min_reconnect_time, b"min_reconnect_time", i32);
    get!(peer_connect_timeout, b"peer_connect_timeout", sec);
    get!(connection_speed, b"connection_speed", i32);
    get!(inactivity_timeout, b"inactivity_timeout", sec);
    get!(unchoke_interval, b"unchoke_interval", sec);
    get!(
        optimistic_unchoke_interval,
        b"optimistic_unchoke_interval",
        sec
    );
    get!(num_want, b"num_want", i32);
    get!(initial_picker_threshold, b"initial_picker_threshold", i32);
    get!(allowed_fast_set_size, b"allowed_fast_set_size", i32);
    get!(suggest_mode, b"suggest_mode", en);
    get!(max_queued_disk_bytes, b"max_queued_disk_bytes", i64);
    get!(handshake_timeout, b"handshake_timeout", sec);
    get!(send_buffer_low_watermark, b"send_buffer_low_watermark", i64);
    get!(send_buffer_watermark, b"send_buffer_watermark", usize);
    get!(
        send_buffer_watermark_factor,
        b"send_buffer_watermark_factor",
        i32
    );
    get!(choking_algorithm, b"choking_algorithm", en);
    get!(seed_choking_algorithm, b"seed_choking_algorithm", en);
    get!(cache_size, b"cache_size", i32);
    get!(cache_buffer_chunk_size, b"cache_buffer_chunk_size", i32);
    get!(cache_expiry, b"cache_expiry", sec);
    get!(disk_io_write_mode, b"disk_io_write_mode", en);
    get!(disk_io_read_mode, b"disk_io_read_mode", en);
    get!(outgoing_port, b"outgoing_port", i32);
    get!(num_outgoing_ports, b"num_outgoing_ports", i32);
    get!(peer_dscp, b"peer_dscp", i32);
    get!(active_downloads, b"active_downloads", i32);
    get!(active_seeds, b"active_seeds", i32);
    get!(active_checking, b"active_checking", i32);
    get!(active_dht_limit, b"active_dht_limit", i32);
    get!(active_tracker_limit, b"active_tracker_limit", i32);
    get!(active_lsd_limit, b"active_lsd_limit", i32);
    get!(active_limit, b"active_limit", i32);
    get!(active_loaded_limit, b"active_loaded_limit", i32);
    get!(auto_manage_interval, b"auto_manage_interval", i32);
    get!(seed_time_limit, b"seed_time_limit", i32);
    get!(auto_scrape_interval, b"auto_scrape_interval", sec);
    get!(auto_scrape_min_interval, b"auto_scrape_min_interval", sec);
    get!(max_peerlist_size, b"max_peerlist_size", i32);
    get!(max_paused_peerlist_size, b"max_paused_peerlist_size", i32);
    get!(min_announce_interval, b"min_announce_interval", sec);
    get!(auto_manage_startup, b"auto_manage_startup", sec);
    get!(seeding_piece_quota, b"seeding_piece_quota", i32);
    get!(max_rejects, b"max_rejects", i32);
    get!(recv_socket_buffer_size, b"recv_socket_buffer_size", i64);
    get!(send_socket_buffer_size, b"send_socket_buffer_size", i64);
    get!(max_peer_recv_buffer_size, b"max_peer_recv_buffer_size", i64);
    get!(
        file_checks_delay_per_block,
        b"file_checks_delay_per_block",
        i32
    );
    get!(read_cache_line_size, b"read_cache_line_size", i32);
    get!(write_cache_line_size, b"write_cache_line_size", i32);
    get!(optimistic_disk_retry, b"optimistic_disk_retry", sec);
    get!(max_suggest_pieces, b"max_suggest_pieces", i32);
    get!(
        local_service_announce_interval,
        b"local_service_announce_interval",
        sec
    );
    get!(dht_announce_interval, b"dht_announce_interval", sec);
    get!(udp_tracker_token_expiry, b"udp_tracker_token_expiry", sec);
    get!(default_cache_min_age, b"default_cache_min_age", i32);
    get!(
        num_optimistic_unchoke_slots,
        b"num_optimistic_unchoke_slots",
        i32
    );
    get!(
        default_est_reciprocation_rate,
        b"default_est_reciprocation_rate",
        i32
    );
    get!(
        increase_est_reciprocation_rate,
        b"increase_est_reciprocation_rate",
        i32
    );
    get!(
        decrease_est_reciprocation_rate,
        b"decrease_est_reciprocation_rate",
        i32
    );
    get!(max_pex_peers, b"max_pex_peers", i32);
    get!(tick_interval, b"tick_interval", ms);
    get!(share_mode_target, b"share_mode_target", i32);
    get!(upload_rate_limit, b"upload_rate_limit", i64);
    get!(download_rate_limit, b"download_rate_limit", i64);
    get!(local_upload_rate_limit, b"local_upload_rate_limit", i64);
    get!(local_download_rate_limit, b"local_download_rate_limit", i64);
    get!(dht_upload_rate_limit, b"dht_upload_rate_limit", i64);
    get!(unchoke_slots_limit, b"unchoke_slots_limit", i32);
    get!(half_open_limit, b"half_open_limit", i32);
    get!(connections_limit, b"connections_limit", i32);
    get!(connections_slack, b"connections_slack", i32);
    get!(utp_target_delay, b"utp_target_delay", ms);
    get!(utp_gain_factor, b"utp_gain_factor", i32);
    get!(utp_min_timeout, b"utp_min_timeout", ms);
    get!(utp_syn_resends, b"utp_syn_resends", i32);
    get!(utp_fin_resends, b"utp_fin_resends", i32);
    get!(utp_num_resends, b"utp_num_resends", i32);
    get!(utp_connect_timeout, b"utp_connect_timeout", ms);
    get!(utp_delayed_ack, b"utp_delayed_ack", i32);
    get!(utp_loss_multiplier, b"utp_loss_multiplier", i32);
    get!(mixed_mode_algorithm, b"mixed_mode_algorithm", en);
    get!(listen_queue_size, b"listen_queue_size", i32);
    get!(torrent_connect_boost, b"torrent_connect_boost", i32);
    get!(alert_queue_size, b"alert_queue_size", usize);
    get!(max_metadata_size, b"max_metadata_size", i64);
    get!(hashing_threads, b"hashing_threads", i64);
    get!(checking_mem_usage, b"checking_mem_usage", i32);
    get!(predictive_piece_announce, b"predictive_piece_announce", i32);
    get!(aio_threads, b"aio_threads", i64);
    get!(aio_max, b"aio_max", i32);
    get!(network_threads, b"network_threads", i32);
    get!(ssl_listen, b"ssl_listen", i32);
    get!(tracker_backoff, b"tracker_backoff", i32);
    get!(share_ratio_limit, b"share_ratio_limit", i32);
    get!(seed_time_ratio_limit, b"seed_time_ratio_limit", i32);
    get!(peer_turnover, b"peer_turnover", i32);
    get!(peer_turnover_cutoff, b"peer_turnover_cutoff", i32);
    get!(peer_turnover_interval, b"peer_turnover_interval", sec);
    get!(
        connect_seed_every_n_download,
        b"connect_seed_every_n_download",
        i32
    );
    get!(max_http_recv_buffer_size, b"max_http_recv_buffer_size", i64);
    get!(max_retry_port_bind, b"max_retry_port_bind", i32);
    get!(alert_mask, b"alert_mask", amask);
    get!(out_enc_policy, b"out_enc_policy", en);
    get!(in_enc_policy, b"in_enc_policy", en);
    get!(allowed_enc_level, b"allowed_enc_level", u8i);
    get!(inactive_down_rate, b"inactive_down_rate", i64);
    get!(inactive_up_rate, b"inactive_up_rate", i64);
    get!(proxy_type, b"proxy_type", en);
    get!(proxy_port, b"proxy_port", i32);
    get!(i2p_port, b"i2p_port", i32);
    get!(cache_size_volatile, b"cache_size_volatile", i32);
    get!(urlseed_max_request_bytes, b"urlseed_max_request_bytes", i64);
    get!(
        web_seed_name_lookup_retry,
        b"web_seed_name_lookup_retry",
        sec
    );
    get!(close_file_interval, b"close_file_interval", sec);
    get!(utp_cwnd_reduce_timer, b"utp_cwnd_reduce_timer", ms);
    get!(max_web_seed_connections, b"max_web_seed_connections", i32);
    get!(resolver_cache_timeout, b"resolver_cache_timeout", sec);
    get!(
        send_not_sent_low_watermark,
        b"send_not_sent_low_watermark",
        i64
    );
    get!(
        rate_choker_initial_threshold,
        b"rate_choker_initial_threshold",
        i64
    );
    get!(upnp_lease_duration, b"upnp_lease_duration", sec);
    get!(
        max_concurrent_http_announces,
        b"max_concurrent_http_announces",
        i32
    );
    get!(dht_max_peers_reply, b"dht_max_peers_reply", i32);
    get!(dht_search_branching, b"dht_search_branching", i32);
    get!(dht_max_fail_count, b"dht_max_fail_count", i32);
    get!(dht_max_torrents, b"dht_max_torrents", i32);
    get!(dht_max_dht_items, b"dht_max_dht_items", i32);
    get!(dht_max_peers, b"dht_max_peers", i32);
    get!(
        dht_max_torrent_search_reply,
        b"dht_max_torrent_search_reply",
        i32
    );
    get!(dht_block_timeout, b"dht_block_timeout", sec);
    get!(dht_block_ratelimit, b"dht_block_ratelimit", i32);
    get!(dht_item_lifetime, b"dht_item_lifetime", sec);
    get!(
        dht_sample_infohashes_interval,
        b"dht_sample_infohashes_interval",
        sec
    );
    get!(
        dht_max_infohashes_sample_count,
        b"dht_max_infohashes_sample_count",
        i32
    );
    get!(max_piece_count, b"max_piece_count", i32);
    get!(metadata_token_limit, b"metadata_token_limit", i64);
    get!(disk_write_mode, b"disk_write_mode", en);
    get!(mmap_file_size_cutoff, b"mmap_file_size_cutoff", i32);
    get!(i2p_inbound_quantity, b"i2p_inbound_quantity", i32);
    get!(i2p_outbound_quantity, b"i2p_outbound_quantity", i32);
    get!(i2p_inbound_length, b"i2p_inbound_length", i32);
    get!(i2p_outbound_length, b"i2p_outbound_length", i32);
    get!(announce_port, b"announce_port", i32);
    get!(
        i2p_inbound_length_variance,
        b"i2p_inbound_length_variance",
        i32
    );
    get!(
        i2p_outbound_length_variance,
        b"i2p_outbound_length_variance",
        i32
    );
    get!(natpmp_lease_duration, b"natpmp_lease_duration", sec);
    get!(
        min_websocket_announce_interval,
        b"min_websocket_announce_interval",
        sec
    );
    get!(
        webtorrent_connection_timeout,
        b"webtorrent_connection_timeout",
        i32
    );
    get!(max_webtorrent_offers, b"max_webtorrent_offers", i32);

    // BitDaemon extensions
    get!(web_seed_blocks_per_tick, b"web_seed_blocks_per_tick", i32);
    get!(save_resume_data_interval, b"save_resume_data_interval", sec);
}
