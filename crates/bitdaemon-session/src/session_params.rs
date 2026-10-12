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
    let mut e = Entry::Dict(Default::default());
    let d = e.dict_mut().expect("dict");

    d.insert(b"dht".to_vec(), dht_save_state(&sp.dht_settings));
    d.insert(b"dht state".to_vec(), save_dht_state(&sp.dht_state));
    d.insert(b"settings".to_vec(), save_settings_to_dict(&sp.settings));

    let mut ext = Entry::Dict(Default::default());
    let m = ext.dict_mut().expect("dict");
    for (k, v) in &sp.ext_state {
        m.insert(k.clone(), Entry::String(v.clone()));
    }
    d.insert(b"extensions".to_vec(), ext);

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

    e
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

    let st = e.dict_find_dict(b"dht state");
    if st.node_type() == NodeType::Dict {
        params.dht_state = read_dht_state(&st);
    }

    let sett = e.dict_find_dict(b"settings");
    if sett.node_type() == NodeType::Dict {
        params.settings = read_settings_dict(&sett);
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

/// `save_settings_to_dict` — sparse: only non-default fields are
/// written, under upstream `settings_pack` key names.
#[must_use]
pub fn save_settings_to_dict(s: &SessionSettings) -> Entry {
    let dflt = SessionSettings::default();
    let mut e = Entry::Dict(Default::default());
    let d = e.dict_mut().expect("dict");

    macro_rules! secs {
        ($k:expr, $v:expr, $dv:expr) => {
            if $v != $dv {
                d.insert($k.to_vec(), Entry::Int($v.as_secs() as i64));
            }
        };
    }
    macro_rules! i {
        ($k:expr, $v:expr, $dv:expr) => {
            if $v != $dv {
                d.insert($k.to_vec(), Entry::Int($v));
            }
        };
    }
    macro_rules! b {
        ($k:expr, $v:expr, $dv:expr) => {
            if $v != $dv {
                d.insert($k.to_vec(), Entry::Int($v as i64));
            }
        };
    }

    i!(
        b"connections_limit",
        i64::from(s.connections_limit),
        i64::from(dflt.connections_limit)
    );
    i!(
        b"active_limit",
        i64::from(s.active_limit),
        i64::from(dflt.active_limit)
    );
    i!(
        b"active_downloads",
        i64::from(s.active_downloads),
        i64::from(dflt.active_downloads)
    );
    i!(
        b"active_seeds",
        i64::from(s.active_seeds),
        i64::from(dflt.active_seeds)
    );
    i!(
        b"unchoke_slots_limit",
        i64::from(s.unchoke_slots),
        i64::from(dflt.unchoke_slots)
    );
    i!(
        b"num_optimistic_unchoke_slots",
        i64::from(s.optimistic_unchoke_slots),
        i64::from(dflt.optimistic_unchoke_slots)
    );
    secs!(b"request_timeout", s.request_timeout, dflt.request_timeout);
    secs!(b"peer_timeout", s.peer_timeout, dflt.peer_timeout);
    secs!(
        b"request_queue_time",
        s.request_queue_time,
        dflt.request_queue_time
    );
    i!(
        b"max_allowed_in_request_queue",
        i64::from(s.max_allowed_in_queue),
        i64::from(dflt.max_allowed_in_queue)
    );
    i!(
        b"upload_rate_limit",
        s.upload_rate_limit,
        dflt.upload_rate_limit
    );
    i!(
        b"download_rate_limit",
        s.download_rate_limit,
        dflt.download_rate_limit
    );
    secs!(b"piece_timeout", s.piece_timeout, dflt.piece_timeout);
    i!(
        b"max_queued_disk_bytes",
        s.max_disk_write_queue,
        dflt.max_disk_write_queue
    );
    i!(
        b"send_buffer_watermark",
        s.send_buffer_watermark as i64,
        dflt.send_buffer_watermark as i64
    );
    i!(
        b"alert_queue_size",
        s.alert_queue_size as i64,
        dflt.alert_queue_size as i64
    );
    i!(
        b"alert_mask",
        i64::from(s.alert_mask.bits()),
        i64::from(dflt.alert_mask.bits())
    );
    b!(b"enable_dht", s.dht_enabled, dflt.dht_enabled);
    b!(
        b"enable_incoming_utp",
        s.enable_incoming_utp,
        dflt.enable_incoming_utp
    );
    b!(
        b"enable_outgoing_utp",
        s.enable_outgoing_utp,
        dflt.enable_outgoing_utp
    );
    b!(b"prefer_rc4", s.prefer_rc4, dflt.prefer_rc4);
    i!(
        b"connection_speed",
        i64::from(s.connection_speed),
        i64::from(dflt.connection_speed)
    );
    i!(
        b"tick_interval",
        s.tick_interval.as_millis() as i64,
        dflt.tick_interval.as_millis() as i64
    );
    i!(
        b"save_resume_data_interval",
        s.save_resume_interval.as_secs() as i64,
        dflt.save_resume_interval.as_secs() as i64
    );
    i!(
        b"web_seed_blocks_per_tick",
        i64::from(s.web_seed_blocks_per_tick),
        i64::from(dflt.web_seed_blocks_per_tick)
    );
    i!(
        b"min_reconnect_time",
        i64::from(s.min_reconnect_time),
        i64::from(dflt.min_reconnect_time)
    );
    i!(
        b"max_peerlist_size",
        i64::from(s.max_peerlist_size),
        i64::from(dflt.max_peerlist_size)
    );
    i!(
        b"max_failcount",
        i64::from(s.max_failcount),
        i64::from(dflt.max_failcount)
    );
    b!(
        b"allow_multiple_connections_per_ip",
        s.allow_multiple_connections_per_ip,
        dflt.allow_multiple_connections_per_ip
    );
    i!(
        b"auto_manage_interval",
        i64::from(s.auto_manage_interval),
        i64::from(dflt.auto_manage_interval)
    );
    b!(b"anonymous_mode", s.anonymous_mode, dflt.anonymous_mode);
    i!(
        b"seed_time_limit",
        i64::from(s.seed_time_limit),
        i64::from(dflt.seed_time_limit)
    );
    i!(
        b"share_ratio_limit",
        i64::from(s.share_ratio_limit),
        i64::from(dflt.share_ratio_limit)
    );
    i!(
        b"seed_time_ratio_limit",
        i64::from(s.seed_time_ratio_limit),
        i64::from(dflt.seed_time_ratio_limit)
    );
    i!(
        b"allowed_fast_set_size",
        i64::from(s.allowed_fast_set_size),
        i64::from(dflt.allowed_fast_set_size)
    );
    if s.user_agent != dflt.user_agent {
        d.insert(b"user_agent".to_vec(), Entry::String(s.user_agent.clone()));
    }
    if s.webtorrent_stun_server != dflt.webtorrent_stun_server {
        d.insert(
            b"webtorrent_stun_server".to_vec(),
            Entry::String(s.webtorrent_stun_server.clone()),
        );
    }
    i!(
        b"webtorrent_connection_timeout",
        i64::from(s.webtorrent_connection_timeout),
        i64::from(dflt.webtorrent_connection_timeout)
    );
    i!(
        b"max_webtorrent_offers",
        i64::from(s.max_webtorrent_offers),
        i64::from(dflt.max_webtorrent_offers)
    );
    e
}

/// `load_pack_from_dict` — applies known keys; unknown keys are
/// skipped (upstream ignores them too).
#[must_use]
pub fn read_settings_dict(n: &BdecodeNode<'_>) -> SessionSettings {
    use std::time::Duration;
    let mut s = SessionSettings::default();
    macro_rules! g {
        ($k:expr) => {
            n.dict_find_int_value($k, i64::MIN)
        };
    }
    macro_rules! i {
        ($k:expr, $cur:expr) => {
            let v = g!($k);
            if v != i64::MIN {
                *$cur = v as i32;
            }
        };
    }
    macro_rules! l {
        ($k:expr, $cur:expr) => {
            let v = g!($k);
            if v != i64::MIN {
                *$cur = v;
            }
        };
    }
    macro_rules! b {
        ($k:expr, $cur:expr) => {
            let v = g!($k);
            if v != i64::MIN {
                *$cur = v != 0;
            }
        };
    }
    macro_rules! sec {
        ($k:expr, $cur:expr) => {
            let v = g!($k);
            if v != i64::MIN {
                *$cur = Duration::from_secs(v.max(0) as u64);
            }
        };
    }
    i!(b"connections_limit", &mut s.connections_limit);
    i!(b"active_limit", &mut s.active_limit);
    i!(b"active_downloads", &mut s.active_downloads);
    i!(b"active_seeds", &mut s.active_seeds);
    i!(b"unchoke_slots_limit", &mut s.unchoke_slots);
    i!(
        b"num_optimistic_unchoke_slots",
        &mut s.optimistic_unchoke_slots
    );
    sec!(b"request_timeout", &mut s.request_timeout);
    sec!(b"peer_timeout", &mut s.peer_timeout);
    sec!(b"request_queue_time", &mut s.request_queue_time);
    i!(b"max_allowed_in_request_queue", &mut s.max_allowed_in_queue);
    l!(b"upload_rate_limit", &mut s.upload_rate_limit);
    l!(b"download_rate_limit", &mut s.download_rate_limit);
    sec!(b"piece_timeout", &mut s.piece_timeout);
    l!(b"max_queued_disk_bytes", &mut s.max_disk_write_queue);
    let v = g!(b"send_buffer_watermark");
    if v != i64::MIN {
        s.send_buffer_watermark = v.max(0) as usize;
    }
    let v = g!(b"alert_queue_size");
    if v != i64::MIN {
        s.alert_queue_size = v.max(0) as usize;
    }
    let v = g!(b"alert_mask");
    if v != i64::MIN {
        s.alert_mask = crate::alerts::AlertCategory::from_bits(v.max(0) as u32);
    }
    b!(b"enable_dht", &mut s.dht_enabled);
    b!(b"enable_incoming_utp", &mut s.enable_incoming_utp);
    b!(b"enable_outgoing_utp", &mut s.enable_outgoing_utp);
    b!(b"prefer_rc4", &mut s.prefer_rc4);
    i!(b"connection_speed", &mut s.connection_speed);
    let v = g!(b"tick_interval");
    if v != i64::MIN {
        s.tick_interval = Duration::from_millis(v.max(0) as u64);
    }
    sec!(b"save_resume_data_interval", &mut s.save_resume_interval);
    i!(b"web_seed_blocks_per_tick", &mut s.web_seed_blocks_per_tick);
    i!(b"min_reconnect_time", &mut s.min_reconnect_time);
    i!(b"max_peerlist_size", &mut s.max_peerlist_size);
    i!(b"max_failcount", &mut s.max_failcount);
    b!(
        b"allow_multiple_connections_per_ip",
        &mut s.allow_multiple_connections_per_ip
    );
    i!(b"auto_manage_interval", &mut s.auto_manage_interval);
    b!(b"anonymous_mode", &mut s.anonymous_mode);
    i!(b"seed_time_limit", &mut s.seed_time_limit);
    i!(b"share_ratio_limit", &mut s.share_ratio_limit);
    i!(b"seed_time_ratio_limit", &mut s.seed_time_ratio_limit);
    i!(b"allowed_fast_set_size", &mut s.allowed_fast_set_size);
    let ua = n.dict_find_string_value(b"user_agent", &[]);
    if !ua.is_empty() {
        s.user_agent = ua.to_vec();
    }
    let stun = n.dict_find_string_value(b"webtorrent_stun_server", &[]);
    if !stun.is_empty() {
        s.webtorrent_stun_server = stun.to_vec();
    }
    i!(
        b"webtorrent_connection_timeout",
        &mut s.webtorrent_connection_timeout
    );
    i!(b"max_webtorrent_offers", &mut s.max_webtorrent_offers);
    s
}
