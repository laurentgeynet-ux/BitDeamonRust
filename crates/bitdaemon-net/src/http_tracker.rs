//! Port of the announce/response half of
//! `http_tracker_connection.cpp`: URL building (`tracker_request`
//! → query string) and `on_response` bencode parsing (compact
//! `peers`/`peers6` strings, dict-list `peers`, scrape responses,
//! failure/warning fields).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bitdaemon_bencode::{BdecodeNode, NodeType, bdecode};
use thiserror::Error;

/// `announce` URL builder + response parser failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HttpTrackerError {
    /// `errors::invalid_tracker_response`.
    #[error("invalid tracker response")]
    InvalidResponse,
    /// `errors::invalid_peers_entry`.
    #[error("invalid peers entry")]
    InvalidPeers,
    /// `errors::invalid_files_entry` (scrape).
    #[error("invalid files entry")]
    InvalidFiles,
    /// `errors::invalid_hash_entry` (scrape).
    #[error("invalid hash entry")]
    InvalidHashEntry,
    /// `errors::tracker_failure` — `failure reason` was present.
    #[error("tracker failure: {0}")]
    Failure(String),
}

/// `extract_peer_info` input — one entry of a dict-style `peers` list.
#[derive(Debug, Clone, Default)]
pub struct DictPeer {
    /// `peer id`.
    pub peer_id: Option<Vec<u8>>,
    /// `ip` — dotted quad or hostname.
    pub ip: String,
    /// `port`.
    pub port: u16,
}

/// `tracker_response` — the parsed announce/scrape answer.
#[derive(Debug, Clone, Default)]
pub struct TrackerResponse {
    /// `resp.interval` (seconds).
    pub interval: i32,
    /// `resp.min_interval`.
    pub min_interval: i32,
    /// `resp.complete` — seeders (-1 = absent).
    pub complete: i64,
    /// `resp.incomplete` — leechers (-1 = absent).
    pub incomplete: i64,
    /// `resp.downloaded` — scrape only.
    pub downloaded: i64,
    /// `resp.downloaders` — scrape only.
    pub downloaders: i64,
    /// `resp.trackerid`.
    pub tracker_id: Option<Vec<u8>>,
    /// `resp.failure_reason`.
    pub failure_reason: Option<String>,
    /// `resp.warning_message`.
    pub warning_message: Option<String>,
    /// Compact/dict peers.
    pub peers: Vec<SocketAddr>,
    /// `peer_id` of dict-style peers.
    pub peer_ids: Vec<Option<Vec<u8>>>,
    /// `resp.external_ip`.
    pub external_ip: Option<IpAddr>,
}

/// Percent-escape like `aux::escape_string` (unreserved chars only).
fn escape(s: &[u8]) -> String {
    s.iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02x}")
            }
        })
        .collect()
}

/// Build the announce query string — `tracker_request` → GET params.
/// `bind_ip`/`trackerid` are optional.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn announce_query(
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    listen_port: u16,
    uploaded: i64,
    downloaded: i64,
    left: i64,
    event: i32,
    num_want: i32,
    key: i32,
    compact: bool,
    no_peer_id: bool,
    tracker_id: Option<&[u8]>,
) -> String {
    // upstream event names: 0 → no param, 1 completed, 2 started,
    // 3 stopped (paused is client-internal)
    const EVENTS: [&str; 4] = ["", "completed", "started", "stopped"];
    let mut q = format!(
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}",
        escape(info_hash),
        escape(peer_id),
        listen_port,
        uploaded,
        downloaded,
        left
    );
    if compact {
        q += "&compact=1";
    }
    if no_peer_id {
        q += "&no_peer_id=1";
    }
    q += &format!("&key={key:x}");
    if (1..=3).contains(&event) {
        q += &format!("&event={}", EVENTS[event as usize]);
    }
    if num_want >= 0 {
        q += &format!("&numwant={num_want}");
    }
    if let Some(t) = tracker_id {
        if !t.is_empty() {
            q += &format!("&trackerid={}", escape(t));
        }
    }
    q
}

fn parse_compact_peers(data: &[u8], stride: usize) -> Result<Vec<SocketAddr>, HttpTrackerError> {
    if data.len() % stride != 0 {
        return Err(HttpTrackerError::InvalidPeers);
    }
    let mut out = Vec::with_capacity(data.len() / stride);
    for c in data.chunks_exact(stride) {
        let port = u16::from_be_bytes([c[stride - 2], c[stride - 1]]);
        let ip = if stride == 6 {
            IpAddr::V4(Ipv4Addr::new(c[0], c[1], c[2], c[3]))
        } else {
            let mut o = [0u8; 16];
            o.copy_from_slice(&c[..16]);
            IpAddr::V6(Ipv6Addr::from(o))
        };
        out.push(SocketAddr::new(ip, port));
    }
    Ok(out)
}

/// `extract_peer_info` — one dict-list peer entry.
fn extract_peer(node: &BdecodeNode<'_>) -> Option<DictPeer> {
    if node.node_type() != NodeType::Dict {
        return None;
    }
    let pid = node.dict_find_string(b"peer id");
    let peer_id = (pid.node_type() == NodeType::String).then(|| pid.string_value().to_vec());
    let ip = node.dict_find_string(b"ip");
    if ip.node_type() != NodeType::String || ip.string_value().is_empty() {
        return None;
    }
    let port = node.dict_find_int_value(b"port", 0);
    if !(0..=65535).contains(&port) {
        return None;
    }
    Some(DictPeer {
        peer_id,
        ip: String::from_utf8_lossy(ip.string_value()).into_owned(),
        port: port as u16,
    })
}

/// `on_response` — parse the bencoded tracker reply.
///
/// `scrape` switches to the `files`/`info_hash` form.
pub fn parse_response(
    body: &[u8],
    scrape: bool,
    scrape_ih: Option<&[u8; 20]>,
) -> Result<TrackerResponse, HttpTrackerError> {
    let e = bdecode(body);
    if e.node_type() != NodeType::Dict {
        return Err(HttpTrackerError::InvalidResponse);
    }
    let mut resp = TrackerResponse {
        interval: e.dict_find_int_value(b"interval", 1800) as i32,
        min_interval: e.dict_find_int_value(b"min interval", 30) as i32,
        ..Default::default()
    };

    let tid = e.dict_find_string(b"tracker id");
    if tid.node_type() == NodeType::String {
        resp.tracker_id = Some(tid.string_value().to_vec());
    }

    let failure = e.dict_find_string(b"failure reason");
    if failure.node_type() == NodeType::String {
        resp.failure_reason = Some(String::from_utf8_lossy(failure.string_value()).into_owned());
        return Err(HttpTrackerError::Failure(
            resp.failure_reason.clone().unwrap_or_default(),
        ));
    }
    let warning = e.dict_find_string(b"warning message");
    if warning.node_type() == NodeType::String {
        resp.warning_message = Some(String::from_utf8_lossy(warning.string_value()).into_owned());
    }

    if scrape {
        let files = e.dict_find_dict(b"files");
        if files.node_type() != NodeType::Dict {
            return Err(HttpTrackerError::InvalidFiles);
        }
        let ih = scrape_ih.ok_or(HttpTrackerError::InvalidHashEntry)?;
        let scrape_data = files.dict_find_dict(ih);
        if scrape_data.node_type() != NodeType::Dict {
            return Err(HttpTrackerError::InvalidHashEntry);
        }
        resp.complete = scrape_data.dict_find_int_value(b"complete", -1);
        resp.incomplete = scrape_data.dict_find_int_value(b"incomplete", -1);
        resp.downloaded = scrape_data.dict_find_int_value(b"downloaded", -1);
        resp.downloaders = scrape_data.dict_find_int_value(b"downloaders", -1);
        return Ok(resp);
    }

    resp.complete = e.dict_find_int_value(b"complete", -1);
    resp.incomplete = e.dict_find_int_value(b"incomplete", -1);
    resp.downloaded = e.dict_find_int_value(b"downloaded", -1);

    let peers_ent = e.dict_find(b"peers");
    match peers_ent.node_type() {
        NodeType::String => {
            resp.peers = parse_compact_peers(peers_ent.string_value(), 6)?;
        }
        NodeType::List => {
            // dict-list form: resolve-able addresses aren't — keep ip
            // strings in peer_ids-compatible shape; only parseable
            // numeric IPs become SocketAddrs.
            for i in 0..peers_ent.list_size() {
                if let Some(p) = extract_peer(&peers_ent.list_at(i)) {
                    if let Ok(ip) = p.ip.parse::<IpAddr>() {
                        resp.peer_ids.push(p.peer_id);
                        resp.peers.push(SocketAddr::new(ip, p.port));
                    }
                }
            }
        }
        _ => {}
    }

    let p6 = e.dict_find_string(b"peers6");
    if p6.node_type() == NodeType::String {
        resp.peers
            .extend(parse_compact_peers(p6.string_value(), 18)?);
    }

    let ext = e.dict_find_string(b"external ip");
    if ext.node_type() == NodeType::String {
        let v = ext.string_value();
        match v.len() {
            4 => resp.external_ip = Some(IpAddr::V4(Ipv4Addr::new(v[0], v[1], v[2], v[3]))),
            16 => {
                let mut o = [0u8; 16];
                o.copy_from_slice(v);
                resp.external_ip = Some(IpAddr::V6(Ipv6Addr::from(o)));
            }
            _ => {}
        }
    }
    Ok(resp)
}
