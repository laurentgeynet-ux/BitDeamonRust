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
    /// `errors::invalid_peer_dict` — a `peers` list entry that isn't
    /// a dict (only fatal when *every* entry is invalid).
    #[error("invalid peer dict")]
    InvalidPeerDict,
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
/// Upstream keeps the `ip` field verbatim — it may be a hostname the
/// session resolves later, not necessarily a numeric IP.
#[derive(Debug, Clone, Default)]
pub struct DictPeer {
    /// `peer id` — only copied when exactly 20 bytes (upstream
    /// zeroes it otherwise).
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
    /// i2p destinations (`peers` as 32-byte entries when the request
    /// was marked `i2p`).
    pub i2p_peers: Vec<[u8; 32]>,
    /// dict-style `peers` entries whose `ip` didn't parse as a
    /// numeric address — upstream keeps them as `peer_entry.hostname`
    /// for the session to resolve.
    pub hostname_peers: Vec<DictPeer>,
}

/// `aux::escape_string` — libtorrent's exact unreserved table
/// (`-_!.~*()` + alnum, lowercase hex escapes).
fn escape(s: &[u8]) -> String {
    String::from_utf8_lossy(&bitdaemon_core::escape_string::escape_string(s)).into_owned()
}

/// Session settings that gate optional announce params — the
/// `settings_pack` conditions from `http_tracker_connection::start()`.
#[derive(Debug, Clone)]
pub struct AnnounceOptions {
    /// `in_enc_policy != pe_disabled && announce_crypto_support`
    /// → `&supportcrypto=1`.
    pub supportcrypto: bool,
    /// `report_redundant_bytes` → `&redundant=<bytes>`.
    pub report_redundant: bool,
    /// `anonymous_mode` — suppresses `&ip=`/announce-ip reporting.
    pub anonymous_mode: bool,
    /// `settings_pack::announce_ip` → `&ip=` (suppressed when
    /// `anonymous_mode`).
    pub announce_ip: Option<String>,
    /// `ssrf_mitigation` — reject announce URLs whose baked-in query
    /// already contains tracker arguments.
    pub ssrf_mitigation: bool,
    /// `min_announce_interval` — floor applied to the response
    /// `interval` (`tracker_response::interval`).
    pub min_announce_interval: i32,
}

impl Default for AnnounceOptions {
    /// Upstream `settings_pack` defaults: `announce_crypto_support`
    /// (with `in_enc_policy != pe_disabled`), `report_redundant_bytes`,
    /// `ssrf_mitigation` are all on; `min_announce_interval` = 5 min.
    fn default() -> Self {
        Self {
            supportcrypto: true,
            report_redundant: true,
            anonymous_mode: false,
            announce_ip: None,
            ssrf_mitigation: true,
            min_announce_interval: 300,
        }
    }
}

/// Build the announce query string — the exact parameter order of
/// `http_tracker_connection::start()`: `info_hash` then the
/// `snprintf` block (`peer_id`…`no_peer_id`) then the conditional
/// `supportcrypto`/`redundant`/`trackerid`/`ip`/`ipv4`/`ipv6` tails.
/// `corrupt`, `numwant`, `compact=1`, `no_peer_id=1` are always
/// emitted; `key` is `%08X` uppercase-hex.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn announce_query(
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    listen_port: u16,
    uploaded: i64,
    downloaded: i64,
    left: i64,
    corrupt: i64,
    redundant: i64,
    key: u32,
    event: i32,
    num_want: i32,
    tracker_id: Option<&[u8]>,
    opts: &AnnounceOptions,
    ipv4: &[Ipv4Addr],
    ipv6: &[Ipv6Addr],
) -> String {
    // upstream event_t: 1 completed, 2 started, 3 stopped, 4 paused
    const EVENTS: [&str; 4] = ["completed", "started", "stopped", "paused"];
    let mut q = format!(
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&corrupt={}&key={key:08X}",
        escape(info_hash),
        escape(peer_id),
        listen_port,
        uploaded,
        downloaded,
        left,
        corrupt,
    );
    if (1..=4).contains(&event) {
        q += &format!("&event={}", EVENTS[event as usize - 1]);
    }
    q += &format!("&numwant={num_want}&compact=1&no_peer_id=1");
    if opts.supportcrypto {
        q += "&supportcrypto=1";
    }
    if opts.report_redundant {
        q += &format!("&redundant={redundant}");
    }
    if let Some(t) = tracker_id {
        if !t.is_empty() {
            q += &format!("&trackerid={}", escape(t));
        }
    }
    if !opts.anonymous_mode {
        if let Some(ip) = &opts.announce_ip {
            if !ip.is_empty() {
                q += &format!("&ip={}", escape(ip.as_bytes()));
            }
        }
    }
    for v4 in ipv4 {
        q += &format!("&ipv4={}", escape(v4.to_string().as_bytes()));
    }
    for v6 in ipv6 {
        q += &format!("&ipv6={}", escape(v6.to_string().as_bytes()));
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

/// `extract_peer_info` — one dict-list peer entry. `Err` carries the
/// error upstream would set (`invalid_peer_dict` vs
/// `invalid_tracker_response`); it's only fatal when *no* entry in
/// the list parses.
fn extract_peer(node: &BdecodeNode<'_>) -> Result<DictPeer, HttpTrackerError> {
    if node.node_type() != NodeType::Dict {
        return Err(HttpTrackerError::InvalidPeerDict);
    }
    // upstream copies pid only when exactly 20 bytes, else zeroes it
    let pid = node.dict_find_string(b"peer id");
    let peer_id = (pid.node_type() == NodeType::String && pid.string_value().len() == 20)
        .then(|| pid.string_value().to_vec());
    let ip = node.dict_find_string(b"ip");
    if ip.node_type() != NodeType::String {
        return Err(HttpTrackerError::InvalidResponse);
    }
    let port_node = node.dict_find_int(b"port");
    if port_node.node_type() != NodeType::Int {
        return Err(HttpTrackerError::InvalidResponse);
    }
    let port = port_node.int_value();
    if !(0..=65535).contains(&port) {
        return Err(HttpTrackerError::InvalidResponse);
    }
    Ok(DictPeer {
        peer_id,
        ip: String::from_utf8_lossy(ip.string_value()).into_owned(),
        port: port as u16,
    })
}

/// `on_response` — parse the bencoded tracker reply.
///
/// `scrape` switches to the `files`/`info_hash` form; `i2p` makes
/// `peers` a list of 32-byte destinations instead of compact v4
/// (`tracker_request_flags_t::i2p`).
pub fn parse_response(
    body: &[u8],
    scrape: bool,
    scrape_ih: Option<&[u8; 20]>,
    i2p: bool,
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
            let data = peers_ent.string_value();
            if i2p {
                // 32-byte i2p destinations
                if data.len() % 32 != 0 {
                    return Err(HttpTrackerError::InvalidPeers);
                }
                for c in data.chunks_exact(32) {
                    let mut d = [0u8; 32];
                    d.copy_from_slice(c);
                    resp.i2p_peers.push(d);
                }
            } else {
                resp.peers = parse_compact_peers(data, 6)?;
            }
        }
        NodeType::List => {
            // dict-list form. `ip` may be a hostname — upstream stores
            // it verbatim (`peer_entry.hostname`) for later resolve;
            // only a fully-invalid list fails the response.
            let mut parse_error = None;
            for i in 0..peers_ent.list_size() {
                match extract_peer(&peers_ent.list_at(i)) {
                    Ok(p) => {
                        if let Ok(ip) = p.ip.parse::<IpAddr>() {
                            resp.peer_ids.push(p.peer_id.clone());
                            resp.peers.push(SocketAddr::new(ip, p.port));
                        } else {
                            resp.hostname_peers.push(p);
                        }
                    }
                    Err(e) => parse_error = Some(e),
                }
            }
            if resp.peers.is_empty() && resp.hostname_peers.is_empty() {
                if let Some(e) = parse_error {
                    return Err(e);
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

/// `http_tracker_connection` scrape URL — replace the **first**
/// "announce" with "scrape" (`str.find` upstream —
/// `errors::scrape_not_available` → `None`).
#[must_use]
pub fn scrape_url(url: &str) -> Option<String> {
    let pos = url.find("announce")?;
    let mut s = url.to_string();
    s.replace_range(pos..pos + 8, "scrape");
    Some(s)
}

/// scrape GET query — just `info_hash` (compact binary per BEP-15
/// semantics for http scrapes too).
#[must_use]
pub fn scrape_query(info_hash: &[u8; 20]) -> String {
    format!("info_hash={}", escape(info_hash))
}
