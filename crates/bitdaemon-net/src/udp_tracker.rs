//! Port of `udp_tracker_connection.cpp` — the UDP tracker protocol
//! (BEP-15): connect (64-bit `connection_id` handshake), announce and
//! scrape exchanges, over the injectable `DatagramSocket`.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use thiserror::Error;
use tokio::time::timeout;

use crate::transport::DatagramSocket;

/// `0x41727101980` — the magic connection id (BEP-15).
pub const CONNECT_MAGIC: u64 = 0x417_2710_1980;

/// Action codes.
pub const ACTION_CONNECT: i32 = 0;
/// announce.
pub const ACTION_ANNOUNCE: i32 = 1;
/// scrape.
pub const ACTION_SCRAPE: i32 = 2;
/// error.
pub const ACTION_ERROR: i32 = 3;

/// UDP tracker failures.
#[derive(Debug, Error)]
pub enum UdpTrackerError {
    /// The tracker sent `action=3` with this message.
    #[error("tracker error: {0}")]
    Tracker(String),
    /// Reply action didn't match the in-flight request, or the
    /// transaction id didn't match.
    #[error("unexpected tracker reply")]
    UnexpectedReply,
    /// `invalid_tracker_response_length` — peer list not a multiple
    /// of the address stride.
    #[error("invalid tracker response length")]
    BadLength,
    /// Reply was too short to parse.
    #[error("tracker reply truncated")]
    Truncated,
    /// The exchange timed out.
    #[error("tracker timeout")]
    Timeout,
    /// transport I/O.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// `tracker_request` (announce side).
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    /// `req.info_hash`.
    pub info_hash: [u8; 20],
    /// `req.pid`.
    pub peer_id: [u8; 20],
    /// `req.downloaded`.
    pub downloaded: i64,
    /// `req.left`.
    pub left: i64,
    /// `req.uploaded`.
    pub uploaded: i64,
    /// `req.event` (0 none, 1 completed, 2 started, 3 stopped).
    pub event: i32,
    /// announce_ip override (0 = default).
    pub announce_ip: Ipv4Addr,
    /// `req.key` — randomized peer key (`std::uint32_t` upstream).
    pub key: u32,
    /// `req.num_want` (-1 = tracker default).
    pub num_want: i32,
    /// `req.listen_port`.
    pub listen_port: u16,
    /// `request_string` — the URL query tail (sent when non-empty).
    pub request_string: String,
}

/// `tracker_response` (announce side).
#[derive(Debug, Clone, Default)]
pub struct AnnounceResponse {
    /// `resp.interval` (seconds — the caller clamps with
    /// `min_announce_interval` like upstream).
    pub interval: i32,
    /// `resp.min_interval` — fixed 60s for UDP upstream.
    pub min_interval: i32,
    /// `resp.incomplete` — leechers.
    pub incomplete: i32,
    /// `resp.complete` — seeders.
    pub complete: i32,
    /// `resp.peers` — compact peer list.
    pub peers: Vec<SocketAddr>,
    /// `resp.trackerid`.
    pub tracker_id: Option<Vec<u8>>,
}

/// One scrape entry.
#[derive(Debug, Clone, Default)]
pub struct ScrapeResponse {
    /// seeders.
    pub complete: i32,
    /// completed downloads.
    pub downloaded: i32,
    /// leechers.
    pub incomplete: i32,
    /// `name` field when present.
    pub name: Option<String>,
}

/// `send_udp_connect` — the 16-byte connect packet.
#[must_use]
pub fn build_connect(transaction_id: u32) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&CONNECT_MAGIC.to_be_bytes());
    out[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    out[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    out
}

/// `send_udp_announce` — the announce packet (98 bytes + optional
/// `request_string` trailer).
#[must_use]
pub fn build_announce(connection_id: u64, transaction_id: u32, req: &AnnounceRequest) -> Vec<u8> {
    let mut out = Vec::with_capacity(98 + req.request_string.len().min(255) + 2);
    out.extend(connection_id.to_be_bytes());
    out.extend(ACTION_ANNOUNCE.to_be_bytes());
    out.extend(transaction_id.to_be_bytes());
    out.extend(req.info_hash);
    out.extend(req.peer_id);
    out.extend(req.downloaded.to_be_bytes());
    out.extend(req.left.to_be_bytes());
    out.extend(req.uploaded.to_be_bytes());
    out.extend(req.event.to_be_bytes());
    out.extend(u32::from(req.announce_ip).to_be_bytes());
    out.extend(req.key.to_be_bytes());
    out.extend(req.num_want.to_be_bytes());
    out.extend(req.listen_port.to_be_bytes());
    if !req.request_string.is_empty() {
        let s = req.request_string.as_bytes();
        let s = &s[..s.len().min(255)];
        out.push(2u8); // URL trailer marker
        out.push(s.len() as u8);
        out.extend(s);
    }
    out
}

/// `send_udp_scrape` — connection_id + action + tid + info_hashes.
#[must_use]
pub fn build_scrape(connection_id: u64, transaction_id: u32, info_hashes: &[[u8; 20]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + 20 * info_hashes.len());
    out.extend(connection_id.to_be_bytes());
    out.extend(ACTION_SCRAPE.to_be_bytes());
    out.extend(transaction_id.to_be_bytes());
    for ih in info_hashes {
        out.extend(ih);
    }
    out
}

fn rd_i32(b: &[u8], off: usize) -> i32 {
    i32::from_be_bytes(b[off..off + 4].try_into().unwrap_or([0; 4]))
}
fn rd_i64(b: &[u8], off: usize) -> i64 {
    i64::from_be_bytes(b[off..off + 8].try_into().unwrap_or([0; 8]))
}

/// `on_announce_response` — parse the announce reply.
///
/// `v6` selects the 18-byte peer stride.
pub fn parse_announce_response(
    buf: &[u8],
    expected_action: i32,
    expected_tid: u32,
    v6: bool,
) -> Result<AnnounceResponse, UdpTrackerError> {
    if buf.len() < 8 {
        return Err(UdpTrackerError::Truncated);
    }
    let action = rd_i32(buf, 0);
    let tid = rd_i32(buf, 4) as u32;
    if tid != expected_tid {
        return Err(UdpTrackerError::UnexpectedReply);
    }
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).into_owned();
        return Err(UdpTrackerError::Tracker(msg));
    }
    if action != expected_action {
        return Err(UdpTrackerError::UnexpectedReply);
    }
    if buf.len() < 20 {
        return Err(UdpTrackerError::Truncated);
    }
    let mut resp = AnnounceResponse {
        interval: rd_i32(buf, 8),
        min_interval: 60,
        incomplete: rd_i32(buf, 12),
        complete: rd_i32(buf, 16),
        ..Default::default()
    };
    let peers = &buf[20..];
    let stride = if v6 { 18 } else { 6 };
    if peers.len() % stride != 0 {
        return Err(UdpTrackerError::BadLength);
    }
    resp.peers.reserve(peers.len() / stride);
    for chunk in peers.chunks_exact(stride) {
        let port = u16::from_be_bytes([chunk[stride - 2], chunk[stride - 1]]);
        let ip = if v6 {
            let mut o = [0u8; 16];
            o.copy_from_slice(&chunk[..16]);
            Ipv6Addr::from(o).into()
        } else {
            Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]).into()
        };
        resp.peers.push(SocketAddr::new(ip, port));
    }
    Ok(resp)
}

/// `on_connect_response` — `[0][tid][connection_id u64]`.
pub fn parse_connect_response(buf: &[u8], expected_tid: u32) -> Result<u64, UdpTrackerError> {
    if buf.len() < 16 {
        return Err(UdpTrackerError::Truncated);
    }
    let tid = rd_i32(buf, 4) as u32;
    // upstream on_receive() checks the transaction id first — stale
    // error packets are ignored, not reported as failures.
    if tid != expected_tid {
        return Err(UdpTrackerError::UnexpectedReply);
    }
    let action = rd_i32(buf, 0);
    if action == ACTION_ERROR {
        return Err(UdpTrackerError::Tracker(
            String::from_utf8_lossy(&buf[8..]).into_owned(),
        ));
    }
    if action != ACTION_CONNECT {
        return Err(UdpTrackerError::UnexpectedReply);
    }
    Ok(rd_i64(buf, 8) as u64)
}

/// `on_scrape_response` — `[2][tid]` then 12-byte entries.
pub fn parse_scrape_response(
    buf: &[u8],
    expected_tid: u32,
) -> Result<Vec<ScrapeResponse>, UdpTrackerError> {
    if buf.len() < 8 {
        return Err(UdpTrackerError::Truncated);
    }
    let action = rd_i32(buf, 0);
    let tid = rd_i32(buf, 4) as u32;
    if action == ACTION_ERROR {
        return Err(UdpTrackerError::Tracker(
            String::from_utf8_lossy(&buf[8..]).into_owned(),
        ));
    }
    if tid != expected_tid || action != ACTION_SCRAPE {
        return Err(UdpTrackerError::UnexpectedReply);
    }
    let entries = &buf[8..];
    // upstream requires at least one 12-byte entry and reads exactly
    // one; extra trailing bytes are ignored, not an error.
    if entries.len() < 12 {
        return Err(UdpTrackerError::BadLength);
    }
    Ok(entries
        .chunks_exact(12)
        .map(|c| ScrapeResponse {
            complete: rd_i32(c, 0),
            downloaded: rd_i32(c, 4),
            incomplete: rd_i32(c, 8),
            name: None,
        })
        .collect())
}

/// The full `udp_tracker_connection` exchange: connect → announce,
/// over any `DatagramSocket`. `read_timeout` bounds each step.
pub async fn udp_announce<S: DatagramSocket>(
    socket: &S,
    target: SocketAddr,
    req: &AnnounceRequest,
    read_timeout: Duration,
) -> Result<AnnounceResponse, UdpTrackerError> {
    let tid: u32 = rand::random();
    socket.send_to(&build_connect(tid), target).await?;

    let mut buf = vec![0u8; 2048];
    let conn_id = loop {
        let (n, _from) = match timeout(read_timeout, socket.recv_from(&mut buf)).await {
            Ok(r) => r?,
            Err(_) => return Err(UdpTrackerError::Timeout),
        };
        if let Ok(id) = parse_connect_response(&buf[..n], tid) {
            break id;
        }
    };

    let tid2: u32 = rand::random();
    socket
        .send_to(&build_announce(conn_id, tid2, req), target)
        .await?;

    loop {
        let (n, _from) = match timeout(read_timeout, socket.recv_from(&mut buf)).await {
            Ok(r) => r?,
            Err(_) => return Err(UdpTrackerError::Timeout),
        };
        match parse_announce_response(&buf[..n], ACTION_ANNOUNCE, tid2, target.is_ipv6()) {
            Err(UdpTrackerError::UnexpectedReply) => continue, // ignore stale packets
            r => return r,
        }
    }
}
