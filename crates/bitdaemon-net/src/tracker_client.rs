// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `tracker_event.hpp` + the wire halves of
//! `http_tracker_connection.cpp`/`udp_tracker_connection.cpp`.
//!
//! `tracker_connection` — one full announce exchange per call:
//! HTTP via `http_get` + `parse_response`, UDP via the BEP-15
//! connect→announce handshake over the injected `DatagramSocket`.
//! Retries/failover live in `session::tracker_list`; this module is
//! the wire half of `http_tracker_connection`/`udp_tracker_connection`'s
//! `start()`/`on_response` pair.

use std::net::SocketAddr;
use std::time::Duration;

use crate::http_conn::{HttpError, http_get};
use crate::http_tracker::{HttpTrackerError, TrackerResponse, announce_query, parse_response};
use crate::transport::{BoxedPeerStream, DatagramSocket};
use crate::udp_tracker::{
    AnnounceRequest, UdpTrackerError, build_announce, build_connect, parse_announce_response,
    parse_connect_response,
};

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::mpsc;

/// Demux des reponses UDP tracker sur la socket **partagee**
/// (ADR-0008) : upstream utilise une socket dediee par connexion
/// tracker, ici la pompe UDP unique route chaque datagramme entrant
/// a l'attente ouverte par `transaction_id`. Une reponse BEP-15 est
/// `u32 action` (0=connect, 1=announce, 2=scrape, 3=error) suivi de
/// `u32 transaction_id` — les trames DHT (`d...`) et LSD
/// (`BT-SEARCH`) ne peuvent pas matcher `action <= 3`.
/// Datagramme entrant route a une attente tracker.
type UdpReply = (SocketAddr, Vec<u8>);

#[derive(Debug, Default)]
pub struct UdpTrackerDemux {
    waiters: Mutex<HashMap<u32, mpsc::UnboundedSender<UdpReply>>>,
    /// `m_connection_cache` — `udp_tracker_connection_manager`'s
    /// per-address `connection_id` cache (`udp_tracker_connection.cpp`
    /// skips the connect round-trip while a cached id is unexpired).
    /// Keyed by tracker IP like upstream (`m_connection_cache.find(
    /// m_target.address())`); expiry = `udp_tracker_token_expiry`.
    conn_cache: Mutex<HashMap<std::net::IpAddr, (u64, std::time::Instant)>>,
}

impl UdpTrackerDemux {
    /// cached `connection_id` for `ip` when still fresh.
    #[must_use]
    pub fn cached_conn_id(&self, ip: std::net::IpAddr) -> Option<u64> {
        self.conn_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ip)
            .filter(|(_, exp)| *exp > std::time::Instant::now())
            .map(|(id, _)| *id)
    }

    /// record `id` for `ip`, expiring after `ttl`
    /// (`udp_tracker_token_expiry`).
    pub fn store_conn_id(&self, ip: std::net::IpAddr, id: u64, ttl: Duration) {
        self.conn_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(ip, (id, std::time::Instant::now() + ttl));
    }

    /// Enregistre une attente pour `tid` ; la pompe appellera
    /// [`Self::route`] sur chaque datagramme.
    #[must_use]
    pub fn register(&self, tid: u32) -> mpsc::UnboundedReceiver<UdpReply> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tid, tx);
        rx
    }

    /// Libere l'attente `tid` (fin d'echange ou timeout).
    pub fn unregister(&self, tid: u32) {
        self.waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&tid);
    }

    /// `true` si `buf` est une reponse tracker consommee par un
    /// waiter — a appeler dans la pompe UDP avant le routage
    /// DHT/LSD/NAT-PMP.
    #[must_use]
    pub fn route(&self, from: SocketAddr, buf: &[u8]) -> bool {
        if buf.len() < 8 {
            return false;
        }
        let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if action > 3 {
            return false;
        }
        let tid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let tx = self
            .waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tid)
            .cloned();
        match tx {
            Some(tx) => {
                let _ = tx.send((from, buf.to_vec()));
                true
            }
            None => false,
        }
    }
}

/// Attend la reponse `tid` via le demux (remplace le `recv_from`
/// direct qui se faisait voler les paquets par la pompe UDP). La
/// registration est retiree au `Drop` — la boucle de retry peut
/// reutiliser le meme waiter sans fuite dans le demux.
struct UdpWaiter<'a> {
    tid: u32,
    rx: mpsc::UnboundedReceiver<UdpReply>,
    demux: &'a UdpTrackerDemux,
}

impl Drop for UdpWaiter<'_> {
    fn drop(&mut self) {
        self.demux.unregister(self.tid);
    }
}

/// Enregistre l'attente **avant** l'envoi du datagramme — sinon une
/// reponse tres rapide arrive avant le `register` et est perdue.
fn udp_waiter(demux: &UdpTrackerDemux, tid: u32) -> UdpWaiter<'_> {
    UdpWaiter {
        tid,
        rx: demux.register(tid),
        demux,
    }
}

/// Receive the next reply for `tid`, dropping packets that didn't
/// come from `expected` (upstream `on_receive`: `m_target != ep` →
/// ignore; a `None` expected = proxied hostname target, where
/// upstream accepts any source since `m_target` is unspecified).
async fn wait_reply(
    waiter: &mut UdpWaiter<'_>,
    timeout: Duration,
    expected: Option<SocketAddr>,
) -> Result<Vec<u8>, UdpTrackerError> {
    loop {
        let (from, buf) = tokio::time::timeout(timeout, waiter.rx.recv())
            .await
            .map_err(|_| UdpTrackerError::Timeout)?
            .ok_or(UdpTrackerError::Timeout)?;
        if expected.is_none_or(|e| e == from) {
            return Ok(buf);
        }
    }
}

/// announce parameters shared by both protocols — the fields of
/// upstream `tracker_request` (`tracker_manager.hpp`).
#[derive(Debug, Clone)]
pub struct AnnounceParams {
    /// `req.url` — the tracker announce URL (also feeds the UDP
    /// `request_string` trailer).
    pub url: String,
    /// `req.info_hash` — v1 info hash.
    pub info_hash: [u8; 20],
    /// `req.pid` — our peer id.
    pub peer_id: [u8; 20],
    /// `req.listen_port`.
    pub port: u16,
    /// `req.uploaded`.
    pub uploaded: i64,
    /// `req.downloaded`.
    pub downloaded: i64,
    /// `req.left`.
    pub left: i64,
    /// `req.corrupt` — downloaded bytes that failed the hash check
    /// (always sent on HTTP announces).
    pub corrupt: i64,
    /// `req.redundant` — received bytes we already had (sent when
    /// `report_redundant_bytes` is on).
    pub redundant: i64,
    /// `req.event` — `event_t`: 0 none, 1 completed, 2 started,
    /// 3 stopped, 4 paused.
    pub event: i32,
    /// `req.num_want`.
    pub num_want: i32,
    /// `req.key` — our random announce key (`std::uint32_t`
    /// upstream, rendered `%08X` on HTTP).
    pub key: u32,
    /// `req.trackerid` — echoed `tracker id` from a previous
    /// response (empty = none).
    pub tracker_id: Vec<u8>,
    /// `req.ipv4` — per-listen-socket external v4 addresses →
    /// repeated `&ipv4=` params.
    pub ipv4: Vec<std::net::Ipv4Addr>,
    /// `req.ipv6` — repeated `&ipv6=` params.
    pub ipv6: Vec<std::net::Ipv6Addr>,
    /// `tracker_request::private_torrent` — the torrent's metainfo
    /// `private` flag; in `anonymous_mode` a private torrent still
    /// sends the real user-agent (private trackers may require it).
    pub private_torrent: bool,
    /// `tracker_request::high_priority` — set by `force_reannounce`
    /// and inherited by failover re-announces; the engine's HTTP
    /// announce queue inserts these at the front like upstream's
    /// `tracker_manager`.
    pub high_priority: bool,
    /// `tracker_request_flags_t::i2p` — interpret `peers` as
    /// 32-byte destinations in HTTP responses.
    pub i2p: bool,
    /// `req.filter` — shared `ip_filter` attached when
    /// `apply_ip_filter_to_trackers` + the torrent's
    /// `apply_ip_filter` flag are both set; resolved tracker
    /// endpoints are dropped through it (`on_filter` upstream:
    /// all-blocked → `banned_by_ip_filter` failure).
    pub filter: Option<std::sync::Arc<crate::ip_filter::IpFilter>>,
}

/// announce errors (protocol or transport). Carries the metadata
/// upstream's `tracker_connection::fail(ec, op, msg, interval,
/// min_interval)` delivers to `tracker_request_error` — HTTP status
/// codes (for the 410→`dont_try_again` rule), retry-interval hints
/// from failure responses, and the `operation_t` phase for alerts.
#[derive(Debug, thiserror::Error)]
pub enum AnnounceError {
    /// http layer — `HttpError::Status(code)` keeps the status so
    /// the session can apply the `410 Gone` rule.
    #[error("http: {0}")]
    Http(#[from] HttpError),
    /// http tracker protocol.
    #[error("tracker: {0}")]
    HttpTracker(#[from] HttpTrackerError),
    /// udp tracker protocol.
    #[error("udp tracker: {0}")]
    Udp(#[from] UdpTrackerError),
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// `errors::timed_out` — the whole exchange exceeded
    /// `tracker_completion_timeout`/`stop_tracker_timeout`.
    #[error("timed out")]
    TimedOut,
    /// tracker refused (`failure_reason`) — `interval`/`min_interval`
    /// are the tracker-provided retry hints upstream feeds
    /// `announce_infohash::failed` (`tracker_manager.cpp:183`).
    #[error("failure: {message}")]
    Failure {
        /// the `failure reason` string.
        message: String,
        /// `interval` from the failure response (seconds; 0 = absent).
        interval: i32,
        /// `min interval` from the failure response (seconds).
        min_interval: i32,
    },
    /// `errors::ssrf_mitigation` — the tracker URL already carried
    /// tracker query args while `ssrf_mitigation` is enabled, or the
    /// endpoint filter dropped every resolved address.
    #[error("ssrf mitigation: tracker url contains announce arguments")]
    Ssrf,
    /// engine-side setup/transport failure carrying an
    /// `operation_t` label (`hostname_lookup`, `connect`,
    /// `sock_read`…) for `tracker_error_alert::op`.
    #[error("{op}: {message}")]
    Op {
        /// `operation_t` name.
        op: &'static str,
        /// error detail.
        message: String,
    },
}

impl AnnounceError {
    /// HTTP status code when the tracker answered with a non-200
    /// status (`http_category` upstream — the `410` check).
    #[must_use]
    pub fn http_status(&self) -> Option<i32> {
        match self {
            Self::Http(HttpError::Status(c)) => Some(*c),
            _ => None,
        }
    }

    /// `tracker_connection::fail`'s `retry_interval` — the
    /// failure response's `interval`, falling back to
    /// `min interval` (`interval ?: min_interval` upstream).
    #[must_use]
    pub fn retry_interval(&self) -> Duration {
        let (iv, min_iv) = match self {
            Self::Failure {
                interval,
                min_interval,
                ..
            } => (*interval, *min_interval),
            Self::HttpTracker(HttpTrackerError::Failure {
                interval,
                min_interval,
                ..
            }) => (*interval, *min_interval),
            _ => (0, 0),
        };
        Duration::from_secs((if iv == 0 { min_iv } else { iv }).max(0) as u64)
    }

    /// `operation_t` — which announce phase failed (alert payload).
    #[must_use]
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Op { op, .. } => op,
            Self::Http(HttpError::TimedOut) | Self::TimedOut => "timed_out",
            Self::Http(_) => "sock_read",
            Self::HttpTracker(_) | Self::Failure { .. } | Self::Ssrf => "bittorrent",
            Self::Udp(_) => "udp_tracker",
            Self::Io(_) => "sock_read",
        }
    }
}

/// `http_tracker_connection::start` URL building — the full GET
/// target: announce URL + query params (`?`/`&` join), with the
/// `ssrf_mitigation` rejection for pre-baked tracker arguments.
pub fn announce_url(
    p: &AnnounceParams,
    opts: &crate::http_tracker::AnnounceOptions,
) -> Result<String, AnnounceError> {
    if let Some(i) = p.url.find('?') {
        if opts.ssrf_mitigation && crate::parse_url::has_tracker_query_string(&p.url[i + 1..]) {
            return Err(AnnounceError::Ssrf);
        }
    }
    let q = announce_query(
        &p.info_hash,
        &p.peer_id,
        p.port,
        p.uploaded,
        p.downloaded,
        p.left,
        p.corrupt,
        p.redundant,
        p.key,
        p.event,
        p.num_want,
        (!p.tracker_id.is_empty()).then_some(p.tracker_id.as_slice()),
        opts,
        &p.ipv4,
        &p.ipv6,
    );
    let sep = if p.url.contains('?') { "&" } else { "?" };
    Ok(format!("{}{sep}{q}", p.url))
}

/// `http_tracker_connection::on_response` — GET `url` verbatim
/// (for a redirect retry `url` is the resolved `Location`, matching
/// upstream `http_connection::on_redirect` which re-GETs it as-is),
/// non-200 → `HttpError::Status` (the session applies the 410 rule),
/// then `parse_tracker_response`; the reply interval is floored at
/// `min_announce_interval` like upstream.
#[allow(clippy::too_many_arguments)]
pub async fn announce_http_get(
    s: &mut BoxedPeerStream,
    url: &str,
    i2p: bool,
    min_announce_interval: i32,
    extra_headers: &[(String, String)],
    max_response: usize,
    read_timeout: Option<Duration>,
    stats: Option<&mut crate::http_conn::WireStats>,
) -> Result<TrackerResponse, AnnounceError> {
    let resp = http_get(s, url, extra_headers, max_response, read_timeout, stats).await?;
    if resp.status() != 200 {
        return Err(HttpError::Status(resp.status()).into());
    }
    let mut r = parse_response(&resp.body(), false, None, i2p).map_err(|e| match e {
        HttpTrackerError::Failure {
            message,
            interval,
            min_interval,
        } => AnnounceError::Failure {
            message,
            interval,
            min_interval,
        },
        other => AnnounceError::HttpTracker(other),
    })?;
    r.interval = r.interval.max(min_announce_interval);
    Ok(r)
}

/// `http_tracker_connection` — announce over an established stream
/// (proxy/tunnel handshake already done by the caller). `p.url` is
/// the full announce URL; our params are appended like upstream
/// (`?`/`&` depending on a baked-in query string, with the
/// `ssrf_mitigation` check). `read_timeout` is the per-read
/// `tracker_receive_timeout`; `stats` collects wire bytes.
pub async fn announce_http(
    s: &mut BoxedPeerStream,
    p: &AnnounceParams,
    opts: &crate::http_tracker::AnnounceOptions,
    extra_headers: &[(String, String)],
    max_response: usize,
    read_timeout: Option<Duration>,
    stats: Option<&mut crate::http_conn::WireStats>,
) -> Result<TrackerResponse, AnnounceError> {
    let url = announce_url(p, opts)?;
    announce_http_get(
        s,
        &url,
        p.i2p,
        opts.min_announce_interval,
        extra_headers,
        max_response,
        read_timeout,
        stats,
    )
    .await
}

/// `update_transaction_id` — upstream draws `random(0xfffffffe) + 1`
/// (never 0, never 0xffffffff).
fn new_tid() -> u32 {
    rand::random::<u32>() % 0xffff_fffe + 1
}

/// `tracker_connection`-scale timeouts for one UDP exchange:
/// `receive` bounds each datagram wait (`tracker_receive_timeout`);
/// the caller wraps the whole call in the completion deadline
/// (`tracker_completion_timeout`/`stop_tracker_timeout`).
#[derive(Debug, Clone, Copy)]
pub struct TrackerTimeouts {
    /// per-datagram wait (`tracker_receive_timeout`, 10 s upstream).
    pub receive: Duration,
    /// `udp_tracker_token_expiry` — connection-id cache TTL (60 s).
    pub conn_id_ttl: Duration,
}

/// UDP tracker destination — `m_target` upstream: either a resolved
/// endpoint or a hostname the SOCKS5 proxy resolves (`proxy_hostnames`
/// and a socks5 proxy → `send_hostname` ATYP-3 datagrams, no local
/// DNS leak, see `udp_tracker_connection::start`).
#[derive(Debug, Clone, Copy)]
pub enum UdpTarget<'a> {
    /// Resolved endpoint.
    Addr(SocketAddr),
    /// Unresolved `host:port` — requires a proxy datagram lane that
    /// implements [`DatagramSocket::send_hostname`].
    Name { host: &'a str, port: u16 },
}

impl UdpTarget<'_> {
    /// `m_target.is_v6()` — names resolve to the unspecified v4
    /// placeholder upstream; literal IPv6 names report v6.
    fn is_ipv6(self) -> bool {
        match self {
            Self::Addr(a) => a.is_ipv6(),
            Self::Name { host, .. } => host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_ipv6()),
        }
    }

    /// `udp_socket::send`/`send_hostname` with
    /// `tracker_connection` set — the per-datagram proxy gate.
    async fn send(self, dgram: &dyn DatagramSocket, pkt: &[u8]) -> std::io::Result<usize> {
        let flags = crate::transport::DatagramSendFlags::TRACKER_CONNECTION;
        match self {
            Self::Addr(a) => dgram.send_to_flags(pkt, a, flags).await,
            Self::Name { host, port } => dgram.send_hostname(host, port, pkt, flags).await,
        }
    }
}

/// BEP-15 connect phase — answered `connection_id`s are cached per
/// tracker address (`udp_tracker_connection_manager::
/// m_connection_cache` upstream) and reused while unexpired.
async fn udp_connect(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: UdpTarget<'_>,
    timeouts: &TrackerTimeouts,
    stats: &mut crate::http_conn::WireStats,
) -> Result<u64, AnnounceError> {
    // the connection-id cache is keyed by resolved IP — a proxied
    // hostname has none (`m_target` is the unspecified placeholder).
    let cache_key = match tracker {
        UdpTarget::Addr(a) => Some(a.ip()),
        UdpTarget::Name { .. } => None,
    };
    if let Some(id) = cache_key.and_then(|ip| demux.cached_conn_id(ip)) {
        return Ok(id);
    }
    let tid = new_tid();
    let mut w = udp_waiter(demux, tid);
    let pkt = build_connect(tid);
    let _ = tracker.send(dgram, &pkt).await?;
    stats.sent += pkt.len() as i64 + 28; // UDP/IP header estimate
    let expected = match tracker {
        UdpTarget::Addr(a) => Some(a),
        UdpTarget::Name { .. } => None,
    };
    loop {
        let buf = wait_reply(&mut w, timeouts.receive, expected).await?;
        stats.recv += buf.len() as i64 + 28;
        match parse_connect_response(&buf, tid) {
            Ok(id) => {
                if let Some(ip) = cache_key {
                    demux.store_conn_id(ip, id, timeouts.conn_id_ttl);
                }
                return Ok(id);
            }
            Err(UdpTrackerError::Tracker(m)) => {
                return Err(AnnounceError::Failure {
                    message: m,
                    interval: 0,
                    min_interval: 0,
                });
            }
            Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// `udp_tracker_connection` — the BEP-15 exchange: (cached-)connect
/// then announce, one datagram each (upstream sends once — a receive
/// timeout fails the announce; `m_attempts` is just a counter).
/// Stray packets failing the tid/length checks are ignored and
/// rearm the `timeout` receive deadline like upstream's
/// `restart_read_timeout`. `opts` carries
/// `anonymous_mode`/`announce_ip`/`min_announce_interval` like the
/// session settings upstream reads.
pub async fn announce_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: UdpTarget<'_>,
    p: &AnnounceParams,
    opts: &crate::http_tracker::AnnounceOptions,
    timeouts: TrackerTimeouts,
    stats: &mut crate::http_conn::WireStats,
) -> Result<TrackerResponse, AnnounceError> {
    let conn_id = udp_connect(dgram, demux, tracker, &timeouts, stats).await?;

    // the ip field: settings announce_ip when !anonymous_mode and it
    // parses as v4, else 0 (upstream udp_tracker_connection).
    let announce_ip = if !opts.anonymous_mode {
        opts.announce_ip
            .as_deref()
            .and_then(|s| s.parse::<std::net::IpAddr>().ok())
            .and_then(|ip| match ip {
                std::net::IpAddr::V4(v4) => Some(v4),
                std::net::IpAddr::V6(_) => None,
            })
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED)
    } else {
        std::net::Ipv4Addr::UNSPECIFIED
    };
    // `request_string` trailer: the announce URL's path+query.
    let request_string = crate::parse_url::parse_url_components(&p.url)
        .map(|c| c.path)
        .unwrap_or_default();

    // announce exchange — one datagram; strays rearm the deadline
    let tid = new_tid();
    let req = AnnounceRequest {
        info_hash: p.info_hash,
        peer_id: p.peer_id,
        downloaded: p.downloaded,
        left: p.left,
        uploaded: p.uploaded,
        event: p.event,
        announce_ip,
        key: p.key,
        num_want: p.num_want,
        listen_port: p.port,
        request_string: request_string.clone(),
    };
    let mut w = udp_waiter(demux, tid);
    let pkt = build_announce(conn_id, tid, &req);
    let _ = tracker.send(dgram, &pkt).await?;
    stats.sent += pkt.len() as i64 + 28;
    let expected = match tracker {
        UdpTarget::Addr(a) => Some(a),
        UdpTarget::Name { .. } => None,
    };
    loop {
        let buf = wait_reply(&mut w, timeouts.receive, expected).await?;
        stats.recv += buf.len() as i64 + 28;
        match parse_announce_response(&buf, 1, tid, tracker.is_ipv6()) {
            Ok(r) => {
                return Ok(TrackerResponse {
                    interval: r.interval.max(opts.min_announce_interval),
                    min_interval: 60,
                    complete: i64::from(r.complete),
                    incomplete: i64::from(r.incomplete),
                    tracker_id: r.tracker_id,
                    peers: r.peers,
                    ..Default::default()
                });
            }
            Err(UdpTrackerError::Tracker(m)) => {
                return Err(AnnounceError::Failure {
                    message: m,
                    interval: 0,
                    min_interval: 0,
                });
            }
            Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// `scrape_udp` — (cached-)connect then a single scrape datagram
/// (BEP-15 action 2); one send each, like `announce_udp` upstream.
pub async fn scrape_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: UdpTarget<'_>,
    info_hash: &[u8; 20],
    timeouts: TrackerTimeouts,
    stats: &mut crate::http_conn::WireStats,
) -> Result<crate::udp_tracker::ScrapeResponse, AnnounceError> {
    use crate::udp_tracker::{build_scrape, parse_scrape_response};
    let conn_id = udp_connect(dgram, demux, tracker, &timeouts, stats).await?;
    let tid = new_tid();
    let pkt = build_scrape(conn_id, tid, &[*info_hash]);
    let mut w = udp_waiter(demux, tid);
    let _ = tracker.send(dgram, &pkt).await?;
    stats.sent += pkt.len() as i64 + 28;
    let expected = match tracker {
        UdpTarget::Addr(a) => Some(a),
        UdpTarget::Name { .. } => None,
    };
    loop {
        let buf = wait_reply(&mut w, timeouts.receive, expected).await?;
        stats.recv += buf.len() as i64 + 28;
        match parse_scrape_response(&buf, tid) {
            Ok(v) => {
                return v
                    .into_iter()
                    .next()
                    .ok_or(AnnounceError::Udp(UdpTrackerError::BadLength));
            }
            Err(UdpTrackerError::Tracker(m)) => {
                return Err(AnnounceError::Failure {
                    message: m,
                    interval: 0,
                    min_interval: 0,
                });
            }
            Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// the full scrape GET URL — `url` + binary `info_hash` query arg
/// (`?`/`&` join), verbatim for redirect retries.
#[must_use]
pub fn scrape_url_full(url: &str, info_hash: &[u8; 20]) -> String {
    let q = crate::http_tracker::scrape_query(info_hash);
    let sep = if url.contains('?') { "&" } else { "?" };
    format!("{url}{sep}{q}")
}

/// `scrape` reply path — GET `url` verbatim, non-200 →
/// `HttpError::Status`, then the `files`/`info_hash` dict parse
/// (upstream maps it onto the same `tracker_response` fields).
#[allow(clippy::too_many_arguments)]
pub async fn scrape_http_get(
    s: &mut BoxedPeerStream,
    url: &str,
    info_hash: &[u8; 20],
    extra_headers: &[(String, String)],
    max_response: usize,
    read_timeout: Option<Duration>,
    stats: Option<&mut crate::http_conn::WireStats>,
) -> Result<TrackerResponse, AnnounceError> {
    let resp = http_get(s, url, extra_headers, max_response, read_timeout, stats).await?;
    if resp.status() != 200 {
        return Err(HttpError::Status(resp.status()).into());
    }
    parse_response(&resp.body(), true, Some(info_hash), false).map_err(|e| match e {
        HttpTrackerError::Failure {
            message,
            interval,
            min_interval,
        } => AnnounceError::Failure {
            message,
            interval,
            min_interval,
        },
        other => AnnounceError::HttpTracker(other),
    })
}

/// `scrape_http` — GET the scrape url with `info_hash`, parse the
/// `files`/`info_hash` dict (upstream maps it onto the same
/// `tracker_response` fields). `url` is the full scrape URL (from
/// [`crate::http_tracker::scrape_url`]); scrape requests carry only
/// `info_hash` upstream.
pub async fn scrape_http(
    s: &mut BoxedPeerStream,
    url: &str,
    info_hash: &[u8; 20],
    extra_headers: &[(String, String)],
    max_response: usize,
    read_timeout: Option<Duration>,
    stats: Option<&mut crate::http_conn::WireStats>,
) -> Result<TrackerResponse, AnnounceError> {
    let url = scrape_url_full(url, info_hash);
    scrape_http_get(
        s,
        &url,
        info_hash,
        extra_headers,
        max_response,
        read_timeout,
        stats,
    )
    .await
}
