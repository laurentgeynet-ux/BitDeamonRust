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
}

impl UdpTrackerDemux {
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

async fn wait_reply(
    waiter: &mut UdpWaiter<'_>,
    timeout: Duration,
) -> Result<Vec<u8>, UdpTrackerError> {
    tokio::time::timeout(timeout, waiter.rx.recv())
        .await
        .map_err(|_| UdpTrackerError::Timeout)?
        .map(|(_from, buf)| buf)
        .ok_or(UdpTrackerError::Timeout)
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

/// announce errors (protocol or transport).
#[derive(Debug, thiserror::Error)]
pub enum AnnounceError {
    /// http layer.
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
    /// tracker refused (`failure_reason`).
    #[error("failure: {0}")]
    Failure(String),
    /// `errors::ssrf_mitigation` — the tracker URL already carried
    /// tracker query args while `ssrf_mitigation` is enabled.
    #[error("ssrf mitigation: tracker url contains announce arguments")]
    Ssrf,
}

/// `http_tracker_connection` — announce over an established stream
/// (proxy/tunnel handshake already done by the caller). `p.url` is
/// the full announce URL; our params are appended like upstream
/// (`?`/`&` depending on a baked-in query string, with the
/// `ssrf_mitigation` check).
pub async fn announce_http(
    s: &mut BoxedPeerStream,
    p: &AnnounceParams,
    opts: &crate::http_tracker::AnnounceOptions,
    extra_headers: &[(String, String)],
    max_response: usize,
) -> Result<TrackerResponse, AnnounceError> {
    // upstream start(): a pre-baked `?` query means `&` join, and is
    // rejected under ssrf_mitigation when it holds announce args.
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
    let url = format!("{}{sep}{q}", p.url);
    let resp = http_get(s, &url, extra_headers, max_response).await?;
    // upstream on_response(): anything but 200 fails with
    // http_category(status_code) + the reason phrase.
    if resp.status() != 200 {
        return Err(HttpError::Status(resp.status()).into());
    }
    let mut r = parse_response(&resp.body(), false, None, p.i2p).map_err(|e| match e {
        HttpTrackerError::Failure(m) => AnnounceError::Failure(m),
        other => AnnounceError::HttpTracker(other),
    })?;
    r.interval = r.interval.max(opts.min_announce_interval);
    Ok(r)
}

/// `update_transaction_id` — upstream draws `random(0xfffffffe) + 1`
/// (never 0, never 0xffffffff).
fn new_tid() -> u32 {
    rand::random::<u32>() % 0xffff_fffe + 1
}

/// `udp_tracker_connection` — the BEP-15 exchange: connect then
/// announce, one datagram each (upstream sends once — a receive
/// timeout fails the announce; `m_attempts` is just a counter).
/// Stray packets failing the tid/length checks are ignored and
/// rearm the `timeout` receive deadline like upstream's
/// `restart_read_timeout`. `opts` carries
/// `anonymous_mode`/`announce_ip`/`min_announce_interval` like the
/// session settings upstream reads.
pub async fn announce_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: SocketAddr,
    p: &AnnounceParams,
    opts: &crate::http_tracker::AnnounceOptions,
    timeout: Duration,
) -> Result<TrackerResponse, AnnounceError> {
    // connect exchange
    let conn_id = {
        let tid = new_tid();
        let mut w = udp_waiter(demux, tid);
        let _ = dgram.send_to(&build_connect(tid), tracker).await?;
        loop {
            let buf = wait_reply(&mut w, timeout).await?;
            match parse_connect_response(&buf, tid) {
                Ok(id) => break id,
                Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
                Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    };

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
    let _ = dgram
        .send_to(&build_announce(conn_id, tid, &req), tracker)
        .await?;
    loop {
        let buf = wait_reply(&mut w, timeout).await?;
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
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// `scrape_udp` — connect then a single scrape datagram (BEP-15
/// action 2); one send each, like `announce_udp` upstream.
pub async fn scrape_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: SocketAddr,
    info_hash: &[u8; 20],
    timeout: Duration,
) -> Result<crate::udp_tracker::ScrapeResponse, AnnounceError> {
    use crate::udp_tracker::{
        build_connect, build_scrape, parse_connect_response, parse_scrape_response,
    };
    let conn_id = {
        let tid = new_tid();
        let mut w = udp_waiter(demux, tid);
        let _ = dgram.send_to(&build_connect(tid), tracker).await?;
        loop {
            let buf = wait_reply(&mut w, timeout).await?;
            match parse_connect_response(&buf, tid) {
                Ok(id) => break id,
                Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
                Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    };
    let tid = new_tid();
    let pkt = build_scrape(conn_id, tid, &[*info_hash]);
    let mut w = udp_waiter(demux, tid);
    let _ = dgram.send_to(&pkt, tracker).await?;
    loop {
        let buf = wait_reply(&mut w, timeout).await?;
        match parse_scrape_response(&buf, tid) {
            Ok(v) => {
                return v
                    .into_iter()
                    .next()
                    .ok_or(AnnounceError::Udp(UdpTrackerError::BadLength));
            }
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply | UdpTrackerError::Truncated) => continue,
            Err(e) => return Err(e.into()),
        }
    }
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
) -> Result<TrackerResponse, AnnounceError> {
    let q = crate::http_tracker::scrape_query(info_hash);
    let sep = if url.contains('?') { "&" } else { "?" };
    let url = format!("{url}{sep}{q}");
    let resp = http_get(s, &url, extra_headers, max_response).await?;
    if resp.status() != 200 {
        return Err(HttpError::Status(resp.status()).into());
    }
    parse_response(&resp.body(), true, Some(info_hash), false).map_err(|e| match e {
        HttpTrackerError::Failure(m) => AnnounceError::Failure(m),
        other => AnnounceError::HttpTracker(other),
    })
}
