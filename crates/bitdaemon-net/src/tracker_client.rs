// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

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
/// direct qui se faisait voler les paquets par la pompe UDP).
struct UdpWaiter {
    tid: u32,
    rx: mpsc::UnboundedReceiver<UdpReply>,
}

/// Enregistre l'attente **avant** l'envoi du datagramme — sinon une
/// reponse tres rapide arrive avant le `register` et est perdue.
fn udp_waiter(demux: &UdpTrackerDemux, tid: u32) -> UdpWaiter {
    UdpWaiter {
        tid,
        rx: demux.register(tid),
    }
}

async fn wait_reply(
    demux: &UdpTrackerDemux,
    waiter: UdpWaiter,
    timeout: Duration,
) -> Result<Vec<u8>, UdpTrackerError> {
    let mut waiter = waiter;
    let res = tokio::time::timeout(timeout, waiter.rx.recv()).await;
    drop(waiter.rx);
    demux.unregister(waiter.tid);
    res.map_err(|_| UdpTrackerError::Timeout)?
        .map(|(_from, buf)| buf)
        .ok_or(UdpTrackerError::Timeout)
}

/// announce parameters shared by both protocols.
#[derive(Debug, Clone)]
pub struct AnnounceParams {
    /// v1 info hash.
    pub info_hash: [u8; 20],
    /// our peer id.
    pub peer_id: [u8; 20],
    /// listen port announced.
    pub port: u16,
    /// uploaded bytes.
    pub uploaded: i64,
    /// downloaded bytes.
    pub downloaded: i64,
    /// bytes left.
    pub left: i64,
    /// `event` — 0 none, 1 completed, 2 started, 3 stopped.
    pub event: i32,
    /// `numwant` (-1 = omit).
    pub num_want: i32,
    /// `key` — our random announce key.
    pub key: i32,
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
}

/// `http_tracker_connection` — announce over an established stream
/// (proxy/tunnel handshake already done by the caller). `path` is
/// the URL path+query minus our params (announce params appended).
pub async fn announce_http(
    s: &mut BoxedPeerStream,
    host: &str,
    base_path: &str,
    p: &AnnounceParams,
    tracker_id: Option<&[u8]>,
    extra_headers: &[(String, String)],
    max_response: usize,
) -> Result<TrackerResponse, AnnounceError> {
    let q = announce_query(
        &p.info_hash,
        &p.peer_id,
        p.port,
        p.uploaded,
        p.downloaded,
        p.left,
        p.event,
        p.num_want,
        p.key,
        true,
        false,
        tracker_id,
    );
    let sep = if base_path.contains('?') { "&" } else { "?" };
    let path = format!("{base_path}{sep}{q}");
    let resp = http_get(s, &path, host, extra_headers, max_response).await?;
    let r = parse_response(&resp.body(), false, None).map_err(|e| match e {
        HttpTrackerError::Failure(m) => AnnounceError::Failure(m),
        other => AnnounceError::HttpTracker(other),
    })?;
    Ok(r)
}

/// `udp_tracker_connection` — the full BEP-15 exchange: connect
/// (fresh transaction ids each attempt), retry `max_tries` times
/// with `timeout` per datagram round-trip.
pub async fn announce_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: SocketAddr,
    p: &AnnounceParams,
    timeout: Duration,
    max_tries: u32,
) -> Result<TrackerResponse, AnnounceError> {
    // connect exchange (one retry inside upstream too)
    let mut conn_id = None;
    for _ in 0..max_tries.max(1) {
        let tid = rand::random::<u32>();
        let w = udp_waiter(demux, tid);
        let _ = dgram.send_to(&build_connect(tid), tracker).await?;
        let buf = wait_reply(demux, w, timeout).await?;
        match parse_connect_response(&buf, tid) {
            Ok(id) => {
                conn_id = Some(id);
                break;
            }
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply) => continue, // stale packet
            Err(e) => return Err(e.into()),
        }
    }
    let conn_id = conn_id.ok_or(UdpTrackerError::Timeout)?;

    // announce exchange
    for _ in 0..max_tries.max(1) {
        let tid = rand::random::<u32>();
        let req = AnnounceRequest {
            info_hash: p.info_hash,
            peer_id: p.peer_id,
            downloaded: p.downloaded,
            left: p.left,
            uploaded: p.uploaded,
            event: p.event,
            announce_ip: std::net::Ipv4Addr::UNSPECIFIED,
            key: p.key,
            num_want: p.num_want,
            listen_port: p.port,
            request_string: String::new(),
        };
        let w = udp_waiter(demux, tid);
        let _ = dgram
            .send_to(&build_announce(conn_id, tid, &req), tracker)
            .await?;
        let buf = wait_reply(demux, w, timeout).await?;
        match parse_announce_response(&buf, 1, tid, tracker.is_ipv6()) {
            Ok(r) => {
                return Ok(TrackerResponse {
                    interval: r.interval,
                    complete: i64::from(r.complete),
                    incomplete: i64::from(r.incomplete),
                    tracker_id: r.tracker_id,
                    peers: r.peers,
                    ..Default::default()
                });
            }
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(UdpTrackerError::Timeout.into())
}

/// `scrape_udp` — connect then a single scrape datagram (BEP-15
/// action 2); `max_tries` round-trip retries like `announce_udp`.
pub async fn scrape_udp(
    dgram: &dyn DatagramSocket,
    demux: &UdpTrackerDemux,
    tracker: SocketAddr,
    info_hash: &[u8; 20],
    timeout: Duration,
    max_tries: u32,
) -> Result<crate::udp_tracker::ScrapeResponse, AnnounceError> {
    use crate::udp_tracker::{
        build_connect, build_scrape, parse_connect_response, parse_scrape_response,
    };
    let mut conn_id = None;
    for _ in 0..max_tries.max(1) {
        let tid = rand::random::<u32>();
        let w = udp_waiter(demux, tid);
        let _ = dgram.send_to(&build_connect(tid), tracker).await?;
        let buf = wait_reply(demux, w, timeout).await?;
        match parse_connect_response(&buf, tid) {
            Ok(id) => {
                conn_id = Some(id);
                break;
            }
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    let conn_id = conn_id.ok_or(UdpTrackerError::Timeout)?;
    for _ in 0..max_tries.max(1) {
        let tid = rand::random::<u32>();
        let pkt = build_scrape(conn_id, tid, &[*info_hash]);
        let w = udp_waiter(demux, tid);
        let _ = dgram.send_to(&pkt, tracker).await?;
        let buf = wait_reply(demux, w, timeout).await?;
        match parse_scrape_response(&buf, tid) {
            Ok(mut v) => {
                return v
                    .pop()
                    .ok_or(AnnounceError::Udp(UdpTrackerError::BadLength));
            }
            Err(UdpTrackerError::Tracker(m)) => return Err(AnnounceError::Failure(m)),
            Err(UdpTrackerError::UnexpectedReply) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(UdpTrackerError::Timeout.into())
}

/// `scrape_http` — GET the scrape url with `info_hash`, parse the
/// `files`/`info_hash` dict (upstream maps it onto the same
/// `tracker_response` fields).
pub async fn scrape_http(
    s: &mut BoxedPeerStream,
    host: &str,
    base_path: &str,
    info_hash: &[u8; 20],
    extra_headers: &[(String, String)],
    max_response: usize,
) -> Result<TrackerResponse, AnnounceError> {
    let q = crate::http_tracker::scrape_query(info_hash);
    let sep = if base_path.contains('?') { "&" } else { "?" };
    let path = format!("{base_path}{sep}{q}");
    let resp = http_get(s, &path, host, extra_headers, max_response).await?;
    parse_response(&resp.body(), true, Some(info_hash)).map_err(|e| match e {
        HttpTrackerError::Failure(m) => AnnounceError::Failure(m),
        other => AnnounceError::HttpTracker(other),
    })
}
