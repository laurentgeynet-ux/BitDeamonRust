// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `tracker_connection` — one full announce exchange per call:
//! HTTP via [`http_get`] + `parse_response`, UDP via the BEP-15
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
    tracker: SocketAddr,
    p: &AnnounceParams,
    timeout: Duration,
    max_tries: u32,
) -> Result<TrackerResponse, AnnounceError> {
    let mut buf = vec![0u8; 2048];

    // connect exchange (one retry inside upstream too)
    let mut conn_id = None;
    for _ in 0..max_tries.max(1) {
        let tid = rand::random::<u32>();
        let _ = dgram.send_to(&build_connect(tid), tracker).await?;
        let (n, _from) = tokio::time::timeout(timeout, dgram.recv_from(&mut buf))
            .await
            .map_err(|_| UdpTrackerError::Timeout)??;
        match parse_connect_response(&buf[..n], tid) {
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
        let _ = dgram
            .send_to(&build_announce(conn_id, tid, &req), tracker)
            .await?;
        let (n, _from) = tokio::time::timeout(timeout, dgram.recv_from(&mut buf))
            .await
            .map_err(|_| UdpTrackerError::Timeout)??;
        match parse_announce_response(&buf[..n], 1, tid, tracker.is_ipv6()) {
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
