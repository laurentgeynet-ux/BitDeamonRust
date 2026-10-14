// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `websocket_tracker_connection` — WebTorrent-style JSON announce
//! over a WebSocket stream (`websocket.rs` handshake + frames).
//!
//! Unlike HTTP/UDP trackers, the ws protocol is WebRTC signaling:
//! an announce carries `offers` and the response carries `offer` /
//! `answer` — **peer lists are not delivered** (upstream never
//! parses a `peers` field). Strings are latin-1→UTF-8 encoded so a
//! raw 20-byte info_hash / peer_id survives JSON verbatim.

use serde_json::{Value, json};

use crate::tracker_client::AnnounceParams;
use crate::transport::BoxedPeerStream;
use crate::websocket::{WsError, WsOpcode, ws_read_message, ws_upgrade, ws_write_frame};

/// `aux::RTC_MAX_SDP_SIZE` upstream.
pub const RTC_MAX_SDP: usize = 64 * 1024;

/// `rtc_offer` — an inbound WebRTC offer (opaque to us; routed to
/// whoever owns the RTC stack).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcOffer {
    /// `offer_id` (20 bytes, `RTC_OFFER_ID_LEN`).
    pub offer_id: Vec<u8>,
    /// offerer's peer_id (20 bytes).
    pub peer_id: Vec<u8>,
    /// SDP blob.
    pub sdp: String,
}

/// `rtc_answer` — an answer to one of our offers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcAnswer {
    /// `offer_id` it answers.
    pub offer_id: Vec<u8>,
    /// answerer's peer_id.
    pub peer_id: Vec<u8>,
    /// SDP blob.
    pub sdp: String,
}

/// `websocket_tracker_response` — a parsed tracker frame.
#[derive(Debug, Clone, Default)]
pub struct WsTrackerResponse {
    /// `info_hash` (20 bytes, latin-1 decoded).
    pub info_hash: [u8; 20],
    /// `interval` (s).
    pub interval: i64,
    /// `min interval` (s).
    pub min_interval: i64,
    /// `complete` (-1 absent).
    pub complete: i64,
    /// `incomplete` (-1 absent).
    pub incomplete: i64,
    /// `downloaded` (-1 absent).
    pub downloaded: i64,
    /// an offer addressed to us.
    pub offer: Option<RtcOffer>,
    /// an answer to one of our offers.
    pub answer: Option<RtcAnswer>,
    /// `failure reason` — a tracker-side error string; present even
    /// when no `info_hash` allows correlating the response.
    pub failure_reason: Option<String>,
}

/// latin-1 byte string → UTF-8 (`aux::latin1_utf8`).
#[must_use]
pub fn latin1_utf8(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| char::from(b)).collect()
}

/// UTF-8 → latin-1 bytes (`aux::utf8_latin1`).
#[must_use]
pub fn utf8_latin1(s: &str) -> Vec<u8> {
    s.chars().map(|c| c as u32 as u8).collect()
}

/// `do_send` — the announce payload, verbatim field names. `event`
/// matches upstream's `event_t` table (`1..=4` → completed, started,
/// stopped, paused); `corrupt` comes from `AnnounceParams` like
/// upstream `tracker_req().corrupt`.
#[must_use]
pub fn build_announce(p: &AnnounceParams, offers: &[(Vec<u8>, String)]) -> String {
    const EVENT_STRING: [&str; 4] = ["completed", "started", "stopped", "paused"];
    let mut payload = json!({
        "action": "announce",
        "info_hash": latin1_utf8(&p.info_hash),
        "uploaded": p.uploaded,
        "downloaded": p.downloaded,
        "left": p.left,
        "corrupt": p.corrupt,
        "numwant": p.num_want,
        "key": format!("{:08X}", p.key),
        "peer_id": latin1_utf8(&p.peer_id),
    });
    if p.event != 0 && (1..=4).contains(&p.event) {
        payload["event"] = json!(EVENT_STRING[(p.event - 1) as usize]);
    }
    let mut arr = Vec::new();
    for (id, sdp) in offers {
        arr.push(json!({
            "offer_id": latin1_utf8(id),
            "offer": { "type": "offer", "sdp": sdp },
        }));
    }
    if !arr.is_empty() {
        payload["offers"] = json!(arr);
    }
    serde_json::to_string(&payload).unwrap_or_default()
}

/// `queue_answer` — the answer frame sent back for an offer.
#[must_use]
pub fn build_answer(
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    offer_id: &[u8],
    to_peer_id: &[u8],
    sdp: &str,
) -> String {
    serde_json::to_string(&json!({
        "action": "announce",
        "info_hash": latin1_utf8(info_hash),
        "offer_id": latin1_utf8(offer_id),
        "to_peer_id": latin1_utf8(to_peer_id),
        "peer_id": latin1_utf8(peer_id),
        "answer": { "type": "answer", "sdp": sdp },
    }))
    .unwrap_or_default()
}

/// `parse_websocket_tracker_response` — verbatim field mapping.
/// Errors are `WsError::Protocol`-shaped strings (upstream reports
/// `invalid_tracker_response` + a human message).
pub fn parse_response(data: &[u8]) -> Result<WsTrackerResponse, String> {
    let v: Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
    let obj = v.as_object().ok_or("not a json object")?;
    let failure = obj
        .get("failure reason")
        .and_then(Value::as_str)
        .map(String::from);
    let Some(ih) = obj.get("info_hash").and_then(Value::as_str) else {
        // no info_hash: a `failure reason` is still a valid response
        // upstream (uncorrelated tracker error)
        if failure.is_some() {
            return Ok(WsTrackerResponse {
                failure_reason: failure,
                ..Default::default()
            });
        }
        return Err("no info hash in message".into());
    };
    let raw = utf8_latin1(ih);
    if raw.len() != 20 {
        return Err(format!("invalid info hash size {}", raw.len()));
    }
    let mut r = WsTrackerResponse {
        info_hash: raw.try_into().unwrap_or([0; 20]),
        failure_reason: failure,
        ..Default::default()
    };

    let parse_rtc = |obj: &serde_json::Map<String, Value>,
                     key: &str|
     -> Result<(Vec<u8>, Vec<u8>, String), String> {
        let sdp = obj
            .get(key)
            .and_then(|o| o.get("sdp"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if sdp.len() > RTC_MAX_SDP {
            return Err(format!("{key} SDP too large"));
        }
        let id = utf8_latin1(
            obj.get("offer_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        if id.len() != 20 {
            return Err(format!("invalid offer_id size {}", id.len()));
        }
        let pid = utf8_latin1(
            obj.get("peer_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        if pid.len() != 20 {
            return Err(format!("invalid peer_id size {}", pid.len()));
        }
        Ok((id, pid, sdp.to_string()))
    };

    if obj.contains_key("offer") {
        let (id, pid, sdp) = parse_rtc(obj, "offer")?;
        r.offer = Some(RtcOffer {
            offer_id: id,
            peer_id: pid,
            sdp,
        });
    }
    if obj.contains_key("answer") {
        let (id, pid, sdp) = parse_rtc(obj, "answer")?;
        r.answer = Some(RtcAnswer {
            offer_id: id,
            peer_id: pid,
            sdp,
        });
    }

    let get_i = |k: &str, d: i64| obj.get(k).and_then(Value::as_i64).unwrap_or(d);
    if obj.contains_key("interval") {
        r.interval = get_i("interval", 120);
        r.min_interval = get_i("min_interval", 60);
        r.complete = get_i("complete", -1);
        r.incomplete = get_i("incomplete", -1);
        r.downloaded = get_i("downloaded", -1);
    }
    Ok(r)
}

/// `start()`+`do_send()`+`on_read()` — ws handshake on an already
/// connected stream, one announce frame, one response frame.
/// `host`/`path`/`key` drive the HTTP upgrade (`ws_upgrade`).
pub async fn announce_ws(
    s: &mut BoxedPeerStream,
    host: &str,
    path: &str,
    ws_key: &str,
    payload: &str,
) -> Result<WsTrackerResponse, WsError> {
    ws_upgrade(s, host, path, "", ws_key).await?;
    // consume the `101` reply (headers up to CRLF-CRLF) and
    // validate `Sec-WebSocket-Accept`
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = tokio::io::AsyncReadExt::read(s, &mut tmp).await?;
        if n == 0 {
            return Err(WsError::Handshake("eof during upgrade"));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 16 * 1024 {
            return Err(WsError::Handshake("upgrade headers too large"));
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<i32>().ok())
        .unwrap_or(0);
    let accept = head.lines().find_map(|l| {
        l.to_ascii_lowercase()
            .starts_with("sec-websocket-accept:")
            .then(|| l.split(':').nth(1).unwrap_or_default().trim().to_string())
    });
    crate::websocket::validate_handshake(status, accept.as_deref(), ws_key)?;
    // beast's websocket stream sends binary frames by default upstream
    ws_write_frame(
        s,
        WsOpcode::Binary,
        payload.as_bytes(),
        rand::random::<u32>(),
    )
    .await?;
    let (_op, data) = ws_read_message(s).await?;
    parse_response(&data).map_err(WsError::Protocol)
}
