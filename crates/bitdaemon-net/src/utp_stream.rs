// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `utp_stream.cpp`'s stream-facing half (`utp_socket_impl`'s
//! state machine is `net::utp_socket`).
//!
//! `utp_stream` bridge — adapt the poll-style `UtpSocket` to
//! `AsyncRead + AsyncWrite` (`PeerStream`) plus a
//! `PeerConnector` dialing over any injected `DatagramSocket`.
//!
//! Architecture: a `UtpManager` demuxes inbound datagrams by
//! `(remote, conn-id)` into per-socket channels; each socket gets a
//! pump task owning the `UtpSocket` — inbound datagrams and local
//! writes go in, payload bytes come out toward `poll_read`, and the
//! pump drains the socket's `OutDatagram` queue back onto the
//! datagram transport. Everything stays transport-agnostic, so
//! OnionBit circuits carry uTP without real UDP sockets.

use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::BufMut as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::transport::{BoxedPeerStream, DatagramSendFlags, DatagramSocket, PeerConnector};
use crate::utp::{UTP_HEADER_LEN, UtpHeader, UtpType};
use crate::utp_socket::{UtpConfig, UtpError, UtpSocket, UtpState};

/// Message routed to a socket pump (and to the listener queue) —
/// either an inbound datagram or a transport tear-down
/// (`remove_udp_socket` upstream: every socket bound to the dead
/// transport is aborted). `Abort` never reaches the listener queue.
#[derive(Debug)]
pub enum InboundMsg {
    /// One routed datagram + its sender endpoint.
    Datagram(Vec<u8>, SocketAddr),
    /// The datagram transport this socket is bound to went away.
    Abort,
    /// Hot-applied settings — upstream sockets read the shared
    /// `session_settings` live; here the manager pushes the new
    /// snapshot (`UtpSocket::update_cfg`).
    SetConfig(UtpConfig),
}

/// Write-side commands from the `UtpStream` — payload bytes, a
/// graceful close (`poll_shutdown` / drop → FIN), or a close reason
/// (`utp_close_reason` extension on the FIN).
#[derive(Debug)]
enum WriterMsg {
    Data(Vec<u8>),
    Close,
    CloseReason(u16),
}

/// One registered socket: its inbound channel plus the datagram
/// transport the pump replies on (kept for `remove_udp_socket`).
#[derive(Debug)]
struct SocketEntry {
    tx: mpsc::Sender<InboundMsg>,
    dg: Arc<dyn DatagramSocket>,
}

/// `UtpManager` — the `utp_socket_manager` analogue: demux table +
/// the socket pump lifecycle. Cheap to clone.
#[derive(Debug)]
pub struct UtpManager {
    inner: Arc<Mutex<ManagerInner>>,
    /// `remote` → live socket's close-reason slot. The session
    /// stamps `close_reason_t` here at `disconnect()` (upstream
    /// `set_close_reason(m_socket, …)`) — sync storage so a conn
    /// teardown can stamp it without awaiting. The socket pump
    /// applies it to `m_close_reason`, so the next FIN/data packet
    /// carries the extension header.
    close_slots: Arc<std::sync::Mutex<HashMap<SocketAddr, Arc<std::sync::atomic::AtomicU16>>>>,
    /// the live `UtpConfig` — upstream's `utp_socket_manager` reads
    /// `session_settings` through `m_sm` getters on every call, so
    /// settings apply to existing sockets instantly; the pumps pull
    /// the snapshot via `InboundMsg::SetConfig`.
    cfg: std::sync::RwLock<UtpConfig>,
    /// `settings_pack::connections_limit` — SYN-flood guard, atomics
    /// so the session's sync `apply_settings` path can hot-apply it.
    conn_limit: std::sync::atomic::AtomicUsize,
    /// `enable_incoming_utp` — upstream drops unmatched SYNs before
    /// creating a socket when the flag is off.
    accept_incoming: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct ManagerInner {
    /// `(remote, conn_id)` → socket's inbound channel + transport.
    sockets: HashMap<(SocketAddr, u16), SocketEntry>,
    /// unmatched SYNs go here (the acceptor's queue) — upstream's
    /// `utp_socket_manager` hands unknown SYNs to the listen socket.
    listener: Option<mpsc::Sender<InboundMsg>>,
    /// tasks we spawned (kept alive; aborted with the manager).
    tasks: Vec<JoinHandle<()>>,
    /// `m_restrict_mtu`/`m_mtu_idx` — rolling window of failed
    /// path-MTU probes; the cap is the max of the last 3.
    restrict_mtu: [i32; 3],
    mtu_idx: usize,
}

impl Default for ManagerInner {
    fn default() -> Self {
        Self {
            sockets: HashMap::new(),
            listener: None,
            tasks: Vec::new(),
            restrict_mtu: [65536; 3],
            mtu_idx: 0,
        }
    }
}

impl ManagerInner {
    /// `restrict_mtu()` — `max` of the rolling window.
    fn mtu_cap(&self) -> i32 {
        self.restrict_mtu.iter().copied().max().unwrap_or(65536)
    }
}

impl UtpManager {
    /// new manager — call [`UtpManager::route`] on every datagram
    /// the transport receives (or run [`UtpManager::pump_in`]).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ManagerInner::default())),
            close_slots: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cfg: std::sync::RwLock::new(UtpConfig::default()),
            // upstream `settings_pack::connections_limit` default
            conn_limit: std::sync::atomic::AtomicUsize::new(200),
            accept_incoming: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// the manager's live socket configuration — `connect`/
    /// `accept_syn` callers pass `manager.config()` so new sockets
    /// see hot-applied settings, like upstream's `m_sm` getters.
    #[must_use]
    pub fn config(&self) -> UtpConfig {
        self.cfg.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Hot-apply `cfg`: stored for future `connect`/`accept_syn`
    /// *and* broadcast to every live socket pump
    /// (`InboundMsg::SetConfig` → `UtpSocket::update_cfg`) — upstream
    /// sockets observe `session_settings` mutations immediately.
    pub fn set_config(&self, cfg: UtpConfig) {
        *self.cfg.write().unwrap_or_else(|e| e.into_inner()) = cfg.clone();
        if let Ok(inner) = self.inner.try_lock() {
            for e in inner.sockets.values() {
                let _ = e.tx.try_send(InboundMsg::SetConfig(cfg.clone()));
            }
        }
    }

    /// `enable_incoming_utp` — unmatched SYNs are dropped (not
    /// counted) when off, matching upstream's check before the
    /// listen-socket branch.
    pub fn set_accept_incoming(&self, on: bool) {
        self.accept_incoming
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// `utp_socket_manager::incoming_packet` — demux to the right
    /// socket by `(remote, conn_id)`. The header gates mirror
    /// upstream (`< 20` or `version != 1` are dropped here, before
    /// any socket sees them — the *type* nibble is deliberately not
    /// filtered, upstream counts unknown types at socket level).
    /// Dispatch is `try_send` outside the lock: datagrams are lossy
    /// by nature and a full socket queue must not stall the shared
    /// receive pump. Returns false when nothing consumed the packet
    /// (no matching socket and not a SYN the listener takes —
    /// upstream falls through to the other UDP consumers).
    pub async fn route(&self, from: SocketAddr, buf: &[u8]) -> bool {
        if buf.len() < UTP_HEADER_LEN || buf[0] & 0xf != 1 {
            return false;
        }
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let (tx, listener) = {
            let inner = self.inner.lock().await;
            // upstream `equal_range(id)` + `match(ep, id)`: the
            // incoming conn-id is always the *receiver's* recv_id, so
            // an exact `(ep, id)` hit is the upstream shape
            let tx = inner.sockets.get(&(from, id)).map(|e| e.tx.clone());
            // unmatched ST_SYN (type nibble 4) → the listen queue —
            // only when `enable_incoming_utp` is on and we're under
            // the upstream SYN-flood cap (`connections_limit * 2`)
            let listener = if buf[0] >> 4 == 4
                && self
                    .accept_incoming
                    .load(std::sync::atomic::Ordering::Relaxed)
                && inner.sockets.len()
                    <= self
                        .conn_limit
                        .load(std::sync::atomic::Ordering::Relaxed)
                        .saturating_mul(2)
            {
                inner.listener.clone()
            } else {
                None
            };
            (tx, listener)
        };
        let msg = InboundMsg::Datagram(buf.to_vec(), from);
        if let Some(tx) = tx {
            if tx.try_send(msg).is_err() {
                // socket queue full — the datagram is lost; upstream
                // processes inline so there is no equivalent counter
                tracing::trace!("utp demux: dropping datagram for {from} (socket queue full)");
            }
            true
        } else if let Some(tx) = listener {
            if tx.try_send(msg).is_err() {
                tracing::trace!("utp demux: dropping SYN from {from} (listener queue full)");
            }
            true
        } else {
            false
        }
    }

    /// `settings_pack::connections_limit` for the SYN-flood guard.
    pub fn set_connection_limit(&self, limit: usize) {
        self.conn_limit
            .store(limit, std::sync::atomic::Ordering::Relaxed);
    }

    /// `utp_socket_manager::restrict_mtu` — record a failed MTU.
    pub async fn restrict_mtu(&self, mtu: i32) {
        let mut inner = self.inner.lock().await;
        let idx = inner.mtu_idx;
        inner.restrict_mtu[idx] = mtu;
        inner.mtu_idx = (idx + 1) % inner.restrict_mtu.len();
    }

    /// `mtu_for_dest` — path-MTU guess for `remote` clamped to the
    /// rolling restrict cap.
    async fn mtu_for(&self, remote: SocketAddr, socks5: bool) -> i32 {
        let cap = self.inner.lock().await.mtu_cap();
        UtpSocket::mtu_for_dest(&remote.ip(), socks5, cap)
    }

    /// register the SYN listener — returns the acceptor's queue
    /// (one listener per manager; replacing drops the old queue).
    pub async fn set_listener(&self) -> mpsc::Receiver<InboundMsg> {
        let (tx, rx) = mpsc::channel(64);
        self.inner.lock().await.listener = Some(tx);
        rx
    }

    /// accept one inbound SYN — build the responder socket
    /// (`send_id = SYN conn_id`, `recv_id = +1` upstream), register
    /// its pump, feed the SYN in, and return the stream. The SYN
    /// must parse as version-1 `ST_SYN` and the socket impl must
    /// accept it (`incoming_packet` return) — otherwise no socket
    /// is registered at all, matching upstream where the rejected
    /// packet simply isn't uTP.
    pub async fn accept_syn(
        self: &Arc<Self>,
        dg: Arc<dyn DatagramSocket>,
        from: SocketAddr,
        syn: &[u8],
        cfg: UtpConfig,
    ) -> io::Result<UtpStream> {
        let h = UtpHeader::parse(syn)
            .filter(|h| h.ty == UtpType::Syn)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not a syn"))?;
        let id = h.connection_id;
        // upstream `utp_socket_manager::incoming_packet`: responder
        // send_id = the SYN's connection id, recv_id = send_id + 1
        let mut sock = UtpSocket::new(id.wrapping_add(1), id, cfg.clone());
        sock.init_mtu(self.mtu_for(from, cfg.proxy_socks5).await as u16);
        // Feed the SYN *synchronously* — upstream constructs the
        // socket impl already-Connected (the SYN is what creates it).
        // Routing it through the pump's inbound channel let
        // `tokio::select!` deliver a pending `write()` first, which
        // emitted data packets toward the unset remote (0.0.0.0:0)
        // and poisoned outbuf/cwnd before the handshake — a rare but
        // permanent stall.
        if !sock.incoming_packet(syn, from, Instant::now()) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "syn rejected"));
        }
        sock.send_deferred_ack();
        let recv_id = sock.recv_id();
        let rx = self.register((from, recv_id), dg.clone()).await;
        let (rd_tx, rd_rx) = mpsc::channel::<Vec<u8>>(64);
        let (wr_tx, wr_rx) = mpsc::unbounded_channel::<WriterMsg>();
        // state notifications ride a `watch` — latest-value
        // semantics, never blocks the pump when the consumer isn't
        // listening (upstream fires callbacks fire-and-forget). A
        // bounded mpsc here deadlocked the pump on the first
        // post-Connected transition nobody read.
        let (st_tx, st_rx) = tokio::sync::watch::channel(UtpState::None);
        let last_error = Arc::new(std::sync::Mutex::new(None));
        // `close_reason` extension — the slot shared with the pump;
        // `UtpManager::set_close_reason` reaches it by remote.
        let close_slot = Arc::new(std::sync::atomic::AtomicU16::new(0));
        self.close_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(from, close_slot.clone());
        let their_close = Arc::new(std::sync::atomic::AtomicU16::new(0));

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            Some(rd_tx),
            st_tx,
            last_error.clone(),
            close_slot.clone(),
            their_close.clone(),
            (from, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);
        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            last_error,
            their_close_reason: their_close,
            read_buf: std::collections::VecDeque::new(),
            closed: false,
        })
    }

    /// `pump_in` — convenience loop routing every datagram from
    /// `dg` (spawn it once per datagram transport).
    pub fn pump_in(self: &Arc<Self>, dg: Arc<dyn DatagramSocket>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((n, from)) = dg.recv_from(&mut buf).await {
                me.route(from, &buf[..n]).await;
            }
        })
    }

    /// register a socket pump keyed by `(remote, recv_id)`; `dg` is
    /// retained so [`UtpManager::remove_datagram_socket`] can abort
    /// everything bound to a dead transport.
    async fn register(
        &self,
        key: (SocketAddr, u16),
        dg: Arc<dyn DatagramSocket>,
    ) -> mpsc::Receiver<InboundMsg> {
        let (tx, rx) = mpsc::channel(64);
        self.inner
            .lock()
            .await
            .sockets
            .insert(key, SocketEntry { tx, dg });
        rx
    }

    async fn unregister(&self, key: (SocketAddr, u16)) {
        self.inner.lock().await.sockets.remove(&key);
        self.close_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key.0);
    }

    /// `set_close_reason(m_socket, r)` — the session's disconnect
    /// cause stamped on the socket belonging to `remote`; the pump
    /// stamps it on the wire (uTP `close_reason` extension header).
    /// No-op when the remote has no live uTP socket here.
    pub fn set_close_reason(&self, remote: SocketAddr, reason: u16) {
        let map = self.close_slots.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = map.get(&remote) {
            slot.store(reason, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// `remove_udp_socket` — abort every uTP socket bound to `dg`.
    /// Upstream iterates `m_utp_sockets` calling `s->abort()` when a
    /// listen transport disappears (proxy down, tunnel torn down).
    pub async fn remove_datagram_socket(&self, dg: &Arc<dyn DatagramSocket>) {
        let senders: Vec<mpsc::Sender<InboundMsg>> = {
            let inner = self.inner.lock().await;
            inner
                .sockets
                .values()
                .filter(|e| Arc::ptr_eq(&e.dg, dg))
                .map(|e| e.tx.clone())
                .collect()
        };
        for tx in senders {
            let _ = tx.send(InboundMsg::Abort).await;
        }
    }

    /// dial `remote` — create the socket + pump, return the stream
    /// once the SYN/STATE handshake completes (`poll_connectable`).
    pub async fn connect(
        self: &Arc<Self>,
        dg: Arc<dyn DatagramSocket>,
        remote: SocketAddr,
        cfg: UtpConfig,
    ) -> io::Result<UtpStream> {
        // upstream `new_utp_socket`: send_id = random,
        // recv_id = send_id - 1 (the SYN carries recv_id)
        let send_id = rand::random::<u16>();
        let mut sock = UtpSocket::new(send_id.wrapping_sub(1), send_id, cfg.clone());
        sock.init_mtu(self.mtu_for(remote, cfg.proxy_socks5).await as u16);
        sock.connect(remote);
        let recv_id = sock.recv_id();
        let rx = self.register((remote, recv_id), dg.clone()).await;
        let (rd_tx, rd_rx) = mpsc::channel::<Vec<u8>>(64);
        let (wr_tx, wr_rx) = mpsc::unbounded_channel::<WriterMsg>();
        let (st_tx, st_rx) = tokio::sync::watch::channel(UtpState::None);
        let last_error = Arc::new(std::sync::Mutex::new(None));
        let close_slot = Arc::new(std::sync::atomic::AtomicU16::new(0));
        self.close_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(remote, close_slot.clone());
        let their_close = Arc::new(std::sync::atomic::AtomicU16::new(0));

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            Some(rd_tx),
            st_tx,
            last_error.clone(),
            close_slot.clone(),
            their_close.clone(),
            (remote, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);

        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            last_error,
            their_close_reason: their_close,
            read_buf: std::collections::VecDeque::new(),
            closed: false,
        })
    }
}

impl Default for UtpManager {
    fn default() -> Self {
        Self::new()
    }
}

/// pump task — owns the `UtpSocket`; multiplexes inbound datagrams,
/// write requests, and the retransmit tick.
#[allow(clippy::too_many_arguments)]
async fn socket_pump(
    mut sock: UtpSocket,
    dg: Arc<dyn DatagramSocket>,
    mut inbound_rx: mpsc::Receiver<InboundMsg>,
    mut wr_rx: mpsc::UnboundedReceiver<WriterMsg>,
    rd_tx: Option<mpsc::Sender<Vec<u8>>>,
    st_tx: tokio::sync::watch::Sender<UtpState>,
    last_error: Arc<std::sync::Mutex<Option<UtpError>>>,
    close_slot: Arc<std::sync::atomic::AtomicU16>,
    their_close: Arc<std::sync::atomic::AtomicU16>,
    key: (SocketAddr, u16),
    mgr: Arc<UtpManager>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut readbuf = vec![0u8; 16 * 1024];
    let mut last_state = UtpState::None;
    let mut rd_tx = rd_tx;
    // `close_seen`: Close delivered or writer gone — the socket's
    // `m_out_eof` analogue for the *channel* side; `wr_dead` marks
    // the channel itself closed (recv() returns None forever — the
    // select arm must be disarmed or it would busy-loop).
    let mut close_seen = false;
    let mut wr_dead = false;
    loop {
        // `set_close_reason` — a stamped reason rides every
        // subsequent packet until close (upstream mirrors it on
        // `m_close_reason`).
        let cr = close_slot.load(std::sync::atomic::Ordering::Relaxed);
        if cr != 0 {
            sock.set_close_reason(cr);
        }
        // drain outgoing datagrams — `dont_fragment` rides
        // `udp_socket::send`'s flags (MTU probes only). Send errors
        // feed back into the socket (`utp_stream.cpp:1786-1831`):
        // EMSGSIZE clamps the MTU ceiling and retries without DF,
        // WouldBlock stalls, anything else kills the conn.
        while let Some(d) = sock.pop_outbox() {
            tracing::trace!(
                "pump {key:?} -> ty={} seq={} ack={} len={}",
                d.bytes[0] >> 4,
                u16::from_be_bytes([d.bytes[16], d.bytes[17]]),
                u16::from_be_bytes([d.bytes[18], d.bytes[19]]),
                d.bytes.len()
            );
            // uTP datagrams are `peer_connection` traffic — the
            // socks5 proxy gate consults `proxy_peer_connections`.
            let flags = if d.dont_fragment {
                DatagramSendFlags::DONT_FRAGMENT
            } else {
                DatagramSendFlags::NONE
            }
            .or(DatagramSendFlags::PEER_CONNECTION);
            match dg.send_to_flags(&d.bytes, d.to, flags).await {
                Ok(_) => {
                    if sock.stalled() {
                        sock.on_writable();
                    }
                }
                Err(e) => sock.note_send_error(d, &e),
            }
        }
        // push received payload toward the reader — only while the
        // read channel has capacity. A stalled reader must never
        // park the pump: ACKs, ticks and the rest of the receive
        // path keep running upstream-style.
        if let Some(tx) = rd_tx.as_ref().filter(|t| t.capacity() > 0) {
            match sock.read(&mut readbuf) {
                Ok(n) if n > 0 => {
                    if tx.send(readbuf[..n].to_vec()).await.is_err() {
                        // reader gone — upstream `destroy()`:
                        // `close()` keeps flushing so the FIN leaves,
                        // but unsent buffered data is dropped
                        drop(rd_tx.take());
                        sock.destroy();
                    }
                }
                Err(e) => {
                    *last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(e);
                    // a pending `poll_read` must observe EOF/error
                    // *now* — upstream delivers it to the read
                    // handler as soon as the FIN'd data drains
                    drop(rd_tx.take());
                }
                _ => {}
            }
        }
        if sock.state() != last_state {
            last_state = sock.state();
            let _ = st_tx.send(last_state);
        }
        // `get_close_reason` — publish the peer's reason as soon as
        // the FIN carries it (upstream `peer_connection` reads it at
        // disconnect, long before socket deletion)
        if sock.their_close_reason != 0 {
            their_close.store(
                sock.their_close_reason,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if matches!(last_state, UtpState::Deleting | UtpState::ErrorWait) {
            break;
        }
        tokio::select! {
            Some(msg) = inbound_rx.recv() => {
                match msg {
                    InboundMsg::Abort => sock.abort(),
                    InboundMsg::SetConfig(cfg) => sock.update_cfg(cfg),
                    InboundMsg::Datagram(buf, from) => {
                        tracing::trace!("pump {key:?} <- ty={} seq={} ack={} len={}", buf[0]>>4,
                            u16::from_be_bytes([buf[16],buf[17]]),
                            u16::from_be_bytes([buf[18],buf[19]]), buf.len());
                        sock.incoming_packet(&buf, from, Instant::now());
                        // batch whatever else is queued into this
                        // receive round — upstream flushes deferred
                        // acks once per *batch* (`m_deferred_ack_list`),
                        // not once per datagram
                        while let Ok(msg) = inbound_rx.try_recv() {
                            match msg {
                                InboundMsg::Abort => {
                                    sock.abort();
                                    break;
                                }
                                InboundMsg::SetConfig(cfg) => sock.update_cfg(cfg),
                                InboundMsg::Datagram(buf, from) => {
                                    sock.incoming_packet(&buf, from, Instant::now());
                                }
                            }
                        }
                        sock.send_deferred_ack();
                    }
                }
            }
            msg = wr_rx.recv(), if !wr_dead => {
                match msg {
                    Some(WriterMsg::Data(buf)) if !close_seen => {
                        let _ = sock.write(&buf);
                    }
                    // post-close payload — upstream `broken_pipe`;
                    // dropped, it must not be packetized past the FIN
                    Some(WriterMsg::Data(_)) => {}
                    Some(WriterMsg::CloseReason(code)) => sock.set_close_reason(code),
                    Some(WriterMsg::Close) => {
                        // poll_shutdown — upstream `close()`: FIN
                        // once the queue drains
                        close_seen = true;
                        sock.close();
                    }
                    None => {
                        // stream dropped — `destroy()` upstream:
                        // FIN out, unsent buffers dropped, reader
                        // side already handled by rd_tx
                        close_seen = true;
                        wr_dead = true;
                        drop(rd_tx.take());
                        sock.destroy();
                    }
                }
            }
            _ = tick.tick() => {
                sock.tick(Instant::now());
                if let Some(mtu) = sock.take_failed_mtu() {
                    mgr.restrict_mtu(i32::from(mtu)).await;
                }
            }
            else => break,
        }
    }
    if let Some(e) = sock.error() {
        *last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(e);
    }
    if sock.their_close_reason != 0 {
        tracing::debug!("pump {key:?} peer close_reason={}", sock.their_close_reason);
        their_close.store(
            sock.their_close_reason,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    mgr.unregister(key).await;
}

/// `UtpStream` — `PeerStream` impl over the pump channels.
#[derive(Debug)]
pub struct UtpStream {
    rd_rx: mpsc::Receiver<Vec<u8>>,
    wr_tx: mpsc::UnboundedSender<WriterMsg>,
    st_rx: Arc<Mutex<tokio::sync::watch::Receiver<UtpState>>>,
    /// last socket-level error — read failures surface this rather
    /// than collapsing everything to EOF (upstream keeps `m_error`).
    last_error: Arc<std::sync::Mutex<Option<UtpError>>>,
    /// the peer's `close_reason` extension value — stamped by the
    /// pump at teardown (upstream `get_close_reason` is read by
    /// `peer_connection` at disconnect). 0 = none received.
    their_close_reason: Arc<std::sync::atomic::AtomicU16>,
    read_buf: std::collections::VecDeque<u8>,
    /// `poll_shutdown` ran — writes past this point are
    /// `broken_pipe` (upstream rejects them in `write_some`).
    closed: bool,
}

/// socket error → `io::Error` (upstream propagates `m_error` to the
/// connect/read/write handlers unchanged).
fn utp_error_to_io(e: UtpError) -> io::Error {
    match e {
        UtpError::Eof => io::Error::new(io::ErrorKind::UnexpectedEof, "utp peer closed"),
        UtpError::ConnectionReset => {
            io::Error::new(io::ErrorKind::ConnectionReset, "utp connection reset")
        }
        UtpError::TimedOut => io::Error::new(io::ErrorKind::TimedOut, "utp timed out"),
        UtpError::Aborted => {
            io::Error::new(io::ErrorKind::ConnectionAborted, "utp connection aborted")
        }
        UtpError::Io(k) => io::Error::from(k),
    }
}

impl UtpStream {
    /// wait until the socket reports `Connected` (or error).
    pub async fn wait_connected(&mut self) -> io::Result<()> {
        let mut rx = self.st_rx.lock().await;
        loop {
            match *rx.borrow() {
                UtpState::Connected => return Ok(()),
                UtpState::ErrorWait | UtpState::Deleting => {
                    // the socket's `m_error` — reset, timeout,
                    // transport failure — not a generic message
                    return Err(match self.take_error() {
                        Some(e) => utp_error_to_io(e),
                        None => io::Error::other("utp connect failed"),
                    });
                }
                _ => {}
            }
            if rx.changed().await.is_err() {
                return Err(io::Error::other("utp pump died"));
            }
        }
    }

    /// `utp_stream::set_close_reason` — tag the FIN with a
    /// `utp_close_reason` code (`close_reason_t`). Call before
    /// `poll_shutdown`/drop; the extension rides the FIN.
    pub fn set_close_reason(&self, code: u16) {
        let _ = self.wr_tx.send(WriterMsg::CloseReason(code));
    }

    /// `get_close_reason` — the peer's `close_reason` code (0 when
    /// the FIN carried none / socket still live).
    #[must_use]
    pub fn their_close_reason(&self) -> u16 {
        self.their_close_reason
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// the last socket error surfaced by the pump, if any.
    fn take_error(&self) -> Option<UtpError> {
        self.last_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.read_buf.is_empty() {
            let n = buf.remaining().min(self.read_buf.len());
            for _ in 0..n {
                buf.put_u8(self.read_buf.pop_front().unwrap_or(0));
            }
            return Poll::Ready(Ok(()));
        }
        match self.rd_rx.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => {
                self.read_buf.extend(bytes);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => match self.take_error() {
                // upstream: `m_error == eof`/none → clean EOF
                None | Some(UtpError::Eof) => Poll::Ready(Ok(())),
                Some(e) => Poll::Ready(Err(utp_error_to_io(e))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // upstream `write_some` on a closed stream → `broken_pipe`;
        // the pump also drops post-Close payload as backstop
        if self.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "utp write after close",
            )));
        }
        match self.wr_tx.send(WriterMsg::Data(buf.to_vec())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => {
                self.closed = true;
                Poll::Ready(Err(io::Error::other("utp closed")))
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // upstream `utp_stream::close` → `m_impl.close()` — the FIN
        // leaves once the write buffer and nagle packet drain; the
        // pump keeps running until `FinSent` resolves
        self.closed = true;
        let _ = self.wr_tx.send(WriterMsg::Close);
        Poll::Ready(Ok(()))
    }
}

/// `UtpConnector` — `PeerConnector` impl: dial uTP over any
/// injected `DatagramSocket` (the OnionBit seam). Socket settings
/// come from [`UtpManager::config`] so hot-applied values reach new
/// connections (upstream reads `session_settings` per socket).
#[derive(Debug)]
pub struct UtpConnector {
    /// demux manager (share across all connections on the transport).
    pub manager: Arc<UtpManager>,
    /// datagram transport.
    pub datagram: Arc<dyn DatagramSocket>,
}

/// `UtpAcceptor` — `PeerAcceptor` impl: unmatched SYNs on the
/// manager's listener queue become responder `UtpStream`s.
#[derive(Debug)]
pub struct UtpAcceptor {
    /// demux manager (must be the same instance routing datagrams).
    pub manager: Arc<UtpManager>,
    /// datagram transport replies go out on.
    pub datagram: Arc<dyn DatagramSocket>,
    /// SYN queue (from `UtpManager::set_listener`).
    rx: Mutex<mpsc::Receiver<InboundMsg>>,
}

impl UtpAcceptor {
    /// install the listener on `manager`.
    pub async fn new(manager: Arc<UtpManager>, datagram: Arc<dyn DatagramSocket>) -> Self {
        let rx = Mutex::new(manager.set_listener().await);
        Self {
            manager,
            datagram,
            rx,
        }
    }
}

impl crate::transport::PeerAcceptor for UtpAcceptor {
    fn accept(self: Arc<Self>) -> crate::transport::AcceptFut {
        let me = self.clone();
        Box::pin(async move {
            let mut rx = me.rx.lock().await;
            let (syn, from) = loop {
                match rx.recv().await {
                    Some(InboundMsg::Datagram(b, f)) => break (b, f),
                    Some(InboundMsg::Abort | InboundMsg::SetConfig(_)) => continue,
                    None => {
                        return Err(crate::NetError::NoTransport("utp listener closed"));
                    }
                }
            };
            let s = me
                .manager
                .accept_syn(me.datagram.clone(), from, &syn, me.manager.config())
                .await?;
            Ok((Box::new(s) as BoxedPeerStream, Some(from)))
        })
    }
}

impl PeerConnector for UtpConnector {
    fn connect(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>>
    {
        Box::pin(async move { self.connect_with_bind_report(addr, None).await.map(|r| r.0) })
    }

    fn connect_with_bind_report(
        self: Arc<Self>,
        addr: SocketAddr,
        bind: Option<SocketAddr>,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = crate::Result<(BoxedPeerStream, Option<SocketAddr>)>>
                + Send
                + Sync,
        >,
    > {
        let me = self.clone();
        Box::pin(async move {
            let _ = bind; // uTP shares the datagram socket — no per-connect bind
            let mut s = me
                .manager
                .connect(me.datagram.clone(), addr, me.manager.config())
                .await?;
            s.wait_connected().await?;
            // the conn's `local_endpoint()` is the shared datagram
            // socket's bound address upstream.
            let local = Some(me.datagram.local_addr());
            Ok((Box::new(s) as BoxedPeerStream, local))
        })
    }
}
