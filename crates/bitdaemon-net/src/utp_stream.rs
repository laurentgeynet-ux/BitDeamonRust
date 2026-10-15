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
    /// `connections_limit` — unmatched SYNs are dropped once
    /// `sockets.len() > limit * 2` (upstream SYN-flood guard).
    conn_limit: usize,
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
            // upstream `settings_pack::connections_limit` default
            conn_limit: 200,
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
        }
    }

    /// `utp_socket_manager::incoming_packet` — demux to the right
    /// socket by `(remote, conn_id)`. The header gates mirror
    /// upstream (`< 20` or `version != 1` are dropped here, before
    /// any socket sees them). Dispatch is `try_send` outside the
    /// lock: datagrams are lossy by nature and a full socket queue
    /// must not stall the shared receive pump.
    pub async fn route(&self, from: SocketAddr, buf: &[u8]) {
        if buf.len() < UTP_HEADER_LEN || buf[0] & 0xf != 1 {
            return;
        }
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let (tx, listener) = {
            let inner = self.inner.lock().await;
            // upstream `equal_range(id)` + `match(ep, id)`: the
            // incoming conn-id is always the *receiver's* recv_id, so
            // an exact `(ep, id)` hit is the upstream shape
            let tx = inner.sockets.get(&(from, id)).map(|e| e.tx.clone());
            // unmatched ST_SYN (type nibble 4) → the listen queue,
            // unless we're over the upstream SYN-flood cap
            // (`connections_limit * 2` sockets)
            let listener = if buf[0] >> 4 == 4 && inner.sockets.len() <= inner.conn_limit * 2 {
                inner.listener.clone()
            } else {
                None
            };
            (tx, listener)
        };
        let msg = InboundMsg::Datagram(buf.to_vec(), from);
        if let Some(tx) = tx {
            let _ = tx.try_send(msg);
        } else if let Some(tx) = listener {
            let _ = tx.try_send(msg);
        }
    }

    /// `settings_pack::connections_limit` for the SYN-flood guard.
    pub async fn set_connection_limit(&self, limit: usize) {
        self.inner.lock().await.conn_limit = limit;
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

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            rd_tx,
            st_tx,
            last_error.clone(),
            (from, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);
        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            last_error,
            read_buf: std::collections::VecDeque::new(),
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

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            rd_tx,
            st_tx,
            last_error.clone(),
            (remote, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);

        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            last_error,
            read_buf: std::collections::VecDeque::new(),
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
    rd_tx: mpsc::Sender<Vec<u8>>,
    st_tx: tokio::sync::watch::Sender<UtpState>,
    last_error: Arc<std::sync::Mutex<Option<UtpError>>>,
    key: (SocketAddr, u16),
    mgr: Arc<UtpManager>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut readbuf = vec![0u8; 16 * 1024];
    let mut last_state = UtpState::None;
    let mut write_closed = false;
    loop {
        // drain outgoing datagrams — `dont_fragment` rides
        // `udp_socket::send`'s flags (MTU probes only)
        for d in sock.drain_outbox() {
            tracing::trace!(
                "pump {key:?} -> ty={} seq={} ack={} len={}",
                d.bytes[0] >> 4,
                u16::from_be_bytes([d.bytes[16], d.bytes[17]]),
                u16::from_be_bytes([d.bytes[18], d.bytes[19]]),
                d.bytes.len()
            );
            let flags = if d.dont_fragment {
                DatagramSendFlags::DONT_FRAGMENT
            } else {
                DatagramSendFlags::NONE
            };
            let _ = dg.send_to_flags(&d.bytes, d.to, flags).await;
        }
        // push received payload toward the reader — only while the
        // read channel has capacity. A stalled reader must never
        // park the pump: ACKs, ticks and the rest of the receive
        // path keep running upstream-style.
        if rd_tx.capacity() > 0 {
            match sock.read(&mut readbuf) {
                Ok(n) if n > 0 => {
                    if rd_tx.send(readbuf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
                // EOF/reset surface on the stream via `last_error`;
                // the socket's own state machine decides when the
                // pump ends (CloseWait semantics upstream)
                Err(e) => {
                    *last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(e);
                }
                _ => {}
            }
        }
        if sock.state() != last_state {
            last_state = sock.state();
            let _ = st_tx.send(last_state);
        }
        if matches!(last_state, UtpState::Deleting | UtpState::ErrorWait) {
            break;
        }
        tokio::select! {
            Some(msg) = inbound_rx.recv() => {
                match msg {
                    InboundMsg::Abort => sock.abort(),
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
                                InboundMsg::Datagram(buf, from) => {
                                    sock.incoming_packet(&buf, from, Instant::now());
                                }
                            }
                        }
                        sock.send_deferred_ack();
                    }
                }
            }
            msg = wr_rx.recv(), if !write_closed => {
                match msg {
                    Some(WriterMsg::Data(buf)) => {
                        let _ = sock.write(&buf);
                    }
                    Some(WriterMsg::Close) | None => {
                        // poll_shutdown or stream dropped — upstream
                        // `close()`: FIN once the queue drains
                        write_closed = true;
                        sock.close();
                    }
                    Some(WriterMsg::CloseReason(code)) => sock.set_close_reason(code),
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
    read_buf: std::collections::VecDeque<u8>,
}

impl UtpStream {
    /// wait until the socket reports `Connected` (or error).
    pub async fn wait_connected(&mut self) -> io::Result<()> {
        let mut rx = self.st_rx.lock().await;
        loop {
            match *rx.borrow() {
                UtpState::Connected => return Ok(()),
                UtpState::ErrorWait | UtpState::Deleting => {
                    return Err(io::Error::other("utp connect failed"));
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
                Some(UtpError::ConnectionReset) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "utp connection reset",
                ))),
                Some(UtpError::TimedOut) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "utp timed out",
                ))),
                Some(UtpError::Aborted) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "utp connection aborted",
                ))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.wr_tx.send(WriterMsg::Data(buf.to_vec())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::other("utp closed"))),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // upstream `utp_stream::close` → `m_impl.close()` — the FIN
        // leaves once the write buffer and nagle packet drain; the
        // pump keeps running until `FinSent` resolves
        let _ = self.wr_tx.send(WriterMsg::Close);
        Poll::Ready(Ok(()))
    }
}

/// `UtpConnector` — `PeerConnector` impl: dial uTP over any
/// injected `DatagramSocket` (the OnionBit seam).
#[derive(Debug)]
pub struct UtpConnector {
    /// demux manager (share across all connections on the transport).
    pub manager: Arc<UtpManager>,
    /// datagram transport.
    pub datagram: Arc<dyn DatagramSocket>,
    /// socket config.
    pub cfg: UtpConfig,
}

/// `UtpAcceptor` — `PeerAcceptor` impl: unmatched SYNs on the
/// manager's listener queue become responder `UtpStream`s.
#[derive(Debug)]
pub struct UtpAcceptor {
    /// demux manager (must be the same instance routing datagrams).
    pub manager: Arc<UtpManager>,
    /// datagram transport replies go out on.
    pub datagram: Arc<dyn DatagramSocket>,
    /// socket config.
    pub cfg: UtpConfig,
    /// SYN queue (from `UtpManager::set_listener`).
    rx: Mutex<mpsc::Receiver<InboundMsg>>,
}

impl UtpAcceptor {
    /// install the listener on `manager`.
    pub async fn new(
        manager: Arc<UtpManager>,
        datagram: Arc<dyn DatagramSocket>,
        cfg: UtpConfig,
    ) -> Self {
        let rx = Mutex::new(manager.set_listener().await);
        Self {
            manager,
            datagram,
            cfg,
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
                    Some(InboundMsg::Abort) => continue,
                    None => {
                        return Err(crate::NetError::NoTransport("utp listener closed"));
                    }
                }
            };
            let s = me
                .manager
                .accept_syn(me.datagram.clone(), from, &syn, me.cfg.clone())
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
        let me = self.clone();
        Box::pin(async move {
            let mut s = me
                .manager
                .connect(me.datagram.clone(), addr, me.cfg.clone())
                .await?;
            s.wait_connected().await?;
            Ok(Box::new(s) as BoxedPeerStream)
        })
    }
}
