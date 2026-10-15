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

use crate::transport::{BoxedPeerStream, DatagramSocket, PeerConnector};
use crate::utp_socket::{UtpConfig, UtpSocket, UtpState};

/// One inbound datagram routed to a socket pump.
type Inbound = (Vec<u8>, SocketAddr);

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
}

#[derive(Debug)]
struct ManagerInner {
    /// `(remote, conn_id)` → socket's inbound channel.
    sockets: HashMap<(SocketAddr, u16), mpsc::Sender<Inbound>>,
    /// unmatched SYNs go here (the acceptor's queue) — upstream's
    /// `utp_socket_manager` hands unknown SYNs to the listen socket.
    listener: Option<mpsc::Sender<Inbound>>,
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
            close_slots: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// `utp_socket_manager::incoming_packet` — demux to the right
    /// socket by `(remote, conn_id)` (uTP header byte 0 type ≠ RST;
    /// id at bytes 2–3 big-endian).
    pub async fn route(&self, from: SocketAddr, buf: &[u8]) {
        if buf.len() < 20 {
            return;
        }
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let inner = self.inner.lock().await;
        // try both receive-id and send-id slots (SYN arrives on the
        // receive id; subsequent packets on the send id — upstream
        // tries both lookups).
        for key in [
            (from, id),
            (from, id.wrapping_sub(1)),
            (from, id.wrapping_add(1)),
        ] {
            if let Some(tx) = inner.sockets.get(&key) {
                let _ = tx.send((buf.to_vec(), from)).await;
                return;
            }
        }
        // unmatched ST_SYN (type nibble 4) → the listen queue,
        // unless we're over the upstream SYN-flood cap
        // (`connections_limit * 2` sockets)
        if buf[0] >> 4 == 4 && inner.sockets.len() <= inner.conn_limit * 2 {
            if let Some(tx) = &inner.listener {
                let _ = tx.send((buf.to_vec(), from)).await;
            }
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
    pub async fn set_listener(&self) -> mpsc::Receiver<Inbound> {
        let (tx, rx) = mpsc::channel(64);
        self.inner.lock().await.listener = Some(tx);
        rx
    }

    /// accept one inbound SYN — build the responder socket
    /// (`send_id = SYN conn_id`, `recv_id = +1` upstream), register
    /// its pump, feed the SYN in, and return the stream.
    pub async fn accept_syn(
        self: &Arc<Self>,
        dg: Arc<dyn DatagramSocket>,
        from: SocketAddr,
        syn: &[u8],
        cfg: UtpConfig,
    ) -> io::Result<UtpStream> {
        if syn.len() < 20 || syn[0] >> 4 != 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a syn"));
        }
        let id = u16::from_be_bytes([syn[2], syn[3]]);
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
        sock.incoming_packet(syn, from, Instant::now());
        sock.send_deferred_ack();
        let recv_id = sock.recv_id();
        let rx = self.register((from, recv_id)).await;
        let (rd_tx, rd_rx) = mpsc::channel::<Vec<u8>>(64);
        let (wr_tx, wr_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        // state notifications ride a `watch` — latest-value
        // semantics, never blocks the pump when the consumer isn't
        // listening (upstream fires callbacks fire-and-forget). A
        // bounded mpsc here deadlocked the pump on the first
        // post-Connected transition nobody read.
        let (st_tx, st_rx) = tokio::sync::watch::channel(UtpState::None);
        // `close_reason` extension — the slot shared with the pump;
        // `UtpManager::set_close_reason` reaches it by remote.
        let close_slot = Arc::new(std::sync::atomic::AtomicU16::new(0));
        self.close_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(from, close_slot.clone());

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            rd_tx,
            st_tx,
            close_slot.clone(),
            (from, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);
        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            close_slot,
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

    /// register a socket pump keyed by `(remote, recv_id)`.
    async fn register(&self, key: (SocketAddr, u16)) -> mpsc::Receiver<Inbound> {
        let (tx, rx) = mpsc::channel(64);
        self.inner.lock().await.sockets.insert(key, tx);
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
        let rx = self.register((remote, recv_id)).await;
        let (rd_tx, rd_rx) = mpsc::channel::<Vec<u8>>(64);
        let (wr_tx, wr_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (st_tx, st_rx) = tokio::sync::watch::channel(UtpState::None);
        let close_slot = Arc::new(std::sync::atomic::AtomicU16::new(0));
        self.close_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(remote, close_slot.clone());

        let dg2 = dg.clone();
        let pump = tokio::spawn(socket_pump(
            sock,
            dg2,
            rx,
            wr_rx,
            rd_tx,
            st_tx,
            close_slot.clone(),
            (remote, recv_id),
            self.clone(),
        ));
        self.inner.lock().await.tasks.push(pump);

        Ok(UtpStream {
            rd_rx,
            wr_tx,
            st_rx: Arc::new(Mutex::new(st_rx)),
            close_slot,
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
    mut inbound_rx: mpsc::Receiver<Inbound>,
    mut wr_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    rd_tx: mpsc::Sender<Vec<u8>>,
    st_tx: tokio::sync::watch::Sender<UtpState>,
    close_slot: Arc<std::sync::atomic::AtomicU16>,
    key: (SocketAddr, u16),
    mgr: Arc<UtpManager>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut readbuf = vec![0u8; 16 * 1024];
    let mut last_state = UtpState::None;
    loop {
        // `set_close_reason` — a stamped reason rides every
        // subsequent packet until close (upstream mirrors it on
        // `m_close_reason`).
        let cr = close_slot.load(std::sync::atomic::Ordering::Relaxed);
        if cr != 0 {
            sock.set_close_reason(cr);
        }
        // drain outgoing datagrams
        for d in sock.drain_outbox() {
            tracing::trace!(
                "pump {key:?} -> ty={} seq={} ack={} len={}",
                d.bytes[0] >> 4,
                u16::from_be_bytes([d.bytes[16], d.bytes[17]]),
                u16::from_be_bytes([d.bytes[18], d.bytes[19]]),
                d.bytes.len()
            );
            let _ = dg.send_to(&d.bytes, d.to).await;
        }
        // push any received payload toward the reader
        match sock.read(&mut readbuf) {
            Ok(n) if n > 0 => {
                if rd_tx.send(readbuf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
            Err(_) => break, // EOF/reset → close the stream
            _ => {}
        }
        if sock.state() != last_state {
            last_state = sock.state();
            let _ = st_tx.send(last_state);
        }
        if matches!(last_state, UtpState::Deleting | UtpState::ErrorWait) {
            break;
        }
        tokio::select! {
            Some((buf, from)) = inbound_rx.recv() => {
                tracing::trace!("pump {key:?} <- ty={} seq={} ack={} len={}", buf[0]>>4,
                    u16::from_be_bytes([buf[16],buf[17]]),
                    u16::from_be_bytes([buf[18],buf[19]]), buf.len());
                sock.incoming_packet(&buf, from, Instant::now());
                sock.send_deferred_ack();
            }
            Some(buf) = wr_rx.recv() => {
                let _ = sock.write(&buf);
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
    mgr.unregister(key).await;
}

/// `UtpStream` — `PeerStream` impl over the pump channels.
#[derive(Debug)]
pub struct UtpStream {
    rd_rx: mpsc::Receiver<Vec<u8>>,
    wr_tx: mpsc::UnboundedSender<Vec<u8>>,
    st_rx: Arc<Mutex<tokio::sync::watch::Receiver<UtpState>>>,
    close_slot: Arc<std::sync::atomic::AtomicU16>,
    read_buf: std::collections::VecDeque<u8>,
}

impl UtpStream {
    /// `utp_stream::set_close_reason` — stamp the `close_reason_t`
    /// discriminant sent on the wire when this stream tears down.
    pub fn set_close_reason(&self, reason: u16) {
        self.close_slot
            .store(reason, std::sync::atomic::Ordering::Relaxed);
    }

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
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
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
        match self.wr_tx.send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::other("utp closed"))),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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
    rx: Mutex<mpsc::Receiver<Inbound>>,
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
            let (syn, from) = me
                .rx
                .lock()
                .await
                .recv()
                .await
                .ok_or(crate::NetError::NoTransport("utp listener closed"))?;
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
                .connect(me.datagram.clone(), addr, me.cfg.clone())
                .await?;
            s.wait_connected().await?;
            // the conn's `local_endpoint()` is the shared datagram
            // socket's bound address upstream.
            let local = Some(me.datagram.local_addr());
            Ok((Box::new(s) as BoxedPeerStream, local))
        })
    }
}
