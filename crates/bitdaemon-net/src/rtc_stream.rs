// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/rtc_stream.cpp` — upstream wraps a libdatachannel
//! `DataChannel` in an asio stream. We keep the same shape over an
//! injected [`RtcChannel`]: send buffers go out via `send`, inbound
//! payloads arrive through `feed` and drain into `AsyncRead`.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// `rtc_parse_endpoint` — `"host:port"` split at the last colon
/// (no bracket handling, verbatim upstream).
pub fn rtc_parse_endpoint(addr: &str) -> io::Result<SocketAddr> {
    let pos = addr
        .rfind(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "parse_failed"))?;
    let host = &addr[..pos];
    let port: u16 = addr[pos + 1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "parse_failed"))?;
    let ip: std::net::IpAddr = host
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "parse_failed"))?;
    Ok(SocketAddr::new(ip, port))
}

/// Injected WebRTC data channel — the `rtc::DataChannel` seam
/// (ADR-0008; a `webrtc` crate or OnionBit's lanes implement it).
pub trait RtcChannel: Send + Sync + Debug + 'static {
    /// `channel.send(binary)` — queues a binary message.
    fn send(&self, data: &[u8]);
    /// `channel.availableAmount()` — bytes buffered inbound.
    fn available(&self) -> usize;
    /// `is_open` — channel established.
    fn is_open(&self) -> bool;
    /// `channel.close()`.
    fn close(&self);
}

/// Shared inbound state — the channel owner feeds messages; the
/// stream drains them in `poll_read`.
#[derive(Debug)]
pub struct RtcInbound {
    /// `m_incoming` + queue — received messages not yet consumed.
    pub queue: VecDeque<Vec<u8>>,
    /// `m_read_buffer` remainder of a partially-read message.
    pub pending: VecDeque<u8>,
    /// channel closed while data may still be queued.
    pub closed: bool,
    /// read waker for `poll_read` starvation.
    pub waker: Option<Waker>,
}

/// `rtc_stream` — AsyncRead/Write over an [`RtcChannel`].
#[derive(Debug)]
pub struct RtcStream {
    chan: Arc<dyn RtcChannel>,
    inb: Arc<Mutex<RtcInbound>>,
    /// `m_write_buffer`/`m_write_buffer_size` — bytes queued while
    /// the channel applies back-pressure (upstream splits writes).
    write_buf: VecDeque<u8>,
}

impl RtcStream {
    /// `rtc_stream_impl(ioc, init)` — attach a channel + its inbound
    /// mailbox (the channel owner pushes into `inb`).
    #[must_use]
    pub fn new(chan: Arc<dyn RtcChannel>, inb: Arc<Mutex<RtcInbound>>) -> Self {
        Self {
            chan,
            inb,
            write_buf: VecDeque::new(),
        }
    }

    /// Channel-side hook — push an inbound binary message
    /// (`onMessage` upstream).
    pub fn feed(inb: &Arc<Mutex<RtcInbound>>, msg: Vec<u8>) {
        let mut g = inb.lock().unwrap_or_else(|e| e.into_inner());
        g.queue.push_back(msg);
        if let Some(w) = g.waker.take() {
            w.wake();
        }
    }

    /// channel closed (`onClosed` upstream) — pending reads see EOF
    /// once the queue drains.
    pub fn notify_closed(inb: &Arc<Mutex<RtcInbound>>) {
        let mut g = inb.lock().unwrap_or_else(|e| e.into_inner());
        g.closed = true;
        if let Some(w) = g.waker.take() {
            w.wake();
        }
    }

    /// `is_open`.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.chan.is_open()
    }
}

impl AsyncRead for RtcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut g = self.inb.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(front) = g.queue.front_mut() {
            let take = front.len().min(buf.remaining());
            buf.put_slice(&front[..take]);
            if take == front.len() {
                g.queue.pop_front();
            } else {
                front.drain(..take);
            }
            return Poll::Ready(Ok(()));
        }
        if g.closed {
            return Poll::Ready(Ok(())); // EOF
        }
        g.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for RtcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.chan.is_open() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "rtc channel closed",
            )));
        }
        // upstream flushes the backlog first (ordered stream)
        if self.write_buf.is_empty() {
            self.chan.send(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        self.write_buf.extend(buf.iter().copied());
        let n = buf.len();
        // try to flush
        let mut take = Vec::with_capacity(self.write_buf.len());
        while let Some(b) = self.write_buf.pop_front() {
            take.push(b);
        }
        self.chan.send(&take);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.chan.close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug)]
    struct Echo(Mutex<Vec<u8>>, std::sync::atomic::AtomicBool);
    impl RtcChannel for Echo {
        fn send(&self, d: &[u8]) {
            self.0.lock().unwrap().extend_from_slice(d);
        }
        fn available(&self) -> usize {
            self.0.lock().unwrap().len()
        }
        fn is_open(&self) -> bool {
            !self.1.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn close(&self) {
            self.1.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn mk() -> (RtcStream, Arc<Mutex<RtcInbound>>, Arc<Echo>) {
        let inb = Arc::new(Mutex::new(RtcInbound {
            queue: VecDeque::new(),
            pending: VecDeque::new(),
            closed: false,
            waker: None,
        }));
        let ch = Arc::new(Echo(
            Mutex::new(Vec::new()),
            std::sync::atomic::AtomicBool::new(false),
        ));
        (
            RtcStream::new(ch.clone() as Arc<dyn RtcChannel>, inb.clone()),
            inb,
            ch,
        )
    }

    #[test]
    fn parse_endpoint() {
        assert_eq!(
            rtc_parse_endpoint("1.2.3.4:6881").unwrap(),
            "1.2.3.4:6881".parse().unwrap()
        );
        assert!(rtc_parse_endpoint("nocolon").is_err());
        assert!(rtc_parse_endpoint("1.2.3.4:noport").is_err());
        assert!(rtc_parse_endpoint("notanip:1").is_err());
    }

    #[tokio::test]
    async fn read_feed_eof() {
        let (mut s, inb, _ch) = mk();
        RtcStream::feed(&inb, b"hello".to_vec());
        RtcStream::feed(&inb, b" world".to_vec());
        let mut buf = [0u8; 16];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b" world");
        RtcStream::notify_closed(&inb);
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn write_and_close() {
        let (mut s, _inb, ch) = mk();
        s.write_all(b"abc").await.unwrap();
        assert_eq!(ch.available(), 3);
        s.shutdown().await.unwrap();
        assert!(s.write_all(b"x").await.is_err());
    }
}
