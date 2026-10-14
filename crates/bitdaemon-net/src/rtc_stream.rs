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

/// `rtc_parse_endpoint` — `"address:port"` split at the last colon
/// (no bracket handling). The port goes through `std::stoul`
/// upstream: leading whitespace, an optional sign and a digit
/// *prefix* are accepted, and the value is truncated to `u16`.
fn stoul_u16(s: &str) -> Option<u16> {
    let t = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let digits: usize = t.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let v: u64 = t[..digits].parse().ok()?; // out_of_range → parse_failed
    let v = if neg { v.wrapping_neg() } else { v };
    Some(v as u16)
}

pub fn rtc_parse_endpoint(addr: &str) -> io::Result<SocketAddr> {
    let bad = || io::Error::new(io::ErrorKind::InvalidInput, "parse_failed");
    let pos = addr.rfind(':').ok_or_else(bad)?;
    let host = &addr[..pos];
    let port = stoul_u16(&addr[pos + 1..]).ok_or_else(bad)?;
    let ip: std::net::IpAddr = host.parse().map_err(|_| bad())?;
    Ok(SocketAddr::new(ip, port))
}

/// Injected WebRTC data channel — the `rtc::DataChannel` seam
/// (ADR-0008; a `webrtc` crate or OnionBit's lanes implement it).
pub trait RtcChannel: Send + Sync + Debug + 'static {
    /// `channel.send(binary)` — one datachannel message per call.
    fn send(&self, data: &[u8]);
    /// `channel.availableAmount()` — bytes buffered inbound.
    fn available(&self) -> usize;
    /// `channel.bufferedAmount()` — unsent bytes the channel still
    /// holds. Upstream returns `would_block` while this is > 0 and
    /// resumes writes on `onBufferedAmountLow`.
    fn buffered_amount(&self) -> usize {
        0
    }
    /// `channel.maxMessageSize()` — upper bound of one `send`
    /// (libdatachannel default is 256 KiB).
    fn max_message_size(&self) -> usize {
        256 * 1024
    }
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
    /// channel closed — upstream delivers `connection_reset` to
    /// pending ops and `not_connected` to later ones.
    pub closed: bool,
    /// a text (non-binary) message arrived — upstream `bad_message`.
    pub bad_message: bool,
    /// read waker for `poll_read` starvation.
    pub waker: Option<Waker>,
    /// write waker — woken by [`RtcStream::notify_buffered_low`].
    pub write_waker: Option<Waker>,
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

    /// channel closed (`onClosed` upstream) — pending reads get
    /// `connection_reset`, later reads `not_connected`; both map to
    /// an error here rather than a clean EOF.
    pub fn notify_closed(inb: &Arc<Mutex<RtcInbound>>) {
        let mut g = inb.lock().unwrap_or_else(|e| e.into_inner());
        g.closed = true;
        if let Some(w) = g.waker.take() {
            w.wake();
        }
        if let Some(w) = g.write_waker.take() {
            w.wake();
        }
    }

    /// a non-binary message arrived (`rtc::string` upstream) → the
    /// pending read completes with `bad_message`.
    pub fn feed_bad_message(inb: &Arc<Mutex<RtcInbound>>) {
        let mut g = inb.lock().unwrap_or_else(|e| e.into_inner());
        g.bad_message = true;
        if let Some(w) = g.waker.take() {
            w.wake();
        }
    }

    /// `onBufferedAmountLow` — the channel drained below its
    /// threshold; blocked writers retry.
    pub fn notify_buffered_low(inb: &Arc<Mutex<RtcInbound>>) {
        let mut g = inb.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = g.write_waker.take() {
            w.wake();
        }
    }

    /// `is_open`.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.chan.is_open()
    }

    /// `available()` — queued inbound bytes + the channel's own
    /// `availableAmount`.
    #[must_use]
    pub fn available(&self) -> usize {
        let g = self.inb.lock().unwrap_or_else(|e| e.into_inner());
        g.queue.iter().map(Vec::len).sum::<usize>() + self.chan.available()
    }
}

impl AsyncRead for RtcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut g = self.inb.lock().unwrap_or_else(|e| e.into_inner());
        // upstream `onClosed` → `connection_reset`; a closed channel
        // errors reads even with queued data
        if g.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "rtc channel closed",
            )));
        }
        if g.bad_message {
            g.bad_message = false;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad_message",
            )));
        }
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
        g.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for RtcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.chan.is_open() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "rtc channel closed",
            )));
        }
        // upstream `write_some`: would_block while the channel holds
        // unsent data; writes resume on `onBufferedAmountLow`
        if self.chan.buffered_amount() > 0 {
            self.write_buf.extend(buf.iter().copied());
            let mut g = self.inb.lock().unwrap_or_else(|e| e.into_inner());
            g.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        // `issue_write` — drain the backlog + this buffer as
        // `max_message_size`-bounded datachannel messages
        let max = self.chan.max_message_size().max(1);
        let mut pending = std::mem::take(&mut self.write_buf)
            .into_iter()
            .chain(buf.iter().copied())
            .collect::<Vec<u8>>();
        let mut sent = 0usize;
        while sent < pending.len() {
            let end = (sent + max).min(pending.len());
            self.chan.send(&pending[sent..end]);
            sent = end;
            if self.chan.buffered_amount() > 0 && sent < pending.len() {
                // channel started buffering mid-flush — keep the rest
                self.write_buf.extend(pending.drain(sent..));
                let mut g = self.inb.lock().unwrap_or_else(|e| e.into_inner());
                g.write_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(buf.len()))
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
            bad_message: false,
            waker: None,
            write_waker: None,
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
        // upstream onClosed → connection_reset, not a clean EOF
        assert!(s.read(&mut buf).await.is_err());
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
