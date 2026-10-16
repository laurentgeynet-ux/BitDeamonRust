//! Port of `utp_stream.cpp`'s `utp_socket_impl` — the uTP reliable
//! stream state machine: SYN handshake, DATA/STATE/FIN/RESET, seq/ack
//! tracking, reorder buffer, SACK, LEDBAT congestion control, MTU
//! probing, retransmission and `tick()` timeouts.
//!
//! Adaptations (documented in PORTING.md):
//! - upstream sends datagrams inline via `utp_socket_manager::send_packet`;
//!   here packets are appended to `outbox` and drained by the caller,
//!   which delivers them through the injected `DatagramSocket`
//!   (ADR-0008 — anonymous circuits). A `None` send error is assumed;
//!   a transport that cannot send reports `WouldBlock` via
//!   `UtpSocket::stalled`-equivalent flow is handled by re-queueing.
//! - upstream user-provided iovec buffers become a byte-queue write
//!   buffer and a `read(&mut [u8])` pull API.
//! - upstream callbacks (`on_read`/`on_write`/`on_connect`) become
//!   pollable state: `UtpSocket::state`, `UtpSocket::error`.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bitdaemon_common::random::random;
use bitdaemon_common::sliding_average::SlidingAverage;
use bitdaemon_common::timestamp_history::{TimestampHistory, compare_less_wrap};
use thiserror::Error;

use crate::packet_buffer::PacketBuffer;
use crate::utp::{UTP_HEADER_LEN, UtpExtension, UtpHeader, UtpType};

/// `ACK_MASK`.
const ACK_MASK: u16 = 0xffff;
/// `dup_ack_limit`.
const DUP_ACK_LIMIT: u8 = 3;
const ETHERNET_MTU: u32 = 1500;
const INET_MIN_MTU: u32 = 576;
const IPV4_HEADER: u32 = 20;
const UDP_HEADER: u32 = 8;

/// `state_t` — the impl state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum UtpState {
    /// `state_t::none`.
    None = 0,
    /// `state_t::syn_sent`.
    SynSent,
    /// `state_t::connected`.
    Connected,
    /// `state_t::fin_sent`.
    FinSent,
    /// `state_t::error_wait`.
    ErrorWait,
    /// `state_t::deleting`.
    Deleting,
}

/// Socket-level errors mirroring the `error_code`s upstream stores in
/// `m_error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum UtpError {
    /// `asio::error::eof` — graceful close.
    #[error("end of stream")]
    Eof,
    /// `asio::error::connection_reset`.
    #[error("connection reset")]
    ConnectionReset,
    /// `asio::error::timed_out`.
    #[error("connection timed out")]
    TimedOut,
    /// `asio::error::connection_aborted`.
    #[error("connection aborted")]
    Aborted,
    /// transport-level send/recv `error_code` (upstream stores the
    /// raw `error_code` in `m_error`; we keep the `io::ErrorKind`).
    #[error("socket i/o error: {0:?}")]
    Io(io::ErrorKind),
}

/// Upstream `counters` channels the uTP layer increments, in the
/// declaration order of `performance_counters.hpp`. Emitted through
/// [`UtpConfig::counter_sink`]; a `None` sink skips the call like
/// upstream's `TORRENT_ASSERT`/`inc_stats_counter` on a counters
/// object, i.e. it is a pure observability hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtpStat {
    /// `utp_packet_loss`.
    PacketLoss,
    /// `utp_timeout`.
    Timeout,
    /// `utp_packets_in`.
    PacketsIn,
    /// `utp_packets_out`.
    PacketsOut,
    /// `utp_fast_retransmit`.
    FastRetransmit,
    /// `utp_packet_resend`.
    PacketResend,
    /// `utp_samples_above_target`.
    SamplesAboveTarget,
    /// `utp_samples_below_target`.
    SamplesBelowTarget,
    /// `utp_payload_pkts_in`.
    PayloadPktsIn,
    /// `utp_payload_pkts_out`.
    PayloadPktsOut,
    /// `utp_invalid_pkts_in`.
    InvalidPktsIn,
    /// `utp_redundant_pkts_in`.
    RedundantPktsIn,
    /// `num_utp_idle + m_state` — socket-state gauge; the payload is
    /// the `UtpState` discriminant the delta applies to.
    StateGauge(usize),
}

/// `m_sm.inc_stats_counter(stat, delta)` sink.
pub type UtpCounterSink = Arc<dyn Fn(UtpStat, i64) + Send + Sync>;

/// `utp_socket_manager` settings getters — defaults verbatim from
/// `settings_pack.cpp`.
#[derive(Clone)]
pub struct UtpConfig {
    /// `utp_target_delay` (ms → µs inside).
    pub target_delay_ms: i32,
    /// `utp_gain_factor`.
    pub gain_factor: i32,
    /// `utp_min_timeout`.
    pub min_timeout: i32,
    /// `utp_syn_resends`.
    pub syn_resends: u32,
    /// `utp_fin_resends`.
    pub fin_resends: u32,
    /// `utp_num_resends`.
    pub num_resends: u32,
    /// `utp_connect_timeout`.
    pub connect_timeout: i32,
    /// `utp_loss_multiplier` (percent).
    pub loss_multiplier: i64,
    /// `utp_cwnd_reduce_timer` (ms).
    pub cwnd_reduce_timer: i64,
    /// `proxy_type == socks5 || socks5_pw` — affects `mtu_for_dest`.
    pub proxy_socks5: bool,
    /// `utp_socket_manager::m_counters` — receives the same stat
    /// increments upstream posts to `counters`.
    pub counter_sink: Option<UtpCounterSink>,
}

impl std::fmt::Debug for UtpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpConfig")
            .field("target_delay_ms", &self.target_delay_ms)
            .field("gain_factor", &self.gain_factor)
            .field("min_timeout", &self.min_timeout)
            .field("syn_resends", &self.syn_resends)
            .field("fin_resends", &self.fin_resends)
            .field("num_resends", &self.num_resends)
            .field("connect_timeout", &self.connect_timeout)
            .field("loss_multiplier", &self.loss_multiplier)
            .field("cwnd_reduce_timer", &self.cwnd_reduce_timer)
            .field("proxy_socks5", &self.proxy_socks5)
            .finish_non_exhaustive()
    }
}

impl Default for UtpConfig {
    fn default() -> Self {
        Self {
            target_delay_ms: 100,
            gain_factor: 3000,
            min_timeout: 500,
            syn_resends: 2,
            fin_resends: 2,
            num_resends: 3,
            connect_timeout: 3000,
            loss_multiplier: 50,
            cwnd_reduce_timer: 100,
            proxy_socks5: false,
            counter_sink: None,
        }
    }
}

/// `packet` — an entry in `m_inbuf`/`m_outbuf`/`m_receive_buffer`.
#[derive(Debug)]
struct Packet {
    buf: Vec<u8>,
    size: u16,
    header_size: u16,
    allocated: usize,
    num_transmissions: u32,
    need_resend: bool,
    mtu_probe: bool,
    send_time: Instant,
}

impl Packet {
    fn new(allocated: usize) -> Self {
        Self {
            buf: vec![0; allocated],
            size: 0,
            header_size: 0,
            allocated,
            num_transmissions: 0,
            need_resend: false,
            mtu_probe: false,
            send_time: Instant::now(),
        }
    }
    fn seq_nr(&self) -> u16 {
        u16::from_be_bytes([self.buf[16], self.buf[17]])
    }
}

/// One queued datagram to deliver via `DatagramSocket`.
#[derive(Debug)]
pub struct OutDatagram {
    /// Destination.
    pub to: SocketAddr,
    /// Wire bytes.
    pub bytes: Vec<u8>,
    /// `udp_socket::dont_fragment` (MTU probes).
    pub dont_fragment: bool,
}

/// `utp_socket_impl`.
pub struct UtpSocket {
    cfg: UtpConfig,
    /// `m_remote_address` + `m_port`.
    remote: SocketAddr,
    /// datagrams to send (`m_sm.send_packet` calls).
    outbox: VecDeque<OutDatagram>,

    m_error: Option<UtpError>,
    state: UtpState,

    /// `m_inbuf` / `m_outbuf`.
    inbuf: PacketBuffer<Packet>,
    outbuf: PacketBuffer<Packet>,
    /// `m_receive_buffer` — in-order payloads awaiting `read()`.
    receive_buffer: VecDeque<Packet>,
    /// `m_write_buffer` — flattened iovecs.
    write_buffer: VecDeque<u8>,
    /// `m_nagle_packet`.
    nagle_packet: Option<Box<Packet>>,
    /// `m_needs_resend` — seq numbers (upstream stores `packet*`).
    needs_resend: VecDeque<u16>,

    m_timeout: Instant,
    m_last_history_step: Instant,
    m_next_loss: Instant,
    /// fixed-point <<16.
    m_cwnd: i64,
    m_delay_hist: TimestampHistory,
    m_their_delay_hist: TimestampHistory,
    m_ssthres: i32,
    m_buffered_incoming_bytes: i32,
    m_reply_micro: u32,
    m_adv_wnd: u32,
    m_bytes_in_flight: i32,
    m_write_buffer_size: i32,
    m_receive_buffer_size: i32,
    m_receive_buffer_capacity: i32,
    m_delay_sample_hist: [u32; 3],
    m_in_packets: u32,
    m_out_packets: u32,
    m_send_delay: i32,
    m_recv_delay: i32,
    m_rtt: SlidingAverage<16>,
    m_close_reason: u16,

    m_send_id: u16,
    m_recv_id: u16,
    m_ack_nr: u16,
    m_seq_nr: u16,
    m_acked_seq_nr: u16,
    m_fast_resend_seq_nr: u16,
    m_nagle_seq_nr: u16,
    m_in_eof_seq_nr: u16,
    m_loss_seq_nr: u16,
    m_mtu: u16,
    m_mtu_floor: u16,
    m_mtu_ceiling: u16,
    m_mtu_seq: u16,
    m_duplicate_acks: u8,
    m_num_timeouts: u8,
    m_delay_sample_idx: usize,

    /// `m_sm.restrict_mtu(m_mtu)` — set when a give-up packet was
    /// larger than the floor (MTU probe likely killed the conn);
    /// the caller feeds it to `UtpSocketDemux::restrict_mtu`.
    failed_mtu: Option<u16>,

    /// close reason received from the peer (`on_close_reason`).
    pub their_close_reason: u16,

    m_in_eof: bool,
    m_out_eof: bool,
    m_nagle: bool,
    m_slow_start: bool,
    m_cwnd_full: bool,
    m_deferred_ack: bool,
    m_stalled: bool,
    m_confirmed: bool,
}

impl UtpSocket {
    /// `utp_socket_impl(recv_id, send_id, userdata, sm)`.
    #[must_use]
    pub fn new(recv_id: u16, send_id: u16, cfg: UtpConfig) -> Self {
        debug_assert!(recv_id == send_id.wrapping_add(1) || send_id == recv_id.wrapping_add(1));
        let now = Instant::now();
        let sock = Self {
            m_timeout: now
                + std::time::Duration::from_millis(u64::try_from(cfg.connect_timeout).unwrap_or(0)),
            m_last_history_step: now,
            m_next_loss: now,
            remote: SocketAddr::from(([0, 0, 0, 0], 0)),
            outbox: VecDeque::new(),
            m_error: None,
            state: UtpState::None,
            inbuf: PacketBuffer::default(),
            outbuf: PacketBuffer::default(),
            receive_buffer: VecDeque::new(),
            write_buffer: VecDeque::new(),
            nagle_packet: None,
            needs_resend: VecDeque::new(),
            m_cwnd: i64::from(ETHERNET_MTU) << 16,
            m_delay_hist: TimestampHistory::default(),
            m_their_delay_hist: TimestampHistory::default(),
            m_ssthres: 0,
            m_buffered_incoming_bytes: 0,
            m_reply_micro: 0,
            m_adv_wnd: ETHERNET_MTU,
            m_bytes_in_flight: 0,
            m_write_buffer_size: 0,
            m_receive_buffer_size: 0,
            m_receive_buffer_capacity: 1024 * 1024,
            m_delay_sample_hist: [u32::MAX; 3],
            m_in_packets: 0,
            m_out_packets: 0,
            m_send_delay: 0,
            m_recv_delay: 0,
            m_rtt: SlidingAverage::default(),
            m_close_reason: 0,
            m_send_id: send_id,
            m_recv_id: recv_id,
            m_ack_nr: 0,
            m_seq_nr: 0,
            m_acked_seq_nr: 0,
            m_fast_resend_seq_nr: 0,
            m_nagle_seq_nr: 0,
            m_in_eof_seq_nr: 0,
            m_loss_seq_nr: 0,
            m_mtu: (ETHERNET_MTU - IPV4_HEADER - UDP_HEADER - 8 - 24 - 36) as u16,
            m_mtu_floor: (INET_MIN_MTU - IPV4_HEADER - UDP_HEADER) as u16,
            m_mtu_ceiling: (ETHERNET_MTU - IPV4_HEADER - UDP_HEADER) as u16,
            m_mtu_seq: 0,
            m_duplicate_acks: 0,
            m_num_timeouts: 0,
            m_delay_sample_idx: 0,
            failed_mtu: None,
            their_close_reason: 0,
            m_in_eof: false,
            m_out_eof: false,
            m_nagle: true,
            m_slow_start: true,
            m_cwnd_full: false,
            m_deferred_ack: false,
            m_stalled: false,
            m_confirmed: false,
            cfg,
        };
        sock.inc(UtpStat::StateGauge(UtpState::None as usize), 1);
        sock
    }

    /// `state()`.
    #[must_use]
    pub fn state(&self) -> UtpState {
        self.state
    }
    /// `m_error`.
    #[must_use]
    pub fn error(&self) -> Option<UtpError> {
        self.m_error
    }
    /// `m_confirmed`.
    #[must_use]
    pub fn confirmed(&self) -> bool {
        self.m_confirmed
    }
    /// `m_in_eof` — the peer sent its FIN.
    #[must_use]
    pub fn in_eof_seen(&self) -> bool {
        self.m_in_eof
    }
    /// `remote_endpoint()`.
    #[must_use]
    pub fn remote(&self) -> SocketAddr {
        self.remote
    }
    /// `m_send_delay`.
    #[must_use]
    pub fn send_delay(&self) -> i32 {
        self.m_send_delay
    }
    /// `m_recv_delay`.
    #[must_use]
    pub fn recv_delay(&self) -> i32 {
        self.m_recv_delay
    }
    /// `available()`.
    #[must_use]
    pub fn available(&self) -> usize {
        self.m_receive_buffer_size as usize
    }
    /// `m_recv_id`.
    #[must_use]
    pub fn recv_id(&self) -> u16 {
        self.m_recv_id
    }
    /// `m_send_id`.
    #[must_use]
    pub fn send_id(&self) -> u16 {
        self.m_send_id
    }
    /// `match(ep, id)` — demux check.
    #[must_use]
    pub fn matches(&self, ep: SocketAddr, id: u16) -> bool {
        self.remote == ep && self.m_recv_id == id
    }
    /// Drain queued datagrams (`m_sm.send_packet` calls).
    pub fn drain_outbox(&mut self) -> impl Iterator<Item = OutDatagram> + '_ {
        self.outbox.drain(..)
    }

    /// Pop the next queued datagram — the pump prefers this over
    /// [`UtpSocket::drain_outbox`] so send errors can requeue while
    /// iterating (`note_send_error`).
    pub fn pop_outbox(&mut self) -> Option<OutDatagram> {
        self.outbox.pop_front()
    }

    /// `m_sm.inc_stats_counter`.
    fn inc(&self, stat: UtpStat, delta: i64) {
        if let Some(sink) = &self.cfg.counter_sink {
            sink(stat, delta);
        }
    }

    fn set_state(&mut self, s: UtpState) {
        if s == self.state {
            return;
        }
        self.inc(UtpStat::StateGauge(self.state as usize), -1);
        self.state = s;
        self.inc(UtpStat::StateGauge(self.state as usize), 1);
    }

    /// `do_connect(ep)` → `send_syn`.
    pub fn connect(&mut self, ep: SocketAddr) {
        self.remote = ep;
        self.send_syn();
    }

    /// `close()` — flush then FIN. Mirrors upstream's guards: the FIN
    /// may only leave once the write buffer is empty *and* a pending
    /// Nagle packet or a queued packet under `m_seq_nr` has been
    /// sent — otherwise the FIN could overtake queued payload or be
    /// stamped with payload in its header buffer.
    pub fn close(&mut self) {
        self.m_out_eof = true;
        if self.state != UtpState::Connected
            || self.m_write_buffer_size > 0
            || self.nagle_packet.is_some()
            || self.outbuf.at(u32::from(self.m_seq_nr)).is_some()
        {
            return;
        }
        self.send_fin();
    }

    /// `abort()`.
    pub fn abort(&mut self) {
        self.m_error = Some(UtpError::Aborted);
        self.set_state(UtpState::ErrorWait);
        self.test_socket_state();
    }

    /// `utp_socket_impl::destroy()` — the owner went away: `close()`
    /// (the FIN still flushes queued/Nagle data), then drop the
    /// unsent write buffer and the undelivered read queue. With no
    /// reader or writer left, an un-established or already-failed
    /// socket is deleted outright (`operation_aborted` on every
    /// handler upstream).
    pub fn destroy(&mut self) {
        self.close();
        self.receive_buffer.clear();
        self.m_receive_buffer_size = 0;
        self.write_buffer.clear();
        self.m_write_buffer_size = 0;
        while self.send_pkt(0) {}
        if matches!(
            self.state,
            UtpState::ErrorWait | UtpState::None | UtpState::SynSent
        ) {
            self.set_state(UtpState::Deleting);
        }
    }

    /// `m_close_reason` setter — carried by extension 3 on the next
    /// packet(s) (and the FIN), per upstream `set_close_reason`.
    pub fn set_close_reason(&mut self, code: u16) {
        self.m_close_reason = code;
    }

    /// `utp_stream::write_some` — queue payload. A write after
    /// `close()`/`FIN` is a `broken_pipe` upstream — return 0 without
    /// touching the buffer (appending could otherwise be picked up
    /// by a packet stamped *after* the queued FIN).
    pub fn write(&mut self, buf: &[u8]) -> usize {
        if self.m_out_eof || self.state >= UtpState::FinSent {
            return 0;
        }
        self.write_buffer.extend(buf);
        self.m_write_buffer_size += buf.len() as i32;
        // issue_write → drain while send_pkt() returns true
        while self.send_pkt(0) {}
        buf.len()
    }

    /// `read_some` — pull buffered payload into `out`. Returns the
    /// number of bytes copied or the stored error (EOF/reset/…).
    pub fn read(&mut self, out: &mut [u8]) -> Result<usize, UtpError> {
        if self.m_receive_buffer_size <= 0 {
            if self.receive_buffer.is_empty()
                && self.m_in_eof
                && self.m_in_eof_seq_nr == self.m_ack_nr
            {
                return Err(UtpError::Eof);
            }
            if let Some(e) = self.m_error {
                return Err(e);
            }
            return Ok(0);
        }

        let mut ret = 0usize;
        let mut target = 0usize;
        let mut pop_packets = 0;
        for p in self.receive_buffer.iter_mut() {
            if target == out.len() {
                break;
            }
            let to_copy = usize::from(p.size - p.header_size)
                .min(out.len() - target)
                .min(usize::from(u16::MAX) - usize::from(p.header_size));
            out[target..target + to_copy].copy_from_slice(
                &p.buf[usize::from(p.header_size)..usize::from(p.header_size) + to_copy],
            );
            ret += to_copy;
            self.m_receive_buffer_size -= to_copy as i32;
            p.header_size += to_copy as u16;
            target += to_copy;
            if p.header_size == p.size {
                pop_packets += 1;
            }
            if self.m_receive_buffer_size <= 0 {
                break;
            }
        }
        self.receive_buffer.drain(..pop_packets);
        if ret == 0
            && self.receive_buffer.is_empty()
            && self.m_in_eof
            && self.m_in_eof_seq_nr == self.m_ack_nr
        {
            return Err(UtpError::Eof);
        }
        Ok(ret)
    }

    // ---- internal: packet send machinery ----

    fn send_packet(&mut self, bytes: &[u8], dont_fragment: bool) {
        self.outbox.push_back(OutDatagram {
            to: self.remote,
            bytes: bytes.to_vec(),
            dont_fragment,
        });
    }

    /// `send_pkt`'s send-error legs (`utp_stream.cpp:1786-1831`) —
    /// the pump reports the transport's verdict here.
    /// - `EMSGSIZE`/`WSAEMSGSIZE`: clamp the MTU ceiling below the
    ///   refused size; the packet is requeued without DF (upstream
    ///   clears `mtu_probe` and resends inline — push_front is the
    ///   immediate retry).
    /// - `WouldBlock`/`Interrupted`: `m_stalled` + queue back — the
    ///   pump calls [`UtpSocket::on_writable`] on the next
    ///   successful send (upstream `subscribe_writable`).
    /// - anything else: `m_error` + `error_wait` (`release_packet`).
    pub fn note_send_error(&mut self, mut d: OutDatagram, err: &io::Error) {
        if matches!(err.raw_os_error(), Some(90) | Some(10040)) {
            self.m_mtu_ceiling = u16::try_from(d.bytes.len().saturating_sub(1)).unwrap_or(u16::MAX);
            self.update_mtu_limits();
            self.m_mtu_seq = 0;
            if d.bytes.len() >= UTP_HEADER_LEN {
                let seq = u16::from_be_bytes([d.bytes[16], d.bytes[17]]);
                if let Some(p) = self.outbuf.at_mut(u32::from(seq)) {
                    if u16::from_be_bytes([p.buf[16], p.buf[17]]) == seq {
                        p.mtu_probe = false;
                    }
                }
            }
            d.dont_fragment = false;
            self.outbox.push_front(d);
            return;
        }
        if matches!(
            err.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
        ) {
            // `m_stalled` — retried at the next drain; upstream
            // defers a queued ACK, our requeued STATE rides again
            // the same way
            self.m_stalled = true;
            self.outbox.push_front(d);
            return;
        }
        // the failed send counted `m_out_packets`/`num_transmissions`
        // at queue time; upstream increments only on success
        self.m_out_packets = self.m_out_packets.saturating_sub(1);
        self.inc(UtpStat::PacketsOut, -1);
        if self.m_error.is_none() {
            self.m_error = Some(UtpError::Io(err.kind()));
            self.set_state(UtpState::ErrorWait);
            self.test_socket_state();
        }
    }

    /// `m_stalled` — transport refused a send with `WouldBlock`.
    #[must_use]
    pub fn stalled(&self) -> bool {
        self.m_stalled
    }

    /// `writable()` — a send succeeded after a `WouldBlock`; clear
    /// the stall and flush pending writes (`issue_write`).
    pub fn on_writable(&mut self) {
        if !self.m_stalled {
            return;
        }
        self.m_stalled = false;
        if matches!(self.state, UtpState::Deleting | UtpState::ErrorWait) {
            return;
        }
        while self.send_pkt(0) {}
    }

    /// Refresh the settings snapshot — upstream reads the shared
    /// `session_settings` live (`m_sm` getters); the manager pushes
    /// hot-applied `UtpConfig` updates through here.
    pub fn update_cfg(&mut self, cfg: UtpConfig) {
        // keep this socket's counter sink — `m_stats` (the manager's
        // counters object) does not change identity on reconfig
        let sink = self.cfg.counter_sink.clone();
        self.cfg = cfg;
        if self.cfg.counter_sink.is_none() {
            self.cfg.counter_sink = sink;
        }
    }

    /// `send_syn`.
    fn send_syn(&mut self) {
        self.m_seq_nr = random(0xffff) as u16;
        self.m_acked_seq_nr = self.m_seq_nr.wrapping_sub(1) & ACK_MASK;
        self.m_loss_seq_nr = self.m_acked_seq_nr;
        self.m_ack_nr = 0;
        self.m_fast_resend_seq_nr = self.m_seq_nr;

        let mut p = Packet::new(UTP_HEADER_LEN);
        p.size = UTP_HEADER_LEN as u16;
        p.header_size = UTP_HEADER_LEN as u16;
        p.send_time = Instant::now();
        let ts = micros_now();
        let mut h = [0u8; UTP_HEADER_LEN];
        h[0] = ((UtpType::Syn as u8) << 4) | 1;
        h[1] = UtpExtension::None as u8;
        // SYN carries the *receive* id — peer responds on id+1.
        h[2..4].copy_from_slice(&self.m_recv_id.to_be_bytes());
        h[4..8].copy_from_slice(&ts.to_be_bytes());
        h[8..12].copy_from_slice(&self.m_reply_micro.to_be_bytes());
        // wnd_size = 0
        h[16..18].copy_from_slice(&self.m_seq_nr.to_be_bytes());
        // ack_nr = 0
        p.buf[..UTP_HEADER_LEN].copy_from_slice(&h);

        self.send_packet(&p.buf[..usize::from(p.size)], false);
        p.num_transmissions += 1;
        self.outbuf.insert(u32::from(self.m_seq_nr), p);
        self.m_seq_nr = self.m_seq_nr.wrapping_add(1) & ACK_MASK;
        self.set_state(UtpState::SynSent);
    }

    /// `send_fin`.
    fn send_fin(&mut self) {
        debug_assert!(self.m_write_buffer_size == 0);
        debug_assert!(self.m_out_eof);
        debug_assert!(self.nagle_packet.is_none());
        self.send_pkt(PKT_FIN);
        self.set_state(if self.m_error.is_none() {
            UtpState::FinSent
        } else {
            UtpState::ErrorWait
        });
    }

    /// `send_reset(ack_nr)`.
    fn send_reset(&mut self, ack_nr: u16) {
        let mut h = [0u8; UTP_HEADER_LEN];
        h[0] = ((UtpType::Reset as u8) << 4) | 1;
        h[1] = UtpExtension::None as u8;
        h[2..4].copy_from_slice(&self.m_send_id.to_be_bytes());
        h[4..8].copy_from_slice(&micros_now().to_be_bytes());
        h[8..12].copy_from_slice(&self.m_reply_micro.to_be_bytes());
        let seq = random(0xffff) as u16;
        h[16..18].copy_from_slice(&seq.to_be_bytes());
        h[18..20].copy_from_slice(&ack_nr.to_be_bytes());
        self.send_packet(&h, false);
    }

    /// `write_sack`.
    fn write_sack(&self, buf: &mut [u8]) {
        debug_assert!(!self.inbuf.is_empty());
        let mut ack_nr = self.m_ack_nr.wrapping_add(2) & ACK_MASK;
        for b in buf.iter_mut() {
            *b = 0;
            let mut mask = 1u8;
            for _ in 0..8 {
                if self.inbuf.at(u32::from(ack_nr)).is_some() {
                    *b |= mask;
                }
                mask <<= 1;
                ack_nr = ack_nr.wrapping_add(1) & ACK_MASK;
            }
        }
    }

    /// `write_payload` — pull `size` bytes from the write buffer.
    fn write_payload(&mut self, ptr: &mut [u8]) {
        let n = ptr.len();
        for b in ptr.iter_mut() {
            *b = self.write_buffer.pop_front().unwrap_or(0);
        }
        self.m_write_buffer_size -= n as i32;
        debug_assert!(self.m_write_buffer_size >= 0);
    }

    /// `send_pkt(flags)` — the heart of the send path. Returns true
    /// when it may be called again (payload still pending).
    fn send_pkt(&mut self, flags: u8) -> bool {
        if self.m_stalled {
            if flags & PKT_ACK != 0 {
                self.m_deferred_ack = true;
            }
            return false;
        }

        let force = (flags & PKT_ACK != 0) || (flags & PKT_FIN != 0) || self.m_out_eof;
        let force_flush_nagle = self.m_out_eof && self.m_write_buffer_size > 0;

        // resend queue first (`m_needs_resend` stores seq numbers —
        // upstream stores `packet*`)
        while let Some(&seq) = self.needs_resend.front() {
            if self.outbuf.at(u32::from(seq)).is_none() {
                self.needs_resend.pop_front();
                continue;
            }
            if !self.resend_packet_seq(seq, false) {
                if !force {
                    return false;
                }
                if matches!(self.state, UtpState::ErrorWait | UtpState::Deleting) || self.m_stalled
                {
                    return false;
                }
                break;
            }
            // `resend_packet_seq` unlinks its own entry (upstream
            // erases inside `resend_packet`); a still-matching head
            // is a stale entry — pop it
            if self.needs_resend.front() == Some(&seq) {
                self.needs_resend.pop_front();
            }
            // don't fast-resend this packet
            if self.m_fast_resend_seq_nr == seq {
                self.m_fast_resend_seq_nr = self.m_fast_resend_seq_nr.wrapping_add(1) & ACK_MASK;
            }
        }

        // MTU DISCOVERY
        let mtu_probe = self.m_mtu_seq == 0
            && self.m_seq_nr != 0
            && (self.m_cwnd >> 16) > i64::from(self.m_mtu_floor) * 3;
        let effective_mtu = if mtu_probe {
            i32::from(self.m_mtu)
        } else {
            i32::from(self.m_mtu_floor)
        };

        // `close_reason` can be cleared on the nagle path (upstream
        // does not update a close-reason ext on a nagled packet)
        let mut close_reason = u32::from(self.m_close_reason);

        let mut sack = 0i32;
        if !self.inbuf.is_empty() {
            let max_sack_size =
                effective_mtu - UTP_HEADER_LEN as i32 - 2 - if close_reason != 0 { 6 } else { 0 };
            sack = (self.inbuf.span() as i32 + 7) / 8;
            if sack > max_sack_size {
                sack = max_sack_size;
            }
        }

        let header_size = UTP_HEADER_LEN as i32
            + if sack != 0 { sack + 2 } else { 0 }
            + if close_reason != 0 { 6 } else { 0 };

        let nagle_size = self
            .nagle_packet
            .as_ref()
            .map_or(0, |p| i32::from(p.size - p.header_size));
        let mut payload_size =
            (self.m_write_buffer_size + nagle_size).min(effective_mtu - header_size);
        debug_assert!(payload_size >= 0);

        if i64::from(self.m_bytes_in_flight + payload_size)
            > (self.m_cwnd >> 16).min(i64::from(self.m_adv_wnd))
        {
            if flags & PKT_ACK != 0 {
                payload_size = 0;
            }
            self.m_cwnd_full = true;
            if !force {
                return false;
            }
        }
        if payload_size == 0 && !force {
            return false;
        }

        let mut p: Packet;
        let h_seq_nr;

        if self.nagle_packet.is_none() || (payload_size == 0 && force && self.m_cwnd_full) {
            p = Packet::new(effective_mtu as usize);
            p.size = (header_size + payload_size) as u16;
            p.header_size = header_size as u16;
            let ext = if sack != 0 {
                UtpExtension::Sack as u8
            } else if close_reason != 0 {
                UtpExtension::CloseReason as u8
            } else {
                UtpExtension::None as u8
            };
            let ty = if payload_size != 0 {
                UtpType::Data
            } else {
                UtpType::State
            };
            p.buf[0] = ((ty as u8) << 4) | 1;
            p.buf[1] = ext;
            p.buf[2..4].copy_from_slice(&self.m_send_id.to_be_bytes());
            p.buf[16..18].copy_from_slice(&self.m_seq_nr.to_be_bytes());
            if payload_size > 0 {
                self.inc(UtpStat::PayloadPktsOut, 1);
                self.write_payload(&mut p.buf[usize::from(p.header_size)..usize::from(p.size)]);
            }
            h_seq_nr = self.m_seq_nr;
        } else {
            // pick up the nagle packet
            p = *self
                .nagle_packet
                .take()
                .unwrap_or_else(|| Box::new(Packet::new(0)));
            h_seq_nr = p.seq_nr();
            debug_assert!(h_seq_nr == self.m_seq_nr);
            let ext = p.buf[1];

            if ext == UtpExtension::Sack as u8 {
                if self.inbuf.is_empty() {
                    remove_sack_header(&mut p);
                    sack = 0;
                } else {
                    sack = i32::from(p.buf[UTP_HEADER_LEN + 1]);
                }
            } else {
                sack = 0;
            }
            // close_reason extension is not updated on nagle packets
            close_reason = 0;

            // signed arithmetic, like upstream's `std::int32_t`
            // `size_left`: when the packet was allocated against a
            // larger MTU than the current `effective_mtu`, the third
            // operand goes negative — the append must be skipped,
            // not wrap to a huge unsigned value
            let size_left = (i32::try_from(p.allocated).unwrap_or(i32::MAX) - i32::from(p.size))
                .min(self.m_write_buffer_size)
                .min(effective_mtu - i32::from(p.size));
            if size_left > 0 {
                let start = usize::from(p.size);
                self.write_payload(&mut p.buf[start..start + size_left as usize]);
                p.size += size_left as u16;
            }

            if self.m_bytes_in_flight > 0
                && i32::from(p.size) < p.allocated.min(effective_mtu as usize) as i32
                && !force
                && self.m_nagle
                && compare_less_wrap(
                    u32::from(self.m_acked_seq_nr),
                    u32::from(self.m_nagle_seq_nr),
                    ACK_MASK as u32,
                )
            {
                self.nagle_packet = Some(Box::new(p));
                return false;
            }
        }

        // write SACK + close-reason extension chain after the header
        let mut ptr = UTP_HEADER_LEN;
        if sack != 0 {
            p.buf[ptr] = if close_reason != 0 {
                UtpExtension::CloseReason as u8
            } else {
                UtpExtension::None as u8
            };
            ptr += 1;
            p.buf[ptr] = sack as u8;
            ptr += 1;
            self.write_sack(&mut p.buf[ptr..ptr + sack as usize]);
            ptr += sack as usize;
        }
        if close_reason != 0 {
            p.buf[ptr] = UtpExtension::None as u8;
            p.buf[ptr + 1] = 4;
            p.buf[ptr + 2..ptr + 6].copy_from_slice(&close_reason.to_be_bytes());
        }

        // nagle hold-off on a fresh packet
        if self.m_bytes_in_flight > 0
            && i32::from(p.size) < p.allocated.min(effective_mtu as usize) as i32
            && !force_flush_nagle
            && !force
            && self.m_nagle
            && compare_less_wrap(
                u32::from(self.m_acked_seq_nr),
                u32::from(self.m_nagle_seq_nr),
                ACK_MASK as u32,
            )
        {
            debug_assert!(self.nagle_packet.is_none());
            debug_assert!(h_seq_nr == self.m_seq_nr);
            self.nagle_packet = Some(Box::new(p));
            return false;
        }

        if (mtu_probe || p.mtu_probe) && i32::from(p.size) >= i32::from(self.m_mtu_floor) {
            p.mtu_probe = true;
            self.m_mtu_seq = self.m_seq_nr;
        } else {
            p.mtu_probe = false;
        }

        let now = Instant::now();
        p.send_time = now;
        p.buf[8..12].copy_from_slice(&self.m_reply_micro.to_be_bytes());
        let wnd = (self.m_receive_buffer_capacity
            - self.m_buffered_incoming_bytes
            - self.m_receive_buffer_size)
            .max(0) as u32;
        p.buf[12..16].copy_from_slice(&wnd.to_be_bytes());
        p.buf[18..20].copy_from_slice(&self.m_ack_nr.to_be_bytes());
        if flags & PKT_FIN != 0 {
            p.buf[0] = ((UtpType::Fin as u8) << 4) | 1;
        }
        p.buf[4..8].copy_from_slice(&micros_now().to_be_bytes());

        self.send_packet(&p.buf[..usize::from(p.size)], p.mtu_probe);
        self.m_out_packets += 1;
        self.inc(UtpStat::PacketsOut, 1);
        p.num_transmissions += 1;
        if self.m_bytes_in_flight == 0 {
            self.m_timeout = now
                + std::time::Duration::from_millis(
                    u64::try_from(self.packet_timeout()).unwrap_or(0),
                );
        }
        if self.m_deferred_ack {
            self.m_deferred_ack = false;
        }

        if p.size > p.header_size {
            if i32::from(p.size) < p.allocated.min(effective_mtu as usize) as i32 {
                self.m_nagle_seq_nr = self.m_seq_nr;
            }
            let new_in_flight = i32::from(p.size - p.header_size);
            self.insert_packet(p);
            self.m_seq_nr = self.m_seq_nr.wrapping_add(1) & ACK_MASK;
            if !self.m_stalled {
                self.m_bytes_in_flight += new_in_flight;
            }
        } else if flags & PKT_FIN != 0 {
            self.insert_packet(p);
        } else if self.m_out_eof
            && self.m_write_buffer_size == 0
            && self.nagle_packet.is_none()
            && self.state == UtpState::Connected
        {
            self.send_fin();
        }

        self.m_write_buffer_size > 0 && !self.m_cwnd_full && !self.m_stalled
    }

    /// `insert_packet` — a replaced packet marked `need_resend` must
    /// be unlinked from the resend queue, else its (already replaced)
    /// seq would spuriously resend the *new* packet.
    fn insert_packet(&mut self, p: Packet) {
        if let Some(old) = self.outbuf.insert(u32::from(self.m_seq_nr), p) {
            if old.need_resend {
                if let Some(pos) = self.needs_resend.iter().position(|&s| s == old.seq_nr()) {
                    self.needs_resend.remove(pos);
                }
            } else {
                self.m_bytes_in_flight -= i32::from(old.size - old.header_size);
            }
        }
    }

    /// `resend_packet` by seq number.
    fn resend_packet_seq(&mut self, seq: u16, fast_resend: bool) -> bool {
        if self.m_error.is_some() || self.m_stalled {
            return false;
        }

        // mtu-probe loss bookkeeping
        if (self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK) == self.m_mtu_seq && self.m_mtu_seq != 0
        {
            self.m_mtu_seq = 0;
            if let Some(p) = self.outbuf.at_mut(u32::from(seq)) {
                p.mtu_probe = false;
                self.m_mtu_ceiling = p.size.saturating_sub(1);
            }
            self.update_mtu_limits();
        }

        let window_size_left = ((self.m_cwnd >> 16).min(i64::from(self.m_adv_wnd))
            - i64::from(self.m_bytes_in_flight)) as i32;
        let (size, header_size, need_resend) = match self.outbuf.at(u32::from(seq)) {
            Some(p) => (i32::from(p.size), i32::from(p.header_size), p.need_resend),
            None => return false,
        };
        if !fast_resend && size - header_size > window_size_left && self.m_bytes_in_flight > 0 {
            self.m_cwnd_full = true;
            return false;
        }
        if need_resend {
            self.m_bytes_in_flight += size - header_size;
        }

        self.inc(UtpStat::PacketResend, 1);
        if fast_resend {
            self.inc(UtpStat::FastRetransmit, 1);
        }

        let now = Instant::now();
        // pre-compute the refreshed SACK payload before taking a
        // mutable borrow of `outbuf` (`write_sack` reads `inbuf`)
        let sack_refresh = match self.outbuf.at(u32::from(seq)) {
            Some(p)
                if p.buf[1] == UtpExtension::Sack as u8
                    && u16::from_be_bytes([p.buf[18], p.buf[19]]) != self.m_ack_nr
                    && !self.inbuf.is_empty() =>
            {
                let sack_size = usize::from(p.buf[UTP_HEADER_LEN + 1]);
                let mut tmp = vec![0u8; sack_size];
                self.write_sack(&mut tmp);
                Some(tmp)
            }
            _ => None,
        };
        let mut send_bytes = Vec::new();
        {
            let p = match self.outbuf.at_mut(u32::from(seq)) {
                Some(p) => p,
                None => return false,
            };
            p.need_resend = false;
            p.buf[8..12].copy_from_slice(&self.m_reply_micro.to_be_bytes());
            p.send_time = now;
            p.buf[4..8].copy_from_slice(&micros_now().to_be_bytes());

            // refresh SACK extension
            if p.buf[1] == UtpExtension::Sack as u8 {
                let ack = u16::from_be_bytes([p.buf[18], p.buf[19]]);
                if ack != self.m_ack_nr {
                    if let Some(tmp) = &sack_refresh {
                        p.buf[UTP_HEADER_LEN + 2..UTP_HEADER_LEN + 2 + tmp.len()]
                            .copy_from_slice(tmp);
                    } else {
                        remove_sack_header(p);
                    }
                }
            }
            p.buf[18..20].copy_from_slice(&self.m_ack_nr.to_be_bytes());
            p.num_transmissions += 1;
            send_bytes.extend_from_slice(&p.buf[..usize::from(p.size)]);
        }
        self.send_packet(&send_bytes, false);
        self.m_out_packets += 1;
        self.inc(UtpStat::PacketsOut, 1);
        // upstream erases the packet's `m_needs_resend` link inside
        // `resend_packet` — without this a timeout-driven resend
        // leaves `seq` queued and `send_pkt` sends it a second time
        if need_resend {
            if let Some(pos) = self.needs_resend.iter().position(|&s| s == seq) {
                self.needs_resend.remove(pos);
            }
        }
        true
    }

    /// `experienced_loss`.
    fn experienced_loss(&mut self, seq_nr: u16, now: Instant) {
        self.inc(UtpStat::PacketLoss, 1);
        if compare_less_wrap(
            u32::from(seq_nr),
            u32::from(self.m_loss_seq_nr) + 1,
            ACK_MASK as u32,
        ) {
            return;
        }
        if self.m_next_loss >= now {
            return;
        }
        self.m_next_loss = now
            + std::time::Duration::from_millis(
                u64::try_from(self.cfg.cwnd_reduce_timer).unwrap_or(0),
            );
        self.m_cwnd =
            (self.m_cwnd * self.cfg.loss_multiplier / 100).max(i64::from(self.m_mtu) << 16);
        self.m_loss_seq_nr = self.m_seq_nr;
        if self.m_slow_start {
            self.m_ssthres = (self.m_cwnd >> 16) as i32;
            self.m_slow_start = false;
        }
    }

    /// `maybe_inc_acked_seq_nr`.
    fn maybe_inc_acked_seq_nr(&mut self) {
        let mut incremented = false;
        let end = if self.state == UtpState::FinSent {
            self.m_seq_nr.wrapping_add(1) & ACK_MASK
        } else {
            self.m_seq_nr
        };
        while (self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK) != end
            && self
                .outbuf
                .at(u32::from(self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK))
                .is_none()
        {
            if self.m_fast_resend_seq_nr == self.m_acked_seq_nr {
                self.m_fast_resend_seq_nr = self.m_fast_resend_seq_nr.wrapping_add(1) & ACK_MASK;
            }
            self.m_acked_seq_nr = self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK;
            incremented = true;
        }
        if !incremented {
            return;
        }
        if compare_less_wrap(
            u32::from(self.m_loss_seq_nr),
            u32::from(self.m_acked_seq_nr),
            ACK_MASK as u32,
        ) {
            self.m_loss_seq_nr = self.m_acked_seq_nr;
        }
        self.m_duplicate_acks = 0;
    }

    /// `ack_packet` — returns RTT (µs).
    fn ack_packet(&mut self, seq_nr: u16, receive_time: Instant) -> u32 {
        let p = match self.outbuf.remove(u32::from(seq_nr)) {
            Some(p) => p,
            None => return u32::MAX,
        };
        if p.need_resend {
            if let Some(pos) = self.needs_resend.iter().position(|&s| s == seq_nr) {
                self.needs_resend.remove(pos);
            }
        } else {
            self.m_bytes_in_flight -= i32::from(p.size - p.header_size);
        }
        if seq_nr == self.m_mtu_seq && self.m_mtu_seq != 0 {
            self.m_mtu_floor = self.m_mtu_floor.max(p.size);
            self.update_mtu_limits();
        }
        self.maybe_inc_acked_seq_nr();

        let rtt = if receive_time >= p.send_time {
            u32::try_from(receive_time.duration_since(p.send_time).as_micros()).unwrap_or(u32::MAX)
        } else {
            100_000 // non-monotonic clock fallback
        };
        self.m_rtt.add_sample((rtt / 1000) as i32);
        rtt
    }

    /// `parse_sack` — returns `(min_rtt, acked_bytes)`.
    fn parse_sack(&mut self, packet_ack: u16, sack: &[u8], now: Instant) -> (u32, i32) {
        if sack.is_empty() {
            return (0, 0);
        }
        let mut ack_nr = packet_ack.wrapping_add(2) & ACK_MASK;
        let mut resend = [0u16; 5];
        let mut num_to_resend = 0usize;
        let mut acked_bytes = 0i32;
        let mut min_rtt = u32::MAX;

        // implicitly lost
        if !compare_less_wrap(
            u32::from(packet_ack.wrapping_add(1) & ACK_MASK),
            u32::from(self.m_fast_resend_seq_nr),
            ACK_MASK as u32,
        ) {
            resend[num_to_resend] = packet_ack.wrapping_add(1) & ACK_MASK;
            num_to_resend += 1;
        }

        'outer: for &byte in sack {
            let mut mask = 1u8;
            for _ in 0..8 {
                if mask & byte != 0 {
                    if let Some(p) = self.outbuf.at(u32::from(ack_nr)) {
                        acked_bytes += i32::from(p.size - p.header_size);
                        let rtt = self.ack_packet(ack_nr, now);
                        min_rtt = min_rtt.min(rtt);
                    } else {
                        // maybe already acked by a previous SACK
                        self.maybe_inc_acked_seq_nr();
                    }
                } else if !compare_less_wrap(
                    u32::from(ack_nr),
                    u32::from(self.m_fast_resend_seq_nr),
                    ACK_MASK as u32,
                ) && num_to_resend < resend.len()
                {
                    resend[num_to_resend] = ack_nr;
                    num_to_resend += 1;
                }
                mask <<= 1;
                ack_nr = ack_nr.wrapping_add(1) & ACK_MASK;
                if ack_nr == self.m_seq_nr {
                    break 'outer;
                }
            }
        }

        if self.outbuf.is_empty() {
            self.m_duplicate_acks = 0;
        }

        // scan backwards: only resend packets followed by >dup_ack_limit acked ones
        let mut last_resend = packet_ack
            .wrapping_add(1)
            .wrapping_add((sack.len() * 8) as u16)
            & ACK_MASK;
        let mut dups = 0;
        'rev: for i in (0..sack.len()).rev() {
            let bitfield = sack[i];
            let mut mask = 0x80u8;
            for _ in 0..8 {
                if mask & bitfield != 0 {
                    dups += 1;
                }
                if dups > DUP_ACK_LIMIT {
                    break 'rev;
                }
                last_resend = last_resend.wrapping_sub(1) & ACK_MASK;
                mask >>= 1;
            }
        }
        if dups <= DUP_ACK_LIMIT {
            num_to_resend = 0;
        }
        while num_to_resend > 0
            && !compare_less_wrap(
                u32::from(resend[num_to_resend - 1]),
                u32::from(last_resend),
                ACK_MASK as u32,
            )
        {
            num_to_resend -= 1;
        }

        let mut cut_cwnd = true;
        for &pkt_seq in resend.iter().take(num_to_resend) {
            if self.outbuf.at(u32::from(pkt_seq)).is_none() {
                continue;
            }
            if cut_cwnd && (pkt_seq != self.m_mtu_seq || self.m_mtu_seq == 0) {
                self.experienced_loss(pkt_seq, now);
                cut_cwnd = false;
            }
            if self.resend_packet_seq(pkt_seq, true) {
                self.m_duplicate_acks = 0;
                self.m_fast_resend_seq_nr = pkt_seq.wrapping_add(1) & ACK_MASK;
            }
            if self.m_stalled {
                break;
            }
        }
        (min_rtt, acked_bytes)
    }

    /// `consume_incoming_data` — returns true if the packet was
    /// consumed/dropped like upstream's `true` paths.
    fn consume_incoming_data(&mut self, seq_nr: u16, ty: UtpType, payload: &[u8]) -> bool {
        if ty != UtpType::Data {
            return false;
        }
        if self.m_in_eof && self.m_ack_nr == self.m_in_eof_seq_nr {
            return true;
        }
        if self.m_receive_buffer_size
            >= self.m_receive_buffer_capacity - self.m_buffered_incoming_bytes
        {
            return false;
        }

        if seq_nr == (self.m_ack_nr.wrapping_add(1) & ACK_MASK) {
            if self.m_buffered_incoming_bytes + self.m_receive_buffer_size + payload.len() as i32
                > self.m_receive_buffer_capacity
            {
                return true;
            }
            self.incoming(payload);
            self.m_ack_nr = self.m_ack_nr.wrapping_add(1) & ACK_MASK;

            // drain reorder buffer
            loop {
                let next = self.m_ack_nr.wrapping_add(1) & ACK_MASK;
                let p = match self.inbuf.remove(u32::from(next)) {
                    Some(p) => p,
                    None => break,
                };
                let size = p.size - p.header_size;
                self.m_buffered_incoming_bytes -= i32::from(size);
                self.incoming(&p.buf[usize::from(p.header_size)..usize::from(p.size)]);
                self.m_ack_nr = next;
            }
        } else {
            // out of order
            if !compare_less_wrap(u32::from(self.m_ack_nr), u32::from(seq_nr), ACK_MASK as u32)
                || self.inbuf.at(u32::from(seq_nr)).is_some()
            {
                return true;
            }
            if self.m_buffered_incoming_bytes + self.m_receive_buffer_size + payload.len() as i32
                > self.m_receive_buffer_capacity
            {
                return true;
            }
            let mut p = Packet::new(payload.len());
            p.size = payload.len() as u16;
            p.header_size = 0;
            p.buf[..payload.len()].copy_from_slice(payload);
            self.m_buffered_incoming_bytes += i32::from(p.size);
            self.inbuf.insert(u32::from(seq_nr), p);
        }
        false
    }

    /// `incoming` — append payload to the receive buffer.
    fn incoming(&mut self, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        let mut p = Packet::new(payload.len());
        p.size = payload.len() as u16;
        p.header_size = 0;
        p.buf[..payload.len()].copy_from_slice(payload);
        self.m_receive_buffer_size += i32::from(p.size);
        self.receive_buffer.push_back(p);
    }

    /// `update_mtu_limits` — binary search the path MTU.
    fn update_mtu_limits(&mut self) {
        if self.m_mtu_floor > self.m_mtu_ceiling {
            // probe dropped in steady state — restart the search half
            // way down
            self.m_mtu_ceiling = self.m_mtu_floor;
            self.m_mtu_floor =
                ((INET_MIN_MTU - IPV4_HEADER - UDP_HEADER) as u16 + self.m_mtu_ceiling) / 2;
        }
        self.m_mtu = (self.m_mtu_floor + self.m_mtu_ceiling) / 2;
        if (self.m_cwnd >> 16) < i64::from(self.m_mtu) {
            self.m_cwnd = i64::from(self.m_mtu) << 16;
        }
        self.m_mtu_seq = 0;
    }

    /// `packet_timeout` (ms).
    fn packet_timeout(&self) -> i32 {
        if self.state == UtpState::None {
            return 3000;
        }
        if self.m_num_timeouts >= 7 {
            return 60000;
        }
        let mut timeout = self
            .cfg
            .min_timeout
            .max(self.m_rtt.mean() + self.m_rtt.avg_deviation() * 2);
        if self.m_num_timeouts > 0 {
            timeout += (1 << (i32::from(self.m_num_timeouts) - 1)) * 1000;
        }
        timeout.min(60000)
    }

    /// `do_ledbat` — LEDBAT congestion control.
    fn do_ledbat(&mut self, acked_bytes: i32, delay: i32, in_flight: i32) {
        if in_flight <= 0 || acked_bytes <= 0 {
            return;
        }
        // upstream `max(1, m_sm.target_delay())` — the getter
        // already multiplies the setting by 1000, so the clamp
        // applies to the *µs* value; wrapping matches upstream int32
        // arithmetic on absurd settings without panicking
        let target_delay = self.cfg.target_delay_ms.wrapping_mul(1000).max(1);
        let cwnd_saturated = self.m_bytes_in_flight + acked_bytes + i32::from(self.m_mtu)
            > (self.m_cwnd >> 16) as i32;

        let window_factor = (i64::from(acked_bytes) << 16) / i64::from(in_flight);
        let delay_factor =
            (i64::from(target_delay.wrapping_sub(delay)) << 16) / i64::from(target_delay);
        let scaled_gain: i64;

        if delay >= target_delay {
            if self.m_slow_start {
                self.m_ssthres = ((self.m_cwnd >> 16) / 2) as i32;
                self.m_slow_start = false;
            }
            self.inc(UtpStat::SamplesAboveTarget, 1);
        } else {
            self.inc(UtpStat::SamplesBelowTarget, 1);
        }

        let linear_gain = ((window_factor * delay_factor) >> 16) * i64::from(self.cfg.gain_factor);

        if cwnd_saturated {
            let exponential_gain = i64::from(acked_bytes) << 16;
            if self.m_slow_start {
                if self.m_ssthres != 0
                    && ((self.m_cwnd + exponential_gain) >> 16) > i64::from(self.m_ssthres)
                {
                    self.m_slow_start = false;
                    scaled_gain = linear_gain;
                } else {
                    scaled_gain = exponential_gain.max(linear_gain);
                }
            } else {
                scaled_gain = linear_gain;
            }
        } else {
            scaled_gain = 0;
        }

        let scaled_gain = scaled_gain.min(i64::MAX - self.m_cwnd - 1);

        if (self.m_cwnd + scaled_gain) >> 16 < i64::from(self.m_mtu) {
            self.m_cwnd = i64::from(self.m_mtu) << 16;
        } else {
            self.m_cwnd += scaled_gain;
        }

        let window_size_left = (self.m_cwnd >> 16).min(i64::from(self.m_adv_wnd))
            - i64::from(in_flight)
            + i64::from(acked_bytes);
        if window_size_left >= i64::from(self.m_mtu) {
            self.m_cwnd_full = false;
        }
    }

    /// `incoming_packet` — the receive-side state machine. `buf` is
    /// one whole datagram. Returns false when the packet should be
    /// treated as *not* a uTP packet for this socket (upstream's
    /// `return false` paths — e.g. a SYN with the wrong conn-id while
    /// listening).
    pub fn incoming_packet(&mut self, buf: &[u8], ep: SocketAddr, receive_time: Instant) -> bool {
        self.inc(UtpStat::PacketsIn, 1);
        // parse rejects len < 20, version != 1 and type > ST_SYN —
        // upstream counts each as `utp_invalid_pkts_in`.
        let ph = match UtpHeader::parse(buf) {
            Some(h) => h,
            None => {
                self.inc(UtpStat::InvalidPktsIn, 1);
                return false;
            }
        };

        if ph.ty != UtpType::Syn && ph.connection_id != self.m_recv_id {
            self.inc(UtpStat::InvalidPktsIn, 1);
            return false;
        }

        if self.state == UtpState::None && ph.ty == UtpType::Syn {
            self.remote = ep;
        }
        if self.state != UtpState::None && ph.ty == UtpType::Syn {
            self.inc(UtpStat::InvalidPktsIn, 1);
            return true;
        }

        let mut step = false;
        if receive_time.duration_since(self.m_last_history_step)
            > std::time::Duration::from_secs(60)
        {
            step = true;
            self.m_last_history_step = receive_time;
        }

        let mut their_delay = 0u32;
        if ph.timestamp_us != 0 {
            let timestamp = micros_now();
            self.m_reply_micro = timestamp.wrapping_sub(ph.timestamp_us);
            let prev_base = if self.m_their_delay_hist.initialized() {
                self.m_their_delay_hist.base()
            } else {
                0
            };
            their_delay = self.m_their_delay_hist.add_sample(self.m_reply_micro, step);
            let base_change = self.m_their_delay_hist.base() as i64 - i64::from(prev_base);
            if prev_base != 0
                && base_change < 0
                && base_change > -10000
                && self.m_delay_hist.initialized()
            {
                self.m_delay_hist.adjust_base((-base_change) as i32);
            }
        }

        let state_or_fin = ph.ty == UtpType::State || ph.ty == UtpType::Fin;

        let cmp_seq_nr = if matches!(
            self.state,
            UtpState::SynSent | UtpState::FinSent | UtpState::Deleting
        ) && state_or_fin
        {
            self.m_seq_nr
        } else {
            self.m_seq_nr.wrapping_sub(1) & ACK_MASK
        };

        if (self.state != UtpState::None || ph.ty != UtpType::Syn)
            && (compare_less_wrap(u32::from(cmp_seq_nr), u32::from(ph.ack_nr), ACK_MASK as u32)
                || compare_less_wrap(
                    u32::from(ph.ack_nr),
                    u32::from(self.m_acked_seq_nr.wrapping_sub(DUP_ACK_LIMIT as u16)),
                    ACK_MASK as u32,
                ))
        {
            self.inc(UtpStat::RedundantPktsIn, 1);
            return true;
        }

        if self.m_in_eof
            && compare_less_wrap(
                u32::from(self.m_in_eof_seq_nr),
                u32::from(ph.seq_nr),
                ACK_MASK as u32,
            )
            && !(self.m_in_eof_seq_nr == ph.seq_nr && state_or_fin)
        {
            return true;
        }

        if ph.ty == UtpType::Data {
            self.inc(UtpStat::PayloadPktsIn, 1);
        }

        let max_packets_reorder =
            u32::try_from(16.max(self.m_receive_buffer_capacity / 1100)).unwrap_or(16);
        if self.state != UtpState::None
            && self.state != UtpState::SynSent
            && compare_less_wrap(
                u32::from(self.m_ack_nr.wrapping_add(max_packets_reorder as u16) & ACK_MASK),
                u32::from(ph.seq_nr),
                ACK_MASK as u32,
            )
        {
            self.inc(UtpStat::RedundantPktsIn, 1);
            return true;
        }

        if ph.ty == UtpType::Reset {
            if compare_less_wrap(u32::from(cmp_seq_nr), u32::from(ph.ack_nr), ACK_MASK as u32) {
                return true;
            }
            self.m_error = Some(UtpError::ConnectionReset);
            self.set_state(UtpState::ErrorWait);
            self.test_socket_state();
            return true;
        }

        self.m_in_packets += 1;

        let sample = if ph.delay_us == i32::MAX as u32 {
            0
        } else {
            ph.delay_us
        };

        if sample != 0 {
            let d = self.m_delay_hist.add_sample(sample, step);
            self.m_delay_sample_hist[self.m_delay_sample_idx] = d;
            self.m_delay_sample_idx = (self.m_delay_sample_idx + 1) % 3;
        }

        let mut acked_bytes = 0i32;
        let prev_bytes_in_flight = self.m_bytes_in_flight;
        self.m_adv_wnd = ph.wnd_size;

        if ph.ack_nr == self.m_acked_seq_nr && !self.outbuf.is_empty() && ph.ty == UtpType::State {
            self.m_duplicate_acks += 1;
        }

        let mut min_rtt = u32::MAX;

        // process cumulative ACK
        if self.state != UtpState::None
            && compare_less_wrap(
                u32::from(self.m_acked_seq_nr),
                u32::from(ph.ack_nr),
                ACK_MASK as u32,
            )
        {
            let next_ack_nr = ph.ack_nr;
            let mut ack_nr = self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK;
            while ack_nr != (next_ack_nr.wrapping_add(1) & ACK_MASK) {
                if self.m_fast_resend_seq_nr == ack_nr {
                    self.m_fast_resend_seq_nr =
                        self.m_fast_resend_seq_nr.wrapping_add(1) & ACK_MASK;
                }
                if let Some(p) = self.outbuf.at(u32::from(ack_nr)) {
                    acked_bytes += i32::from(p.size - p.header_size);
                    let rtt = self.ack_packet(ack_nr, receive_time);
                    min_rtt = min_rtt.min(rtt);
                }
                ack_nr = ack_nr.wrapping_add(1) & ACK_MASK;
            }
            self.maybe_inc_acked_seq_nr();
            if self.outbuf.is_empty() {
                self.m_duplicate_acks = 0;
            }
        }

        // extension chain
        let mut header_size = UTP_HEADER_LEN;
        let mut extension = ph.extension;
        while extension != 0 {
            if header_size + 2 > buf.len() {
                self.inc(UtpStat::InvalidPktsIn, 1);
                return true;
            }
            let next_extension = buf[header_size];
            let len = usize::from(buf[header_size + 1]);
            header_size += 2;
            if header_size + len > buf.len() {
                self.inc(UtpStat::InvalidPktsIn, 1);
                return true;
            }
            match extension {
                x if x == UtpExtension::Sack as u8 => {
                    // upstream assigns (`std::tie(rtt, acked_bytes) =
                    // parse_sack`) — a second SACK overwrites
                    let (rtt, ab) = self.parse_sack(
                        ph.ack_nr,
                        &buf[header_size..header_size + len],
                        receive_time,
                    );
                    min_rtt = min_rtt.min(rtt);
                    acked_bytes = ab;
                }
                x if x == UtpExtension::CloseReason as u8 => {
                    self.parse_close_reason(&buf[header_size..header_size + len]);
                }
                _ => {}
            }
            header_size += len;
            extension = next_extension;
        }

        self.m_num_timeouts = 0;
        self.m_timeout = receive_time
            + std::time::Duration::from_millis(u64::try_from(self.packet_timeout()).unwrap_or(0));

        if self.m_duplicate_acks >= DUP_ACK_LIMIT
            && (self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK) == self.m_fast_resend_seq_nr
        {
            // fast resend — upstream increments
            // `m_fast_resend_seq_nr` *before* calling
            // `experienced_loss`/`resend_packet`, passing the
            // already-incremented value to the loss-rate guard
            let seq = self.m_fast_resend_seq_nr;
            self.m_fast_resend_seq_nr = self.m_fast_resend_seq_nr.wrapping_add(1) & ACK_MASK;
            if let Some(p) = self.outbuf.at(u32::from(seq)) {
                let is_probe = p.mtu_probe;
                if !is_probe {
                    self.experienced_loss(self.m_fast_resend_seq_nr, receive_time);
                }
                self.resend_packet_seq(seq, true);
            }
        }

        let payload_size = buf.len() - header_size;

        if ph.ty == UtpType::Fin {
            if ph.seq_nr == (self.m_ack_nr.wrapping_add(1) & ACK_MASK) || ph.seq_nr == self.m_ack_nr
            {
                self.m_ack_nr = ph.seq_nr;
            }
            if !self.m_in_eof {
                self.m_in_eof = true;
                self.m_in_eof_seq_nr = if payload_size > 0 {
                    ph.seq_nr.wrapping_add(1) & ACK_MASK
                } else {
                    ph.seq_nr
                };
            }
            self.m_deferred_ack = true;
            if self.m_ack_nr == self.m_in_eof_seq_nr && self.receive_buffer.is_empty() {
                // everything up to FIN delivered
            }
            return true;
        }

        if matches!(self.state, UtpState::ErrorWait | UtpState::Deleting) {
            return true;
        }

        let payload = &buf[header_size..];

        match self.state {
            UtpState::None => {
                if ph.ty == UtpType::Syn {
                    self.set_state(UtpState::Connected);
                    self.remote = ep;
                    self.m_ack_nr = ph.seq_nr;
                    self.m_seq_nr = random(0xffff) as u16;
                    self.m_acked_seq_nr = self.m_seq_nr.wrapping_sub(1) & ACK_MASK;
                    self.m_loss_seq_nr = self.m_acked_seq_nr;
                    self.m_fast_resend_seq_nr = self.m_seq_nr;
                    if self.m_send_id != ph.connection_id {
                        return false;
                    }
                    self.m_deferred_ack = true;
                }
            }
            UtpState::SynSent | UtpState::Connected => {
                if self.state == UtpState::SynSent {
                    if ph.ack_nr != self.m_seq_nr.wrapping_sub(1) & ACK_MASK {
                        return true;
                    }
                    self.set_state(UtpState::Connected);
                    if ph.ty == UtpType::Data {
                        self.m_ack_nr = ph.seq_nr;
                    } else {
                        self.m_ack_nr = ph.seq_nr.wrapping_sub(1) & ACK_MASK;
                    }
                }

                if sample != 0 && acked_bytes != 0 && prev_bytes_in_flight != 0 {
                    let delay_min = *self.m_delay_sample_hist.iter().min().unwrap_or(&0);
                    let delay = delay_min.min(min_rtt);
                    self.do_ledbat(acked_bytes, delay as i32, prev_bytes_in_flight);
                    self.m_send_delay = delay as i32;
                }
                self.m_recv_delay = their_delay.min(min_rtt) as i32;

                self.consume_incoming_data(ph.seq_nr, ph.ty, payload);

                let has_ack = matches!(ph.ty, UtpType::Data | UtpType::Fin | UtpType::Syn);
                let prev_out = self.m_out_packets;
                if ph.ty != UtpType::Syn {
                    self.m_confirmed = true;
                }
                while self.send_pkt(0) {}
                if has_ack && prev_out == self.m_out_packets {
                    self.m_deferred_ack = true;
                }
                if matches!(self.state, UtpState::ErrorWait | UtpState::Deleting) {
                    return true;
                }
            }
            UtpState::FinSent => {
                if ph.ty == UtpType::Data {
                    self.m_deferred_ack = true;
                }
                if !self.consume_incoming_data(ph.seq_nr, ph.ty, payload)
                    && self.m_acked_seq_nr == self.m_seq_nr
                {
                    self.m_error = Some(UtpError::Eof);
                    self.set_state(UtpState::ErrorWait);
                    self.test_socket_state();
                }
            }
            UtpState::Deleting | UtpState::ErrorWait => {
                self.send_reset(ph.seq_nr);
            }
        }
        true
    }

    /// `parse_close_reason` — upstream forwards it to the stream via
    /// `on_close_reason`; we store it for the caller to poll.
    fn parse_close_reason(&mut self, ptr: &[u8]) {
        if ptr.len() != 4 {
            return;
        }
        self.their_close_reason = u16::from_be_bytes([ptr[2], ptr[3]]);
    }

    /// `test_socket_state` — returns true if the socket transitioned
    /// to `Deleting`.
    fn test_socket_state(&mut self) -> bool {
        if self.m_error.is_none() {
            return false;
        }
        debug_assert!(matches!(
            self.state,
            UtpState::ErrorWait | UtpState::Deleting
        ));
        // upstream cancels handlers and deletes if nothing pending —
        // with our pull API the impl transitions to deleting once the
        // error is observable
        self.set_state(UtpState::Deleting);
        true
    }

    /// `init_mtu` — called with `mtu_for_dest(remote)` when the
    /// socket is created (outgoing or accepted).
    pub fn init_mtu(&mut self, mtu: u16) {
        self.m_mtu_ceiling = mtu;
        // start in the middle of the PMTU search space
        self.m_mtu = ((u32::from(self.m_mtu_ceiling) + u32::from(self.m_mtu_floor)) / 2) as u16;
        if self.m_mtu > self.m_mtu_ceiling {
            self.m_mtu = self.m_mtu_ceiling;
        }
        if self.m_mtu_floor > mtu {
            self.m_mtu_floor = mtu;
        }
        if (self.m_cwnd >> 16) < i64::from(self.m_mtu) {
            self.m_cwnd = i64::from(self.m_mtu) << 16;
        }
    }

    /// `m_sm.restrict_mtu(m_mtu)` hook — `Some(mtu)` once a packet
    /// larger than the floor caused a give-up (upstream pushes it
    /// into the manager's rolling `m_restrict_mtu` window).
    pub fn take_failed_mtu(&mut self) -> Option<u16> {
        self.failed_mtu.take()
    }

    /// `utp_socket_manager::mtu_for_dest` — path-MTU guess for a
    /// destination (Teredo/SOCKS5/v6 adjustments), clamped to the
    /// manager's rolling restrict cap.
    #[must_use]
    pub fn mtu_for_dest(addr: &std::net::IpAddr, socks5: bool, restrict_cap: i32) -> i32 {
        let mut mtu = if crate::ip_helpers::is_teredo(addr) {
            1280
        } else {
            i32::try_from(ETHERNET_MTU).unwrap_or(1500)
        };
        mtu -= UDP_HEADER as i32;
        if socks5 {
            // IP layer (assume the proxy runs over IPv4) + SOCKS layer
            mtu -= IPV4_HEADER as i32 + 6 + if addr.is_ipv4() { 4 } else { 16 };
        } else {
            mtu -= if addr.is_ipv4() { IPV4_HEADER } else { 40 } as i32;
        }
        mtu.min(restrict_cap)
    }

    /// `send_deferred_ack` — flush a queued pure-ACK.
    pub fn send_deferred_ack(&mut self) {
        if !self.m_deferred_ack {
            return;
        }
        self.m_deferred_ack = false;
        self.send_pkt(PKT_ACK);
    }

    /// `tick(now)` — timeout handling, loss marking, retransmission.
    pub fn tick(&mut self, now: Instant) {
        if matches!(self.state, UtpState::ErrorWait | UtpState::Deleting) {
            return;
        }
        // flush a deferred ack even when not timed out
        if self.m_deferred_ack {
            self.send_deferred_ack();
        }
        if now <= self.m_timeout {
            return;
        }

        // TIMEOUT
        let mut ignore_loss = false;
        if (self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK) == self.m_mtu_seq
            && (self.m_seq_nr.wrapping_sub(1) & ACK_MASK) == self.m_mtu_seq
            && self.m_mtu_seq != 0
        {
            self.m_mtu_ceiling = self.m_mtu.saturating_sub(1);
            self.update_mtu_limits();
            ignore_loss = true;
        }

        if !self.outbuf.is_empty() || self.m_close_reason != 0 {
            if !ignore_loss {
                self.m_num_timeouts = self.m_num_timeouts.saturating_add(1);
            }
            self.inc(UtpStat::Timeout, 1);
        }

        if u32::from(self.m_num_timeouts) > self.cfg.num_resends
            || (self.m_num_timeouts > 0 && !self.m_confirmed)
        {
            self.m_error = Some(UtpError::TimedOut);
            self.set_state(UtpState::ErrorWait);
            self.test_socket_state();
            return;
        }

        if !ignore_loss {
            if self.m_bytes_in_flight == 0 && (self.m_cwnd >> 16) >= i64::from(self.m_mtu) {
                self.m_cwnd = (self.m_cwnd * 2 / 3).max(i64::from(self.m_mtu) << 16);
            } else {
                self.m_cwnd = i64::from(self.m_mtu) << 16;
            }
            self.m_timeout = now
                + std::time::Duration::from_millis(
                    u64::try_from(self.packet_timeout()).unwrap_or(0),
                );
            self.m_loss_seq_nr = self.m_seq_nr;
            self.m_slow_start = true;
        }
        self.m_mtu_seq = 0;

        // mark all outstanding packets for resend
        let mut i = self.m_acked_seq_nr;
        while i != (self.m_seq_nr.wrapping_add(1) & ACK_MASK) {
            if let Some(p) = self.outbuf.at_mut(u32::from(i)) {
                if !p.need_resend {
                    p.need_resend = true;
                    if i != self.m_acked_seq_nr && i != self.m_seq_nr {
                        self.needs_resend.push_back(i);
                    }
                    self.m_bytes_in_flight -= i32::from(p.size - p.header_size);
                }
            }
            i = i.wrapping_add(1) & ACK_MASK;
        }

        let next = self.m_acked_seq_nr.wrapping_add(1) & ACK_MASK;
        if let Some(p) = self.outbuf.at(u32::from(next)) {
            let n = p.num_transmissions;
            if n >= self.cfg.num_resends
                || (self.state == UtpState::SynSent && n >= self.cfg.syn_resends)
                || (self.state == UtpState::FinSent && n >= self.cfg.fin_resends)
            {
                // the packet that killed the conn was bigger than the
                // floor — probably a dead MTU probe; tell the manager
                // to be more conservative (`m_sm.restrict_mtu`)
                if i32::from(p.size) > i32::from(self.m_mtu_floor) {
                    self.failed_mtu = Some(self.m_mtu);
                }
                self.m_error = Some(UtpError::TimedOut);
                self.set_state(UtpState::ErrorWait);
                self.test_socket_state();
                return;
            }
            if self.m_fast_resend_seq_nr == next {
                self.m_fast_resend_seq_nr = self.m_fast_resend_seq_nr.wrapping_add(1) & ACK_MASK;
            }
            self.resend_packet_seq(next, false);
        } else if self.state < UtpState::FinSent {
            self.send_pkt(0);
        } else if self.state == UtpState::FinSent {
            self.m_error = Some(UtpError::Eof);
            self.set_state(UtpState::ErrorWait);
            self.test_socket_state();
        }
    }
}

impl Drop for UtpSocket {
    /// `~utp_socket_impl` — releases the socket-state gauge.
    /// Upstream also cancels a deferred-ack registration; the flag
    /// lives on the socket itself here, so there is nothing to undo.
    fn drop(&mut self) {
        self.inc(UtpStat::StateGauge(self.state as usize), -1);
    }
}

const PKT_ACK: u8 = 1;
const PKT_FIN: u8 = 2;

/// `remove_sack_header` — drop a stale SACK extension in place.
/// The SACK's `next` byte becomes the header extension id upstream
/// (`h->extension = ptr[0]`): a chained close-reason survives.
fn remove_sack_header(p: &mut Packet) {
    let next_ext = p.buf[UTP_HEADER_LEN];
    let sack_size = usize::from(p.buf[UTP_HEADER_LEN + 1]) + 2;
    let hdr = usize::from(p.header_size);
    p.buf.copy_within(hdr..usize::from(p.size), UTP_HEADER_LEN);
    p.size -= sack_size as u16;
    p.header_size -= sack_size as u16;
    p.buf[1] = next_ext;
}

fn micros_now() -> u32 {
    // upstream uses `clock_type` (a monotonic clock) for the
    // timestamp fields; a wall clock would corrupt the delay
    // samples when NTP steps it.
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    (start.elapsed().as_micros() as u64 & 0xffff_ffff) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A05/R13 — a Nagle packet allocated against the probe MTU is
    /// picked up when the current `effective_mtu` (`m_mtu_floor`, a
    /// probe is already in flight) is *smaller* than `p.size`:
    /// upstream's signed `size_left` goes negative and the append is
    /// skipped — the old `usize` arithmetic underflowed (debug
    /// panic, release wraparound → bogus payload splice).
    #[test]
    fn nagle_pickup_below_effective_mtu_skips_append() {
        let mut s = UtpSocket::new(1, 2, UtpConfig::default());
        s.remote = "127.0.0.1:6881".parse().unwrap();
        s.set_state(UtpState::Connected);
        s.m_adv_wnd = 1_000_000;
        s.m_mtu_seq = 7; // probe in flight → effective_mtu = m_mtu_floor
        s.m_bytes_in_flight = 100;
        // queued payload waiting to be appended
        s.write_buffer.extend(std::iter::repeat_n(0xABu8, 500));
        s.m_write_buffer_size = 500;
        // the held Nagle packet: allocated at probe size (~1010),
        // already larger than the floor (548)
        let mut p = Packet::new(1010);
        p.size = 20 + 700;
        p.header_size = 20;
        p.buf[0] = 1; // ST_DATA v1
        p.buf[16..18].copy_from_slice(&42u16.to_be_bytes());
        s.m_seq_nr = 42;
        s.nagle_packet = Some(Box::new(p));

        // must not panic and must not append — the packet leaves
        // with its original size, buffered data stays queued
        let _ = s.send_pkt(0);
        let sent = s.pop_outbox().unwrap();
        assert_eq!(sent.bytes.len(), 720);
        assert_eq!(s.m_write_buffer_size, 500);
    }
}
