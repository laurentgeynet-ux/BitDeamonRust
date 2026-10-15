//! Port of the uTP wire layer from `aux_/utp_stream.hpp`: the 20-byte
//! `utp_header`, packet/extension types, and the socket-manager demux
//! shape (`utp_socket_manager` routes datagrams by `connection_id`
//! receive id).

use std::collections::HashMap;
use std::net::SocketAddr;

use bitdaemon_common::random::random;

/// `utp_socket_state_t` — first nibble of `type_ver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpType {
    /// `ST_DATA`.
    Data = 0,
    /// `ST_FIN`.
    Fin = 1,
    /// `ST_STATE` — pure ACK.
    State = 2,
    /// `ST_RESET`.
    Reset = 3,
    /// `ST_SYN`.
    Syn = 4,
}

impl UtpType {
    /// From the header nibble (`get_type()`).
    #[must_use]
    pub fn from_raw(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Data,
            1 => Self::Fin,
            2 => Self::State,
            3 => Self::Reset,
            4 => Self::Syn,
            _ => return None,
        })
    }
}

/// `utp_extensions_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpExtension {
    /// `utp_no_extension`.
    None = 0,
    /// `utp_sack`.
    Sack = 1,
    /// `utp_close_reason` (id 2 is skipped upstream — deprecated).
    CloseReason = 3,
}

/// `utp_header` — the fixed 20-byte uTP header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtpHeader {
    /// `type_ver` high nibble.
    pub ty: UtpType,
    /// `type_ver` low nibble (must be 1).
    pub version: u8,
    /// `extension` — chained extension id.
    pub extension: u8,
    /// `connection_id` (receive id of the *destination* socket).
    pub connection_id: u16,
    /// `timestamp_microseconds`.
    pub timestamp_us: u32,
    /// `timestamp_difference_microseconds` — LEDBAT delay echo.
    pub delay_us: u32,
    /// `wnd_size`.
    pub wnd_size: u32,
    /// `seq_nr`.
    pub seq_nr: u16,
    /// `ack_nr`.
    pub ack_nr: u16,
}

/// Header length.
pub const UTP_HEADER_LEN: usize = 20;

impl UtpHeader {
    /// Serialize.
    #[must_use]
    pub fn encode(&self) -> [u8; UTP_HEADER_LEN] {
        let mut out = [0u8; UTP_HEADER_LEN];
        out[0] = ((self.ty as u8) << 4) | (self.version & 0x0f);
        out[1] = self.extension;
        out[2..4].copy_from_slice(&self.connection_id.to_be_bytes());
        out[4..8].copy_from_slice(&self.timestamp_us.to_be_bytes());
        out[8..12].copy_from_slice(&self.delay_us.to_be_bytes());
        out[12..16].copy_from_slice(&self.wnd_size.to_be_bytes());
        out[16..18].copy_from_slice(&self.seq_nr.to_be_bytes());
        out[18..20].copy_from_slice(&self.ack_nr.to_be_bytes());
        out
    }

    /// Parse; `None` when the buffer is short or version != 1.
    #[must_use]
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < UTP_HEADER_LEN {
            return None;
        }
        let ty = UtpType::from_raw(buf[0] >> 4)?;
        let version = buf[0] & 0x0f;
        if version != 1 {
            return None;
        }
        Some(Self {
            ty,
            version,
            extension: buf[1],
            connection_id: u16::from_be_bytes([buf[2], buf[3]]),
            timestamp_us: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            delay_us: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            wnd_size: u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]),
            seq_nr: u16::from_be_bytes([buf[16], buf[17]]),
            ack_nr: u16::from_be_bytes([buf[18], buf[19]]),
        })
    }
}

/// Walk the extension chain after the 20-byte header; yields
/// `(ext_type, payload)` pairs (SACK mask, close-reason string, …).
/// Mirrors upstream's `ptr`/`ph->extension` walk.
pub struct UtpExtensions<'a> {
    buf: &'a [u8],
    ext: u8,
    pos: usize,
}

impl<'a> UtpExtensions<'a> {
    /// `buf` is the whole packet, `first_ext` the header's `extension`
    /// byte.
    #[must_use]
    pub fn new(buf: &'a [u8], first_ext: u8) -> Self {
        Self {
            buf,
            ext: first_ext,
            pos: UTP_HEADER_LEN,
        }
    }
}

impl<'a> Iterator for UtpExtensions<'a> {
    type Item = (u8, &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        // each extension: [next_ext u8][len u8][payload]
        if self.ext == 0 || self.pos + 2 > self.buf.len() {
            return None;
        }
        let next = self.buf[self.pos];
        let len = usize::from(self.buf[self.pos + 1]);
        if self.pos + 2 + len > self.buf.len() {
            return None;
        }
        let ty = self.ext;
        self.ext = next;
        let start = self.pos + 2;
        self.pos = start + len;
        Some((ty, &self.buf[start..start + len]))
    }
}

/// `utp_socket_manager` — demux incoming datagrams to uTP sockets.
/// `S` is the per-socket sink (upstream `utp_socket_impl*`). Upstream
/// keys `m_utp_sockets` (a multimap) by `recv_id` alone and lets
/// `match(ep, id)` verify the remote endpoint; folding the endpoint
/// into the key gives the same discrimination with a plain map.
#[derive(Debug)]
pub struct UtpSocketDemux<S> {
    /// `m_utp_sockets` — `(remote, recv_id)` → socket.
    sockets: HashMap<(SocketAddr, u16), S>,
    /// `m_restrict_mtu` — rolling window of path-MTU failures; the
    /// cap is the *max* of the last 3 (upstream semantics: a single
    /// failure doesn't permanently clamp new sockets).
    restrict_mtu: [i32; 3],
    /// `m_mtu_idx`.
    mtu_idx: usize,
}

impl<S> Default for UtpSocketDemux<S> {
    fn default() -> Self {
        Self {
            sockets: HashMap::new(),
            restrict_mtu: [65536; 3],
            mtu_idx: 0,
        }
    }
}

impl<S> UtpSocketDemux<S> {
    /// `new_utp_socket` (outgoing branch) — `send_id` is random,
    /// `recv_id = send_id - 1`; the socket registers under
    /// `(remote, recv_id)`. Returns `(send_id, recv_id)`.
    pub fn new_socket(&mut self, remote: SocketAddr, s: S) -> (u16, u16) {
        let send_id = random(0xffff) as u16;
        let recv_id = send_id.wrapping_sub(1);
        self.sockets.insert((remote, recv_id), s);
        (send_id, recv_id)
    }

    /// `remove_socket(id)` — by the socket's `(remote, recv_id)` key.
    pub fn remove(&mut self, remote: SocketAddr, recv_id: u16) -> Option<S> {
        self.sockets.remove(&(remote, recv_id))
    }

    /// `incoming_packet` — returns `(recv_id, header, payload)` when
    /// the datagram is a valid uTP packet addressed to a known socket
    /// on that exact endpoint (`match(ep, id)` upstream).
    /// `None` means "not for us" (non-uTP traffic shares the UDP
    /// socket — DHT/trackers).
    pub fn route<'a>(&self, from: SocketAddr, buf: &'a [u8]) -> Option<(u16, UtpHeader, &'a [u8])> {
        let h = UtpHeader::parse(buf)?;
        let payload = &buf[UTP_HEADER_LEN..];
        if !self.sockets.contains_key(&(from, h.connection_id)) {
            return None;
        }
        Some((h.connection_id, h, payload))
    }

    /// `num_sockets()`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sockets.len()
    }
    /// empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sockets.is_empty()
    }

    /// `utp_socket_manager::restrict_mtu` — record an MTU that
    /// killed a connection (rotating slot).
    pub fn restrict_mtu(&mut self, mtu: i32) {
        self.restrict_mtu[self.mtu_idx] = mtu;
        self.mtu_idx = (self.mtu_idx + 1) % self.restrict_mtu.len();
    }

    /// `restrict_mtu()` — the current cap (`max` of the window).
    #[must_use]
    pub fn mtu_cap(&self) -> i32 {
        self.restrict_mtu.iter().copied().max().unwrap_or(65536)
    }
}
