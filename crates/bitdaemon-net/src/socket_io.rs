//! Port of `aux_/socket_io.hpp`/`.cpp` — big-endian wire helpers:
//! integer read/write, address/endpoint byte encoding (6-byte v4,
//! 18-byte v6), and `read_endpoint_list` for compact peer lists in
//! tracker responses and DHT traffic.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bitdaemon_bencode::BdecodeNode;

/// `aux::read_uint16` / `write_uint16` — BE.
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Wrap a byte slice for sequential reads.
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes consumed so far.
    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Bytes remaining.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The unread tail.
    #[must_use]
    pub fn tail(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    /// `read_uint8`.
    pub fn read_u8(&mut self) -> io::Result<u8> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read_u8"))?;
        self.pos += 1;
        Ok(b)
    }

    /// `read_uint16` — big-endian.
    pub fn read_u16(&mut self) -> io::Result<u16> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// `read_uint32`.
    pub fn read_u32(&mut self) -> io::Result<u32> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `read_uint64`.
    pub fn read_u64(&mut self) -> io::Result<u64> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_be_bytes(b.try_into().expect("8 bytes")))
    }

    /// Read `n` bytes.
    pub fn read_bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_bytes"));
        }
        let r = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(r)
    }
}

/// `write_uint8`.
pub fn write_u8(v: u8, out: &mut Vec<u8>) {
    out.push(v);
}

/// `write_uint16` — big-endian.
pub fn write_u16(v: u16, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// `write_uint32`.
pub fn write_u32(v: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// `write_uint64`.
pub fn write_u64(v: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// `write_address` — 4 bytes for v4, 16 for v6.
pub fn write_address(a: IpAddr, out: &mut Vec<u8>) {
    match a {
        IpAddr::V4(v4) => out.extend_from_slice(&v4.octets()),
        IpAddr::V6(v6) => out.extend_from_slice(&v6.octets()),
    }
}

/// `read_v4_address` / `read_v6_address`.
pub fn read_address(cur: &mut Cursor<'_>, v6: bool) -> io::Result<IpAddr> {
    if v6 {
        let b = cur.read_bytes(16)?;
        Ok(IpAddr::V6(Ipv6Addr::from(
            <[u8; 16]>::try_from(b).expect("16 bytes"),
        )))
    } else {
        let b = cur.read_bytes(4)?;
        Ok(IpAddr::V4(Ipv4Addr::from(
            <[u8; 4]>::try_from(b).expect("4 bytes"),
        )))
    }
}

/// `write_endpoint` — address + port.
pub fn write_endpoint(e: SocketAddr, out: &mut Vec<u8>) {
    write_address(e.ip(), out);
    write_u16(e.port(), out);
}

/// `read_v4_endpoint` / `read_v6_endpoint`.
pub fn read_endpoint(cur: &mut Cursor<'_>, v6: bool) -> io::Result<SocketAddr> {
    let addr = read_address(cur, v6)?;
    let port = cur.read_u16()?;
    Ok(match addr {
        IpAddr::V4(a) => SocketAddr::V4(SocketAddrV4::new(a, port)),
        IpAddr::V6(a) => SocketAddr::V6(SocketAddrV6::new(a, port, 0, 0)),
    })
}

/// `address_to_bytes` — 4 or 16 bytes.
#[must_use]
pub fn address_to_bytes(a: IpAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    write_address(a, &mut v);
    v
}

/// `endpoint_to_bytes` — 6 or 18 bytes.
#[must_use]
pub fn endpoint_to_bytes(e: SocketAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(18);
    write_endpoint(e, &mut v);
    v
}

/// `aux::read_endpoint_list<tcp>` — compact peer lists in tracker
/// responses: strings of 6 (v4) or 18 (v6) bytes.
#[must_use]
pub fn read_endpoint_list(n: &BdecodeNode<'_>) -> Vec<SocketAddr> {
    use bitdaemon_bencode::NodeType;
    let mut ret = Vec::new();
    if n.node_type() != NodeType::List {
        return ret;
    }
    for i in 0..n.list_size() {
        let e = n.list_at(i);
        if e.node_type() != NodeType::String {
            return ret;
        }
        let b = e.string_value();
        let mut cur = Cursor::new(b);
        match b.len() {
            6 => {
                if let Ok(ep) = read_endpoint(&mut cur, false) {
                    ret.push(ep);
                }
            }
            18 => {
                if let Ok(ep) = read_endpoint(&mut cur, true) {
                    ret.push(ep);
                }
            }
            _ => continue,
        }
    }
    ret
}

/// `aux::parse_endpoint` — `"ip:port"` / `"[v6]:port"` text form.
pub fn parse_endpoint(s: &str) -> io::Result<SocketAddr> {
    s.parse::<SocketAddr>().map_err(|_| {
        // upstream trims brackets/spaces; SocketAddr already handles
        // "[::1]:6881". Try host:port split for unbracketed v6? Upstream
        // only accepts bracketed v6 too — report invalid.
        io::Error::new(io::ErrorKind::InvalidInput, "invalid endpoint")
    })
}
