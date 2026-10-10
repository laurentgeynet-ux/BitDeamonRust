//! Port of `socks5_stream.hpp` + `socks5_stream.cpp` — the SOCKS4/5
//! handshake state machine (auth-negotiation → optional user/pass
//! sub-negotiation → CONNECT or UDP ASSOCIATE → bound-address reply).
//!
//! Upstream drives it through asio callbacks; here it's a single
//! `async fn` over any `AsyncRead + AsyncWrite` stream — which keeps
//! the same wire protocol but lets a tunnelled `PeerStream` go through
//! it unchanged.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// `aux::socks_error` — SOCKS protocol failures.
#[derive(Debug, Error)]
pub enum SocksError {
    /// `unsupported_version`.
    #[error("SOCKS version not supported")]
    UnsupportedVersion,
    /// `unsupported_authentication_method`.
    #[error("SOCKS authentication method not supported")]
    UnsupportedAuthMethod,
    /// `unsupported_authentication_version`.
    #[error("SOCKS authentication version not supported")]
    UnsupportedAuthVersion,
    /// `username_required` — proxy picked user/pass, we have none.
    #[error("SOCKS proxy requires a username")]
    UsernameRequired,
    /// `authentication_error`.
    #[error("SOCKS authentication failed")]
    AuthenticationFailed,
    /// `general_failure` / mapped response codes.
    #[error("SOCKS proxy failure: {0}")]
    ProxyFailure(#[source] std::io::Error),
    /// `no_identd` / `identd_error` (SOCKS4).
    #[error("SOCKS4 identd failure: {0}")]
    Identd(u8),
    /// transport I/O.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// SOCKS command (`m_command` upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocksCommand {
    /// `connect` — relay a TCP stream (1).
    Connect,
    /// `udp associate` — relay UDP datagrams (3).
    UdpAssociate,
}

impl SocksCommand {
    fn code(self) -> u8 {
        match self {
            Self::Connect => 1,
            Self::UdpAssociate => 3,
        }
    }
}

/// Where to connect through the proxy — an IP endpoint or a
/// hostname (`m_dst_name`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocksTarget {
    /// Numeric endpoint (`m_remote_endpoint`).
    Addr(SocketAddr),
    /// Hostname + port (`m_dst_name`, < 255 chars).
    Name { name: String, port: u16 },
}

/// `async_connect` + the handshake chain — performs the SOCKS
/// handshake on an already-connected stream.
///
/// `version` is 4 or 5; `user`/`password` are only used with v5
/// (v4 puts `user` in the CONNECT request as the userid).
pub async fn socks_connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    version: u8,
    user: &str,
    password: &str,
    target: &SocksTarget,
    command: SocksCommand,
) -> Result<(), SocksError> {
    match version {
        5 => socks5(stream, user, password, target, command).await,
        4 => socks4(stream, user, target, command).await,
        _ => Err(SocksError::UnsupportedVersion),
    }
}

async fn socks5<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    user: &str,
    password: &str,
    target: &SocksTarget,
    command: SocksCommand,
) -> Result<(), SocksError> {
    // methods advertisement: [5, 1, 0] or [5, 2, 0, 2]
    let mut buf = Vec::with_capacity(4);
    buf.push(5u8);
    if user.is_empty() {
        buf.extend([1, 0]);
    } else {
        buf.extend([2, 0, 2]);
    }
    stream.write_all(&buf).await?;

    // method choice: [version, method]
    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).await?;
    if resp[0] != 5 {
        return Err(SocksError::UnsupportedVersion);
    }
    match resp[1] {
        0 => {}
        2 => {
            // user/pass sub-negotiation: [1, ulen, user, plen, pass]
            if user.is_empty() {
                return Err(SocksError::UsernameRequired);
            }
            let mut auth = Vec::with_capacity(user.len() + password.len() + 3);
            auth.push(1u8);
            auth.push(user.len().min(0xff) as u8);
            auth.extend(user.bytes().take(0xff));
            auth.push(password.len().min(0xff) as u8);
            auth.extend(password.bytes().take(0xff));
            stream.write_all(&auth).await?;
            let mut aresp = [0u8; 2];
            stream.read_exact(&mut aresp).await?;
            if aresp[0] != 1 {
                return Err(SocksError::UnsupportedAuthVersion);
            }
            if aresp[1] != 0 {
                return Err(SocksError::AuthenticationFailed);
            }
        }
        _ => return Err(SocksError::UnsupportedAuthMethod),
    }

    // CONNECT: [5, cmd, 0, atyp, addr, port]
    let mut req = Vec::with_capacity(10);
    req.extend([5, command.code(), 0]);
    match target {
        SocksTarget::Name { name, port } => {
            req.push(3);
            req.push(name.len().min(0xff) as u8);
            req.extend(name.bytes().take(0xff));
            req.extend(port.to_be_bytes());
        }
        SocksTarget::Addr(SocketAddr::V4(a)) => {
            req.push(1);
            req.extend(a.ip().octets());
            req.extend(a.port().to_be_bytes());
        }
        SocksTarget::Addr(SocketAddr::V6(a)) => {
            req.push(4);
            req.extend(a.ip().octets());
            req.extend(a.port().to_be_bytes());
        }
    }
    stream.write_all(&req).await?;

    // reply: [ver, rep, rsv, atyp, bnd.addr(var), bnd.port]
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    if hdr[0] != 5 {
        return Err(SocksError::UnsupportedVersion);
    }
    if hdr[1] != 0 {
        let e = match hdr[1] {
            2 => std::io::ErrorKind::PermissionDenied,
            3 => std::io::ErrorKind::AddrNotAvailable,
            4 => std::io::ErrorKind::HostUnreachable,
            5 => std::io::ErrorKind::ConnectionRefused,
            6 => std::io::ErrorKind::TimedOut,
            8 => std::io::ErrorKind::InvalidInput,
            _ => std::io::ErrorKind::Other,
        };
        return Err(SocksError::ProxyFailure(std::io::Error::new(
            e,
            format!("SOCKS5 reply {}", hdr[1]),
        )));
    }
    // drain the bound address
    match hdr[3] {
        1 => {
            let mut a = [0u8; 4 + 2];
            stream.read_exact(&mut a).await?;
        }
        4 => {
            let mut a = [0u8; 16 + 2];
            stream.read_exact(&mut a).await?;
        }
        3 => {
            let mut l = [0u8; 1];
            stream.read_exact(&mut l).await?;
            let mut a = vec![0u8; usize::from(l[0]) + 2];
            stream.read_exact(&mut a).await?;
        }
        _ => return Err(SocksError::UnsupportedVersion),
    }
    Ok(())
}

async fn socks4<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    user: &str,
    target: &SocksTarget,
    command: SocksCommand,
) -> Result<(), SocksError> {
    let addr = match target {
        SocksTarget::Addr(SocketAddr::V4(a)) => *a,
        // SOCKS4 has no IPv6 / hostname support upstream
        _ => {
            return Err(SocksError::ProxyFailure(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SOCKS4 requires an IPv4 endpoint",
            )));
        }
    };

    let mut req = Vec::with_capacity(user.len() + 9);
    req.push(4u8);
    req.push(command.code());
    req.extend(addr.port().to_be_bytes());
    req.extend(addr.ip().octets());
    req.extend(user.bytes());
    req.push(0);
    stream.write_all(&req).await?;

    let mut resp = [0u8; 8];
    stream.read_exact(&mut resp).await?;
    if resp[0] != 0 {
        return Err(SocksError::UnsupportedVersion);
    }
    match resp[1] {
        90 => Ok(()),
        91 => Err(SocksError::ProxyFailure(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "SOCKS4 request rejected",
        ))),
        r @ (92 | 93) => Err(SocksError::Identd(r)),
        _ => Err(SocksError::ProxyFailure(std::io::Error::new(
            std::io::ErrorKind::Other,
            "SOCKS4 general failure",
        ))),
    }
}

/// Convenience builders for tests/other crates.
#[must_use]
pub fn v4_target(ip: Ipv4Addr, port: u16) -> SocksTarget {
    SocksTarget::Addr(SocketAddr::new(ip.into(), port))
}

/// IPv6 target helper.
#[must_use]
pub fn v6_target(ip: Ipv6Addr, port: u16) -> SocksTarget {
    SocksTarget::Addr(SocketAddr::new(ip.into(), port))
}
