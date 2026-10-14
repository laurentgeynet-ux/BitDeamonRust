//! Port of `aux_/http_stream.hpp` — HTTP `CONNECT` proxy handshake:
//! `CONNECT host:port HTTP/1.0` + `Host:` + optional
//! `Proxy-Authorization: Basic`, then a byte-at-a-time header scan for
//! `\n\n` or `\r\n\r\n` and a 200 status check. Faithful to upstream's
//! framing (including the byte-wise read loop).

use std::net::SocketAddr;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// HTTP CONNECT failures.
#[derive(Debug, Error)]
pub enum HttpConnectError {
    /// Non-200 / malformed status (upstream `operation_not_supported`).
    #[error("HTTP CONNECT rejected by proxy")]
    Rejected,
    /// transport I/O.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// `aux::base64encode` — RFC 4648 standard alphabet with padding.
#[must_use]
pub fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(*chunk.get(1).unwrap_or(&0));
        let b2 = u32::from(*chunk.get(2).unwrap_or(&0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// `format_host_for_connect` — `host:port`, brackets around IPv6.
fn format_host(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// `http_stream::connected`+`handshake1/2` — perform the HTTP CONNECT
/// exchange on an established stream to the proxy.
///
/// `host` is the original hostname when known (the proxy resolves
/// domains); `target` is the fallback endpoint. `user`/`password` add
/// a `Proxy-Authorization: Basic` header when `user` is non-empty.
pub async fn http_connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    target: SocketAddr,
    host: Option<&str>,
    user: &str,
    password: &str,
) -> Result<(), HttpConnectError> {
    let remote_host = host.map_or_else(|| target.to_string(), |h| format_host(h, target.port()));

    let mut req = format!("CONNECT {remote_host} HTTP/1.0\r\nHost: {remote_host}\r\n");
    if !user.is_empty() {
        req += &format!(
            "Proxy-Authorization: Basic {}\r\n",
            base64_encode(format!("{user}:{password}").as_bytes())
        );
    }
    req += "\r\n";
    stream.write_all(req.as_bytes()).await?;

    // read one byte at a time until the response headers close
    // (\n\n or \r\n\r\n) — same scan as upstream's handshake2
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).await?;
        buf.push(b[0]);
        let n = buf.len();
        let done = n > 2
            && buf[n - 1] == b'\n'
            && (buf[n - 2] == b'\n'
                || (n > 4 && buf[n - 2] == b'\r' && buf[n - 3] == b'\n' && buf[n - 4] == b'\r'));
        if done {
            break;
        }
    }

    // "HTTP/1.x CODE ..." — the code follows the first space; upstream
    // parses it with `parse_decimal` (leading digit prefix, trailing
    // bytes ignored), not a strict numeric parse.
    let status = buf
        .iter()
        .position(|&b| b == b' ')
        .ok_or(HttpConnectError::Rejected)?;
    let digits = &buf[status + 1..];
    let code_len = digits.iter().take_while(|b| b.is_ascii_digit()).count();
    let code = std::str::from_utf8(&digits[..code_len])
        .ok()
        .and_then(|s| s.parse::<u32>().ok());
    if code != Some(200) {
        return Err(HttpConnectError::Rejected);
    }
    Ok(())
}
