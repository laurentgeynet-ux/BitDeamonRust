// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `ssl.cpp` + `ssl_stream.hpp` (peer/tracker side) on
//! rustls. Upstream TLS facts preserved:
//! - `m_peer_ssl_ctx` is `verify_none` — BitTorrent SSL peer certs
//!   are self-signed; identity is the info-hash, not the cert
//!   (SSL torrents check the cert *hash* in `torrent_info`, done at
//!   the peer layer, not the TLS layer).
//! - `no_legacy_tls_versions` → TLS 1.2+ only.
//! - server-side SNI callback picks the torrent's cert context
//!   (`ssl_server_name_callback_impl`) → `SniResolver`.
//! - `set_trust_certificate` → PEM into the root store.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// `ssl::context::tls_client` — verify-none + TLS 1.2+, matching
/// upstream `m_peer_ssl_ctx` (`verify_none` + `no_legacy_tls_versions`).
#[derive(Debug)]
struct VerifyNone;

impl rustls::client::danger::ServerCertVerifier for VerifyNone {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// peer-facing client config (`m_peer_ssl_ctx`): verify-none,
/// TLS 1.2+, no client auth.
#[must_use]
pub fn peer_client_config() -> Arc<rustls::ClientConfig> {
    let cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(VerifyNone))
        .with_no_client_auth();
    Arc::new(cfg)
}

/// tracker-facing client config (`m_ssl_ctx`): real PKI — system
/// roots where available + `set_trust_certificate` PEMs.
#[must_use]
pub fn tracker_client_config(roots: Arc<rustls::RootCertStore>) -> Arc<rustls::ClientConfig> {
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(cfg)
}

/// `set_trust_certificate` — load PEM certs/keys into a root store.
/// Returns how many certs were processed (upstream returns the
/// count or a negative error).
#[must_use]
pub fn set_trust_certificate(roots: &mut rustls::RootCertStore, pem: &[u8]) -> i32 {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut n = 0i32;
    for cert in CertificateDer::pem_slice_iter(pem).flatten() {
        n += i32::from(roots.add(cert).is_ok());
    }
    n
}

/// `ssl_stream` — a `PeerStream` after the TLS handshake. Generic
/// over the transport like upstream's `ssl_stream<tcp::socket>`.
pub struct SslStream<S> {
    io: tokio_rustls::TlsStream<S>,
    /// `get_peer_address` passthrough for logging/alerts.
    pub remote: SocketAddr,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for SslStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for SslStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

impl<S> std::fmt::Debug for SslStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SslStream")
            .field("remote", &self.remote)
            .finish()
    }
}

/// `handshake(client)` — `set_host_name` + `async_connect_handshake`:
/// SNI comes from `host` (the tracker hostname / peer cert name).
pub async fn ssl_connect<S>(
    stream: S,
    host: &str,
    remote: SocketAddr,
    cfg: Arc<rustls::ClientConfig>,
) -> io::Result<SslStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let tls = TlsConnector::from(cfg).connect(name, stream).await?;
    Ok(SslStream {
        io: tls.into(),
        remote,
    })
}

/// `handshake(server)` — `async_accept_handshake`. `cfg` carries the
/// session's `m_peer_ssl_ctx`: TLS 1.2+, no client-cert requirement
/// (upstream doesn't request client certs on peer sockets).
pub async fn ssl_accept<S>(
    stream: S,
    remote: SocketAddr,
    cfg: Arc<rustls::ServerConfig>,
) -> io::Result<SslStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let tls = TlsAcceptor::from(cfg).accept(stream).await?;
    Ok(SslStream {
        io: tls.into(),
        remote,
    })
}

/// `ssl_server_name_callback_impl` — per-info-hash cert selection.
/// Keys are the SNI names the announce layer used (the SSL torrent
/// cert name). Returns the `CertifiedKey` for `name`, if known.
pub struct SniResolver {
    map: std::collections::HashMap<String, Arc<rustls::sign::CertifiedKey>>,
    fallback: Option<Arc<rustls::sign::CertifiedKey>>,
}

impl std::fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SniResolver")
            .field("names", &self.map.len())
            .finish()
    }
}

impl SniResolver {
    /// ctor — `fallback` serves unknown SNIs (upstream rejects with
    /// `SSL_TLSEXT_ERR_ALERT_FATAL` when no torrent ctx matches; a
    /// fallback keeps the listener usable for mixed-mode sessions).
    #[must_use]
    pub fn new(fallback: Option<Arc<rustls::sign::CertifiedKey>>) -> Self {
        Self {
            map: std::collections::HashMap::new(),
            fallback,
        }
    }

    /// `set_context` registration — `name` is the SNI host (SSL
    /// torrents use the cert's CN, tracked per torrent).
    pub fn register(&mut self, name: &str, cert: Arc<rustls::sign::CertifiedKey>) {
        self.map.insert(name.to_string(), cert);
    }

    /// remove a torrent's cert when it leaves the session.
    pub fn unregister(&mut self, name: &str) {
        self.map.remove(name);
    }
}

impl rustls::server::ResolvesServerCert for SniResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        client_hello
            .server_name()
            .and_then(|n| self.map.get(n).cloned())
            .or_else(|| self.fallback.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

    #[test]
    fn verify_none_accepts_anything() {
        let v = VerifyNone;
        let cert = CertificateDer::from(vec![0x30, 0x00]);
        let name = ServerName::try_from("example.test").expect("name");
        let r = v.verify_server_cert(&cert, &[], &name, &[], UnixTime::now());
        assert!(matches!(r, Ok(ServerCertVerified { .. })));
        assert_eq!(r.map(|_| ()), Ok(()));
    }

    #[test]
    fn configs_build() {
        let _ = peer_client_config();
        let roots = std::sync::Arc::new(rustls::RootCertStore::empty());
        let _ = tracker_client_config(roots);
    }
}
