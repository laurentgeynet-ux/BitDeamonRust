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
use std::sync::{Arc, OnceLock};
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

/// `torrent::verify_peer_cert` leaf check — the leaf certificate
/// must carry a DNS SAN equal to the torrent name or `"*"`;
/// when no SAN matches, the *most specific* (last) Common Name
/// is tried. Intermediate certs are not name-checked (upstream
/// `depth > 0 → return true`).
fn leaf_name_matches(cert: &rustls::pki_types::CertificateDer<'_>, expected: &str) -> bool {
    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
    let Ok((_, x)) = X509Certificate::from_der(cert.as_ref()) else {
        return false;
    };
    if let Ok(Some(san)) = x.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::DNSName(s) = name
                && (*s == "*" || *s == expected)
            {
                return true;
            }
        }
    }
    let mut cn = None;
    for attr in x.subject().iter_common_name() {
        cn = attr.as_str().ok();
    }
    matches!(cn, Some(s) if s == "*" || s == expected)
}

/// `ssl::set_trust_certificate` + `verify_peer` — the trust anchors
/// of one SSL torrent (the embedded `ssl-cert` root, or
/// `add_torrent_params::root_certificate`).
fn anchors_from_pem(pem: &[u8]) -> io::Result<Vec<rustls::pki_types::TrustAnchor<'static>>> {
    use rustls::pki_types::pem::PemObject;
    let mut out = Vec::new();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(pem).flatten() {
        let ta = webpki::anchor_from_trusted_cert(&cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
            .to_owned();
        out.push(ta);
    }
    if out.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no roots in PEM",
        ));
    }
    Ok(out)
}

/// `CryptoProvider` shared by every BitDaemon-built TLS config.
/// Configs go through `builder_with_provider` with this provider
/// rather than the ambient `ClientConfig::builder()` /
/// `ServerConfig::builder()`: the process-level default panics
/// when several providers are compiled in but none is installed —
/// exactly what a consumer enabling `ring` next to this crate's
/// `aws_lc_rs` triggers — and a library must not install a
/// process-wide provider that would pre-empt the embedding app's
/// own choice.
#[must_use]
pub fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    static PROVIDER: OnceLock<Arc<rustls::crypto::CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .clone()
}

fn sig_algs() -> rustls::crypto::WebPkiSupportedAlgorithms {
    rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms
}

fn cert_chain(
    ee: &rustls::pki_types::CertificateDer<'_>,
    ints: &[rustls::pki_types::CertificateDer<'_>],
    anchors: &[rustls::pki_types::TrustAnchor<'_>],
    now: rustls::pki_types::UnixTime,
    usage: webpki::KeyUsage,
) -> Result<(), rustls::Error> {
    let cert = webpki::EndEntityCert::try_from(ee).map_err(|e| {
        rustls::Error::InvalidCertificate(rustls::CertificateError::Other(rustls::OtherError(
            std::sync::Arc::new(e),
        )))
    })?;
    cert.verify_for_usage(sig_algs().all, anchors, ints, now, usage, None, None)
        .map(|_| ())
        .map_err(|e| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::Other(rustls::OtherError(
                std::sync::Arc::new(e),
            )))
        })
}

/// `m_ssl_ctx` verify callback — **client side** of an SSL torrent:
/// the peer's chain must verify against the torrent's embedded root
/// (`preverified`) and its leaf SAN/CN must match the torrent name
/// or `"*"` (`torrent::verify_peer_cert`).
#[derive(Debug)]
pub struct TorrentPeerVerifier {
    anchors: Vec<rustls::pki_types::TrustAnchor<'static>>,
    expected: String,
}

impl TorrentPeerVerifier {
    /// ctor — `root_pem` is the torrent's embedded `ssl-cert` (or
    /// `add_torrent_params::root_certificate`), `expected` is the
    /// torrent `name` (`m_torrent_file->name()` upstream).
    pub fn new(root_pem: &[u8], expected: &str) -> io::Result<Self> {
        Ok(Self {
            anchors: anchors_from_pem(root_pem)?,
            expected: expected.to_string(),
        })
    }
}

impl rustls::client::danger::ServerCertVerifier for TorrentPeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        cert_chain(
            end_entity,
            intermediates,
            &self.anchors,
            now,
            webpki::KeyUsage::server_auth(),
        )?;
        if !leaf_name_matches(end_entity, &self.expected) {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName,
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, d, &sig_algs())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, d, &sig_algs())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        sig_algs().supported_schemes()
    }
}

/// `m_ssl_ctx` verify callback — **server side**: SSL torrents are
/// mutual-TLS (`verify_fail_if_no_peer_cert` upstream). The peer's
/// chain must verify against a registered torrent's root and its
/// leaf name must match *that* torrent's name (or `"*"`).
/// `registry` is the shared [`SslTorrentRoots`] store so the
/// acceptor picks the same per-torrent trust anchors the SNI
/// dispatch uses.
#[derive(Debug)]
pub struct TorrentClientVerifier {
    registry: Arc<SslTorrentRoots>,
    subjects: Vec<rustls::DistinguishedName>,
}

impl TorrentClientVerifier {
    /// ctor — wraps the shared registry.
    #[must_use]
    pub fn new(registry: Arc<SslTorrentRoots>) -> Self {
        let subjects = registry
            .map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .flat_map(|v| v.subjects.iter().cloned())
            .collect();
        Self { registry, subjects }
    }
}

impl rustls::server::danger::ClientCertVerifier for TorrentClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &self.subjects
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        let map = self.registry.map.lock().unwrap_or_else(|e| e.into_inner());
        for v in map.values() {
            if cert_chain(
                end_entity,
                intermediates,
                &v.anchors,
                now,
                webpki::KeyUsage::client_auth(),
            )
            .is_ok()
                && leaf_name_matches(end_entity, &v.expected)
            {
                return Ok(rustls::server::danger::ClientCertVerified::assertion());
            }
        }
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, d, &sig_algs())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, d, &sig_algs())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        sig_algs().supported_schemes()
    }
}

/// per-torrent trust roots for the acceptor's client-cert verify —
/// `torrent` name → embedded root anchors + expected leaf name.
/// Shared with [`SniResolver`] so `register_ssl_torrent` sets up
/// both the server cert selection and the peer-cert trust anchor.
#[derive(Debug, Default)]
pub struct SslTorrentRoots {
    map: std::sync::Mutex<std::collections::HashMap<String, SslTorrentRoot>>,
}

/// one registered torrent's verify material.
#[derive(Debug)]
struct SslTorrentRoot {
    anchors: Vec<rustls::pki_types::TrustAnchor<'static>>,
    expected: String,
    subjects: Vec<rustls::DistinguishedName>,
}

impl SslTorrentRoots {
    /// new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// `init_ssl` + `set_ssl_certificate` on the acceptor side: register
/// a torrent's `(root anchors, name, serving cert)` so the TLS
/// listener can both present the right cert (SNI) and verify peer
/// certs (mutual TLS).
pub fn register_ssl_torrent(
    resolver: &SniResolver,
    roots: &SslTorrentRoots,
    sni_name: &str,
    torrent_name: &str,
    root_pem: &[u8],
    cert_pem: &str,
    key_pem: &str,
) -> io::Result<()> {
    use rustls::pki_types::pem::PemObject;
    use x509_parser::prelude::{FromDer, X509Certificate};
    let anchors = anchors_from_pem(root_pem)?;
    // root_hint_subjects: DER of each anchor's subject Name.
    let mut subjects = Vec::new();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(root_pem).flatten() {
        if let Ok((_, x)) = X509Certificate::from_der(cert.as_ref()) {
            subjects.push(rustls::DistinguishedName::from(
                x.subject().as_raw().to_vec(),
            ));
        }
    }
    roots.map.lock().unwrap_or_else(|e| e.into_inner()).insert(
        sni_name.to_string(),
        SslTorrentRoot {
            anchors,
            expected: torrent_name.to_string(),
            subjects,
        },
    );
    resolver.register(sni_name, certified_key(cert_pem, key_pem)?);
    Ok(())
}

/// drop a torrent's roots + cert on removal.
pub fn unregister_ssl_torrent(resolver: &SniResolver, roots: &SslTorrentRoots, sni_name: &str) {
    resolver.unregister(sni_name);
    roots
        .map
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(sni_name);
}

/// `torrent::m_ssl_ctx` client config for one SSL torrent:
/// chain-verify the peer cert against the embedded root, name-check
/// the leaf against the torrent name, and present our own
/// cert/key (`set_ssl_certificate`) — SSL torrents are mutual-TLS.
pub fn peer_ssl_client_config(
    root_pem: &[u8],
    torrent_name: &str,
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> io::Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    let verifier = TorrentPeerVerifier::new(root_pem, torrent_name)?;
    let b = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    let cfg = match (client_cert_pem, client_key_pem) {
        (Some(c), Some(k)) => {
            let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(c)
                .collect::<Result<_, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(k)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            b.with_client_auth_cert(certs, key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        }
        _ => b.with_no_client_auth(),
    };
    Ok(Arc::new(cfg))
}

/// peer-facing client config (`m_peer_ssl_ctx`): verify-none,
/// TLS 1.2+, no client auth.
pub fn peer_client_config() -> io::Result<Arc<rustls::ClientConfig>> {
    let cfg = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(VerifyNone))
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// tracker-facing client config (`m_ssl_ctx`): real PKI — system
/// roots where available + `set_trust_certificate` PEMs.
pub fn tracker_client_config(
    roots: Arc<rustls::RootCertStore>,
) -> io::Result<Arc<rustls::ClientConfig>> {
    let cfg = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
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

/// `session_impl::m_peer_ssl_ctx` accept side — a `ServerConfig`
/// from PEM material (`set_ssl_certificate` upstream). `dh` params
/// are OpenSSL DHE-specific; rustls is ECDHE-only, so they are
/// ignored like upstream on LibreSSL builds.
pub fn ssl_server_config(cert_pem: &str, key_pem: &str) -> io::Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::pem::PemObject;
    let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no certificates in PEM",
        ));
    }
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let cfg = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// per-torrent `CertifiedKey` for the SNI resolver — same PEM pair
/// as [`ssl_server_config`].
pub fn certified_key(cert_pem: &str, key_pem: &str) -> io::Result<Arc<rustls::sign::CertifiedKey>> {
    use rustls::pki_types::pem::PemObject;
    let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let signer = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    Ok(Arc::new(rustls::sign::CertifiedKey::new(certs, signer)))
}

/// `ssl_server_name_callback_impl` — per-info-hash cert selection.
/// Keys are the SNI names the announce layer used (the SSL torrent
/// cert name). Returns the `CertifiedKey` for `name`, if known.
pub struct SniResolver {
    map: std::sync::Mutex<std::collections::HashMap<String, Arc<rustls::sign::CertifiedKey>>>,
    fallback: Option<Arc<rustls::sign::CertifiedKey>>,
}

impl std::fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SniResolver")
            .field("names", &self.map.lock().map(|m| m.len()).unwrap_or(0))
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
            map: std::sync::Mutex::new(std::collections::HashMap::new()),
            fallback,
        }
    }

    /// `set_context` registration — `name` is the SNI host (SSL
    /// torrents use the hex info-hash, upstream
    /// `ssl_server_name_callback_impl`).
    pub fn register(&self, name: &str, cert: Arc<rustls::sign::CertifiedKey>) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string(), cert);
    }

    /// remove a torrent's cert when it leaves the session.
    pub fn unregister(&self, name: &str) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
    }
}

impl rustls::server::ResolvesServerCert for SniResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        client_hello
            .server_name()
            .and_then(|n| {
                self.map
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(n)
                    .cloned()
            })
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
        let _ = peer_client_config().expect("peer cfg");
        let roots = std::sync::Arc::new(rustls::RootCertStore::empty());
        let _ = tracker_client_config(roots).expect("tracker cfg");
    }

    /// `torrent::verify_peer_cert` end-to-end: a CA root + leaf
    /// signed by it; the leaf SAN/CN must match the torrent name.
    fn ca_and_leaf(cn_or_san: &str, use_san: bool) -> (String, CertificateDer<'static>) {
        let mut ca_p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_k = rcgen::KeyPair::generate().expect("ca key");
        let ca = ca_p.self_signed(&ca_k).expect("ca cert");

        let mut leaf_p = if use_san {
            rcgen::CertificateParams::new(vec![cn_or_san.to_string()]).expect("leaf params")
        } else {
            let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("leaf params");
            p.distinguished_name
                .push(rcgen::DnType::CommonName, cn_or_san);
            p
        };
        leaf_p.is_ca = rcgen::IsCa::NoCa;
        let leaf_k = rcgen::KeyPair::generate().expect("leaf key");
        let leaf = leaf_p.signed_by(&leaf_k, &ca, &ca_k).expect("leaf cert");
        (ca.pem(), leaf.der().clone())
    }

    #[test]
    fn torrent_verifier_matches_leaf_name() {
        let sni = ServerName::try_from("abcd".to_string()).expect("sni");
        // SAN match
        let (root, leaf) = ca_and_leaf("my-torrent", true);
        let v = TorrentPeerVerifier::new(root.as_bytes(), "my-torrent").expect("verifier");
        let r = v.verify_server_cert(&leaf, &[], &sni, &[], UnixTime::now());
        assert!(r.is_ok(), "SAN match failed: {r:?}");
        // CN fallback (no SAN)
        let (root, leaf) = ca_and_leaf("my-torrent", false);
        let v = TorrentPeerVerifier::new(root.as_bytes(), "my-torrent").expect("verifier");
        let r = v.verify_server_cert(&leaf, &[], &sni, &[], UnixTime::now());
        assert!(r.is_ok(), "CN match failed: {r:?}");
        // wrong torrent name → reject
        let v = TorrentPeerVerifier::new(root.as_bytes(), "other-torrent").expect("verifier");
        assert!(
            v.verify_server_cert(&leaf, &[], &sni, &[], UnixTime::now())
                .is_err()
        );
        // wrong root → chain fails
        let (root2, _) = ca_and_leaf("x", true);
        let v = TorrentPeerVerifier::new(root2.as_bytes(), "my-torrent").expect("verifier");
        assert!(
            v.verify_server_cert(&leaf, &[], &sni, &[], UnixTime::now())
                .is_err()
        );
    }
}
