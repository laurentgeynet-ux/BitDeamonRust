// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `resolver_interface` — injectable hostname resolution.
//!
//! Under anonymity, DNS MUST NOT leak: the session resolves through
//! whatever `Resolver` is injected (e.g. SOCKS5 remote-DNS, an onion
//! resolver, or a fixed map). `SystemResolver` (tokio's `lookup_host`)
//! is provided for non-anonymous deployments only — upstream's
//! `resolver.cpp` wraps the platform resolver the same way.

use std::fmt::Debug;
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

/// `resolver_flags::cache_only` — answer only from the cache; a miss
/// fails instead of triggering a network lookup (upstream uses this
/// during shutdown paths).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResolveFlags {
    /// `cache_only`.
    pub cache_only: bool,
}

/// `resolver_interface::async_resolve` — resolve `host` (no port) to
/// addresses; the caller appends the port.
pub trait Resolver: Send + Sync + Debug + 'static {
    /// `async_resolve(host, flags)` → addresses. `host` may be a
    /// dotted IP (parsed directly, no lookup).
    fn resolve(
        &self,
        host: &str,
        flags: ResolveFlags,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<IpAddr>>> + Send + '_>>;
}

/// Shared handle.
pub type BoxedResolver = Arc<dyn Resolver>;

/// `resolve` convenience — host may be `ip` or `ip:port` or `name`.
/// When `host` parses as an IP literal it is returned directly
/// (upstream short-circuits the same way before touching DNS).
pub async fn resolve_host(
    r: &dyn Resolver,
    host: &str,
    port: u16,
    flags: ResolveFlags,
) -> io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    Ok(r.resolve(host, flags)
        .await?
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect())
}

/// Platform resolver via tokio (`resolver.cpp`'s `res` fallback).
/// NOT anonymous — inject an onion/SAM/SOCKS resolver instead when
/// running behind circuits.
#[derive(Debug, Default)]
pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn resolve(
        &self,
        host: &str,
        _flags: ResolveFlags,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<IpAddr>>> + Send + '_>>
    {
        let host = host.to_string();
        Box::pin(async move {
            let addrs = tokio::task::spawn_blocking(move || (host.as_str(), 0).to_socket_addrs())
                .await
                .map_err(io::Error::other)??;
            Ok(addrs.map(|a| a.ip()).collect())
        })
    }
}
