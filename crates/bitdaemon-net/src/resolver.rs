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

/// `resolver_interface::resolver_flags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResolveFlags {
    /// `cache_only` (bit 0) — answer only from the cache; a miss
    /// fails instead of triggering a network lookup (upstream uses
    /// this during shutdown paths).
    pub cache_only: bool,
    /// `abort_on_shutdown` (bit 1) — a non-critical lookup; upstream
    /// routes it through a cancellable resolver that `abort()` kills
    /// at shutdown. Here it's passed through to the injected resolver,
    /// which decides how to honor it.
    pub abort_on_shutdown: bool,
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

/// `dns_cache_entry`.
#[derive(Debug)]
struct CacheEntry {
    /// `last_seen` — staleness clock (upstream uses insertion time).
    last_seen: std::time::Instant,
    /// `addresses`.
    addrs: std::sync::Arc<Vec<IpAddr>>,
}

/// `failed_dns_cache_entry` — the error is kept as kind+message
/// (`io::Error` isn't `Clone`).
#[derive(Debug, Clone)]
struct FailedEntry {
    last_seen: std::time::Instant,
    kind: io::ErrorKind,
    msg: String,
}

#[derive(Debug, Default)]
struct CacheState {
    cache: std::collections::HashMap<String, CacheEntry>,
    failed: std::collections::HashMap<String, FailedEntry>,
    /// `m_callbacks` — waiters for an in-flight lookup.
    inflight: std::collections::HashMap<
        String,
        Vec<tokio::sync::oneshot::Sender<io::Result<Vec<IpAddr>>>>,
    >,
}

/// `resolver` — the caching front of `resolver.cpp` around any
/// injected `Resolver`: `m_max_size = 700`, `m_timeout = 1200 s`,
/// failures cached for `timeout / 8`, oldest-entry eviction,
/// single-flight per hostname, `cache_only` flag.
#[derive(Debug)]
pub struct CachingResolver {
    inner: BoxedResolver,
    st: std::sync::Arc<std::sync::Mutex<CacheState>>,
    /// `m_timeout`.
    timeout: std::time::Duration,
    /// `m_max_size`.
    max_size: usize,
}

impl CachingResolver {
    /// ctor — upstream defaults 700 entries / 1200 s.
    #[must_use]
    pub fn new(inner: BoxedResolver) -> Self {
        Self {
            inner,
            st: std::sync::Arc::new(std::sync::Mutex::new(CacheState::default())),
            timeout: std::time::Duration::from_secs(1200),
            max_size: 700,
        }
    }

    /// `set_cache_timeout` — <= 0 disables caching.
    pub fn set_cache_timeout(&mut self, t: std::time::Duration) {
        self.timeout = t;
        if t.is_zero() {
            if let Ok(mut g) = self.st.lock() {
                g.cache.clear();
                g.failed.clear();
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Resolver for CachingResolver {
    fn resolve(
        &self,
        host: &str,
        flags: ResolveFlags,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<IpAddr>>> + Send + '_>>
    {
        // `make_address` short-circuit — dotted IPs never hit DNS.
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Box::pin(async move { Ok(vec![ip]) });
        }
        let now = std::time::Instant::now();
        let mut rx = None;
        {
            let mut g = self.lock();
            if let Some(e) = g.cache.get(host)
                && (flags.cache_only || e.last_seen + self.timeout >= now)
            {
                let addrs = e.addrs.clone();
                return Box::pin(async move { Ok((*addrs).clone()) });
            }
            if let Some(e) = g.failed.get(host)
                && (flags.cache_only || e.last_seen + self.timeout / 8 >= now)
            {
                let (kind, msg) = (e.kind, e.msg.clone());
                return Box::pin(async move { Err(io::Error::new(kind, msg)) });
            }
            if flags.cache_only {
                return Box::pin(async move {
                    Err(io::Error::new(io::ErrorKind::NotFound, "host_not_found"))
                });
            }
            match g.inflight.get_mut(host) {
                Some(waiters) => {
                    // join the outstanding lookup
                    let (tx, r) = tokio::sync::oneshot::channel();
                    waiters.push(tx);
                    rx = Some(r);
                }
                None => {
                    g.inflight.insert(host.to_string(), Vec::new());
                }
            }
        }
        if let Some(r) = rx {
            return Box::pin(async move {
                r.await
                    .unwrap_or_else(|_| Err(io::Error::other("resolver task dropped")))
            });
        }
        // driver — owned clones so the boxed future doesn't borrow
        // `self` or `host` (trait's elided `'_` won't cover both).
        let inner = self.inner.clone();
        let st = self.st.clone();
        let host_owned = host.to_string();
        let timeout = self.timeout;
        let max_size = self.max_size;
        Box::pin(async move {
            let res = inner.resolve(&host_owned, flags).await;
            {
                let mut g = st.lock().unwrap_or_else(|e| e.into_inner());
                match &res {
                    Ok(addrs) => {
                        // upstream erases any failed-cache entry for
                        // the host on success.
                        g.failed.remove(&host_owned);
                        if g.cache.len() >= max_size
                            && let Some(oldest) = g
                                .cache
                                .iter()
                                .min_by_key(|(_, e)| e.last_seen)
                                .map(|(k, _)| k.clone())
                        {
                            g.cache.remove(&oldest);
                        }
                        g.cache.insert(
                            host_owned.clone(),
                            CacheEntry {
                                last_seen: std::time::Instant::now(),
                                addrs: std::sync::Arc::new(addrs.clone()),
                            },
                        );
                    }
                    Err(e) => {
                        if g.failed.len() >= max_size
                            && let Some(oldest) = g
                                .failed
                                .iter()
                                .min_by_key(|(_, e)| e.last_seen)
                                .map(|(k, _)| k.clone())
                        {
                            g.failed.remove(&oldest);
                        }
                        g.failed.insert(
                            host_owned.clone(),
                            FailedEntry {
                                last_seen: std::time::Instant::now(),
                                kind: e.kind(),
                                msg: e.to_string(),
                            },
                        );
                    }
                }
                // wake single-flight waiters (`m_callbacks`).
                let waiters = g.inflight.remove(&host_owned).unwrap_or_default();
                for tx in waiters {
                    let shared = res
                        .as_ref()
                        .map(|a| a.clone())
                        .map_err(|e| io::Error::new(e.kind(), e.to_string()));
                    let _ = tx.send(shared);
                }
                let _ = timeout;
            }
            res
        })
    }
}
