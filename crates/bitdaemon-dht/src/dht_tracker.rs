// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_tracker` — owns the per-address-family [`DhtNode`]s and
//! drives their periodic work. Upstream binds real sockets and
//! posts through `io_context`; here transports are injected, so
//! the tracker only multiplexes `incoming`/`poll`/`tick`.

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use crate::dht_node::DhtNode;
use crate::dht_settings::DhtSettings;
use crate::node_id::NodeId;

/// secret rotation cadence upstream (`refresh_key_timer`, 5 min).
pub const SECRET_ROTATION: Duration = Duration::from_secs(300);

/// Per-family node map (`m_nodes` keyed by listen socket).
#[derive(Default)]
pub struct DhtTracker {
    /// v4 node (`"n4"`/`"nodes"` protocol).
    pub v4: Option<DhtNode>,
    /// v6 node (`"n6"`/`"nodes6"`).
    pub v6: Option<DhtNode>,
    last_key_rotation: Option<Instant>,
}

impl DhtTracker {
    /// `new_socket` — create the node for `bind`'s family.
    pub fn bind(&mut self, bind_ip: IpAddr, id: Option<NodeId>, settings: DhtSettings) {
        let node = DhtNode::new(id, bind_ip, bind_ip.is_ipv6(), settings);
        if bind_ip.is_ipv6() {
            self.v6 = Some(node);
        } else {
            self.v4 = Some(node);
        }
    }

    /// `delete_socket`.
    pub fn unbind(&mut self, v6: bool) {
        if v6 {
            self.v6 = None;
        } else {
            self.v4 = None;
        }
    }

    /// `get_node(v6)` — the foreign-protocol lookup used for `want`.
    #[must_use]
    pub fn get_node(&self, v6: bool) -> Option<&DhtNode> {
        if v6 {
            self.v6.as_ref()
        } else {
            self.v4.as_ref()
        }
    }

    /// feed a datagram; returns any immediate reply to send.
    pub fn incoming(&mut self, from: SocketAddr, buf: &[u8]) -> Option<(Vec<u8>, SocketAddr)> {
        let v6 = from.is_ipv6();
        self.get_node_mut(v6).and_then(|n| n.incoming(buf, from))
    }

    /// drain outgoing datagrams from both nodes.
    pub fn poll(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        if let Some(n) = self.v6.as_mut() {
            if let Some(d) = n.poll() {
                return Some(d);
            }
        }
        self.v4.as_mut().and_then(DhtNode::poll)
    }

    /// `tick` — rpc timeouts, storage expiry, secret rotation.
    /// Returns the caller's next-wake delay.
    pub fn tick(&mut self) -> Duration {
        let mut wait = Duration::from_secs(60);
        for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
            wait = wait.min(n.tick());
        }
        let now = Instant::now();
        if self
            .last_key_rotation
            .is_none_or(|t| now.duration_since(t) >= SECRET_ROTATION)
        {
            self.last_key_rotation = Some(now);
            for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
                n.rotate_secrets();
            }
        }
        wait
    }

    /// mutable access for `load_state` bootstrap seeding.
    pub fn get_node_mut(&mut self, v6: bool) -> Option<&mut DhtNode> {
        if v6 {
            self.v6.as_mut()
        } else {
            self.v4.as_mut()
        }
    }
}
