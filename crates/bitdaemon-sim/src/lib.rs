// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of upstream `test/simulation` — a deterministic network
//! harness. Upstream drives real asio sockets through a simulated
//! clock + packet filters; we sit at the `DatagramSocket` /
//! `PeerConnector` seam (ADR-0008) so *no* code under test needs
//! sim awareness.
//!
//! - [`SimNet`] owns endpoints keyed by `SocketAddr`, with per-link
//!   latency/loss and a single delivery queue ordered by virtual
//!   time.
//! - [`SimSocket`] implements `net::transport::DatagramSocket`.
//! - [`SimClock`] is the virtual time source driving `run_to`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod clock;
mod net;
mod socket;

pub use clock::SimClock;
pub use net::{Delivery, Link, SimNet};
pub use socket::SimSocket;
