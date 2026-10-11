// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `version.cpp` — the version string. Upstream reports
//! `libtorrent/x.y.z`; we report our own crate version (documented
//! deviation: the string identifies *this* implementation).

/// `lt::version_str`.
#[must_use]
pub fn version_str() -> &'static str {
    concat!("BitDaemon/", env!("CARGO_PKG_VERSION"))
}
