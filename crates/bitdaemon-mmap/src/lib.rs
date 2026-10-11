// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `bitdaemon-mmap` — mmap-backed storage backend.
//!
//! Port of `mmap_storage.cpp`, `mmap.cpp`, `file_view_pool.cpp` and
//! `mmap_disk_io.cpp`/`mmap_disk_job.hpp`. This is the **single
//! `unsafe` carve-out** of the workspace (ADR-0010): `memmap2`'s
//! mapping calls are unsafe by nature, so they live here behind
//! `#![allow(unsafe_code)]` — every other crate stays
//! `#![forbid(unsafe_code)]`.
//!
//! The crate is optional: `bitdaemon-disk`'s default backend is
//! `PreadStorage`; downstream opts into mmap via
//! [`mmap_storage_constructor`] as a `StorageFactory`.

#![allow(unsafe_code)]

pub mod file_view_pool;
pub mod mmap_storage;

pub use file_view_pool::FileViewPool;
pub use mmap_storage::{MmapStorage, mmap_storage_constructor};
