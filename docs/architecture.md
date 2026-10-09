# Architecture

How libtorrent's C++ structure maps onto BitDaemon's crates and
concurrency model. Read with [`AGENTS.md`](../AGENTS.md) and the ADRs.

## Crate graph

```
                        ┌─────────────┐
                        │ bitdaemon   │  facade (re-exports)
                        │  (facade)   │
                        └──────┬──────┘
                               │
                        ┌──────▼──────┐
                        │  session    │  Session actor, Torrent,
                        │             │  alerts, settings, choker,
                        │             │  piece picker, extensions
                        └─┬───┬───┬───┘
              ┌───────────┘   │   └───────────┐
        ┌─────▼─────┐   ┌─────▼────┐   ┌──────▼─────┐
        │   disk    │   │   net    │   │    dht     │
        │ storage,  │   │ peer wire│   │  kademlia  │
        │ jobs, pool│   │ trackers,│   │            │
        │           │   │ uTP, NAT │   │            │
        └─────┬─────┘   └─────┬────┘   └──────┬─────┘
              └──────┬────────┘               │
               ┌─────▼──────┐          ┌──────▼───┐
               │   core     │          │ (net too)│
               │ metainfo,  │          └──────────┘
               │ file list, │
               │ magnets    │
               └─────┬──────┘
        ┌────────────┴─────────────┐
        │  common        bencode   │   foundation layer
        └──────────────────────────┘
```

Dependency direction is strictly downward — see ADR-0003.

## Concurrency model (C++ → Rust)

| Upstream (boost::asio) | BitDaemon (tokio) |
|---|---|
| `session_impl` on **one** network thread; everything else `post`s into it | One **actor task** owning all torrents; `SessionHandle` = `mpsc::Sender<Command>`; replies via `oneshot` |
| `alert_manager` queue polled by the user | `Alert` enum on a `broadcast`/`mpsc` channel (`SessionHandle::alerts()`) |
| `disk_io_thread` job queue + `job_queue` completions | `spawn_blocking` pool (Phase 3 eval: dedicated pool) returning via `mpsc` |
| `deadline_timer` per peer/tracker/torrent | `tokio::time::{Sleep, Interval}` inside the actor/peer tasks |
| Per-socket `async_read`/`async_write` chains | `Framed` (tokio-util codec) per peer task, or select! in the actor |
| `io_service::strand`-like ordering for peer jobs | serialized naturally by actor message order |

**Peer tasks**: upstream keeps each `peer_connection`'s I/O callbacks on
the network thread. The port may either (a) run each peer as a task
feeding decoded messages into the session actor (preferred — isolates
socket I/O), or (b) run socket reads inline in the actor via `select!`.
Decide in Phase 4; record as ADR.

## Data flow — downloading a piece (target state)

```
peer task ──piece msg──▶ disk::write job ──▶ session actor
     (socket)                (blocking pool)      │ verifies hash,
                                                  ▼ updates picker,
                                    alert channel ◀── piece_finished
                                                  ▼
                                          peers get Have
```

## Public API shape (target)

```rust
let session = Session::spawn(SessionParams::default())?;
let handle  = session.add_torrent(AddTorrentParams::magnet(uri)?).await?;
handle.set_download_limit(512 * 1024);
while let Some(alert) = session.next_alert().await { /* … */ }
```

Mirrors `session`/`session_handle`/`torrent_handle`, but:
- `Session` is the handle; the actor is internal (upstream exposes both
  `session` and `session_handle` — we unify: `Session` *is* the handle,
  `Session::spawn` creates actor+handle).
- `settings_pack` → typed `Settings` (ADR-0004).
- `alert` hierarchy → `Alert` enum streamed from a channel.
- `error_code` → crate `Result` types (ADR-0004).

## Storage layout (Phase 3 target)

One `Storage` implementation over `std::fs` positioned I/O covers what
upstream splits across `pread`/`posix`/`mmap_storage`; `mmap` becomes an
optional `memmap2` feature. `PartFile`, `FilePool`, disk buffer slab
(`Bytes`-based), `HashPicker` (v2) as upstream. `DiskIo` is a trait so
tests can inject `disabled`-equivalent and (Phase 8) simulator backends.

## Wire compatibility contract

Formats that must round-trip bit-exactly with upstream: `.torrent`
(bencode + info-hash rules), resume data (`*.resume` /
`read_resume_data`), `.session` state file, DHT messages, tracker
requests, peer-wire messages, uTP packets. Golden vectors from the C++
implementation back this contract (ADR-0006).
