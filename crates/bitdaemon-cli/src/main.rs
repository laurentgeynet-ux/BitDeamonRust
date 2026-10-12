//! `bitdaemon` — example client binary.
//!
//! Rust equivalent of libtorrent's `examples/client_test.cpp`:
//! add a torrent/magnet, drive real swarm traffic on the default
//! OS transports (`TokioTcpConnector`/`TokioTcpAcceptor`/
//! `TokioUdpSocket`), print torrent status once a second.

#![forbid(unsafe_code)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon::core::add_torrent_params::AddTorrentParams;
use bitdaemon::core::magnet_uri::parse_magnet;
use bitdaemon::core::torrent_info::load_torrent;
use bitdaemon::disk::disk_io::PreadDiskIo;
use bitdaemon::engine::Engine;
use bitdaemon::net::os_transports::{TokioTcpAcceptor, TokioTcpConnector, TokioUdpSocket};
use bitdaemon::session::session_handle::lock as slock;
use clap::Parser;

/// Simple BitTorrent client built on bitdaemon.
#[derive(Parser, Debug)]
#[command(name = "bitdaemon", version, about)]
struct Args {
    /// .torrent file path or magnet: URI.
    torrent: String,

    /// Download directory.
    #[arg(short, long, default_value = ".")]
    save_path: PathBuf,

    /// Listen port for incoming peer connections.
    #[arg(short, long, default_value_t = 6881)]
    port: u16,

    /// Direct peer to connect to (ip:port), repeatable.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Seed mode — skip hash-check, assume files complete
    /// (upstream `add_torrent_params::flag_seed_mode`).
    #[arg(long)]
    seed: bool,

    /// Download rate limit in bytes/s (upstream
    /// `settings_pack::download_rate_limit`; 0 = unlimited).
    #[arg(long, default_value_t = 0)]
    limit: i64,

    /// Enable uTP on the shared UDP socket (upstream
    /// `enable_outgoing_utp`/`enable_incoming_utp` — opt-in here
    /// while the uTP path is newer than TCP).
    #[arg(long)]
    utp: bool,

    /// Disable the OS write cache for data files (upstream
    /// `settings_pack::disk_io_write_mode = disable_os_cache` —
    /// qBittorrent's "OS cache" toggle; write-through on Windows,
    /// O_SYNC on POSIX). Prevents dirty-page storms at high rates.
    #[arg(long)]
    no_os_cache: bool,

    /// Disable the OS read cache for data files (upstream
    /// `settings_pack::disk_io_read_mode = disable_os_cache`).
    #[arg(long)]
    no_os_read_cache: bool,

    /// SOCKS5 proxy `host:port` — routes peer connections and the
    /// UDP lane (DHT/uTP/UDP trackers) through it (upstream
    /// `proxy_*` settings; ADR-0020).
    #[arg(long)]
    proxy: Option<String>,

    /// SOCKS5 proxy credentials `user:pass`.
    #[arg(long)]
    proxy_auth: Option<String>,

    /// Disable UPnP/SSDP port-mapping discovery (ADR-0018).
    #[arg(long)]
    no_upnp: bool,

    /// Generic disk I/O threads (upstream `aio_threads`, default 10).
    #[arg(long, default_value_t = 10)]
    aio_threads: i64,

    /// Dedicated hashing threads (upstream `hashing_threads`,
    /// default 1 — ADR-0022).
    #[arg(long, default_value_t = 1)]
    hashing_threads: i64,

    /// Disable `SetFileValidData` on non-sparse file extension
    /// (Windows; enabled by default for qBittorrent parity —
    /// upstream `enable_set_file_valid_data`, ADR-0022).
    #[arg(long)]
    no_set_file_valid_data: bool,

    /// SSL-torrent server certificate PEM (upstream
    /// `set_ssl_certificate` cert path).
    #[arg(long)]
    ssl_cert: Option<PathBuf>,

    /// SSL-torrent private key PEM.
    #[arg(long)]
    ssl_key: Option<PathBuf>,

    /// DH params PEM (upstream arg; rustls is ECDHE-only — ignored).
    #[arg(long)]
    ssl_dh: Option<PathBuf>,

    /// Extra listen port that expects TLS-wrapped BitTorrent
    /// handshakes (upstream `listen_interfaces` `…s` suffix).
    #[arg(long)]
    ssl_port: Option<u16>,

    /// Fastresume file (libtorrent `.fastresume` format) — restores
    /// have-bits and unfinished blocks; the positional .torrent/magnet
    /// supplies metadata when the resume file has no info dict.
    #[arg(long)]
    resume: Option<PathBuf>,

    /// Stop gracefully after N seconds (bench tool — same path as
    /// ctrl-c, writes `.resume` before exiting).
    #[arg(long)]
    stop_after: Option<u64>,
}

fn load_atp(arg: &str, save_path: &std::path::Path) -> anyhow::Result<AddTorrentParams> {
    let mut atp = if arg.starts_with("magnet:") {
        parse_magnet(arg.as_bytes()).map_err(|e| anyhow::anyhow!("magnet: {e}"))?
    } else {
        let buf = std::fs::read(arg)?;
        load_torrent(&buf).map_err(|e| anyhow::anyhow!("torrent: {e}"))?
    };
    atp.save_path = save_path.as_os_str().as_encoded_bytes().to_vec();
    atp.storage_mode = bitdaemon::core::StorageMode::Allocate;
    Ok(atp)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let mut atp = if let Some(resume) = &args.resume {
        let buf = std::fs::read(resume)?;
        let mut a = bitdaemon::core::resume_data::read_resume_data(
            &buf,
            &bitdaemon::core::LoadTorrentLimits::default(),
        )
        .map_err(|e| anyhow::anyhow!("resume: {e}"))?;
        // a resume without an embedded info dict still needs metadata
        // from the positional .torrent/magnet
        if a.ti.is_none() {
            let base = load_atp(&args.torrent, &args.save_path)?;
            a.ti = base.ti;
            a.info_hashes = base.info_hashes;
            if a.trackers.is_empty() {
                a.trackers = base.trackers;
            }
        }
        a.save_path = args.save_path.as_os_str().as_encoded_bytes().to_vec();
        a.storage_mode = bitdaemon::core::StorageMode::Allocate;
        a
    } else {
        load_atp(&args.torrent, &args.save_path)?
    };
    if args.seed {
        atp.flags |= bitdaemon::common::flags::TorrentFlags::SEED_MODE;
        // seed_mode upstream still requires the `have` bits set —
        // claim every piece (verify is skipped by contract).
        if let Some(ti) = &atp.ti {
            let mut bf = bitdaemon::common::Bitfield::with_size(ti.num_pieces() as usize);
            bf.set_all();
            atp.have_pieces = bf;
        }
    }

    // disk io — `pread_disk_io(aio_threads, poster)`; the poster is a
    // no-op here because the engine tick drains the completion queue.
    // Upstream defaults: hashing pool = 1 thread, no_atime = true,
    // SetFileValidData on for qBittorrent parity (ADR-0022).
    let disk = PreadDiskIo::new(args.aio_threads, Arc::new(|| {}));
    disk.set_hash_threads(args.hashing_threads);
    {
        use bitdaemon::disk::open_mode::OpenMode;
        let mut extra = OpenMode::NO_ATIME;
        if !args.no_set_file_valid_data {
            extra |= OpenMode::ALLOW_SET_FILE_VALID_DATA;
        }
        disk.set_open_mode_extra(extra);
    }

    // `torrent::init` semantics: the session builds the storage from
    // the atp (`.torrent` now, magnet at `MetadataReceived`).
    let storage = bitdaemon::common::types::StorageIndex::INVALID;

    let connector: Arc<TokioTcpConnector> = Arc::new(TokioTcpConnector);
    let udp = TokioUdpSocket::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        args.port,
    ))
    .await?;
    let udp_port = bitdaemon::net::transport::DatagramSocket::local_addr(&udp).port();

    let mut eng = Engine::new(connector, Some(Arc::new(udp)));
    // OS resolver — the default transports' DNS companion (anonymous
    // deployments inject a circuit resolver instead; ADR-0008).
    eng.set_resolver(Arc::new(bitdaemon::net::resolver::SystemResolver));
    // `client_test` upstream runs with `alert_mask = all_categories`;
    // the session default is `error`-only.
    {
        let mut s = slock(&eng.session);
        s.settings.alert_mask = bitdaemon::session::alerts::AlertCategory::ALL;
        s.alerts
            .set_mask(bitdaemon::session::alerts::AlertCategory::ALL);
    }
    if args.no_os_cache {
        slock(&eng.session).settings.disk_io_write_mode =
            bitdaemon::session::settings::DiskIoMode::DisableOsCache;
    }
    if args.no_os_read_cache {
        slock(&eng.session).settings.disk_io_read_mode =
            bitdaemon::session::settings::DiskIoMode::DisableOsCache;
    }
    slock(&eng.session).set_disk(disk);
    slock(&eng.session).set_listen_port(args.port);
    slock(&eng.session).set_dht_port(udp_port);
    if args.limit > 0 {
        slock(&eng.session).settings.download_rate_limit = args.limit;
        eng.download_limiter.set_limit(args.limit);
    }
    if let Some(proxy) = &args.proxy {
        let (host, port) = proxy
            .rsplit_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().unwrap_or(1080)))
            .unwrap_or_else(|| (proxy.clone(), 1080));
        let (user, pass) = args
            .proxy_auth
            .as_deref()
            .and_then(|a| a.split_once(':'))
            .map(|(u, p)| (u.to_string(), p.to_string()))
            .unwrap_or_default();
        let mut ps = bitdaemon::net::proxy::ProxySettings {
            hostname: host,
            port,
            username: user,
            password: pass,
            proxy_type: bitdaemon::net::proxy::ProxyType::Socks5Password,
            proxy_peer_connections: true,
            proxy_tracker_connections: true,
            proxy_hostnames: true,
        };
        if ps.username.is_empty() {
            ps.proxy_type = bitdaemon::net::proxy::ProxyType::Socks5;
        }
        {
            let mut s = slock(&eng.session);
            s.proxies.peer = ps.clone();
            s.proxies.tracker = ps.clone();
            s.proxies.dht = ps;
        }
        // UDP lane through the proxy (ADR-0020) — before enable_dht.
        eng.apply_udp_proxy().await;
    }
    // NAT-PMP port mapping via the default gateway (ADR-0019);
    // no-op when the service is off or no gateway is found.
    eng.start_natpmp_auto(args.port, 3600).await;
    // UPnP: SSDP M-SEARCH burst (ADR-0018) — IGDs discovered later
    // get TCP+UDP port mappings for the listen port.
    if !args.no_upnp {
        eng.start_upnp().await;
    }
    slock(&eng.session).enable_dht(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        None,
        bitdaemon::dht::dht_settings::DhtSettings::default(),
    );

    {
        let mut out = Vec::new();
        let ih = slock(&eng.session).add_torrent_params(&atp, storage, &mut out);
        for o in out {
            eng.dispatch_out(o);
        }
        match ih {
            Some(ih) => {
                println!("added {}", bitdaemon::common::hex::to_hex(ih.as_bytes()));
                let handle = bitdaemon::session::session_handle::SessionHandle::from_shared(
                    eng.session.clone(),
                )
                .find_torrent(&ih);
                // SSL torrent: our PEM keypair + hex-ih SNI dispatch.
                if let (Some(cert), Some(key)) = (&args.ssl_cert, &args.ssl_key) {
                    let cert = std::fs::read_to_string(cert)?;
                    let key = std::fs::read_to_string(key)?;
                    let dh = args
                        .ssl_dh
                        .as_ref()
                        .and_then(|p| std::fs::read_to_string(p).ok())
                        .unwrap_or_default();
                    if let Some(h) = &handle {
                        h.set_ssl_certificate(cert, key, dh);
                    }
                }
                eng.refresh_ssl_certs();
                for p in &args.peers {
                    match p.parse::<std::net::SocketAddr>() {
                        Ok(addr) => {
                            println!("connect_peer {addr}");
                            if let Some(h) = &handle {
                                h.connect_peer(addr);
                            }
                        }
                        Err(e) => eprintln!("bad --peer {p}: {e}"),
                    }
                }
            }
            None => anyhow::bail!("add_torrent failed"),
        }
    }

    // inbound peer pump
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.port);
    let acceptor = TokioTcpAcceptor::bind(listen_addr).await?;
    eng.start_acceptor_at(Arc::new(acceptor), Some(listen_addr), 0);
    if let Some(ssl_port) = args.ssl_port {
        let ssl_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), ssl_port);
        let ssl_acceptor = TokioTcpAcceptor::bind(ssl_addr).await?;
        eng.start_ssl_acceptor_at(Arc::new(ssl_acceptor), Some(ssl_addr), 1);
        println!("ssl listen on :{ssl_port}");
    }
    if args.utp {
        eng.enable_utp().await;
        println!("uTP enabled (outgoing + incoming)");
    }
    eng.start_udp_pump();

    // upstream `dht_bootstrap_nodes` setting — seed the routing
    // table from the well-known public routers.
    for host in [
        "router.bittorrent.com",
        "router.utorrent.com",
        "router.bitcomet.com",
        "dht.transmissionbt.com",
        "dht.aelitis.com",
    ] {
        eng.dispatch_out(bitdaemon::session::session::SessionOut::DhtAddNode {
            host: host.into(),
            port: 6881,
        });
    }

    // status printer + ctrl-c stop
    let ses = eng.session.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut guard = slock(&ses);
            for a in guard.pop_alerts() {
                println!("alert: {a:?}");
            }
            let dht_nodes = guard.dht_state().nodes.len() + guard.dht_state().nodes6.len();
            if dht_nodes > 0 {
                println!("dht: {dht_nodes} nodes in routing table");
            }
            if let Some(d) = &guard.disk {
                let q = d.queue_stats();
                println!(
                    "disk queue: w={} h={} other={} parked={} write_blocks={} cache={} unhashed={} bufs={} threads={} bp={}",
                    q.queued_writes,
                    q.queued_hashes,
                    q.queued_other,
                    q.parked_hashes,
                    q.write_blocks,
                    q.cached_blocks,
                    q.num_unhashed,
                    q.disk_blocks_in_use,
                    q.threads,
                    d.write_queue_exceeded(),
                );
            }
            for (ih, t) in &guard.torrents {
                let st = t.status(&guard.settings, guard.queue_position(ih));
                let err = if t.last_error.is_empty() {
                    String::new()
                } else {
                    format!(" | err: {}", t.last_error)
                };
                let missing: Vec<u32> = (0..t.num_pieces as usize)
                    .filter(|i| *i < t.have.size() && !t.have.get_bit(*i))
                    .map(|i| i as u32)
                    .take(8)
                    .collect();
                let miss = if missing.is_empty() {
                    String::new()
                } else {
                    format!(" | missing {missing:?}")
                };
                println!(
                    "{}: {:.1}% | {} peers | have {}/{} | {} down {} up B/s | {:?}{err}{miss}",
                    bitdaemon::common::hex::to_hex(&ih.as_bytes()[..8]),
                    st.progress * 100.0,
                    st.num_peers,
                    t.have.count(),
                    t.num_pieces,
                    st.download_rate,
                    st.upload_rate,
                    st.state,
                );
            }
        }
    });

    eng.run(
        async {
            match args.stop_after {
                Some(s) => tokio::time::sleep(std::time::Duration::from_secs(s)).await,
                None => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        },
        // 0 → `settings.connection_speed` (upstream default 30/s).
        0,
    )
    .await;

    // client_test behavior: drop a fastresume file per torrent on
    // shutdown — `<save_path>/<v1-infohash>.resume`, loadable via
    // `--resume` on the next run (or by a C++ client).
    {
        let guard = slock(&eng.session);
        for (ih, t) in &guard.torrents {
            let buf = t.write_resume_data();
            if buf.is_empty() {
                continue;
            }
            let path = args.save_path.join(format!(
                "{}.resume",
                bitdaemon::common::hex::to_hex(ih.as_bytes())
            ));
            match std::fs::write(&path, &buf) {
                Ok(()) => println!("saved {}", path.display()),
                Err(e) => eprintln!("resume save {}: {e}", path.display()),
            }
        }
    }
    Ok(())
}
