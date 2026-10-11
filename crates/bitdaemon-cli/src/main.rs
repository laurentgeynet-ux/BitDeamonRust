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
use bitdaemon::disk::storage::StorageParams;
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
    let mut atp = load_atp(&args.torrent, &args.save_path)?;
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

    // disk io — `pread_disk_io(4, poster)`; the poster is a no-op
    // here because the engine tick drains the completion queue.
    let disk = PreadDiskIo::new(4, Arc::new(|| {}));

    // storage for the torrent (factory path → StorageIndex)
    let storage = if let Some(ti) = &atp.ti {
        let params = StorageParams {
            files: ti.files(),
            renamed: &Default::default(),
            path: args.save_path.clone(),
            part_file_dir: PathBuf::new(),
            mode: match atp.storage_mode {
                bitdaemon::core::StorageMode::Sparse => {
                    bitdaemon::disk::storage::StorageMode::Sparse
                }
                bitdaemon::core::StorageMode::Allocate => {
                    bitdaemon::disk::storage::StorageMode::Allocate
                }
            },
            priorities: &[],
            info_hash: *atp.info_hashes.v1.as_bytes(),
            v1: atp.info_hashes.has_v1(),
            v2: atp.info_hashes.has_v2(),
        };
        disk.new_torrent(&params)
    } else {
        // magnets land without metadata — deferred storage
        bitdaemon::common::types::StorageIndex(0)
    };

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
    if args.no_os_cache {
        slock(&eng.session).settings.disable_os_write_cache = true;
    }
    if args.no_os_read_cache {
        slock(&eng.session).settings.disable_os_read_cache = true;
    }
    slock(&eng.session).set_disk(disk);
    slock(&eng.session).set_listen_port(args.port);
    slock(&eng.session).set_dht_port(udp_port);
    if args.limit > 0 {
        slock(&eng.session).settings.download_rate_limit = args.limit;
        eng.download_limiter.set_limit(args.limit);
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
    let acceptor = TokioTcpAcceptor::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        args.port,
    ))
    .await?;
    eng.start_acceptor(Arc::new(acceptor));
    if let Some(ssl_port) = args.ssl_port {
        let ssl_acceptor =
            TokioTcpAcceptor::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), ssl_port))
                .await?;
        eng.start_ssl_acceptor(Arc::new(ssl_acceptor));
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
            let _ = tokio::signal::ctrl_c().await;
        },
        // 0 → `settings.connection_speed` (upstream default 30/s).
        0,
    )
    .await;
    Ok(())
}
