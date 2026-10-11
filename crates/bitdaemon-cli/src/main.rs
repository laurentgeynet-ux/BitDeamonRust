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
    eng.start_udp_pump();

    // upstream `dht_bootstrap_nodes` setting — seed the routing
    // table from the well-known public routers.
    for host in ["router.bittorrent.com", "dht.transmissionbt.com"] {
        eng.dispatch_out(bitdaemon::session::session::SessionOut::DhtAddNode {
            host: host.into(),
            port: 6881,
        });
    }

    // status printer + ctrl-c stop
    let ses = eng.session.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let mut guard = slock(&ses);
            for a in guard.pop_alerts() {
                println!("alert: {a:?}");
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
