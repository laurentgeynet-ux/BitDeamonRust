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
    let atp = load_atp(&args.torrent, &args.save_path)?;

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
    slock(&eng.session).set_disk(disk);
    slock(&eng.session).listen_port = args.port;
    slock(&eng.session).set_dht_port(udp_port);
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
            Some(ih) => println!("added {}", bitdaemon::common::hex::to_hex(ih.as_bytes())),
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

    // status printer + ctrl-c stop
    let ses = eng.session.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            for (ih, t) in &slock(&ses).torrents {
                let st = t.status();
                println!(
                    "{}: {:.1}% | {} peers | {} down {} up B/s | {:?}",
                    bitdaemon::common::hex::to_hex(&ih.as_bytes()[..8]),
                    st.progress * 100.0,
                    st.num_peers,
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
        8,
    )
    .await;
    Ok(())
}
