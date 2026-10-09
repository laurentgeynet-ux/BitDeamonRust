//! `bitdaemon` — example client binary.
//!
//! Rust equivalent of libtorrent's `examples/client_test.cpp`:
//! add torrents/magnet links, watch progress, drive real swarm traffic.
//! This is the primary manual-testing harness for the port.

#![forbid(unsafe_code)]

use clap::Parser;

/// Simple BitTorrent client built on bitdaemon.
#[derive(Parser, Debug)]
#[command(name = "bitdaemon", version, about)]
struct Args {
    /// .torrent file path or magnet: URI.
    torrent: Option<String>,

    /// Download directory.
    #[arg(short, long, default_value = ".")]
    save_path: String,

    /// Listen port for incoming peer connections.
    #[arg(short, long, default_value_t = 6881)]
    port: u16,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    println!("bitdaemon placeholder — session crate not implemented yet");
    println!(
        "torrent: {:?}, save to {}, port {}",
        args.torrent, args.save_path, args.port
    );
}
