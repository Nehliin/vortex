use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use vortex_bittorrent::{Command, State, Torrent, generate_peer_id};

pub fn decode_info_hash_hex(s: &str) -> [u8; 20] {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect::<Vec<u8>>()
        .try_into()
        .unwrap()
}

#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct TorrentInfo {
    /// Info hash of the torrent you want to download.
    /// The metadata will be automatically downloaded
    /// in the swarm before download starts
    #[arg(short, long)]
    info_hash: Option<String>,
    /// Torrent file containing the metadata of the torrent.
    /// if this is provided the initial metadata download will
    /// be skipped and the torrent downlaod can start immediately
    #[arg(short, long)]
    torrent_file: Option<PathBuf>,
}

/// Vortex bittorrent client cli. Fast trackerless torrent downloads using modern io_uring techniques.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    torrent_info: TorrentInfo,
    /// Path where the downloaded files should be saved
    #[arg(short, long)]
    download_folder: PathBuf,
}

fn main() {
    let mut log_builder = env_logger::builder();
    log_builder.filter_level(log::LevelFilter::Error).init();
    let builder = PrometheusBuilder::new();
    builder.install().unwrap();

    let cli = Cli::parse();

    let (tx, rc) = std::sync::mpsc::channel();

    let state = match cli.torrent_info {
        TorrentInfo {
            info_hash: Some(info_hash),
            torrent_file: None,
        } => State::unstarted(decode_info_hash_hex(&info_hash), cli.download_folder),
        TorrentInfo {
            info_hash: None,
            torrent_file: Some(metadata),
        } => {
            let parsed_metadata =
                lava_torrent::torrent::v1::Torrent::read_from_file(metadata).unwrap();
            State::unstarted_from_metadata(parsed_metadata, cli.download_folder).unwrap()
        }
        _ => unreachable!(),
    };

    let info_hash_id = Id::from_bytes(state.info_hash()).unwrap();

    std::thread::spawn(move || {
        let mut builder = Dht::builder();
        let dht_boostrap_nodes = PathBuf::from("dht_boostrap_nodes");
        if dht_boostrap_nodes.exists() {
            let list = std::fs::read_to_string(&dht_boostrap_nodes).unwrap();
            let cached_nodes: Vec<String> = list.lines().map(|line| line.to_string()).collect();
            builder.extra_bootstrap(&cached_nodes);
        }

        let dht_client = builder.build().unwrap();
        log::info!("Bootstrapping!");
        dht_client.bootstrapped();
        log::info!("Bootstrapping done!");
        dht_client.announce_peer(info_hash_id, None).unwrap();

        loop {
            // query
            let all_peers = dht_client.get_peers(info_hash_id);
            for peers in all_peers {
                log::info!("Got {} peers", peers.len());
                tx.send(Command::ConnectToPeers(peers)).unwrap();
            }
            let bootstrap_nodes = dht_client.to_bootstrap();
            let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
            std::fs::write("dht_boostrap_nodes", dht_bootstrap_nodes_contet.as_bytes()).unwrap();
            std::thread::sleep(Duration::from_secs(20));
        }
    });

    let id = generate_peer_id();
    let mut torrrent = Torrent::new(id, state);
    let start_time = Instant::now();
    torrrent.start(rc).unwrap();
    println!("DOWNLOADED in {}", start_time.elapsed().as_secs());
}
