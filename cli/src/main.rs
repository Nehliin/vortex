use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use crossbeam_channel::{bounded, select, tick};
use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use vortex_bittorrent::{Command, State, Torrent, TorrentEvent, generate_peer_id};

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    let mut command_q = heapless::spsc::Queue::new();
    let mut event_q = heapless::spsc::Queue::new();

    let (command_tx, command_rc) = command_q.split();
    let (event_tx, mut event_rc) = event_q.split();
    // Keeping the mutex out here makes the hotter path
    // inside bittorrent faster since the spsc queue reciever is
    // simpler (and better suited to an event loop) compared to a mpmc channel.
    let command_tx = Arc::new(Mutex::new(command_tx));

    let id = generate_peer_id();
    let mut torrent = Torrent::new(id, state);
    let start_time = Instant::now();
    let (shutdown_signal_tx, shudown_signal_rc) = bounded(1);

    std::thread::scope(|s| {
        s.spawn(move || {
            torrent.start(event_tx, command_rc).unwrap();
            println!("T SHUTDONW");
        });
        let cmd_tx_clone = command_tx.clone();
        s.spawn(move || {
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
            // TODO: Remove announcement when complete?
            dht_client.announce_peer(info_hash_id, None).unwrap();

            let fetch_interval = tick(Duration::from_secs(20));

            loop {
                select! {
                    recv(fetch_interval) -> _ => {
                        println!("QUERY");
                        // query
                        let all_peers = dht_client.get_peers(info_hash_id);
                        for peers in all_peers {
                            log::info!("Got {} peers", peers.len());
                            cmd_tx_clone
                                .lock()
                                .enqueue(Command::ConnectToPeers(peers))
                                .unwrap();
                        }
                    }
                    recv(shudown_signal_rc) -> _ => {
                        println!("SHUTTING DOWN");
                        let bootstrap_nodes = dht_client.to_bootstrap();
                        let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
                        std::fs::write("dht_boostrap_nodes", dht_bootstrap_nodes_contet.as_bytes())
                            .unwrap();
                        break;
                    }

                }
            }
        });

        'outer: loop {
            while let Some(event) = event_rc.dequeue() {
                match event {
                    TorrentEvent::TorrentComplete => {
                        let elapsed = start_time.elapsed();
                        println!("Download complete in: {}s", elapsed.as_secs());
                        let _ = command_tx.lock().enqueue(Command::Stop);
                        shutdown_signal_tx.send(()).unwrap();
                        break 'outer;
                    }
                    TorrentEvent::MetadataComplete(torrent) => {
                        println!("METADATA COMPLETE");
                    }
                    TorrentEvent::PeerMetrics {
                        conn_id,
                        throuhgput,
                        endgame,
                        snubbed,
                    } => {}
                    TorrentEvent::TorrentMetrics {
                        pieces_completed,
                        pieces_allocated,
                        num_connections,
                    } => {}
                }
            }
        }
    });
}
