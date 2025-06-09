use std::{
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use crossbeam_channel::{Receiver, Sender, bounded, select, tick};
use heapless::spsc::{Consumer, Producer};
use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event},
};
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

fn dht_thread(
    info_hash_id: Id,
    cmd_tx: Arc<Mutex<Producer<Command, 64>>>,
    shutdown_signal_rc: Receiver<()>,
) {
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

    let query = || {
        println!("DHT query");
        // query
        let all_peers = dht_client.get_peers(info_hash_id);
        let mut cmd_tx = cmd_tx.lock();
        for peers in all_peers {
            log::info!("Got {} peers", peers.len());
            cmd_tx.enqueue(Command::ConnectToPeers(peers)).unwrap();
        }
    };

    query();
    loop {
        select! {
            recv(fetch_interval) -> _ => {
                query();
            }
            recv(shutdown_signal_rc) -> _ => {
                println!("DHT shutdown");
                let bootstrap_nodes = dht_client.to_bootstrap();
                let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
                std::fs::write("dht_boostrap_nodes", dht_bootstrap_nodes_contet.as_bytes())
                    .unwrap();
                break;
            }

        }
    }
}

// Chart with throughput
// gauge with percent done below
// make inline?

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

fn main() -> io::Result<()> {
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
    let (event_tx, event_rc) = event_q.split();
    // Keeping the mutex out here makes the hotter path
    // inside bittorrent faster since the spsc queue reciever is
    // simpler (and better suited to an event loop) compared to a mpmc channel.
    let command_tx = Arc::new(Mutex::new(command_tx));

    let id = generate_peer_id();
    let mut torrent = Torrent::new(id, state);
    let (shutdown_signal_tx, shutdown_signal_rc) = bounded(1);

    std::thread::scope(|s| {
        s.spawn(move || {
            torrent.start(event_tx, command_rc).unwrap();
            println!("T thread shutdown");
        });
        let cmd_tx_clone = command_tx.clone();
        s.spawn(move || dht_thread(info_hash_id, cmd_tx_clone, shutdown_signal_rc));

        let mut app = VortexApp::new(command_tx, event_rc, shutdown_signal_tx);
        let terminal = ratatui::init();
        let result = app.run(terminal);
        ratatui::restore();
        result
    })
}

struct VortexApp<'queue> {
    cmd_tx: Arc<Mutex<Producer<'queue, Command, 64>>>,
    event_rc: Consumer<'queue, TorrentEvent, 512>,
    should_exit: bool,
    start_time: Instant,
    // Signal the other threads we they shutdown
    shutdown_signal_tx: Sender<()>,
}

impl<'queue> VortexApp<'queue> {
    fn new(
        cmd_tx: Arc<Mutex<Producer<'queue, Command, 64>>>,
        event_rc: Consumer<'queue, TorrentEvent, 512>,
        shutdown_signal_tx: Sender<()>,
    ) -> Self {
        Self {
            cmd_tx,
            event_rc,
            should_exit: false,
            start_time: Instant::now(),
            shutdown_signal_tx,
        }
    }

    fn run(&mut self, mut terminal: DefaultTerminal) -> io::Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        let _ = self.cmd_tx.lock().enqueue(Command::Stop);
        let _ = self.shutdown_signal_tx.send(());
        self.should_exit = true;
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget("Hello world", frame.area());
    }

    fn handle_events(&mut self) -> io::Result<()> {
        while let Some(event) = self.event_rc.dequeue() {
            match event {
                TorrentEvent::TorrentComplete => {
                    let elapsed = self.start_time.elapsed();
                    println!("Download complete in: {}s", elapsed.as_secs());
                    self.shutdown();
                }
                TorrentEvent::MetadataComplete(_torrent) => {
                    println!("Metadata complete");
                }
                TorrentEvent::PeerMetrics {
                    conn_id,
                    throuhgput,
                    endgame,
                    snubbed,
                } => {
                    println!(
                        "throuhgput: {conn_id} {throuhgput} endgame: {endgame}, snubbed: {snubbed}"
                    );
                }
                TorrentEvent::TorrentMetrics {
                    pieces_completed,
                    pieces_allocated,
                    num_connections,
                } => {
                    println!(
                        "pieces: {pieces_completed} {pieces_allocated}, conn {num_connections}"
                    );
                }
            }
        }
        // if matches!(event::read()?, Event::Key(_)) {
        //     return Ok(());
        // }
        Ok(())
    }
}

impl Drop for VortexApp<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
