use std::{
    fs::OpenOptions, io::ErrorKind, net::TcpListener, num::ParseIntError, path::PathBuf,
    sync::mpsc::SyncSender, time::Duration,
};

use clap::{Args, Parser};
use color_eyre::eyre::{eyre, WrapErr};
use crossbeam_channel::{bounded, select, tick, Receiver};
use heapless::spsc::Queue;
use mainline::{Dht, Id};
use ratatui::{
    layout::{Constraint, Layout},
    prelude::{Buffer, Rect},
    widgets::Widget,
};
use vortex_bittorrent::{Command, PeerId, State, Torrent, TorrentEvent};

mod app;
mod config;
mod ui;

use app::{AppState, VortexApp};
use ui::{
    extract_throughput_data, InfoData, InfoPanel, ProgressBar, ProgressState, ThroughputData,
    ThroughputGraph,
};

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn decode_info_hash_hex(s: &str) -> color_eyre::eyre::Result<[u8; 20]> {
    let byte_vec = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<Result<Vec<u8>, ParseIntError>>()?;

    match byte_vec.try_into() {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(eyre!("Invalid length")),
    }
}

fn dht_thread(
    info_hash_id: Id,
    port: u16,
    cmd_tx: SyncSender<Command>,
    shutdown_signal_rc: Receiver<()>,
    dht_cache_path: PathBuf,
    skip_dht_cache: bool,
) -> color_eyre::eyre::Result<()> {
    let mut builder = Dht::builder();
    if dht_cache_path.exists() && !skip_dht_cache {
        let list = std::fs::read_to_string(&dht_cache_path).wrap_err("Failed to dht_cache")?;
        let cached_nodes: Vec<String> = list.lines().map(|line| line.to_string()).collect();
        builder.extra_bootstrap(&cached_nodes);
    }

    let dht_client = builder.build().wrap_err("Failed to build DHT client")?;
    log::info!("Bootstrapping!");
    dht_client.bootstrapped();
    log::info!("Bootstrapping done!");
    dht_client
        .announce_peer(info_hash_id, Some(port))
        .wrap_err("Failed to announce ourself on the DHT")?;

    let fetch_interval = tick(Duration::from_secs(20));
    let announce_interval = tick(Duration::from_mins(10));

    let query = || {
        let all_peers = dht_client.get_peers(info_hash_id);
        for peers in all_peers {
            log::info!("Got {} peers", peers.len());
            cmd_tx
                .send(Command::ConnectToPeers(peers))
                .expect("Command channel to not be full");
        }
    };

    query();
    loop {
        select! {
            recv(fetch_interval) -> _ => {
                query();
            }
            recv(announce_interval) -> _ => {
                dht_client.announce_peer(info_hash_id, Some(port))
                    .wrap_err("Failed to announce ourself on the DHT")?;
            }
            recv(shutdown_signal_rc) -> _ => {
                let bootstrap_nodes = dht_client.to_bootstrap();
                let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
                std::fs::write(&dht_cache_path, dht_bootstrap_nodes_contet.as_bytes())
                    .wrap_err("Failed to write to dht cache")?;
                break Ok(());
            }

        }
    }
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

    #[arg(short, long)]
    magnet_link: Option<String>,
}

/// Vortex bittorrent client cli. Fast trackerless torrent downloads using modern io_uring techniques.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Port for the listener
    #[arg(short, long)]
    port: Option<u16>,
    #[command(flatten)]
    torrent_info: TorrentInfo,
    /// Path to the config file (defaults to $XDG_CONFIG_HOME/vortex/config.toml)
    /// the file will created it if doesn't already exists.
    #[arg(short, long)]
    config_file: Option<PathBuf>,
    /// Path where the downloaded files should be saved (defaults to $XDG_DATA_HOME/vortex/downloads)
    #[arg(short, long)]
    download_folder: Option<PathBuf>,
    /// Log file path (defaults to $XDG_STATE_HOME/vortex/vortex.log)
    #[arg(long)]
    log_file: Option<PathBuf>,
    /// DHT cache path (defaults to $XDG_CACHE_HOME/vortex/dht_bootstrap_nodes)
    #[arg(long)]
    dht_cache: Option<PathBuf>,
    /// Skip use of the DHT node cache and rebuild from bootstrap nodes
    #[arg(long)]
    skip_dht_cache: bool,
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    // load or create config file (auto-creates if doesn't exist)
    let mut vortex_config =
        config::load_or_create_config(cli.config_file).wrap_err("Failed to load config")?;

    // merge cli args with config
    vortex_config = config::merge_with_cli_args(
        vortex_config,
        cli.download_folder.clone(),
        cli.log_file.clone(),
        cli.dht_cache.clone(),
        cli.port,
    );

    let paths = config::resolve_paths(&vortex_config);

    std::fs::create_dir_all(&paths.download_folder)?;
    if let Some(parent) = paths.log_file.parent() {
        std::fs::create_dir_all(parent).wrap_err("Failed to create log file directory")?;
    }
    if let Some(parent) = paths.dht_cache.parent() {
        std::fs::create_dir_all(parent).wrap_err("Failed to create dht cache directory")?;
    }

    let target = Box::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&paths.log_file)
            .wrap_err("Failed to open log file")?,
    );
    env_logger::builder()
        .target(env_logger::Target::Pipe(target))
        .filter_level(log::LevelFilter::Debug)
        .init();

    #[cfg(feature = "metrics")]
    {
        use metrics_exporter_prometheus::PrometheusBuilder;
        let builder = PrometheusBuilder::new().with_recommended_naming(true);
        builder.install().wrap_err("Failed to install prometheus")?;
    }

    let bt_config = vortex_config.bittorrent;
    let root = paths.download_folder.clone();

    let mut state = match cli.torrent_info {
        TorrentInfo {
            info_hash: Some(info_hash),
            torrent_file: None,
            magnet_link: None,
        } => {
            match lava_torrent::torrent::v1::Torrent::read_from_file(
                root.join(info_hash.to_lowercase()),
            ) {
                // Metadata has been saved from previous run
                Ok(metadata) => State::from_metadata_and_root(metadata, root.clone(), bt_config)?,
                Err(lava_torrent::LavaTorrentError::Io(io_err)) => {
                    if io_err.kind() == ErrorKind::NotFound {
                        State::unstarted(
                            decode_info_hash_hex(&info_hash).wrap_err("Invalid info hash")?,
                            root.clone(),
                            bt_config,
                        )
                    } else {
                        return Err(eyre!("Failed looking for stored metadata {io_err}"));
                    }
                }
                Err(err) => return Err(eyre!("Failed looking for stored metadata {err}")),
            }
        }
        TorrentInfo {
            info_hash: None,
            magnet_link: None,
            torrent_file: Some(metadata),
        } => {
            let parsed_metadata = lava_torrent::torrent::v1::Torrent::read_from_file(metadata)
                .wrap_err("Invalid torrent file")?;
            State::from_metadata_and_root(parsed_metadata, root.clone(), bt_config)
                .wrap_err("Failed initialzing state")?
        }
        TorrentInfo {
            info_hash: None,
            magnet_link: Some(magnet_link),
            torrent_file: None,
        } => {
            let hash_str = magnet_link
                .split("btih:")
                .nth(1)
                .and_then(|s| s.get(0..40))
                .ok_or_else(|| eyre!("Invalid magnet link: could not find info hash"))?;

            let info_hash = decode_info_hash_hex(hash_str)
                .wrap_err("Failed to decode hash from magnet link")?;

            State::unstarted(
                info_hash,
                root.clone(),
                bt_config,
            )
        }
        _ => unreachable!(),
    };

    let info_hash_id = Id::from_bytes(state.info_hash()).wrap_err("Invalid info_hash")?;

    let mut event_q: Queue<TorrentEvent, 512> = heapless::spsc::Queue::new();

    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(256);
    let (event_tx, event_rc) = event_q.split();

    let addr = format!("0.0.0.0:{}", vortex_config.port.unwrap_or_default());
    let listener =
        TcpListener::bind(&addr).wrap_err(format!("Failed binding tcp listener to {addr}"))?;

    let port = listener.local_addr().unwrap().port();
    let id = PeerId::generate();
    let metadata = state.as_ref().metadata().cloned();
    let is_complete = state.is_complete();
    let mut torrent = Torrent::new(id, state);
    let (shutdown_signal_tx, shutdown_signal_rc) = bounded(1);

    let dht_cache_path = paths.dht_cache.clone();
    std::thread::scope(|s| {
        s.spawn(move || {
            torrent.start(event_tx, command_rc, listener).unwrap();
        });
        let cmd_tx_clone = command_tx.clone();
        s.spawn(move || {
            dht_thread(
                info_hash_id,
                port,
                cmd_tx_clone,
                shutdown_signal_rc,
                dht_cache_path,
                cli.skip_dht_cache,
            )
            .expect("DHT thread failed")
        });

        let mut app = VortexApp::new(
            command_tx,
            event_rc,
            shutdown_signal_tx,
            metadata,
            root,
            is_complete,
        );
        let terminal = ratatui::init();
        let result = app.run(terminal);
        ratatui::restore();
        result
    })
}

impl Widget for &mut VortexApp<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let [progress_area, graph_area, info_area] = Layout::vertical([
            Constraint::Length(4),
            Constraint::Fill(1),
            Constraint::Max(5),
        ])
        .areas(area);

        let progress_state = match self.state() {
            AppState::DownloadingMetadata => ProgressState::DownloadingMetadata {
                metadata_progress: self.best_metadata_progress,
            },
            AppState::Seeding => ProgressState::Seeding,
            AppState::Downloading => {
                if let Some(metadata) = &self.metadata {
                    let download_throughput = self
                        .total_download_throughput
                        .recent()
                        .map(|(_, throughput)| *throughput)
                        .unwrap_or_default();

                    ProgressState::Downloading {
                        pieces_completed: self.pieces_completed,
                        total_pieces: metadata.pieces.len(),
                        piece_length: metadata.piece_length,
                        total_length: metadata.length,
                        download_throughput,
                    }
                } else {
                    ProgressState::DownloadingMetadata {
                        metadata_progress: self.best_metadata_progress,
                    }
                }
            }
        };

        ProgressBar::new(progress_state).render(progress_area, buf);

        let throughput_data = ThroughputData {
            download: extract_throughput_data(&self.total_download_throughput),
            upload: extract_throughput_data(&self.total_upload_throughput),
        };

        ThroughputGraph::new(throughput_data).render(graph_area, buf);

        let info_data = InfoData {
            download_throughput: self
                .total_download_throughput
                .recent()
                .copied()
                .map(|(_, v)| v)
                .unwrap_or_default(),
            upload_throughput: self
                .total_upload_throughput
                .recent()
                .copied()
                .map(|(_, v)| v)
                .unwrap_or_default(),
            num_connections: self.num_connections,
            name: self
                .metadata
                .as_ref()
                .map(|meta| meta.name.to_owned())
                .unwrap_or("unknown".to_owned()),
            time: self.time_field,
        };

        InfoPanel::new(info_data).render(info_area, buf);
    }
}
