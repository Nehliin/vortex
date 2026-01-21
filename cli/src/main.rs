use std::{
    fs::OpenOptions,
    io::{self, ErrorKind},
    net::TcpListener,
    path::PathBuf,
    sync::mpsc::SyncSender,
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use crossbeam_channel::{Receiver, Sender, bounded, select, tick};
use heapless::{HistoryBuf, spsc::Consumer, spsc::Queue};
use human_bytes::human_bytes;
use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Alignment, Constraint, Layout},
    prelude::{Buffer, Rect},
    style::{Color, Modifier, Style, Stylize, palette::tailwind},
    text::{Span, Text},
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, Row, Table, Widget},
};
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppState {
    DownloadingMetadata,
    Downloading,
    Seeding,
}

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

fn dht_thread(info_hash_id: Id, cmd_tx: SyncSender<Command>, shutdown_signal_rc: Receiver<()>) {
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
        let all_peers = dht_client.get_peers(info_hash_id);
        for peers in all_peers {
            log::info!("Got {} peers", peers.len());
            cmd_tx.send(Command::ConnectToPeers(peers)).unwrap();
        }
    };

    query();
    loop {
        select! {
            recv(fetch_interval) -> _ => {
                query();
            }
            recv(shutdown_signal_rc) -> _ => {
                let bootstrap_nodes = dht_client.to_bootstrap();
                let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
                std::fs::write("dht_boostrap_nodes", dht_bootstrap_nodes_contet.as_bytes())
                    .unwrap();
                break;
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
    let target = Box::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("vortex.log")?,
    );
    env_logger::builder()
        .target(env_logger::Target::Pipe(target))
        .filter_level(log::LevelFilter::Debug)
        .init();
    let builder = PrometheusBuilder::new().with_recommended_naming(true);
    builder.install().unwrap();

    let cli = Cli::parse();

    let config = Config::default();
    let root = cli.download_folder.clone();

    let mut state = match cli.torrent_info {
        TorrentInfo {
            info_hash: Some(info_hash),
            torrent_file: None,
        } => {
            match lava_torrent::torrent::v1::Torrent::read_from_file(
                root.join(info_hash.to_lowercase()),
            ) {
                // Metadata has been saved from previous run
                Ok(metadata) => {
                    State::from_metadata_and_root(metadata, cli.download_folder, config)?
                }
                Err(lava_torrent::LavaTorrentError::Io(io_err)) => {
                    if io_err.kind() == ErrorKind::NotFound {
                        State::unstarted(
                            decode_info_hash_hex(&info_hash),
                            cli.download_folder,
                            config,
                        )
                    } else {
                        panic!("Failed looking for stored metadata {io_err}");
                    }
                }
                Err(err) => panic!("Failed looking for stored metadata {err}"),
            }
        }
        TorrentInfo {
            info_hash: None,
            torrent_file: Some(metadata),
        } => {
            let parsed_metadata =
                lava_torrent::torrent::v1::Torrent::read_from_file(metadata).unwrap();
            State::from_metadata_and_root(parsed_metadata, cli.download_folder, config)?
        }
        _ => unreachable!(),
    };

    let info_hash_id = Id::from_bytes(state.info_hash()).unwrap();

    let mut event_q: Queue<TorrentEvent, 512> = heapless::spsc::Queue::new();

    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(256);
    let (event_tx, event_rc) = event_q.split();

    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let id = PeerId::generate();
    let metadata = state.as_ref().metadata().cloned();
    let is_complete = state.is_complete();
    let mut torrent = Torrent::new(id, state);
    let (shutdown_signal_tx, shutdown_signal_rc) = bounded(1);

    std::thread::scope(|s| {
        s.spawn(move || {
            torrent.start(event_tx, command_rc, listener).unwrap();
        });
        let cmd_tx_clone = command_tx.clone();
        s.spawn(move || dht_thread(info_hash_id, cmd_tx_clone, shutdown_signal_rc));

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

struct VortexApp<'queue> {
    cmd_tx: SyncSender<Command>,
    event_rc: Consumer<'queue, TorrentEvent>,
    should_exit: bool,
    start_time: Instant,
    total_download_throughput: Box<HistoryBuf<(f64, f64), 256>>,
    total_upload_throughput: Box<HistoryBuf<(f64, f64), 256>>,
    pieces_completed: usize,
    num_connections: usize,
    num_unchoked: usize,
    metadata: Option<Box<lava_torrent::torrent::v1::Torrent>>,
    root: PathBuf,
    is_complete: bool,
    // Signal the other threads we they shutdown
    shutdown_signal_tx: Sender<()>,
}

impl<'queue> VortexApp<'queue> {
    fn new(
        cmd_tx: SyncSender<Command>,
        event_rc: Consumer<'queue, TorrentEvent>,
        shutdown_signal_tx: Sender<()>,
        metadata: Option<Box<lava_torrent::torrent::v1::Torrent>>,
        root: PathBuf,
        is_complete: bool,
    ) -> Self {
        Self {
            cmd_tx,
            event_rc,
            should_exit: false,
            start_time: Instant::now(),
            pieces_completed: 0,
            total_download_throughput: Box::new(HistoryBuf::new()),
            total_upload_throughput: Box::new(HistoryBuf::new()),
            num_connections: 0,
            num_unchoked: 0,
            is_complete,
            metadata,
            root,
            shutdown_signal_tx,
        }
    }

    fn state(&self) -> AppState {
        if self.metadata.is_none() {
            AppState::DownloadingMetadata
        } else if self.is_complete {
            AppState::Seeding
        } else {
            AppState::Downloading
        }
    }

    fn run(&mut self, mut terminal: DefaultTerminal) -> io::Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.draw(frame))?;
            // separate from torrent events?
            self.handle_events()?;
        }
        Ok(())
    }

    fn shutdown(&mut self) {
        let _ = self.cmd_tx.send(Command::Stop);
        let _ = self.shutdown_signal_tx.send(());
        self.should_exit = true;
    }

    fn draw(&mut self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    fn handle_events(&mut self) -> io::Result<()> {
        while let Some(event) = self.event_rc.dequeue() {
            match event {
                TorrentEvent::TorrentComplete => {
                    self.is_complete = true;
                }
                TorrentEvent::ListenerStarted { port: _ } => {
                    // Nothing to do here
                }
                TorrentEvent::MetadataComplete(metadata) => {
                    self.metadata = Some(metadata.clone());
                    let root = self.root.clone();
                    // Store the metadata as the info hash in the download folder, that will
                    // ensure it's possible to recover from downloads that's already started
                    // The thread is used to keep this non blocking
                    std::thread::spawn(move || {
                        let path = root.join(metadata.info_hash());
                        if let Err(err) = metadata.write_into_file(path) {
                            log::error!("Failed to save metadata to disk: {err}")
                        }
                    });
                }
                TorrentEvent::TorrentMetrics {
                    pieces_completed,
                    pieces_allocated: _,
                    peer_metrics,
                    num_unchoked,
                } => {
                    self.pieces_completed = pieces_completed;
                    self.num_connections = peer_metrics.len();
                    self.num_unchoked = num_unchoked;
                    let tick_download_throughput: u64 =
                        peer_metrics.iter().map(|val| val.download_throughput).sum();
                    let tick_upload_throughput: u64 =
                        peer_metrics.iter().map(|val| val.upload_throughput).sum();
                    let curr_time = self.start_time.elapsed().as_secs_f64();
                    self.total_download_throughput
                        .write((curr_time, tick_download_throughput as f64));
                    self.total_upload_throughput
                        .write((curr_time, tick_upload_throughput as f64));
                }
            }
        }
        if event::poll(Duration::from_millis(16))? {
            match event::read()? {
                // it's important to check that the event is a key press event as
                // crossterm also emits key release and repeat events on Windows.
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            };
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if let KeyCode::Char('q') = key_event.code {
            self.shutdown()
        }
    }

    fn render_progress_bar(&self, area: Rect, buf: &mut Buffer) {
        let state = self.state();
        let is_dimmed = state == AppState::DownloadingMetadata;

        let (percent, label, gauge_color) = match state {
            AppState::DownloadingMetadata => {
                (0, "Downloading metadata...".to_string(), Color::Gray)
            }
            AppState::Seeding => (100, "Seeding".to_string(), Color::Cyan),
            AppState::Downloading => {
                if let Some(metadata) = &self.metadata {
                    let pct = (100.0
                        * (self.pieces_completed as f64 / metadata.pieces.len() as f64))
                        as u16;
                    let remaining =
                        metadata.length - self.pieces_completed as i64 * metadata.piece_length;

                    let maybe_seconds_left = (remaining as u64).checked_div(
                        self.total_download_throughput
                            .recent()
                            .map(|(_, throughput)| *throughput as u64)
                            .unwrap_or_default(),
                    );
                    let lbl = if let Some(seconds_left) = maybe_seconds_left {
                        let estimated_time_left =
                            humantime::Duration::from(Duration::from_secs(seconds_left));
                        format!("{pct}% (est {estimated_time_left} remaining)")
                    } else {
                        format!("{pct}%")
                    };
                    (pct, lbl, Color::Green)
                } else {
                    (0, "0%".to_string(), Color::Green)
                }
            }
        };

        let border_color = if is_dimmed {
            tailwind::SLATE.c600
        } else {
            tailwind::SLATE.c200
        };

        let block = Block::new()
            .borders(Borders::all())
            .border_type(ratatui::widgets::BorderType::Rounded)
            .fg(border_color);

        Gauge::default()
            .block(block)
            .gauge_style(gauge_color)
            .label(label)
            .percent(percent)
            .render(area, buf);
    }

    fn render_combined_graph(&self, area: Rect, buf: &mut Buffer, is_dimmed: bool) {
        let download_data: Vec<(f64, f64)> = self
            .total_download_throughput
            .oldest_ordered()
            .copied()
            .collect();
        let upload_data: Vec<(f64, f64)> = self
            .total_upload_throughput
            .oldest_ordered()
            .copied()
            .collect();

        let (oldest_time, _) = download_data.first().copied().unwrap_or((0.0, 0.0));
        let (newest_time, _) = download_data.last().copied().unwrap_or((0.0, 0.0));

        let all_values: Vec<f64> = download_data
            .iter()
            .chain(upload_data.iter())
            .map(|(_, v)| *v)
            .collect();

        let smallest = all_values
            .iter()
            .copied()
            .min_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);

        let largest = all_values
            .iter()
            .copied()
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);

        let dimmed_color = tailwind::SLATE.c500;

        let label_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        };

        let x_labels = vec![
            Span::styled("".to_string(), label_style),
            Span::styled("Time ".to_string(), label_style),
        ];
        let y_labels = vec![
            Span::styled(
                format!("{}/s", human_bytes::human_bytes(smallest)),
                label_style,
            ),
            Span::styled(
                format!("{}/s", human_bytes::human_bytes((smallest + largest) / 2.0)),
                label_style,
            ),
            Span::styled(
                format!("{}/s", human_bytes::human_bytes(largest)),
                label_style,
            ),
        ];

        let download_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default().fg(Color::Green)
        };
        let upload_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default().fg(Color::Magenta)
        };

        let datasets = vec![
            Dataset::default()
                .name("Download")
                .graph_type(ratatui::widgets::GraphType::Line)
                .marker(ratatui::symbols::Marker::Braille)
                .style(download_style)
                .data(&download_data),
            Dataset::default()
                .name("Upload")
                .graph_type(ratatui::widgets::GraphType::Line)
                .marker(ratatui::symbols::Marker::Braille)
                .style(upload_style)
                .data(&upload_data),
        ];

        let border_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default()
        };

        let axis_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default().fg(Color::Gray)
        };

        let chart = Chart::new(datasets)
            .block(
                Block::bordered()
                    .title("Throughput")
                    .title_alignment(Alignment::Center)
                    .border_type(ratatui::widgets::BorderType::Rounded)
                    .border_style(border_style),
            )
            .x_axis(
                Axis::default()
                    .style(axis_style)
                    .labels(x_labels)
                    .bounds([oldest_time, newest_time]),
            )
            .y_axis(
                Axis::default()
                    .style(axis_style)
                    .labels(y_labels)
                    .bounds([smallest, largest]),
            )
            // Ensure it's not in the way of the current progress
            .legend_position(Some(ratatui::widgets::LegendPosition::TopLeft));

        chart.render(area, buf);
    }

    fn render_info(&self, area: Rect, buf: &mut Buffer, is_dimmed: bool) {
        const HEADERS: [&str; 5] = [
            "Download speed:",
            "Upload speed:",
            "Peers:",
            "Unchoked:",
            "Name:",
        ];

        let download_throughput = self
            .total_download_throughput
            .recent()
            .copied()
            .map(|(_, v)| v)
            .unwrap_or_default();

        let upload_throughput = self
            .total_upload_throughput
            .recent()
            .copied()
            .map(|(_, v)| v)
            .unwrap_or_default();

        let name = self
            .metadata
            .as_ref()
            .map(|meta| meta.name.to_owned())
            .unwrap_or("unknown".to_owned());

        let dimmed_color = tailwind::SLATE.c500;
        let style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default()
        };

        let row = Row::new([
            Text::styled(format!("{}/s", human_bytes(download_throughput)), style),
            Text::styled(format!("{}/s", human_bytes(upload_throughput)), style),
            Text::styled(format!("{}", self.num_connections), style),
            Text::styled(format!("{}", self.num_unchoked), style),
            Text::styled(name, style),
        ]);

        let border_style = if is_dimmed {
            Style::default().fg(dimmed_color)
        } else {
            Style::default()
        };

        Table::new(vec![row], [Constraint::Fill(1); 5])
            .header(Row::new(HEADERS).style(style))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style),
            )
            .style(style)
            .render(area, buf);
    }
}

impl Widget for &mut VortexApp<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let [progress_area, graph_area, info_area] = Layout::vertical([
            Constraint::Length(4), // Progress bar
            Constraint::Fill(1),   // Graph
            Constraint::Max(5),    // Info area
        ])
        .areas(area);

        let is_dimmed = self.state() == AppState::DownloadingMetadata;
        self.render_progress_bar(progress_area, buf);
        self.render_combined_graph(graph_area, buf, is_dimmed);
        self.render_info(info_area, buf, is_dimmed);
    }
}

impl Drop for VortexApp<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
