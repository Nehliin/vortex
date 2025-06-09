use std::{
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use crossbeam_channel::{Receiver, Sender, bounded, select, tick};
use heapless::{spsc::{Consumer, Producer}, HistoryBuffer};
use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Layout},
    prelude::{Buffer, Rect},
    style::{Modifier, Style, Stylize, palette::tailwind},
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, Padding, Widget},
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
        //println!("DHT query");
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
    log_builder.filter_level(log::LevelFilter::Off).init();
    let builder = PrometheusBuilder::new();
    builder.install().unwrap();

    let cli = Cli::parse();

    let mut state = match cli.torrent_info {
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
    let metadata = state.as_ref().state().map(|state| state.0.metadata.clone());
    let mut torrent = Torrent::new(id, state);
    let (shutdown_signal_tx, shutdown_signal_rc) = bounded(1);

    std::thread::scope(|s| {
        s.spawn(move || {
            torrent.start(event_tx, command_rc).unwrap();
            println!("T thread shutdown");
        });
        let cmd_tx_clone = command_tx.clone();
        s.spawn(move || dht_thread(info_hash_id, cmd_tx_clone, shutdown_signal_rc));

        let mut app = VortexApp::new(command_tx, event_rc, shutdown_signal_tx, metadata);
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
    peer_throughput: ahash::HashMap<usize, u64>,
    // doesn't work
    total_throughput: Box<HistoryBuffer<(f64, f64), 16>>,
    pieces_completed: usize,
    num_connections: usize,
    metadata: Option<Box<lava_torrent::torrent::v1::Torrent>>,
    // Signal the other threads we they shutdown
    shutdown_signal_tx: Sender<()>,
}

impl<'queue> VortexApp<'queue> {
    fn new(
        cmd_tx: Arc<Mutex<Producer<'queue, Command, 64>>>,
        event_rc: Consumer<'queue, TorrentEvent, 512>,
        shutdown_signal_tx: Sender<()>,
        metadata: Option<Box<lava_torrent::torrent::v1::Torrent>>,
    ) -> Self {
        Self {
            cmd_tx,
            event_rc,
            should_exit: false,
            start_time: Instant::now(),
            pieces_completed: 0,
            total_throughput: Box::new(HistoryBuffer::new()),
            num_connections: 0,
            peer_throughput: Default::default(),
            metadata,
            shutdown_signal_tx,
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
        let _ = self.cmd_tx.lock().enqueue(Command::Stop);
        let _ = self.shutdown_signal_tx.send(());
        self.should_exit = true;
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    fn handle_events(&mut self) -> io::Result<()> {
        while let Some(event) = self.event_rc.dequeue() {
            match event {
                TorrentEvent::TorrentComplete => {
                    //let elapsed = self.start_time.elapsed();
                    self.shutdown();
                }
                TorrentEvent::MetadataComplete(metadata) => {
                    self.metadata = Some(metadata);
                }
                TorrentEvent::PeerMetrics {
                    conn_id,
                    throuhgput,
                    endgame: _,
                    snubbed: _,
                } => {
                    // println!(
                    //     "throuhgput: {conn_id} {throuhgput} endgame: {endgame}, snubbed: {snubbed}"
                    // );
                    self.peer_throughput
                        .entry(conn_id)
                        .and_modify(|val| *val = throuhgput)
                        .or_insert(throuhgput);
                }
                TorrentEvent::TorrentMetrics {
                    pieces_completed,
                    pieces_allocated: _,
                    num_connections,
                } => {
                    self.pieces_completed = pieces_completed;
                    self.num_connections = num_connections;
                    let tick_throuhput: u64 = self.peer_throughput.values().copied().sum();
                    let curr_time = self.start_time.elapsed().as_secs_f64();
                    self.total_throughput
                        .write((curr_time, tick_throuhput as f64));
                }
            }
            // TODO: Higher than necessary, updates are slower than this
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
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if let KeyCode::Char('q') = key_event.code {
            self.shutdown()
        }
    }

    fn render_throughput(&self, top: Rect, buf: &mut Buffer) {
        let (oldest_time, _) = self.total_throughput.oldest_ordered().next().copied().unwrap_or((0.0, 0.0));
        let (newest_time, _) = self.total_throughput.recent().copied().unwrap_or((0.0, 0.0));

        let smallest = self
            .total_throughput
            .oldest_ordered()
            .copied()
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(_, throughput)| throughput)
            .unwrap_or(0.0);

        let largest = self
            .total_throughput
            .oldest_ordered()
            .copied()
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(_, throughput)| throughput)
            .unwrap_or(0.0);

        let x_labels = vec![
            Span::styled(
                format!("{oldest_time}s"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}s", (newest_time + oldest_time) / 2.0),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{newest_time}s"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ];
        let y_labels = vec![
            Span::styled(
                format!("{}/s", human_bytes::human_bytes(smallest)),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}/s", human_bytes::human_bytes((smallest + largest) / 2.0)),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}/s", human_bytes::human_bytes(largest)),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ];
        let data = vec![
            Dataset::default()
                .graph_type(ratatui::widgets::GraphType::Line)
                .marker(ratatui::symbols::Marker::Braille)
                .style(Style::default().fg(ratatui::style::Color::Green))
                .data(&self.total_throughput),
        ];

        let chart = Chart::new(data)
            .block(Block::bordered().border_type(ratatui::widgets::BorderType::Rounded))
            .x_axis(
                Axis::default()
                    .style(Style::default().fg(ratatui::style::Color::Gray))
                    .labels(x_labels)
                    .bounds([oldest_time, newest_time]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(ratatui::style::Color::Gray))
                    .labels(y_labels)
                    .bounds([smallest, largest]),
            );

        chart.render(top, buf);
    }

    fn render_completion(&self, bottom: Rect, buf: &mut Buffer) {
        if let Some(metadata) = &self.metadata {
            let percent =
                (100.0 * (self.pieces_completed as f64 / metadata.pieces.len() as f64)) as u16;
            let title = title_block(&metadata.name);
            Gauge::default()
                .block(title)
                .gauge_style(tailwind::GREEN.c400)
                .percent(percent)
                .render(bottom, buf);
        } else {
            let title = title_block("Downloading metadata...");
            Gauge::default()
                .block(title)
                .gauge_style(tailwind::GREEN.c400)
                .percent(0)
                .render(bottom, buf);
        }
    }
}

impl Widget for &VortexApp<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(80), Constraint::Fill(1)]).areas(area);
        self.render_throughput(top, buf);
        self.render_completion(bottom, buf);
    }
}

impl Drop for VortexApp<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn title_block(title: &str) -> Block {
    let title = Line::from(title).centered();
    Block::new()
        .borders(Borders::all())
        .border_type(ratatui::widgets::BorderType::Rounded)
        .padding(Padding::vertical(1))
        .title(title)
        .fg(tailwind::SLATE.c200)
}
