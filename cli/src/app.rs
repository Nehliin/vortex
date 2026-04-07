//! Application state and lifecycle management.

use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::SyncSender,
    },
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::Sender;
use heapless::{HistoryBuf, spsc::Consumer};
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};
use vortex_bittorrent::{Command, MetadataProgress, TorrentEvent};

use crate::ui::Time;

/// Current state of the torrent application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppState {
    /// Downloading torrent metadata from peers
    DownloadingMetadata,
    /// Actively downloading torrent data
    Downloading,
    /// Download complete, seeding to other peers
    Seeding,
    /// Torrent is paused
    Paused {
        // To ensure we return to the correct state
        was_seeding: bool,
        // Keep track of the last simulated data to the graph
        last_simulated_update: Instant,
    },
}

pub const METADATA_DIR: &str = "metadata";

pub enum Metadata {
    /// Full metadata is available
    Full(Box<lava_torrent::torrent::v1::Torrent>),
    /// Metadata is being downloaded, with the name of the torrent if available
    Partial { name: String },
    /// No metadata available
    None,
}

impl Metadata {
    pub fn is_none(&self) -> bool {
        matches!(self, Metadata::None)
    }
}

pub struct VortexApp<'queue> {
    /// Command sender to the torrent event loop
    pub cmd_tx: SyncSender<Command>,
    /// Event receiver from the torrent event loop
    pub event_rc: Consumer<'queue, TorrentEvent>,
    /// Flag to signal application shutdown
    pub should_exit: bool,
    /// Application start time for elapsed time calculations
    pub start_time: Instant,
    /// Historical download throughput data (time, bytes/sec)
    pub total_download_throughput: Box<HistoryBuf<(f64, f64), 256>>,
    /// Historical upload throughput data (time, bytes/sec)
    pub total_upload_throughput: Box<HistoryBuf<(f64, f64), 256>>,
    /// Number of pieces completed
    pub pieces_completed: usize,
    /// Number of active peer connections
    pub num_connections: usize,
    /// Torrent metadata (available after metadata download)
    pub metadata: Metadata,
    /// The progress for downloading metada from the furthest along peer
    pub best_metadata_progress: MetadataProgress,
    /// Root directory for downloads
    pub root: PathBuf,
    pub time_field: Time,
    /// Signal sender to shutdown other threads
    pub shutdown_signal_tx: Sender<()>,
    pub state: AppState,
    pub dht_paused: Arc<AtomicBool>,
}

impl<'queue> VortexApp<'queue> {
    pub fn new(
        cmd_tx: SyncSender<Command>,
        event_rc: Consumer<'queue, TorrentEvent>,
        shutdown_signal_tx: Sender<()>,
        metadata: Metadata,
        root: PathBuf,
        is_complete: bool,
        dht_paused: Arc<AtomicBool>,
    ) -> Self {
        let state = if is_complete {
            AppState::Seeding
        } else if metadata.is_none() {
            AppState::DownloadingMetadata
        } else {
            AppState::Downloading
        };

        Self {
            cmd_tx,
            event_rc,
            should_exit: false,
            start_time: Instant::now(),
            pieces_completed: 0,
            total_download_throughput: Box::new(HistoryBuf::new()),
            total_upload_throughput: Box::new(HistoryBuf::new()),
            num_connections: 0,
            best_metadata_progress: Default::default(),
            metadata,
            time_field: Time::StartedAt(SystemTime::now()),
            root,
            shutdown_signal_tx,
            state,
            dht_paused,
        }
    }

    pub fn run(&mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        let _ = self.cmd_tx.send(Command::Stop);
        let _ = self.shutdown_signal_tx.send(());
        self.should_exit = true;
    }

    fn draw(&mut self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    pub fn handle_events(&mut self) -> io::Result<()> {
        while let Some(event) = self.event_rc.dequeue() {
            match event {
                TorrentEvent::TorrentComplete => {
                    let download_time = match self.time_field {
                        Time::StartedAt(system_time) => system_time.elapsed().unwrap(),
                        Time::DownloadTime(duration) => duration,
                    };
                    self.time_field = Time::DownloadTime(download_time);
                    self.state = AppState::Seeding;
                }
                TorrentEvent::Running { port: _ } => {
                    self.state = match self.state {
                        AppState::Paused { was_seeding, .. } => {
                            if was_seeding {
                                AppState::Seeding
                            } else if self.metadata.is_none() {
                                AppState::DownloadingMetadata
                            } else {
                                AppState::Downloading
                            }
                        }
                        AppState::Seeding => AppState::Seeding,
                        _ => {
                            if self.metadata.is_none() {
                                AppState::DownloadingMetadata
                            } else {
                                AppState::Downloading
                            }
                        }
                    };
                    self.dht_paused.store(false, Ordering::Relaxed);
                }
                TorrentEvent::Paused => {
                    self.state = AppState::Paused {
                        was_seeding: matches!(self.state, AppState::Seeding),
                        last_simulated_update: Instant::now(),
                    };
                    self.dht_paused.store(true, Ordering::Relaxed);
                }
                TorrentEvent::MetadataComplete(metadata) => {
                    self.metadata = Metadata::Full(Box::new((*metadata).clone()));
                    self.state = AppState::Downloading;
                    self.time_field = Time::StartedAt(SystemTime::now());
                    let root = self.root.clone();
                    // Store the metadata as the info hash in the download folder, that will
                    // ensure it's possible to recover from downloads that's already started
                    // The thread is used to keep this non blocking
                    let metadata_dir = root.join(METADATA_DIR);
                    let path = metadata_dir.join(metadata.info_hash());
                    std::thread::spawn(move || {
                        if let Err(err) = std::fs::create_dir_all(&metadata_dir) {
                            log::error!("Failed to create metadata directory: {err}");
                        } else if let Err(err) = metadata.write_into_file(path) {
                            log::error!("Failed to save metadata to disk: {err}");
                        }
                    });
                }
                TorrentEvent::TorrentMetrics {
                    pieces_completed,
                    pieces_allocated: _,
                    peer_metrics,
                } => {
                    self.pieces_completed = pieces_completed;
                    self.num_connections = peer_metrics.len();
                    self.best_metadata_progress = peer_metrics
                        .iter()
                        .filter_map(|peer| peer.metadata_progress)
                        .max_by(|x, y| x.completed_pieces.cmp(&y.completed_pieces))
                        .unwrap_or_default();
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

        match &mut self.state {
            AppState::Paused {
                last_simulated_update,
                ..
            } if last_simulated_update.elapsed() >= Duration::from_secs(1) => {
                // Simulate data so the graph isn't static
                let curr_time = self.start_time.elapsed().as_secs_f64();
                self.total_download_throughput.write((curr_time, 0.0));
                self.total_upload_throughput.write((curr_time, 0.0));
                *last_simulated_update = Instant::now();
            }
            _ => {}
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
        match key_event.code {
            KeyCode::Char('q') => self.shutdown(),
            KeyCode::Char('p') if !matches!(self.state, AppState::Paused { .. }) => {
                let _ = self.cmd_tx.send(Command::Pause);
            }
            KeyCode::Char('r') if matches!(self.state, AppState::Paused { .. }) => {
                let _ = self.cmd_tx.send(Command::Resume);
            }
            _ => {}
        }
    }
}

impl Drop for VortexApp<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
