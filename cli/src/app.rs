//! Application state and lifecycle management.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::SyncSender,
    },
    time::{Duration, Instant, SystemTime},
};

use color_eyre::eyre::Context;
use crossbeam_channel::Sender;
use heapless::{HistoryBuf, spsc::Consumer};
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    widgets::ListState,
};
use vortex_bittorrent::{Command, MetadataProgress, State, TorrentEvent};

use crate::ui::Time;

#[allow(clippy::large_enum_variant)]
/// Current state of the torrent application.
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
    /// A torrent has been picked but its threads haven't been started yet.
    SelectedTorrent { state: State },
    /// Picking among the torrents with stored metadata
    SelectingTorrent {
        items: Vec<(String, String)>,
        /// Owns both the selected index and the scroll offset
        list_state: ListState,
        /// The item awaiting delete confirmation, if any
        pending_delete_index: Option<usize>,
        metadata_path: PathBuf,
        download_root: PathBuf,
    },
}

pub const METADATA_DIR: &str = "metadata";

#[derive(Clone)]
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

pub struct VortexApp<'queue, F> {
    pub setup: AppSetup<'queue>,
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
    /// The progress for downloading metada from the furthest along peer
    pub best_metadata_progress: MetadataProgress,
    pub time_field: Time,
    pub state: AppState,
    /// Used to kick of the vortex bittorrent threads
    pub spawn_torrent_threads: Option<F>,
}

impl<'queue, F> VortexApp<'queue, F> {
    fn started_state(state: &State, metadata: &Metadata) -> AppState {
        if state.is_complete() {
            AppState::Seeding
        } else if metadata.is_none() {
            AppState::DownloadingMetadata
        } else {
            AppState::Downloading
        }
    }

    pub fn shutdown(&mut self) {
        let _ = self.setup.cmd_tx.send(Command::Stop);
        let _ = self.setup.shutdown_signal_tx.send(());
        self.should_exit = true;
    }
}

pub struct AppSetup<'queue> {
    /// Command sender to the torrent event loop
    pub cmd_tx: SyncSender<Command>,
    /// Event receiver from the torrent event loop
    pub event_rc: Consumer<'queue, TorrentEvent>,
    /// Signal sender to shutdown other threads
    pub shutdown_signal_tx: Sender<()>,
    /// Torrent metadata (available after metadata download)
    pub metadata: Metadata,
    /// Root directory for downloads
    pub root: PathBuf,
    /// Flag stating if the dht should pause or not
    pub pause_dht: Arc<AtomicBool>,
    /// Config used
    pub config: vortex_bittorrent::Config,
}

impl<'queue, F: FnOnce(State) -> color_eyre::Result<()>> VortexApp<'queue, F> {
    pub fn new(
        setup: AppSetup<'queue>,
        mut app_state: AppState,
        spawn_torrent_threads: F,
    ) -> color_eyre::Result<Self> {
        let spawn_torrent_threads = if let AppState::SelectedTorrent { state } = app_state {
            app_state = Self::started_state(&state, &setup.metadata);
            spawn_torrent_threads(state)?;
            None
        } else {
            Some(spawn_torrent_threads)
        };
        Ok(Self {
            setup,
            should_exit: false,
            start_time: Instant::now(),
            pieces_completed: 0,
            total_download_throughput: Box::new(HistoryBuf::new()),
            total_upload_throughput: Box::new(HistoryBuf::new()),
            num_connections: 0,
            best_metadata_progress: Default::default(),
            time_field: Time::StartedAt(SystemTime::now()),
            spawn_torrent_threads,
            state: app_state,
        })
    }

    pub fn run(&mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    pub fn handle_events(&mut self) -> color_eyre::Result<()> {
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
            AppState::SelectedTorrent { state } => {
                // Start torrent threads once a torrent has been selected
                let spawn_torrent_threads = self
                    .spawn_torrent_threads
                    .take()
                    .expect("Must only select torrent once");
                let new_state = Self::started_state(state, &self.setup.metadata);
                let AppState::SelectedTorrent { state } =
                    std::mem::replace(&mut self.state, new_state)
                else {
                    unreachable!();
                };
                spawn_torrent_threads(state)?;
            }
            // No torrent is running while the menu is up, so there are no torrent
            // events to drain and the keys mean something else entirely.
            AppState::SelectingTorrent { .. } => return self.handle_selection_events(),
            _ => {}
        }
        while let Some(event) = self.setup.event_rc.dequeue() {
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
                            } else if self.setup.metadata.is_none() {
                                AppState::DownloadingMetadata
                            } else {
                                AppState::Downloading
                            }
                        }
                        AppState::Seeding => AppState::Seeding,
                        _ => {
                            if self.setup.metadata.is_none() {
                                AppState::DownloadingMetadata
                            } else {
                                AppState::Downloading
                            }
                        }
                    };
                    self.setup.pause_dht.store(false, Ordering::Relaxed);
                }
                TorrentEvent::Paused => {
                    self.state = AppState::Paused {
                        was_seeding: matches!(self.state, AppState::Seeding),
                        last_simulated_update: Instant::now(),
                    };
                    self.setup.pause_dht.store(true, Ordering::Relaxed);
                }
                TorrentEvent::MetadataComplete(metadata) => {
                    self.setup.metadata = Metadata::Full(Box::new((*metadata).clone()));
                    self.state = AppState::Downloading;
                    self.time_field = Time::StartedAt(SystemTime::now());
                    let root = self.setup.root.clone();
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
                    progress,
                    peer_metrics,
                    ..
                } => {
                    self.pieces_completed = progress.as_ref().map_or(0, |p| p.total_completed());
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
                let _ = self.setup.cmd_tx.send(Command::Pause);
            }
            KeyCode::Char('r') if matches!(self.state, AppState::Paused { .. }) => {
                let _ = self.setup.cmd_tx.send(Command::Resume);
            }
            _ => {}
        }
    }

    fn handle_selection_events(&mut self) -> color_eyre::Result<()> {
        if event::poll(Duration::from_millis(16))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && let AppState::SelectingTorrent {
                items,
                list_state,
                pending_delete_index,
                metadata_path,
                download_root,
            } = &mut self.state
        {
            // The list clamps the selection when rendering, so it's only ever out of
            // range between a keypress and the next draw.
            let selected = list_state.selected().filter(|idx| *idx < items.len());
            if let Some(idx) = *pending_delete_index {
                match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') => {
                        let (hash, name) = &items[idx];

                        let base_name = if name.is_empty() {
                            hash.as_str()
                        } else {
                            name.as_str()
                        };
                        let file_path = download_root.join(base_name);
                        if file_path.exists() {
                            if file_path.is_file() {
                                let _ = std::fs::remove_file(&file_path);
                            } else if file_path.is_dir() {
                                let _ = std::fs::remove_dir_all(&file_path);
                            }
                        }

                        let metadata_file = metadata_path.join(hash);
                        let _ = std::fs::remove_file(&metadata_file);

                        items.remove(idx);
                        *pending_delete_index = None;

                        if items.is_empty() {
                            // Not much to do here
                            self.should_exit = true;
                        }
                    }
                    KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                        *pending_delete_index = None;
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    KeyCode::Up => list_state.select_previous(),
                    KeyCode::Down => list_state.select_next(),
                    KeyCode::Enter => {
                        if let Some(idx) = selected {
                            let (hash, _) = items[idx].clone();
                            let metadata = lava_torrent::torrent::v1::Torrent::read_from_file(
                                self.setup.root.join(METADATA_DIR).join(hash.to_lowercase()),
                            )
                            .context("Failed to open metadata file")?;
                            let state = State::from_metadata_and_root(
                                metadata.clone(),
                                self.setup.root.clone(),
                                self.setup.config,
                            )?;
                            self.setup.metadata = Metadata::Full(Box::new(metadata));
                            self.state = AppState::SelectedTorrent { state };
                        }
                    }
                    KeyCode::Char('d') | KeyCode::Char('D') => {
                        *pending_delete_index = selected;
                    }
                    KeyCode::Char('q') => {
                        self.should_exit = true;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

impl<F> Drop for VortexApp<'_, F> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
