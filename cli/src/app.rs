//! Application state and lifecycle management.

use std::{
    io,
    path::PathBuf,
    sync::mpsc::SyncSender,
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::Sender;
use heapless::{HistoryBuf, spsc::Consumer};
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};
use vortex_bittorrent::{Command, TorrentEvent};

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
    pub metadata: Option<Box<lava_torrent::torrent::v1::Torrent>>,
    /// Root directory for downloads
    pub root: PathBuf,
    /// Whether the torrent download is complete
    pub is_complete: bool,
    pub time_field: Time,
    /// Signal sender to shutdown other threads
    pub shutdown_signal_tx: Sender<()>,
}

impl<'queue> VortexApp<'queue> {
    pub fn new(
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
            is_complete,
            metadata,
            time_field: Time::StartedAt(SystemTime::now()),
            root,
            shutdown_signal_tx,
        }
    }

    pub fn state(&self) -> AppState {
        if self.metadata.is_none() {
            AppState::DownloadingMetadata
        } else if self.is_complete {
            AppState::Seeding
        } else {
            AppState::Downloading
        }
    }

    pub fn run(&mut self, mut terminal: DefaultTerminal) -> io::Result<()> {
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
                    self.is_complete = true;
                }
                TorrentEvent::ListenerStarted { port: _ } => {
                    // Nothing to do here
                }
                TorrentEvent::MetadataComplete(metadata) => {
                    self.metadata = Some(metadata.clone());
                    self.time_field = Time::StartedAt(SystemTime::now());
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
                } => {
                    self.pieces_completed = pieces_completed;
                    self.num_connections = peer_metrics.len();
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
}

impl Drop for VortexApp<'_> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
