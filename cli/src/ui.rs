//! UI components for the TUI application.

use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use clap::CommandFactory;
use color_eyre::eyre::Result as EyreResult;
use heapless::HistoryBuf;
use human_bytes::human_bytes;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    layout::{Alignment, Constraint, Layout},
    prelude::{Buffer, Rect},
    style::{Color, Modifier, Style, Stylize, palette::tailwind},
    text::Span,
    widgets::{
        Axis, Block, Borders, Chart, Dataset, Gauge, List, ListItem, ListState, Paragraph, Row,
        Table, Widget,
    },
};
use vortex_bittorrent::MetadataProgress;

use crate::{Cli, app::AppState};

fn download_style() -> Style {
    Style::default().fg(Color::Green)
}

fn upload_style() -> Style {
    Style::default().fg(Color::Magenta)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProgressState {
    DownloadingMetadata {
        metadata_progress: MetadataProgress,
    },
    Downloading {
        pieces_completed: usize,
        total_pieces: usize,
        piece_length: i64,
        total_length: i64,
        download_throughput: f64,
    },
    Seeding,
    PausedSeeding,
    PausedDownloading {
        pieces_completed: usize,
        total_pieces: usize,
    },
    PausedMetadata {
        metadata_progress: MetadataProgress,
    },
}

pub struct ProgressBar {
    state: ProgressState,
}

impl ProgressBar {
    pub fn new(state: ProgressState) -> Self {
        Self { state }
    }

    fn calculate_display(&self) -> (u16, String, Color) {
        match self.state {
            ProgressState::DownloadingMetadata { metadata_progress } => {
                let pct = if metadata_progress.total_piece == 0 {
                    0
                } else {
                    (100.0
                        * (metadata_progress.completed_pieces as f64
                            / metadata_progress.total_piece as f64)) as u16
                };
                (pct, "Downloading metadata...".to_string(), Color::Gray)
            }
            ProgressState::Seeding => (100, "Seeding".to_string(), Color::Cyan),
            ProgressState::PausedSeeding => (100, "Paused".to_string(), Color::DarkGray),
            ProgressState::PausedDownloading {
                pieces_completed,
                total_pieces,
            } => {
                let pct = (100.0 * (pieces_completed as f64 / total_pieces as f64)) as u16;
                (pct, format!("Paused ({pct}%)"), Color::DarkGray)
            }
            ProgressState::PausedMetadata { metadata_progress } => {
                let pct = if metadata_progress.total_piece == 0 {
                    0
                } else {
                    (100.0
                        * (metadata_progress.completed_pieces as f64
                            / metadata_progress.total_piece as f64)) as u16
                };
                (
                    pct,
                    format!("Paused (downloading metadata {pct}%)"),
                    Color::DarkGray,
                )
            }
            ProgressState::Downloading {
                pieces_completed,
                total_pieces,
                piece_length,
                total_length,
                download_throughput,
            } => {
                let pct = (100.0 * (pieces_completed as f64 / total_pieces as f64)) as u16;
                let remaining = total_length - pieces_completed as i64 * piece_length;

                let maybe_seconds_left = (remaining as u64)
                    .checked_div(download_throughput as u64)
                    .filter(|&s| s > 0);

                let label = if let Some(seconds_left) = maybe_seconds_left {
                    let estimated_time_left =
                        humantime::Duration::from(Duration::from_secs(seconds_left));
                    format!("{pct}% (est {estimated_time_left} remaining)")
                } else {
                    format!("{pct}%")
                };

                (pct, label, Color::Green)
            }
        }
    }
}

impl Widget for ProgressBar {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let (percent, label, gauge_color) = self.calculate_display();

        let block = Block::new()
            .borders(Borders::all())
            .border_type(ratatui::widgets::BorderType::Rounded)
            .fg(tailwind::SLATE.c200);

        Gauge::default()
            .block(block)
            .gauge_style(gauge_color)
            .label(label)
            .percent(percent)
            .render(area, buf);
    }
}

pub struct ThroughputData {
    pub download: Vec<(f64, f64)>,
    pub upload: Vec<(f64, f64)>,
}

pub struct ThroughputGraph {
    data: ThroughputData,
}

impl ThroughputGraph {
    pub fn new(data: ThroughputData) -> Self {
        Self { data }
    }

    fn calculate_value_bounds(&self) -> (f64, f64) {
        let all_values: Vec<f64> = self
            .data
            .download
            .iter()
            .chain(self.data.upload.iter())
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

        (smallest, largest)
    }
}

impl Widget for ThroughputGraph {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let label_style = Style::default().add_modifier(Modifier::BOLD);

        let (oldest_time, _) = self.data.download.first().copied().unwrap_or((0.0, 0.0));
        let (newest_time, _) = self.data.download.last().copied().unwrap_or((0.0, 0.0));

        let (smallest, largest) = self.calculate_value_bounds();

        let x_labels = vec![
            Span::styled("".to_string(), label_style),
            Span::styled("Time ".to_string(), label_style),
        ];
        let y_labels = vec![
            Span::styled(format!("{}/s", human_bytes(smallest)), label_style),
            Span::styled(
                format!("{}/s", human_bytes((smallest + largest) / 2.0)),
                label_style,
            ),
            Span::styled(format!("{}/s", human_bytes(largest)), label_style),
        ];

        let datasets = vec![
            Dataset::default()
                .name("Download")
                .graph_type(ratatui::widgets::GraphType::Line)
                .marker(ratatui::symbols::Marker::Braille)
                .style(download_style())
                .data(&self.data.download),
            Dataset::default()
                .name("Upload")
                .graph_type(ratatui::widgets::GraphType::Line)
                .marker(ratatui::symbols::Marker::Braille)
                .style(upload_style())
                .data(&self.data.upload),
        ];

        let border_style = Style::default();
        let axis_style = Style::default().fg(Color::Gray);

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
            .legend_position(Some(ratatui::widgets::LegendPosition::TopLeft));

        chart.render(area, buf);
    }
}

#[derive(Clone, Copy)]
pub enum Time {
    StartedAt(SystemTime),
    DownloadTime(Duration),
}

pub struct InfoData {
    pub name: String,
    pub download_throughput: f64,
    pub upload_throughput: f64,
    pub num_connections: usize,
    pub time: Time,
}

pub struct InfoPanel {
    data: InfoData,
}

impl InfoPanel {
    pub fn new(data: InfoData) -> Self {
        Self { data }
    }
}

impl Widget for InfoPanel {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let default_style = Style::default();

        let (time_header_text, time_text, time_style) = match self.data.time {
            Time::StartedAt(system_time) => (
                "Started at:",
                humantime::format_rfc3339_seconds(system_time).to_string(),
                default_style,
            ),
            Time::DownloadTime(duration) => (
                "Downloaded in:",
                // only keep seconds
                humantime::Duration::from(Duration::from_secs(duration.as_secs())).to_string(),
                Style::default().fg(Color::Green),
            ),
        };
        let headers: [&str; 5] = [
            "Name:",
            "Download speed:",
            "Upload speed:",
            "Peers:",
            time_header_text,
        ];

        let row = Row::new([
            ratatui::text::Text::styled(self.data.name, default_style),
            ratatui::text::Text::styled(
                format!("{}/s", human_bytes(self.data.download_throughput)),
                download_style(),
            ),
            ratatui::text::Text::styled(
                format!("{}/s", human_bytes(self.data.upload_throughput)),
                upload_style(),
            ),
            ratatui::text::Text::styled(format!("{}", self.data.num_connections), default_style),
            ratatui::text::Text::styled(time_text, time_style),
        ]);

        Table::new(vec![row], [ratatui::layout::Constraint::Fill(1); 5])
            .header(Row::new(headers).style(default_style))
            .block(
                Block::default()
                    .borders(Borders::TOP | Borders::LEFT | Borders::RIGHT)
                    .border_type(ratatui::widgets::BorderType::Rounded),
            )
            .style(default_style)
            .render(area, buf);
    }
}

pub struct Footer {
    pub state: AppState,
}

impl Widget for Footer {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let key_style = Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD);
        let desc_style = Style::default().fg(Color::Gray);
        let sep_style = Style::default().fg(Color::DarkGray);

        let mut spans = vec![
            Span::styled(" q ", key_style),
            Span::styled("quit", desc_style),
            Span::styled(" │ ", sep_style),
        ];

        if matches!(self.state, AppState::Paused { .. }) {
            spans.push(Span::styled("r ", key_style));
            spans.push(Span::styled("resume ", desc_style));
        } else {
            spans.push(Span::styled("p ", key_style));
            spans.push(Span::styled("pause ", desc_style));
        }

        let title = ratatui::text::Line::from(spans);
        Block::default()
            .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
            .border_type(ratatui::widgets::BorderType::Rounded)
            .title_bottom(title.centered())
            .render(area, buf);
    }
}

pub fn extract_throughput_data(buf: &HistoryBuf<(f64, f64), 256>) -> Vec<(f64, f64)> {
    buf.oldest_ordered().copied().collect()
}

pub fn select_torrent(
    metadata_path: PathBuf,
    download_root: PathBuf,
) -> EyreResult<Option<String>> {
    std::fs::create_dir_all(&metadata_path)?;

    let mut items = Vec::new();
    for entry in std::fs::read_dir(&metadata_path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();
        if name_str.len() == 40 && name_str.chars().all(|c| c.is_ascii_hexdigit()) {
            let hash = name_str.to_string();
            let display_name = lava_torrent::torrent::v1::Torrent::read_from_file(entry.path())
                .ok()
                .map(|t| t.name)
                .unwrap_or_else(|| hash.clone());

            items.push((hash, display_name));
        }
    }

    if items.is_empty() {
        Cli::command().print_help().unwrap();
        println!();
        return Ok(None);
    }

    let mut terminal = ratatui::init();
    let result = run_selection_menu(&mut terminal, items, metadata_path, download_root)?;
    ratatui::restore();
    Ok(result)
}

struct SelectionState {
    items: Vec<(String, String)>,
    selected_index: usize,
    should_quit: bool,
    selected_hash: Option<String>,
    delete_pending: bool,
    pending_delete_index: Option<usize>,
    metadata_path: PathBuf,
    download_root: PathBuf,
}

pub fn run_selection_menu(
    terminal: &mut DefaultTerminal,
    items: Vec<(String, String)>,
    metadata_path: PathBuf,
    download_root: PathBuf,
) -> EyreResult<Option<String>> {
    let mut state = SelectionState {
        items,
        selected_index: 0,
        should_quit: false,
        selected_hash: None,
        delete_pending: false,
        pending_delete_index: None,
        metadata_path,
        download_root,
    };

    while !state.should_quit {
        terminal.draw(|f| draw_selection(f, &state))?;
        handle_selection_events(&mut state)?;
    }

    Ok(state.selected_hash)
}

fn draw_selection(frame: &mut Frame, state: &SelectionState) {
    let area = frame.area();
    let [list_area, help_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(3)]).areas(area);

    let list_items: Vec<ListItem> = state
        .items
        .iter()
        .map(|(_, name)| ListItem::new(name.as_str()))
        .collect();

    let mut list_state = ListState::default();
    list_state.select(Some(state.selected_index));

    let list = List::new(list_items)
        .block(Block::bordered().title("Select a torrent"))
        .highlight_style(Style::new().bg(Color::Yellow).fg(Color::Black))
        .highlight_symbol(">> ");

    frame.render_stateful_widget(list, list_area, &mut list_state);

    let help_text = if state.delete_pending {
        format!(
            "Delete '{}'? (y/n)",
            state.items[state.pending_delete_index.unwrap_or(state.selected_index)].1
        )
    } else {
        "↑/↓: navigate, Enter: select, d: delete, q: quit".to_string()
    };

    let help = Paragraph::new(help_text)
        .block(Block::bordered())
        .alignment(Alignment::Center);
    frame.render_widget(help, help_area);
}

fn handle_selection_events(state: &mut SelectionState) -> EyreResult<()> {
    if event::poll(Duration::from_millis(16))?
        && let Event::Key(key) = event::read()?
        && key.kind == KeyEventKind::Press
    {
        if state.delete_pending {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    let idx = state.pending_delete_index.unwrap_or(state.selected_index);
                    let (hash, name) = &state.items[idx];

                    let base_name = if name.is_empty() {
                        hash.as_str()
                    } else {
                        name.as_str()
                    };
                    let download_root = &state.download_root;

                    let file_path = download_root.join(base_name);
                    if file_path.exists() && file_path.is_file() {
                        if let Err(e) = std::fs::remove_file(&file_path) {
                            log::error!("Failed to delete file {}: {}", file_path.display(), e);
                        } else {
                            log::info!("Deleted file: {}", file_path.display());
                        }
                    }

                    let dir_path = download_root.join(base_name);
                    if dir_path.exists() && dir_path.is_dir() {
                        if let Err(e) = std::fs::remove_dir_all(&dir_path) {
                            log::error!("Failed to delete directory {}: {}", dir_path.display(), e);
                        } else {
                            log::info!("Deleted directory: {}", dir_path.display());
                        }
                    }

                    let metadata_file = state.metadata_path.join(hash);
                    if let Err(e) = std::fs::remove_file(&metadata_file) {
                        log::error!(
                            "Failed to delete metadata file {}: {}",
                            metadata_file.display(),
                            e
                        );
                    } else {
                        log::info!("Deleted metadata file: {}", metadata_file.display());
                    }

                    state.items.remove(idx);

                    if state.selected_index >= state.items.len() {
                        state.selected_index = state.items.len().saturating_sub(1);
                    }

                    state.delete_pending = false;
                    state.pending_delete_index = None;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    state.delete_pending = false;
                    state.pending_delete_index = None;
                }
                _ => {}
            }
        } else {
            match key.code {
                KeyCode::Up => {
                    if state.selected_index > 0 {
                        state.selected_index -= 1;
                    }
                }
                KeyCode::Down => {
                    if state.selected_index + 1 < state.items.len() {
                        state.selected_index += 1;
                    }
                }
                KeyCode::Enter => {
                    let (hash, _) = state.items[state.selected_index].clone();
                    state.selected_hash = Some(hash);
                    state.should_quit = true;
                }
                KeyCode::Char('d') | KeyCode::Char('D') => {
                    state.delete_pending = true;
                    state.pending_delete_index = Some(state.selected_index);
                }
                KeyCode::Char('q') => {
                    state.should_quit = true;
                }
                _ => {}
            }
        }
    }
    Ok(())
}
