//! UI components for the TUI application.

use std::time::{Duration, SystemTime};

use heapless::HistoryBuf;
use human_bytes::human_bytes;
use ratatui::{
    layout::Alignment,
    prelude::{Buffer, Rect},
    style::{Color, Modifier, Style, Stylize, palette::tailwind},
    text::Span,
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, Row, Table, Widget},
};
use vortex_bittorrent::MetadataProgress;

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
            .block(Block::default().borders(Borders::ALL))
            .style(default_style)
            .render(area, buf);
    }
}

pub fn extract_throughput_data(buf: &HistoryBuf<(f64, f64), 256>) -> Vec<(f64, f64)> {
    buf.oldest_ordered().copied().collect()
}
