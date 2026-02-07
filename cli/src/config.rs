//! Configuration management for Vortex CLI with XDG directory support.

use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct VortexConfig {
    #[serde(default)]
    pub paths: PathsConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    #[serde(default)]
    pub bittorrent: vortex_bittorrent::Config,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PathsConfig {
    /// Custom download folder (overrides XDG default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_folder: Option<PathBuf>,

    /// Custom log file path (overrides XDG default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_file: Option<PathBuf>,

    /// Custom DHT cache path (overrides XDG default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dht_cache: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ResolvedPaths {
    pub download_folder: PathBuf,
    pub log_file: PathBuf,
    pub dht_cache: PathBuf,
}

// Default: ~/.config/vortex/config.toml
pub fn get_config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("vortex")
        .join("config.toml")
}

// Default: ~/.local/share/vortex/downloads
pub fn get_default_download_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("vortex")
        .join("downloads")
}

// Default: ~/.local/state/vortex/vortex.log
pub fn get_default_log_path() -> PathBuf {
    dirs::state_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("vortex")
        .join("vortex.log")
}

// Default: ~/.cache/vortex/dht_bootstrap_nodes
pub fn get_default_dht_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("vortex")
        .join("dht_bootstrap_nodes")
}

pub fn load_or_create_config(
    config_path: Option<PathBuf>,
) -> color_eyre::eyre::Result<VortexConfig> {
    let config_path = config_path.unwrap_or_else(get_config_path);

    if config_path.exists() {
        let contents = fs::read_to_string(&config_path)?;
        let config: VortexConfig = toml::from_str(&contents)?;
        Ok(config)
    } else {
        let default_config = VortexConfig::default();

        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let config_content = create_default_config_content();
        fs::write(&config_path, config_content)?;

        log::info!("Created default config file at: {}", config_path.display());

        Ok(default_config)
    }
}

fn create_default_config_content() -> String {
    include_str!("../vortex.config.toml.example").to_string()
}

pub fn merge_with_cli_args(
    mut config: VortexConfig,
    download_folder: Option<PathBuf>,
    log_file: Option<PathBuf>,
    dht_cache: Option<PathBuf>,
    port: Option<u16>,
) -> VortexConfig {
    if let Some(path) = download_folder {
        config.paths.download_folder = Some(path);
    }

    if let Some(path) = log_file {
        config.paths.log_file = Some(path);
    }

    if let Some(path) = dht_cache {
        config.paths.dht_cache = Some(path);
    }

    if let Some(port_val) = port {
        config.port = Some(port_val);
    }

    config
}

pub fn resolve_paths(config: &VortexConfig) -> ResolvedPaths {
    ResolvedPaths {
        download_folder: config
            .paths
            .download_folder
            .clone()
            .unwrap_or_else(get_default_download_path),
        log_file: config
            .paths
            .log_file
            .clone()
            .unwrap_or_else(get_default_log_path),
        dht_cache: config
            .paths
            .dht_cache
            .clone()
            .unwrap_or_else(get_default_dht_cache_path),
    }
}
