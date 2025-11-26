use std::{collections::HashMap, path::PathBuf};

use lava_torrent::torrent::v1::TorrentBuilder;
use metrics_exporter_prometheus::PrometheusBuilder;
use sha1::{Digest, Sha1};

/// Temporary directory that auto-cleans on drop
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(prefix: &str) -> Self {
        let path = format!("/tmp/vortex_test_{}_{}", prefix, rand::random::<u32>());
        std::fs::create_dir_all(&path).unwrap();
        let path = std::fs::canonicalize(path).unwrap();
        Self { path }
    }

    pub fn add_file(&self, file_path: &str, data: &[u8]) {
        let file_path = self.path.join(file_path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(file_path, data).unwrap();
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Calculate SHA1 hashes for a set of test files
pub fn calculate_file_hashes(files: &HashMap<String, Vec<u8>>) -> HashMap<String, Vec<u8>> {
    files
        .iter()
        .map(|(name, data)| {
            let mut hasher = Sha1::new();
            hasher.update(data);
            (name.clone(), hasher.finalize().to_vec())
        })
        .collect()
}

/// Verify downloaded files match expected hashes
pub fn verify_downloaded_files(
    download_dir: &TempDir,
    torrent_name: &str,
    expected_hashes: &HashMap<String, Vec<u8>>,
    peer_name: &str,
) {
    log::info!("Verifying {}'s file hashes...", peer_name);
    for (file_name, expected_hash) in expected_hashes {
        let downloaded_path = download_dir.path().join(torrent_name).join(file_name);
        let downloaded_data = std::fs::read(&downloaded_path)
            .unwrap_or_else(|_| panic!("Failed to read {} file: {}", peer_name, file_name));

        let mut hasher = Sha1::new();
        hasher.update(&downloaded_data);
        let actual_hash = hasher.finalize().to_vec();

        assert_eq!(
            actual_hash, *expected_hash,
            "Hash mismatch for {} file: {}",
            peer_name, file_name
        );
        log::info!("{} file {} hash verified", peer_name, file_name);
    }
}

/// Create a torrent from test files
///
/// Note: FileStore expects files to be in root/torrent_name/, so this function
/// creates files in that structure within the returned TempDir
pub fn create_test_torrent(
    test_files: &HashMap<String, Vec<u8>>,
    torrent_name: &str,
    piece_length: i64,
) -> (TempDir, lava_torrent::torrent::v1::Torrent) {
    let seeder_dir = TempDir::new(&format!("{}_seeder", torrent_name));

    // Add files to seeder directory in the structure FileStore expects
    for (file_path, data) in test_files {
        seeder_dir.add_file(&format!("{}/{}", torrent_name, file_path), data);
    }

    // Build torrent metadata from the torrent subdirectory
    let metadata = TorrentBuilder::new(seeder_dir.path().join(torrent_name), piece_length)
        .set_name(torrent_name.to_string())
        .build()
        .unwrap();

    (seeder_dir, metadata)
}

/// Initialize logging and metrics for tests
pub fn init_test_environment() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    let builder = PrometheusBuilder::new();
    if let Err(err) = builder.install() {
        log::error!("failed installing PrometheusBuilder: {err}");
    }
}
