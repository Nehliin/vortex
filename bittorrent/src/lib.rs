mod buf_pool;
mod buf_ring;
mod event_loop;
mod file_store;
mod io_utils;
mod peer_comm;
mod piece_selector;
mod torrent;

use peer_comm::*;

pub use peer_protocol::PeerId;
pub use torrent::{Command, PeerMetrics, State, Torrent, TorrentEvent};

#[cfg(feature = "fuzzing")]
pub use peer_protocol::*;

/// Common test setup utils
#[cfg(test)]
mod test_utils {
    use std::{collections::HashMap, path::PathBuf};

    use crate::{
        file_store::FileStore,
        peer_connection::PeerConnection,
        peer_protocol::{ParsedHandshake, PeerId},
        piece_selector::SUBPIECE_SIZE,
        torrent::{Config, InitializedState, State},
    };
    use lava_torrent::torrent::v1::{Torrent, TorrentBuilder};
    use socket2::{Domain, Protocol, Socket, Type};

    pub fn generate_peer(
        fast_ext: bool,
        conn_id: crate::event_loop::ConnectionId,
    ) -> PeerConnection {
        let socket_a = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        let peer_addr = "127.0.0.1:0".parse().unwrap();
        PeerConnection::new(
            socket_a,
            peer_addr,
            conn_id,
            ParsedHandshake {
                peer_id: PeerId::generate(),
                fast_ext,
                extension_protocol: fast_ext, // Set extension protocol to same as fast_ext for testing
            },
        )
    }

    #[derive(Debug)]
    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(path: &str) -> Self {
            let path = format!("/tmp/{path}");
            std::fs::create_dir_all(&path).unwrap();
            let path: PathBuf = path.into();
            Self {
                path: std::fs::canonicalize(path).unwrap(),
            }
        }

        fn add_file(&self, file_path: &str, data: &[u8]) {
            let file_path = self.path.as_path().join(file_path);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(file_path, data).unwrap();
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    fn setup_torrent(
        torrent_name: &str,
        torrent_tmp_dir: TempDir,
        piece_len: usize,
        file_data: HashMap<String, Vec<u8>>,
    ) -> Torrent {
        setup_torrent_with_metadata_size(torrent_name, torrent_tmp_dir, piece_len, file_data, false)
    }

    fn setup_torrent_with_metadata_size(
        torrent_name: &str,
        torrent_tmp_dir: TempDir,
        piece_len: usize,
        file_data: HashMap<String, Vec<u8>>,
        large_metadata: bool,
    ) -> Torrent {
        use lava_torrent::bencode::BencodeElem;

        file_data.iter().for_each(|(path, data)| {
            torrent_tmp_dir.add_file(path, data);
        });

        let mut builder = TorrentBuilder::new(&torrent_tmp_dir.path, piece_len as i64)
            .set_name(torrent_name.to_string());

        if large_metadata {
            // Add extra info fields to make the metadata larger
            // This will make the torrent's info dictionary larger, requiring multiple pieces for metadata download

            builder = builder.add_extra_info_field(
                "comment".to_string(),
                BencodeElem::String("This is a test torrent created for testing BEP 9 metadata extension with large metadata that requires multiple pieces to download. ".repeat(100))
            );

            builder = builder.add_extra_info_field(
                "created by".to_string(),
                BencodeElem::String("Vortex BitTorrent Client - Test Suite with Large Metadata Generation Capabilities".repeat(10))
            );

            builder = builder.add_extra_info_field(
                "creation date".to_string(),
                BencodeElem::Integer(1640995200), // Jan 1, 2022
            );

            builder = builder.add_extra_info_field(
                "encoding".to_string(),
                BencodeElem::String("UTF-8".to_string()),
            );

            let mut announce_list = Vec::new();
            for i in 0..50 {
                announce_list.push(format!("http://tracker{i}.example.com:8080/announce"));
                announce_list.push(format!("udp://tracker{i}.example.com:8080/announce"));
            }
            builder = builder.add_extra_info_field(
                "announce-list".to_string(),
                BencodeElem::List(announce_list.into_iter().map(BencodeElem::String).collect()),
            );

            for i in 0..50 {
                builder = builder.add_extra_info_field(
                    format!("test_field_{i}"),
                    BencodeElem::String(format!("This is test field number {} with some additional padding data to make the metadata larger. {}", i, "x".repeat(200)))
                );
            }

            // Add multiple large binary fields
            for i in 0..5 {
                builder = builder.add_extra_info_field(
                    format!("test_binary_data_{i}"),
                    BencodeElem::Bytes(vec![i as u8; 2048]), // 2KB of binary data each
                );
            }

            // Add a very large description field
            builder = builder.add_extra_info_field(
                "description".to_string(),
                BencodeElem::String("A".repeat(3000)), // 3KB description
            );
        }
        builder.build().unwrap()
    }

    pub fn setup_test() -> State {
        let files: HashMap<String, Vec<u8>> =
            [("f3.txt".to_owned(), vec![3_u8; SUBPIECE_SIZE as usize * 16])]
                .into_iter()
                .collect();
        let torrent_name = format!("{}", rand::random::<u16>());
        let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
        let root = download_tmp_dir.path.clone();
        let torrent_info = setup_torrent(
            &torrent_name,
            torrent_tmp_dir,
            (SUBPIECE_SIZE * 2) as usize,
            files,
        );
        let download_tmp_dir_path = root.clone();
        let file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();
        let state = InitializedState::new(&torrent_info, Config::default());
        State::inprogress(
            torrent_info.info_hash_bytes().try_into().unwrap(),
            root,
            file_store,
            torrent_info,
            state,
        )
    }

    pub fn setup_uninitialized_test() -> (State, lava_torrent::torrent::v1::Torrent) {
        setup_uninitialized_test_with_metadata_size(false)
    }

    pub fn setup_uninitialized_test_with_metadata_size(
        large_metadata: bool,
    ) -> (State, lava_torrent::torrent::v1::Torrent) {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 100]),
            ("f3.txt".to_owned(), vec![3_u8; SUBPIECE_SIZE as usize * 16]),
        ]
        .into_iter()
        .collect();
        let torrent_name = format!("{}", rand::random::<u16>());
        let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
        let root = download_tmp_dir.path.clone();

        // Create the torrent to get the metadata, but don't initialize the state with it
        let torrent_info = setup_torrent_with_metadata_size(
            &torrent_name,
            torrent_tmp_dir,
            (SUBPIECE_SIZE * 2) as usize,
            files,
            large_metadata,
        );
        let info_hash = torrent_info.info_hash_bytes().try_into().unwrap();
        let uninitialized_state = State::unstarted(info_hash, root);

        (uninitialized_state, torrent_info)
    }

    pub fn setup_seeding_test() -> State {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 100]),
            ("f3.txt".to_owned(), vec![3_u8; SUBPIECE_SIZE as usize * 16]),
        ]
        .into_iter()
        .collect();
        let torrent_name = format!("{}", rand::random::<u16>());
        let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
        let root = download_tmp_dir.path.clone();

        let torrent_info = setup_torrent(
            &torrent_name,
            torrent_tmp_dir,
            (SUBPIECE_SIZE * 2) as usize,
            files.clone(),
        );

        // Add files to the download directory so they're available for seeding
        // Files need to be in the subdirectory matching the torrent name
        files.iter().for_each(|(path, data)| {
            download_tmp_dir.add_file(&format!("{}/{}", torrent_name, path), data);
        });

        // Use from_metadata_and_root to create a state with already completed pieces
        State::from_metadata_and_root(torrent_info, root).unwrap()
    }
}
