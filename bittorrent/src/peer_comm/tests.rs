use std::{collections::HashMap, path::PathBuf};

use lava_torrent::torrent::v1::{Torrent, TorrentBuilder};
use socket2::{Domain, Protocol, Socket, Type};

use crate::{TorrentState, file_store::FileStore, generate_peer_id, piece_selector::SUBPIECE_SIZE};

use super::{peer_connection::PeerConnection, peer_protocol::PeerMessage};

fn generate_peer<'a>(fast_ext: bool, conn_id: usize) -> PeerConnection<'a> {
    let socket_a = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
    PeerConnection::new(socket_a, generate_peer_id(), conn_id, fast_ext)
}

struct TempDir {
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
    piece_len: usize,
    file_data: HashMap<String, Vec<u8>>,
) -> (FileStore, Torrent) {
    let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
    let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
    file_data.iter().for_each(|(path, data)| {
        torrent_tmp_dir.add_file(path, data);
    });

    let torrent_info = TorrentBuilder::new(&torrent_tmp_dir.path, piece_len as i64)
        .set_name(torrent_name.to_string())
        .build()
        .unwrap();

    let download_tmp_dir_path = download_tmp_dir.path.clone();
    let file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();
    (file_store, torrent_info)
}

fn setup_test() -> (FileStore, Torrent, TorrentState) {
    let files: HashMap<String, Vec<u8>> = [
        ("f1.txt".to_owned(), vec![1_u8; 64]),
        ("f2.txt".to_owned(), vec![2_u8; 100]),
        ("f3.txt".to_owned(), vec![3_u8; SUBPIECE_SIZE as usize * 16]),
    ]
    .into_iter()
    .collect();
    let (file_store, torrent_info) = setup_torrent(
        &format!("{}", rand::random::<u16>()),
        SUBPIECE_SIZE as usize,
        files,
    );

    let torrent_state = TorrentState::new(&torrent_info);
    (file_store, torrent_info, torrent_state)
}

#[test]
fn fast_ext_have_all() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.is_interested);
        assert!(a.pending_disconnect.is_none());
        for piece_id in 0..torrent_info.pieces.len() {
            assert!(
                torrent_state
                    .piece_selector
                    .do_peer_have_piece(a.conn_id, piece_id)
            );
        }

        // Peers that do not state they support fast_ext are disconnected
        let mut b = generate_peer(false, 1);
        b.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(b.pending_disconnect.is_some());
    });
}

#[test]
fn fast_ext_have_none() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveNone,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        for piece_id in 0..torrent_info.pieces.len() {
            assert!(
                !torrent_state
                    .piece_selector
                    .do_peer_have_piece(a.conn_id, piece_id)
            );
        }

        // Peers that do not state they support fast_ext are disconnected
        let mut b = generate_peer(false, 1);
        b.handle_message(
            PeerMessage::HaveNone,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(b.pending_disconnect.is_some());
    });
}

#[test]
fn have() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::Have { index: 12 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        // We should be interestead now since we do not have the piece
        assert!(a.is_interested);
        for piece_id in 0..torrent_info.pieces.len() {
            if piece_id == 12 {
                assert!(
                    torrent_state
                        .piece_selector
                        .do_peer_have_piece(a.conn_id, piece_id)
                );
            } else {
                assert!(
                    !torrent_state
                        .piece_selector
                        .do_peer_have_piece(a.conn_id, piece_id)
                );
            }
        }
    });
}

#[test]
fn have_without_intrest() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
    // Needed to avoid hitting asserts
    torrent_state.piece_selector.mark_inflight(12);
    torrent_state.piece_selector.mark_complete(12);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::Have { index: 12 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        // We should not be interestead now since we do already have the piece
        assert!(!a.is_interested);
        for piece_id in 0..torrent_info.pieces.len() {
            if piece_id == 12 {
                assert!(
                    torrent_state
                        .piece_selector
                        .do_peer_have_piece(a.conn_id, piece_id)
                );
            } else {
                assert!(
                    !torrent_state
                        .piece_selector
                        .do_peer_have_piece(a.conn_id, piece_id)
                );
            }
        }
    });
}
