use std::{
    cell::OnceCell,
    collections::VecDeque,
    io::{self},
    net::{SocketAddrV4, TcpListener},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, Sender},
};

use event_loop::{EventLoop, EventType};
use file_store::FileStore;
use heapless::spsc::{Consumer, Producer};
use io_uring::{
    IoUring, opcode,
    types::{self},
};
use io_utils::UserData;
use piece_selector::{CompletedPiece, Piece, PieceSelector, Subpiece};
use slab::Slab;
use thiserror::Error;

mod buf_pool;
mod buf_ring;
mod event_loop;
mod file_store;
mod io_utils;
mod peer_comm;
mod piece_selector;

use peer_comm::{peer_connection::PeerConnection, *};

#[cfg(feature = "fuzzing")]
pub use peer_protocol::*;

pub use peer_protocol::PeerId;
pub use peer_protocol::generate_peer_id;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Encountered IO issue: {0}")]
    Io(#[from] io::Error),
    #[error("Peer provider disconnected")]
    PeerProviderDisconnect,
}

pub struct Torrent {
    our_id: PeerId,
    state: State,
}

impl Torrent {
    pub fn new(our_id: PeerId, state: State) -> Self {
        Self { our_id, state }
    }

    pub fn start(
        &mut self,
        event_tx: Producer<TorrentEvent, 512>,
        command_rc: Consumer<Command, 64>,
    ) -> Result<(), Error> {
        // check ulimit
        let mut ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .setup_clamp()
            .setup_cqsize(4096)
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(4096)
            .unwrap();

        let mut events = Slab::with_capacity(4096);
        let event_idx = events.insert(EventType::Accept);
        let user_data = UserData::new(event_idx, None);

        let listener = TcpListener::bind(("0.0.0.0", 6881)).unwrap();
        let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
            .build()
            .user_data(user_data.as_u64());

        unsafe {
            ring.submission().push(&accept_op).unwrap();
        }
        ring.submission().sync();
        let mut event_loop = EventLoop::new(self.our_id, events);
        event_loop.run(ring, &mut self.state, event_tx, command_rc)
    }
}

/// Commands that can be sent to the event loop through the command channel
#[derive(Debug)]
pub enum Command {
    /// Connect to peers at the given address
    /// already connected peers will be filtered out
    ConnectToPeers(Vec<SocketAddrV4>),
    /// Stop the event loop gracefully
    Stop,
}

/// Events from the inprogress torrent
#[derive(Debug)]
pub enum TorrentEvent {
    TorrentComplete,
    MetadataComplete(Box<lava_torrent::torrent::v1::Torrent>),
    PeerMetrics {
        /// Note that these are not stable and might
        /// be reused
        conn_id: usize,
        throuhgput: u64,
        endgame: bool,
        snubbed: bool,
    },
    TorrentMetrics {
        pieces_completed: usize,
        pieces_allocated: usize,
        num_connections: usize,
    },
}

pub struct InitializedState {
    piece_selector: PieceSelector,
    num_unchoked: u32,
    max_unchoked: u32,
    completed_piece_rc: Receiver<CompletedPiece>,
    completed_piece_tx: Sender<CompletedPiece>,
    pieces: Vec<Option<Piece>>,
    is_complete: bool,
}

impl InitializedState {
    pub fn new(torrent: &lava_torrent::torrent::v1::Torrent) -> Self {
        let mut pieces = Vec::with_capacity(torrent.pieces.len());
        for _ in 0..torrent.pieces.len() {
            pieces.push(None);
        }
        let (tx, rc) = std::sync::mpsc::channel();
        Self {
            piece_selector: PieceSelector::new(torrent),
            num_unchoked: 0,
            max_unchoked: 8,
            completed_piece_rc: rc,
            completed_piece_tx: tx,
            pieces,
            is_complete: false,
        }
    }

    #[allow(dead_code)]
    pub fn num_allocated(&self) -> usize {
        self.pieces
            .iter()
            .filter(|piece| piece.as_ref().is_some_and(|piece| piece.ref_count > 0))
            .count()
    }

    #[inline]
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    // TODO: Put this in the event loop directly instead when that is easier to test
    pub(crate) fn update_torrent_status(&mut self, connections: &mut Slab<PeerConnection>) {
        while let Ok(completed_piece) = self.completed_piece_rc.try_recv() {
            match completed_piece.hash_matched {
                Ok(hash_matched) => {
                    if hash_matched {
                        self.piece_selector.mark_complete(completed_piece.index);
                        for (conn_id, peer) in connections.iter_mut() {
                            if let Some(bitfield) =
                                self.piece_selector.interesting_peer_pieces(conn_id)
                            {
                                if !bitfield.any()
                                    && peer.is_interesting
                                    && peer.queued.is_empty()
                                    && peer.inflight.is_empty()
                                {
                                    // We are no longer interestead in this peer
                                    peer.not_interested(false);
                                }
                            }
                            peer.have(completed_piece.index as i32, false);
                        }
                        log::debug!("Piece {} completed!", completed_piece.index);

                        if self.piece_selector.completed_all() {
                            self.is_complete = true;
                            // We are no longer interestead in any of the
                            // peers
                            for (_, peer) in connections.iter_mut() {
                                peer.not_interested(false);
                            }
                        }
                    } else {
                        // Only need to mark this as not hashing when it fails
                        // since otherwise it will be marked as completed and this is moot
                        self.piece_selector.mark_not_hashing(completed_piece.index);
                        // TODO: disconnect, there also might be a minimal chance of a race
                        // condition here where the connection id is replaced (by disconnect +
                        // new connection so that the wrong peer is marked) but this should be
                        // EXTREMELY rare
                        log::error!("Piece hash didn't match expected hash!");
                        self.piece_selector.mark_not_allocated(
                            completed_piece.index as i32,
                            completed_piece.conn_id,
                        );
                    }
                }
                Err(err) => {
                    log::error!(
                        "Failed to sync and hash piece: {} Error: {err}",
                        completed_piece.index
                    );
                }
            }
        }
    }

    // Allocates a piece and increments the piece ref count
    fn allocate_piece(
        &mut self,
        index: i32,
        conn_id: usize,
        file_store: &FileStore,
    ) -> VecDeque<Subpiece> {
        log::debug!("Allocating piece: conn_id: {conn_id}, index: {index}");
        self.piece_selector.mark_allocated(index, conn_id);
        match &mut self.pieces[index as usize] {
            Some(allocated_piece) => allocated_piece.allocate_remaining_subpieces(),
            None => {
                let length = self.piece_selector.piece_len(index);
                // SAFETY: There only exist a single piece per index in the torrent_state
                // piece vector which guarantees that there can never be two concurrent writable
                // piece views for the same index
                let piece_view = unsafe { file_store.writable_piece_view(index).unwrap() };
                let mut piece = Piece::new(index, length, piece_view);
                let subpieces = piece.allocate_remaining_subpieces();
                self.pieces[index as usize] = Some(piece);
                subpieces
            }
        }
    }

    // Deallocate a piece
    fn deallocate_piece(&mut self, index: i32, conn_id: usize) {
        log::debug!("Deallocating piece: conn_id: {conn_id}, index: {index}");
        // Mark the piece as interesting again so it can be picked again
        // if necessary
        self.piece_selector
            .update_peer_piece_intrest(conn_id, index as usize);
        // The piece might have been mid hashing when a timeout is received
        // (two separate peer) which causes to be completed whilst another peer
        // tried to download it. It's fine (TODO: confirm)
        if let Some(piece) = self.pieces[index as usize].as_mut() {
            // Will we reach 0 in the ref count?
            if piece.ref_count == 1 {
                log::debug!("marked as not allocated: conn_id: {conn_id}, index: {index}");
                self.piece_selector.mark_not_allocated(index, conn_id);
            }
            piece.ref_count = piece.ref_count.saturating_sub(1)
        }
    }

    pub fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }
}

pub struct FileAndMetadata {
    pub file_store: FileStore,
    pub metadata: Box<lava_torrent::torrent::v1::Torrent>,
}

pub struct State {
    info_hash: [u8; 20],
    // TODO: Consider checking this is accessible at construction
    root: PathBuf,
    torrent_state: Option<InitializedState>,
    file: OnceCell<FileAndMetadata>,
}

impl State {
    pub fn info_hash(&self) -> [u8; 20] {
        self.info_hash
    }

    pub fn unstarted(info_hash: [u8; 20], root: PathBuf) -> Self {
        Self {
            info_hash,
            root,
            torrent_state: None,
            file: OnceCell::new(),
        }
    }

    pub fn unstarted_from_metadata(
        metadata: lava_torrent::torrent::v1::Torrent,
        root: PathBuf,
    ) -> io::Result<Self> {
        let file_store = FileStore::new(&root, &metadata)?;
        Ok(Self {
            info_hash: metadata
                .info_hash_bytes()
                .try_into()
                .expect("Invalid info hash"),
            root,
            torrent_state: Some(InitializedState::new(&metadata)),
            file: OnceCell::from(FileAndMetadata {
                file_store,
                metadata: Box::new(metadata),
            }),
        })
    }

    pub fn inprogress(
        info_hash: [u8; 20],
        root: PathBuf,
        file_store: FileStore,
        metadata: lava_torrent::torrent::v1::Torrent,
        state: InitializedState,
    ) -> Self {
        Self {
            info_hash,
            root,
            torrent_state: Some(state),
            file: OnceCell::from(FileAndMetadata {
                file_store,
                metadata: Box::new(metadata),
            }),
        }
    }

    pub fn as_ref(&mut self) -> StateRef<'_> {
        StateRef {
            info_hash: self.info_hash,
            root: &self.root,
            torrent: &mut self.torrent_state,
            full: &self.file,
        }
    }
}

pub struct StateRef<'state> {
    info_hash: [u8; 20],
    root: &'state Path,
    torrent: &'state mut Option<InitializedState>,
    full: &'state OnceCell<FileAndMetadata>,
}

impl<'e_iter, 'state: 'e_iter> StateRef<'state> {
    pub fn info_hash(&self) -> &[u8; 20] {
        &self.info_hash
    }

    pub fn state(
        &'e_iter mut self,
    ) -> Option<(&'state FileAndMetadata, &'e_iter mut InitializedState)> {
        if let Some(f) = self.full.get() {
            // SAFETY: If full has been initialized the torrent must have been initialized
            // as well
            unsafe { Some((f, self.torrent.as_mut().unwrap_unchecked())) }
        } else {
            None
        }
    }

    #[inline]
    pub fn is_initialzied(&self) -> bool {
        self.full.get().is_some()
    }

    pub fn init(&'e_iter mut self, metadata: lava_torrent::torrent::v1::Torrent) -> io::Result<()> {
        if self.is_initialzied() {
            return Err(io::Error::other("State initialized twice"));
        }
        *self.torrent = Some(InitializedState::new(&metadata));
        self.full
            .set(FileAndMetadata {
                file_store: FileStore::new(self.root, &metadata)?,
                metadata: Box::new(metadata),
            })
            .map_err(|_e| io::Error::other("State initialized twice"))?;
        Ok(())
    }
}

/// Common test setup utils
#[cfg(test)]
mod test_utils {
    use std::{collections::HashMap, path::PathBuf};

    use crate::{
        InitializedState, State, file_store::FileStore, generate_peer_id,
        peer_comm::peer_protocol::ParsedHandshake, peer_connection::PeerConnection,
        piece_selector::SUBPIECE_SIZE,
    };
    use lava_torrent::torrent::v1::{Torrent, TorrentBuilder};
    use socket2::{Domain, Protocol, Socket, Type};

    pub fn generate_peer(fast_ext: bool, conn_id: usize) -> PeerConnection {
        let socket_a = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        PeerConnection::new(
            socket_a,
            conn_id,
            ParsedHandshake {
                peer_id: generate_peer_id(),
                fast_ext,
                extension_protocol: fast_ext, // Set extension protocol to same as fast_ext for testing
            },
        )
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
        torrent_tmp_dir: TempDir,
        download_tmp_dir: TempDir,
        piece_len: usize,
        file_data: HashMap<String, Vec<u8>>,
    ) -> (FileStore, Torrent) {
        setup_torrent_with_metadata_size(
            torrent_name,
            torrent_tmp_dir,
            download_tmp_dir,
            piece_len,
            file_data,
            false,
        )
    }

    fn setup_torrent_with_metadata_size(
        torrent_name: &str,
        torrent_tmp_dir: TempDir,
        download_tmp_dir: TempDir,
        piece_len: usize,
        file_data: HashMap<String, Vec<u8>>,
        large_metadata: bool,
    ) -> (FileStore, Torrent) {
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

        let torrent_info = builder.build().unwrap();
        let download_tmp_dir_path = download_tmp_dir.path.clone();
        let file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();
        (file_store, torrent_info)
    }

    pub fn setup_test() -> State {
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
        let (file_store, torrent_info) = setup_torrent(
            &torrent_name,
            torrent_tmp_dir,
            download_tmp_dir,
            (SUBPIECE_SIZE * 2) as usize,
            files,
        );
        let state = InitializedState::new(&torrent_info);
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
        let (_, torrent_info) = setup_torrent_with_metadata_size(
            &torrent_name,
            torrent_tmp_dir,
            download_tmp_dir,
            (SUBPIECE_SIZE * 2) as usize,
            files,
            large_metadata,
        );

        let info_hash = torrent_info.info_hash_bytes().try_into().unwrap();
        let uninitialized_state = State::unstarted(info_hash, root);

        (uninitialized_state, torrent_info)
    }
}
