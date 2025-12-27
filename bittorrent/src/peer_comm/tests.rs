use std::time::{Duration, Instant};

use bt_bencode::Deserializer;
use bytes::Buf;
use heapless::spsc::Queue;
use serde::Deserialize;
use slotmap::SlotMap;

use crate::{
    event_loop::{ConnectionId, EventData, EventId, tick},
    io_utils::{BackloggedSubmissionQueue, SubmissionQueue},
    peer_comm::{extended_protocol::MetadataMessage, peer_connection::DisconnectReason},
    peer_connection::OutgoingMsg,
    piece_selector::{SUBPIECE_SIZE, Subpiece},
    test_utils::{
        generate_peer, setup_seeding_test, setup_test, setup_uninitialized_test,
        setup_uninitialized_test_with_metadata_size,
    },
    torrent::{self, TorrentEvent},
};

use super::{peer_connection::PeerConnection, peer_protocol::PeerMessage};

// Mock SubmissionQueue for tests that need to call disconnect
struct MockSubmissionQueue;

impl SubmissionQueue for MockSubmissionQueue {
    fn sync(&mut self) {}

    fn capacity(&self) -> usize {
        128
    }

    fn len(&self) -> usize {
        0
    }

    fn is_full(&self) -> bool {
        false
    }

    unsafe fn push(
        &mut self,
        _entry: &io_uring::squeue::Entry,
    ) -> Result<(), io_uring::squeue::PushError> {
        Ok(())
    }
}

#[track_caller]
fn sent_and_marked_interested(peer: &PeerConnection) {
    assert!(peer.is_interesting);
    assert!(
        peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::Interested,
            ordered: false,
        }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::Interested,
            ordered: true,
        })
    );
}

#[track_caller]
fn sent_and_marked_not_interested(peer: &PeerConnection) {
    assert!(!peer.is_interesting);
    assert!(
        peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::NotInterested,
            ordered: false,
        }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::NotInterested,
            ordered: true,
        })
    );
}

#[test]
fn fast_ext_have_all() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        assert!(!connections[key_a].is_interesting);
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_a].conn_id)
        );
        sent_and_marked_interested(&connections[key_a]);
        assert!(connections[key_a].pending_disconnect.is_none());
        for piece_id in 0..file_and_info.metadata.pieces.len() {
            assert!(
                torrent_state
                    .piece_selector
                    .interesting_peer_pieces(connections[key_a].conn_id)
                    .unwrap()[piece_id]
            );
        }

        // Peers that do not state they support fast_ext are disconnected
        let key_b = connections.insert_with_key(|k| generate_peer(false, k));
        connections[key_b].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        assert!(connections[key_b].pending_disconnect.is_some());

        // Do not mark as interestead if we've already completed the torrent
        let key_c = connections.insert_with_key(|k| generate_peer(true, k));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.is_complete = true;
        assert!(!connections[key_c].is_interesting);
        connections[key_c].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_c].conn_id)
        );
        assert!(!connections[key_c].is_interesting);
    });
}

#[test]
fn fast_ext_have_none() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].is_interesting = true;
        connections[key_a].handle_message(PeerMessage::HaveNone, &mut state_ref, scope);
        // we are not interestead in peers that have nothing
        sent_and_marked_not_interested(&connections[key_a]);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_a].conn_id)
        );
        assert!(connections[key_a].pending_disconnect.is_none());
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(connections[key_a].conn_id)
                .unwrap()
                .any()
        );
        // Peers that do not state they support fast_ext are disconnected
        let key_b = connections.insert_with_key(|k| generate_peer(false, k));
        connections[key_b].handle_message(PeerMessage::HaveNone, &mut state_ref, scope);
        assert!(connections[key_b].pending_disconnect.is_some());
    });
}

#[test]
fn have() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::Have { index: 7 }, &mut state_ref, scope);
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        assert!(torrent_state.piece_selector.bitfield_received(key_a));
        assert!(connections[key_a].pending_disconnect.is_none());
        // We should be interestead now since we do not have the piece
        sent_and_marked_interested(&connections[key_a]);
        connections[key_a].outgoing_msgs_buffer.clear();
        for piece_id in 0..file_and_info.metadata.pieces.len() {
            if piece_id == 7 {
                assert!(
                    torrent_state
                        .piece_selector
                        .interesting_peer_pieces(key_a)
                        .unwrap()[piece_id]
                );
            } else {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(key_a)
                        .unwrap()[piece_id]
                );
            }
        }
        // Does not send interested again
        connections[key_a].handle_message(PeerMessage::Have { index: 7 }, &mut state_ref, scope);
        assert!(connections[key_a].outgoing_msgs_buffer.is_empty());
        let (_, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key_a, &mut connections[key_a].endgame)
            .unwrap();
        assert_eq!(index, 7);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key_a, &file_and_info.file_store);
        connections[key_a].append_and_fill(&mut subpieces);

        let key_b = connections.insert_with_key(|k| generate_peer(true, k));
        assert!(!connections[key_b].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(torrent_state.piece_selector.is_allocated(index as usize));
        connections[key_b].handle_message(PeerMessage::Have { index }, &mut state_ref, scope);
        // Piece is still interesting since it's not completed
        sent_and_marked_interested(&connections[key_b]);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .interesting_peer_pieces(key_b)
                .unwrap()[index as usize]
        );

        let key_c = connections.insert_with_key(|k| generate_peer(true, k));

        // Complete the piece
        connections[key_a].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key_a].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a].inflight.is_empty());
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 0);
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);

        // C is not interesting
        assert!(!connections[key_c].is_interesting);
        connections[key_c].handle_message(PeerMessage::Have { index }, &mut state_ref, scope);
        // Piece is NOT interesting since it's completed
        assert!(!connections[key_c].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(key_c)
                .unwrap()[index as usize]
        );
    });
}

#[test]
fn have_invalid_indicies() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::Have { index: -1 }, &mut state_ref, scope);
        assert!(connections[key_a].pending_disconnect.is_some());
        let key_b = connections.insert_with_key(|k| generate_peer(false, k));
        let (_, torrent_state) = state_ref.state().unwrap();
        connections[key_b].handle_message(
            PeerMessage::Have {
                index: torrent_state.num_pieces() as i32,
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_b].pending_disconnect.is_some());
    });
}

#[test]
fn have_without_interest() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let (_, torrent_state) = state_ref.state().unwrap();
        // Needed to avoid hitting asserts
        torrent_state.piece_selector.mark_complete(7);
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::Have { index: 7 }, &mut state_ref, scope);
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_a].conn_id)
        );
        assert!(connections[key_a].pending_disconnect.is_none());
        // We should not be interestead now since we do already have the piece
        assert!(!connections[key_a].is_interesting);
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(connections[key_a].conn_id)
                .unwrap()
                .any()
        );
    });
}

#[test]
fn slow_start() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));
        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].network_stats.prev_download_throughput, 0);
        assert_eq!(connections[key].network_stats.download_throughput, 0);
        assert!(connections[key].target_inflight > 1);
        let old_desired_queue = connections[key].target_inflight;

        connections[key].peer_choking = false;

        // To control exactly how much is requested we set up
        // Have messages just before next_piece calls, otherwise
        // tick will allocate other pieces
        connections[key].handle_message(PeerMessage::Have { index: 1 }, &mut state_ref, scope);

        let (_, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        assert_eq!(index, 1);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert_eq!(
            connections[key].network_stats.download_throughput,
            (SUBPIECE_SIZE * 2) as u64
        );
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].network_stats.prev_download_throughput, 0);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 2);

        tick(
            &Duration::from_millis(1500),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        assert_eq!(
            connections[key].network_stats.prev_download_throughput,
            21845
        );
        assert_eq!(connections[key].network_stats.download_throughput, 0);
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 2);

        connections[key].handle_message(PeerMessage::Have { index: 2 }, &mut state_ref, scope);
        let (_, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        assert_eq!(index, 2);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        assert_eq!(
            connections[key].network_stats.prev_download_throughput,
            (SUBPIECE_SIZE * 2) as u64
        );
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 4);

        connections[key].handle_message(PeerMessage::Have { index: 3 }, &mut state_ref, scope);

        let (_, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        assert_eq!(index, 3);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 3,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 3,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        assert_eq!(
            connections[key].network_stats.prev_download_throughput,
            (SUBPIECE_SIZE * 2) as u64
        );
        // No longer slow start
        assert!(!connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 6);
    });
}

#[test]
fn desired_queue_size() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        connections[key].peer_choking = false;
        connections[key].slow_start = false;
        let (_, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        // 2 subpieces * 3
        assert_eq!(connections[key].target_inflight, 6);

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        // Never go below 1
        assert_eq!(connections[key].target_inflight, 1);
        // TODO: Test max
    });
}

// Test that we deal with peers that support the fast extension correctly
// when receving chokes
#[test]
fn peer_choke_recv_supports_fast() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        // First, the peer needs to have pieces to be interesting
        for index in 1..7 {
            connections[key].handle_message(PeerMessage::Have { index }, &mut state_ref, scope);
        }

        connections[key].slow_start = false;

        assert!(connections[key].peer_choking);
        connections[key].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        assert!(!connections[key].peer_choking);

        // Now allocate additional pieces manually
        let mut allocated_pieces = Vec::new();

        // Get the first piece that was allocated during unchoke
        let (_, torrent_state) = state_ref.state().unwrap();
        if let Some(piece) = torrent_state.pieces.iter().position(|p| p.is_some()) {
            allocated_pieces.push(piece as i32);
        }

        // Allocate 5 more pieces
        for _ in 0..5 {
            let (_, torrent_state) = state_ref.state().unwrap();
            if let Some(index) = torrent_state
                .piece_selector
                .next_piece(key, &mut connections[key].endgame)
            {
                allocated_pieces.push(index);
                let (file_and_info, torrent_state) = state_ref.state().unwrap();
                let mut subpieces =
                    torrent_state.allocate_piece(index, key, &file_and_info.file_store);
                connections[key].append_and_fill(&mut subpieces);
            }
        }

        assert_eq!(allocated_pieces.len(), 6);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 6);
        assert_eq!(connections[key].target_inflight, 4);
        assert_eq!(connections[key].queued.len(), 8);
        assert_eq!(connections[key].inflight.len(), 4);

        // Complete the first piece
        let first_piece = allocated_pieces[0];
        connections[key].handle_message(
            PeerMessage::Piece {
                index: first_piece,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: first_piece,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        tick(
            &Duration::from_millis(650),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        // Want an odd number here to test releasing in flight pieces
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert_eq!(connections[key].queued.len(), 1);

        for index in allocated_pieces.iter().skip(1) {
            // The last allocated piece should still be allocated
            let (_, torrent_state) = state_ref.state().unwrap();
            assert!(torrent_state.piece_selector.is_allocated(*index as usize));
        }
        connections[key].handle_message(PeerMessage::Choke, &mut state_ref, scope);
        assert!(connections[key].peer_choking);
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert!(connections[key].queued.is_empty());
        // 1 piece completed (pending hashing), one was released
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 4);
        assert!(
            !torrent_state
                .piece_selector
                .is_allocated(*allocated_pieces.last().unwrap() as usize)
        );
    });
}

#[test]
fn peer_choke_recv_does_not_support_fast() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(false, k));

        // First, the peer needs to have pieces to be interesting
        for index in 1..7 {
            connections[key].handle_message(PeerMessage::Have { index }, &mut state_ref, scope);
        }

        connections[key].slow_start = false;

        assert!(connections[key].peer_choking);
        connections[key].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        assert!(!connections[key].peer_choking);

        // Now allocate additional pieces manually
        let mut allocated_pieces = Vec::new();

        // Get the first piece that was allocated during unchoke
        let (_, torrent_state) = state_ref.state().unwrap();
        if let Some(piece) = torrent_state.pieces.iter().position(|p| p.is_some()) {
            allocated_pieces.push(piece as i32);
        }

        // Allocate 5 more pieces
        for _ in 0..5 {
            let (_, torrent_state) = state_ref.state().unwrap();
            if let Some(index) = torrent_state
                .piece_selector
                .next_piece(key, &mut connections[key].endgame)
            {
                allocated_pieces.push(index);
                let (file_and_info, torrent_state) = state_ref.state().unwrap();
                let mut subpieces =
                    torrent_state.allocate_piece(index, key, &file_and_info.file_store);
                connections[key].append_and_fill(&mut subpieces);
            }
        }

        assert_eq!(allocated_pieces.len(), 6);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 6);
        assert_eq!(connections[key].target_inflight, 4);
        assert_eq!(connections[key].queued.len(), 8);
        assert_eq!(connections[key].inflight.len(), 4);

        // Complete the first piece
        let first_piece = allocated_pieces[0];
        connections[key].handle_message(
            PeerMessage::Piece {
                index: first_piece,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: first_piece,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        tick(
            &Duration::from_millis(650),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        // Want an odd number here to test releasing in flight pieces
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert_eq!(connections[key].queued.len(), 1);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .is_allocated(*allocated_pieces.last().unwrap() as usize)
        );

        connections[key].handle_message(PeerMessage::Choke, &mut state_ref, scope);
        assert!(connections[key].peer_choking);
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 0);
        assert_eq!(connections[key].queued.len(), 0);
        // 1 piece completed (pending hashing), one was released
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 0);
        // index = first_piece is not inflight over the network but pending hashing and thus is
        // still marked inflight
        for i in allocated_pieces.iter().skip(1) {
            assert!(!torrent_state.piece_selector.is_allocated(*i as usize));
        }
    });
}

#[test]
fn unchoke_recv() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(false, k));

        assert!(connections[key].peer_choking);
        connections[key].is_interesting = false;
        connections[key].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        assert!(!connections[key].peer_choking);
        // No intrest so nothing is downloaded
        assert!(connections[key].queued.is_empty());
        assert!(connections[key].inflight.is_empty());
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 0);

        let key = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        assert!(connections[key].peer_choking);
        assert!(connections[key].is_interesting);
        connections[key].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        assert!(!connections[key].peer_choking);
        // Peer is interesting so we start downloading
        assert!(!(connections[key].queued.is_empty() && connections[key].inflight.is_empty()));
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(!torrent_state.pieces.is_empty());
    });
}

// TODO: num_unchoked after disconnecting (tick maybe should return num disconnected? for easy testing)

#[test]
fn bitfield_recv() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        {
            let key_b = connections.insert_with_key(|k| generate_peer(true, k));
            assert!(connections[key_b].pending_disconnect.is_none());
            let (_, torrent_state) = state_ref.state().unwrap();
            let invalid_field =
                bitvec::bitvec!(u8, bitvec::order::Msb0; 1; torrent_state.num_pieces() - 1);
            connections[key_b].handle_message(
                PeerMessage::Bitfield(invalid_field),
                &mut state_ref,
                scope,
            );
            assert!(connections[key_b].pending_disconnect.is_some());
        }

        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        assert!(!connections[key_a].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            !torrent_state
                .piece_selector
                .bitfield_received(connections[key_a].conn_id)
        );
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; torrent_state.num_pieces());
        field.set(2, false);
        field.set(4, false);
        connections[key_a].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        // We are interestead since we do not have the pieces
        sent_and_marked_interested(&connections[key_a]);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_a].conn_id)
        );

        for i in 0..torrent_state.num_pieces() {
            if i == 2 || i == 4 {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(connections[key_a].conn_id)
                        .unwrap()[i]
                );
            } else {
                assert!(
                    torrent_state
                        .piece_selector
                        .interesting_peer_pieces(connections[key_a].conn_id)
                        .unwrap()[i]
                );
                // MOCK that the pieces have been completed by a
                torrent_state.piece_selector.mark_complete(i);
            }
        }

        let key_b = connections.insert_with_key(|k| generate_peer(true, k));
        assert!(!connections[key_b].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            !torrent_state
                .piece_selector
                .bitfield_received(connections[key_b].conn_id)
        );
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
        field.set(2, false);
        field.set(4, false);
        connections[key_b].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        // Still not interestead since no new pieces can be downloaded received
        assert!(!connections[key_b].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_b].conn_id)
        );

        let key_c = connections.insert_with_key(|k| generate_peer(true, k));
        assert!(!connections[key_c].is_interesting);
        assert!(
            !torrent_state
                .piece_selector
                .bitfield_received(connections[key_c].conn_id)
        );
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
        field.set(2, false);
        connections[key_c].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        // New piece can be downloaded
        sent_and_marked_interested(&connections[key_c]);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key_c].conn_id)
        );
    });
}

// TODO test we do not resend the not_interested message
#[test]
fn interest_is_updated_when_recv_piece() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        assert!(!connections[key].is_interesting);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            !torrent_state
                .piece_selector
                .bitfield_received(connections[key].conn_id)
        );
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(2, true);
        field.set(4, true);
        connections[key].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        // We are interestead since we do not have the pieces
        sent_and_marked_interested(&connections[key]);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key].conn_id)
        );

        let index_a = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index_a, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        let index_b = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index_b, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 4);
        assert!(connections[key].queued.is_empty());

        connections[key].handle_message(
            PeerMessage::Piece {
                index: index_a,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: index_a,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert!(connections[key].is_interesting);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: index_b,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: index_b,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        sent_and_marked_not_interested(&connections[key]);
    });
}

#[test]
fn send_have_to_peers_when_piece_completes() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        let key_b = connections.insert_with_key(|k| generate_peer(true, k));
        let _key_c = connections.insert_with_key(|k| generate_peer(true, k));

        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(2, true);
        connections[key_a].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(4, true);
        connections[key_b].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index_a = torrent_state
            .piece_selector
            .next_piece(key_a, &mut connections[key_a].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index_a, key_a, &file_and_info.file_store);
        connections[key_a].append_and_fill(&mut subpieces);
        let index_b = torrent_state
            .piece_selector
            .next_piece(key_b, &mut connections[key_b].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index_b, key_b, &file_and_info.file_store);
        connections[key_b].append_and_fill(&mut subpieces);

        connections[key_a].handle_message(
            PeerMessage::Piece {
                index: index_a,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key_a].handle_message(
            PeerMessage::Piece {
                index: index_a,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(150));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        for (_, peer) in &mut connections {
            assert!(
                peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: index_a },
                    ordered: false,
                }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: index_a },
                    ordered: true,
                })
            );
            // Clear for next check
            peer.outgoing_msgs_buffer.clear();
        }
        connections[key_b].handle_message(
            PeerMessage::Piece {
                index: index_b,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key_b].handle_message(
            PeerMessage::Piece {
                index: index_b,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        for (_, peer) in &connections {
            assert!(
                peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: index_b },
                    ordered: false,
                }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: index_b },
                    ordered: true,
                })
            )
        }
    });
}

#[test]
fn assume_intrest_when_request_recv() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        assert!(!connections[key_a].peer_interested);
        connections[key_a].handle_message(
            PeerMessage::Request {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );
        // Assume the peer is interestead we recv a request from them
        assert!(connections[key_a].peer_interested);
    });
}

#[test]
fn piece_recv() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);

        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        let prev_target_infligt = connections[key].target_inflight;
        assert_eq!(torrent_state.num_allocated(), 1);
        assert!(torrent_state.pieces[index as usize].is_some());
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 2);
        assert!(connections[key].queued.is_empty());
        assert_eq!(torrent_state.piece_selector.total_allocated(), 1);
        assert!(torrent_state.piece_selector.is_allocated(index as usize));
        assert_eq!(torrent_state.piece_selector.total_completed(), 0);

        let now = Instant::now();
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(
            torrent_state.pieces[index as usize]
                .as_ref()
                .unwrap()
                .completed_subpieces[0]
        );
        assert_eq!(connections[key].inflight.len(), 1);
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 1);
        assert!(now - connections[key].last_seen < Duration::from_millis(1));
        assert!(now - connections[key].last_received_subpiece.unwrap() < Duration::from_millis(1));
        assert!(
            !torrent_state.pieces[index as usize]
                .as_ref()
                .unwrap()
                .completed_subpieces
                .all()
        );

        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key].inflight.is_empty());
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 2);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 0);
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert_eq!(torrent_state.piece_selector.total_allocated(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(index as usize));
        assert!(torrent_state.piece_selector.has_downloaded(index as usize));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
    });
}

#[test]
fn handles_duplicate_piece_recv() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let prev_target_infligt = connections[key].target_inflight;
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 1);
        assert_eq!(connections[key].inflight.len(), 1);
        std::thread::sleep(Duration::from_millis(100));
        // Same message again
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key].last_seen.elapsed() < Duration::from_millis(1));
        // Timestamps for last_received_subpiece are not updated
        assert!(
            connections[key].last_received_subpiece.unwrap().elapsed() > Duration::from_millis(100)
        );
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 1);
        assert_eq!(connections[key].inflight.len(), 1);
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key].inflight.is_empty());
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 0);
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert_eq!(torrent_state.piece_selector.total_allocated(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(index as usize));
        assert!(torrent_state.piece_selector.has_downloaded(index as usize));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.piece_selector.total_allocated(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(index as usize));
        assert!(torrent_state.piece_selector.has_downloaded(index as usize));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
    });
}

#[test]
fn invalid_piece() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces =
            torrent_state.allocate_piece(2, connections[key_a].conn_id, &file_and_info.file_store);
        connections[key_a].append_and_fill(&mut subpieces);
        assert!(connections[key_a].pending_disconnect.is_none());
        connections[key_a].handle_message(
            PeerMessage::Piece {
                index: -2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a].pending_disconnect.is_some());
        let key_a2 = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a2].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces =
            torrent_state.allocate_piece(2, connections[key_a2].conn_id, &file_and_info.file_store);
        connections[key_a2].append_and_fill(&mut subpieces);
        assert!(connections[key_a2].pending_disconnect.is_none());
        connections[key_a2].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE + 1,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a2].pending_disconnect.is_some());

        let key_a3 = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a3].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces =
            torrent_state.allocate_piece(2, connections[key_a3].conn_id, &file_and_info.file_store);
        connections[key_a3].append_and_fill(&mut subpieces);
        assert!(connections[key_a3].pending_disconnect.is_none());
        connections[key_a3].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; (SUBPIECE_SIZE + 1) as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a3].pending_disconnect.is_some());
    });
}

// TODO: ensure we request as many pieces as possible to actuall fill up all available queue spots
// when starting up connections
// #[test]
// fn request_piece() {}

// TODO timeout tests + Tests that ensure we handle redundant data
#[test]
fn snubbed_peer() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        // Hack to prevent this from requesting things
        connections[key].is_interesting = false;
        connections[key].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        connections[key].is_interesting = true;
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key, &mut connections[key].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 2);
        assert!(connections[key].queued.is_empty());
        assert_eq!(torrent_state.num_allocated(), 1);
        connections[key].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                // TODO: THIS MIGHT BE INCORRECT
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key].last_received_subpiece.is_some());
        assert!(connections[key].slow_start);
        assert!(!connections[key].snubbed);
        // Simulate time passing
        connections[key].last_received_subpiece = Some(Instant::now() - Duration::from_secs(3));
        assert_eq!(connections[key].inflight.len(), 1);
        assert!(!connections[key].inflight[0].timed_out);
        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );
        assert!(!connections[key].slow_start);
        assert!(connections[key].snubbed);
        assert!(connections[key].inflight[0].timed_out);
        let (_, torrent_state) = state_ref.state().unwrap();
        assert!(!torrent_state.piece_selector.is_allocated(index as usize));
        assert_eq!(torrent_state.num_allocated(), 1);
        assert_eq!(connections[key].target_inflight, 1);
        assert_eq!(connections[key].inflight.len(), 2);
        let inflight = connections[key].inflight[1];
        connections[key].handle_message(
            PeerMessage::Piece {
                index: inflight.index,
                begin: inflight.offset,
                data: vec![3; inflight.size as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        tick(
            &Duration::from_secs(1),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );
        assert!(!connections[key].snubbed);
        assert!(connections[key].target_inflight > 1);
    });
}

#[test]
fn reject_request_requests_new() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        // Hack to prevent this from requesting things
        connections[key_a].is_interesting = false;
        connections[key_a].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        connections[key_a].is_interesting = true;
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(connections[key_a].conn_id, &mut connections[key_a].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(
            index,
            connections[key_a].conn_id,
            &file_and_info.file_store,
        );
        connections[key_a].append_and_fill(&mut subpieces);
        assert_eq!(connections[key_a].inflight.len(), 2);
        assert!(connections[key_a].inflight.contains(&Subpiece {
            index,
            offset: 0,
            size: SUBPIECE_SIZE,
            timed_out: false,
        }));
        assert_eq!(torrent_state.num_allocated(), 1);
        connections[key_a].handle_message(
            PeerMessage::RejectRequest {
                index,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );
        assert!(!connections[key_a].inflight.contains(&Subpiece {
            index,
            offset: 0,
            size: SUBPIECE_SIZE,
            timed_out: false,
        }));
        // New piece started
        let (_, torrent_state) = state_ref.state().unwrap();
        assert_eq!(torrent_state.num_allocated(), 1);
        assert!(!torrent_state.piece_selector.is_allocated(index as usize));
        assert_eq!(connections[key_a].inflight.len(), 3);
    });
}

#[test]
fn invalid_reject_request() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_b = connections.insert_with_key(|k| generate_peer(false, k));
        connections[key_b].handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );
        assert!(matches!(
            connections[key_b].pending_disconnect,
            Some(DisconnectReason::ProtocolError(_))
        ));
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        // Hack to prevent this from requesting things
        connections[key_a].is_interesting = false;
        connections[key_a].handle_message(PeerMessage::Unchoke, &mut state_ref, scope);
        connections[key_a].is_interesting = true;
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let mut subpieces =
            torrent_state.allocate_piece(2, connections[key_a].conn_id, &file_and_info.file_store);
        connections[key_a].append_and_fill(&mut subpieces);
        assert!(connections[key_a].pending_disconnect.is_none());
        connections[key_a].handle_message(
            PeerMessage::RejectRequest {
                index: -2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a].pending_disconnect.is_some());
        connections[key_a].pending_disconnect = None;
        connections[key_a].handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: SUBPIECE_SIZE + 1,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );
        assert!(connections[key_a].pending_disconnect.is_some());
        connections[key_a].pending_disconnect = None;
        assert_eq!(connections[key_a].inflight.len(), 2);
        connections[key_a].handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE + 1,
            },
            &mut state_ref,
            scope,
        );
        assert_eq!(connections[key_a].inflight.len(), 2);
        assert!(connections[key_a].pending_disconnect.is_some());
    });
}

#[test]
fn endgame_mode() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);

        let key_b = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key_b].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);

        let (file_and_info, torrent_state) = state_ref.state().unwrap();

        // Set up so that half of the pieces have been requested and that a part of those have been
        // completed
        let num_pieces_half = torrent_state.num_pieces() / 2;
        for i in 0..num_pieces_half {
            let (_, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key_a, &mut connections[key_a].endgame)
                .unwrap();
            assert!(!connections[key_a].endgame);
            let mut subpieces =
                torrent_state.allocate_piece(index, key_a, &file_and_info.file_store);
            connections[key_a].append_and_fill(&mut subpieces);
            if i % 2 == 0 {
                connections[key_a].handle_message(
                    PeerMessage::Piece {
                        index,
                        begin: 0,
                        data: vec![3; SUBPIECE_SIZE as usize].into(),
                    },
                    &mut state_ref,
                    scope,
                );
                connections[key_a].handle_message(
                    PeerMessage::Piece {
                        index,
                        begin: SUBPIECE_SIZE,
                        data: vec![3; SUBPIECE_SIZE as usize].into(),
                    },
                    &mut state_ref,
                    scope,
                );
            }
        }
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        let (_, torrent_state) = state_ref.state().unwrap();
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert!(!connections[key_a].endgame);
        assert!(!connections[key_b].endgame);
        let (_, torrent_state) = state_ref.state().unwrap();
        let remaining = torrent_state.num_pieces()
            - torrent_state.piece_selector.total_allocated()
            - torrent_state.piece_selector.total_completed();
        // request the rest from the other peer so that everything has been allocated
        for _ in 0..remaining {
            let (_, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key_b, &mut connections[key_b].endgame)
                .unwrap();
            assert!(!connections[key_b].endgame);
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let mut subpieces =
                torrent_state.allocate_piece(index, key_b, &file_and_info.file_store);
            connections[key_b].append_and_fill(&mut subpieces);
        }
        for _ in 0..remaining {
            let (_, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key_a, &mut connections[key_a].endgame)
                .unwrap();
            assert!(connections[key_a].endgame);
            // Never request something we are in the process of downloading
            assert!(
                !connections[key_a]
                    .queued
                    .iter()
                    .any(|piece| piece.index == index)
            );
            assert!(
                !connections[key_a]
                    .inflight
                    .iter()
                    .any(|piece| piece.index == index)
            );
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let mut subpieces =
                torrent_state.allocate_piece(index, key_a, &file_and_info.file_store);
            connections[key_a].append_and_fill(&mut subpieces);
        }
    });
}

// BEP 10 and BEP 9 Tests

#[test]
fn extension_protocol_handshake() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        // Create peer with extension protocol support
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;
        assert!(connections[key_a].extensions.is_empty());
        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();

        // Simulate receiving extension handshake with ut_metadata support
        let handshake_data = format!(
            "d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}e1:v14:TestClient 1.0ee"
        );
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should have created metadata extension
        assert!(!connections[key_a].extensions.is_empty());
        assert!(connections[key_a].extensions.contains_key(&1)); // ut_metadata extension ID
        assert_eq!(connections[key_a].max_queue_size, 200); // Default value since no reqq specified
        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn extension_handshake_with_reqq() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();

        // Handshake with custom queue size
        let handshake_data =
            format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}e4:reqqi100eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        assert_eq!(connections[key_a].max_queue_size, 100);
        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn extension_handshake_malformed() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let invalid_data = b"invalid bencoded data";
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: invalid_data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key_a].pending_disconnect.is_some());
    });
}

#[test]
fn extension_handshake_missing_m_field() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        // Missing 'm' field
        let handshake_data = br#"d1:v14:TestClient 1.0ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key_a].pending_disconnect.is_some());
        assert!(matches!(
            connections[key_a].pending_disconnect,
            Some(DisconnectReason::ProtocolError(_))
        ));
    });
}

#[test]
fn metadata_extension_request_message() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();
        // First send handshake to set up extension
        let handshake_data = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Clear any messages from handshake
        connections[key_a].outgoing_msgs_buffer.clear();

        // Send metadata request (message type 0, piece 0)
        let request_data = br#"d8:msg_typei0e5:piecei0ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1, // ut_metadata extension ID
                data: request_data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );
        let metadata = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode();

        assert!(!connections[key_a].outgoing_msgs_buffer.is_empty());
        let response = &connections[key_a].outgoing_msgs_buffer[0];
        if let PeerMessage::Extended { id, data } = &response.message {
            let mut de = Deserializer::from_slice(&data[..]);
            let message: MetadataMessage = <MetadataMessage>::deserialize(&mut de).unwrap();
            assert_eq!(
                message,
                MetadataMessage {
                    msg_type: 1, // DATA
                    piece: 0,
                    total_size: Some(metadata.len() as i32)
                }
            );
            let metadata_piece = &data[de.byte_offset()..];
            assert_eq!(metadata_piece, &metadata);
            assert_eq!(*id, 3); // The peer should use the ID we told it to use (3)
        } else {
            panic!("Expected Extended message");
        }
        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn metadata_extension_data_message() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();
        let handshake_data = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        let mut data_msg = format!("d8:msg_typei1e5:piecei0e10:total_sizei{expected_size}ee")
            .as_bytes()
            .to_vec();
        // Append some dummy metadata
        data_msg.extend_from_slice(&vec![0u8; expected_size as usize]);

        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: data_msg.into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn metadata_extension_reject_message() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();
        // Set up extension
        let handshake_data = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        let reject_data = br#"d8:msg_typei2e5:piecei0ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: reject_data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn metadata_extension_invalid_message_type() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();
        // Set up extension
        let handshake_data = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Send invalid message type (message type 99)
        let invalid_data = br#"d8:msg_typei99e5:piecei0ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: invalid_data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should not disconnect for unknown message types (as per BEP 9)
        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn extension_message_unknown_id() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        // Send message with unknown extension ID (no handshake first)
        let data = br#"d8:msg_typei0e5:piecei0ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 99, // Unknown extension ID
                data: data.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should not disconnect for unknown extension IDs
        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn metadata_extension_piece_bounds_validation() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();

        // Set up extension with known metadata size
        let handshake_data = format!("d1:md11:ut_metadatai3ee13:metadata_sizei{expected_size}eee");
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Send request for negative piece index
        let invalid_request = br#"d8:msg_typei0e5:piecei-1ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: invalid_request.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should handle gracefully (implementation specific)
        // This test ensures the implementation doesn't panic

        // Send request for very large piece index
        let large_request = br#"d8:msg_typei0e5:piecei999999ee"#;
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: large_request.to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key_a].pending_disconnect.is_some());
    });
}

#[test]
fn extension_handshake_generates_correct_message() {
    use crate::peer_comm::extended_protocol::extension_handshake_msg;
    let mut download_state = setup_test();
    download_state.listener_port = Some(1234);

    let mut state_ref = download_state.as_ref();
    let handshake = extension_handshake_msg(&mut state_ref, &torrent::Config::default());

    if let PeerMessage::Extended { id, data } = handshake {
        assert_eq!(id, 0); // Handshake uses ID 0

        let expected_size = state_ref
            .state()
            .unwrap()
            .0
            .metadata
            .construct_info()
            .encode()
            .len();
        // Parse the bencoded data to verify structure
        let parsed: bt_bencode::Value = bt_bencode::from_slice(&data).unwrap();
        let dict = parsed.as_dict().unwrap();

        // Should contain 'm' field with ut_metadata
        assert!(dict.contains_key("m".as_bytes()));
        let m = dict.get("m".as_bytes()).unwrap().as_dict().unwrap();
        assert!(m.contains_key("ut_metadata".as_bytes()));

        assert!(dict.contains_key("v".as_bytes()));
        assert_eq!(dict.get("p".as_bytes()).unwrap().as_u64().unwrap(), 1234);
        assert_eq!(
            dict.get("reqq".as_bytes()).unwrap().as_u64().unwrap(),
            state_ref.config.max_reported_outstanding_requests
        );
        assert_eq!(
            dict.get("metadata_size".as_bytes())
                .unwrap()
                .as_u64()
                .unwrap(),
            expected_size as u64
        );
    } else {
        panic!("Expected Extended message");
    }
}

#[test]
fn metadata_download_single_piece() {
    let (mut download_state, torrent_info) = setup_uninitialized_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        // Verify state is not initialized
        assert!(!state_ref.is_initialzied());
        assert!(state_ref.state().is_none());

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        // Get the actual metadata that should be downloaded
        let metadata_bytes = torrent_info.construct_info().encode();

        // Set up extension handshake - peer tells us they have metadata of this size
        let handshake_data = format!(
            "d1:md11:ut_metadatai3ee13:metadata_sizei{}ee",
            metadata_bytes.len()
        );
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should have created metadata extension and sent requests
        assert!(!connections[key_a].extensions.is_empty());
        assert!(connections[key_a].extensions.contains_key(&1)); // ut_metadata extension ID
        assert!(!connections[key_a].outgoing_msgs_buffer.is_empty()); // Should have sent metadata requests

        // Clear outgoing messages
        connections[key_a].outgoing_msgs_buffer.clear();

        // Since metadata is small (< 16KiB), it should be a single piece
        let mut data_msg = format!(
            "d8:msg_typei1e5:piecei0e10:total_sizei{}ee",
            metadata_bytes.len()
        )
        .as_bytes()
        .to_vec();
        data_msg.extend_from_slice(&metadata_bytes);

        // Send the metadata as a DATA message
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1, // ut_metadata extension ID from our side
                data: data_msg.into(),
            },
            &mut state_ref,
            scope,
        );

        // Check if there was a hash mismatch or other error
        if let Some(reason) = &connections[key_a].pending_disconnect {
            panic!("Peer got disconnected: {reason:?}");
        }

        // State should now be initialized with the downloaded metadata
        if !state_ref.is_initialzied() {
            // Let's check what went wrong - maybe the metadata size doesn't match?
            panic!(
                "State was not initialized after receiving metadata. Metadata size: {}, Expected info hash: {:?}",
                metadata_bytes.len(),
                state_ref.info_hash()
            );
        }

        let (file_and_meta, torrent_state) = state_ref.state().unwrap();

        // Verify the metadata matches what we sent
        assert_eq!(
            file_and_meta.metadata.construct_info().encode(),
            metadata_bytes
        );
        assert_eq!(torrent_state.num_pieces(), torrent_info.pieces.len());

        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn metadata_download_multiple_pieces() {
    // Use large metadata to test real multi-piece download
    let (mut download_state, torrent_info) = setup_uninitialized_test_with_metadata_size(true);

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        // Verify state is not initialized
        assert!(!state_ref.is_initialzied());

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[key_a].extended_extension = true;

        // Get the actual metadata - this should now be large enough to require multiple pieces
        let metadata_bytes = torrent_info.construct_info().encode();
        println!(
            "Metadata size: {} bytes (should be > {} for multi-piece)",
            metadata_bytes.len(),
            SUBPIECE_SIZE
        );

        // Verify we have large enough metadata to test multi-piece download
        assert!(
            metadata_bytes.len() > SUBPIECE_SIZE as usize,
            "Metadata should be larger than {SUBPIECE_SIZE} bytes to test multi-piece download"
        );

        // Set up extension handshake
        let handshake_data = format!(
            "d1:md11:ut_metadatai3ee13:metadata_sizei{}ee",
            metadata_bytes.len()
        );
        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.as_bytes().to_vec().into(),
            },
            &mut state_ref,
            scope,
        );

        // Should have requested multiple pieces
        assert!(!connections[key_a].outgoing_msgs_buffer.is_empty());
        connections[key_a].outgoing_msgs_buffer.clear();

        // Calculate the number of pieces needed
        let piece_size = SUBPIECE_SIZE as usize;
        let num_pieces = metadata_bytes.len().div_ceil(piece_size);
        println!(
            "Will need {} pieces to download {} bytes of metadata",
            num_pieces,
            metadata_bytes.len()
        );

        // Send all pieces except the last one
        for piece_idx in 0..(num_pieces - 1) {
            let start_offset = piece_idx * piece_size;
            let end_offset = start_offset + piece_size;

            let mut data_msg = format!(
                "d8:msg_typei1e5:piecei{}e10:total_sizei{}ee",
                piece_idx,
                metadata_bytes.len()
            )
            .as_bytes()
            .to_vec();
            data_msg.extend_from_slice(&metadata_bytes[start_offset..end_offset]);

            connections[key_a].handle_message(
                PeerMessage::Extended {
                    id: 1,
                    data: data_msg.into(),
                },
                &mut state_ref,
                scope,
            );

            // Should still not be initialized, but may request next piece
            assert!(!state_ref.is_initialzied());
            connections[key_a].outgoing_msgs_buffer.clear();
        }

        // Send the final piece
        let final_piece_idx = num_pieces - 1;
        let start_offset = final_piece_idx * piece_size;

        let mut data_msg = format!(
            "d8:msg_typei1e5:piecei{}e10:total_sizei{}ee",
            final_piece_idx,
            metadata_bytes.len()
        )
        .as_bytes()
        .to_vec();
        data_msg.extend_from_slice(&metadata_bytes[start_offset..]);

        connections[key_a].handle_message(
            PeerMessage::Extended {
                id: 1,
                data: data_msg.into(),
            },
            &mut state_ref,
            scope,
        );

        // Now the state should be initialized with the complete metadata
        if let Some(reason) = &connections[key_a].pending_disconnect {
            panic!("Peer got disconnected: {reason:?}");
        }

        assert!(
            state_ref.is_initialzied(),
            "State should be initialized after receiving all metadata pieces"
        );
        let (file_and_meta, torrent_state) = state_ref.state().unwrap();

        // Verify the metadata matches what we sent
        assert_eq!(
            file_and_meta.metadata.construct_info().encode(),
            metadata_bytes
        );
        assert_eq!(torrent_state.num_pieces(), torrent_info.pieces.len());

        assert!(connections[key_a].pending_disconnect.is_none());
    });
}

#[test]
fn upload_throughput_tracking() {
    let mut download_state = setup_seeding_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        // Setup: peer have no pieces and is interested in our data
        connections[key].handle_message(PeerMessage::HaveNone, &mut state_ref, scope);
        connections[key].handle_message(PeerMessage::Interested, &mut state_ref, scope);

        assert_eq!(connections[key].network_stats.upload_throughput, 0);
        assert_eq!(connections[key].network_stats.prev_upload_throughput, 0);

        connections[key].handle_message(
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );

        // After sending the piece, upload throughput should be tracked
        assert_eq!(
            connections[key].network_stats.upload_throughput,
            SUBPIECE_SIZE as u64
        );

        // Request another subpiece
        connections[key].handle_message(
            PeerMessage::Request {
                index: 0,
                begin: SUBPIECE_SIZE,
                length: SUBPIECE_SIZE,
            },
            &mut state_ref,
            scope,
        );

        // Upload throughput should accumulate
        assert_eq!(
            connections[key].network_stats.upload_throughput,
            (SUBPIECE_SIZE * 2) as u64
        );

        // Simulate a tick to move current throughput to prev_throughput
        let mut event_q = Queue::<TorrentEvent, 512>::new();
        let (mut event_tx, _event_rx) = event_q.split();
        tick(
            &Duration::from_millis(1500),
            &mut connections,
            &Default::default(),
            &mut state_ref,
            &mut event_tx,
        );

        // After tick, current throughput is reset and moved to prev
        assert_eq!(
            connections[key].network_stats.prev_upload_throughput,
            ((SUBPIECE_SIZE * 2) as f64 / 1.5) as u64
        );
        assert_eq!(connections[key].network_stats.upload_throughput, 0);
    });
}

#[test]
fn unchoke_selection_based_on_download_throughput() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        // Set max_unchoked to 10 so we have room for regular and optimistic unchokes
        torrent_state.config.max_unchoked = 10;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 12 interested peers with varying download throughputs
        // All peers start choked
        let peer_ids: Vec<_> = (0..12)
            .map(|i| {
                let key = connections.insert_with_key(|k| generate_peer(true, k));
                connections[key].peer_interested = true;
                connections[key].is_choking = true;
                // Simulate different download amounts in last round
                connections[key].network_stats.downloaded_in_last_round = i * 1000;
                key
            })
            .collect();

        // Verify initial state
        assert_eq!(torrent_state.num_unchoked, 0);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // With max_unchoked = 10, we expect:
        // - 8 regular unchoke slots (10 - 10/5)
        // - 2 optimistic unchoke slots reserved

        // The top 8 peers by download throughput should be unchoked (indices 11 down to 4)
        for i in 4..12 {
            assert!(
                !connections[peer_ids[i]].is_choking,
                "Peer {} with throughput {} should be unchoked",
                i,
                connections[peer_ids[i]]
                    .network_stats
                    .downloaded_in_last_round
            );
            assert!(!connections[peer_ids[i]].optimistically_unchoked);
        }

        // The bottom 4 peers should still be choked
        for i in 0..4 {
            assert!(
                connections[peer_ids[i]].is_choking,
                "Peer {} with throughput {} should be choked",
                i,
                connections[peer_ids[i]]
                    .network_stats
                    .downloaded_in_last_round
            );
        }

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 8);
    });
}

#[test]
fn unchoke_chokes_non_interested_peers() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 3 peers - 2 start unchoked, 1 starts choked
        let interested_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[interested_peer].peer_interested = true;
        connections[interested_peer].is_choking = false; // Currently unchoked
        connections[interested_peer]
            .network_stats
            .downloaded_in_last_round = 5000;
        torrent_state.num_unchoked += 1; // Track unchoked peer

        let not_interested_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[not_interested_peer].peer_interested = false;
        connections[not_interested_peer].is_choking = false; // Currently unchoked
        connections[not_interested_peer]
            .network_stats
            .downloaded_in_last_round = 10000;
        torrent_state.num_unchoked += 1; // Track unchoked peer

        let new_interested_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[new_interested_peer].peer_interested = true;
        connections[new_interested_peer].is_choking = true;
        connections[new_interested_peer]
            .network_stats
            .downloaded_in_last_round = 3000;

        // Verify initial state: 2 unchoked
        assert_eq!(torrent_state.num_unchoked, 2);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Not interested peer should be choked even with high throughput
        assert!(connections[not_interested_peer].is_choking);

        // Interested peers should remain or become unchoked
        assert!(!connections[interested_peer].is_choking);
        assert!(!connections[new_interested_peer].is_choking);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 2);
    });
}

#[test]
fn unchoke_chokes_pending_disconnect_peers() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        let pending_disconnect_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[pending_disconnect_peer].peer_interested = true;
        connections[pending_disconnect_peer].is_choking = false; // Currently unchoked
        connections[pending_disconnect_peer].pending_disconnect = Some(DisconnectReason::Idle);
        connections[pending_disconnect_peer]
            .network_stats
            .downloaded_in_last_round = 10000;
        torrent_state.num_unchoked += 1; // Track unchoked peer

        let healthy_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[healthy_peer].peer_interested = true;
        connections[healthy_peer].is_choking = true;
        connections[healthy_peer]
            .network_stats
            .downloaded_in_last_round = 3000;

        // Verify initial state: 1 unchoked
        assert_eq!(torrent_state.num_unchoked, 1);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Pending disconnect peer should be choked
        assert!(connections[pending_disconnect_peer].is_choking);

        // Healthy peer should be unchoked
        assert!(!connections[healthy_peer].is_choking);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 1);
    });
}

#[test]
fn unchoke_promotes_optimistic_unchoke_to_regular() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create a peer that was optimistically unchoked and has good throughput
        let opt_unchoked_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[opt_unchoked_peer].peer_interested = true;
        connections[opt_unchoked_peer].is_choking = false;
        connections[opt_unchoked_peer].optimistically_unchoked = true;
        connections[opt_unchoked_peer]
            .network_stats
            .downloaded_in_last_round = 10000;
        torrent_state.num_unchoked += 1; // Track unchoked peer

        // Create some lower throughput peers
        for i in 0..3 {
            let key = connections.insert_with_key(|k| generate_peer(true, k));
            connections[key].peer_interested = true;
            connections[key].is_choking = true;
            connections[key].network_stats.downloaded_in_last_round = i * 1000;
        }

        // Verify initial state: 1 unchoked (optimistically)
        assert_eq!(torrent_state.num_unchoked, 1);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Optimistically unchoked peer should be promoted (no longer optimistically unchoked)
        assert!(!connections[opt_unchoked_peer].is_choking);
        assert!(!connections[opt_unchoked_peer].optimistically_unchoked);

        // The optimistic unchoke time scaler should be reset
        assert_eq!(torrent_state.ticks_to_recalc_optimistic_unchoke, 0);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 4);
    });
}

#[test]
fn optimistic_unchoke_selection() {
    use std::time::Instant;

    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 10;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create choked interested peers with different last_optimistically_unchoked times
        let old_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[old_peer].peer_interested = true;
        connections[old_peer].is_choking = true;
        connections[old_peer].last_optimistically_unchoked =
            Some(Instant::now() - std::time::Duration::from_secs(100));

        let recent_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[recent_peer].peer_interested = true;
        connections[recent_peer].is_choking = true;
        connections[recent_peer].last_optimistically_unchoked =
            Some(Instant::now() - std::time::Duration::from_secs(10));

        let never_unchoked_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[never_unchoked_peer].peer_interested = true;
        connections[never_unchoked_peer].is_choking = true;
        connections[never_unchoked_peer].last_optimistically_unchoked = None;

        // Verify initial state: all choked
        assert_eq!(torrent_state.num_unchoked, 0);

        // Recalculate optimistic unchokes
        torrent_state.recalculate_optimistic_unchokes(&mut connections);

        // With max_unchoked = 10, num_opt_unchoked = max(1, 10/5) = 2
        // Both never_unchoked_peer and old_peer should be optimistically unchoked
        // (they have the longest wait times: infinity and 100 seconds)
        // Note: must be both optimistically_unchoked AND not choking (active)
        let num_actively_opt_unchoked = connections
            .values()
            .filter(|p| p.optimistically_unchoked && !p.is_choking)
            .count();

        assert_eq!(num_actively_opt_unchoked, 2);

        // The peers with longest wait should be selected and actually unchoked
        assert!(
            connections[old_peer].optimistically_unchoked && !connections[old_peer].is_choking,
            "Old peer should be optimistically unchoked and not choking"
        );
        assert!(
            connections[never_unchoked_peer].optimistically_unchoked
                && !connections[never_unchoked_peer].is_choking,
            "Never unchoked peer should be optimistically unchoked and not choking"
        );

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 2);
    });
}

#[test]
fn optimistic_unchoke_rotates_out_previous() {
    use std::time::Instant;

    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create a peer that's currently optimistically unchoked
        let current_opt = connections.insert_with_key(|k| generate_peer(true, k));
        connections[current_opt].peer_interested = true;
        connections[current_opt].is_choking = false;
        connections[current_opt].optimistically_unchoked = true;
        connections[current_opt].last_optimistically_unchoked =
            Some(Instant::now() - std::time::Duration::from_secs(5));
        torrent_state.num_unchoked += 1; // Track unchoked peer

        // Create a peer that has been waiting longer
        let waiting_peer = connections.insert_with_key(|k| generate_peer(true, k));
        connections[waiting_peer].peer_interested = true;
        connections[waiting_peer].is_choking = true;
        connections[waiting_peer].last_optimistically_unchoked =
            Some(Instant::now() - std::time::Duration::from_secs(100));

        // Verify initial state: 1 unchoked (optimistically)
        assert_eq!(torrent_state.num_unchoked, 1);

        // Recalculate optimistic unchokes
        torrent_state.recalculate_optimistic_unchokes(&mut connections);

        // With max_unchoked = 5, num_opt_unchoked = max(1, 5/5) = 1
        // The waiting peer should now be optimistically unchoked and not choking
        assert!(
            connections[waiting_peer].optimistically_unchoked
                && !connections[waiting_peer].is_choking
        );

        // Only 1 peer should be actively (not choked) optimistically unchoked
        let num_actively_opt_unchoked = connections
            .values()
            .filter(|p| p.optimistically_unchoked && !p.is_choking)
            .count();

        assert_eq!(num_actively_opt_unchoked, 1);

        // The current_opt should have been choked (rotated out)
        assert!(connections[current_opt].is_choking);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 1);
    });
}

#[test]
fn optimistic_unchoke_ignores_non_interested_peers() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create a not-interested peer
        let not_interested = connections.insert_with_key(|k| generate_peer(true, k));
        connections[not_interested].peer_interested = false;
        connections[not_interested].is_choking = true;

        // Create an interested peer
        let interested = connections.insert_with_key(|k| generate_peer(true, k));
        connections[interested].peer_interested = true;
        connections[interested].is_choking = true;

        // Verify initial state: all choked
        assert_eq!(torrent_state.num_unchoked, 0);

        // Recalculate optimistic unchokes
        torrent_state.recalculate_optimistic_unchokes(&mut connections);

        // Only the interested peer should be optimistically unchoked
        assert!(!connections[not_interested].optimistically_unchoked);
        assert!(
            connections[interested].optimistically_unchoked && !connections[interested].is_choking
        );

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 1);
    });
}

#[test]
fn optimistic_unchoke_ignores_pending_disconnect_peers() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 5;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create a peer pending disconnect
        let pending = connections.insert_with_key(|k| generate_peer(true, k));
        connections[pending].peer_interested = true;
        connections[pending].is_choking = true;
        connections[pending].pending_disconnect = Some(DisconnectReason::Idle);

        // Create a healthy peer
        let healthy = connections.insert_with_key(|k| generate_peer(true, k));
        connections[healthy].peer_interested = true;
        connections[healthy].is_choking = true;

        // Verify initial state: all choked
        assert_eq!(torrent_state.num_unchoked, 0);

        // Recalculate optimistic unchokes
        torrent_state.recalculate_optimistic_unchokes(&mut connections);

        // Only the healthy peer should be optimistically unchoked
        assert!(!connections[pending].optimistically_unchoked);
        assert!(connections[healthy].optimistically_unchoked);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked, 1);
    });
}

#[test]
fn unchoke_reserves_slots_for_optimistic() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        torrent_state.config.max_unchoked = 10;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 15 interested peers with varying throughput - all start choked
        for i in 0..15 {
            let key = connections.insert_with_key(|k| generate_peer(true, k));
            connections[key].peer_interested = true;
            connections[key].is_choking = true;
            connections[key].network_stats.downloaded_in_last_round = i * 1000;
        }

        // Verify initial state: all choked
        assert_eq!(torrent_state.num_unchoked, 0);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // With max_unchoked = 10:
        // - optimistic_slots = max(1, 10/5) = 2
        // - regular_slots = 10 - 2 = 8

        // Should have exactly 8 non-optimistically unchoked peers
        let regular_unchoked = connections
            .values()
            .filter(|p| !p.is_choking && !p.optimistically_unchoked)
            .count();

        assert_eq!(regular_unchoked, 8);
        assert_eq!(torrent_state.num_unchoked, 8);

        // Verify num_unchoked matches actual unchoked count
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
    });
}

#[test]
fn optimistically_unchoked_disconnect_resets_timer() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));
        {
            let (_, torrent_state) = state_ref.state().unwrap();

            torrent_state.config.max_unchoked = 5;

            // A single peer that is optimistically unchoked
            connections[key].peer_interested = true;
            connections[key].is_choking = false;
            connections[key].optimistically_unchoked = true;
            torrent_state.num_unchoked += 1;
            assert!(torrent_state.ticks_to_recalc_optimistic_unchoke > 0);
        }

        let mut sq = BackloggedSubmissionQueue::new(MockSubmissionQueue);
        let mut events = SlotMap::<EventId, EventData>::with_key();

        connections[key].disconnect(&mut sq, &mut events, &mut state_ref);

        {
            let (_, torrent_state) = state_ref.state().unwrap();
            assert_eq!(torrent_state.ticks_to_recalc_optimistic_unchoke, 0);
            assert_eq!(torrent_state.num_unchoked, 0);
        }
    });
}
#[test]
fn upload_only_field_in_extended_handshake() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));
        let (file_and_meta, _) = state_ref.state().unwrap();
        let metadata_size = file_and_meta.metadata.construct_info().encode().len() as i64;

        // Peer hasn't declared upload_only yet
        assert!(!connections[key].is_upload_only);

        // Simulate receiving extended handshake with upload_only field set to 1
        let mut handshake = std::collections::BTreeMap::new();
        let m: std::collections::BTreeMap<&str, u8> = [("ut_metadata", 1u8)].into_iter().collect();
        handshake.insert("m", bt_bencode::to_value(&m).unwrap());
        handshake.insert("upload_only", bt_bencode::to_value(&1i64).unwrap());
        // Needed for ut_metadata
        handshake.insert(
            "metadata_size",
            bt_bencode::to_value(dbg!(&metadata_size)).unwrap(),
        );

        let handshake_data = bt_bencode::to_vec(&handshake).unwrap();
        connections[key].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.into(),
            },
            &mut state_ref,
            scope,
        );

        // Peer should now be marked as upload_only based on handshake field
        assert!(connections[key].is_upload_only);

        // Test with upload_only = 0
        let key2 = connections.insert_with_key(|k| generate_peer(true, k));
        let mut handshake2 = std::collections::BTreeMap::new();
        let m2: std::collections::BTreeMap<&str, u8> = [("ut_metadata", 1u8)].into_iter().collect();
        handshake2.insert("m", bt_bencode::to_value(&m2).unwrap());
        handshake2.insert("upload_only", bt_bencode::to_value(&0i64).unwrap());
        // Needed for ut_metadata
        handshake2.insert(
            "metadata_size",
            bt_bencode::to_value(dbg!(&metadata_size)).unwrap(),
        );

        let handshake_data2 = bt_bencode::to_vec(&handshake2).unwrap();
        connections[key2].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data2.into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(!connections[key2].is_upload_only);
    });
}

#[test]
fn upload_only_extension_message_received() {
    let mut download_state = setup_test();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        // Peer hasn't declared upload_only yet
        assert!(!connections[key].is_upload_only);

        // Simulate receiving extended handshake with upload_only extension enabled
        let mut handshake = std::collections::BTreeMap::new();
        // 3 explicitly does not match our implementations extension id for it
        let m: std::collections::BTreeMap<&str, u8> = [("upload_only", 3u8)].into_iter().collect();
        handshake.insert("m", bt_bencode::to_value(&m).unwrap());

        let handshake_data = bt_bencode::to_vec(&handshake).unwrap();
        connections[key].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.into(),
            },
            &mut state_ref,
            scope,
        );

        // Test receiving upload_only extension message (value = 1)
        let upload_only_msg = [1_u8].as_slice();
        connections[key].handle_message(
            PeerMessage::Extended {
                id: 2, // ID for upload_only extension
                data: upload_only_msg.into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(connections[key].is_upload_only);

        // Test receiving upload_only = 0 (peer is no longer upload only)
        let upload_only_msg = [0_u8].as_slice();
        connections[key].handle_message(
            PeerMessage::Extended {
                id: 2,
                data: upload_only_msg.into(),
            },
            &mut state_ref,
            scope,
        );

        assert!(!connections[key].is_upload_only);
    });
}

#[test]
fn upload_only_extension_message_sent_on_torrent_complete() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));
        let other_peer = connections.insert_with_key(|k| generate_peer(true, k));
        // Doesn't support the extension
        let third_peer = connections.insert_with_key(|k| generate_peer(true, k));

        // Set up extension support
        let mut handshake = std::collections::BTreeMap::new();
        let m: std::collections::BTreeMap<&str, u8> = [("upload_only", 3u8)].into_iter().collect();
        handshake.insert("m", bt_bencode::to_value(&m).unwrap());
        let handshake_data = bt_bencode::to_vec(&handshake).unwrap();
        connections[key].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.into(),
            },
            &mut state_ref,
            scope,
        );

        let mut handshake = std::collections::BTreeMap::new();
        let m: std::collections::BTreeMap<&str, u8> = [("upload_only", 3u8)].into_iter().collect();
        handshake.insert("m", bt_bencode::to_value(&m).unwrap());
        let handshake_data = bt_bencode::to_vec(&handshake).unwrap();
        connections[other_peer].handle_message(
            PeerMessage::Extended {
                id: 0,
                data: handshake_data.into(),
            },
            &mut state_ref,
            scope,
        );

        connections[key].outgoing_msgs_buffer.clear();

        // Note that the peer is marked as upload only with this message
        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);

        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();

        // Download all pieces to complete the torrent
        for _i in 0..num_pieces {
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key, &mut connections[key].endgame)
                .unwrap();
            let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
            connections[key].append_and_fill(&mut subpieces);

            // Complete the piece
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: 0,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: SUBPIECE_SIZE,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
        }

        let (_, torrent_state) = state_ref.state().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert!(torrent_state.is_complete);

        // Should have sent upload_only extension message with value 1
        let has_upload_only_msg = connections[key].outgoing_msgs_buffer.iter_mut().any(|msg| {
            if let PeerMessage::Extended { id: 3, data } = &mut msg.message {
                data.len() == 1 && data.get_u8() == 1
            } else {
                false
            }
        });
        dbg!(&connections[key].outgoing_msgs_buffer);
        assert!(
            has_upload_only_msg,
            "Should send upload_only extension message when torrent completes"
        );
        let has_upload_only_msg =
            connections[other_peer]
                .outgoing_msgs_buffer
                .iter_mut()
                .any(|msg| {
                    if let PeerMessage::Extended { id: 3, data } = &mut msg.message {
                        data.len() == 1 && data.get_u8() == 1
                    } else {
                        false
                    }
                });
        assert!(
            has_upload_only_msg,
            "Should send upload_only extension message when torrent completes"
        );
        let has_upload_only_msg =
            connections[third_peer]
                .outgoing_msgs_buffer
                .iter_mut()
                .any(|msg| {
                    if let PeerMessage::Extended { id: 3, data } = &mut msg.message {
                        data.len() == 1 && data.get_u8() == 1
                    } else {
                        false
                    }
                });
        assert!(
            !has_upload_only_msg,
            "Should not send upload_only extension message to peer that doesn't support it"
        );
    });
}

#[test]
fn redundant_connection_disconnect_both_upload_only() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        // HaveAll automatically marks peer as upload_only
        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        assert!(connections[key].is_upload_only);

        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();

        // Download all pieces to complete the torrent
        for _i in 0..num_pieces {
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key, &mut connections[key].endgame)
                .unwrap();
            let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
            connections[key].append_and_fill(&mut subpieces);

            // Complete the piece
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: 0,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: SUBPIECE_SIZE,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
        }

        let (_, torrent_state) = state_ref.state().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);

        // Both sides are upload only, should have pending disconnect
        assert!(
            connections[key].pending_disconnect.is_some(),
            "Should disconnect when both sides are upload only"
        );
        assert!(
            matches!(
                connections[key].pending_disconnect,
                Some(DisconnectReason::RedundantConnection)
            ),
            "Disconnect reason should be RedundantConnection"
        );
    });
}

#[test]
fn no_disconnect_when_peer_not_upload_only() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key = connections.insert_with_key(|k| generate_peer(true, k));

        connections[key].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        // simulate it's not upload_only
        connections[key].is_upload_only = false;

        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();

        // Download all pieces to complete the torrent
        for _i in 0..num_pieces {
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key, &mut connections[key].endgame)
                .unwrap();
            let mut subpieces = torrent_state.allocate_piece(index, key, &file_and_info.file_store);
            connections[key].append_and_fill(&mut subpieces);

            // Complete the piece
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: 0,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
            connections[key].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: SUBPIECE_SIZE,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
        }

        let (_, torrent_state) = state_ref.state().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);

        // Peer is not upload only, so we should NOT disconnect (they might want to download from us)
        assert!(
            connections[key].pending_disconnect.is_none(),
            "Should NOT disconnect when peer is not upload only (they might download from us)"
        );
    });
}

#[test]
fn disconnect_upload_only_peer_when_no_longer_interesting() {
    let mut download_state = setup_test();
    let mut event_q = Queue::<TorrentEvent, 512>::new();
    let (mut event_tx, _event_rx) = event_q.split();

    rayon::in_place_scope(|scope| {
        let mut state_ref = download_state.as_ref();
        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();
        let key_a = connections.insert_with_key(|k| generate_peer(true, k));
        let key_b = connections.insert_with_key(|k| generate_peer(true, k));

        // key_a is upload only (via HaveAll) and has all pieces
        connections[key_a].handle_message(PeerMessage::HaveAll, &mut state_ref, scope);
        assert!(connections[key_a].is_upload_only);

        // key_b has all pieces but sent bitfield (not all set initially)
        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
        field.set(0, false);
        connections[key_b].handle_message(PeerMessage::Bitfield(field), &mut state_ref, scope);
        assert!(!connections[key_b].is_upload_only);

        // Now send Have for piece 0 to key_b
        connections[key_b].handle_message(PeerMessage::Have { index: 0 }, &mut state_ref, scope);

        // Download a piece from key_b to make progress
        let (file_and_info, torrent_state) = state_ref.state().unwrap();
        let index = torrent_state
            .piece_selector
            .next_piece(key_b, &mut connections[key_b].endgame)
            .unwrap();
        let mut subpieces = torrent_state.allocate_piece(index, key_b, &file_and_info.file_store);
        connections[key_b].append_and_fill(&mut subpieces);

        connections[key_b].handle_message(
            PeerMessage::Piece {
                index,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );
        connections[key_b].handle_message(
            PeerMessage::Piece {
                index,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut state_ref,
            scope,
        );

        let (_, torrent_state) = state_ref.state().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);

        // We're still interested in both peers, so no disconnect yet
        assert!(connections[key_a].pending_disconnect.is_none());
        assert!(connections[key_b].pending_disconnect.is_none());

        // Now complete all remaining pieces
        let (_, torrent_state) = state_ref.state().unwrap();
        let num_pieces = torrent_state.num_pieces();
        for _i in 0..(num_pieces - 1) {
            let (file_and_info, torrent_state) = state_ref.state().unwrap();
            let index = torrent_state
                .piece_selector
                .next_piece(key_b, &mut connections[key_b].endgame);

            if index.is_none() {
                break;
            }
            let index = index.unwrap();
            let mut subpieces =
                torrent_state.allocate_piece(index, key_b, &file_and_info.file_store);
            connections[key_b].append_and_fill(&mut subpieces);

            connections[key_b].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: 0,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
            connections[key_b].handle_message(
                PeerMessage::Piece {
                    index,
                    begin: SUBPIECE_SIZE,
                    data: vec![3; SUBPIECE_SIZE as usize].into(),
                },
                &mut state_ref,
                scope,
            );
        }

        let (_, torrent_state) = state_ref.state().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections, &mut event_tx);
        assert!(torrent_state.is_complete);
        assert!(!connections[key_a].is_interesting);
        // key_a is upload only and we're no longer interested
        // but we're also upload only now, so it should disconnect
        assert!(
            connections[key_a].pending_disconnect.is_some(),
            "Should disconnect upload_only peer when both sides are upload only"
        );
        assert!(
            matches!(
                connections[key_a].pending_disconnect,
                Some(DisconnectReason::RedundantConnection)
            ),
            "Disconnect reason should be RedundantConnection"
        );

        assert!(!connections[key_b].is_interesting);
        // key_b is not upload only, so we keep the connection (they might download from us)
        assert!(
            connections[key_b].pending_disconnect.is_none(),
            "Should NOT disconnect non-upload_only peer"
        );
    });
}

#[test]
fn seeding_round_robin_rotation_after_quota() {
    let mut download_state = setup_seeding_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        // Verify we're in seeding mode
        assert!(torrent_state.is_complete);

        torrent_state.config.max_unchoked = 3;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 3 interested peers
        let peer_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_a].peer_interested = true;

        let peer_b = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_b].peer_interested = true;

        let peer_c = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_c].peer_interested = true;

        // Unchoke peer A manually and set it up as having completed its quota
        connections[peer_a].is_choking = false;
        connections[peer_a].last_unchoked = Some(Instant::now() - Duration::from_secs(90)); // 1.5 minutes ago
        torrent_state.num_unchoked = 1;

        // Calculate quota: piece_length * SEEDING_PIECE_QUOTA
        // piece_length = SUBPIECE_SIZE * 2 = 32768
        // quota = 32768 * 20 = 655360 bytes
        let piece_length = torrent_state.piece_selector.avg_piece_length();
        let quota_bytes = (piece_length * 20) as u64; // SEEDING_PIECE_QUOTA = 20

        // Set peer A as having uploaded more than quota
        connections[peer_a].network_stats.upload_since_unchoked = quota_bytes + 100_000;
        connections[peer_a].network_stats.uploaded_in_last_round = 50_000;

        // Peers B and C have never been unchoked
        assert!(connections[peer_b].is_choking);
        assert!(connections[peer_c].is_choking);
        assert_eq!(connections[peer_b].last_unchoked, None);
        assert_eq!(connections[peer_c].last_unchoked, None);

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Peer A should now be choked (completed quota)
        assert!(
            connections[peer_a].is_choking,
            "Peer A should be choked after completing quota"
        );

        // At least one of peers B or C should be unchoked
        let b_unchoked = !connections[peer_b].is_choking;
        let c_unchoked = !connections[peer_c].is_choking;
        assert!(
            b_unchoked && c_unchoked,
            "Both of peers B or C should be unchoked"
        );

        // Verify num_unchoked is consistent
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked as usize, 2);
    });
}

#[test]
fn seeding_quota_not_met_keeps_unchoked() {
    let mut download_state = setup_seeding_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        // Verify we're in seeding mode
        assert!(torrent_state.is_complete);

        torrent_state.config.max_unchoked = 3;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 3 interested peers
        let peer_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_a].peer_interested = true;

        let peer_b = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_b].peer_interested = true;

        let peer_c = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_c].peer_interested = true;

        // Unchoke peer A manually
        connections[peer_a].is_choking = false;
        connections[peer_a].last_unchoked = Some(Instant::now() - Duration::from_secs(90)); // 1.5 minutes ago
        torrent_state.num_unchoked = 1;

        // Calculate quota
        let piece_length = torrent_state.piece_selector.avg_piece_length();
        let quota_bytes = (piece_length * 20) as u64; // SEEDING_PIECE_QUOTA = 20

        // Set peer A as having uploaded LESS than quota (only half)
        connections[peer_a].network_stats.upload_since_unchoked = quota_bytes / 2;
        connections[peer_a].network_stats.uploaded_in_last_round = 50_000;

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Peer A should remain unchoked (quota not met)
        assert!(
            !connections[peer_a].is_choking,
            "Peer A should remain unchoked since quota not met"
        );

        // Verify num_unchoked is consistent
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
    });
}

#[test]
fn seeding_time_threshold_not_met_keeps_unchoked() {
    let mut download_state = setup_seeding_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        // Verify we're in seeding mode
        assert!(torrent_state.is_complete);

        torrent_state.config.max_unchoked = 3;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Create 3 interested peers
        let peer_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_a].peer_interested = true;

        let peer_b = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_b].peer_interested = true;

        let peer_c = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_c].peer_interested = true;

        // Unchoke peer A manually but only 30 seconds ago (< 1 minute)
        connections[peer_a].is_choking = false;
        connections[peer_a].last_unchoked = Some(Instant::now() - Duration::from_secs(30));
        torrent_state.num_unchoked = 1;

        // Calculate quota
        let piece_length = torrent_state.piece_selector.avg_piece_length();
        let quota_bytes = (piece_length * 20) as u64; // SEEDING_PIECE_QUOTA = 20

        // Set peer A as having uploaded MORE than quota, but not enough time has passed
        connections[peer_a].network_stats.upload_since_unchoked = quota_bytes + 100_000;
        connections[peer_a].network_stats.uploaded_in_last_round = 50_000;

        // Recalculate unchokes
        torrent_state.recalculate_unchokes(&mut connections);

        // Peer A should remain unchoked (time threshold not met)
        assert!(
            !connections[peer_a].is_choking,
            "Peer A should remain unchoked since time threshold (1 minute) not met"
        );

        // Verify num_unchoked is consistent
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
    });
}

#[test]
fn seeding_quota_complete_peers_deprioritized() {
    let mut download_state = setup_seeding_test();

    rayon::in_place_scope(|_scope| {
        let mut state_ref = download_state.as_ref();
        let (_, torrent_state) = state_ref.state().unwrap();

        // Verify we're in seeding mode
        assert!(torrent_state.is_complete);

        // Set max_unchoked to 2 so we have limited slots
        torrent_state.config.max_unchoked = 3;

        let mut connections = SlotMap::<ConnectionId, PeerConnection>::with_key();

        // Calculate quota
        let piece_length = torrent_state.piece_selector.avg_piece_length();
        let quota_bytes = (piece_length * 20) as u64; // SEEDING_PIECE_QUOTA = 20

        // Create peer A and B - both unchoked and have completed their quota
        let peer_a = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_a].peer_interested = true;
        connections[peer_a].is_choking = false;
        connections[peer_a].last_unchoked = Some(Instant::now() - Duration::from_secs(90));
        connections[peer_a].network_stats.upload_since_unchoked = quota_bytes + 100_000;
        connections[peer_a].network_stats.uploaded_in_last_round = 50_000;

        let peer_b = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_b].peer_interested = true;
        connections[peer_b].is_choking = false;
        connections[peer_b].last_unchoked = Some(Instant::now() - Duration::from_secs(120));
        connections[peer_b].network_stats.upload_since_unchoked = quota_bytes + 80_000;
        connections[peer_b].network_stats.uploaded_in_last_round = 30_000;

        torrent_state.num_unchoked = 2;

        // Create peer C and D - never been unchoked
        let peer_c = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_c].peer_interested = true;

        let peer_d = connections.insert_with_key(|k| generate_peer(true, k));
        connections[peer_d].peer_interested = true;

        // Verify initial state: 2 unchoked (A and B, both quota complete)
        assert_eq!(torrent_state.num_unchoked, 2);

        // Recalculate unchokes with max_unchoked = 3
        // With optimistic unchoke slots (3/5 = 0, max(1, 0) = 1), we have 2 regular slots
        torrent_state.recalculate_unchokes(&mut connections);

        // Peers A and B (quota complete) should be choked
        // Peers C and/or D (never unchoked) should be unchoked
        let a_choked = connections[peer_a].is_choking;
        let b_choked = connections[peer_b].is_choking;
        let c_unchoked = !connections[peer_c].is_choking;
        let d_unchoked = !connections[peer_d].is_choking;

        // At least one of the quota-complete peers should be choked
        assert!(
            a_choked && b_choked,
            "Both quota-complete peer (A and B) should be choked"
        );

        // At least one of the never-unchoked peers should be unchoked
        assert!(
            c_unchoked && d_unchoked,
            "Both never-unchoked peer (C and D) should be unchoked"
        );

        // Verify num_unchoked is consistent
        let actual_unchoked = connections.values().filter(|p| !p.is_choking).count();
        assert_eq!(torrent_state.num_unchoked as usize, actual_unchoked);
        assert_eq!(torrent_state.num_unchoked as usize, 2);
    });
}
