use std::time::{Duration, Instant};

use slab::Slab;

use crate::{
    TorrentState,
    event_loop::tick,
    peer_comm::peer_connection::DisconnectReason,
    peer_connection::OutgoingMsg,
    piece_selector::{SUBPIECE_SIZE, Subpiece},
    test_utils::{generate_peer, setup_test},
};

use super::{peer_connection::PeerConnection, peer_protocol::PeerMessage};

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
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        assert!(!a.is_interesting);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        sent_and_marked_interested(&a);
        assert!(a.pending_disconnect.is_none());
        for piece_id in 0..torrent_info.pieces.len() {
            assert!(
                torrent_state
                    .piece_selector
                    .interesting_peer_pieces(a.conn_id)
                    .unwrap()[piece_id]
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

        // Do not mark as interestead if we've already completed the torrent
        let mut a = generate_peer(true, 3);
        torrent_state.is_complete = true;
        assert!(!a.is_interesting);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(!a.is_interesting);
    });
}

#[test]
fn fast_ext_have_none() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.is_interesting = true;
        a.handle_message(
            PeerMessage::HaveNone,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // we are not interestead in peers that have nothing
        sent_and_marked_not_interested(&a);
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(a.conn_id)
                .unwrap()
                .any()
        );
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
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::Have { index: 8 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        // We should be interestead now since we do not have the piece
        sent_and_marked_interested(&a);
        assert!(a.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::Interested,
            ordered: false
        }));
        a.outgoing_msgs_buffer.clear();
        for piece_id in 0..torrent_info.pieces.len() {
            if piece_id == 8 {
                assert!(
                    torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)
                        .unwrap()[piece_id]
                );
            } else {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)
                        .unwrap()[piece_id]
                );
            }
        }
        // Does not send interested again
        a.handle_message(
            PeerMessage::Have { index: 8 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.outgoing_msgs_buffer.is_empty());
        let mut subpieces = torrent_state.allocate_piece(8, &file_store);
        a.append_and_fill(&mut subpieces);

        let mut b = generate_peer(true, 1);
        assert!(!b.is_interesting);
        assert!(torrent_state.piece_selector.is_allocated(8));
        // Does not mark as interestead since the piece is inflight
        b.handle_message(
            PeerMessage::Have { index: 8 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(
            torrent_state
                .piece_selector
                .interesting_peer_pieces(a.conn_id)
                .unwrap()[8]
        );
    });
}

#[test]
fn have_invalid_indicies() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::Have { index: -1 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.pending_disconnect.is_some());
        let mut b = generate_peer(false, 0);
        b.handle_message(
            PeerMessage::Have {
                index: torrent_state.num_pieces() as i32,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(b.pending_disconnect.is_some());
    });
}

#[test]
fn have_without_interest() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    // Needed to avoid hitting asserts
    torrent_state.piece_selector.mark_allocated(8);
    torrent_state.piece_selector.mark_complete(8);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::Have { index: 8 },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Interpret the Have as HaveNone, the bitfield might have been omitted
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));
        assert!(a.pending_disconnect.is_none());
        // We should not be interestead now since we do already have the piece
        assert!(!a.is_interesting);
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(a.conn_id)
                .unwrap()
                .any()
        );
    });
}

#[test]
fn slow_start() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);
        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].prev_throughput, 0);
        assert_eq!(connections[key].throughput, 0);
        assert!(connections[key].target_inflight > 1);
        let old_desired_queue = connections[key].target_inflight;

        connections[key].peer_choking = false;
        let mut subpieces = torrent_state.allocate_piece(1, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert_eq!(connections[key].throughput, (SUBPIECE_SIZE * 2) as u64);
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].prev_throughput, 0);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 2);

        tick(
            &Duration::from_millis(1500),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        assert_eq!(connections[key].prev_throughput, 21845);
        assert_eq!(connections[key].throughput, 0);
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 2);

        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        assert_eq!(connections[key].prev_throughput, (SUBPIECE_SIZE * 2) as u64);
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 4);

        let mut subpieces = torrent_state.allocate_piece(3, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 3,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 3,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        assert_eq!(connections[key].prev_throughput, (SUBPIECE_SIZE * 2) as u64);
        // No longer slow start
        assert!(!connections[key].slow_start);
        assert_eq!(connections[key].target_inflight, old_desired_queue + 6);
    });
}

#[test]
fn desired_queue_size() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].peer_choking = false;
        connections[key].slow_start = false;
        let mut subpieces = torrent_state.allocate_piece(1, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        // 2 subpieces * 3
        assert_eq!(connections[key].target_inflight, 6);

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
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
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].peer_choking = false;
        connections[key].slow_start = false;
        for i in 1..7 {
            let mut subpieces = torrent_state.allocate_piece(i, &file_store);
            connections[key].append_and_fill(&mut subpieces);
        }
        assert_eq!(torrent_state.num_allocated(), 6);
        assert_eq!(connections[key].target_inflight, 4);
        assert_eq!(connections[key].queued.len(), 8);
        assert_eq!(connections[key].inflight.len(), 4);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        tick(
            &Duration::from_millis(650),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        // Want an odd number here to test releasing in flight pieces
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert_eq!(connections[key].queued.len(), 1);
        assert!(torrent_state.piece_selector.is_allocated(6));

        connections[key].handle_message(
            PeerMessage::Choke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(connections[key].is_choking);
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert_eq!(connections[key].queued.len(), 0);
        // 1 piece completed (pending hashing), one was released
        assert_eq!(torrent_state.num_allocated(), 4);
        // No longer inflight!
        assert!(!torrent_state.piece_selector.is_allocated(6));
    });
}

#[test]
fn peer_choke_recv_does_not_support_fast() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(false, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].peer_choking = false;
        connections[key].slow_start = false;
        for i in 1..7 {
            let mut subpieces = torrent_state.allocate_piece(i, &file_store);
            connections[key].append_and_fill(&mut subpieces);
        }
        assert_eq!(torrent_state.num_allocated(), 6);
        assert_eq!(connections[key].target_inflight, 4);
        assert_eq!(connections[key].queued.len(), 8);
        assert_eq!(connections[key].inflight.len(), 4);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                data: vec![1; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 1,
                begin: SUBPIECE_SIZE,
                data: vec![2; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        tick(
            &Duration::from_millis(650),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        // Want an odd number here to test releasing in flight pieces
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 9);
        assert_eq!(connections[key].queued.len(), 1);
        assert!(torrent_state.piece_selector.is_allocated(6));

        connections[key].handle_message(
            PeerMessage::Choke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(connections[key].is_choking);
        assert_eq!(connections[key].target_inflight, 9);
        assert_eq!(connections[key].inflight.len(), 0);
        assert_eq!(connections[key].queued.len(), 0);
        // 1 piece completed (pending hashing), one was released
        assert_eq!(torrent_state.num_allocated(), 0);
        // index = 1 is not inflight over the network but pending hashing and thus is
        // still marked inflight
        for i in 2..7 {
            // No longer inflight!
            assert!(!torrent_state.piece_selector.is_allocated(i));
        }
    });
}

#[test]
fn unchoke_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(false, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        assert!(connections[key].peer_choking);
        connections[key].is_interesting = false;
        connections[key].handle_message(
            PeerMessage::Unchoke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(!connections[key].peer_choking);
        // No intrest so nothing is downloaded
        assert!(connections[key].queued.is_empty());
        assert!(connections[key].inflight.is_empty());
        assert_eq!(torrent_state.num_allocated(), 0);

        let a = generate_peer(true, 1);
        let mut connections = Slab::new();
        let key = connections.insert(a);
        connections[key].handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(connections[key].peer_choking);
        assert!(connections[key].is_interesting);
        connections[key].handle_message(
            PeerMessage::Unchoke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(!connections[key].peer_choking);
        // Peer is interesting so we start downloading
        assert!(!(connections[key].queued.is_empty() && connections[key].inflight.is_empty()));
        assert!(!torrent_state.pieces.is_empty());
    });
}

// TODO: num_unchoked after disconnecting (tick maybe should return num disconnected? for easy testing)

#[test]
fn bitfield_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        {
            let mut b = generate_peer(true, 0);
            assert!(b.pending_disconnect.is_none());
            let invalid_field =
                bitvec::bitvec!(u8, bitvec::order::Msb0; 1; torrent_state.num_pieces() - 1);
            b.handle_message(
                PeerMessage::Bitfield(invalid_field),
                &mut torrent_state,
                &file_store,
                &torrent_info,
                scope,
            );
            assert!(b.pending_disconnect.is_some());
        }

        let mut a = generate_peer(true, 0);
        assert!(!a.is_interesting);
        assert!(!torrent_state.piece_selector.bitfield_received(a.conn_id));
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; torrent_state.num_pieces());
        field.set(2, false);
        field.set(4, false);
        a.handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // We are interestead since we do not have the pieces
        sent_and_marked_interested(&a);
        assert!(torrent_state.piece_selector.bitfield_received(a.conn_id));

        for i in 0..torrent_state.num_pieces() {
            if i == 2 || i == 4 {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)
                        .unwrap()[i]
                );
            } else {
                assert!(
                    torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)
                        .unwrap()[i]
                );
                // MOCK that the pieces have been completed by a
                torrent_state.piece_selector.mark_allocated(i);
                torrent_state.piece_selector.mark_complete(i);
            }
        }

        let mut b = generate_peer(true, 1);
        assert!(!b.is_interesting);
        assert!(!torrent_state.piece_selector.bitfield_received(b.conn_id));
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
        field.set(2, false);
        field.set(4, false);
        b.handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Still not interestead since no new pieces can be downloaded received
        assert!(!b.is_interesting);
        assert!(torrent_state.piece_selector.bitfield_received(b.conn_id));

        let mut c = generate_peer(true, 2);
        assert!(!c.is_interesting);
        assert!(!torrent_state.piece_selector.bitfield_received(c.conn_id));
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
        field.set(2, false);
        c.handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // New piece can be downloaded
        sent_and_marked_interested(&c);
        assert!(torrent_state.piece_selector.bitfield_received(c.conn_id));
    });
}

// TODO test we do not resend the not_interested message
#[test]
fn interest_is_updated_when_recv_piece() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        assert!(!connections[key].is_interesting);
        assert!(
            !torrent_state
                .piece_selector
                .bitfield_received(connections[key].conn_id)
        );
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(2, true);
        field.set(4, true);
        connections[key].handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // We are interestead since we do not have the pieces
        sent_and_marked_interested(&connections[key]);
        assert!(
            torrent_state
                .piece_selector
                .bitfield_received(connections[key].conn_id)
        );

        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        let mut subpieces = torrent_state.allocate_piece(4, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 4);
        assert!(connections[key].queued.is_empty());

        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections);
        assert!(connections[key].is_interesting);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 4,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 4,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections);
        sent_and_marked_not_interested(&connections[key]);
    });
}

#[test]
fn send_have_to_peers_when_piece_completes() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let b = generate_peer(true, 0);
        let c = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key_a = connections.insert(a);
        let key_b = connections.insert(b);
        connections.insert(c);

        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(2, true);
        connections[key_a].handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let mut field = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
        field.set(4, true);
        connections[key_b].handle_message(
            PeerMessage::Bitfield(field),
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        connections[key_a].append_and_fill(&mut subpieces);
        let mut subpieces = torrent_state.allocate_piece(4, &file_store);
        connections[key_b].append_and_fill(&mut subpieces);

        connections[key_a].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key_a].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(150));
        torrent_state.update_torrent_status(&mut connections);
        for (_, peer) in &mut connections {
            assert!(
                peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: 2 },
                    ordered: false,
                }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: 2 },
                    ordered: true,
                })
            );
            // Clear for next check
            peer.outgoing_msgs_buffer.clear();
        }
        connections[key_b].handle_message(
            PeerMessage::Piece {
                index: 4,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key_b].handle_message(
            PeerMessage::Piece {
                index: 4,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections);
        for (_, peer) in &connections {
            assert!(
                peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: 4 },
                    ordered: false,
                }) || peer.outgoing_msgs_buffer.contains(&OutgoingMsg {
                    message: PeerMessage::Have { index: 4 },
                    ordered: true,
                })
            )
        }
    });
}

#[test]
fn assume_intrest_when_request_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(!a.peer_interested);
        a.handle_message(
            PeerMessage::Request {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Assume the peer is interestead we recv a request from them
        assert!(a.peer_interested);
    });
}

#[test]
fn piece_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );

        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        let prev_target_infligt = connections[key].target_inflight;
        assert_eq!(torrent_state.num_allocated(), 1);
        assert!(torrent_state.pieces[2].is_some());
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 2);
        assert!(connections[key].queued.is_empty());
        assert_eq!(torrent_state.piece_selector.total_inflight(), 1);
        assert!(torrent_state.piece_selector.is_allocated(2));
        assert_eq!(torrent_state.piece_selector.total_completed(), 0);

        let now = Instant::now();
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(
            torrent_state.pieces[2]
                .as_ref()
                .unwrap()
                .completed_subpieces[0]
        );
        assert_eq!(connections[key].inflight.len(), 1);
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 1);
        assert!(now - connections[key].last_seen < Duration::from_millis(1));
        assert!(now - connections[key].last_received_subpiece.unwrap() < Duration::from_millis(1));
        assert!(
            !torrent_state.pieces[2]
                .as_ref()
                .unwrap()
                .completed_subpieces
                .all()
        );

        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(connections[key].inflight.is_empty());
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 2);
        assert_eq!(torrent_state.num_allocated(), 0);
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections);
        assert_eq!(torrent_state.piece_selector.total_inflight(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(2));
        assert!(torrent_state.piece_selector.has_completed(2));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
    });
}

#[test]
fn handles_duplicate_piece_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let prev_target_infligt = connections[key].target_inflight;
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert_eq!(connections[key].target_inflight, prev_target_infligt + 1);
        assert_eq!(connections[key].inflight.len(), 1);
        std::thread::sleep(Duration::from_millis(100));
        // Same message again
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
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
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(connections[key].inflight.is_empty());
        assert_eq!(torrent_state.num_allocated(), 0);
        // To ensure we do not miss the completion event
        std::thread::sleep(Duration::from_millis(100));
        torrent_state.update_torrent_status(&mut connections);
        assert_eq!(torrent_state.piece_selector.total_inflight(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(2));
        assert!(torrent_state.piece_selector.has_completed(2));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert_eq!(torrent_state.piece_selector.total_inflight(), 0);
        assert!(!torrent_state.piece_selector.is_allocated(2));
        assert!(torrent_state.piece_selector.has_completed(2));
        assert_eq!(torrent_state.piece_selector.total_completed(), 1);
    });
}

#[test]
fn invalid_piece() {
    let (file_store, torrent_info) = setup_test();
    rayon::scope(|scope| {
        let mut torrent_state = TorrentState::new(&torrent_info);
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        a.append_and_fill(&mut subpieces);
        assert!(a.pending_disconnect.is_none());
        a.handle_message(
            PeerMessage::Piece {
                index: -2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.pending_disconnect.is_some());

        let mut torrent_state = TorrentState::new(&torrent_info);
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        a.append_and_fill(&mut subpieces);
        assert!(a.pending_disconnect.is_none());
        a.handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: SUBPIECE_SIZE + 1,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.pending_disconnect.is_some());

        let mut torrent_state = TorrentState::new(&torrent_info);
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        a.append_and_fill(&mut subpieces);
        assert!(a.pending_disconnect.is_none());
        a.handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; (SUBPIECE_SIZE + 1) as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.pending_disconnect.is_some());
    });
}

// TODO: ensure we request as many pieces as possible to actuall fill up all available queue spots
// when starting up connections
// #[test]
// fn request_piece() {}

// TODO timeout tests + Tests that ensure we handle redundant data
#[test]
fn snubbed_peer() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);
        connections[key].handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Hack to prevent this from requesting things
        connections[key].is_interesting = false;
        connections[key].handle_message(
            PeerMessage::Unchoke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        connections[key].is_interesting = true;
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        assert_eq!(connections[key].inflight.len(), 2);
        assert!(connections[key].queued.is_empty());
        assert_eq!(torrent_state.num_allocated(), 1);
        connections[key].handle_message(
            PeerMessage::Piece {
                index: 2,
                begin: 0,
                data: vec![3; SUBPIECE_SIZE as usize].into(),
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
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
            &file_store,
            &mut torrent_state,
        );
        assert!(!connections[key].slow_start);
        assert!(connections[key].snubbed);
        assert!(connections[key].inflight[0].timed_out);
        assert!(!torrent_state.piece_selector.is_allocated(2));
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
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );
        assert!(!connections[key].snubbed);
        assert!(connections[key].target_inflight > 1);
    });
}

#[test]
fn reject_request_requests_new() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Hack to prevent this from requesting things
        a.is_interesting = false;
        a.handle_message(
            PeerMessage::Unchoke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        a.is_interesting = true;
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        a.append_and_fill(&mut subpieces);
        assert_eq!(a.inflight.len(), 2);
        assert!(a.inflight.contains(&Subpiece {
            index: 2,
            offset: 0,
            size: SUBPIECE_SIZE,
            timed_out: false,
        }));
        assert_eq!(torrent_state.num_allocated(), 1);
        a.handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(!a.inflight.contains(&Subpiece {
            index: 2,
            offset: 0,
            size: SUBPIECE_SIZE,
            timed_out: false,
        }));
        // New piece started
        assert_eq!(torrent_state.num_allocated(), 1);
        assert!(!torrent_state.piece_selector.is_allocated(2));
        // Last piece only have one subpiece
        if torrent_state.pieces[8].is_some() {
            assert_eq!(a.inflight.len(), 2);
        } else {
            assert_eq!(a.inflight.len(), 3);
        }
    });
}

#[test]
fn invalid_reject_request() {
    let (file_store, torrent_info) = setup_test();
    rayon::scope(|scope| {
        let mut torrent_state = TorrentState::new(&torrent_info);
        let mut b = generate_peer(false, 0);
        b.handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(matches!(
            b.pending_disconnect,
            Some(DisconnectReason::ProtocolError(_))
        ));
        let mut a = generate_peer(true, 0);
        a.handle_message(
            PeerMessage::HaveAll,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        // Hack to prevent this from requesting things
        a.is_interesting = false;
        a.handle_message(
            PeerMessage::Unchoke,
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        a.is_interesting = true;
        let mut subpieces = torrent_state.allocate_piece(2, &file_store);
        a.append_and_fill(&mut subpieces);
        assert!(a.pending_disconnect.is_none());
        a.handle_message(
            PeerMessage::RejectRequest {
                index: -2,
                begin: 0,
                length: SUBPIECE_SIZE,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        a.handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: SUBPIECE_SIZE + 1,
                length: SUBPIECE_SIZE,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert!(a.pending_disconnect.is_none());
        // Both are invalid and ignored
        assert_eq!(a.inflight.len(), 2);
        a.handle_message(
            PeerMessage::RejectRequest {
                index: 2,
                begin: 0,
                length: SUBPIECE_SIZE + 1,
            },
            &mut torrent_state,
            &file_store,
            &torrent_info,
            scope,
        );
        assert_eq!(a.inflight.len(), 2);
        assert!(a.pending_disconnect.is_none());
    });
}
