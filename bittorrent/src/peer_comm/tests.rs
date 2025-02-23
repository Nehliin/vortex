use std::time::Duration;

use slab::Slab;

use crate::{
    event_loop::tick,
    peer_connection::OutgoingMsg,
    piece_selector::SUBPIECE_SIZE,
    test_utils::{generate_peer, setup_test},
};

use super::peer_protocol::PeerMessage;

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
        assert!(a.is_interested);
        assert!(a.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::Interested,
            ordered: false
        }));
        for piece_id in 0..torrent_info.pieces.len() {
            if piece_id == 8 {
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
    torrent_state.piece_selector.mark_inflight(8);
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
        assert!(!a.is_interested);
        for piece_id in 0..torrent_info.pieces.len() {
            if piece_id == 8 {
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
fn slow_start() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
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
        assert!(connections[key].desired_queue_size > 1);
        let old_desired_queue = connections[key].desired_queue_size;

        connections[key].peer_choking = false;
        connections[key].request_piece(1, &mut torrent_state.piece_selector, &file_store);
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
        assert_eq!(connections[key].desired_queue_size, old_desired_queue + 2);

        tick(
            &Duration::from_millis(1500),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        assert_eq!(connections[key].prev_throughput, 21845);
        assert_eq!(connections[key].throughput, 0);
        assert!(connections[key].slow_start);
        assert_eq!(connections[key].desired_queue_size, old_desired_queue + 2);

        connections[key].request_piece(2, &mut torrent_state.piece_selector, &file_store);
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
        assert_eq!(connections[key].desired_queue_size, old_desired_queue + 4);

        connections[key].request_piece(3, &mut torrent_state.piece_selector, &file_store);
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
        assert_eq!(connections[key].desired_queue_size, old_desired_queue + 6);
    });
}

#[test]
fn desired_queue_size() {
    let (file_store, torrent_info, mut torrent_state) = setup_test();
    rayon::scope(|scope| {
        let a = generate_peer(true, 0);
        let mut connections = Slab::new();
        let key = connections.insert(a);

        connections[key].peer_choking = false;
        connections[key].slow_start = false;
        connections[key].request_piece(1, &mut torrent_state.piece_selector, &file_store);
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
        assert_eq!(connections[key].desired_queue_size, 6);

        tick(
            &Duration::from_secs(1),
            &mut connections,
            &file_store,
            &mut torrent_state,
        );

        // Never go below 1
        assert_eq!(connections[key].desired_queue_size, 1);
        // TODO: Test max
    });
}

// TODO: ensure we request as many pieces as possible to actuall fill up all available queue spots
// when starting up connections
#[test]
fn request_piece() {}

#[test]
fn request_timeout() {}

// test that rejecting a request doesn't cause an timeout later on
#[test]
fn reject_request() {}
