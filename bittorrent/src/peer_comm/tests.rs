use std::time::Duration;

use slab::Slab;

use crate::{
    TorrentState,
    event_loop::tick,
    peer_connection::OutgoingMsg,
    piece_selector::SUBPIECE_SIZE,
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
                    .interesting_peer_pieces(a.conn_id)[piece_id]
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
                        .interesting_peer_pieces(a.conn_id)[piece_id]
                );
            } else {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)[piece_id]
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
        let mut subpieces = torrent_state.request_new_piece(8, &file_store);
        a.append_and_fill(&mut subpieces);

        let mut b = generate_peer(true, 1);
        assert!(!b.is_interesting);
        assert!(torrent_state.piece_selector.is_inflight(8));
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
                .interesting_peer_pieces(a.conn_id)[8]
        );
    });
}

#[test]
fn have_without_interest() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
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
        assert!(!a.is_interesting);
        assert!(
            !torrent_state
                .piece_selector
                .interesting_peer_pieces(a.conn_id)
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
        let mut subpieces = torrent_state.request_new_piece(1, &file_store);
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

        let mut subpieces = torrent_state.request_new_piece(2, &file_store);
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

        let mut subpieces = torrent_state.request_new_piece(3, &file_store);
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
        let mut subpieces = torrent_state.request_new_piece(1, &file_store);
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
            let mut subpieces = torrent_state.request_new_piece(i, &file_store);
            connections[key].append_and_fill(&mut subpieces);
        }
        assert_eq!(torrent_state.currently_downloading.len(), 6);
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
        assert!(torrent_state.piece_selector.is_inflight(6));

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
        assert_eq!(torrent_state.currently_downloading.len(), 4);
        // No longer inflight!
        assert!(!torrent_state.piece_selector.is_inflight(6));
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
            let mut subpieces = torrent_state.request_new_piece(i, &file_store);
            connections[key].append_and_fill(&mut subpieces);
        }
        assert_eq!(torrent_state.currently_downloading.len(), 6);
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
        assert!(torrent_state.piece_selector.is_inflight(6));

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
        assert_eq!(torrent_state.currently_downloading.len(), 0);
        // index = 1 is not inflight over the network but pending hashing and thus is
        // still marked inflight
        for i in 2..7 {
            // No longer inflight!
            assert!(!torrent_state.piece_selector.is_inflight(i));
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
        assert!(torrent_state.currently_downloading.is_empty());

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
        assert!(!torrent_state.currently_downloading.is_empty());
    });
}

// TODO: num_unchoked after disconnecting (tick maybe should return num disconnected? for easy testing)

#[test]
fn bitfield_recv() {
    let (file_store, torrent_info) = setup_test();
    let mut torrent_state = TorrentState::new(&torrent_info);
    rayon::scope(|scope| {
        let mut a = generate_peer(true, 0);
        assert!(!a.is_interesting);
        assert!(!torrent_state.piece_selector.bitfield_received(a.conn_id));
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(usize, bitvec::order::Msb0; 1; num_pieces);
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

        for i in 0..num_pieces {
            if i == 2 || i == 4 {
                assert!(
                    !torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)[i]
                );
            } else {
                assert!(
                    torrent_state
                        .piece_selector
                        .interesting_peer_pieces(a.conn_id)[i]
                );
                // MOCK that the pieces have been requested/completed by a
                if i % 2 == 0 {
                    torrent_state.piece_selector.mark_inflight(i);
                    torrent_state.piece_selector.mark_complete(i);
                } else {
                    torrent_state.piece_selector.mark_inflight(i);
                }
            }
        }

        let mut b = generate_peer(true, 1);
        assert!(!b.is_interesting);
        assert!(!torrent_state.piece_selector.bitfield_received(b.conn_id));
        let num_pieces = torrent_state.num_pieces();
        let mut field = bitvec::bitvec!(usize, bitvec::order::Msb0; 1; num_pieces);
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
        let mut field = bitvec::bitvec!(usize, bitvec::order::Msb0; 1; num_pieces);
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
        let mut field = bitvec::bitvec!(usize, bitvec::order::Msb0; 0; num_pieces);
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

        let mut subpieces = torrent_state.request_new_piece(2, &file_store);
        connections[key].append_and_fill(&mut subpieces);
        let mut subpieces = torrent_state.request_new_piece(4, &file_store);
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

// TODO: ensure we request as many pieces as possible to actuall fill up all available queue spots
// when starting up connections
// #[test]
// fn request_piece() {}

// #[test]
// fn request_timeout() {}

// // test that rejecting a request doesn't cause an timeout later on
// #[test]
// fn reject_request() {}
