use std::time::Duration;

use bytes::Bytes;
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
        assert!(a.outgoing_msgs_buffer.contains(&OutgoingMsg {
            message: PeerMessage::Interested,
            ordered: false
        }));
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
