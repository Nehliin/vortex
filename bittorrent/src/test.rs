use std::{future::Future, time::Duration};

use tokio_uring::net::TcpListener;

use crate::peer_message::PeerMessage;

use super::*;
#[test]
fn it_works() {
    let torrent = std::fs::read("test_torrent.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
    println!("infohash: {:?}", metainfo.info().info_hash());
    println!("pices: {}", metainfo.info().pieces().count());
    println!("files: {}", metainfo.info().files().count());
}

fn setup_test() -> (
    TorrentManager,
    PeerConnection,
    impl Future<Output = anyhow::Result<PeerConnectiontmp>>,
    Receiver<Vec<u8>>,
) {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();

    let torrent = std::fs::read("../test.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
    let info_hash: [u8; 20] = metainfo.info().info_hash().try_into().unwrap();

    let port: u16 = (rand::random::<f32>() * (u16::MAX - 2000) as f32) as u16 + 2000;
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let info_hash_clone = info_hash;
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    std::thread::spawn(move || {
        let listener = TcpListener::bind(addr).unwrap();
        tokio_uring::start(async move {
            let (connection, _conn_addr) = listener.accept().await.unwrap();
            let buf = vec![0u8; 68];
            let (bytes_read, buf) = connection.read(buf).await;
            let bytes_read = bytes_read.unwrap();
            assert_eq!(bytes_read, 68);
            assert_eq!(
                PeerConnectiontmp::handshake(info_hash_clone, [0; 20]),
                buf[..bytes_read]
            );

            let buf = PeerConnectiontmp::handshake(info_hash, [1; 20]).to_vec();
            let (res, _buf) = connection.write(buf).await;
            res.unwrap();
            log::info!("Connected!");
            let mut buf = vec![0; 1024];
            loop {
                let (res, used_buf) = connection.read(buf).await;
                let bytes_read = res.unwrap();
                if bytes_read == 0 || (tx.send(used_buf[..bytes_read].to_vec()).await).is_err() {
                    break;
                }
                buf = used_buf;
            }
        });
    });

    let torrent_manager = TorrentManager::new(metainfo.info().clone());
    let (sender, receiver) = tokio::sync::mpsc::channel(256);
    let peer_handle = PeerConnection {
        peer_id: [1; 20],
        sender,
    };
    let state = torrent_manager.torrent_state.clone();
    let peer_connection = PeerConnectiontmp::new(addr, [0; 20], [1; 20], info_hash, state, receiver);
    (torrent_manager, peer_handle, peer_connection, rx)
}

#[test]
fn test_incoming_choke() {
    let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_choking);
        connection
            .process_incoming(PeerMessage::Choke)
            .await
            .unwrap();
        assert!(connection.state().peer_choking)
    });
}

// TODO: Test actions within the torrent manager here
#[test]
fn test_incoming_unchoke_when_not_interested() {
    let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_choking);
        assert!(!connection.state().is_interested);
        connection
            .process_incoming(PeerMessage::Choke)
            .await
            .unwrap();
        assert!(connection.state().peer_choking);
        connection
            .process_incoming(PeerMessage::Unchoke)
            .await
            .unwrap();
        assert!(!connection.state().peer_choking);
    });
}

#[test]
fn test_incoming_interestead_when_not_choking() {
    let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_choking);
        assert!(!connection.state().is_interested);
        connection.state_mut().is_choking = false;
        torrent_manager.torrent_state.lock().num_unchoked += 1;
        connection
            .process_incoming(PeerMessage::Interested)
            .await
            .unwrap();
        let data = sent_data.recv().await.unwrap();
        let msg = PeerMessage::try_from(data.as_slice()).unwrap();
        assert_eq!(msg, PeerMessage::Unchoke);
        connection
            .stream
            .shutdown(std::net::Shutdown::Both)
            .unwrap();
    });
}

#[test]
fn test_incoming_interestead_when_choking_with_free_unchoke_spots() {
    let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_choking);
        assert!(!connection.state().is_interested);
        connection
            .process_incoming(PeerMessage::Interested)
            .await
            .unwrap();
        // There is one free unchoke spot left
        let num_unchoked = torrent_manager.torrent_state.lock().num_unchoked;
        let max_unchoked = torrent_manager.torrent_state.lock().max_unchoked;
        assert!(num_unchoked < max_unchoked);
        let data = sent_data.recv().await.unwrap();
        let msg = PeerMessage::try_from(data.as_slice()).unwrap();
        assert_eq!(msg, PeerMessage::Unchoke);
        // Needed to prevent dead lock
        connection
            .stream
            .shutdown(std::net::Shutdown::Both)
            .unwrap();
    });
}

#[test]
fn test_incoming_interestead_when_choking_without_free_unchoke_spots() {
    let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_choking);
        assert!(!connection.state().is_interested);
        torrent_manager.torrent_state.lock().num_unchoked += 1;
        connection
            .process_incoming(PeerMessage::Interested)
            .await
            .unwrap();
        // There are no free unchoke spot left
        let num_unchoked = torrent_manager.torrent_state.lock().num_unchoked;
        let max_unchoked = torrent_manager.torrent_state.lock().max_unchoked;
        assert!(num_unchoked >= max_unchoked);
        let timeout = tokio::time::timeout(Duration::from_secs(3), sent_data.recv()).await;
        match timeout {
            Err(_) => {}
            Ok(_) => panic!("should timeout"),
        }
        connection
            .stream
            .shutdown(std::net::Shutdown::Both)
            .unwrap();
    });
}

// TODO: Test actions within the torrent manager here
#[test]
fn test_incoming_bitfield() {
    let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        let mut bitfield_one: BitBox<u8, Msb0> = connection.state().peer_pieces.clone();
        let mut bitfield_two: BitBox<u8, Msb0> = connection.state().peer_pieces.clone();
        assert_eq!(bitfield_one.leading_zeros(), bitfield_one.len());
        bitfield_one.set(7, true);
        bitfield_one.set(100, true);
        connection
            .process_incoming(PeerMessage::Bitfield(bitfield_one.clone().as_ref()))
            .await
            .unwrap();
        assert_eq!(connection.state().peer_pieces, bitfield_one);
        bitfield_two.set(3, true);
        connection
            .process_incoming(PeerMessage::Bitfield(bitfield_two.as_ref()))
            .await
            .unwrap();
        assert_eq!(connection.state().peer_pieces, bitfield_one | bitfield_two);
    });
}

#[test]
fn test_incoming_have() {
    let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

    tokio_uring::start(async move {
        let connection = peer_connection_fut.await.unwrap();
        assert!(!connection.state().peer_pieces.get(5).unwrap());
        connection
            .process_incoming(PeerMessage::Have { index: 5 })
            .await
            .unwrap();
        assert!(connection.state().peer_pieces.get(5).unwrap());
        connection
            .process_incoming(PeerMessage::Have { index: 7 })
            .await
            .unwrap();
        assert!(connection.state().peer_pieces.get(5).unwrap());
        assert!(connection.state().peer_pieces.get(7).unwrap());
    });
}
