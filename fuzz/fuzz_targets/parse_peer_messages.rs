#![no_main]
use std::{collections::HashSet, convert::TryFrom};

use bittorrent::{network_io::PeerMessageDecoder, peer_message::PeerMessage};
use bytes::{Buf, Bytes};
use libfuzzer_sys::fuzz_target;
use tokio_uring::net::{TcpListener, TcpStream};
use tokio_util::{codec::Decoder, sync::CancellationToken};

// A list of bytes that we pretend is read from the socket
/*fuzz_target!(|data: Vec<PeerMessage>| {
    let (tx, rc) = tokio::sync::oneshot::channel();
    /*let mut decoder = PeerMessageDecoder;
    let mut packets: Vec<PeerMessage> = data
        .iter()
        .filter_map(|msg| decoder.decode(&mut msg.as_slice().into()).ok().flatten())
        .collect();*/
    let mut set: HashSet<PeerMessage> = data.clone().into_iter().collect();
    std::thread::spawn(move || {
        tokio_uring::start(async move {
            rc.await.unwrap();
            let sender = TcpStream::connect("0.0.0.0:1337".parse().unwrap())
                .await
                .unwrap();
            for bytes in data {
                let (res, _) = sender.write_all(bytes.into_bytes()).await;
                res.unwrap();
            }
        });
    });
    let receiver = TcpListener::bind("0.0.0.0:1337".parse().unwrap()).unwrap();
    tokio_uring::start(async move {
        tx.send(()).unwrap();
        let (stream, _) = receiver.accept().await.unwrap();
        let sendable_stream = bittorrent::network_io::SendableStream(stream);
        let cancellation_token = CancellationToken::new();
        let (_, mut rc) = bittorrent::network_io::start_network_thread(
            [1; 20],
            sendable_stream,
            cancellation_token,
        );
        while let Some(msg) = rc.recv().await {
            assert!(set.remove(&msg));
        }
        assert!(set.is_empty());
    });
});*/

fuzz_target!(|data: Vec<u8>| {
    let bytes: Bytes = data.into();
    dbg!(&bytes);
    if let Ok(packet) = PeerMessage::try_from(bytes) {
        let mut to_bytes = packet.clone().into_bytes();
        dbg!(&to_bytes);
        let len = to_bytes.get_i32();
        dbg!(len);
        assert_eq!(len as usize, to_bytes.remaining());
        assert_eq!(PeerMessage::try_from(to_bytes).unwrap(), packet);
    }
});
