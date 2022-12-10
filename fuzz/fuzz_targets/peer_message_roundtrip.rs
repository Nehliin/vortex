#![no_main]

use bittorrent::peer_message::PeerMessage;
use bittorrent::peer_message::PeerMessageDecoder;
use bytes::Buf;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|messages: (Vec<PeerMessage>, Vec<usize>)| {
    let mut encoded = BytesMut::new();
    for msg in messages.0.iter() {
        encoded.reserve(msg.encoded_size());
        msg.clone().encode(&mut encoded);
    }

    let mut decoder = PeerMessageDecoder::default();
    let mut parsed = Vec::new();
    for split in messages.1.iter() {
        if *split <= encoded.remaining() {
            let mut partial = encoded.split_to(*split);
            while let Some(decoded) = decoder.decode(&mut partial) {
                parsed.push(decoded);
            }
        } else {
            while let Some(decoded) = decoder.decode(&mut encoded) {
                parsed.push(decoded);
            }
        }
    }
    while let Some(msg) = decoder.decode(&mut encoded) {
        parsed.push(msg);
    }

    assert_eq!(messages.0, parsed);
    assert!(!encoded.has_remaining());
});
