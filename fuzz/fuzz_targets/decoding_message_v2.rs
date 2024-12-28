#![no_main]

use bittorrent_io::peer_message::PeerMessage;
use bittorrent_io::peer_message::PeerMessageDecoder;
use bytes::Buf;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|messages: (Vec<PeerMessage>, Vec<usize>)| {
    let mut encoded = BytesMut::new();
    for msg in messages.0.iter() {
        encoded.reserve(msg.encoded_size());
        msg.clone().encode(&mut encoded);
    }

    let mut decoder = PeerMessageDecoder::new(1 << 12);
    let mut parsed = Vec::new();
    for split in messages.1.iter() {
        if *split <= encoded.remaining() {
            let mut partial = encoded.split_to(*split);
            decoder.append_data(&mut partial);
            while let Some(Ok(decoded)) = decoder.next() {
                parsed.push(decoded);
            }
        } else {
            //decoder.append_data(&mut encoded);
            //while let Some(Ok(decoded)) = decoder.next() {
                //parsed.push(decoded);
            //}
        }
    }
    decoder.append_data(&mut encoded);
    while let Some(Ok(decoded)) = decoder.next() {
        parsed.push(decoded);
    }

    assert_eq!(messages.0, parsed);
});
