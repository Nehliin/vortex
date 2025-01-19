#![no_main]

use bytes::Buf;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use vortex_bittorrent::PeerMessage;
use vortex_bittorrent::PeerMessageDecoder;

fuzz_target!(|messages: (Vec<PeerMessage>, Vec<usize>)| {
    let mut encoded = BytesMut::new();
    for msg in messages.0.iter() {
        let mut msg_buf = vec![0; msg.encoded_size()];
        msg.encode(&mut msg_buf);
        encoded.extend_from_slice(&msg_buf);
    }

    let mut decoder = PeerMessageDecoder::new(1 << 12);
    let mut parsed = Vec::new();
    for split in messages.1.iter() {
        if *split <= encoded.remaining() {
            let partial = encoded.split_to(*split);
            decoder.append_data(&partial);
            while let Some(Ok(decoded)) = decoder.next() {
                parsed.push(decoded);
            }
        }
    }
    decoder.append_data(&encoded);
    while let Some(Ok(decoded)) = decoder.next() {
        parsed.push(decoded);
    }

    assert_eq!(messages.0, parsed);
    assert_eq!(decoder.remaining(), 0);
});
