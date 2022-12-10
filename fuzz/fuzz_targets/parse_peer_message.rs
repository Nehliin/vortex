#![no_main]

use bittorrent::peer_message::PeerMessageDecoder;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: Vec<Vec<u8>>| {
    let mut decoder = PeerMessageDecoder::default();
    for partial in data {
        decoder.decode(&mut partial.as_slice());
    }
});
