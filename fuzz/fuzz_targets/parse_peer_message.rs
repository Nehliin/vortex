#![no_main]

use libfuzzer_sys::fuzz_target;
use vortex_bittorrent::PeerMessageDecoder;

fuzz_target!(|data: Vec<Vec<u8>>| {
    let mut decoder = PeerMessageDecoder::new(1 << 12);
    for partial in data {
        decoder.append_data(partial.as_slice());
        while decoder.next().is_some() {}
    }
});
