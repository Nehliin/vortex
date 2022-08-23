#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use utp_socket::{
    reorder_buffer::ReorderBuffer,
    utp_packet::{self, Packet, PacketHeader},
};

fuzz_target!(|data: Vec<u16>| {
    let mut buffer = ReorderBuffer::new(64);

    for seq_nr in data.iter() {
        buffer.insert(
            Packet {
                header: PacketHeader {
                    seq_nr: *seq_nr,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        )
    }

    for seq_nr in data.iter() {
        let packet = buffer.get(*seq_nr).unwrap();
        assert_eq!(packet.header.seq_nr, *seq_nr);
    }
});
