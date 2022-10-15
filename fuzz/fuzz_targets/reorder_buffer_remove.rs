#![no_main]
use libfuzzer_sys::fuzz_target;

use utp_socket::{
    reorder_buffer::ReorderBuffer,
    utp_packet::{self, Packet, PacketHeader},
};

fuzz_target!(|data: Vec<(u16, u16)>| {
    let mut buffer = ReorderBuffer::new(64);

    let mut unique = std::collections::HashSet::new();
    let mut expected_size = 0;
    for (seq_nr, len) in data.iter() {
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
                data: vec![2; *len as usize].into(),
            },
        );
        if unique.insert(*seq_nr) {
            expected_size += *len as usize;
        }
        assert_eq!(buffer.size(), expected_size);
    }

    for seq_nr in unique.iter() {
        let packet = buffer.remove(*seq_nr).unwrap();
        expected_size -= packet.data.len();
        assert_eq!(packet.header.seq_nr, *seq_nr);
        assert_eq!(buffer.size(), expected_size);
    }

    for (seq_nr, _) in data.iter() {
        assert!(buffer.get(*seq_nr).is_none());
    }

    assert!(buffer.is_empty());
});
