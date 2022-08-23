use crate::utp_packet::Packet;

pub struct ReorderBuffer {
    buffer: Vec<Option<Packet>>,
    first: usize,
    last: usize,
}

impl ReorderBuffer {
    // size in PACKETS
    pub fn new(size: usize) -> Self {
        ReorderBuffer {
            buffer: vec![Option::<Packet>::None; size],
            first: 0,
            last: 0,
        }
    }

    pub fn insert(&mut self, position: i32, packet: Packet) {
        if self.get(position).is_some() {
            return;
        }

        let (first_val, last_val) = if let (Some(first_val), Some(last_val)) = (
            self.buffer[self.first]
                .as_ref()
                .map(|p| p.header.seq_nr as i32),
            self.buffer[self.last]
                .as_ref()
                .map(|p| p.header.seq_nr as i32),
        ) {
            (first_val, last_val)
        } else {
            self.buffer[self.first] = Some(packet);
            debug_assert!(self.last == self.first);
            return;
        };

        match position.cmp(&first_val) {
            std::cmp::Ordering::Less => {
                // If the available space is less than the distance to
                // the first value we need to realloc
                if self.buffer.len() as i32 - (last_val - first_val) <= first_val - position {
                    // Ensure current span + new value fits
                    // by calculating dist between position (< first_val)
                    // and last_val
                    self.resize(1 + (last_val - position) as usize);
                    // This is conceptually the same as what's done in the 
                    // else branch but avoids the rem_euclid operation since 
                    // we know that first always = 0 after resizing
                    let new_first = self.buffer.len() - (first_val - position) as usize;
                    self.buffer[new_first] = Some(packet);
                    self.first = new_first;
                } else {
                    // there is capacity for it
                    let new_first = (self.first as i32 - (first_val - position))
                        .rem_euclid(self.buffer.len() as i32);
                    self.buffer[new_first as usize] = Some(packet);
                    self.first = new_first as usize;
                }
            }
            std::cmp::Ordering::Greater => {
                // there is capacity for it
                if first_val as usize + self.buffer.len() > position as usize {
                    let index = (self.first + (position - first_val) as usize) % self.buffer.len();
                    if last_val < packet.header.seq_nr as i32 {
                        self.last = index;
                    }
                    self.buffer[index] = Some(packet);
                } else {
                    self.resize(1 + (position - first_val) as usize);
                    let index = self.first + (position - first_val) as usize;
                    if last_val < packet.header.seq_nr as i32 {
                        self.last = index;
                    }
                    self.buffer[index] = Some(packet);
                }
            }
            std::cmp::Ordering::Equal => unreachable!(),
        }
    }

    fn resize(&mut self, min_size: usize) {
        let new_size = std::cmp::max(min_size, self.buffer.len() * 2);
        let mut buf_new = vec![Option::<Packet>::None; new_size];
        let first_part = &self.buffer[self.first..];
        let second_part = &self.buffer[..self.first];
        // Can't use ptr copy since Bytes isn't copy
        buf_new[0..first_part.len()].clone_from_slice(first_part);
        buf_new[first_part.len()..first_part.len() + second_part.len()]
            .clone_from_slice(second_part);
        let old_cap = self.buffer.len();
        self.buffer = buf_new;
        match self.last.cmp(&self.first) {
            // Move it to an unrwapped position
            std::cmp::Ordering::Less => self.last += old_cap - self.first,
            // Move it to the new first position
            std::cmp::Ordering::Equal => self.last = 0,
            // Move first position steps down since first is moved to 0
            std::cmp::Ordering::Greater => self.last -= self.first,
        }
        self.first = 0;
    }

    pub fn get(&self, position: i32) -> Option<&Packet> {
        let first_val = self.buffer[self.first].as_ref()?.header.seq_nr as i32;
        let index =
            (self.first as i32 + (position - first_val)).rem_euclid(self.buffer.len() as i32);
        self.buffer[index as usize]
            .as_ref()
            .filter(|packet| packet.header.seq_nr == position as u16)
    }

    pub fn remove(&mut self, index: usize) -> Option<Packet> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {

    use bytes::Bytes;

    use crate::utp_packet::PacketHeader;

    use super::*;

    #[test]
    fn insertion_orderd() {
        let data = vec![
            Packet {
                header: PacketHeader {
                    seq_nr: 1,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 2,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 3,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        ];

        let mut buffer = ReorderBuffer::new(256);

        for packet in data.into_iter() {
            buffer.insert(packet.header.seq_nr as i32, packet);
        }

        for seq_nr in 1..3 {
            let packet = buffer.get(seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr as i32, seq_nr);
        }
    }

    #[test]
    fn insertion_unorderd() {
        let data = vec![
            Packet {
                header: PacketHeader {
                    seq_nr: 3,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 1,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 4,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        ];

        let mut buffer = ReorderBuffer::new(256);

        for packet in data.into_iter() {
            buffer.insert(packet.header.seq_nr as i32, packet);
        }

        let packet = buffer.get(1).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 1);
        let packet = buffer.get(3).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 3);
        let packet = buffer.get(4).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 4);
    }

    #[test]
    fn insertion_unorderd_large_gap() {
        let data = vec![
            Packet {
                header: PacketHeader {
                    seq_nr: 253,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 747,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 108,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        ];

        let mut buffer = ReorderBuffer::new(256);

        for packet in data.into_iter() {
            buffer.insert(packet.header.seq_nr as i32, packet);
        }

        let packet = buffer.get(108).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 108);
        let packet = buffer.get(253).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 253);
        let packet = buffer.get(747).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 747);
    }

    #[test]
    fn insertion_orderd_large_gap() {
        let data = vec![
            Packet {
                header: PacketHeader {
                    seq_nr: 245,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
            Packet {
                header: PacketHeader {
                    seq_nr: 922,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        ];

        let mut buffer = ReorderBuffer::new(256);

        for packet in data.into_iter() {
            buffer.insert(packet.header.seq_nr as i32, packet);
        }

        let packet = buffer.get(245).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 245);
        let packet = buffer.get(922).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 922);
    }

    #[test]
    fn index_collision() {
        // Tests the case where the seq_nr
        // mod capacity yields an existing entry
        // which doesn't match the one being inserted
        // caught by fuzzing
        let mut buffer = ReorderBuffer::new(64);
        buffer.insert(
            2570,
            Packet {
                header: PacketHeader {
                    seq_nr: 2570,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        );
        buffer.insert(
            2698,
            Packet {
                header: PacketHeader {
                    seq_nr: 2698,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            },
        );
        let packet = buffer.get(2570).unwrap();
        assert_eq!(packet.header.seq_nr, 2570);
        let packet = buffer.get(2698).unwrap();
        assert_eq!(packet.header.seq_nr, 2698);
    }

    #[test]
    fn resizing() {
        // Ensures the existing span + the new value 
        // fits in the resized buffer. Caught by fuzzing
        let input = [25413, 25392, 16744, 2607];
        let mut buffer = ReorderBuffer::new(64);

        for seq_nr in input.iter() {
            buffer.insert(
                *seq_nr,
                Packet {
                    header: PacketHeader {
                        seq_nr: *seq_nr as u16,
                        ack_nr: 0,
                        conn_id: 0,
                        packet_type: crate::utp_packet::PacketType::Data,
                        timestamp_microseconds: 0,
                        timestamp_difference_microseconds: 0,
                        wnd_size: 0,
                        extension: 0,
                    },
                    data: Bytes::new(),
                },
            );
        }

        for seq_nr in input.iter() {
            let packet = buffer.get(*seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, *seq_nr as u16);
        }
    }
}
