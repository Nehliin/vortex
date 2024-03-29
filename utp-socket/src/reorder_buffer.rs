use crate::utp_packet::Packet;

#[derive(Debug)]
pub struct ReorderBuffer {
    buffer: Box<[Option<Packet>]>,
    first: usize,
    last: usize,
    size: usize,
}

impl ReorderBuffer {
    // size in PACKETS
    pub fn new(size: usize) -> Self {
        ReorderBuffer {
            // TODO vec not needed
            buffer: vec![Option::<Packet>::None; size].into_boxed_slice(),
            first: 0,
            last: 0,
            size: 0,
        }
    }

    pub fn insert(&mut self, packet: Packet) {
        if self.get(packet.header.seq_nr).is_some() {
            return;
        }

        let position = packet.header.seq_nr as i32;

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
            self.size += packet.data.len();
            self.buffer[self.first] = Some(packet);
            debug_assert!(self.last == self.first);
            return;
        };

        self.size += packet.data.len();
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
        let mut buf_new = vec![Option::<Packet>::None; new_size].into_boxed_slice();
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

    #[inline]
    fn index_of(&self, position: i32) -> Option<usize> {
        let first_val = self.buffer[self.first].as_ref()?.header.seq_nr as i32;
        Some(
            (self.first as i32 + (position - first_val)).rem_euclid(self.buffer.len() as i32)
                as usize,
        )
    }

    #[inline]
    pub fn get(&self, position: u16) -> Option<&Packet> {
        let index = self.index_of(position as i32)?;
        self.buffer[index]
            .as_ref()
            .filter(|packet| packet.header.seq_nr == position)
    }

    // TODO make sequential removal more efficient
    pub fn remove(&mut self, position: u16) -> Option<Packet> {
        let index = self.index_of(position as i32)?;

        let maybe_packet = self.buffer[index].take();
        if let Some(packet) = maybe_packet.as_ref() {
            if packet.header.seq_nr == position {
                if self.first == index {
                    // Only one element in the buffer
                    if self.buffer[self.last].is_none() {
                        self.first = 0;
                        self.last = 0;
                    } else {
                        // find new first index
                        self.first += 1;
                        self.first %= self.buffer.len();
                        while self.first != self.last && self.buffer[self.first].is_none() {
                            self.first += 1;
                            self.first %= self.buffer.len();
                        }
                    }
                } else if self.last == index {
                    // Only one element in the buffer
                    if self.buffer[self.first].is_none() {
                        self.first = 0;
                        self.last = 0;
                    } else {
                        // find new last index
                        self.last =
                            (self.last as i32 - 1).rem_euclid(self.buffer.len() as i32) as usize;
                        while self.first != self.last && self.buffer[self.last].is_none() {
                            self.last = (self.last as i32 - 1).rem_euclid(self.buffer.len() as i32)
                                as usize;
                        }
                    }
                }
                if let Some(packet) = maybe_packet {
                    self.size -= packet.data.len();
                    return Some(packet);
                } else {
                    return None;
                }
            } else {
                self.buffer[index] = maybe_packet;
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        let empty = self.buffer[self.first].is_none();
        // santiy check
        if empty {
            debug_assert!(self.buffer[self.last].is_none());
        }
        empty
    }

    // Lenght in the number of packets
    #[inline]
    pub fn len(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            self.iter().count()
        }
    }

    // Size in bytes
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.size
    }

    // TODO remove allocations from this
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Packet> + 'a> {
        if self.first <= self.last {
            Box::new(
                self.buffer[self.first..self.last + 1]
                    .iter()
                    .filter_map(|maybe_packet| maybe_packet.as_ref()),
            )
        } else {
            Box::new(
                self.buffer[self.first..]
                    .iter()
                    .chain(self.buffer[..self.last + 1].iter())
                    .filter_map(|maybe_packet| maybe_packet.as_ref()),
            )
        }
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
            buffer.insert(packet);
        }

        for seq_nr in 1..3 {
            let packet = buffer.get(seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, seq_nr);
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
            buffer.insert(packet);
        }

        let packet = buffer.get(1).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 1);
        let packet = buffer.get(3).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 3);
        let packet = buffer.get(4).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 4);
        assert_eq!(buffer.len(), 3);
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
            buffer.insert(packet);
        }

        let packet = buffer.get(108).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 108);
        let packet = buffer.get(253).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 253);
        let packet = buffer.get(747).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 747);
        assert_eq!(buffer.len(), 3);
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
            buffer.insert(packet);
        }

        let packet = buffer.get(245).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 245);
        let packet = buffer.get(922).unwrap();
        assert_eq!(packet.header.seq_nr as usize, 922);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn index_collision() {
        // Tests the case where the seq_nr
        // mod capacity yields an existing entry
        // which doesn't match the one being inserted
        // caught by fuzzing
        let mut buffer = ReorderBuffer::new(64);
        buffer.insert(Packet {
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
        });
        buffer.insert(Packet {
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
        });
        let packet = buffer.get(2570).unwrap();
        assert_eq!(packet.header.seq_nr, 2570);
        let packet = buffer.get(2698).unwrap();
        assert_eq!(packet.header.seq_nr, 2698);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn resizing() {
        // Ensures the existing span + the new value
        // fits in the resized buffer. Caught by fuzzing
        let input = [25413, 25392, 16744, 2607];
        let mut buffer = ReorderBuffer::new(64);

        for seq_nr in input.iter() {
            buffer.insert(Packet {
                header: PacketHeader {
                    seq_nr: *seq_nr,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            });
        }

        for seq_nr in input.iter() {
            let packet = buffer.get(*seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, *seq_nr);
        }
        assert_eq!(buffer.len(), 4);
    }

    #[test]
    fn removal() {
        let input = [3, 6, 7];
        let mut buffer = ReorderBuffer::new(64);

        for seq_nr in input.iter() {
            buffer.insert(Packet {
                header: PacketHeader {
                    seq_nr: *seq_nr,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            });
        }

        assert!(buffer.get(8).is_none());
        assert!(buffer.get(5).is_none());
        assert_eq!(buffer.len(), 3);

        for seq_nr in input.iter() {
            let packet = buffer.remove(*seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, *seq_nr);
        }

        assert_eq!(buffer.len(), 0);
        for seq_nr in input.iter() {
            assert!(buffer.get(*seq_nr).is_none());
        }
    }

    #[test]
    fn removal_of_last_with_wraparound() {
        // Ensures calculating the new last index
        // works as expected when wrapping around
        // found by fuzzing
        let input = [57078, 56842];
        let mut buffer = ReorderBuffer::new(64);

        for seq_nr in input.iter() {
            buffer.insert(Packet {
                header: PacketHeader {
                    seq_nr: *seq_nr,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            });
        }
        assert_eq!(buffer.len(), 2);
        for seq_nr in input.iter() {
            let packet = buffer.remove(*seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, *seq_nr);
        }

        assert_eq!(buffer.len(), 0);
        for seq_nr in input.iter() {
            assert!(buffer.get(*seq_nr).is_none());
        }
    }

    #[test]
    fn removal_of_last_with_wraparound_v2() {
        // Ensures calculating the new last index
        // works as expected when wrapping around
        // found by fuzzing
        let input = [22320, 22370, 14126];
        let mut buffer = ReorderBuffer::new(64);

        for seq_nr in input.iter() {
            buffer.insert(Packet {
                header: PacketHeader {
                    seq_nr: *seq_nr,
                    ack_nr: 0,
                    conn_id: 0,
                    packet_type: crate::utp_packet::PacketType::Data,
                    timestamp_microseconds: 0,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    extension: 0,
                },
                data: Bytes::new(),
            });
        }

        assert_eq!(buffer.len(), 3);
        for seq_nr in input.iter() {
            let packet = buffer.remove(*seq_nr).unwrap();
            assert_eq!(packet.header.seq_nr, *seq_nr);
        }

        assert_eq!(buffer.len(), 0);
        for seq_nr in input.iter() {
            assert!(buffer.get(*seq_nr).is_none());
        }
    }
}
