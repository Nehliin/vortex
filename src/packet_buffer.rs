use crate::utp_packet::Packet;

pub struct PacketBuffer {
    buffer: Vec<Option<Packet>>,
    cursor: usize,
}

impl PacketBuffer {
    // size in PACKETS
    pub fn new(size: usize) -> Self {
        PacketBuffer {
            buffer: vec![Option::<Packet>::None; size],
            cursor: 0,
        }
    }

    // Super ineffective, TODO fix this
    pub fn insert(&mut self, index: usize, packet: Packet) {
        todo!();
        /*if let Some(spot) = self.buffer.get_mut(index) {
            if spot.is_none() {
                *spot = Some(packet);
                return;
            } else {
                let mut i = self.cursor;
                while let Some(spot) = self.buffer.get_mut(i) {
                    if i == self.buffer.len() {
                        i = 0;
                    } else if spot.is_some() {
                        i += 1;
                        // looped
                    } else if i == self.cursor {
                        break;
                    } else {
                        *spot = Some(packet);
                        return;
                    }
                }

                // realloc
                self.buffer.extend_from_within(0..self.cursor);
                for i in 0..self.cursor {
                    self.buffer[i] = None;
                }
                self.cursor = 0;
            }
        }*/
    }

    pub fn get(&self, index: usize) -> Option<&Packet> {
        unimplemented!();
    }

    pub fn remove(&mut self, index: usize) -> Option<Packet> {
        unimplemented!()
    }
    
}
