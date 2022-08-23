use bytes::{Buf, Bytes, BytesMut};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PacketType {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

// repr c instead? and just send directly over socket?
#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    pub seq_nr: u16,
    pub ack_nr: u16,
    pub conn_id: u16,
    pub packet_type: PacketType,
    pub timestamp_microseconds: u32,
    pub timestamp_difference_microseconds: u32,
    pub wnd_size: u32,
    pub extension: u8,
}

impl TryFrom<&[u8]> for PacketHeader {
    type Error = anyhow::Error;

    fn try_from(mut bytes: &[u8]) -> anyhow::Result<Self> {
        const HEADER_SIZE: usize = 20;
        anyhow::ensure!(
            bytes.len() >= HEADER_SIZE,
            "Error: Packet to small to parse"
        );
        let first_byte = bytes.get_u8();
        let packet_type = first_byte >> 4;
        anyhow::ensure!(packet_type < 5, "Error: Packet type not recognized");
        let packet_type: PacketType = unsafe { std::mem::transmute(packet_type) };
        let version = first_byte & 0b0000_1111;
        anyhow::ensure!(version == 1, "Error: Packet version not supported");
        let extension = bytes.get_u8();
        let conn_id = bytes.get_u16();
        let timestamp_microseconds = bytes.get_u32();
        let timestamp_difference_microseconds = bytes.get_u32();
        let wnd_size = bytes.get_u32();
        let seq_nr = bytes.get_u16();
        let ack_nr = bytes.get_u16();

        Ok(Self {
            seq_nr,
            ack_nr,
            conn_id,
            packet_type,
            timestamp_microseconds,
            timestamp_difference_microseconds,
            wnd_size,
            extension,
        })
    }
}

impl PacketHeader {
    pub fn to_bytes(&self) -> Bytes {
        use bytes::BufMut;
        let mut bytes = BytesMut::new();

        let mut first_byte = self.packet_type as u8;
        first_byte <<= 4;
        first_byte |= 0b0000_0001;

        // type and version
        bytes.put_u8(first_byte);
        // 0 so doesn't matter for now if to_be should be used or not
        bytes.put_u8(self.extension);
        bytes.put_u16(self.conn_id);
        bytes.put_u32(self.timestamp_microseconds);
        bytes.put_u32(self.timestamp_difference_microseconds);
        bytes.put_u32(self.wnd_size);
        bytes.put_u16(self.seq_nr);
        bytes.put_u16(self.ack_nr);
        let res = bytes.freeze();
        log::debug!("{:02x?}", &res[..]);
        res
    }
}

#[derive(Clone, Debug)]
pub struct Packet {
    pub header: PacketHeader,
    // Todo bytes might not be necessesary here
    pub data: Bytes,
}

impl Packet {
    #[inline]
    pub fn neeeds_ack(&self) -> bool {
        self.header.packet_type == PacketType::Data
            || self.header.packet_type == PacketType::Fin
            || self.header.packet_type == PacketType::Syn
    }
}

// Not very rusty at all, stolen from libutp to test
// impact on connection errors
pub fn get_microseconds() -> u64 {
    static mut OFFSET: u64 = 0;
    static mut PREVIOUS: u64 = 0;

    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };

    if res < 0 {
        panic!("clock get time failed");
    }

    let mut now = ts.tv_sec as u64 * 1000000 + ts.tv_nsec as u64 / 1000;
    unsafe {
        now += OFFSET;
        if PREVIOUS > now {
            OFFSET += PREVIOUS - now;
            now = PREVIOUS;
        }
        PREVIOUS = now;
    }

    now
}
