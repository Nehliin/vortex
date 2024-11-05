use std::{io::{self, ErrorKind}, os::fd::RawFd};

use bytes::Buf;

#[derive(Debug)]
pub struct PeerConnection {
    pub fd: RawFd,
    pub peer_id: [u8; 20],
    /// This side is choking the peer
    pub is_choking: bool,
    /// This side is interested what the peer has to offer
    pub is_interested: bool,
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // TODO: do we need this both with queued?
    //pub currently_downloading: Vec<Piece>,
    //pub queue_capacity: usize,
    //pub queued: Vec<InflightSubpiece>,
    //pub slow_start: bool,
    //pub moving_rtt: MovingRttAverage,
    // TODO calculate this meaningfully
    //pub througput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    //pub pending_disconnect: bool,
}


// pub peer_id: [u8; 20],
// pub peer_addr: SocketAddr,
async fn handshake(
    our_peer_id: [u8; 20],
    info_hash: [u8; 20],
    stream: &TcpStream,
) -> anyhow::Result<[u8; 20]> {
    let mut buf: Vec<u8> = Vec::with_capacity(68);
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    buf.put_u8(PROTOCOL.len() as u8);
    buf.put_slice(PROTOCOL);
    buf.put_slice(&[0_u8; 8] as &[u8]);
    buf.put_slice(&info_hash as &[u8]);
    buf.put_slice(&our_peer_id as &[u8]);
    let (res, buf) = stream.write_all(buf).await;
    res?;
    let (res, buf) = stream.read(buf).await;
    let read_bytes = res?;
    anyhow::ensure!(read_bytes == 68);
    let mut buf = buf.as_slice();
    let str_len = buf.get_u8();
    anyhow::ensure!(str_len == 19);
    anyhow::ensure!(buf.chunk().get(..str_len as usize) == Some(b"BitTorrent protocol" as &[u8]));
    buf.advance(str_len as usize);
    // Skip extensions for now
    buf.advance(8);
    let peer_info_hash: [u8; 20] = buf.chunk()[..20].try_into()?;
    anyhow::ensure!(info_hash == peer_info_hash);
    buf.advance(20_usize);
    let peer_id = buf.chunk()[..20].try_into()?;
    buf.advance(20_usize);
    anyhow::ensure!(!buf.has_remaining());
    Ok(peer_id)
}

fn generate_peer_id() -> [u8; 20] {
    // Based on http://www.bittorrent.org/beps/bep_0020.html
    const PREFIX: [u8; 8] = *b"-VT0010-";
    let generatated = rand::random::<[u8; 12]>();
    let mut result: [u8; 20] = [0; 20];
    result[0..8].copy_from_slice(&PREFIX);
    result[8..].copy_from_slice(&generatated);
    result
}

// TODO split up
pub fn incoming_handshake(
    fd: RawFd,
    info_hash: [u8; 20],
    mut buffer: &[u8],
) -> io::Result<PeerConnection> {
    if buffer.len() < 68 {
        // Meh?
        return Err(ErrorKind::UnexpectedEof.into());
    }
    let str_len = buffer.get_u8() as usize;
    if &buffer[..str_len] != b"BitTorrent protocol" as &[u8] {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(str_len);
    // Skip extensions for now
    buffer.advance(8);
    let peer_info_hash: [u8; 20] = buffer[..20]
        .try_into()
        .map_err(|_err| ErrorKind::InvalidData)?;
    if peer_info_hash != info_hash {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(20_usize);
    let peer_id = buffer[..20]
        .try_into()
        .map_err(|_err| ErrorKind::InvalidData)?;
    Ok(PeerConnection {
        fd,
        peer_id,
        is_choking: true,
        is_interested: false,
        peer_choking: true,
        peer_interested: false,
    })
}
