use std::rc::Rc;

use bytes::{Buf, BytesMut};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_uring::{buf::IoBuf, net::TcpStream};
use tokio_util::{codec::Decoder, sync::CancellationToken};

use crate::peer_message::PeerMessage;

pub struct PeerMessageDecoder;

impl Decoder for PeerMessageDecoder {
    type Item = PeerMessage;

    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Can we read the lenght?
        if src.remaining() < std::mem::size_of::<i32>() {
            return Ok(None);
        }
        let lenght = src.get_i32() as usize;
        if src.remaining() < lenght || lenght < 1 {
            return Ok(None);
        }
        let message = src.split_to(lenght);
        let msg = PeerMessage::try_from(message.freeze())?;
        Ok(Some(msg))
    }
}

// Safe since the inner Rc have yet to
// have been cloned at this point and importantly
// there are not inflight operations at this point
//
// TODO this should be safe but consider passing around an
// Arc wrapped listener instead
pub struct SendableStream(pub TcpStream);
unsafe impl Send for SendableStream {}

pub fn start_network_thread(
    peer_id: [u8; 20],
    sendable_stream: SendableStream,
    _cancellation_token: CancellationToken,
) -> (UnboundedSender<PeerMessage>, UnboundedReceiver<PeerMessage>) {
    let (incoming_tx, incoming_rc) = tokio::sync::mpsc::unbounded_channel();
    let (outgoing_tx, mut outgoing_rc): (
        UnboundedSender<PeerMessage>,
        UnboundedReceiver<PeerMessage>,
    ) = tokio::sync::mpsc::unbounded_channel();

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let sendable_stream = sendable_stream;
            let stream = Rc::new(sendable_stream.0);
            let stream_clone = stream.clone();

            // Send loop, should be cancelled automatically in the next iteration when outgoing_rc is dropped.
            tokio_uring::spawn(async move {
                while let Some(outgoing) = outgoing_rc.recv().await {
                    // TODO Reuse buf and also try to coalece messages
                    // and use write vectored instead. I.e try to receive 3-5
                    // and write vectored. Have a timeout so it's not stalled forever
                    // and writes less if no more msgs are incoming
                    let (result, _buf) = stream_clone.write_all(outgoing.into_bytes()).await;
                    if let Err(err) = result {
                        log::error!("[Peer: {peer_id:?}] Sending PeerMessage failed: {err}");
                    }
                }
                log::info!("[Peer: {peer_id:?}] Shutting down send loop");
            });

            // Spec says max size of request is 2^14 so double that for safety
            let mut read_buf = BytesMut::zeroed(1 << 15);
            // Data read but not fully parsable yet, need to fetch more 
            // before attempting again
            let mut pending = BytesMut::new();
            let mut decoder = PeerMessageDecoder;
            loop {
                // Ensure we can always read the max size packet
                // this should not allocate frequently and should be able 
                // to utilize the BytesMut optimization where memory is reclaimed 
                // without allocating again.
                if read_buf.capacity() < (1 << 14) {
                    read_buf.reserve((1 << 14) - read_buf.capacity());
                }
                let (result, buf) = stream.read(read_buf).await;
                read_buf = buf;
                match result {
                    Ok(0) => {
                        log::info!("Shutting down connection");
                        break;
                    }
                    Ok(bytes_read) => {
                        // If we have remaining data unsplit with read_buf 
                        // This should often be continous memory ex: 
                        // |----data----|--pending-|---read_buf--|
                        // unless ofc read_buf have been reallocated 
                        // or reclaimed non continous memory
                        if pending.has_remaining() {
                            pending.unsplit(read_buf);
                            read_buf = std::mem::take(&mut pending);
                        }
                        let mut data = read_buf.split_to(bytes_read);
                        while let Some(msg) = decoder.decode(&mut data).ok().flatten() {
                            incoming_tx.send(msg).unwrap();
                        }
                        // if there is data left put it into pending
                        if data.has_remaining() {
                            pending = data.split();
                        }
                    }
                    Err(err) => {
                        log::error!("Failed to read from peer connection: {err}");
                        break;
                    }
                }
            }
        });
    });
    (outgoing_tx, incoming_rc)
}
