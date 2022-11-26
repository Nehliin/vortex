use core::slice;
use std::{mem::ManuallyDrop, net::SocketAddr, path::PathBuf, rc::Rc};

use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    sync::mpsc::Receiver,
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_uring::{
    buf::{fixed::FixedBuf, IoBuf, IoBufMut},
    fs::File,
    net::TcpStream,
};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::{peer_connection::PeerConnection, peer_message::PeerMessage};

// socket-> subpiece -> disk using same handle, skip intermededitery vec HOW TO CHECK HASHSUM?
// socket -> logic -> disk -> logic -> socket // piece request should use the same handle
// logic -> socket

#[derive(Debug)]
pub struct IoBufHandle {
    data: ManuallyDrop<Box<[u8]>>,
    drop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl IoBufHandle {
    fn new(mut fixed_buf: FixedBuf) -> Self {
        // Safe: Data is tied to the lifetime of the fixed buf which won't be dropped
        // until the oneshot sender sends the notification when IoBufHandle is dropped.
        // IoBufHandle won't double free because of the ManuallyDrop.
        let data = unsafe {
            ManuallyDrop::new(Box::from_raw(slice::from_raw_parts_mut(
                fixed_buf.stable_mut_ptr(),
                fixed_buf.bytes_init(),
            )))
        };
        let (drop_tx, drop_rc) = tokio::sync::oneshot::channel();
        // Ensure the handle is not dropped until the IoBufHandle has been dropped
        tokio_uring::spawn(async move {
            // Don't care if the sender is dropped prematurely
            let _ = drop_rc.await;
            drop(fixed_buf);
        });
        IoBufHandle {
            data,
            drop_tx: Some(drop_tx),
        }
    }
}

// or simply deref?
unsafe impl IoBuf for IoBufHandle {
    #[inline(always)]
    fn stable_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline(always)]
    fn bytes_init(&self) -> usize {
        <[u8]>::len(&self.data)
    }

    #[inline(always)]
    fn bytes_total(&self) -> usize {
        self.bytes_init()
    }
}

impl Drop for IoBufHandle {
    fn drop(&mut self) {
        self.drop_tx
            .take()
            .unwrap()
            .send(())
            .expect("FixedBuf dropped while in use! UB most likely");
    }
}

#[derive(Debug)]
pub enum FileOperation {
    Write { offset: u64, data: Vec<u8> },
    Close,
}

pub struct FileHandle {
    file_op_tx: UnboundedSender<FileOperation>,
}

impl FileHandle {
    #[inline]
    pub fn write(&self, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        self.file_op_tx
            .send(FileOperation::Write { offset, data })
            .context("Disk io thread have shutdown")
    }

    #[inline]
    pub fn close(&self) -> anyhow::Result<()> {
        self.file_op_tx
            .send(FileOperation::Close)
            .context("Disk io thread have shutdown")
    }
}

#[derive(Debug)]
struct FileCreationMsg {
    path: PathBuf,
    file_op_rc: UnboundedReceiver<FileOperation>,
}

async fn file_creation_task(mut file_creator: Receiver<FileCreationMsg>) {
    while let Some(mut file_creation_msg) = file_creator.recv().await {
        // file operation task
        tokio_uring::spawn(async move {
            let file = Rc::new(File::create(&file_creation_msg.path).await.unwrap());
            while let Some(job) = file_creation_msg.file_op_rc.recv().await {
                match job {
                    FileOperation::Write { offset, data } => {
                        let file_clone = file.clone();
                        tokio_uring::spawn(async move {
                            let (result, _buf) = file_clone.write_all_at(data, offset).await;
                            if let Err(err) = result {
                                // TODO: This will silently corrupt the file and is currently
                                // not recoverable
                                log::error!("Failed to write piece: {err}");
                            }
                        });
                    }
                    FileOperation::Close => {
                        file.sync_all().await.unwrap();
                        break;
                    }
                }
            }
            // TODO closing it here?
        });
    }
}

#[derive(Debug)]
struct PeerConnectionCreationMsg {
    // TODO: could send specific connection error here potentially
    notiy_connected: tokio::sync::oneshot::Sender<[u8; 20]>,
    addr: SocketAddr,
    our_id: [u8; 20],
    info_hash: [u8; 20],
    incoming_tx: UnboundedSender<PeerMessage>,
    outgoing_rc: UnboundedReceiver<PeerMessage>,
    cancellation_token: CancellationToken,
}

struct PendingMsg {
    // Number of bytes remaining
    remaining_bytes: i32,
    // Bytes accumalated so far
    partial: BytesMut,
}

// Consider using the framed writes and encode/decode traits
fn parse_msgs(
    incoming_tx: &UnboundedSender<PeerMessage>,
    mut incoming: BytesMut,
    pending_msg: &mut Option<PendingMsg>,
) {
    while let Some(mut pending) = pending_msg.take() {
        // There exist enough data to finish the pending message
        if incoming.remaining() as i32 >= pending.remaining_bytes {
            let mut remainder = incoming.split_off(pending.remaining_bytes as usize);
            pending.partial.unsplit(incoming.split());
            log::trace!("Extending partial: {}", pending.remaining_bytes);
            let msg = PeerMessage::try_from(pending.partial.freeze()).unwrap();
            incoming_tx.send(msg).unwrap();
            // Should we try to start parsing a new msg?
            if remainder.remaining() >= std::mem::size_of::<i32>() {
                let len_rem = remainder.get_i32();
                log::debug!("Starting new message with len: {len_rem}");
                // If we don't start from the 0 here the extend from slice will
                // duplicate the partial data
                let partial: BytesMut = BytesMut::new();
                *pending_msg = Some(PendingMsg {
                    remaining_bytes: len_rem,
                    partial,
                });
            } else {
                log::trace!("Buffer spent");
                // This might not be true if we are unlucky
                // and it's possible for a i32 to split between
                // to separate receive operations
                // THIS WILL PANIC
                if remainder.remaining() != 0 {
                    log::error!(
                        "Buffer spent but not empty, remaining: {}",
                        remainder.remaining()
                    );
                }
                *pending_msg = None;
            }
            incoming = remainder;
        } else {
            log::trace!("More data needed");
            pending.remaining_bytes -= incoming.len() as i32;
            pending.partial.unsplit(incoming);
            *pending_msg = Some(pending);
            // Return to read more data!
            return;
        }
    }
}

async fn peer_connection_creation_task(
    mut peer_connection_creator: Receiver<PeerConnectionCreationMsg>,
) {
    while let Some(PeerConnectionCreationMsg {
        notiy_connected,
        addr,
        our_id,
        info_hash,
        incoming_tx,
        mut outgoing_rc,
        cancellation_token,
    }) = peer_connection_creator.recv().await
    {
        // Spawn new task for managing the peer
        tokio_uring::spawn(async move {
            let stream = Rc::new(TcpStream::connect(addr).await.unwrap());
            println!("Pre handshake");
            let peer_id = PeerConnection::handshake(&stream, our_id, info_hash)
                .await
                .unwrap();
            println!("SENDING PERE_ID");
            notiy_connected.send(peer_id).unwrap();
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
                        break;
                    }
                }
                println!("SHUTTING DOW");
            });

            // Spec says max size of request is 2^14 so double that for safety
            let mut read_buf = BytesMut::zeroed(1 << 15);
            let mut maybe_msg_len: Option<PendingMsg> = None;
            loop {
                debug_assert_eq!(read_buf.len(), 1 << 15);
                tokio::select! {
                    (result,buf) = stream.read(read_buf) => {
                        read_buf = buf;
                        match result {
                            Ok(0) => {
                                log::info!("Shutting down connection");
                                break;
                            }
                            Ok(bytes_read) => {
                                let remainder = read_buf.split_off(bytes_read);
                                if maybe_msg_len.is_none() && bytes_read < std::mem::size_of::<i32>() {
                                    panic!("Not enough data received");
                                }
                                let mut buf = read_buf.clone();
                                if maybe_msg_len.is_none() {
                                    let pending = PendingMsg {
                                        remaining_bytes: buf.get_i32(),
                                        partial: BytesMut::new(),
                                    };
                                    maybe_msg_len = Some(pending);
                                }
                                parse_msgs(&incoming_tx, buf, &mut maybe_msg_len);
                                read_buf.unsplit(remainder);
                            }
                            Err(err) => {
                                log::error!("Failed to read from peer connection: {err}");
                                break;
                            }
                        }
                    },
                    _ = cancellation_token.cancelled() => {
                        log::info!("Cancelling tcp stream read");
                        break;
                    }
                }
            }
        });
    }
}

#[derive(Clone)]
pub struct IoDriver {
    file_creator_tx: Sender<FileCreationMsg>,
    peer_connection_creator_tx: Sender<PeerConnectionCreationMsg>,
    cancellation_token: CancellationToken,
}

impl Drop for IoDriver {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

// TODO the driver should have a single cancellation token
// that's propagated down everywhere

// Io thread
// Read files can be done by using oneshot channels at first
// but later it should be "write through" i.e one piece request goes to the io IoDriver
// and it both reads from the file and then writes on the socket
// submit file operations, read,close,write
// add and remove connections, read/write sockets
//
// Main thread process incoming data
impl IoDriver {
    pub fn new() -> Self {
        let (file_creator_tx, file_creator_rc) = tokio::sync::mpsc::channel(64);
        let (peer_connection_creator_tx, peer_connection_creator_rc) =
            tokio::sync::mpsc::channel(64);
        let cancellation_token = CancellationToken::new();
        let child_token = cancellation_token.child_token();
        std::thread::Builder::new()
            .name("io_thread".to_string())
            .spawn(move || {
                // TODO builder?
                tokio_uring::start(async move {
                    // Listen for file creation messages
                    tokio_uring::spawn(file_creation_task(file_creator_rc));
                    // Listen for peer connection creation messages
                    tokio_uring::spawn(peer_connection_creation_task(peer_connection_creator_rc));
                    child_token.cancelled().await;
                })
            })
            .unwrap();
        Self {
            cancellation_token,
            file_creator_tx,
            peer_connection_creator_tx,
        }
    }

    pub async fn connect(
        &self,
        addr: SocketAddr,
        our_id: [u8; 20],
        info_hash: [u8; 20],
    ) -> anyhow::Result<(
        [u8; 20],
        UnboundedSender<PeerMessage>,
        UnboundedReceiver<PeerMessage>,
    )> {
        let (incoming_tx, incoming_rc) = tokio::sync::mpsc::unbounded_channel();
        let (outgoing_tx, outgoing_rc) = tokio::sync::mpsc::unbounded_channel();
        let (notiy_connected_tx, notiy_connected_rc) = tokio::sync::oneshot::channel();
        self.peer_connection_creator_tx
            .send(PeerConnectionCreationMsg {
                notiy_connected: notiy_connected_tx,
                addr,
                our_id,
                info_hash,
                incoming_tx,
                outgoing_rc,
                cancellation_token: self.cancellation_token.child_token(),
            })
            .await
            .context("Peer connection creation task no longer exist")?;
        let peer_id = notiy_connected_rc.await.context("Connect failed")?;
        Ok((peer_id, outgoing_tx, incoming_rc))
    }

    /*pub async fn accept(&self) -> PeerConnection {
        // creates the channels and sends "Connect" msg to the thread which spawns a new task
        // handling the conneciton. This may take a cancellation token which can be used on the io
        // task on the network thread
    }*/

    pub async fn create_file(&self, path: PathBuf) -> anyhow::Result<FileHandle> {
        let (file_op_tx, file_op_rc) = tokio::sync::mpsc::unbounded_channel();
        let handle = FileHandle { file_op_tx };
        self.file_creator_tx
            .send(FileCreationMsg { path, file_op_rc })
            .await
            .context("File creation task no longer exists")?;
        Ok(handle)
    }
}

impl Default for IoDriver {
    fn default() -> Self {
        Self::new()
    }
}
