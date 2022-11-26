use core::slice;
use std::{
    cell::RefCell, iter, mem::ManuallyDrop, net::SocketAddr, path::PathBuf, rc::Rc,
    thread::ThreadId,
};

use anyhow::Context;
use bitvec::prelude::BitBox;
use bytes::{Buf, BytesMut};
use tokio::{
    sync::mpsc::Receiver,
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender},
};
use tokio_uring::{
    buf::{
        fixed::{FixedBuf, FixedBufRegistry},
        BoundedBuf, BoundedBufMut, IoBuf, IoBufMut, Slice,
    },
    fs::File,
    net::TcpStream,
};
use tokio_util::sync::CancellationToken;

use crate::{peer_connection::PeerConnection, peer_message::PeerMessage};

// socket-> subpiece -> disk using same handle, skip intermededitery vec HOW TO CHECK HASHSUM?
// socket -> logic -> disk -> logic -> socket // piece request should use the same handle
// logic -> socket

#[derive(Debug)]
pub struct Buffer {
    buffer: FixedBuf,
    // Ensure this buffer is dropped in the right thread
    thread_id: ThreadId,
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.buffer.buf_index() == other.buffer.buf_index() && self.thread_id == other.thread_id
    }
}

impl Eq for Buffer {}

// Since it's not clonable and destruction is checked to
// happen in the same thread it was created on this is "safe"
// to send across threads
unsafe impl Send for Buffer {}

impl Drop for Buffer {
    fn drop(&mut self) {
        // Sanify check
        assert_eq!(self.thread_id, std::thread::current().id());
        OCCUPIED.with(|bitset| {
            bitset
                .unwrap()
                .borrow_mut()
                .set(self.buffer.buf_index() as usize, true)
        });
    }
}

impl BoundedBufMut for Buffer {
    type BufMut = FixedBuf;

    fn stable_mut_ptr(&mut self) -> *mut u8 {
        tokio_uring::buf::BoundedBufMut::stable_mut_ptr(&mut self.buffer)
    }

    unsafe fn set_init(&mut self, pos: usize) {
        tokio_uring::buf::BoundedBufMut::set_init(&mut self.buffer, pos)
    }
}

impl BoundedBuf for Buffer {
    type Buf = FixedBuf;

    type Bounds = std::ops::RangeFull;

    #[inline]
    fn slice(self, range: impl std::ops::RangeBounds<usize>) -> tokio_uring::buf::Slice<Self::Buf> {
        self.buffer.slice(range)
    }

    #[inline]
    fn slice_full(self) -> tokio_uring::buf::Slice<Self::Buf> {
        self.buffer.slice_full()
    }

    #[inline]
    fn get_buf(&self) -> &Self::Buf {
        self.buffer.get_buf()
    }

    #[inline]
    fn bounds(&self) -> Self::Bounds {
        self.buffer.bounds()
    }

    #[inline]
    fn from_buf_bounds(buf: Self::Buf, bounds: Self::Bounds) -> Self {
        Buffer {
            buffer: FixedBuf::from_buf_bounds(buf, bounds),
            thread_id: std::thread::current().id(),
        }
    }

    #[inline]
    fn stable_ptr(&self) -> *const u8 {
        tokio_uring::buf::BoundedBuf::stable_ptr(&self.buffer)
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        tokio_uring::buf::BoundedBuf::bytes_init(&self.buffer)
    }

    #[inline]
    fn bytes_total(&self) -> usize {
        tokio_uring::buf::BoundedBuf::bytes_total(&self.buffer)
    }
}

thread_local! {
    static OCCUPIED: Option<RefCell<BitBox>> = None;
}

#[derive(Clone)]
struct BufferPool {
    registry: FixedBufRegistry,
}

impl BufferPool {
    fn new(pool_size: usize) -> Self {
        // Spec says max size of request is 2^14 so double that for safety
        let registry = FixedBufRegistry::new(iter::repeat(vec![0; 1 << 15]).take(pool_size));
        let occupied = (0..pool_size).map(|_| false).collect();
        registry.register().unwrap();
        OCCUPIED.with(|bitset| {
            if bitset.is_some() {
                panic!("Only one buffer pool per thread is supported");
            }
            bitset = &Some(RefCell::new(occupied));
        });
        Self { registry }
    }

    fn get_buffer(&self) -> Buffer {
        let fixed_buf = OCCUPIED.with(|bitset| {
            let bitset = bitset.unwrap().borrow_mut();
            let buf_index = bitset.first_zero().unwrap();
            let fixed_buf = self.registry.check_out(buf_index).unwrap();
            bitset.set(buf_index, true);
            fixed_buf
        });
        Buffer {
            buffer: fixed_buf,
            thread_id: std::thread::current().id(),
        }
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
    buffer_pool: BufferPool,
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
            let peer_id = PeerConnection::handshake(&stream, our_id, info_hash)
                .await
                .unwrap();
            notiy_connected.send(peer_id).unwrap();
            log::info!("[Peer: {peer_id:?}] Connected");
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
                log::info!("[Peer: {peer_id:?}] Shutting down send task");
            });

            let mut prev: Option<Slice<Buffer>> = None;
            loop {
                let read_buf: Buffer = buffer_pool.get_buffer();
                let (result, buf) = stream.read_fixed(read_buf).await;
                match result {
                    Ok(0) => {
                        // TODO: check there isn't data remaining
                        log::info!("Shutting down connection");
                        break;
                    }
                    Ok(bytes_read) => {
                        let current_buf: Slice<Buffer> = buf.slice(..bytes_read);
                        let current_buf_slice: &[u8] = &current_buf.get_ref().buffer;
                        let total = if let Some(prev) = prev {
                            let prev_buf = &prev.get_ref().buffer;
                            prev_buf.chain(current_buf_slice)
                        } else {
                            current_buf_slice.chain(&[] as &[u8])
                        };
                        let lenght = total.get_i32();
                        if total.remaining() as i32 >= lenght {

                        } else {
                            prev = Some(current_buf);
                        }
                        //if ass
                        // Parse lenght
                        // parse header
                        // the body can be a stream of buffers?/small vec of slices
                        /*if prev.is_none() && bytes_read < std::mem::size_of::<i32>() {
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
                        read_buf.unsplit(remainder);*/
                    }
                    Err(err) => {
                        log::error!("Failed to read from peer connection: {err}");
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
                    tokio_uring::spawn(peer_connection_creation_task(
                        peer_connection_creator_rc,
                        buffer_pool.clone(),
                    ));
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
