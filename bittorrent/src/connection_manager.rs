use std::io;
use std::net::SocketAddr;

use ahash::HashMap;
use bytes::BufMut;
use io_uring::opcode;
use io_uring::types;
use io_uring::types::Timespec;
use slotmap::Key;
use slotmap::SlotMap;
use slotmap::new_key_type;
use socket2::Domain;
use socket2::Protocol;
use socket2::SockAddr;
use socket2::Socket;
use socket2::Type;
use std::os::fd::AsRawFd;

use crate::PeerId;
use crate::buf_pool::BufferPool;
use crate::event_loop::EventData;
use crate::event_loop::EventId;
use crate::event_loop::EventType;
use crate::io_utils;
use crate::io_utils::BackloggedSubmissionQueue;
use crate::io_utils::SubmissionQueue;
use crate::peer_comm::peer_connection::PeerConnection;
use crate::peer_comm::peer_protocol::HANDSHAKE_SIZE;
use crate::peer_comm::peer_protocol::write_handshake;

const CONNECT_TIMEOUT: Timespec = Timespec::new().sec(10);

new_key_type! {
    pub struct ConnectionId;
}

pub enum ConnectionState {
    Unconnected { addr: SockAddr },
    Connected { socket: Socket, addr: SockAddr },
    //Handshaking { socket: Socket, addr: SocketAddr },
}

//impl ConnectionState {
//    fn addr(&self) -> &SockAddr {
//        match &self {
//            ConnectionState::Unconnected { addr } => addr,
//            ConnectionState::Connected { addr, .. } => addr,
//        }
//    }
//}

pub struct ConnectionManager {
    established: SlotMap<ConnectionId, PeerConnection>,
    all: HashMap<SockAddr, ConnectionState>,
    max_connections: usize,
    our_id: PeerId,
}

impl ConnectionManager {
    fn validate_addr(&self, peer: &SockAddr) -> bool {
        if self.all.len() >= self.max_connections {
            // log::trace!(
            //     "Ignoring peer: {peer}, max connections ({}) reached",
            //     self.max_connections
            // );
            return false;
        }
        !self.all.contains_key(peer)
    }

    pub fn maybe_connect_to_peer<Q: SubmissionQueue>(
        &mut self,
        peer: SockAddr,
        events: &mut SlotMap<EventId, EventData>,
        sq: &mut BackloggedSubmissionQueue<Q>,
    ) {
        if self.validate_addr(&peer) {
            return;
        }
        // ADD TO PENDING Etc

        let socket = match Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)) {
            Ok(socket) => socket,
            Err(e) => {
                log::error!("Failed to create socket: {e}");
                return;
            }
        };

        let event_idx = events.insert(EventData {
            typ: EventType::Connect {
                socket,
                addr: peer.into(),
            },
            buffers: None,
        });

        let EventType::Connect { socket, addr } = &events[event_idx].typ else {
            unreachable!();
        };

        log::debug!(
            "[{}] Connecting to peer",
            addr.as_socket().expect("must be AF_INET")
        );
        #[cfg(feature = "metrics")]
        {
            let connect_counter = metrics::counter!("peer_connect_attempts");
            connect_counter.increment(1);
        }

        let connect_op = opcode::Connect::new(
            types::Fd(socket.as_raw_fd()),
            addr.as_ptr().cast(),
            addr.len(),
        )
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(event_idx.data().as_ffi());
        let timeout_op = opcode::LinkTimeout::new(&CONNECT_TIMEOUT)
            .build()
            .user_data(event_idx.data().as_ffi());
        // If the queue doesn't fit both events they need
        // to be sent to the backlog so they can be submitted
        // together and not with a arbitrary delay inbetween.
        // That would mess up the timeout
        if sq.remaining() >= 2 {
            sq.push(connect_op);
            sq.push(timeout_op);
        } else {
            sq.push_backlog(connect_op);
            sq.push_backlog(timeout_op);
        }
    }

    // todo do state chagne here
    fn write_handshake<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        info_hash: [u8; 20],
        write_pool: &mut BufferPool,
        events: &mut SlotMap<EventId, EventData>,
        socket: Socket,
        addr: SockAddr,
    ) {
        let mut buffer = write_pool.get_buffer();
        if buffer.remaining_mut() < HANDSHAKE_SIZE {
            panic!("Buffer size is too small for sending a handshake");
        }
        write_handshake(self.our_id, info_hash, &mut buffer);
        io_utils::write(sq, events, socket, addr, buffer)
    }


    // todo do state chagne here
    pub fn on_accepted<Q: SubmissionQueue>(
        &mut self,
        socket: Socket,
        info_hash: [u8; 20],
        events: &mut SlotMap<EventId, EventData>,
        write_pool: &mut BufferPool,
        sq: &mut BackloggedSubmissionQueue<Q>,
    ) -> io::Result<()> {
        let addr = socket.peer_addr()?;
        if self.validate_addr(&addr.into()) {
            return Ok(());
        }
        if addr.is_ipv6() {
            log::error!("Received connection from non ipv4 addr");
            io_utils::close_socket(sq, socket, None, events);
            return Ok(());
        };

        log::info!(
            "Accepted connection: {:?}",
            addr.as_socket().expect("must be AF_INET")
        );
        // Trigger a write handshake here so we end up in the same code path
        // as outgoing connections. It will simplify things greatly
        self.write_handshake(sq, info_hash, write_pool, events, socket, addr);
        Ok(())
    }

    pub fn on_connect(&mut self) -> bool {}
    pub fn on_read(&mut self, socket: Socket, addr: SocketAddr) -> bool {}
}
