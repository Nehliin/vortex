use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;

use ahash::HashMap;
use ahash::HashMapExt;
use bytes::BufMut;
use rayon::Scope;
use slotmap::SlotMap;
use slotmap::new_key_type;
use socket2::Domain;
use socket2::Protocol;
use socket2::SockAddr;
use socket2::Socket;
use socket2::Type;

use crate::PeerId;
use crate::event_loop::EventData;
use crate::event_loop::EventType;
use crate::event_loop::HANDSHAKE_TIMEOUT;
use crate::event_loop::conn_parse_and_handle_msgs;
use crate::io::Io;
use crate::io::SubmissionQueue;
use crate::peer_comm::extended_protocol::extension_handshake_msg;
use crate::peer_comm::peer_connection::PeerConnection;
use crate::peer_comm::peer_protocol::{self, HANDSHAKE_SIZE, parse_handshake, write_handshake};
use crate::torrent::StateRef;

new_key_type! {
    pub struct ConnectionId;
}

/// State of a peer address that the manager is aware of. During the handshake
/// phase the socket lives inside the in-flight `EventData`; once established it
/// lives inside the [`PeerConnection`] stored in `established`. `all` therefore
/// only tracks *state*, never the socket itself.
#[derive(Debug, Clone, Copy)]
pub enum ConnectionState {
    /// Outgoing connect operation in-flight, socket lives in the `Connect` event.
    Connecting,
    /// Handshake write/recv in-flight, socket lives in the `Write`/`Recv` event.
    Handshaking,
    /// Handshake completed, socket lives in `established[id]`. The id is tracked
    /// for future address -> connection lookups (not yet consumed).
    Connected(#[allow(dead_code)] ConnectionId),
}

pub struct ConnectionManager {
    /// Every peer address the manager is aware of, regardless of whether the
    /// connection is incoming or outgoing. Used for de-duplication and to
    /// enforce the max-connection cap across both directions.
    pub(crate) all: HashMap<SockAddr, ConnectionState>,
    max_connections: usize,
    our_id: PeerId,
}

impl ConnectionManager {
    pub fn new(our_id: PeerId, max_connections: usize) -> Self {
        Self {
            all: HashMap::with_capacity(max_connections),
            max_connections,
            our_id,
        }
    }

    /// Returns true if we are allowed to register a new peer with this address,
    /// i.e. we are not already tracking it and we are below the connection cap.
    fn can_accept_new(&self, peer: &SockAddr) -> bool {
        if self.all.len() >= self.max_connections {
            log::trace!(
                "Ignoring peer, max connections ({}) reached",
                self.max_connections
            );
            return false;
        }
        !self.all.contains_key(peer)
    }

    /// Attempt to open an outgoing connection to `peer`. No-op if the peer is
    /// already tracked or the connection cap has been reached.
    pub fn maybe_connect_to_peer<Q: SubmissionQueue>(&mut self, peer: SockAddr, io: &mut Io<Q>) {
        if !self.can_accept_new(&peer) {
            return;
        }

        let socket = match Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)) {
            Ok(socket) => socket,
            Err(e) => {
                log::error!("Failed to create socket: {e}");
                return;
            }
        };

        log::debug!(
            "[{}] Connecting to peer",
            peer.as_socket().expect("must be AF_INET")
        );
        #[cfg(feature = "metrics")]
        {
            let connect_counter = metrics::counter!("peer_connect_attempts");
            connect_counter.increment(1);
        }

        self.all.insert(peer.clone(), ConnectionState::Connecting);
        io.connect(socket, peer);
    }

    fn write_handshake<Q: SubmissionQueue>(
        &mut self,
        io: &mut Io<Q>,
        info_hash: [u8; 20],
        socket: Socket,
        addr: SockAddr,
    ) {
        let mut buffer = io.write_pool.get_buffer();
        if buffer.remaining_mut() < HANDSHAKE_SIZE {
            panic!("Buffer size is too small for sending a handshake");
        }
        write_handshake(self.our_id, info_hash, &mut buffer);
        io.write(socket, addr, buffer)
    }

    /// Handle a freshly accepted incoming connection. Registers the peer and
    /// kicks off a handshake write so incoming and outgoing connections share
    /// the same code path from here on.
    pub fn on_accepted<Q: SubmissionQueue>(
        &mut self,
        socket: Socket,
        info_hash: [u8; 20],
        io: &mut Io<Q>,
    ) -> io::Result<()> {
        let addr = socket.peer_addr()?;
        if addr.is_ipv6() {
            log::error!("Received connection from non ipv4 addr");
            io.close_socket(socket, None);
            return Ok(());
        }
        if !self.can_accept_new(&addr) {
            log::debug!(
                "Rejecting incoming connection from already tracked or excess peer: {:?}",
                addr.as_socket()
            );
            io.close_socket(socket, None);
            return Ok(());
        }

        log::info!(
            "Accepted connection: {:?}",
            addr.as_socket().expect("must be AF_INET")
        );
        self.all.insert(addr.clone(), ConnectionState::Handshaking);
        // Trigger a write handshake here so we end up in the same code path
        // as outgoing connections. It will simplify things greatly
        self.write_handshake(io, info_hash, socket, addr);
        Ok(())
    }

    /// Handle a completed outgoing connect. Transitions the peer into the
    /// handshaking state and writes our handshake.
    pub fn on_connect<Q: SubmissionQueue>(
        &mut self,
        socket: Socket,
        addr: SockAddr,
        info_hash: [u8; 20],
        io: &mut Io<Q>,
    ) {
        log::info!(
            "Connected to: {}",
            addr.as_socket().expect("must be AF_INET")
        );
        #[cfg(feature = "metrics")]
        {
            let connect_success_counter = metrics::counter!("peer_connect_success");
            connect_success_counter.increment(1);
        }
        if let Some(state) = self.all.get_mut(&addr) {
            *state = ConnectionState::Handshaking;
        }
        self.write_handshake(io, info_hash, socket, addr);
    }

    /// Handle a completed handshake write. On success the connection moves on to
    /// receiving the peer's handshake response.
    pub fn on_write<Q: SubmissionQueue>(
        &mut self,
        socket: Socket,
        addr: SockAddr,
        written: usize,
        expected_write: usize,
        io: &mut Io<Q>,
    ) {
        if written == expected_write {
            let fd = socket.as_raw_fd();
            log::debug!(
                "Wrote handshake to unestablished connection: {}",
                addr.as_socket().expect("must be AF_INET")
            );
            let read_event_id = io.events.insert(EventData {
                typ: EventType::Recv { socket, addr },
                buffers: None,
            });
            // Write is only used for unestablished connections aka when doing handshake
            #[cfg(feature = "metrics")]
            {
                let handshake_counter = metrics::counter!("peer_handshake_attempt");
                handshake_counter.increment(1);
            }
            // Multishot isn't used here to simplify error handling
            // when the read is invalid or otherwise doesn't lead to
            // a full connection which does have graceful shutdown mechanisms
            io.recv(read_event_id, fd, &HANDSHAKE_TIMEOUT);
        } else {
            // We don't deal with partial writes for handshakes, it should never happen
            log::error!(
                "Failed to write handshake to unestablished connection: {}",
                addr.as_socket().expect("must be AF_INET")
            );
            self.all.remove(&addr);
            io.close_socket(socket, None);
        }
    }

    /// Handle received handshake data. On a valid handshake the peer is promoted
    /// to an established [`PeerConnection`], multishot receive is started and the
    /// initial extension handshake / bitfield messages are queued.
    ///
    /// Takes `established` directly rather than as `&mut self` since the newly
    /// promoted connection is inserted into it while `state` (which owns the
    /// connection manager) is also needed, and the two would otherwise alias.
    pub fn on_read<'scope, 'state: 'scope, Q: SubmissionQueue>(
        established: &mut SlotMap<ConnectionId, PeerConnection>,
        socket: Socket,
        addr: SockAddr,
        data: &[u8],
        io: &mut Io<Q>,
        state: &mut StateRef<'state>,
        scope: &Scope<'scope>,
    ) -> io::Result<()> {
        let socket_addr: SocketAddr = addr.as_socket().expect("must be AF_INET");
        let fd = socket.as_raw_fd();
        if data.is_empty() {
            log::debug!("[{socket_addr}] No more data when expecting handshake from connection");
            state.connection_manager.all.remove(&addr);
            io.close_socket(socket, None);
            return Ok(());
        }
        // TODO: This could happen due to networks splitting the handshake up
        // so it should be dealt with better, but since the handshake is so
        // small (well below MTU) I suspect that to be rare
        if data.len() < HANDSHAKE_SIZE {
            log::error!("[{socket_addr}] Didn't receive enough data to parse handshake");
            state.connection_manager.all.remove(&addr);
            io.close_socket(socket, None);
            return Err(io::ErrorKind::InvalidData.into());
        }
        let (handshake_data, remainder) = data.split_at(HANDSHAKE_SIZE);
        // Expect this to be the handshake response
        let parsed_handshake = match parse_handshake(*state.info_hash(), handshake_data) {
            Ok(handshake) => handshake,
            Err(err) => {
                log::error!("[{socket_addr}] Failed to parse handshake: {err}");
                state.connection_manager.all.remove(&addr);
                io.close_socket(socket, None);
                return Err(io::ErrorKind::InvalidData.into());
            }
        };

        let conn_id = established.insert_with_key(|conn_id| {
            PeerConnection::new(socket, socket_addr, conn_id, parsed_handshake)
        });
        state
            .connection_manager
            .all
            .insert(addr, ConnectionState::Connected(conn_id));
        log::info!("[{socket_addr}] Finished handshake! [{conn_id:?}]");

        #[cfg(feature = "metrics")]
        {
            let handshake_success_counter = metrics::counter!("peer_handshake_success");
            handshake_success_counter.increment(1);
        }

        let recv_multi_id = io.events.insert(EventData {
            typ: EventType::ConnectedRecv {
                connection_idx: conn_id,
            },
            buffers: None,
        });

        // The initial Recv might have contained more data
        // than just the handshake so need to handle that here
        // since the read_buffer will be overwritten by the next
        // incoming recv cqe
        let connection = &mut established[conn_id];
        connection.stateful_decoder.append_data(remainder);
        conn_parse_and_handle_msgs(connection, state, &mut io.queued_disk_operations, scope);
        if connection.extended_extension {
            connection
                .outgoing_msgs_buffer
                .push(extension_handshake_msg(state, state.config));
        }
        // Recv has been complete, move over to multishot, same user data
        io.recv_multishot(recv_multi_id, fd);

        // TODO: only if fast ext is enabled
        let bitfield_msg = if let Some(torrent_state) = state.state() {
            let completed = torrent_state.piece_selector.downloaded_clone();
            // sent as first message after handshake
            if completed.all() {
                peer_protocol::PeerMessage::HaveAll
            } else if completed.not_any() {
                peer_protocol::PeerMessage::HaveNone
            } else {
                peer_protocol::PeerMessage::Bitfield(completed.into())
            }
        } else {
            peer_protocol::PeerMessage::HaveNone
        };
        let connection = &mut established[conn_id];
        connection.outgoing_msgs_buffer.push(bitfield_msg);
        Ok(())
    }
}
