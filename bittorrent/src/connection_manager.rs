use std::io;
use std::net::SocketAddr;
use std::ops::{Index, IndexMut};
use std::os::fd::{AsRawFd, RawFd};

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
use crate::buf_ring::Bid;
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

/// Lifecycle state of a single connection. A connection keeps the same
/// generational [`ConnectionId`] through every transition: the pending
/// variants own the socket until the handshake completes and it moves into
/// the [`PeerConnection`].
// We do not box since Established should be the majority of the entires
// and we do not want to be chasing pointers in the common case
#[allow(clippy::large_enum_variant)]
pub enum ConnectionState {
    /// Outgoing connection in progress.
    Connecting { socket: Socket, addr: SocketAddr },
    /// Handshake write/recv in-flight.
    Handshaking { socket: Socket, addr: SocketAddr },
    /// Handshake completed and a full PeerConnection has been established
    Established(PeerConnection),
    /// The socket has been handed to io_uring for closing.
    /// The entry is kept, holding on to the address, until the close completes
    /// so that the peer can't be reconnected to in the meantime.
    Closing { addr: SocketAddr },
    /// Transient placeholder enabling in-place variant transitions,
    Dummy,
}

impl ConnectionState {
    /// The peer address, regardless of lifecycle state.
    fn addr(&self) -> SocketAddr {
        match self {
            ConnectionState::Connecting { addr, .. }
            | ConnectionState::Handshaking { addr, .. }
            | ConnectionState::Closing { addr } => *addr,
            ConnectionState::Established(peer) => peer.peer_addr,
            ConnectionState::Dummy => unreachable!(),
        }
    }
}

pub struct ConnectionManager {
    connections: SlotMap<ConnectionId, ConnectionState>,
    max_connections: usize,
    our_id: PeerId,
}

impl ConnectionManager {
    pub fn new(our_id: PeerId, max_connections: usize) -> Self {
        Self {
            connections: SlotMap::with_capacity_and_key(max_connections),
            max_connections,
            our_id,
        }
    }

    // The address scan is linear but since it's bound by max_connections, which
    // is expected to be in the hundreds, it should be fast
    fn can_accept_new(&self, peer: SocketAddr) -> bool {
        if self.connections.len() >= self.max_connections {
            log::trace!(
                "Ignoring peer, max connections ({}) reached",
                self.max_connections
            );
            return false;
        }
        !self.connections.values().any(|state| state.addr() == peer)
    }

    /// Attempt to open an outgoing connection to `peer`. No-op if the peer is
    /// already tracked or the connection cap has been reached.
    pub fn maybe_connect_to_peer<Q: SubmissionQueue>(&mut self, peer: SockAddr, io: &mut Io<Q>) {
        let peer_addr = peer.as_socket().expect("must be AF_INET");
        if !self.can_accept_new(peer_addr) {
            return;
        }

        let socket = match Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)) {
            Ok(socket) => socket,
            Err(e) => {
                log::error!("Failed to create socket: {e}");
                return;
            }
        };

        log::debug!("[{peer_addr}] Connecting to peer");
        #[cfg(feature = "metrics")]
        {
            let connect_counter = metrics::counter!("peer_connect_attempts");
            connect_counter.increment(1);
        }

        let fd = socket.as_raw_fd();
        let conn_id = self.connections.insert(ConnectionState::Connecting {
            socket,
            addr: peer_addr,
        });
        io.connect(conn_id, fd, peer);
    }

    fn write_handshake<Q: SubmissionQueue>(
        &mut self,
        conn_id: ConnectionId,
        info_hash: [u8; 20],
        io: &mut Io<Q>,
    ) {
        let mut buffer = io.write_pool.get_buffer();
        if buffer.remaining_mut() < HANDSHAKE_SIZE {
            panic!("Buffer size is too small for sending a handshake");
        }
        write_handshake(self.our_id, info_hash, &mut buffer);
        let fd = match &self.connections[conn_id] {
            ConnectionState::Handshaking { socket, .. } => socket.as_raw_fd(),
            _ => unreachable!(
                "attempting to schedule handshake write for connection with unexpected state"
            ),
        };
        io.write(conn_id, fd, buffer)
    }

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
        let addr = addr.as_socket().expect("must be AF_INET");
        if !self.can_accept_new(addr) {
            log::debug!(
                "Rejecting incoming connection from already tracked or excess peer: {addr:?}"
            );
            io.close_socket(socket, None);
            return Ok(());
        }

        log::info!("Accepted connection: {addr:?}");
        let conn_id = self
            .connections
            .insert(ConnectionState::Handshaking { socket, addr });
        // Trigger a write handshake here so we end up in the same code path
        // as outgoing connections. It will simplify things greatly
        self.write_handshake(conn_id, info_hash, io);
        Ok(())
    }

    /// The state of a connection that is still live, or `None` if it has been
    /// or is in the process of being disconnected
    fn active(&self, conn_id: ConnectionId) -> Option<&ConnectionState> {
        match self.connections.get(conn_id)? {
            ConnectionState::Closing { .. } => None,
            state => Some(state),
        }
    }

    fn active_mut(&mut self, conn_id: ConnectionId) -> Option<&mut ConnectionState> {
        match self.connections.get_mut(conn_id)? {
            ConnectionState::Closing { .. } => None,
            state => Some(state),
        }
    }

    pub fn on_connect<Q: SubmissionQueue>(
        &mut self,
        conn_id: ConnectionId,
        info_hash: [u8; 20],
        io: &mut Io<Q>,
    ) {
        let Some(entry) = self.active_mut(conn_id) else {
            log::debug!("Connect completed for a disconnected connection: {conn_id:?}");
            return;
        };
        let ConnectionState::Connecting { socket, addr } =
            std::mem::replace(entry, ConnectionState::Dummy)
        else {
            unreachable!("connect completed for non-connecting connection");
        };
        log::info!("Connected to: {}", addr);
        #[cfg(feature = "metrics")]
        {
            let connect_success_counter = metrics::counter!("peer_connect_success");
            connect_success_counter.increment(1);
        }
        *entry = ConnectionState::Handshaking { socket, addr };
        self.write_handshake(conn_id, info_hash, io);
    }

    /// Handle a completed handshake write. On success the connection moves on to
    /// receiving the peer's handshake response.
    pub fn on_write<'state, Q: SubmissionQueue>(
        &mut self,
        conn_id: ConnectionId,
        written: usize,
        expected_write: usize,
        io: &mut Io<Q>,
        state: &mut StateRef<'state>,
    ) {
        let Some(entry) = self.active(conn_id) else {
            log::debug!("Handshake write completed for a disconnected connection: {conn_id:?}");
            return;
        };
        let peer_addr = entry.addr();
        if written == expected_write {
            let fd = match entry {
                ConnectionState::Handshaking { socket, .. } => socket.as_raw_fd(),
                _ => unreachable!("handshake write completed for connection with unexpected state"),
            };
            log::debug!("Wrote handshake to unestablished connection: {peer_addr}");
            let read_event_id = io.events.insert(EventData {
                typ: EventType::Recv {
                    connection_idx: conn_id,
                },
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
            log::error!("Failed to write handshake to unestablished connection: {peer_addr}");
            self.disconnect(conn_id, io, state);
        }
    }

    /// Handle received handshake data. On a valid handshake the peer is promoted
    /// to an established [`PeerConnection`] in place, keeping its id, multishot
    /// receive is started and the initial extension handshake / bitfield
    /// messages are queued.
    pub fn on_read<'scope, 'state: 'scope, Q: SubmissionQueue>(
        &mut self,
        conn_id: ConnectionId,
        read_bid: Option<Bid>,
        len: usize,
        io: &mut Io<Q>,
        state: &mut StateRef<'state>,
        scope: &Scope<'scope>,
    ) -> io::Result<()> {
        let Some(entry) = self.active(conn_id) else {
            log::debug!("Handshake data received for a disconnected connection: {conn_id:?}");
            return Ok(());
        };
        let socket_addr = entry.addr();
        let data: &[u8] = match read_bid {
            Some(bid) => &io.read_ring.get(bid)[..len],
            None => &[],
        };
        if data.is_empty() {
            log::debug!("[{socket_addr}] No more data when expecting handshake from connection");
            self.disconnect(conn_id, io, state);
            return Ok(());
        }
        // TODO: This could happen due to networks splitting the handshake up
        // so it should be dealt with better, but since the handshake is so
        // small (well below MTU) I suspect that to be rare
        if data.len() < HANDSHAKE_SIZE {
            log::error!("[{socket_addr}] Didn't receive enough data to parse handshake");
            self.disconnect(conn_id, io, state);
            return Err(io::ErrorKind::InvalidData.into());
        }
        let (handshake_data, remainder) = data.split_at(HANDSHAKE_SIZE);
        // Expect this to be the handshake response
        let parsed_handshake = match parse_handshake(*state.info_hash(), handshake_data) {
            Ok(handshake) => handshake,
            Err(err) => {
                log::error!("[{socket_addr}] Failed to parse handshake: {err}");
                self.disconnect(conn_id, io, state);
                return Err(io::ErrorKind::InvalidData.into());
            }
        };

        // Promote the connection in place so it keeps its id
        let entry = self.connections.get_mut(conn_id).unwrap();
        let ConnectionState::Handshaking { socket, .. } =
            std::mem::replace(entry, ConnectionState::Dummy)
        else {
            unreachable!("handshake data received for non-handshaking connection");
        };
        let fd = socket.as_raw_fd();
        *entry = ConnectionState::Established(PeerConnection::new(
            socket,
            socket_addr,
            conn_id,
            parsed_handshake,
        ));
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

        let connection = &mut self[conn_id];
        if connection.extended_extension {
            connection
                .outgoing_msgs_buffer
                .push(extension_handshake_msg(state, state.config));
        }

        // The bitfield is only ever sent as the first message (BEP 3) and for
        // fast_ext peers exactly one of HaveAll/HaveNone/Bitfield must appear
        // immediately after the handshake (BEP 6)
        let bitfield_msg = if connection.fast_ext {
            Some(match state.state() {
                Some(torrent_state) => {
                    let piece_selector = &torrent_state.piece_selector;
                    if piece_selector.completed_all() {
                        peer_protocol::PeerMessage::HaveAll
                    } else if piece_selector.completed_none() {
                        peer_protocol::PeerMessage::HaveNone
                    } else {
                        peer_protocol::PeerMessage::Bitfield(
                            piece_selector.completed_clone().into(),
                        )
                    }
                }
                None => peer_protocol::PeerMessage::HaveNone,
            })
        } else {
            state.state().map(|torrent_state| {
                peer_protocol::PeerMessage::Bitfield(
                    torrent_state.piece_selector.completed_clone().into(),
                )
            })
        };
        if let Some(msg) = bitfield_msg {
            connection.outgoing_msgs_buffer.push(msg);
        }

        // The initial Recv might have contained more data
        // than just the handshake so need to handle that here
        // since the read_buffer will be overwritten by the next
        // incoming recv cqe
        connection.stateful_decoder.append_data(remainder);
        conn_parse_and_handle_msgs(connection, state, &mut io.queued_disk_operations, scope);
        // Recv has been complete, move over to multishot, same user data
        io.recv_multishot(recv_multi_id, fd);
        Ok(())
    }

    /// Tear down a connection regardless of its lifecycle state.
    /// The entry is kept until the close completes and [`ConnectionManager::remove`] is called,
    /// which is what frees the slot for the peer to be reconnected to.
    ///
    /// Returns the peer address if this call initiated the disconnect, and `None`
    /// if the connection is already closing or gone, which may happen when several
    /// completions for the same connection arrive in the same event batch.
    pub(crate) fn disconnect<'state, Q: SubmissionQueue>(
        &mut self,
        conn_id: ConnectionId,
        io: &mut Io<Q>,
        state_ref: &mut StateRef<'state>,
    ) -> Option<SocketAddr> {
        let entry = self.connections.get_mut(conn_id)?;
        Self::disconnect_entry(entry, conn_id, io, state_ref)
    }

    fn disconnect_entry<'state, Q: SubmissionQueue>(
        entry: &mut ConnectionState,
        conn_id: ConnectionId,
        io: &mut Io<Q>,
        state_ref: &mut StateRef<'state>,
    ) -> Option<SocketAddr> {
        let owned_entry = std::mem::replace(entry, ConnectionState::Dummy);
        match owned_entry {
            ConnectionState::Connecting { socket, addr }
            | ConnectionState::Handshaking { socket, addr } => {
                *entry = ConnectionState::Closing { addr };
                io.close_socket(socket, Some(conn_id));
                Some(addr)
            }
            ConnectionState::Established(mut peer) => {
                peer.on_disconnect(state_ref);
                let addr = peer.peer_addr;
                *entry = ConnectionState::Closing { addr };
                // Moves the socket out of the connection, which is dropped here
                io.close_socket(peer.socket, Some(conn_id));
                Some(addr)
            }
            ConnectionState::Closing { .. } => {
                *entry = owned_entry;
                None
            }
            ConnectionState::Dummy => unreachable!("connection in a transient state"),
        }
    }

    /// Tear down every connection that has been marked to be disconnected via `pending_disconnect.
    pub(crate) fn execute_pending_disconnects<'state, Q: SubmissionQueue>(
        &mut self,
        io: &mut Io<Q>,
        state_ref: &mut StateRef<'state>,
    ) {
        for (conn_id, entry) in self.connections.iter_mut() {
            let ConnectionState::Established(peer) = entry else {
                continue;
            };
            let Some(reason) = &peer.pending_disconnect else {
                continue;
            };
            log::warn!("Disconnect: {} reason {reason}", peer.peer_id);
            #[cfg(feature = "metrics")]
            {
                let counter = metrics::counter!("disconnects");
                counter.increment(1);
            }
            Self::disconnect_entry(entry, conn_id, io, state_ref);
        }
    }

    pub(crate) fn disconnect_all<'state, Q: SubmissionQueue>(
        &mut self,
        io: &mut Io<Q>,
        state_ref: &mut StateRef<'state>,
    ) {
        for (conn_id, entry) in self.connections.iter_mut() {
            if let Some(addr) = Self::disconnect_entry(entry, conn_id, io, state_ref) {
                log::info!("[{addr}] Closing connection to peer");
            }
        }
    }

    /// Free the slot of a connection whose socket has finished closing.
    pub fn remove_closed(&mut self, conn_id: ConnectionId) {
        let removed = self
            .connections
            .remove(conn_id)
            .expect("connection removed twice");
        assert!(
            matches!(removed, ConnectionState::Closing { .. }),
            "connection removed without having been disconnected"
        );
    }

    pub(crate) fn established_mut(&mut self, conn_id: ConnectionId) -> Option<&mut PeerConnection> {
        match self.connections.get_mut(conn_id) {
            Some(ConnectionState::Established(peer)) => Some(peer),
            _ => None,
        }
    }

    pub(crate) fn iter_established_mut(
        &mut self,
    ) -> impl Iterator<Item = (ConnectionId, &mut PeerConnection)> {
        self.connections
            .iter_mut()
            .filter_map(|(conn_id, state)| match state {
                ConnectionState::Established(peer) => Some((conn_id, peer)),
                _ => None,
            })
    }

    /// True if any connection has completed its handshake
    pub(crate) fn any_established(&self) -> bool {
        self.connections
            .values()
            .any(|state| matches!(state, ConnectionState::Established(_)))
    }

    /// Number of established connections
    #[cfg(any(test, feature = "metrics"))]
    pub(crate) fn num_established(&self) -> usize {
        self.connections
            .values()
            .filter(|state| matches!(state, ConnectionState::Established(_)))
            .count()
    }

    /// Counts all connections, regardless of state
    pub(crate) fn total_connections(&self) -> usize {
        self.connections.len()
    }

    /// Number of connections still in the connect/handshake phase
    #[cfg(feature = "metrics")]
    pub(crate) fn num_pending(&self) -> usize {
        self.connections
            .values()
            .filter(|state| {
                matches!(
                    state,
                    ConnectionState::Connecting { .. } | ConnectionState::Handshaking { .. }
                )
            })
            .count()
    }

    /// True if no connections are tracked at all, including those whose sockets
    /// are still closing
    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(crate) fn fd(&self, conn_id: ConnectionId) -> Option<RawFd> {
        match self.connections.get(conn_id)? {
            ConnectionState::Connecting { socket, .. }
            | ConnectionState::Handshaking { socket, .. } => Some(socket.as_raw_fd()),
            ConnectionState::Established(peer) => Some(peer.socket.as_raw_fd()),
            ConnectionState::Closing { .. } | ConnectionState::Dummy => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_established_with_key(
        &mut self,
        f: impl FnOnce(ConnectionId) -> PeerConnection,
    ) -> ConnectionId {
        self.connections
            .insert_with_key(|conn_id| ConnectionState::Established(f(conn_id)))
    }

    /// Iterate over all established connections
    #[cfg(test)]
    pub(crate) fn values(&self) -> impl Iterator<Item = &PeerConnection> {
        self.connections.values().filter_map(|state| match state {
            ConnectionState::Established(peer) => Some(peer),
            _ => None,
        })
    }
}

// QOL for accessing established connections
impl Index<ConnectionId> for ConnectionManager {
    type Output = PeerConnection;

    fn index(&self, conn_id: ConnectionId) -> &Self::Output {
        match &self.connections[conn_id] {
            ConnectionState::Established(peer) => peer,
            _ => panic!("connection {conn_id:?} is not established"),
        }
    }
}

impl IndexMut<ConnectionId> for ConnectionManager {
    fn index_mut(&mut self, conn_id: ConnectionId) -> &mut Self::Output {
        match &mut self.connections[conn_id] {
            ConnectionState::Established(peer) => peer,
            _ => panic!("connection {conn_id:?} is not established"),
        }
    }
}
