use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    time::Duration,
};

use ahash::AHashMap;
use rand::Rng;
use serde_bytes::ByteBuf;
use tokio::sync::oneshot;
use tokio_uring::net::UdpSocket;

use crate::krpc::protocol::KrpcPacket;

use self::{
    error::{Error, KrpcError},
    protocol::{AnnouncePeer, Answer, FindNodes, GetPeers, Ping, Query},
};

pub mod error;
pub mod protocol;

type ConnectionTable = AHashMap<ByteBuf, oneshot::Sender<Result<Answer, KrpcError>>>;
#[derive(Clone, Default, Debug)]
struct InflightRpcs(Rc<RefCell<ConnectionTable>>);

// Ensures Refcell borrow is never held across await point
impl InflightRpcs {
    fn insert_rpc(&self, transaction_id: ByteBuf) -> oneshot::Receiver<Result<Answer, KrpcError>> {
        let mut table = (*self.0).borrow_mut();
        let (tx, rx) = oneshot::channel();
        table.insert(transaction_id, tx);
        rx
    }

    fn exists(&self, transaction_id: &ByteBuf) -> bool {
        let table = (*self.0).borrow();
        table.contains_key(transaction_id)
    }

    fn remove_rpc(
        &self,
        transaction_id: &ByteBuf,
    ) -> Option<oneshot::Sender<Result<Answer, KrpcError>>> {
        let mut table = (*self.0).borrow_mut();
        table.remove(transaction_id)
    }
}

pub trait Rpc {
    type Response;

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket;
}

pub struct QueryBuilder<'a, T> {
    id: ByteBuf,
    timeout: Option<Duration>,
    body: T,
    client: &'a KrpcClient,
}

impl<T: Rpc> QueryBuilder<'_, T> {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub async fn send(self, addr: SocketAddr) -> Result<T::Response, Error>
    where
        T::Response: TryFrom<Answer>,
    {
        let packet: KrpcPacket = self.body.into_packet(self.id);
        let transaction_id = packet.t.clone();
        let encoded = serde_bencoded::to_vec(&packet)?;

        let rx = self
            .client
            .connection_table
            .insert_rpc(transaction_id.clone());

        let (res, _) = self.client.socket.send_to(encoded, addr).await;
        res?;

        let response =
            match tokio::time::timeout(self.timeout.unwrap_or(Duration::from_secs(5)), rx).await {
                Ok(Ok(response)) => response?,
                Err(elapsed) => {
                    self.client.connection_table.remove_rpc(&transaction_id);
                    return Err(Error::Timeout(elapsed));
                }
                // Sender should never be dropped without sending a response
                Ok(Err(_)) => unreachable!(),
            };

        response.try_into().map_err(|_err| {
            self.client.connection_table.remove_rpc(&transaction_id);
            Error::Protocol(KrpcError::protocol(
                "Unexpected response received".to_string(),
            ))
        })
    }
}

#[derive(Clone)]
pub struct KrpcClient {
    socket: Rc<UdpSocket>,
    connection_table: InflightRpcs,
}

impl KrpcClient {
    fn gen_transaction_id(&self) -> ByteBuf {
        // TODO: Ensure we don't block indefinitely by waiting for ids
        // error out early in that case.
        // const MAX_IDS: usize = 26 * 2 + 10;
        // Probably doesn't need to be Alphanumeric?
        use rand::distributions::Alphanumeric;
        let mut rng = rand::thread_rng();
        loop {
            let id = ByteBuf::from([rng.sample(Alphanumeric), rng.sample(Alphanumeric)]);
            if !self.connection_table.exists(&id) {
                return id;
            }
        }
    }

    pub fn ping(&self, body: Ping) -> QueryBuilder<Ping> {
        let transaction_id = self.gen_transaction_id();
        QueryBuilder {
            id: transaction_id,
            timeout: None,
            body,
            client: self,
        }
    }

    pub fn find_nodes(&self, body: FindNodes) -> QueryBuilder<FindNodes> {
        let transaction_id = self.gen_transaction_id();
        QueryBuilder {
            id: transaction_id,
            timeout: None,
            body,
            client: self,
        }
    }

    pub fn get_peers(&self, body: GetPeers) -> QueryBuilder<GetPeers> {
        let transaction_id = self.gen_transaction_id();
        QueryBuilder {
            id: transaction_id,
            timeout: None,
            body,
            client: self,
        }
    }

    pub fn announce_peer(&self, body: AnnouncePeer) -> QueryBuilder<AnnouncePeer> {
        let transaction_id = self.gen_transaction_id();
        QueryBuilder {
            id: transaction_id,
            timeout: None,
            body,
            client: self,
        }
    }
}

pub type Handler = dyn Fn(IpAddr, Query) -> Result<Answer, KrpcError>;

#[derive(Clone)]
pub struct KrpcServer {
    handler: Rc<RefCell<Option<Box<Handler>>>>,
}

impl KrpcServer {
    pub fn serve(
        &self,
        new_handler: impl Fn(IpAddr, Query) -> Result<Answer, KrpcError> + 'static,
    ) {
        self.handler.replace(Some(Box::new(new_handler)));
    }
}

pub async fn setup_krpc(bind_addr: SocketAddr) -> Result<(KrpcClient, KrpcServer), Error> {
    let socket = Rc::new(UdpSocket::bind(bind_addr).await?);
    let connection_table = InflightRpcs::default();

    let client = KrpcClient {
        socket: socket.clone(),
        connection_table: connection_table.clone(),
    };

    let server = KrpcServer {
        handler: Rc::new(RefCell::new(None)),
    };

    let server_clone = server.clone();
    tokio_uring::spawn(async move {
        let mut recv_buffer = vec![0; 2048];
        loop {
            let (read, buf) = socket.recv_from(std::mem::take(&mut recv_buffer)).await;
            // TODO cancellation and addr should be sent across with packet
            let (recv, addr) = match read {
                Ok(recv_addr) => recv_addr,
                Err(err) => {
                    log::error!("Failed to read from socket: {err}");
                    continue;
                }
            };
            let packet: KrpcPacket = match serde_bencoded::from_bytes(&buf[..recv]) {
                Ok(packet) => packet,
                Err(err) => {
                    log::warn!(
                        "Failed parsing KRPC packet: {err}, packet: {:?}",
                        &buf[..recv]
                    );
                    recv_buffer = buf;
                    continue;
                }
            };
            recv_buffer = buf;

            async fn respond(pkt: KrpcPacket, addr: SocketAddr, socket: &UdpSocket) {
                let Ok(encoded) = serde_bencoded::to_vec(&pkt) else {
                    log::error!("Failed to bencode: {pkt:?}");
                    return;
                };
                let (res, _) = socket.send_to(encoded, addr).await;
                if let Err(err) = res {
                    log::error!("Failed to send response: {err}");
                }
            }

            match packet.y {
                // response
                'r' => {
                    if let Some(response) = packet.r {
                        if let Some(response_sender) = connection_table.remove_rpc(&packet.t) {
                            let _ = response_sender.send(Ok(response));
                        } else {
                            log::warn!("Unknown KRPC response recived, transaction id: {:?} not found in connection_table", packet.t);
                        }
                    } else {
                        log::warn!("Invalid KRPC message received, missing response");
                        respond(
                            KrpcError::protocol("Invalid message".to_string())
                                .into_packet(packet.t),
                            addr,
                            &socket,
                        )
                        .await;
                    }
                }
                // query
                'q' => match (packet.q.as_deref(), packet.a) {
                    (Some("get_peers"), Some(query @ Query::GetPeers { .. }))
                    | (Some("announce_peer"), Some(query @ Query::AnnouncePeer { .. }))
                    | (Some("ping"), Some(query @ Query::Ping { .. }))
                    | (Some("find_node"), Some(query @ Query::FindNode { .. })) => {
                        let handler = server_clone.handler.borrow();
                        if let Some(handler) = handler.as_ref() {
                            log::debug!("Handling incoming query");
                            // TODO Spawn separate task per query?
                            match handler(addr.ip(), query) {
                                Ok(answer) => {
                                    let response_pkt = KrpcPacket {
                                        t: packet.t,
                                        y: 'r',
                                        q: None,
                                        a: None,
                                        r: Some(answer),
                                        e: None,
                                    };
                                    respond(response_pkt, addr, &socket).await;
                                }
                                Err(err) => {
                                    let response_pkt = KrpcPacket {
                                        t: packet.t,
                                        y: 'e',
                                        q: None,
                                        a: None,
                                        r: None,
                                        e: Some(err),
                                    };
                                    respond(response_pkt, addr, &socket).await;
                                }
                            }
                        } else {
                            log::warn!("No krpc handler setup, ignoring incoming query");
                        }
                    }
                    _ => {
                        log::warn!("Unknown query: {}", packet.q.as_deref().unwrap_or("<none>"));
                        respond(
                            KrpcError::method_unknown("unknown query".to_string())
                                .into_packet(packet.t),
                            addr,
                            &socket,
                        )
                        .await;
                    }
                },
                // error
                'e' => {
                    if let Some(error) = packet.e {
                        if let Some(response_sender) = connection_table.remove_rpc(&packet.t) {
                            let _ = response_sender.send(Err(error));
                        } else {
                            log::warn!("Unknown KRPC response recived, transaction id: {:?} not found in connection_table", packet.t);
                        }
                    } else {
                        log::warn!("Invalid KRPC message received, missing error");
                        respond(
                            KrpcError::protocol("Invalid message".to_string())
                                .into_packet(packet.t),
                            addr,
                            &socket,
                        )
                        .await;
                    }
                }
                _ => {
                    log::warn!("Invalid KRPC message received, y: {}", packet.y);
                    respond(
                        KrpcError::protocol("Invalid message".to_string()).into_packet(packet.t),
                        addr,
                        &socket,
                    )
                    .await;
                }
            }
        }
    });
    Ok((client, server))
}
