mod conn_establish;
pub mod epoll_group;

use std::net::Shutdown;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use intmap::IntMap;
use log::{debug, error, info, warn};
use mio::{Token, Waker};
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotRx, TryRecvError};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::socket::{MioSocket, SecureSocket, SecureSocketSync, SyncListener};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, WireMessage};
use crate::mio_tcp::connections::conn_establish::ConnectionHandler;
use crate::mio_tcp::connections::epoll_group::{EpollWorkerGroupHandle, EpollWorkerId, NewConnection};
use crate::NodeConnections;
use crate::serialize::Serializable;
use crate::tcpip::connections::{Callback, ConnCounts};
use crate::tcpip::PeerAddr;

pub type NetworkSerializedMessage = (WireMessage);

pub const SEND_QUEUE_SIZE: usize = 1024;

pub struct Connections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The map of registered connections
    registered_connections: DashMap<NodeId, Arc<PeerConnection<M>>>,
    // A map of addresses to our known peers
    address_map: IntMap<PeerAddr>,
    // A reference to the worker group that handles the epoll workers
    worker_group: EpollWorkerGroupHandle<M>,
    // A reference to the client pooling
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
    // Connection counts
    conn_counts: ConnCounts,
    // Handle establishing new connections
    conn_handler: Arc<ConnectionHandler>,
}

/// Structure that is responsible for handling all connections to a given peer
pub struct PeerConnection<M: Serializable + 'static> {
    // A reference to the main connection structure
    connection: Arc<Connections<M>>,
    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    // A thread-safe counter for generating connection ids
    conn_id_generator: AtomicU32,
    //The map connecting each connection to a token in the MIO Workers
    connections: SkipMap<u32, Option<ConnHandle>>,
    // Sending messages to the connections
    to_send: (ChannelSyncTx<NetworkSerializedMessage>, ChannelSyncRx<NetworkSerializedMessage>),
}

#[derive(Clone)]
pub struct ConnHandle {
    id: u32,
    my_id: NodeId,
    peer_id: NodeId,
    epoll_worker_id: EpollWorkerId,
    token: Token,
    waker: Arc<Waker>,
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl<M> NodeConnections for Connections<M> where M: Serializable + 'static {
    fn is_connected_to_node(&self, node: &NodeId) -> bool {
        self.registered_connections.contains_key(node)
    }

    fn connected_nodes_count(&self) -> usize {
        self.registered_connections.len()
    }

    fn connected_nodes(&self) -> Vec<NodeId> {
        self.registered_connections.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Attempt to connect to a given node
    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<atlas_common::error::Result<()>>> {
        let addr = self.address_map.get(node.into());

        if addr.is_none() {
            todo!()
        }

        let addr = addr.unwrap();

        let current_connections = self.registered_connections.get(&node)
            .map(|entry| {
                entry.value().concurrent_connection_count()
            }).unwrap_or(0);

        let connections = self.conn_counts.get_connections_to_node(self.id, node, self.first_cli);

        let connections = if current_connections > connections { 0 } else { connections - current_connections };

        let mut result_vec = Vec::with_capacity(connections);

        for _ in 0..connections {
            result_vec.push(self.conn_handler.connect_to_node(Arc::clone(self), node, addr.clone()))
        }

        result_vec
    }

    async fn disconnect_from_node(&self, node: &NodeId) -> atlas_common::error::Result<()> {
        let existing_connection = self.registered_connections.remove(node);

        if let Some((node, connection)) = existing_connection {
            for entry in connection.connections.iter() {
                if let Some(conn) = entry.value() {
                    let worker_id = conn.epoll_worker_id;
                    let conn_token = conn.token;

                    self.worker_group.disconnect_connection_from_worker(worker_id, conn_token)?;
                }
            }
        }

        Ok(())
    }
}

impl<M> Connections<M> where M: Serializable + 'static {
    pub(super) fn initialize_connections(id: NodeId, first_cli: NodeId, addr_map: IntMap<PeerAddr>, group_handle: EpollWorkerGroupHandle<M>,
                                         conn_counts: ConnCounts, client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>) -> Result<Self> {
        let conn_handler = Arc::new(ConnectionHandler::initialize(id.clone(), first_cli.clone(), conn_counts.clone()));

        Ok(Self {
            id,
            first_cli,
            registered_connections: Default::default(),
            address_map: addr_map,
            worker_group: group_handle,
            client_pooling,
            conn_counts,
            conn_handler,
        })
    }

    pub(super) fn setup_tcp_server_worker(self: &Arc<Self>, listener: SyncListener) {
        conn_establish::initialize_server(self.id.clone(), self.first_cli.clone(), listener, self.conn_handler.clone(), Arc::clone(self))
    }

    /// Get the connection to a given node
    pub fn get_connection(&self, node: &NodeId) -> Option<Arc<PeerConnection<M>>> {
        let option = self.registered_connections.get(node);

        option.map(|conn| conn.value().clone())
    }

    fn handle_connection_established(self: &Arc<Self>, node: NodeId, socket: SecureSocket) {
        let socket = match socket {
            SecureSocket::Sync(sync) => {
                match sync {
                    SecureSocketSync::Plain(socket) => {
                        socket
                    }
                    SecureSocketSync::Tls(tls, socket) => {
                        socket
                    }
                }
            }
            _ => unreachable!()
        };

        self.handle_connection_established_with_socket(node, socket.into());
    }

    fn handle_connection_established_with_socket(self: &Arc<Self>, node: NodeId, socket: MioSocket) {
        info!("{:?} // Handling established connection to {:?}", self.id, node);

        let option = self.registered_connections.entry(node);

        let peer_conn = option.or_insert_with(||
            {
                let con = Arc::new(PeerConnection::new(Arc::clone(self), self.client_pooling.init_peer_conn(node)));

                debug!("{:?} // Creating new peer connection to {:?}. {:?}", self.id, node,
                    con.client_pool_peer().client_id());

                con
            });

        let concurrency_level = self.conn_counts.get_connections_to_node(self.id, node, self.first_cli);

        let conn_id = peer_conn.gen_conn_id();

        let current_connections = peer_conn.concurrent_connection_count();

        //FIXME: Fix the fact that we are closing the previous connection when we don't actually need to
        // So now we have to multiply the limit because of this
        if current_connections + 1 > concurrency_level * 2 {
            // We have too many connections to this node. We need to close this one.
            warn!("{:?} // Too many connections to {:?}. Closing connection {:?}. Connection count {} vs max {}", self.id, node, conn_id,
            current_connections, concurrency_level);

            if let Err(err) = socket.shutdown(Shutdown::Both) {
                error!("{:?} // Failed to shutdown socket {:?} to {:?}. Error: {:?}", self.id, conn_id, node, err);
            }

            return;
        }

        debug!("{:?} // Registering connection {:?} to {:?}", self.id, conn_id, node);

        //FIXME: This isn't really an atomic operation but I also don't care LOL.
        peer_conn.register_peer_conn_intent(conn_id);

        let conn_details = NewConnection::new(conn_id, node, self.id,
                                              socket, peer_conn.value().clone());

        // We don't register the connection here as we still need some information that will only be provided
        // to us by the worker that will handle the connection.
        // Therefore, the connection will be registered in the worker itself.
        self.worker_group.assign_socket_to_worker(conn_details).expect("Failed to assign socket to worker?");
    }

    /// Handle a connection having broken and being removed from the worker
    fn handle_connection_failed(self: &Arc<Self>, node: NodeId, conn_id: u32) {
        info!("{:?} // Handling failed connection to {:?}. Conn: {:?}", self.id, node, conn_id);

        let connection = if let Some(conn) = self.registered_connections.get(&node) {
            conn.value().clone()
        } else {
            return;
        };

        connection.delete_connection(conn_id);

        if connection.concurrent_connection_count() == 0 {
            self.registered_connections.remove(&node);

            let _ = self.connect_to_node(node);
        }
    }
}

impl<M> PeerConnection<M> where M: Serializable + 'static {
    fn new(connections: Arc<Connections<M>>, client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Self {
        let to_send = channel::new_bounded_sync(SEND_QUEUE_SIZE);

        Self {
            connection: connections,
            client,
            conn_id_generator: AtomicU32::new(0),
            connections: Default::default(),
            to_send,
        }
    }

    /// Get a unique ID for a connection
    fn gen_conn_id(&self) -> u32 {
        self.conn_id_generator.fetch_add(1, Ordering::Relaxed)
    }

    /// Register an active connection into this connection map
    fn register_peer_conn(&self, conn: ConnHandle) {
        self.connections.insert(conn.id, Some(conn));
    }

    // Register an intent of registering this connection
    fn register_peer_conn_intent(&self, id: u32) {
        self.connections.insert(id, None);
    }

    /// Get the amount of concurrent connections we currently have to this peer
    fn concurrent_connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Send the peer a given message
    pub(crate) fn peer_message(&self, msg: WireMessage, callback: Callback) -> Result<()> {
        let from = msg.header().from();
        let to = msg.header().to();

        if let Err(_) = self.to_send.0.send(msg) {
            error!("{:?} // Failed to send peer message to {:?}", from, to);

            return Err(Error::simple(ErrorKind::Communication));
        }

        for conn_ref in self.connections.iter() {
            let conn = conn_ref.value();

            if let Some(conn) = conn {
                conn.waker.wake().expect("Failed to wake connection");
            }
        }

        Ok(())
    }

    /// Take a message from the send queue (blocking)
    fn take_from_to_send(&self) -> Result<NetworkSerializedMessage> {
        self.to_send.1.recv().wrapped(ErrorKind::CommunicationChannel)
    }

    /// Attempt to take a message from the send queue (non blocking)
    fn try_take_from_send(&self) -> Result<Option<NetworkSerializedMessage>> {
        match self.to_send.1.try_recv() {
            Ok(msg) => {
                Ok(Some(msg))
            }
            Err(err) => {
                match err {
                    TryRecvError::ChannelDc => {
                        Err(Error::simple(ErrorKind::CommunicationChannel))
                    }
                    TryRecvError::ChannelEmpty | TryRecvError::Timeout => {
                        Ok(None)
                    }
                }
            }
        }
    }

    fn delete_connection(&self, conn_id: u32) {
        self.connections.remove(&conn_id);
    }

    pub fn client_pool_peer(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        &self.client
    }
}


impl ConnHandle {
    pub fn new(id: u32, my_id: NodeId, peer_id: NodeId,
               epoll_worker: EpollWorkerId,
               conn_token: Token,
               waker: Arc<Waker>) -> Self {
        Self {
            id,
            my_id,
            peer_id,
            epoll_worker_id: epoll_worker,
            cancelled: Arc::new(AtomicBool::new(false)),
            waker,
            token: conn_token,
        }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub fn my_id(&self) -> NodeId {
        self.my_id
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    #[inline]
    pub fn cancelled(&self) -> &Arc<AtomicBool> {
        &self.cancelled
    }
}