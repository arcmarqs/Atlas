use intmap::IntMap;
use rustls::{ClientConfig, ServerConfig};
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::node_id::NodeId;
use crate::tcpip::PeerAddr;

/// Configuration needed for a mio server
pub struct MioConfig {
    // The general config of a node.
    pub node_config: NodeConfig,
    // How many workers should our mio server have
    pub worker_count: usize,
}

/// The configuration of the network node
pub struct NodeConfig {
    /// The id of this `Node`.
    pub id: NodeId,
    /// The ID of the first client in the network
    /// Every peer with id < first_cli is a replica and every peer with id > first_cli is
    /// a client
    pub first_cli: NodeId,
    /// TCP specific configuration
    pub tcp_config: TcpConfig,
    ///The configurations of the client pool config
    pub client_pool_config: ClientPoolConfig,
    ///The configurations from the crypto part of the system
    pub pk_crypto_config: PKConfig
}

pub struct TcpConfig {
    /// The addresses of all nodes in the system (including clients),
    /// as well as the domain name associated with each address.
    ///
    /// For any `NodeConfig` assigned to `c`, the IP address of
    /// `c.addrs[&c.id]` should be equivalent to `localhost`.
    pub addrs: IntMap<PeerAddr>,
    /// Configurations specific to the networking
    pub network_config: TlsConfig,

    /// How many concurrent connections should be established between replica nodes of the system
    pub replica_concurrent_connections: usize,
    /// How many client concurrent connections should be established between replica <-> client connections
    pub client_concurrent_connections: usize
}

pub struct PKConfig {
    /// Our secret key pair.
    pub sk: KeyPair,
    /// The list of public keys of all nodes in the system.
    pub pk: IntMap<PublicKey>,
}

pub struct TlsConfig {
    /// The TLS configuration used to connect to replica nodes. (from client nodes)
    pub async_client_config: ClientConfig,
    /// The TLS configuration used to accept connections from client nodes.
    pub async_server_config: ServerConfig,
    ///The TLS configuration used to accept connections from replica nodes (Synchronously)
    pub sync_server_config: ServerConfig,
    ///The TLS configuration used to connect to replica nodes (from replica nodes) (Synchronousy)
    pub sync_client_config: ClientConfig,
}

pub struct ClientPoolConfig {
    ///The max size for batches of client operations
    pub batch_size: usize,
    ///How many clients should be placed in a single collecting pool (seen in incoming_peer_handling)
    pub clients_per_pool: usize,
    ///The timeout for batch collection in each client pool.
    /// (The first to reach between batch size and timeout)
    pub batch_timeout_micros: u64,
    ///How long should a client pool sleep for before attempting to collect requests again
    /// (It actually will sleep between 3/4 and 5/4 of this value, to make sure they don't all sleep / wake up at the same time)
    pub batch_sleep_micros: u64,
}