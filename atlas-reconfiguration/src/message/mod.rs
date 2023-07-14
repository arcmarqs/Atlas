use std::fmt::Debug;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;

use atlas_common::peer_addr::PeerAddr;
#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use atlas_common::crypto::hash::Digest;
use atlas_common::crypto::signature::Signature;
use atlas_communication::serialize::Serializable;
use atlas_core::serialize::ReconfigurationProtocolMessage;
use atlas_core::timeouts::{RqTimeout, TimeoutKind};

use crate::{QuorumView};
use crate::network_reconfig::KnownNodes;


/// Used to request to join the current quorum
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumEnterRequest {
    node_triple: NodeTriple,
}

/// When a node makes request to join a given network view, the participating nodes
/// must respond with a QuorumNodeJoinResponse.
/// TODO: Decide how many responses we need in order to consider it a valid join request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumNodeJoinApproval {
    network_view_seq: SeqNo,

    requesting_node: NodeId,
    origin_node: NodeId,
}

/// A certificate composed of enough QuorumNodeJoinResponses to consider that enough
/// existing quorum nodes have accepted the new node
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumJoinCertificate {
    network_view_seq: SeqNo,
    approvals: Vec<QuorumNodeJoinApproval>,
}

/// Reason message for the rejection of quorum entering request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumEnterRejectionReason {
    NotAuthorized,
    MissingValues,
    IncorrectNetworkViewSeq,
    NodeIsNotQuorumParticipant,
}

/// A response to a network join request
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumEnterResponse {
    Successful(QuorumNodeJoinApproval),

    Rejected(QuorumEnterRejectionReason),
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumLeaveRequest {
    node_triple: NodeTriple,
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumLeaveResponse {
    network_view_seq: SeqNo,

    requesting_node: NodeId,
    origin_node: NodeId,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct KnownNodesMessage {
    nodes: Vec<NodeTriple>,
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct NodeTriple {
    node_id: NodeId,
    addr: PeerAddr,
    pub_key: Vec<u8>,
}

/// The response to the request to join the network
/// Returns the list of known nodes in the network, including the newly added node
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkJoinResponseMessage {
    Successful(KnownNodesMessage),
    Rejected(NetworkJoinRejectionReason),
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkJoinRejectionReason {
    NotAuthorized,
    MissingValues,
    IncorrectSignature,
    // Clients don't need to connect to other clients, for example, so it is not necessary
    // For them to know about each other
    NotNecessary,
}

/// Reconfiguration message type
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ReconfigurationMessage {
    NetworkReconfig(NetworkReconfigMessage),
    QuorumReconfig(QuorumReconfigMessage),
}

/// Network reconfiguration message
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct NetworkReconfigMessage {
    seq: SeqNo,
    message_type: NetworkReconfigMsgType
}

/// Network reconfiguration messages (Related only to the network view)
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum NetworkReconfigMsgType {
    NetworkJoinRequest(NodeTriple),
    NetworkJoinResponse(NetworkJoinResponseMessage),
    NetworkHelloRequest(NodeTriple),
}

/// A certificate that a given node sent a quorum view
#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct QuorumViewCert {
    /// The quorum view that was sent
    quorum_view: QuorumView,
    /// The digest of the quorum view
    digest: Digest,
    /// The node that sent the quorum view
    sender: NodeId,
    /// The signature of the quorum view (digest) by the node id
    signature: Signature
}

#[derive(Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum QuorumReconfigMessage {
    /// A state request for the current network view
    NetworkViewStateRequest,
    /// The response to the state request
    NetworkViewState(QuorumViewCert),
    /// A request to join the current quorum
    QuorumEnterRequest(QuorumEnterRequest),
    /// The response to the request to join the quorum, 2f+1 responses
    /// are required to consider the request successful and allow for this
    /// to be passed to the ordering protocol as a QuorumJoinCertificate
    QuorumEnterResponse(QuorumEnterResponse),
    /// A message to indicate that a node has entered the quorum
    QuorumUpdated(QuorumViewCert),
    /// A request to leave the current quorum
    QuorumLeaveRequest(QuorumLeaveRequest),
    /// The response to the request to leave the quorum
    QuorumLeaveResponse(QuorumLeaveResponse),
}

/// Messages that will be sent via channel to the reconfiguration module
pub enum ReconfigMessage {
    TimeoutReceived(Vec<RqTimeout>)
}

impl QuorumNodeJoinApproval {
    pub fn new(network_view_seq: SeqNo, requesting_node: NodeId, origin_node: NodeId) -> Self {
        Self { network_view_seq, requesting_node, origin_node }
    }
}

impl NetworkReconfigMessage {
    pub fn new(seq: SeqNo, message_type: NetworkReconfigMsgType) -> Self {
        Self { seq, message_type }
    }

    pub fn into_inner(self) -> (SeqNo, NetworkReconfigMsgType) {
        (self.seq, self.message_type)
    }
}

impl NodeTriple {
    pub fn new(node_id: NodeId, public_key: Vec<u8>, address: PeerAddr) -> Self {
        Self {
            node_id,
            addr: address,
            pub_key: public_key,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn public_key(&self) -> &Vec<u8> {
        &self.pub_key
    }

    pub fn addr(&self) -> &PeerAddr {
        &self.addr
    }
}

impl From<&KnownNodes> for KnownNodesMessage {
    fn from(value: &KnownNodes) -> Self {
        let mut known_nodes = Vec::with_capacity(value.node_keys.len());

        for (node_id, public_key) in value.node_keys() {
            known_nodes.push(NodeTriple {
                node_id: *node_id,
                pub_key: public_key.pk_bytes().to_vec(),
                addr: value.node_addrs.get(node_id).unwrap().clone(),
            });
        }

        KnownNodesMessage {
            nodes: known_nodes,
        }
    }
}

impl KnownNodesMessage {
    pub fn known_nodes(&self) -> &Vec<NodeTriple> {
        &self.nodes
    }

    pub fn into_nodes(self) -> Vec<NodeTriple> {
        self.nodes
    }
}

impl Debug for NodeTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeTriple {{ node_id: {:?}, addr: {:?}}}", self.node_id, self.addr)
    }
}

pub struct ReconfData;

impl Serializable for ReconfData {
    type Message = ReconfigurationMessage;

    //TODO: Implement capnproto messages
}

impl ReconfigurationProtocolMessage for ReconfData {
    type QuorumJoinCertificate = QuorumJoinCertificate;
}

impl QuorumEnterRequest {
    pub fn new(node_triple: NodeTriple) -> Self {
        Self { node_triple }
    }
}

impl QuorumViewCert {
    
    pub fn quorum_view(&self) -> &QuorumView {
        &self.quorum_view
    }
    pub fn digest(&self) -> Digest {
        self.digest
    }
    pub fn sender(&self) -> NodeId {
        self.sender
    }
    pub fn signature(&self) -> Signature {
        self.signature
    }
}