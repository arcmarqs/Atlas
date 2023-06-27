use std::sync::{RwLock, Arc};

use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_common::ordering;
use atlas_execution::state::divisible_state::{StatePart, DivisibleState, PartId, DivisibleStateDescriptor};
use serde::{Serialize, Deserialize};
use sled::Serialize as sled_serialize;
use state_orchestrator::StateOrchestrator;
use state_tree::{StateTree, Node, LeafNode};

pub mod state_orchestrator;
pub mod state_tree;

#[derive(Clone,Serialize,Deserialize)]
pub struct SerializedState {
    pid: u64,
    bytes: Vec<u8>
}


impl SerializedState{
    pub fn from_node(pid: u64,node: sled::Node) -> Self {
        let size = sled_serialize::serialized_size(&node);
        let mut buf : Vec<u8> = Vec::new();
        sled_serialize::serialize_into(&node,&mut buf.as_mut_slice());
        Self{
            pid,
            bytes: buf,
        }
    }

    pub fn to_node(&self) -> sled::Node {
        sled_serialize::deserialize(&mut self.bytes.as_slice()).unwrap()
    }
}

impl <S>StatePart<S> for SerializedState where S: DivisibleState + ?Sized {
    fn descriptor(&self) -> S::PartDescription {
        
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTree {
    root_digest: Digest,

    // the leaves that make this merke tree, they must be in order.
    leaves: Vec<LeafNode>,
}

impl SerializedTree {
    pub fn new(digest: Digest, leaves: Vec<LeafNode>) -> Self {
        Self {
            root_digest: digest,
            leaves,
        }
    }
}

impl PartialEq for SerializedTree {
    fn eq(&self, other: &Self) -> bool {
        self.root_digest == other.root_digest
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl Orderable for SerializedTree {
    fn sequence_number(&self) -> ordering::SeqNo {
        todo!()
    }
}

impl StateTree {
    pub fn to_serialized_tree(&self, node: Arc<RwLock<Node>>) -> Result<SerializedTree, ()>{
        let node_read = node.read().unwrap();
        let digest = node_read.get_hash();
        let pids = node_read.get_pids_concat();

        let leaf_list = {
            let mut vec = Vec::new();

            for pid in pids{
                vec.push(self.get_leaf(pid));
            }
            vec
        };
        Ok(SerializedTree::new(digest,leaf_list))

    }

    pub fn full_serialized_tree(&self) -> Result<SerializedTree, ()> {
        let root = self.bag_peaks();
        if let Some(root) = root {
            self.to_serialized_tree(root)
        } else {
            Err(())
        }
    }
}


impl <S>DivisibleStateDescriptor<S> for SerializedTree where S: DivisibleState + ?Sized{
    fn parts(&self) -> &Vec<S::PartDescription> {
    }

    fn compare_descriptors(&self, other: &Self) -> Vec<S::PartDescription> {
        todo!()
    }
}

impl PartId for LeafNode {
    fn content_description(&self) -> Digest {
        self.get_digest()
    }
}

impl DivisibleState for StateOrchestrator {

    type PartDescription = LeafNode;

    type StateDescriptor = SerializedTree;

    type StatePart = SerializedState;


    fn get_descriptor(&self) -> &Self::StateDescriptor {
        self.orchestrator.
    }

    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> atlas_common::error::Result<()> {
        todo!()
    }

    fn prepare_checkpoint(&mut self) -> atlas_common::error::Result<&Self::StateDescriptor> {
        todo!()
    }

    fn get_parts(&self, parts: &Vec<Self::PartDescription>) -> atlas_common::error::Result<Vec<Self::StatePart>> {
        todo!()
    }

}