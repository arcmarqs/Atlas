use std::sync::{RwLock, Arc};


use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_common::ordering::{self, SeqNo};
use atlas_execution::state::divisible_state::{StatePart, DivisibleState, PartId, DivisibleStateDescriptor, PartDescription};
use serde::{Serialize, Deserialize};
use sled::{Serialize as sled_serialize};
use state_orchestrator::{StateOrchestrator, StateDescriptor};
use state_tree::{StateTree, Node, LeafNode};

pub mod state_orchestrator;
pub mod state_tree;

#[derive(Clone,Serialize,Deserialize)]
pub struct SerializedState {
    pid: u64,
    bytes: Vec<u8>,
    leaf: LeafNode
}

impl SerializedState{
    pub fn from_node(pid: u64,node: sled::Node, leaf: LeafNode) -> Self {
        Self{
            pid,
            bytes: sled_serialize::serialize(&node),
            leaf,

        }
    }

    pub fn to_node(&self) -> sled::Node {
        sled_serialize::deserialize(&mut self.bytes.as_slice()).unwrap()
    }

}

impl StatePart<StateOrchestrator> for SerializedState {
    fn descriptor(&self) -> &LeafNode {
        &self.leaf
    }

    fn id(&self) -> u64 {
        self.pid
    }

    fn length(&self) -> usize {
        self.bytes.len()
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTree {
    root_digest: Digest,
    seqno: SeqNo,
    // the leaves that make this merke tree, they must be in order.
    leaves: Vec<LeafNode>,
}

impl SerializedTree {
    pub fn new(digest: Digest, seqno: SeqNo, leaves: Vec<LeafNode>) -> Self {
        Self {
            root_digest: digest,
            seqno,
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
        self.seqno
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
        Ok(SerializedTree::new(digest,self.seqno,leaf_list))

    }

    pub fn full_serialized_tree(&self) -> Result<SerializedTree, ()> {
        println!("seqno is: {:?}", self.seqno);
        if self.seqno == SeqNo::ONE {
           return Ok(SerializedTree::new(Digest::blank(),self.seqno,vec![]));
        }

        let root = self.bag_peaks();
        if let Some(root) = root {
            self.to_serialized_tree(root)
        } else {
            Err(())
        }
    }
}

impl DivisibleStateDescriptor<StateOrchestrator> for SerializedTree {
    fn parts(&self) -> &Vec<LeafNode> {
       &self.leaves
    }

    fn get_digest(&self) -> &Digest {
        &self.root_digest
    }

    // compare state descriptors and return different parts
    fn compare_descriptors(&self, other: &Self) -> Vec<LeafNode> {
        let mut diff_parts = Vec::new();
        if self.root_digest != other.root_digest {
            if self.seqno >= other.seqno {
                for (index,leaf) in self.leaves.iter().enumerate() {
                    if let Some(other_leaf) = other.leaves.get(index) {
                        if other_leaf.digest != leaf.digest {
                            diff_parts.push(leaf.clone());
                        }
                    } else {
                        diff_parts.push(leaf.clone());
                    }
                }
            } else {
                for (index,other_leaf) in other.leaves.iter().enumerate() {
                    if let Some(leaf) = self.leaves.get(index) {
                        if other_leaf.digest != leaf.digest {
                            diff_parts.push(other_leaf.clone());
                        }
                    } else {
                        diff_parts.push(other_leaf.clone());
                    }
                }
            }
            
        }

        diff_parts
    }
}

impl PartId for LeafNode {
    fn content_description(&self) -> Digest {
        self.get_digest()
    }
}

impl PartDescription for LeafNode {
    fn id(&self) -> &u64 {
        &self.pid
    }
}

impl DivisibleState for StateOrchestrator {
    type PartDescription = LeafNode;

    type StateDescriptor = SerializedTree;

    type StatePart = SerializedState;

    fn get_descriptor(&self) -> Self::StateDescriptor {
        self.get_descriptor().unwrap()
    }

    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> atlas_common::error::Result<()> {
        for part in parts {
            if let Err(()) = self.import_page(part.pid, part.to_node()) {
                panic!("Failed to import Page");
            }
        }

        Ok(())
    }

    fn prepare_checkpoint(&mut self) -> atlas_common::error::Result<Self::StateDescriptor> {
        // need to see how to handle snapshots of the state with sled'
        if let Ok(descriptor) = self.get_descriptor() {
            Ok(descriptor.clone())
        } else {
            Err(atlas_common::error::Error::simple(atlas_common::error::ErrorKind::Persistentdb))
        }
    }

    fn get_parts(
        &self,
        parts: &Vec<Self::PartDescription>,
    ) -> atlas_common::error::Result<Vec<Self::StatePart>> {
        let mut state_parts = Vec::new();
        for leaf in parts {
            if let Some(node) = self.get_page(leaf.pid) {
                let serialized_part = SerializedState::from_node(leaf.pid, node,leaf.clone());

                state_parts.push(serialized_part);
            }
        }

        Ok(state_parts)
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.descriptor.get_seqno())
    }
}
