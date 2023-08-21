use std::sync::{Arc, RwLock};

use atlas_common::error::{Error, ResultWrappedExt};
use atlas_common::ordering::{self, SeqNo};
use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_execution::state::divisible_state::{
    DivisibleState, DivisibleStateDescriptor, PartDescription, PartId, StatePart,
};
use serde::{Deserialize, Serialize};
use sled::{NodeEvent, Serialize as sled_serialize};
use state_orchestrator::StateOrchestrator;
use state_tree::{LeafNode, Node, StateTree};
pub mod state_orchestrator;
pub mod state_tree;

#[derive(Clone, Serialize, Deserialize)]
pub struct SerializedState {
    bytes: Vec<u8>,
    leaf: LeafNode,
}

impl SerializedState {
    pub fn from_node(pid: u64, node: sled::Node, seq: SeqNo) -> Self {
        let bytes = sled_serialize::serialize(&node);
        let mut hasher = blake3::Hasher::new();

        hasher.update(&pid.to_be_bytes());
        hasher.update(bytes.as_slice());

        Self {
            bytes,
            leaf: LeafNode::new(
                seq,
                pid,
                Digest::from_bytes(hasher.finalize().as_bytes()).unwrap(),
            ),
        }
    }

    pub fn to_node(&self) -> sled::Node {
        sled_serialize::deserialize(&mut self.bytes.as_slice()).unwrap()
    }

    pub fn hash(&self) -> Digest {
        let mut hasher = blake3::Hasher::new();

        hasher.update(&self.leaf.pid.to_be_bytes());
        hasher.update(self.bytes.as_slice());

        Digest::from_bytes(hasher.finalize().as_bytes()).unwrap()
    }
}

impl StatePart<StateOrchestrator> for SerializedState {
    fn descriptor(&self) -> &LeafNode {
        &self.leaf
    }

    fn id(&self) -> u64 {
        self.leaf.pid
    }

    fn length(&self) -> usize {
        self.bytes.len()
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    fn hash(&self) -> Digest {
        self.hash()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTree {
    root_digest: Digest,
    seqno: SeqNo,
    // the leaves that make this merke tree, they must be in order.
    leaves: Vec<LeafNode>,
}

impl Default for SerializedTree {
    fn default() -> Self {
        Self {
            root_digest: Digest::blank(),
            seqno: SeqNo::ZERO,
            leaves: Default::default(),
        }
    }
}

impl SerializedTree {
    pub fn new(digest: Digest, seqno: SeqNo, leaves: Vec<LeafNode>) -> Self {
        Self {
            root_digest: digest,
            seqno,
            leaves,
        }
    }

    pub fn from_state(mut state: StateTree) -> Result<Self, ()> {
        state.full_serialized_tree()
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
    pub fn to_serialized_tree(&self, node: Arc<RwLock<Node>>) -> Result<SerializedTree, ()> {
        let node_read = node.read().unwrap();
        let digest = node_read.get_hash();

        let leaf_list = {
            let mut vec = Vec::new();

            for (_,node) in self.leaves.iter() {
                vec.push(node.read().expect("failed to read").get_leaf().clone());
            }
            vec
        };
        Ok(SerializedTree::new(digest, self.seqno, leaf_list))
    }

    pub fn full_serialized_tree(&self) -> Result<SerializedTree, ()> {
        if self.seqno == SeqNo::ZERO {
            return Ok(SerializedTree::new(Digest::blank(), self.seqno, vec![]));
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
                for (index, leaf) in self.leaves.iter().enumerate() {
                    if let Some(other_leaf) = other.leaves.get(index) {
                        if other_leaf.digest != leaf.digest {
                            diff_parts.push(leaf.clone());
                        }
                    } else {
                        diff_parts.push(leaf.clone());
                    }
                }
            } else {
                for (index, other_leaf) in other.leaves.iter().enumerate() {
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

    fn seq_no(&self) -> &SeqNo {
        &self.seqno
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
        match self.get_descriptor_inner() {
            Ok(tree) => tree,
            Err(_) => {
                println!("Cound't get tree");
                SerializedTree::default()
            }
        }
    }

    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> atlas_common::error::Result<()> {
        for part in parts {
            let node = part.to_node();

            if let Err(()) = self.import_page(part.leaf.pid, node) {
                panic!("Failed to import Page");
            }

            self.mk_tree
                .lock()
                .expect("Couldn't aquire lock")
                .insert_leaf(part.leaf.clone());
        }

        self.updates.clear();

        Ok(())
    }

    fn get_parts(
        &mut self,
    ) -> Result<(Vec<SerializedState>, SerializedTree), atlas_common::error::Error> {
        let parts_to_get: Vec<(u64, NodeEvent)> = if let Ok(lock) = self.updates.updates.read() {
            if lock.is_empty() {
                return Ok((vec![], self.get_descriptor()));
            }
            lock.iter().map(|(k, v)| (*k, v.clone())).collect()
        } else {
            return Ok((vec![], self.get_descriptor()));
        };

        let mut state_parts = Vec::new();
        let cur_seq = self
            .mk_tree
            .lock()
            .expect("Couldn't aquire lock")
            .next_seqno();

        for (pid, _) in parts_to_get {
            if let Some(node) = self.get_page(pid) {
                let serialized_part = SerializedState::from_node(pid, node, cur_seq);

                self.mk_tree
                    .lock()
                    .expect("Couldn't aquire lock")
                    .insert_leaf(serialized_part.leaf.clone());

                state_parts.push(serialized_part);
            }
        }

        self.updates.clear();
        self.mk_tree.lock().expect("failed to lock").calculate_tree();

        Ok((state_parts, self.get_descriptor()))
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.mk_tree.lock().expect("failed to get lock").get_seqno())
    }

    fn finalize_transfer(&self) -> atlas_common::error::Result<()> {
        println!("Verifying integrity");

        self.db
            .verify_integrity()
            .wrapped(atlas_common::error::ErrorKind::Error)
    }
}
