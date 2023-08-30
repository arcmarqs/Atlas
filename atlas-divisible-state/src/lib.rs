use std::sync::{Arc, RwLock};
use std::time::Instant;

use atlas_common::error::ResultWrappedExt;
use atlas_common::ordering::{self, SeqNo};
use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_execution::state::divisible_state::{
    DivisibleState, DivisibleStateDescriptor, PartDescription, PartId, StatePart,
};
use atlas_metrics::metrics::metric_duration;
use serde::{Deserialize, Serialize};
use sled::{NodeEvent, Serialize as sled_serialize};
use state_orchestrator::StateOrchestrator;
use state_tree::{LeafNode, Node, StateTree};
use crate::metrics::CREATE_CHECKPOINT_TIME_ID;

pub mod state_orchestrator;
pub mod state_tree;

pub mod metrics;

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

    pub fn from_state(state: StateTree) -> Result<Self, ()> {
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

            for (_, node) in self.leaves.iter() {
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
        let mut lock = self.mk_tree.lock().expect("Couldn't aquire lock");

        for part in parts {
            let node = part.to_node();

            if let Err(()) = self.import_page(part.leaf.pid, node) {
                panic!("Failed to import Page");
            }

            lock.insert_leaf(part.leaf.clone());
        }

        self.db.flush();

        Ok(())
    }

    fn get_parts(
        &mut self,
    ) -> Result<(Vec<SerializedState>, SerializedTree), atlas_common::error::Error> {
        let checkpoint_start = Instant::now();
        let _ = self.db.flush();
     /*    let parts_to_get: Vec<u64> = if let Ok(mut lock) = self.updates.updates.write() {
            if lock.is_empty() {
                return Ok((vec![], self.get_descriptor()));
            }
            
            let ret = lock.iter().map(|k| *k).collect();

            lock.clear();
            ret
        } else {
            return Ok((vec![], self.get_descriptor()));
        };*/

        let mut parts_to_get = self.updates.write().expect("failed to aquire lock");
        println!("updated: {:?}", parts_to_get.len());
        let mut state_parts = Vec::new();

        if !parts_to_get.is_empty() {
            let mut tree_lock = self.mk_tree.lock().expect("Couldn't aquire lock");
            let cur_seq = tree_lock.next_seqno();

            for pid in parts_to_get.drain() {

                if let Some(node) = self.get_page(pid) {
                    let serialized_part = SerializedState::from_node(pid, node, cur_seq);
                    tree_lock.insert_leaf(serialized_part.leaf.clone());
                    state_parts.push(serialized_part);
                }
            }
            
            tree_lock.calculate_tree();
            drop(tree_lock);
        }

        drop(parts_to_get);

        println!("checkpoint finished {:?}", checkpoint_start.elapsed());

        metric_duration(CREATE_CHECKPOINT_TIME_ID, checkpoint_start.elapsed());
        Ok((state_parts, self.get_descriptor()))
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.mk_tree.lock().expect("failed to get lock").get_seqno())
    }

    fn finalize_transfer(&self) -> atlas_common::error::Result<()> {
        println!("Verifying integrity");

        //println!("TOTAL STATE TRANSFERED {:?}", self.db.size_on_disk());

        self.updates.write().expect("failed to acquire lock").clear();

        self.db
            .verify_integrity()
            .wrapped(atlas_common::error::ErrorKind::Error)
    }
}
