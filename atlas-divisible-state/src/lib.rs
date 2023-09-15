use std::time::Instant;

use atlas_common::error::ResultWrappedExt;
use atlas_common::ordering::{self, SeqNo};
use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_execution::state::divisible_state::{
    DivisibleState, DivisibleStateDescriptor, PartDescription, PartId, StatePart,
};
use atlas_metrics::metrics::metric_duration;
use blake3::Hasher;
use serde::{Deserialize, Serialize};
use sled::{IVec, Iter};
use state_orchestrator::StateOrchestrator;
use state_tree::{LeafNode,StateTree};
use crate::metrics::CREATE_CHECKPOINT_TIME_ID;
use crate::state_orchestrator::Prefix;

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
        let sst_pairs = node.iter().map(|(key,value)| (IVec::from(key).to_vec(),value.to_vec())).collect::<Vec<_>>();
        let bytes = bincode::serialize(&sst_pairs).expect("failed to serialize");
        let mut hasher = blake3::Hasher::new();

        //hasher.update(&pid.to_be_bytes());
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

    pub fn from_prefix(id: u64, kv_iter: Iter, seq: SeqNo) -> Self {
        let mut hasher = blake3::Hasher::new();
        let kv_pairs = kv_iter
            .map(|kv| kv.map(|(k, v)| (k.to_vec(), v.to_vec())).expect("fail"))
            .collect::<Vec<_>>();

        let bytes = bincode::serialize(&kv_pairs).expect("failed to serialize");


        //hasher.update(&pid.to_be_bytes());
        hasher.update(bytes.as_slice());

        Self {
            bytes,
            leaf: LeafNode::new(
                seq,
                id,
                Digest::from_bytes(hasher.finalize().as_bytes()).unwrap(),
            ),
        }
    }

    pub fn to_pairs(&self) -> Vec<(Vec<u8>,Vec<u8>)> {
        let kv_pairs: Vec<(Vec<u8>,Vec<u8>)> = bincode::deserialize(&self.bytes).expect("failed to deserialize");

        kv_pairs
    }

    pub fn hash(&self) -> Digest {
        let mut hasher = blake3::Hasher::new();

        //hasher.update(&self.leaf.pid.to_be_bytes());
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
        state.to_serialized_tree()
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
    pub fn to_serialized_tree(&self) -> Result<SerializedTree, ()> {
        let leaf_list = {
            let mut vec = Vec::new();

            for (_, node) in self.leaves.iter() {
                vec.push(*node);
            }
            vec
        };
        
        Ok(SerializedTree::new(self.root.unwrap(), self.seqno, leaf_list))
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
            let pairs = part.to_pairs();
            //let mut batch = sled::Batch::default();            
            self.mk_tree.insert_leaf(part.leaf);

            for (k,v) in pairs {
              self.insert(k, v); 
            }

            //self.db.apply_batch(batch).expect("failed to apply batch");
          
        }
        //let _ = self.db.flush();

        Ok(())
    }

    fn get_parts(
        &mut self,
    ) -> Result<(Vec<SerializedState>, SerializedTree), atlas_common::error::Error> {
        let checkpoint_start = Instant::now();
       
        let mut state_parts = Vec::new();
        let mut parts = self.updates.lock().expect("failed to lock");

        if !parts.is_empty() {
            let len = parts.len();
            println!("{:?}",len);
            let cur_seq = self.mk_tree.next_seqno();
            for (id,prefix) in parts.iter().enumerate() {
                let kv_pairs = self.db.scan_prefix(prefix.as_ref());
                let serialized_part = SerializedState::from_prefix(id as u64,kv_pairs, cur_seq);
                self.mk_tree.insert_leaf(serialized_part.leaf);
                state_parts.push(serialized_part);
            }            

            self.mk_tree.calculate_tree();
            parts.clear();
        }

        drop(parts);

        let mut hasher = blake3::Hasher::new();
        for kv in self.db.iter() {
            let (k,v) = kv.expect("fail");
            hasher.update(&k);
            hasher.update(&v);
        }

        println!("full digest {:?}", Digest::from_bytes(hasher.finalize().as_bytes()).unwrap());

        println!("checkpoint finished {:?}", checkpoint_start.elapsed());

        metric_duration(CREATE_CHECKPOINT_TIME_ID, checkpoint_start.elapsed());
        Ok((state_parts, self.get_descriptor()))
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.mk_tree.get_seqno())
    }

    fn finalize_transfer(&mut self) -> atlas_common::error::Result<()> {           
        
        self.updates.lock().expect("failed to lock").clear();

        self.mk_tree.calculate_tree();
        
        println!("finished st {:?}", self.get_descriptor());
        //println!("TOTAL STATE TRANSFERED {:?}", self.db.size_on_disk());

        //self.mk_tree.calculate_tree();

       println!("Verifying integrity");

        self.db
            .verify_integrity()
            .wrapped(atlas_common::error::ErrorKind::Error)
    }
}
