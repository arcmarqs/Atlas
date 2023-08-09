use std::ops::Deref;
use std::sync::{RwLock, Arc};


use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_common::ordering::{self, SeqNo};
use atlas_execution::state::divisible_state::{StatePart, DivisibleState, PartId, DivisibleStateDescriptor, PartDescription};
use serde::{Serialize, Deserialize};
use sled::{Serialize as sled_serialize, NodeEvent};
use state_orchestrator::{StateOrchestrator, StateUpdates};
use state_tree::{StateTree, Node, LeafNode};
use blake3::Hasher;
pub mod state_orchestrator;
pub mod state_tree;

#[derive(Clone,Serialize,Deserialize)]
pub struct SerializedState {
    bytes: Vec<u8>,
    leaf: LeafNode,
}

impl SerializedState{
    pub fn from_node(pid: u64,node: sled::Node, seq: SeqNo) -> Self {
        let bytes = sled_serialize::serialize(&node);
        let mut hasher = blake3::Hasher::new();

        hasher.update(&pid.to_be_bytes());
        hasher.update(bytes.as_slice());

        Self{
            bytes,
            leaf: LeafNode::new(seq,pid,Digest::from_bytes(hasher.finalize().as_bytes()).unwrap()),
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
        Self { root_digest: Digest::blank(), seqno: SeqNo::ZERO, leaves: Default::default() }
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
        if self.seqno == SeqNo::ZERO {
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
                SerializedTree::default()},
        }
    }

    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> atlas_common::error::Result<()> {
        for part in parts {
            
            self.mk_tree.lock().expect("failed to lock while accepting parts").insert(part.leaf.seqno, part.leaf.pid,part.leaf.digest);
             
            if let Err(()) = self.import_page(part.leaf.pid, part.to_node()) {
                panic!("Failed to import Page");
            }
        }

        self.updates.clear();

        let pids: Vec<u64> = self.mk_tree.lock().unwrap().full_serialized_tree().unwrap().leaves.iter().map(|leaf| leaf.pid).collect();
        println!("page order: {:?}", pids);
        Ok(())
    }

  /*   fn prepare_checkpoint(&mut self) -> atlas_common::error::Result<Self::StateDescriptor> {
        // need to see how to handle snapshots of the state with sled'
        if let Ok(descriptor) = self.get_descriptor() {
            Ok(descriptor.clone())
        } else {
            Err(atlas_common::error::Error::simple(atlas_common::error::ErrorKind::Persistentdb))
        }
    } */

    fn get_parts(
        &mut self,
    ) -> Result<(Vec<SerializedState>, SerializedTree), atlas_common::error::Error> {
        let mut state_parts = Vec::new();
        let parts_to_get: Vec<(u64,NodeEvent)> = {
            let lock = self.updates.updates.read().unwrap();
            lock.iter().map(|(k,v)| (*k,v.clone())).collect()
        };

        for (pid,_) in parts_to_get {
           // let leaf_update = {
          //      let r_lock = self.descriptor.updates.read().unwrap();
          //      r_lock.contains_key(&leaf.pid)
          //  };

            if let Some(node) = self.get_page(pid) {              
                let serialized_part = SerializedState::from_node(pid,node,self.updates.seqno);

                {
                    self.mk_tree.lock().expect("Couldn't aquire lock").insert_leaf(serialized_part.leaf.clone());
                }

                state_parts.push(serialized_part);
            }
        }

        self.updates.clear();
        self.updates.seqno.next();

        let descriptor = self.get_descriptor();

        Ok((state_parts,descriptor))
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.updates.seqno)
    }

}
