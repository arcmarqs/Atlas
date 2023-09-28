use std::collections::btree_map::Values;
use std::ops::Deref;
use std::sync::{Arc,RwLock};
use std::time::Instant;

use atlas_common::crypto::hash::Context;
use atlas_common::error::ResultWrappedExt;
use atlas_common::ordering::{self, SeqNo};
use atlas_common::{crypto::hash::Digest, ordering::Orderable};
use atlas_execution::state::divisible_state::{
    DivisibleState, DivisibleStateDescriptor, PartDescription, PartId, StatePart,
};
use atlas_metrics::metrics::metric_duration;
use serde::{Deserialize, Serialize};
use state_orchestrator::{StateOrchestrator, PREFIX_LEN, Prefix};
use state_tree::{LeafNode,StateTree};
use crate::metrics::CREATE_CHECKPOINT_TIME_ID;

pub mod state_orchestrator;
pub mod state_tree;

pub mod metrics;

#[derive(Clone, Serialize, Deserialize)]
pub struct SerializedState {
    leaf: Arc<LeafNode>,
    bytes: Box<[u8]>,
}

impl SerializedState {
    pub fn from_prefix(prefix: Prefix, kvs: &[(Box<[u8]>,Box<[u8]>)], seq: SeqNo) -> Self {
        let mut hasher = Context::new();

        let bytes: Box<[u8]> = bincode::serialize(&kvs).expect("failed to serialize").into();

        println!("bytes {:?}", bytes.len());
        //hasher.update(&pid.to_be_bytes());
        hasher.update(&bytes);

        Self {
            bytes,
            leaf: LeafNode::new(
                seq,
                prefix,
                hasher.finish(),
            ).into(),
        }
    }

    pub fn to_pairs(&self) -> Box<[(Box<[u8]>,Box<[u8]>)]> {
        let kv_pairs: Box<[(Box<[u8]>,Box<[u8]>)]> = bincode::deserialize(&self.bytes).expect("failed to deserialize");

        kv_pairs
    }

    pub fn hash(&self) -> Digest {
        let mut hasher = Context::new();

        //hasher.update(&self.leaf.pid.to_be_bytes());
        hasher.update(&self.bytes);

        hasher.finish()
    }
}

impl StatePart<StateOrchestrator> for SerializedState {
    fn descriptor(&self) -> &LeafNode {
        self.leaf.as_ref()
    }

    fn id(&self) -> &[u8] {
        self.leaf.id()
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
    tree: Arc<RwLock<StateTree>>
}

impl SerializedTree {
    pub fn new(tree: Arc<RwLock<StateTree>>) -> Self {
        Self {
            tree
        }
    }
}

impl PartialEq for SerializedTree {
    fn eq(&self, other: &Self) -> bool {
        self.tree.read().expect("failed to read").root.eq(&other.tree.read().expect("failed to read").root)
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl Orderable for SerializedTree {
    fn sequence_number(&self) -> ordering::SeqNo {
        self.tree.read().expect("failed to read").seqno
    }
}

impl DivisibleStateDescriptor<StateOrchestrator> for SerializedTree {
    fn parts(&self) -> Box<[Arc<LeafNode>]>{
        self.tree.read().expect("failed to read").leaves.values().cloned().collect::<Box<_>>()
    }

    fn get_digest(&self) -> Option<Digest> {
        self.tree.read().expect("failed to read").root
    }
}

impl PartId for LeafNode {
    fn content_description(&self) -> &[u8] {
        self.get_digest()
    }

    fn seq_no(&self) -> &SeqNo {
        &self.seqno
    }
}

impl PartDescription for LeafNode {
    fn id(&self) -> &[u8] {
        self.get_id()
    }
}

impl DivisibleState for StateOrchestrator {
    type PartDescription = LeafNode;
    type StateDescriptor = SerializedTree;
    type StatePart = SerializedState;

    fn get_descriptor(&self) -> SerializedTree {
        self.get_descriptor_inner()
    }

    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> atlas_common::error::Result<()> {

        for part in parts {
            let pairs = part.to_pairs();
            let prefix = part.id();
            //let mut batch = sled::Batch::default();            
            self.mk_tree.write().expect("failed to write").insert_leaf(part.leaf.id.clone(), part.leaf.clone());

            for (k,v) in pairs.iter() {
              self.db.0.insert([prefix,k.as_ref()].concat(), v.as_ref()); 
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
        let mut tree_lock = self.mk_tree.write().expect("failed to write");
        if !self.updates.is_empty() {
            println!("{:?}", self.updates.len());
            let cur_seq = tree_lock.next_seqno();
            for prefix in self.updates.iter() {
                let kv_iter = self.db.0.scan_prefix(prefix.as_ref());
                let kv_pairs = kv_iter
                .map(|kv| kv.map(|(k, v)| (k[PREFIX_LEN..].into(), v.deref().into())).expect("fail"))
                .collect::<Box<_>>();
                let serialized_part = SerializedState::from_prefix(prefix.clone(),kv_pairs.as_ref(), cur_seq); 
                tree_lock.insert_leaf(prefix.clone(),serialized_part.leaf.clone());
                state_parts.push(serialized_part);      
            } 

            self.updates.clear();           

            tree_lock.calculate_tree();
        }

        println!("checkpoint finished {:?}", checkpoint_start.elapsed());

        metric_duration(CREATE_CHECKPOINT_TIME_ID, checkpoint_start.elapsed());
        Ok((state_parts, self.get_descriptor()))
    }

    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.mk_tree.read().expect("failed to read").get_seqno())
    }

    fn finalize_transfer(&mut self) -> atlas_common::error::Result<()> {           
        
        self.mk_tree.write().expect("failed to get write").calculate_tree();
    
        //println!("finished st {:?}", self.get_descriptor());

        println!("Verifying integrity");

        self.db
            .0.verify_integrity()
            .wrapped(atlas_common::error::ErrorKind::Error)
    }
}
