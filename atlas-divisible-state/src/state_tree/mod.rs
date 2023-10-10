use atlas_common::{crypto::hash::*, ordering::SeqNo};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeMap, sync:: Arc,
};

use crate::state_orchestrator::Prefix;
// This Merkle tree is based on merkle mountain ranges
// The Merkle mountain range was invented by Peter Todd. More detalis can be read at
// [Open Timestamps](https://github.com/opentimestamps/opentimestamps-server/blob/master/doc/merkle-mountain-range.md)
// and the [Grin project](https://github.com/mimblewimble/grin/blob/master/doc/mmr.md).
// Might implement a caching strategy to store the least changed nodes in the merkle tree.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTree {
   // control: AtomicBool,
    // Sequence number of the latest update in the tree.
    pub seqno: SeqNo,
    // Stores the peaks by level, every time a new peak of the same level is inserted, a new internal node with level +1 is created.
    //pub peaks: BTreeMap<u32, NodeRef>,
    pub updated: bool,
    pub root: Option<Digest>,
    // stores references to all leaves, ordered by the part id
    pub leaves: BTreeMap<Prefix, Arc<LeafNode>>,
}

impl Default for StateTree {
    fn default() -> Self {
        Self {
          //  control: AtomicBool::new(false),
            seqno: SeqNo::ZERO,
            updated: false,
            root: Default::default(),
            leaves: Default::default(),
        }
    }
}

impl StateTree {
    pub fn init() -> Self {
        Self {
         //   control: AtomicBool::new(false),
            seqno: SeqNo::ZERO,
            updated: false,
            root: Default::default(),
            leaves: BTreeMap::new(),
        }
    }

    pub fn insert_leaf(&mut self, pid: Prefix , leaf: Arc<LeafNode>) {
        self.seqno = self.seqno.max(leaf.seqno);
        self.leaves.insert(pid, leaf);
    }

    pub fn calculate_tree(&mut self) {
        let mut peaks: BTreeMap<u32,Digest> = BTreeMap::new();       
        for leaf in self.leaves.values() {
            let mut node_digest = leaf.get_digest_cloned();
            let mut level = 0;

            while let Some(same_level) = peaks.insert(level, node_digest) {
                let mut hasher = Context::new();
                hasher.update(same_level.as_ref());
                hasher.update(node_digest.as_ref());
                node_digest = hasher.finish();            
                peaks.remove(&level);

                level +=1;
            }   
        }

      //  println!("peaks: {:?}", peaks);
        self.updated = false;
        self.root = self.bag_peaks(peaks);
    }

    // iterates over peaks and consolidates them into a single node
    fn bag_peaks(&self, mut peaks: BTreeMap<u32, Digest>) -> Option<Digest> {
        let mut bagged_peaks: Vec<Digest> = Vec::new();

        // Iterating in reverse makes the tree more unbalanced, but preserves the order of insertion,
        // this is important when serializing or sending the tree since we send only the root digest and the leaves.
        while !peaks.is_empty() {
            let peak = peaks.pop_last().unwrap().1;
            if let Some(top) = bagged_peaks.pop() {
                let mut hasher = Context::new();
                hasher.update(top.as_ref());
                hasher.update(peak.as_ref());
                let new_top = hasher.finish();
                bagged_peaks.push(new_top);
            } else {
                bagged_peaks.push(peak);
            }
        }

        assert!(bagged_peaks.len() == 1);
        bagged_peaks.pop()
    }

    pub fn next_seqno(&mut self) -> SeqNo {
        let ret = self.seqno;
        self.seqno = self.seqno.next();

        ret
    }

    pub fn get_seqno(&self) -> SeqNo {
        self.seqno
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub seqno: SeqNo,
    pub id: Prefix,
    pub digest: Digest,
}

impl PartialEq for LeafNode {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl PartialOrd for LeafNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.digest.partial_cmp(&other.digest) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.seqno.partial_cmp(&other.seqno)
    }
}

impl LeafNode {
    pub fn new(seqno: SeqNo, id: Prefix, digest: Digest) -> Self {
        Self {seqno, id, digest, }
    }

    pub fn get_digest(&self) -> &[u8] {
        self.digest.as_ref()
    }

    pub fn get_digest_cloned(&self) -> Digest {
        self.digest.clone()
    }

    pub fn update_hash(&mut self, new_digest: Digest) {
        self.digest = new_digest;
    }

    pub fn get_id(&self) -> &[u8] {
        self.id.as_ref()
    }
}
