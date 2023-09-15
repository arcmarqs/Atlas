use atlas_common::{crypto::hash::*, ordering::SeqNo};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::Arc,
};

// This Merkle tree is based on merkle mountain ranges
// The Merkle mountain range was invented by Peter Todd. More detalis can be read at
// [Open Timestamps](https://github.com/opentimestamps/opentimestamps-server/blob/master/doc/merkle-mountain-range.md)
// and the [Grin project](https://github.com/mimblewimble/grin/blob/master/doc/mmr.md).
// Might implement a caching strategy to store the least changed nodes in the merkle tree.

#[derive(Debug, Clone)]
pub struct StateTree {
    // if any node of the tree was updated, we should recalculate the whole tree
    updated: bool,
    // Sequence number of the latest update in the tree.
    pub seqno: SeqNo,
    // Stores the peaks by level, every time a new peak of the same level is inserted, a new internal node with level +1 is created.
    //pub peaks: BTreeMap<u32, NodeRef>,
    pub root: Option<Digest>,
    // stores references to all leaves, ordered by the page id
    pub leaves: BTreeMap<Vec<u8>, LeafNode>,
}

impl Default for StateTree {
    fn default() -> Self {
        Self {
            updated: false,
            seqno: SeqNo::ZERO,
            root: Default::default(),
            leaves: Default::default(),
        }
    }
}

impl StateTree {
    pub fn init() -> Self {
        Self {
            updated: false,
            seqno: SeqNo::ZERO,
            root: Default::default(),
            leaves: BTreeMap::new(),
        }
    }

    pub fn insert_leaf(&mut self, leaf: LeafNode) {
        self.seqno = self.seqno.max(leaf.seqno);
        let leaf_pid = leaf.get_pid();
        if let Some(old) = self.leaves.insert(leaf_pid, leaf) {
            if old.get_digest() == leaf.digest {
                //value already inserted, no need to continue

                return;
            }

            // we changed a node so we must recalculate the tree in the next checkpoint
            self.updated = true;
        }
    }

/*
    pub fn insert_leaf(&mut self, leaf: LeafNode) {
        self.seqno = self.seqno.max(leaf.seqno);
        let leaf_pid = leaf.get_pid();
        let new_leaf = Arc::new(RwLock::new(Node::Leaf(leaf)));
        if let Some(old) = self.leaves.insert(leaf_pid, new_leaf.clone()) {
            if old.read().unwrap().get_hash() == new_leaf.read().unwrap().get_hash() {
                //value already inserted, no need to continue
                return;
            }

            for peak in self.peaks.values().rev().cloned() {
                let contains_pid = peak.read().unwrap().contains_pid(leaf_pid);
                match contains_pid {
                    true => {
                        let mut node = peak.write().unwrap();
                        node.update_node(leaf_pid, new_leaf.clone());
                        return;
                    }

                    false => continue,
                }
            }
        }

        if let Some(same_level) = self.peaks.insert(0, new_leaf.clone()) {
            let new_peak;

            if same_level.read().unwrap().get_left_pids().first().unwrap() < &leaf_pid {
                new_peak = InternalNode::new(same_level, new_leaf.clone());
            } else {
                new_peak = InternalNode::new(new_leaf.clone(), same_level);
            }

            self.peaks.remove(&0);
            self.insert_internal(new_peak);
        }
    }

    pub fn insert(&mut self, seqno: SeqNo, pid: u64, digest: Digest) {
        let new_leaf = Arc::new(RwLock::new(Node::leaf(seqno, pid, digest)));
        if let Some(_) = self.leaves.insert(pid, new_leaf.clone()) {
            for peak in self.peaks.values().rev().cloned() {
                let contains_pid = peak.read().unwrap().contains_pid(pid);
                match contains_pid {
                    true => {
                        let mut node = peak.write().unwrap();
                        node.update_node(pid, new_leaf.clone());
                        return;
                    }

                    false => continue,
                }
            }
        }

        if let Some(same_level) = self.peaks.insert(0, new_leaf.clone()) {
            let new_peak;

            if same_level.read().unwrap().get_left_pids().first().unwrap() < &pid {
                new_peak = InternalNode::new(same_level, new_leaf.clone());
            } else {
                new_peak = InternalNode::new(new_leaf.clone(), same_level);
            }

            self.peaks.remove(&0);
            self.insert_internal(new_peak);
        }
    }

    

    fn insert_internal(&mut self, node: InternalNode) {
        let level: u32 = node.get_level();

        let node_ref = Arc::new(RwLock::new(Node::Internal(node.into())));

        if let Some(same_level) = self.peaks.insert(level, node_ref.clone()) {
            let new_peak = InternalNode::new(same_level, node_ref.clone());

            self.peaks.remove(&level);
            self.insert_internal(new_peak);
        }
    }

   

    pub fn get_leaf(&self, pid: u64) -> LeafNode {
        self.leaves
            .get(&pid)
            .unwrap()
            .clone()
    }
     */

    pub fn calculate_tree(&mut self) {
        let mut peaks = BTreeMap::new();       
        let mut hasher = blake3::Hasher::default();
        for leaf in self.leaves.values() {
            let mut node_digest = leaf.get_digest();
            let mut level = 0;

            while let Some(same_level) = peaks.insert(level, node_digest) {
                let buf = [same_level.as_ref(),node_digest.as_ref()].concat();
                hasher.update(buf.as_ref());
                node_digest = Digest::from_bytes(hasher.finalize().as_bytes()).expect("failed to convert bytes");                
                hasher.reset();
                peaks.remove(&level);

                level +=1;
            }   
        }

      //  println!("peaks: {:?}", peaks);

        self.root = self.bag_peaks(peaks);
    }

    // iterates over peaks and consolidates them into a single node
    fn bag_peaks(&self, peaks: BTreeMap<u32, Digest>) -> Option<Digest> {
        let mut bagged_peaks: Vec<Digest> = Vec::new();
        let mut hasher = blake3::Hasher::default();

        // Iterating in reverse makes the tree more unbalanced, but preserves the order of insertion,
        // this is important when serializing or sending the tree since we send only the root digest and the leaves.
        for peak in peaks.values().rev() {
            if let Some(top) = bagged_peaks.pop() {
                let buf = [top.as_ref(),peak.as_ref()].concat();
                hasher.update(buf.as_ref());
                let new_top = Digest::from_bytes(hasher.finalize().as_bytes()).expect("failed to convert bytes");
                hasher.reset();
                bagged_peaks.push(new_top);
            } else {
                bagged_peaks.push(peak.clone());
            }
        }

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

/* pub type NodeRef = Node;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl Node {
    pub fn leaf(seqno: SeqNo, pid: u64, digest: Digest) -> Node {
        let leaf_node = LeafNode::new(seqno, pid, digest);
        Node::Leaf(leaf_node)
    }

    pub fn internal(left: NodeRef, right: NodeRef) -> Node {
        let internal_node = InternalNode::new(left, right);
        Node::Internal(internal_node)
    }

    pub fn get_level(&self) -> u32 {
        match self {
            Node::Leaf(_) => 0,
            Node::Internal(internal) => internal.get_level(),
        }
    }

    pub fn get_hash(&self) -> Digest {
        match self {
            Node::Leaf(leaf) => leaf.get_digest(),
            Node::Internal(internal) => internal.get_hash(),
        }
    }

    /* 
    // verifies if a node contains a Pid, returns true if it contains the the pid and true if its a leaf
    pub fn contains_pid(&self, pid: u64) -> bool {
        match self {
            Node::Leaf(leaf) => leaf.pid.eq(&pid),
            Node::Internal(internal) => {
                internal.left_pids.contains(&pid) || internal.right_pids.contains(&pid)
            }
        }
    }

    
    pub fn update_node(&mut self, pid: u64, node: NodeRef) -> Digest {
        match self {
            Node::Leaf(leaf) => {
                let lock = node.read().unwrap();
                let new_leaf = lock.get_leaf();
                leaf.digest = new_leaf.digest;
                leaf.digest
            }
            Node::Internal(internal) => {
                let mut hasher = Context::new();
                if internal.left_pids.contains(&pid) {
                    let left_hash = internal.left.write().unwrap().update_node(pid, node);
                    let right_hash = internal.right.read().unwrap().get_hash();
                    hasher.update(left_hash.as_ref());
                    hasher.update(right_hash.as_ref());
                } else if internal.right_pids.contains(&pid) {
                    let left_hash = internal.left.read().unwrap().get_hash();
                    let right_hash = internal.right.write().unwrap().update_node(pid, node);
                    hasher.update(left_hash.as_ref());
                    hasher.update(right_hash.as_ref());
                }
                internal.digest = hasher.finish();
                internal.digest
            }
        }
    }
*/
    pub fn get_leaf(&self) -> &LeafNode {
        match self {
            Node::Leaf(leaf) => leaf,
            _ => panic!("Not a leaf Node"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalNode {
    level: u32,  
    digest: Digest,
    left: NodeRef,
    right: NodeRef,
}

impl InternalNode {
    pub fn new(left: NodeRef, right: NodeRef) -> Self {
        let left_borrow = left.read().unwrap();
        let right_borrow = right.read().unwrap();
        let level = left_borrow.get_level().max(right_borrow.get_level()) + 1;
        let mut hasher = Context::new();

        hasher.update(left_borrow.get_hash().as_ref());
        hasher.update(right_borrow.get_hash().as_ref());

        drop(left_borrow);
        drop(right_borrow);

        Self {
            level,
            digest: hasher.finish(),
            left,
            right,
        }
    }

    pub fn get_level(&self) -> u32 {
        self.level
    }

    pub fn get_hash(&self) -> Digest {
        self.digest
    }
/* 
    pub(crate) fn update_left(&mut self, left: NodeRef) {
        let right_hash = self.right.read().unwrap().get_hash();
        let (left_hash, left_pids) = {
            let lock = left.read().unwrap();

            (lock.get_hash(), lock.get_pids_concat())
        };

        let mut hasher = Context::new();
        hasher.update(left_hash.as_ref());
        hasher.update(right_hash.as_ref());
        self.left = left;
        self.left_pids = left_pids;
    }

    pub(crate) fn update_right(&mut self, right: NodeRef) {
        let (right_hash, right_pids) = {
            let lock = right.read().unwrap();

            (lock.get_hash(), lock.get_pids_concat())
        };
        let left_hash = self.left.read().unwrap().get_hash();
        let mut hasher = Context::new();
        hasher.update(left_hash.as_ref());
        hasher.update(right_hash.as_ref());
        self.right = right;
        self.right_pids = right_pids;
    }
    */
}

impl PartialEq for InternalNode {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl PartialOrd for InternalNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {

        match self.digest.partial_cmp(&other.digest) {
            Some(core::cmp::Ordering::Equal) => Some(Ordering::Equal),
            ord => return ord,
        }
    }
}
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub seqno: SeqNo,
    pub pid: Vec<u8>,
    pub digest: Digest,
}

impl PartialEq for LeafNode {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl PartialOrd for LeafNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.seqno.partial_cmp(&other.seqno) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.pid.partial_cmp(&other.pid)
    }
}

impl LeafNode {
    pub fn new(seqno: SeqNo, pid: Vec<u8>, digest: Digest) -> Self {
        Self { seqno, pid, digest }
    }

    pub fn get_digest(&self) -> Digest {
        self.digest
    }

    pub fn get_pid(&self) -> Vec<u8> {
        self.pid
    }

    pub fn update_hash(&mut self, new_digest: Digest) {
        self.digest = new_digest;
    }
}
