use atlas_common::crypto::hash::*;
use serde::{Serialize, Deserialize, ser::{SerializeMap, SerializeStruct}};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock}, cmp::Ordering,
};

// This Merkle tree is based on merkle mountain ranges
// The Merkle mountain range was invented by Peter Todd. More detalis can be read at
// [Open Timestamps](https://github.com/opentimestamps/opentimestamps-server/blob/master/doc/merkle-mountain-range.md)
// and the [Grin project](https://github.com/mimblewimble/grin/blob/master/doc/mmr.md).
// Might implement a caching strategy to store the least changed nodes in the merkle tree.

#[derive(Debug)]
pub struct StateTree {
    // Stores the peaks by level, every time a new peak of the same level is inserted, a new internal node with level +1 is created.
    pub peaks: BTreeMap<u32, NodeRef>,
    // stores references to all leaves, ordered by the page id
    pub leaves: BTreeMap<u64, NodeRef>,
}

impl StateTree {
    pub fn init() -> Self {
        Self {
            peaks: BTreeMap::new(),
            leaves: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, pid: u64, digest: Digest) {
        let new_leaf = Arc::new(RwLock::new(Node::leaf(pid, digest)));
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
        let level = node.get_level();

        let node_ref = Arc::new(RwLock::new(Node::Internal(node.into())));

        if let Some(same_level) = self.peaks.insert(level, node_ref.clone()) {
            let new_peak = InternalNode::new(same_level, node_ref.clone());

            self.peaks.remove(&level);
            self.insert_internal(new_peak);
        }
    }

    pub fn set_removed(&self, pid: u64) {
        for peak in self.peaks.values().rev().cloned() {
            let contains_pid = peak.read().unwrap().contains_pid(pid);
            match contains_pid {
                true => {
                    let mut node = peak.write().unwrap();
                    node.set_removed(pid);
                    return;
                }

                false => continue,
            }
        }
    }

    pub fn get_leaf(&self, pid: u64) -> LeafNode {
        self.leaves
            .get(&pid)
            .unwrap()
            .read()
            .unwrap()
            .get_leaf()
            .clone()
    }

    // iterates over peaks and consolidates them into a single node
    pub fn bag_peaks(&self) -> Option<NodeRef> {
        let mut bagged_peaks: Vec<NodeRef> = Vec::new();

        // Iterating in reverse makes the tree more unbalanced, but preserves the order of insertion,
        // this is important when serializing or sending the tree since we send only the root digest and the leaves.
        for peak in self.peaks.values().rev() {
            if let Some(top) = bagged_peaks.pop() {
                let new_top = Node::internal(top, peak.clone());
                bagged_peaks.push(Arc::new(RwLock::new(new_top)));
            } else {
                bagged_peaks.push(peak.clone());
            }
        }

        bagged_peaks.pop()
    }
}

pub type NodeRef = Arc<RwLock<Node>>;

// How many children each node has, determines the width of the merkle tree

// The main difference is that each leaf reflects more than one key value pair
// The Leaf enum stores the list of the keys that are reflected in its hash,
// the actual hash includes the values stored on that keys as well so Hash(Node) != Hash(Keys)

// The Hasher is stored in the structure so that we can feed it incrementally.
// still need to consider if its worth it to have, since it won't be used for leafs
// is also pretty much useless for updating the tree since we have to reset it.

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
    Empty,
}

impl Node {
    pub fn empty() -> Node {
        Node::Empty
    }

    pub fn leaf(pid: u64, digest: Digest) -> Node {
        let leaf_node = LeafNode::new(pid, digest);
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
            Node::Empty => 0,
        }
    }

    pub fn get_right_pids(&self) -> Vec<u64> {
        match self {
            Node::Leaf(leaf) => vec![leaf.pid],
            Node::Internal(internal) => internal.get_right_pids().to_owned(),
            Node::Empty => vec![],
        }
    }

    pub fn get_left_pids(&self) -> Vec<u64> {
        match self {
            Node::Leaf(leaf) => vec![leaf.pid],
            Node::Internal(internal) => internal.get_left_pids().to_owned(),
            Node::Empty => vec![],
        }
    }

    pub fn get_hash(&self) -> Digest {
        match self {
            Node::Leaf(leaf) => leaf.get_digest(),
            Node::Internal(internal) => internal.get_hash(),
            Node::Empty => Digest::blank(),
        }
    }

    pub fn get_pids_concat(&self) -> Vec<u64> {
        match self {
            Node::Leaf(leaf) => vec![leaf.pid],
            Node::Internal(internal) => vec![
                internal.get_left_pids().to_owned(),
                internal.get_right_pids().to_owned(),
            ]
            .concat(),
            Node::Empty => vec![],
        }
    }

    // verifies if a node contains a Pid, returns true if it contains the the pid and true if its a leaf
    pub fn contains_pid(&self, pid: u64) -> bool {
        match self {
            Node::Leaf(leaf) => leaf.pid.eq(&pid),
            Node::Internal(internal) => {
                internal.left_pids.contains(&pid) || internal.right_pids.contains(&pid)
            }
            Node::Empty => false,
        }
    }

    pub fn update_node(&mut self, pid: u64, node: NodeRef) -> Digest {
        match self {
            Node::Leaf(leaf) => {
                let lock = node.read().unwrap();
                let new_leaf = lock.get_leaf();
                leaf.digest = new_leaf.digest;
                leaf.removed = false;
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
            Node::Empty => todo!(),
        }
    }

    pub fn set_removed(&mut self, pid: u64) {
        match self {
            Node::Leaf(leaf) => leaf.set_removed(),
            Node::Internal(internal) => {
                if internal.left_pids.contains(&pid) {
                    internal.left.write().unwrap().set_removed(pid);
                } else if internal.right_pids.contains(&pid) {
                    internal.right.write().unwrap().set_removed(pid);
                }
            }
            Node::Empty => todo!(),
        }
    }

    pub fn get_leaf(&self) -> &LeafNode {
        match self {
            Node::Leaf(leaf) => leaf,
            _ => panic!("Not a leaf Node"),
        }
    }
}

#[derive(Debug,Serialize, Deserialize,Clone)]
pub struct InternalNode {
    level: u32,
    // pids that are authenticated by the left and right child node, ordered left to right.
    left_pids: Vec<u64>,
    right_pids: Vec<u64>,
    digest: Digest,
    left: NodeRef,
    right: NodeRef,
}

impl InternalNode {
    pub fn new(left: NodeRef, right: NodeRef) -> Self {
        let left_borrow = left.read().unwrap();
        let right_borrow = right.read().unwrap();
        let level = left_borrow.get_level().max(right_borrow.get_level()) + 1;
        let left_pids = left_borrow.get_pids_concat();
        let right_pids = right_borrow.get_pids_concat();
        let mut hasher = Context::new();

        hasher.update(left_borrow.get_hash().as_ref());
        hasher.update(right_borrow.get_hash().as_ref());

        drop(left_borrow);
        drop(right_borrow);

        Self {
            level,
            left_pids,
            right_pids,
            digest: hasher.finish(),
            left,
            right,
        }
    }

    pub fn get_level(&self) -> u32 {
        self.level
    }

    pub fn get_left_pids(&self) -> &Vec<u64> {
        &self.left_pids
    }

    pub fn get_right_pids(&self) -> &Vec<u64> {
        &self.right_pids
    }

    pub fn get_hash(&self) -> Digest {
        self.digest
    }

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
}

impl PartialEq for InternalNode {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl PartialOrd for InternalNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.level.partial_cmp(&other.level) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.left_pids.partial_cmp(&other.left_pids) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.right_pids.partial_cmp(&other.right_pids) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.digest.partial_cmp(&other.digest) {
            Some(core::cmp::Ordering::Equal) => Some(Ordering::Equal),
            ord => return ord,
        }
      
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub pid: u64,
    pub digest: Digest,
    removed: bool,
}

impl PartialEq for LeafNode {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest && self.removed == other.removed
    }
}

impl PartialOrd for LeafNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.pid.partial_cmp(&other.pid) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.digest.partial_cmp(&other.digest) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.removed.partial_cmp(&other.removed)
    }
}

impl LeafNode {
    pub fn new(pid: u64, digest: Digest) -> Self {
        Self {
            pid,
            digest,
            removed: false,
        }
    }

    pub fn get_digest(&self) -> Digest {
        self.digest
    }

    pub fn get_pid(&self) -> u64 {
        self.pid
    }

    pub fn update_hash(&mut self, new_digest: Digest) {
        self.digest = new_digest;
    }

    pub fn set_removed(&mut self) {
        self.removed = true;
    }
}
