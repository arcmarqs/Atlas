use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, RwLock,
    },
};

use crate::{
    state_tree::{LeafNode, Node, StateTree},
    SerializedTree,
};
use atlas_common::{
    async_runtime::spawn,
    crypto::hash::{Context, Digest},
    ordering::SeqNo,
};
use serde::{Deserialize, Serialize};
use sled::{Config, Db, EventType, Mode, NodeEvent, Subscriber};

#[derive(Debug)]
pub struct StateUpdates {
    pub updates: Arc<RwLock<BTreeMap<u64, NodeEvent>>>,
}

impl Clone for StateUpdates {
    fn clone(&self) -> Self {
        Self {
            updates: self.updates.clone(),
        }
    }
}

impl Default for StateUpdates {
    fn default() -> Self {
        Self {
            updates: Default::default(),
        }
    }
}

impl StateUpdates {
    pub fn new() -> Self {
        Self {
            updates: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }

    fn insert(&self, key: u64, value: NodeEvent) {
        if let Ok(mut lock) = self.updates.write() {
            lock.insert(key, value);
        }
    }

    fn merged(&self, key: u64) {
        if let Ok(mut lock) = self.updates.write() {
            lock.remove(&key);
        }
    }

    pub fn clear(&self) {
        let mut lock = self.updates.write().expect("Failed to lock updates");
        lock.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: Arc<Db>,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: Arc<StateUpdates>,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: Arc<Mutex<StateTree>>,
}

impl StateOrchestrator {
    pub fn new(path: &str) -> Self {
        let mode = Mode::HighThroughput;
        let conf = Config::new()
            .mode(mode)
            .compression_factor(10)
            .use_compression(true)
            .path(path);

        let db = conf.open().unwrap();

        Self {
            db: Arc::new(db),
            updates: Arc::new(StateUpdates::new()),
            mk_tree: Arc::new(Mutex::new(StateTree::default())),
        }
    }

    pub fn get_subscriber(&self) -> Subscriber {
        self.db.watch_prefix(vec![])
    }

    pub fn insert(&self, key: String, value: String) {
        self.db.insert(key, value);
    }

    pub fn checksum_prefix(&self, prefix: &[u8]) -> Digest {
        let mut iterator = self.db.scan_prefix(prefix);
        let mut hasher = Context::new();

        while let Some(Ok((key, value))) = iterator.next() {
            hasher.update(key.as_ref());
            hasher.update(value.as_ref());
        }

        hasher.finish()
    }

    //Exports all entries with a certain prefix.
    pub fn export_prefix(&self, prefix: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let kvs_iter = self.db.scan_prefix(prefix);

        kvs_iter
            .map(|kv_opt| {
                let kv = kv_opt.unwrap();
                vec![kv.0.to_vec(), kv.1.to_vec()]
            })
            .collect()
    }

    pub fn import_prefix(&mut self, mut export_prefix: Vec<Vec<Vec<u8>>>, overwrite: bool) {
        if overwrite {
            for kv in export_prefix.iter_mut() {
                let v = kv.pop().expect("failed to get value from prefix export");
                let k = kv.pop().expect("failed to get key from prefix export");
                let _ = self
                    .db
                    .insert(k, v)
                    .expect("failed to insert value during prefix import");
            }
        } else {
            for kv in export_prefix.iter_mut() {
                let v = kv.pop().expect("failed to get value from tree export");
                let k = kv.pop().expect("failed to get key from tree export");
                let old = self
                    .db
                    .insert(k, v)
                    .expect("failed to insert value during tree import");
                assert!(old.is_none(), "import is overwriting existing data");
            }
        }
    }

    /*  pub fn print_mktree(&self) {
        let tree_lock = self.descriptor.tree.lock().unwrap();
        let root = tree_lock.bag_peaks();

        println!("root: {:?}", root);
        println!("leaves: {:?}", tree_lock.leaves);
        println!("total leaves: {:?}", tree_lock.leaves.len());
    } */

    pub fn get_page(&self, pid: u64) -> Option<sled::Node> {
        self.db.export_node(pid)
    }

    pub fn import_page(&self, pid: u64, node: sled::Node) -> Result<(), ()> {
        if let Ok(()) = self.db.import_node(pid, node) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn get_descriptor_inner(&self) -> Result<SerializedTree, ()> {
        match self.mk_tree.lock() {
            Ok(lock) => lock.full_serialized_tree(),
            Err(_) => Err(()),
        }
    }

    pub fn get_partial_descriptor(&self, node: Arc<RwLock<Node>>) -> Result<SerializedTree, ()> {
        match self.mk_tree.lock() {
            Ok(lock) => lock.to_serialized_tree(node),
            Err(_) => Err(()),
        }
    }
}

pub async fn monitor_changes(state: Arc<StateUpdates>, mut subscriber: Subscriber) {
    while let Some(event) = (&mut subscriber).await {
        match event {
            EventType::Split { lhs, rhs } => {
                state.insert(lhs.pid, lhs);
                state.insert(rhs.pid, rhs);
            }
            EventType::Merge { lhs, rhs, parent } => {
                state.insert(lhs.pid, lhs);
                state.insert(rhs.pid, rhs);
                if let Some(parent_ref) = parent {
                    state.insert(parent_ref.pid, parent_ref);
                }
            }

            EventType::Update(_) => {}
            EventType::Node(n) => {
                state.insert(n.pid, n);
            }
        }
    }
}
