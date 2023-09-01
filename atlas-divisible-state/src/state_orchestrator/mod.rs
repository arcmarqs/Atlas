use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, RwLock,
    },
};

use crate::{
    state_tree::{Node, StateTree},
    SerializedTree,
};
use atlas_common::{crypto::hash::{Context, Digest}, collections::HashSet};
use serde::{Deserialize, Serialize};
use sled::{Config, Db, EventType, Mode, NodeEvent, Subscriber, IVec, Batch};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: Arc<Db>,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: Arc<RwLock<HashSet<u64>>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: Arc<Mutex<StateTree>>,
}

impl StateOrchestrator {
    pub fn new(path: &str) -> Self {
        let conf = Config::new()
            .mode(Mode::HighThroughput)
            .compression_factor(10)
            .use_compression(true)
            .path(path);

        let db = conf.open().unwrap();

        Self {
            db: Arc::new(db),
            updates: Arc::new(RwLock::new(HashSet::default())),
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

pub async fn monitor_changes(state: Arc<RwLock<HashSet<u64>>>, mut subscriber: Subscriber) {
    while let Some(event) = (&mut subscriber).await {
        match event {
            EventType::Split { lhs, rhs } => {
                let mut lock = state.write().expect("failed to acquire lock");
                lock.insert(lhs.pid);
                lock.insert(rhs.pid);
            }
            EventType::Merge { lhs, rhs, parent } => {
                let mut lock = state.write().expect("failed to acquire lock");
                lock.insert(lhs.pid);
                lock.insert(rhs.pid);
                if let Some(parent_ref) = parent {
                    lock.insert(parent_ref.pid);
                }
            }
            EventType::Node(n) => {
                let mut lock = state.write().expect("failed to acquire lock");
                lock.insert(n.pid);
            }
            _ => {}
        }
    }
}
