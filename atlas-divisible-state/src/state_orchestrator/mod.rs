use std::sync::{
        Arc, Mutex
    };

use crate::{
    state_tree::StateTree,
    SerializedTree,
};
use atlas_common::{async_runtime::spawn, collections::ConcurrentHashMap};
use serde::{Deserialize, Serialize};
use sled::{Config, Db, EventType, Mode, Subscriber, Guard};


 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: Arc<Db>,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: Arc<ConcurrentHashMap<u64,()>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: StateTree,
}

impl StateOrchestrator {
    pub fn new(path: &str) -> Self {
        let conf = Config::new()
        .mode(Mode::HighThroughput)
        .path(path);

        let db = conf.open().unwrap();
        let updates = Arc::new(ConcurrentHashMap::default());
        let subscriber = db.watch_prefix(vec![]);

        let ret = Self {
            db: Arc::new(db),
            updates: updates.clone(),
            mk_tree: StateTree::default(),
        };

        let _ = spawn(
            monitor_changes(
                updates.clone(),
                subscriber));

       ret
    }

    pub fn get_subscriber(&self) -> Subscriber {
        self.db.watch_prefix(vec![])
    }

    pub fn insert(&self, key: String, value: String) {
        self.db.insert(key, value);
    }

    pub fn generate_id(&self) -> u64 {
        self.db.generate_id().expect("Failed to Generate id")
    }

  /*   pub fn checksum_prefix(&self, prefix: &[u8]) -> Digest {
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
        let kvs_iter = self.db.scan_prefix(prefix);use atlas_common::persistentdb::sled;


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
    */

    /*  pub fn print_mktree(&self) {
        let tree_lock = self.descriptor.tree.lock().unwrap();
        let root = tree_lock.bag_peaks();

        println!("root: {:?}", root);
        println!("leaves: {:?}", tree_lock.leaves);
        println!("total leaves: {:?}", tree_lock.leaves.len());
    } */

    pub fn get_page<'g>(&self, pid: u64, guard: &'g Guard) -> Option<sled::Node> {
        self.db.export_node(pid,guard)
    }

    pub fn import_page(&self, pid: u64, node: sled::Node) -> Result<(), ()> {
        if let Ok(()) = self.db.import_node(pid, node) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn get_descriptor_inner(&self) -> Result<SerializedTree, ()> {
        self.mk_tree.to_serialized_tree()
    }

}

pub async fn monitor_changes(state: Arc<ConcurrentHashMap<u64,()>>, mut subscriber: Subscriber) {
    while let Some(event) = (&mut subscriber).await {
        match event {
            EventType::Split { lhs, rhs } => {
                state.insert(lhs,());
                state.insert(rhs,());
            }
            EventType::Merge { lhs, rhs, ..} => {
                state.insert(lhs,());
                state.insert(rhs,());
            }
            EventType::Node(n) => {
                state.insert(n,());
            }
            _ => {}
        }
    }
}
