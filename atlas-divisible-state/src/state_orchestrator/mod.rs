use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock, Mutex}, collections::BTreeMap};

use atlas_common::crypto::hash::{Context, Digest};
use sled::{NodeEvent, Db, Mode, Config, Subscriber, EventType};
use crate::{state_tree::{StateTree, LeafNode, Node}, SerializedTree};

const UPDATE_SIZE: usize = 5000;

pub struct StateDescriptor {
    update_counter: AtomicUsize,
    updates: Arc<RwLock<BTreeMap<u64,NodeEvent>>>,
    tree: Arc<Mutex<StateTree>>,
}

impl StateDescriptor {

    pub fn new() -> Self {
     
       Self {
            updates: Arc::new(RwLock::new(BTreeMap::default())),
            tree: Arc::new(Mutex::new(StateTree::init())),
            update_counter: AtomicUsize::new(0),
        }
    }

    fn insert(&self, key: u64, value: NodeEvent) {
        if let Ok(mut lock) = self.updates.write() {
            lock.insert(key, value);
            self.update_counter.fetch_add(1, Ordering::SeqCst);
        }

        if self.update_counter.load(Ordering::SeqCst) >= UPDATE_SIZE {
            self.flush();
        }
    }

    fn merged(&self, key: u64) {
        if let Ok(mut lock) = self.updates.write() {
            lock.remove(&key);
            self.update_counter.fetch_add(1, Ordering::SeqCst);
            self.tree.lock().unwrap().set_removed(key);
        }
    }

    fn flush(&self) {
        let mut lock = self.updates.write().unwrap();
        let mut tree_lock = self.tree.lock().unwrap();
        for (key, node) in lock.iter() {
            let hash = Digest::from_bytes(node.hash().as_bytes()).unwrap();
            tree_lock.insert(key.clone(), hash)
        }
        self.update_counter.store(0, Ordering::SeqCst);
        lock.clear();
    }

}


pub struct StateOrchestrator {
    pub db: Db,
    pub orchestrator: Arc<StateDescriptor>,
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
        
        let orchestrator = Arc::new(StateDescriptor::new());
       
       Self {
            db:db,
            orchestrator: orchestrator,
        }
    }

    pub fn get_subscriber(&self) -> Subscriber {
        self.db.watch_prefix(vec![])
    }
  
    pub fn insert(&self,key: String, value: String) {
        self.db.insert(key,value);
    }

    pub fn checksum_prefix(&self, prefix: &[u8]) -> Digest {
        let mut iterator = self.db.scan_prefix(prefix);
        let mut hasher = Context::new();
        
       while let Some(Ok((key,value))) = iterator.next() {
            hasher.update(key.as_ref());
            hasher.update(value.as_ref());
       }

       hasher.finish()
    }

    //Exports all entries with a certain prefix.
    pub fn export_prefix(&self, prefix: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let kvs_iter = self.db.scan_prefix(prefix);

        kvs_iter.map(|kv_opt| {
            let kv = kv_opt.unwrap();
            vec![kv.0.to_vec(), kv.1.to_vec()]
        }).collect()
    }

    pub fn import_prefix(& mut self, mut export_prefix: Vec<Vec<Vec<u8>>>, overwrite: bool) {
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

    pub fn print_mktree(&self) {
        let tree_lock = self.orchestrator.tree.lock().unwrap();
        let root = tree_lock.bag_peaks();

        println!("root: {:?}", root);
        println!("leaves: {:?}", tree_lock.leaves);
        println!("total leaves: {:?}", tree_lock.leaves.len());
    }

    pub fn get_page(&self, pid: u64) -> sled::Node {
        self.db.export_node(pid).unwrap()
    }

    pub fn get_descriptor(&self) -> SerializedTree {
        self.orchestrator.tree.lock().unwrap().full_serialized_tree()
    }

    pub fn get_partial_descriptor(&self, node: Arc<RwLock<Node>>) -> SerializedTree {
        self.orchestrator.tree.lock().unwrap().to_serialized_tree(node)
    }
}

pub async fn monitor_changes(state: Arc<StateDescriptor>, mut subscriber: Subscriber) {
    while let Some(event) = (&mut subscriber).await {
        match event {
            EventType::Split{ lhs, rhs} => {
                state.insert(lhs.pid, lhs);
                state.insert(rhs.pid, rhs); 
            }
            EventType::Merge{lhs, rhs, parent} =>{
                state.insert(lhs.pid, lhs);
                state.merged(rhs.pid);

                if let Some(parent_ref) = parent {
                    state.insert(parent_ref.pid, parent_ref);
                }
            },
            
            EventType::Update(_) => {
             //   println!("update");
            },
            EventType::Node(n) => {
                state.insert(n.pid, n); 
            },
        }
    }
}