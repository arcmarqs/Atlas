use std::{sync::{
        Arc, Mutex, atomic::{AtomicI32, AtomicUsize}
    }, collections::{BTreeSet, BTreeMap, btree_set::Iter}};

use crate::{
    state_tree::StateTree,
    SerializedTree,
};
use atlas_common::{async_runtime::spawn, collections::{ConcurrentHashMap, HashSet, OrderedMap}};
use serde::{Deserialize, Serialize};
use sled::{Config, Db, EventType, Mode, Subscriber, Guard, IVec};

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Prefix(pub Vec<u8>);

impl Prefix {
    pub fn new(prefix: Vec<u8>) -> Prefix {
        Self(prefix)
    }
    pub fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn truncate(&self, len: usize) -> Prefix{
        Prefix(self.0[..len].to_vec())
    }
}
// A bitmap that registers changed prefixes over a set of keys
#[derive(Debug,Default,Clone)]
pub struct PrefixSet {
    prefix_len: usize,
    prefixes: BTreeSet<Prefix>,
}

impl PrefixSet {
    pub fn new() -> PrefixSet {
        Self { 
            prefix_len: 0, 
            prefixes: BTreeSet::default(), 
        }
    }

    pub fn insert(&mut self, key: Vec<u8>) {

        // if a prefix corresponds to a full key we can simply use the full key
        if self.prefixes.is_empty() {
            let prefix = Prefix::new(key.to_vec());
            self.prefix_len = prefix.0.len();
            self.prefixes.insert(prefix);
        } else {
            let prefix = Prefix::new(key.iter().take(self.prefix_len).map(|b| b.clone()).collect::<Vec<_>>());
            self.prefixes.insert(prefix);
        }

        if self.prefixes.len() >= 512 {
            println!("merging");
            self.merge_prefixes();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.prefixes.len()
    }

    pub fn iter(&self) -> Iter<'_, Prefix> {
        self.prefixes.iter()
    }

    pub fn clear(&mut self) {
        self.prefixes.clear();
        self.prefix_len = 0;
    }

    fn merge_prefixes(&mut self) {
        self.prefix_len -= 1;
        let mut new_set: BTreeSet<Prefix> = BTreeSet::new();
        for prefix in self.prefixes.iter() {
            new_set.insert(prefix.truncate(self.prefix_len));
        }

        self.prefixes = new_set;
    }
}

 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: Arc<Db>,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: Arc<Mutex<PrefixSet>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: StateTree,
}

impl StateOrchestrator {
    pub fn new(path: &str) -> Self {
        let conf = Config::new()
        .mode(Mode::HighThroughput)
        .flush_every_ms(Some(50))
        .path(path);

        let db = conf.open().unwrap();
        for name in db.tree_names() {
           let _ = db.drop_tree(name);
        }
        let updates = Arc::new(Mutex::new(PrefixSet::default()));
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

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
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

pub async fn monitor_changes(state: Arc<Mutex<PrefixSet>>, mut subscriber: Subscriber) {
    while let Some(event) = (&mut subscriber).await {
        match event {
           /* EventType::Split { lhs, rhs, parent } => {
                let mut lock = state.lock().expect("failed to lock");
                lock.insert(lhs);
                lock.insert(rhs);
                lock.insert(parent);
            },
            EventType::Merge { lhs, rhs, ..} => {
                let mut lock = state.lock().expect("failed to lock");
                lock.insert(lhs);
                lock.insert(rhs);
            },
            EventType::Node(n) => {
                let mut lock = state.lock().expect("failed to lock");
                lock.insert(n);
            }, */
             EventType::Update(event) => {
                let mut lock = state.lock().expect("failed to lock");
                for (_,k,_) in event.iter() {
                    lock.insert(k.to_vec());
                }
            }
            _ => ()
        }
    }
}
