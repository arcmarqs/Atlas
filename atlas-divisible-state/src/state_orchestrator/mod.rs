use std::collections::{BTreeSet, btree_set::Iter};

use crate::{
    state_tree::StateTree,
    SerializedTree,
};
use serde::{Deserialize, Serialize};
use sled::{Config, Db, Mode, Subscriber, IVec,};
pub const PREFIX_LEN: usize = 7;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Prefix([u8;7]);

impl Prefix {
    pub fn new(prefix: &[u8]) -> Prefix {
        Self(prefix.try_into().expect("incorrect size"))
    }

    pub fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

   // pub fn truncate(&self, len: usize) -> Prefix {
  //      let new_prefix = self.0[..len];
//
  //      Prefix(new_prefix)
 //   }
}
// A bitmap that registers changed prefixes over a set of keys
#[derive(Debug,Default,Clone)]
pub struct PrefixSet {
    pub prefixes: BTreeSet<Prefix>,
}

impl PrefixSet {
    pub fn new() -> PrefixSet {
        Self { 
            prefixes: BTreeSet::default(), 
        }
    }

    pub fn insert(&mut self, key: &[u8]) {

        // if a prefix corresponds to a full key we can simply use the full key
        
        let prefix = Prefix::new(&key[..PREFIX_LEN]);

      //  if self.prefixes.is_empty() {
      //      self.prefix_len = prefix.0.len();
      //      self.prefixes.insert(prefix);
      //  } else {
            self.prefixes.insert(prefix);
       // }

       // if self.prefixes.len() >= 8000 {
       //     println!("merging");
       //     self.merge_prefixes();
       // }
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
       // self.prefix_len = 0;
    }

   // fn merge_prefixes(&mut self) {
  //      self.prefix_len -= 1;
  //      let mut new_set: BTreeSet<Prefix> = BTreeSet::new();
  //      for prefix in self.prefixes.iter() {
  //          new_set.insert(prefix.truncate(self.prefix_len));
   //     }
   //     self.prefixes = new_set;
   // }
}


#[derive(Debug, Clone,)]
pub struct DbWrapper {
    pub db: Db,
 }

 impl Default for DbWrapper {
    fn default() -> Self {
        Self { db: Config::new().open().expect("failed to open") }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: DbWrapper,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: PrefixSet,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: StateTree,
}

impl StateOrchestrator {
    pub fn new(path: &str) -> Self {
        let conf = Config::new()
        .mode(Mode::HighThroughput)
        .path(path);

        let db = conf.open().unwrap();
        
        let updates = PrefixSet::default();

        let ret = Self {
            db: DbWrapper { db },
            updates: updates.clone(),
            mk_tree: StateTree::default(),
        };

      //  let _ = spawn(
     //       monitor_changes(
     //           updates.clone(),
      //          subscriber));

       ret
    }

    pub fn get_subscriber(&self) -> Subscriber {
        self.db.db.watch_prefix(vec![])
    }

    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Option<IVec> {
        self.updates.insert(&key);
        self.db.db.insert(key, value).expect("Error inserting key")
    }

    pub fn remove(&mut self, key: &[u8])-> Option<IVec> {
        self.updates.insert(&key);
        self.db.db.remove(key).expect("error removing key")
    }

    pub fn get(&self, key: &[u8]) -> Option<IVec> {
        self.db.db.get(key).expect("error getting key")
    }

    pub fn generate_id(&self) -> u64 {
        self.db.db.generate_id().expect("Failed to Generate id")
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
    } 

    pub fn get_page<'g>(&self, pid: u64, guard: &'g Guard) -> Option<sled::Node> {
        self.db.export_node(pid,guard)
    }

    pub fn import_page(&self, pid: u64, node: sled::Node) -> Result<(), ()> {
        if let Ok(()) = self.db.import_node(pid, node) {
            Ok(())
        } else {
            Err(())
        }
    } */

    pub fn get_descriptor_inner(&self) -> Result<SerializedTree, ()> {
        self.mk_tree.to_serialized_tree()
    }

}

/* 
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
                    lock.insert(k.as_ref());
                }
            }
            _ => ()
        }
    }
}
*/
