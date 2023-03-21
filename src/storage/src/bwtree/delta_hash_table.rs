use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use parking_lot::RwLock;
use crate::bwtree::delta_chain::DeltaChain;

use crate::bwtree::PageID;
use crate::bwtree::index_page::{IndexDeltaChain, IndexPage};

/// TODO: replace it with concurrent hash table.
pub struct DeltaHashTable {
    leaf_pages: RwLock<HashMap<PageID, Arc<Mutex<DeltaChain>>>>,
    // TODO: store index-page in lru-cache to avoid use too much memory.
    index_pages: RwLock<HashMap<PageID, Arc<Mutex<IndexDeltaChain>>>>,
}

impl DeltaHashTable {
    pub fn get_data_page(&self, pid: PageID) -> Option<Arc<Mutex<DeltaChain>>> {
        self.leaf_pages.read().get(&pid).cloned()
    }

    pub fn insert(&self, pid: PageID, page: DeltaChain) {
        let page = Arc::new(Mutex::new(page));
        self.leaf_pages.write().insert(pid, page);
    }

    pub fn get_index_page(&self, pid: PageID, page)
}
