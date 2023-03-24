use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use parking_lot::RwLock;
use risingwave_common::cache::{CacheableEntry, LookupResponse, LruCache, LruCacheEventListener};

use crate::bwtree::delta_chain::DeltaChain;
use crate::bwtree::index_page::{IndexPage, IndexPageDeltaChain};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::PageID;

pub type PageHolder = CacheableEntry<PageID, Arc<LeafPage>>;

pub struct MappingTable {
    leaf_pages: Arc<LruCache<PageID, Arc<LeafPage>>>,
    // TODO: store index-page in lru-cache to avoid use too much memory.
    index_pages: RwLock<HashMap<PageID, Arc<RwLock<IndexPageDeltaChain>>>>,
    /// TODO: replace it with concurrent hash table.
    delta_chains: Arc<RwLock<HashMap<PageID, Arc<RwLock<DeltaChain>>>>>,
}

impl MappingTable {
    pub fn get_data_chains(&self, pid: &PageID) -> Option<Arc<RwLock<DeltaChain>>> {
        self.delta_chains.read().get(pid).cloned()
    }

    pub fn remove_delta_chains(&self, pid: &PageID) {
        self.delta_chains.write().remove(pid);
    }

    pub fn get_leaf_page(&self, pid: PageID) -> Option<PageHolder> {
        self.leaf_pages.lookup(pid, &pid)
    }

    pub fn insert_page(&self, pid: PageID, page: Arc<LeafPage>) {
        self.leaf_pages.insert(pid, pid, page.page_size(), page);
    }

    pub fn insert_delta(&self, pid: PageID, chain: DeltaChain) {
        self.delta_chains
            .write()
            .insert(pid, Arc::new(RwLock::new(chain)));
    }

    // we assume that all index-page are in memory
    pub fn get_index_page(&self, pid: &PageID) -> Arc<RwLock<IndexPageDeltaChain>> {
        self.index_pages.read().get(pid).unwrap().clone()
    }

    pub fn insert_index_delta(&self, pid: PageID, chain: Arc<RwLock<IndexPageDeltaChain>>) {
        self.index_pages.write().insert(pid, chain);
    }
}
