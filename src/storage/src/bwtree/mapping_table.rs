use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::cache::{CacheableEntry, LruCache};

use crate::bwtree::delta_chain::DeltaChain;
use crate::bwtree::index_page::IndexPageDeltaChain;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::PageId;

pub type PageHolder = CacheableEntry<PageId, Arc<LeafPage>>;

pub struct MappingTable {
    leaf_pages: Arc<LruCache<PageId, Arc<LeafPage>>>,
    // TODO: store index-page in lru-cache to avoid use too much memory.
    index_pages: RwLock<HashMap<PageId, Arc<RwLock<IndexPageDeltaChain>>>>,
    /// TODO: replace it with concurrent hash table.
    delta_chains: RwLock<HashMap<PageId, Arc<RwLock<DeltaChain>>>>,
}

impl MappingTable {
    pub fn new(shard_bits: usize, capacity: usize) -> Self {
        Self {
            leaf_pages: Arc::new(LruCache::new(shard_bits, capacity)),
            index_pages: RwLock::new(HashMap::default()),
            delta_chains: RwLock::new(HashMap::default()),
        }
    }

    pub fn get_data_chains(&self, pid: &PageId) -> Option<Arc<RwLock<DeltaChain>>> {
        self.delta_chains.read().get(pid).cloned()
    }

    pub fn remove_delta_chains(&self, pid: &PageId) {
        self.delta_chains.write().remove(pid);
    }

    pub fn remove_index_delta(&self, pid: &PageId) {
        self.index_pages.write().remove(pid);
    }

    pub fn get_leaf_page(&self, pid: PageId) -> Option<PageHolder> {
        self.leaf_pages.lookup(pid, &pid)
    }

    pub fn insert_page(&self, pid: PageId, page: Arc<LeafPage>) {
        self.leaf_pages.insert(pid, pid, page.page_size(), page);
    }

    pub fn insert_delta(&self, pid: PageId, chain: DeltaChain) -> Arc<RwLock<DeltaChain>> {
        let delta = Arc::new(RwLock::new(chain));
        self.delta_chains.write().insert(pid, delta.clone());
        delta
    }

    // we assume that all index-page are in memory
    pub fn get_index_page(&self, pid: &PageId) -> Arc<RwLock<IndexPageDeltaChain>> {
        self.index_pages.read().get(pid).unwrap().clone()
    }

    pub fn insert_index_delta(&self, pid: PageId, chain: Arc<RwLock<IndexPageDeltaChain>>) {
        self.index_pages.write().insert(pid, chain);
    }
}
