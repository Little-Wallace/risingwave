use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::cache::LruCache;

use crate::bwtree::index_page::IndexPageDeltaChain;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::page_store::PageStoreRef;
use crate::bwtree::PageId;
use crate::hummock::{HummockError, HummockResult};

pub struct MappingTable {
    leaf_pages: Arc<LruCache<PageId, Arc<LeafPage>>>,
    // TODO: store index-page in lru-cache to avoid use too much memory.
    index_pages: RwLock<HashMap<PageId, Arc<RwLock<IndexPageDeltaChain>>>>,
    /// TODO: replace it with concurrent hash table.
    syncing_pages: RwLock<HashMap<PageId, Arc<LeafPage>>>,
    page_store: PageStoreRef,
}

impl MappingTable {
    pub fn new(shard_bits: usize, capacity: usize, page_store: PageStoreRef) -> Self {
        Self {
            leaf_pages: Arc::new(LruCache::new(shard_bits, capacity)),
            index_pages: RwLock::new(HashMap::default()),
            syncing_pages: RwLock::new(HashMap::default()),
            page_store,
        }
    }

    pub async fn get_or_fetch_page(&self, pid: PageId) -> HummockResult<Arc<LeafPage>> {
        {
            if let Some(holder) = self.syncing_pages.read().get(&pid) {
                return Ok(holder.clone());
            }
        }

        let entry = self
            .leaf_pages
            .lookup_with_request_dedup::<_, HummockError, _>(pid, pid, || {
                let store = self.page_store.clone();
                async move {
                    let page = store.get_data_page(pid).await?;
                    let sz = page.page_size();
                    Ok((Arc::new(page), sz))
                }
            })
            .await?;
        {
            let mut guard = self.syncing_pages.write();
            if let Some(holder) = guard.get(&pid) {
                return Ok(holder.clone());
            }
            let page = entry.value().as_ref().clone();
            let holder = Arc::new(page);
            guard.insert(pid, holder.clone());
            Ok(holder)
        }
    }

    pub fn remove_delta_chains(&self, pid: &PageId) {
        self.syncing_pages.write().remove(pid);
    }

    pub fn remove_index_delta(&self, pid: &PageId) {
        self.index_pages.write().remove(pid);
    }

    pub fn get_leaf_page(&self, pid: PageId) -> Option<Arc<LeafPage>> {
        if let Some(page) = self.syncing_pages.read().get(&pid) {
            return Some(page.clone());
        }
        if let Some(entry) = self.leaf_pages.lookup(pid, &pid) {
            return Some(entry.value().clone());
        }
        None
    }

    pub fn insert_page(&self, pid: PageId, page: Arc<LeafPage>) {
        self.leaf_pages.insert(pid, pid, page.page_size(), page);
    }

    pub fn insert_syncing_page(&self, pid: PageId, page: Arc<LeafPage>) {
        self.syncing_pages.write().insert(pid, page);
    }

    pub fn commit_page(&self, pid: PageId) {
        let mut guard = self.syncing_pages.write();
        if let Some(page) = guard.remove(&pid) {
            self.leaf_pages.insert(pid, pid, page.page_size(), page);
        }
    }

    // we assume that all index-page are in memory
    pub fn get_index_page(&self, pid: &PageId) -> Arc<RwLock<IndexPageDeltaChain>> {
        self.index_pages.read().get(pid).unwrap().clone()
    }

    pub fn insert_index_delta(&self, pid: PageId, chain: Arc<RwLock<IndexPageDeltaChain>>) {
        self.index_pages.write().insert(pid, chain);
    }
}
