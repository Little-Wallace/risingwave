use std::collections::{HashMap, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{get_vnode_id, StateTableKey, TableKey};
use spin::Mutex;

use crate::bwtree::delta_chain::DeltaChain;
use crate::bwtree::gc_page_collector::GcPageCollector;
use crate::bwtree::index_page::PageType;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::page_id_generator::PageIdGenerator;
use crate::bwtree::page_store::PageStore;
use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::HummockResult;
use crate::storage_value::StorageValue;
use crate::store::WriteOptions;

pub struct DirtyPageUpdates {
    pub pages: Vec<(usize, PageId)>,
    pub vnodes: Vec<(usize, PageId)>,
}

pub struct EngineOptions {
    pub leaf_split_size: usize,
    pub leaf_reconcile_size: usize,
    pub leaf_min_merge_size: usize,
    pub index_split_count: usize,
    pub index_reconcile_count: usize,
    pub index_min_merge_count: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            leaf_reconcile_size: 32 * 1024,
            index_split_count: 256,
            leaf_split_size: 64 * 1024,
            leaf_min_merge_size: 32 * 1024,
            index_reconcile_count: 32,
            index_min_merge_count: 128,
        }
    }
}

pub struct PageInfo {
    right_link: PageId,
    smallest_user_key: Bytes,
    largest_user_key: Bytes,
}

pub struct BwTreeEngine {
    pub(crate) vnodes_map: RwLock<HashMap<usize, (PageId, PageType)>>,
    pub(crate) page_mapping: Arc<MappingTable>,
    pub(crate) page_id_manager: Arc<dyn PageIdGenerator>,
    pub(crate) page_store: PageStore,
    pub(crate) updates: Mutex<HashMap<u64, DirtyPageUpdates>>,
    pub(crate) options: EngineOptions,
    pub(crate) gc_collector: GcPageCollector,
}

impl BwTreeEngine {
    pub fn open_engine(
        vnodes: HashMap<usize, (PageId, PageType)>,
        page_mapping: Arc<MappingTable>,
        page_id_manager: Arc<dyn PageIdGenerator>,
        page_store: PageStore,
        options: EngineOptions,
    ) -> Self {
        Self {
            vnodes_map: RwLock::new(vnodes),
            gc_collector: GcPageCollector::new(page_mapping.clone()),
            page_mapping,
            page_id_manager,
            page_store,
            options,
            updates: Mutex::new(HashMap::default()),
        }
    }

    pub async fn get(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let _guard = self.gc_collector.get_snapshot();
        let vnode_id = get_vnode_id(&table_key.0);
        let pinfo = self.vnodes_map.read().get(&vnode_id).cloned();
        match pinfo {
            Some((page_id, ptp)) => match ptp {
                PageType::Index => self.search_index_page(table_key, &page_id, epoch).await,
                PageType::Leaf => self.search_data_page(table_key, page_id, epoch).await,
            },
            None => {
                return Ok(None);
            }
        }
    }

    pub async fn get_new_page_id(&self) -> HummockResult<PageId> {
        self.page_id_manager.get_new_page_id().await
    }

    pub async fn flush(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        // delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> HummockResult<usize> {
        let _guard = self.gc_collector.get_snapshot();
        let mut dirty_pages = vec![];
        let mut dirty_vnodes = vec![];
        let mut flush_size = 0;
        let partitioned_data = kv_pairs
            .into_iter()
            .group_by(|(k, _)| get_vnode_id(k))
            .into_iter()
            .map(|(k, v)| (k, v.collect::<VecDeque<_>>()))
            .collect_vec();
        let mut page_ids = vec![];
        {
            let vnodes_map = self.vnodes_map.read();
            for (vnode_id, _) in &partitioned_data {
                page_ids.push(vnodes_map.get(vnode_id).cloned());
            }
        }
        for ((vnode_id, mut kvs), pinfo_ret) in partitioned_data.into_iter().zip(page_ids) {
            let mut pinfo = match pinfo_ret {
                Some(pid) => pid,
                None => {
                    let pid = self.get_new_page_id().await?;
                    let (delta_chain, sz) = self.create_leaf_page(
                        pid,
                        kvs.into_iter().collect_vec(),
                        write_options.epoch,
                        write_options.table_id,
                    );
                    flush_size += sz;
                    self.page_mapping.insert_delta(pid, delta_chain);
                    dirty_pages.push((vnode_id, pid));
                    dirty_vnodes.push((vnode_id, pid));
                    let mut guard = self.vnodes_map.write();
                    guard.insert(vnode_id, (pid, PageType::Leaf));
                    continue;
                }
            };
            let mut parent_page_id = INVALID_PAGE_ID;
            while pinfo.1 != PageType::Leaf {
                let index_page = self.page_mapping.get_index_page(&pinfo.0);
                parent_page_id = pinfo.0;
                pinfo = index_page.read().get_page_in_range(&kvs.front().unwrap().0);
            }
            let pid = pinfo.0;
            let mut delta = match self.page_mapping.get_data_chains(&pid) {
                Some(delta) => delta,
                None => {
                    // TODO: using queue to avoid several thread reading one page.
                    self.get_leaf_page_delta(pid).await?
                }
            };
            while !kvs.is_empty() {
                let right_link = {
                    // TODO: use optimistic lock mode to avoid hold mutex too long.
                    let mut current_page = delta.write();
                    match current_page.get_pending_merge_page() {
                        Some(page_id) => page_id,
                        None => {
                            let page = current_page.get_page_ref();
                            let right_link = page.get_right_link();
                            let mut last_data = Vec::with_capacity(kvs.len());
                            while let Some(kv) = kvs.front() {
                                if right_link == INVALID_PAGE_ID || kv.0 < page.largest_user_key {
                                    let kv = kvs.pop_front().unwrap();
                                    last_data.push(kv);
                                } else {
                                    break;
                                }
                            }
                            if !last_data.is_empty() {
                                dirty_pages.push((vnode_id, page.get_page_id()));
                                flush_size += self.ingest_batch(
                                    current_page.deref_mut(),
                                    last_data,
                                    write_options.epoch,
                                    write_options.table_id,
                                );
                            }
                            right_link
                        }
                    }
                };
                if !kvs.is_empty() {
                    assert!(right_link != INVALID_PAGE_ID);
                    // TODO: we need seek again because the next page may be too far away
                    delta = self.get_leaf_page_delta(right_link).await?;
                }
            }
        }
        self.updates.lock().insert(
            write_options.epoch,
            DirtyPageUpdates {
                pages: dirty_pages,
                vnodes: dirty_vnodes,
            },
        );
        Ok(flush_size)
    }

    async fn search_index_page(
        &self,
        table_key: TableKey<Bytes>,
        index_page_id: &PageId,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let leaf_page_id = {
            let index_page = self.page_mapping.get_index_page(index_page_id);
            let page = index_page.read();
            let mut pinfo = page.get_page_in_range(&table_key);
            while pinfo.1 != PageType::Leaf {
                let next_page = self.page_mapping.get_index_page(&pinfo.0);
                pinfo = next_page.read().get_page_in_range(&table_key);
            }
            pinfo.0
        };
        self.search_data_page(table_key, leaf_page_id, epoch).await
    }

    async fn search_data_page(
        &self,
        table_key: TableKey<Bytes>,
        mut leaf_page_id: PageId,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let vk = StateTableKey::new(table_key, epoch);
        loop {
            match self.page_mapping.get_data_chains(&leaf_page_id) {
                Some(delta) => {
                    let guard = delta.read();
                    if !guard.get_page_ref().check_valid_read(&vk.user_key) {
                        leaf_page_id = guard.get_page_ref().get_right_link();
                        continue;
                    }
                    return Ok(guard.get(vk));
                }
                None => {
                    let leaf = match self.page_mapping.get_leaf_page(leaf_page_id) {
                        Some(page) => page.value().clone(),
                        None => self.get_leaf_page(leaf_page_id).await?,
                    };
                    if !leaf.check_valid_read(&vk.user_key) {
                        leaf_page_id = leaf.get_right_link();
                        continue;
                    }
                    return Ok(leaf.get(vk));
                }
            }
        }
    }

    fn create_leaf_page(
        &self,
        pid: PageId,
        kvs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
        table_id: TableId,
    ) -> (DeltaChain, usize) {
        let page = LeafPage::empty(pid, epoch);
        let mut chain = DeltaChain::new(Arc::new(page));
        let items = SharedBufferBatch::build_shared_buffer_item_batches(kvs);
        let sz = SharedBufferBatch::measure_batch_size(&items);
        chain.ingest(SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            items,
            sz,
            vec![],
            table_id,
            None,
        ));
        (chain, sz)
    }

    async fn get_leaf_page(&self, pid: PageId) -> HummockResult<Arc<LeafPage>> {
        let page = self.page_store.get_data_page(pid).await?;
        let page = Arc::new(page);
        self.page_mapping.insert_page(pid, page.clone());
        Ok(page)
    }

    pub(crate) async fn get_leaf_page_delta(
        &self,
        pid: PageId,
    ) -> HummockResult<Arc<RwLock<DeltaChain>>> {
        match self.page_mapping.get_data_chains(&pid) {
            Some(delta) => Ok(delta),
            None => {
                let page = match self.page_mapping.get_leaf_page(pid) {
                    Some(page) => page.value().clone(),
                    None => self.get_leaf_page(pid).await?,
                };
                let mut delta = DeltaChain::new(page);
                Ok(self.page_mapping.insert_delta(pid, delta))
            }
        }
    }

    fn ingest_batch(
        &self,
        delta: &mut DeltaChain,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
        table_id: TableId,
    ) -> usize {
        let items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
        let size = SharedBufferBatch::measure_batch_size(&items);
        let buffer = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            items,
            size,
            vec![],
            table_id,
            None,
        );
        delta.ingest(buffer);
        size
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::BytesMut;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::TableKey;

    use crate::bwtree::bw_tree_engine::{BwTreeEngine, EngineOptions};
    use crate::bwtree::mapping_table::MappingTable;
    use crate::bwtree::page_id_generator::LocalPageIdGenerator;
    use crate::bwtree::page_store::PageStore;
    use crate::bwtree::test_utils::{generate_data_with_partition, get_key_with_partition};
    use crate::store::WriteOptions;

    #[tokio::test]
    async fn test_root_smo() {
        let mut engine = BwTreeEngine::open_engine(
            HashMap::default(),
            Arc::new(MappingTable::new(1, 1024)),
            Arc::new(LocalPageIdGenerator::default()),
            PageStore {},
            EngineOptions {
                leaf_split_size: 150,
                leaf_reconcile_size: 50,
                leaf_min_merge_size: 50,
                index_reconcile_count: 2,
                index_split_count: 6,
                index_min_merge_count: 3,
            },
        );
        let mut write_options = WriteOptions {
            epoch: 1,
            table_id: TableId::new(1),
        };
        engine
            .flush(
                vec![
                    generate_data_with_partition(0, b"abcde", b"v0"),
                    generate_data_with_partition(0, b"abcdf", b"v0"),
                ],
                write_options.clone(),
            )
            .await
            .unwrap();
        let v = engine
            .get(TableKey(get_key_with_partition(0, b"abcde")), 1)
            .await
            .unwrap();
        assert!(v.is_some());
        assert_eq!(v.unwrap().as_ref(), b"v0");
        write_options.epoch = 2;
        engine
            .flush(
                vec![generate_data_with_partition(0, b"abcdfg", b"v1")],
                write_options.clone(),
            )
            .await
            .unwrap();
        let _ = engine.flush_dirty_pages_before(2, 2).await;
        let v = engine
            .get(TableKey(get_key_with_partition(0, b"abcdfg")), 2)
            .await
            .unwrap();
        assert!(v.is_some());
        assert_eq!(v.unwrap().as_ref(), b"v1");
        let mut new_data = vec![];
        let mut prefix = BytesMut::new();
        prefix.extend_from_slice(b"abcde");
        for i in 0..20u64 {
            prefix.extend_from_slice(&i.to_le_bytes());
            new_data.push(generate_data_with_partition(0, &prefix, b"v2"));
            prefix.resize(5, 0);
        }
        write_options.epoch = 3;
        engine.flush(new_data, write_options.clone()).await.unwrap();
        // Flush and split the origin page. It would generate the first index-page.
        let data = engine.flush_dirty_pages_before(3, 3).await.unwrap();
        assert_eq!(data.leaf.len(), 3);
        assert_eq!(data.vnodes.len(), 1);
        prefix.extend_from_slice(&1u64.to_le_bytes());
        let v = engine
            .get(TableKey(get_key_with_partition(0, &prefix)), 3)
            .await
            .unwrap();
        assert_eq!(v.unwrap().as_ref(), b"v2");
        println!("==================================");
        let mut new_data = vec![];
        for i in 20..40u64 {
            prefix.extend_from_slice(&i.to_le_bytes());
            new_data.push(generate_data_with_partition(0, &prefix, b"v2"));
            prefix.resize(5, 0);
        }
        write_options.epoch = 4;
        engine.flush(new_data, write_options.clone()).await.unwrap();
        // Flush and split the origin page. It would generate the first index-page.
        let data = engine.flush_dirty_pages_before(4, 4).await.unwrap();
        assert_eq!(data.leaf.len(), 5);
    }
}
