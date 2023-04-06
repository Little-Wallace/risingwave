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
use crate::bwtree::index_page::{IndexPageHolder, PageType};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::page_id_generator::PageIdGenerator;
use crate::bwtree::page_store::PageStore;
use crate::bwtree::{PageId, TypedPage, INVALID_PAGE_ID};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::HummockResult;
use crate::storage_value::StorageValue;
use crate::store::WriteOptions;

pub struct DirtyPageUpdates {
    pub pages: Vec<PageId>,
    pub vnodes: HashMap<usize, PageId>,
    pub is_vnodes_change: bool,
}

pub struct EngineOptions {
    pub leaf_split_size: usize,
    pub leaf_reconcile_size: usize,
    pub leaf_min_merge_size: usize,
    pub index_split_count: usize,
    pub index_reconcile_count: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            leaf_reconcile_size: 32 * 1024,
            index_split_count: 256,
            leaf_split_size: 64 * 1024,
            leaf_min_merge_size: 16 * 1024,
            index_reconcile_count: 32,
        }
    }
}

pub struct PageInfo {
    parent_link: PageId,
    right_link: PageId,
    smallest_user_key: Bytes,
    largest_user_key: Bytes,
}

pub struct BwTreeEngine {
    pub(crate) vnodes: ArcSwap<HashMap<usize, TypedPage>>,
    pub(crate) page_mapping: Arc<MappingTable>,
    pub(crate) page_id_manager: Arc<dyn PageIdGenerator>,
    pub(crate) page_store: PageStore,
    pub(crate) updates: Mutex<HashMap<u64, DirtyPageUpdates>>,
    pub(crate) options: EngineOptions,
}

impl BwTreeEngine {
    pub fn open_engine(
        vnodes: HashMap<usize, TypedPage>,
        page_mapping: Arc<MappingTable>,
        page_id_manager: Arc<dyn PageIdGenerator>,
        page_store: PageStore,
        options: EngineOptions,
    ) -> Self {
        Self {
            vnodes: ArcSwap::new(Arc::new(vnodes)),
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
        let vnode_id = get_vnode_id(&table_key.0);
        let vnodes = self.vnodes.load();
        let page = match vnodes.get(&vnode_id) {
            None => return Ok(None),
            Some(v) => v,
        };
        match page {
            TypedPage::Index(index_page) => {
                self.search_index_page(table_key, index_page, epoch).await
            }
            TypedPage::DataPage(leaf_page_id) => {
                println!("find data page");
                self.search_data_page(table_key, *leaf_page_id, INVALID_PAGE_ID, epoch)
                    .await
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
        let mut dirty_pages = vec![];
        let vnodes = self.vnodes.load_full();
        let mut vnodes_change = false;
        let mut flush_size = 0;
        let partitioned_data = kv_pairs
            .into_iter()
            .group_by(|(k, _)| get_vnode_id(k))
            .into_iter()
            .map(|(k, v)| (k, v.collect::<VecDeque<_>>()))
            .collect_vec();
        for (vnode_id, mut kvs) in partitioned_data {
            let (mut pid, mut current_page_type) = match vnodes.get(&vnode_id) {
                None => {
                    println!("create new data page");
                    let (pid, sz) = self
                        .create_leaf_page(
                            kvs.into_iter().collect_vec(),
                            Bytes::new(),
                            Bytes::new(),
                            write_options.epoch,
                        )
                        .await?;
                    flush_size += sz;
                    let mut new_vnodes = vnodes.as_ref().clone();
                    new_vnodes.insert(vnode_id, TypedPage::DataPage(pid));
                    vnodes_change = true;
                    self.vnodes.store(Arc::new(new_vnodes));
                    continue;
                }
                Some(TypedPage::DataPage(pid)) => (*pid, PageType::Leaf),
                Some(TypedPage::Index(page)) => {
                    page.read().get_page_in_range(&kvs.front().unwrap().0)
                }
            };
            let mut parent_page_id = INVALID_PAGE_ID;
            while current_page_type != PageType::Leaf {
                let index_page = self.page_mapping.get_index_page(&pid);
                let (next_pid, next_type) =
                    index_page.read().get_page_in_range(&kvs.front().unwrap().0);
                parent_page_id = pid;
                pid = next_pid;
                current_page_type = next_type;
            }
            println!("find leaf page: {}", pid);
            let mut delta = match self.page_mapping.get_data_chains(&pid) {
                Some(delta) => delta,
                None => {
                    // TODO: using queue to avoid several thread reading one page.
                    self.get_leaf_page_delta(pid, parent_page_id).await?
                }
            };
            while !kvs.is_empty() {
                let right_link = {
                    let mut current_page = delta.write();
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
                        dirty_pages.push(page.get_page_id());
                        flush_size += self.ingest_batch(
                            current_page.deref_mut(),
                            last_data,
                            write_options.epoch,
                            write_options.table_id,
                        );
                    }
                    right_link
                };
                if !kvs.is_empty() {
                    assert!(right_link != INVALID_PAGE_ID);
                    // TODO: we need seek again because the next page may be too far away
                    delta = self.get_leaf_page_delta(right_link, parent_page_id).await?;
                }
            }
        }
        let mut new_vnodes = HashMap::with_capacity(vnodes.len());
        if vnodes_change {
            let vnodes = self.vnodes.load();
            for (k, v) in vnodes.iter() {
                match v {
                    TypedPage::Index(page) => {
                        new_vnodes.insert(*k, page.read().get_base_page().get_page_id());
                    }
                    TypedPage::DataPage(pid) => {
                        new_vnodes.insert(*k, *pid);
                    }
                }
            }
        }
        self.updates.lock().insert(
            write_options.epoch,
            DirtyPageUpdates {
                vnodes: new_vnodes,
                pages: dirty_pages,
                is_vnodes_change: vnodes_change,
            },
        );
        Ok(flush_size)
    }

    async fn search_index_page(
        &self,
        table_key: TableKey<Bytes>,
        index_page: &IndexPageHolder,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let (leaf_page_id, index_page_id) = {
            let page = index_page.read();
            let mut parent_link = page.get_base_page().get_page_id();
            let mut pinfo = page.get_page_in_range(&table_key);
            while pinfo.1 != PageType::Leaf {
                let next_page = self.page_mapping.get_index_page(&pinfo.0);
                parent_link = pinfo.0;
                pinfo = next_page.read().get_page_in_range(&table_key);
            }
            (pinfo.0, parent_link)
        };
        self.search_data_page(table_key, leaf_page_id, index_page_id, epoch)
            .await
    }

    async fn search_data_page(
        &self,
        table_key: TableKey<Bytes>,
        mut leaf_page_id: PageId,
        parent_link: PageId,
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
                        None => self.get_leaf_page(leaf_page_id, parent_link).await?,
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

    async fn create_leaf_page(
        &self,
        kvs: Vec<(Bytes, StorageValue)>,
        smallest_user_key: Bytes,
        largest_user_key: Bytes,
        epoch: u64,
    ) -> HummockResult<(PageId, usize)> {
        let pid = self.get_new_page_id().await?;
        let page = LeafPage::build(pid, kvs, smallest_user_key, largest_user_key, epoch);
        let sz = page.page_size();
        self.page_mapping.insert_page(pid, Arc::new(page));
        Ok((pid, sz))
    }

    async fn get_leaf_page(
        &self,
        pid: PageId,
        parent_page_id: PageId,
    ) -> HummockResult<Arc<LeafPage>> {
        let mut page = self.page_store.get_data_page(pid).await?;
        let info = self.find_next_page(parent_page_id, &page.get_smallest_key_in_data());
        // TODO: the parent link and right link may be changed by other threads. we can compare
        // epoch before set this page to page-mapping because every reconcile operation would
        // generate a new page with larger epoch.
        page.set_right_link(info.right_link);
        page.set_parent_link(info.parent_link);
        page.smallest_user_key = info.smallest_user_key;
        page.largest_user_key = info.largest_user_key;
        let page = Arc::new(page);
        self.page_mapping.insert_page(pid, page.clone());
        Ok(page)
    }

    async fn get_leaf_page_delta(
        &self,
        pid: PageId,
        parent_page_id: PageId,
    ) -> HummockResult<Arc<RwLock<DeltaChain>>> {
        match self.page_mapping.get_data_chains(&pid) {
            Some(delta) => Ok(delta),
            None => {
                let page = match self.page_mapping.get_leaf_page(pid) {
                    Some(page) => page.value().clone(),
                    None => self.get_leaf_page(pid, parent_page_id).await?,
                };
                println!("create new delta");
                let delta = DeltaChain::new(page);
                Ok(self.page_mapping.insert_delta(pid, delta))
            }
        }
    }

    fn find_next_page(&self, mut parent_link: PageId, user_key: &Bytes) -> PageInfo {
        while parent_link != INVALID_PAGE_ID {
            let parent_page = self.page_mapping.get_index_page(&parent_link);
            let page = parent_page.read();
            let right_link = page.get_right_link_in_range(&user_key);
            let (smallest, largest) = page.get_base_page().get_index_key_in_range(user_key);
            let info = PageInfo {
                parent_link,
                right_link,
                smallest_user_key: smallest,
                largest_user_key: largest,
            };
            if right_link != INVALID_PAGE_ID {
                return info;
            }
            let parent_right_link = page.get_right_link();
            if parent_right_link == INVALID_PAGE_ID {
                return info;
            }
            parent_link = parent_right_link;
        }
        PageInfo {
            parent_link,
            right_link: INVALID_PAGE_ID,
            smallest_user_key: Default::default(),
            largest_user_key: Default::default(),
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
        println!("=================second=========");
        write_options.epoch = 2;
        engine
            .flush(
                vec![generate_data_with_partition(0, b"abcdfg", b"v1")],
                write_options.clone(),
            )
            .await
            .unwrap();
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
        let _ = engine.flush_dirty_pages_before(3, 3).await;
    }
}
