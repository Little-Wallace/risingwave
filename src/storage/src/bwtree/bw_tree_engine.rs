use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{get_vnode_id, StateTableKey, TableKey};
use spin::Mutex;

use crate::bwtree::delta_chain::{Delta, DeltaChain};
use crate::bwtree::index_page::{IndexPageHolder, PageType};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::page_store::PageStore;
use crate::bwtree::{PageID, TypedPage, INVALID_PAGE_ID};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::{HummockResult, SstableObjectIdManagerRef};
use crate::storage_value::StorageValue;
use crate::store::WriteOptions;

const MAX_UPDATE_LEAF_SIZE: usize = 32 * 1024;

pub struct DirtyPageUpdates {
    pub pages: Vec<PageID>,
    pub vnodes: HashMap<usize, PageID>,
    pub is_vnodes_change: bool,
}

pub struct CheckpointData {
    pub leaf_deltas: Vec<(PageID, Arc<Delta>)>,
    pub leaf: Vec<Arc<LeafPage>>,
    pub index: Vec<(PageID, Bytes)>,
    pub index_delta: Vec<(PageID, Bytes)>,
    pub vnodes: HashMap<usize, PageID>,
    pub commited_epoch: u64,
}

pub struct PageInfo {
    parent_link: PageID,
    right_link: PageID,
    smallest_user_key: Bytes,
    largest_user_key: Bytes,
}

pub struct BwTreeEngine {
    pub(crate) vnodes: ArcSwap<HashMap<usize, TypedPage>>,
    pub(crate) page_mapping: Arc<MappingTable>,
    pub(crate) sstable_id_manager: SstableObjectIdManagerRef,
    page_store: PageStore,
    updates: Mutex<HashMap<u64, DirtyPageUpdates>>,
}

impl BwTreeEngine {
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
                self.search_data_page(table_key, *leaf_page_id, INVALID_PAGE_ID, epoch)
                    .await
            }
        }
    }

    pub async fn get_new_page_id(&self) -> HummockResult<PageID> {
        self.sstable_id_manager.get_new_sst_object_id().await
    }

    pub async fn flush(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        // delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> HummockResult<()> {
        let mut dirty_pages = vec![];
        let vnodes = self.vnodes.load_full();
        let mut vnodes_change = false;
        for (vnode_id, d) in &kv_pairs.into_iter().group_by(|(k, _)| get_vnode_id(k)) {
            let kvs = d.collect_vec();
            let (mut pid, mut current_page_type) = match vnodes.get(&vnode_id) {
                None => {
                    let pid = self
                        .create_leaf_page(kvs, Bytes::new(), Bytes::new(), write_options.epoch)
                        .await?;
                    let mut new_vnodes = vnodes.as_ref().clone();
                    new_vnodes.insert(vnode_id, TypedPage::DataPage(pid));
                    vnodes_change = true;
                    self.vnodes.store(Arc::new(new_vnodes));
                    continue;
                }
                Some(TypedPage::DataPage(pid)) => (*pid, PageType::Leaf),
                Some(TypedPage::Index(page)) => {
                    page.read().get_page_in_range(&kvs.first().unwrap().0)
                }
            };
            let mut parent_page_id = INVALID_PAGE_ID;
            while current_page_type != PageType::Leaf {
                let index_page = self.page_mapping.get_index_page(&pid);
                let (next_pid, next_type) =
                    index_page.read().get_page_in_range(&kvs.first().unwrap().0);
                parent_page_id = pid;
                pid = next_pid;
                current_page_type = next_type;
            }
            let mut delta = match self.page_mapping.get_data_chains(&pid) {
                Some(delta) => delta,
                None => {
                    // TODO: using queue to avoid several thread reading one page.
                    self.get_leaf_page_delta(pid, parent_page_id).await?
                }
            };
            let mut last_data = Vec::with_capacity(kvs.len());
            for (key, value) in kvs {
                let current_page = delta.read();
                let page = current_page.get_page();
                if page.get_right_link() == INVALID_PAGE_ID || key < page.largest_user_key {
                    last_data.push((key, value));
                } else {
                    drop(current_page);
                    while !last_data.is_empty() {
                        let (pid, right_link) = self.ingest_batch(
                            &delta,
                            &mut last_data,
                            write_options.epoch,
                            write_options.table_id,
                        );
                        dirty_pages.push(pid);
                        delta = self.get_leaf_page_delta(right_link, parent_page_id).await?;
                    }
                    last_data.push((key, value));
                }
            }
            if !last_data.is_empty() {
                let guard = delta.read();
                let page_id = guard.get_page().get_page_id();
                drop(guard);
                self.ingest_batch(
                    &delta,
                    &mut last_data,
                    write_options.epoch,
                    write_options.table_id,
                );
                dirty_pages.push(page_id);
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
        Ok(())
    }

    pub async fn flush_dirty_pages_before(
        &self,
        epoch: u64,
        safe_epoch: u64,
    ) -> HummockResult<CheckpointData> {
        let mut updates = self
            .updates
            .lock()
            .drain_filter(|k, _v| *k <= epoch)
            .collect_vec();
        updates.sort_by(|a, b| a.0.cmp(&b.0));
        let mut vnodes = HashMap::new();
        for (_, update) in updates.iter().rev() {
            // only keep with the last changed vnodes.
            if update.is_vnodes_change {
                vnodes = update.vnodes.clone();
                break;
            }
        }
        let mut dirty_pages = updates
            .iter()
            .flat_map(|(_, update)| update.pages.clone())
            .collect_vec();
        // sort and dedup because we may change the same page in several epoch.
        dirty_pages.sort();
        dirty_pages.dedup();
        let mut checkpoint = CheckpointData {
            leaf: vec![],
            index: vec![],
            index_delta: vec![],
            vnodes,
            leaf_deltas: vec![],
            commited_epoch: epoch,
        };
        for pid in dirty_pages {
            let dirty_page = self.page_mapping.get_data_chains(&pid).unwrap();
            let delta = match dirty_page.read().flush(epoch) {
                None => continue,
                Some(delta) => delta,
            };
            dirty_page.write().commit(delta.clone(), epoch);
            if dirty_page.read().update_size() > MAX_UPDATE_LEAF_SIZE {
                let new_pages = dirty_page.read().apply_to_page(safe_epoch);
                let mut new_pid = INVALID_PAGE_ID;
                let page_count = new_pages.len();
                let mut leaf_pages = vec![];
                let mut right_buffer = vec![];
                // TODO: use lock to avoid other threads write new memory-delta to the current page.
                for (idx, mut page) in new_pages.into_iter().enumerate() {
                    if idx != 0 {
                        page.set_page_id(new_pid);
                    }
                    if idx + 1 != page_count {
                        new_pid = self.get_new_page_id().await?;
                        page.set_right_link(new_pid);
                    }
                    let p = Arc::new(page);
                    if idx != 0 {
                        if !right_buffer.is_empty() {
                            let mut delta = DeltaChain::new(p.clone());
                            delta.append_delta_from_parent_page(&right_buffer);
                            if delta.buffer_size() > 0 {
                                self.page_mapping.insert_delta(p.get_page_id(), delta);
                            }
                        }
                    } else {
                        // TODO: we can not set right link tree for the origin page (the first one,
                        // we use P to point it). we shall split delta at first and then register
                        // new right pages, finally set right link for the origin page.
                        // There would be a middle state which both read-thread and write-state
                        // would see. We would record split-key and origin page id in all right
                        // pages. All read-request would search P at first.
                        // And then the P would register right link to right pages. Finally P would
                        // move all delta to rest pages and cancel their middle-state.
                        right_buffer = dirty_page.write().set_new_page(p.clone());
                    }
                    self.page_mapping.insert_page(p.get_page_id(), p.clone());
                    leaf_pages.push(p);
                }
                checkpoint.leaf.extend(leaf_pages.clone());
                if leaf_pages.len() > 1 {
                    self.exec_structure_modification_operation(leaf_pages, epoch, &mut checkpoint)
                        .await?;
                }
            } else {
                checkpoint.leaf_deltas.push((pid, delta));
            }
        }
        Ok(checkpoint)
    }

    async fn search_index_page(
        &self,
        table_key: TableKey<Bytes>,
        index_page: &IndexPageHolder,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let page = index_page.read();
        let mut parent_link = page.get_base_page().get_page_id();
        let mut pinfo = page.get_page_in_range(&table_key);
        while pinfo.1 != PageType::Leaf {
            let next_page = self.page_mapping.get_index_page(&pinfo.0);
            parent_link = pinfo.0;
            pinfo = next_page.read().get_page_in_range(&table_key);
        }
        self.search_data_page(table_key, pinfo.0, parent_link, epoch)
            .await
    }

    async fn search_data_page(
        &self,
        table_key: TableKey<Bytes>,
        mut leaf_page_id: PageID,
        parent_link: PageID,
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
                    let leaf = self.get_leaf_page(leaf_page_id, parent_link).await?;
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
    ) -> HummockResult<PageID> {
        let pid = self.get_new_page_id().await?;
        let page = LeafPage::build(pid, kvs, smallest_user_key, largest_user_key, epoch);
        self.page_mapping.insert_page(pid, Arc::new(page));
        Ok(pid)
    }

    async fn get_leaf_page(
        &self,
        pid: PageID,
        parent_page_id: PageID,
    ) -> HummockResult<Arc<LeafPage>> {
        let mut page = self.page_store.get_data_page(pid).await?;
        let info = self.find_next_page(parent_page_id, &page.get_smallest_key_in_data());
        // TODO: the parent link and right link may be changed by other threads. we can compare
        // epoch before set this page to page-mapping because every reconsile operation would
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
        pid: PageID,
        parent_page_id: PageID,
    ) -> HummockResult<Arc<RwLock<DeltaChain>>> {
        match self.page_mapping.get_data_chains(&pid) {
            Some(delta) => Ok(delta),
            None => {
                let page = match self.page_mapping.get_leaf_page(pid) {
                    Some(page) => page.value().clone(),
                    None => self.get_leaf_page(pid, parent_page_id).await?,
                };
                let delta = DeltaChain::new(page);
                Ok(Arc::new(RwLock::new(delta)))
            }
        }
    }

    fn find_next_page(&self, mut parent_link: PageID, user_key: &Bytes) -> PageInfo {
        while parent_link != INVALID_PAGE_ID {
            let parent_page = self.page_mapping.get_index_page(&parent_link);
            let page = parent_page.read();
            let mut right_link = page.get_right_link_in_range(&user_key);
            let (smallest, mut largest) = page.get_base_page().get_index_key_in_range(user_key);
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

    fn find_first_page_in_subtree(&self, page_id: PageID, target_depth: usize) -> (PageID, Bytes) {
        let mut current = page_id;
        loop {
            let parent_page = self.page_mapping.get_index_page(&current);
            let page = parent_page.read();
            let height = page.get_base_page().get_height();
            let (left_smallest, left_link) = page.get_base_page().get_left_page_info();
            if height == target_depth + 1 {
                return (left_link, left_smallest);
            }
            current = left_link;
        }
    }

    fn ingest_batch(
        &self,
        delta: &Arc<RwLock<DeltaChain>>,
        kv_pairs: &mut Vec<(Bytes, StorageValue)>,
        epoch: u64,
        table_id: TableId,
    ) -> (PageID, PageID) {
        let mut guard = delta.write();
        let p = guard.get_page_ref();
        let ret = (p.get_page_id(), p.get_right_link());
        let ingest_data = if p.get_right_link() != INVALID_PAGE_ID
            && p.largest_user_key <= kv_pairs.last().unwrap().0
        {
            let pos = kv_pairs.partition_point(|(k, _)| k.lt(&p.largest_user_key));
            let origin = kv_pairs.split_off(pos);
            std::mem::replace(kv_pairs, origin)
        } else {
            std::mem::take(kv_pairs)
        };
        let items = SharedBufferBatch::build_shared_buffer_item_batches(ingest_data);
        let size = SharedBufferBatch::measure_batch_size(&items);
        let buffer = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            items,
            size,
            vec![],
            table_id,
            None,
        );
        guard.ingest(buffer);
        ret
    }
}
