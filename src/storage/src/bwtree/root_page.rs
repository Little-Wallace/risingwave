use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{get_vnode_id, TableKey};
use spin::Mutex;

use crate::bwtree::delta_chain::{Delta, DeltaChain};
use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, PageType, SMOType,
};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::page_store::PageStore;
use crate::bwtree::sorted_data_builder::BlockBuilder;
use crate::bwtree::sorted_record_block::SortedRecordBlock;
use crate::bwtree::{PageID, TypedPage, VKey, INVALID_PAGE_ID};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SstableObjectIdManager, SstableObjectIdManagerRef};
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

pub struct RootPage {
    pub(crate) vnodes: ArcSwap<HashMap<usize, TypedPage>>,
    pub(crate) page_mapping: Arc<MappingTable>,
    pub(crate) sstable_id_manager: SstableObjectIdManagerRef,
    page_store: PageStore,
    updates: Mutex<HashMap<u64, DirtyPageUpdates>>,
}

impl RootPage {
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

    pub async fn flush_inner(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        // delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> HummockResult<()> {
        let mut dirty_pages = vec![];
        let vnodes = self.vnodes.load_full();
        let mut vnodes_change = false;
        for (vnode_id, d) in &kv_pairs.into_iter().group_by(|(k, _)| get_vnode_id(k)) {
            let mut kvs = d.collect_vec();
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
                let mut page = current_page.get_page();
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
            .drain_filter(|k, v| *k <= epoch)
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
                let mut new_pages = dirty_page.read().apply_to_page(safe_epoch);
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
        index_page: &Arc<RwLock<IndexPageDeltaChain>>,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        let page = index_page.read();
        let mut parent_link = page.get_base_page().get_page_id();
        let mut pinfo = page.get_page_in_range(&table_key);
        while pinfo.1 != PageType::Leaf {
            let next_page = self.page_mapping.get_index_page(&pinfo.0);
            pinfo = next_page.read().get_page_in_range(&table_key);
        }
        self.search_data_page(table_key, pinfo.0, parent_link, epoch)
            .await
    }

    async fn search_data_page(
        &self,
        table_key: TableKey<Bytes>,
        mut leaf_page_id: PageID,
        mut parent_link: PageID,
        epoch: u64,
    ) -> HummockResult<Option<Bytes>> {
        loop {
            // TODO: do not create delta-chains if there is only base page data in tree.
            let delta = match self.page_mapping.get_data_chains(&leaf_page_id) {
                None => {
                    let mut raw_key = BytesMut::new();
                    let vk = VKey {
                        user_key: table_key.0,
                        epoch,
                    };
                    vk.encode_to(&mut raw_key);
                    let d = match self.page_mapping.get_leaf_page(leaf_page_id) {
                        None => self.get_leaf_page_delta(leaf_page_id, parent_link).await?,
                        Some(page) => {
                            return Ok(page.value().get(&raw_key));
                        }
                    };
                    return Ok(d.read().get(&vk.user_key, epoch));
                }
                Some(d) => d,
            };
            let d = delta.read();
            let next_link = d.get_page_ref().get_right_link();
            if next_link != INVALID_PAGE_ID && d.get_page_ref().largest_user_key.le(&table_key.0) {
                leaf_page_id = next_link;
                continue;
            }
            return Ok(d.get(&table_key, epoch));
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
        let mut builder = BlockBuilder::default();
        let mut raw_key = BytesMut::new();
        let mut raw_value = BytesMut::new();
        for (k, v) in kvs {
            let vk = VKey { user_key: k, epoch };
            let v: HummockValue<Bytes> = v.into();
            vk.encode_to(&mut raw_key);
            v.encode(&mut raw_value);
            builder.add(raw_key.as_ref(), raw_value.as_ref());
        }
        let raw = SortedRecordBlock::decode(builder.build(), 0).unwrap();
        let mut page = LeafPage::new(pid, smallest_user_key, largest_user_key, raw);
        self.page_mapping.insert_page(pid, Arc::new(page));
        Ok(pid)
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
                    None => {
                        let mut page = self.page_store.get_data_page(pid).await?;
                        let (right_link, smallest_key, largest_key) =
                            self.find_next_page(parent_page_id, &page.get_smallest_key_in_data());
                        page.set_right_link(right_link);
                        page.set_parent_link(parent_page_id);
                        page.smallest_user_key = smallest_key;
                        page.largest_user_key = largest_key;
                        let page = Arc::new(page);
                        self.page_mapping.insert_page(pid, page.clone());
                        page
                    }
                };
                let delta = DeltaChain::new(page);
                Ok(Arc::new(RwLock::new(delta)))
            }
        }
    }

    fn find_next_page(&self, parent_link: PageID, user_key: &Bytes) -> (PageID, Bytes, Bytes) {
        if parent_link != INVALID_PAGE_ID {
            let parent_page = self.page_mapping.get_index_page(&parent_link);
            let page = parent_page.read();
            let mut right_link = page.get_right_link_in_range(&user_key);
            let (smallest, mut largest) = page.get_base_page().get_index_key_in_range(user_key);
            if right_link != INVALID_PAGE_ID {
                return (right_link, smallest, largest);
            }
            let parent_right_link = page.get_right_link();
            if parent_right_link != INVALID_PAGE_ID {
                drop(page);
                // 0 means search to leaf page
                let info = self.find_first_page_in_subtree(parent_right_link, 0);
                right_link = info.0;
                largest = info.1;
            }
            return (right_link, smallest, largest);
        }
        (INVALID_PAGE_ID, Bytes::new(), Bytes::new())
    }

    fn find_first_page_in_subtree(&self, page_id: PageID, target_depth: usize) -> (PageID, Bytes) {
        let mut current = page_id;
        loop {
            let parent_page = self.page_mapping.get_index_page(&page_id);
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
