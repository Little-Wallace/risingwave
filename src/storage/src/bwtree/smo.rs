use std::collections::HashMap;
use std::sync::Arc;

use async_recursion::async_recursion;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use risingwave_hummock_sdk::key::get_vnode_id;

use crate::bwtree::base_page::BasePage;
use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::bwtree::data_iterator::MergedSharedBufferIterator;
use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, IndexPageHolder, PageType, SMOType,
    SubtreePageInfo,
};
use crate::bwtree::leaf_page::{Delta, LeafPage};
use crate::bwtree::sorted_data_builder::BlockBuilder;
use crate::bwtree::sorted_record_block::SortedRecordBlock;
use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::HummockResult;

const SMO_MERGE: u8 = 0;
const SMO_SPLIT: u8 = 1;
const SMO_RECONCILE: u8 = 2;
const SMO_CREATE_ROOT_PAGE: u8 = 2;

#[derive(Clone)]
pub struct SMORedoLogRecord {
    // When smo_kind equals SMO_MERGE, there would be several origin_pages and one new_pages.
    // When smo_kind equals SMO_SPLIT, there would be one origin_pages and several new_pages.
    // When smo_kind equals SMO_RECONCILE, there is only one item in origin_pages, and one item in
    // new_pages.
    origin_pages: Vec<PageId>,
    new_pages: Vec<PageId>,
    smo_kind: u8,
}

impl SMORedoLogRecord {
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32_le(self.origin_pages.len() as u32);
        for pid in &self.origin_pages {
            buf.put_u64_le(*pid);
        }
        for pid in &self.new_pages {
            buf.put_u64_le(*pid);
        }
        buf.put_u32_le(self.new_pages.len() as u32);
        buf.put_u8(self.smo_kind);
    }
}

#[derive(Clone)]
pub struct IndexPageRedoLogRecord {
    // only for replay.
    pub redo_logs: Vec<SMORedoLogRecord>,
    pub deltas: Vec<IndexPageDelta>,
    pub update_page_id: PageId,
    pub smo_pages_height: usize,
}

impl IndexPageRedoLogRecord {
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32_le(self.redo_logs.len() as u32);
        for log in &self.redo_logs {
            log.encode_into(buf);
        }
        buf.put_u32_le(self.deltas.len() as u32);
        for delta in &self.deltas {
            delta.encode_into(buf);
        }
        buf.put_u64_le(self.update_page_id);
        buf.put_u64_le(self.smo_pages_height as u64);
    }
}

pub struct CheckpointData {
    pub leaf_deltas: Vec<(PageId, Arc<Delta>)>,
    pub leaf: Vec<Arc<BasePage>>,
    pub index: Vec<(PageId, Bytes)>,
    pub index_redo_log: Vec<IndexPageRedoLogRecord>,
    pub vnodes: HashMap<usize, (PageId, PageType)>,
    pub commited_epoch: u64,
}

impl BwTreeEngine {
    pub(crate) async fn update_leaf_page(
        &self,
        current_page: PageId,
        vnode_id: usize,
        epoch: u64,
        safe_epoch: u64,
        iter: &mut MergedSharedBufferIterator,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<IndexPageRedoLogRecord> {
        let dirty_page = self.get_leaf_page_delta(current_page).await?;
        let (largest_key, last_epoch) = (
            dirty_page.get_base_page().largest_user_key.clone(),
            dirty_page.last_epoch(),
        );
        let mut builder = BlockBuilder::default();
        let mut raw_key = BytesMut::new();
        let mut raw_value = BytesMut::new();
        while iter.is_valid() {
            let current_vnode_id = get_vnode_id(&iter.key().user_key);
            if current_vnode_id != vnode_id {
                break;
            }
            if !largest_key.is_empty() && iter.key().user_key.as_ref().ge(largest_key.as_ref()) {
                break;
            }
            iter.key().encode_into(&mut raw_key);
            iter.value().encode(&mut raw_value);
            builder.add(&raw_key, &raw_value);
            raw_key.clear();
            raw_value.clear();
            iter.next();
        }
        let mut record = IndexPageRedoLogRecord {
            redo_logs: vec![],
            deltas: vec![],
            update_page_id: 0,
            smo_pages_height: 0,
        };
        let delta = Arc::new(Delta::new(builder.build(), last_epoch, epoch));
        let mut origin_page = dirty_page.as_ref().clone();
        origin_page.ingest(delta.clone());
        let (new_pages, origin_page_right_link) = {
            let origin_page_right_link = origin_page.get_base_page().get_right_link();
            if origin_page.need_split(self.options.leaf_split_size)
                || origin_page.need_reconcile(self.options.leaf_reconcile_size)
            {
                (
                    origin_page.apply_to_page(
                        self.options.leaf_split_size,
                        self.options.index_split_count / 2,
                        safe_epoch,
                    ),
                    origin_page_right_link,
                )
            } else {
                self.page_mapping
                    .insert_syncing_page(current_page, Arc::new(origin_page));
                checkpoint.leaf_deltas.push((current_page, delta));
                return Ok(record);
            }
        };
        let mut new_pid = INVALID_PAGE_ID;
        let page_count = new_pages.len();
        let mut leaf_pages = vec![];
        for (idx, mut page) in new_pages.into_iter().rev().enumerate() {
            let last_pid = new_pid;
            if idx + 1 != page_count {
                new_pid = self.get_new_page_id().await?;
                page.set_page_id(new_pid);
            }
            if idx == 0 {
                page.set_right_link(origin_page_right_link);
            } else {
                page.set_right_link(last_pid);
            }
            let base_page = Arc::new(page);
            let p = Arc::new(LeafPage::new(base_page.clone()));
            let current_pid = base_page.get_page_id();
            self.page_mapping.insert_syncing_page(current_pid, p);
            record.deltas.push(IndexPageDelta::new(
                SMOType::Add,
                current_pid,
                epoch,
                base_page.smallest_user_key.clone(),
            ));
            leaf_pages.push(base_page);
        }
        checkpoint.leaf.extend(leaf_pages);
        if page_count > 1 {
            record.redo_logs.push(SMORedoLogRecord {
                origin_pages: vec![current_page],
                new_pages: record
                    .deltas
                    .iter()
                    .map(|delta| delta.son.page_id)
                    .collect(),
                smo_kind: SMO_SPLIT,
            });
        } else {
            record.redo_logs.push(SMORedoLogRecord {
                origin_pages: vec![current_page],
                new_pages: vec![current_page],
                smo_kind: SMO_RECONCILE,
            });
        }
        Ok(record)
    }

    #[async_recursion]
    pub(crate) async fn update_index_page(
        &self,
        current_page: PageId,
        vnode_id: usize,
        epoch: u64,
        safe_epoch: u64,
        iter: &mut MergedSharedBufferIterator,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<IndexPageRedoLogRecord> {
        let index_delta_chain = self.page_mapping.get_index_page(&current_page);
        let mut meet_leaf = false;
        let mut shall_split = false;
        let mut shall_reconcile = false;

        let mut record = IndexPageRedoLogRecord {
            redo_logs: vec![],
            deltas: vec![],
            smo_pages_height: 0,
            update_page_id: current_page,
        };
        let mut sons = vec![];
        {
            let read_guard = index_delta_chain.read();
            meet_leaf = read_guard.get_base_page().get_height() == 1;
            record.smo_pages_height = read_guard.get_base_page().get_height() - 1;
            let sub_tree = read_guard.get_base_page().get_sub_tree();
            // use largest key
            for idx in 0..sub_tree.len() {
                if idx + 1 == sub_tree.len() {
                    sons.push((
                        read_guard.get_base_page().get_largest_key(),
                        sub_tree[idx].1,
                    ));
                } else {
                    sons.push((sub_tree[idx + 1].0.clone(), sub_tree[idx].1));
                }
            }
        }
        for (largest_key, pid) in sons {
            if !iter.is_valid() {
                break;
            }
            let current_vnode_id = get_vnode_id(&iter.key().user_key);
            if current_vnode_id != vnode_id {
                break;
            }
            if !largest_key.is_empty() && iter.key().user_key.as_ref().ge(largest_key.as_ref()) {
                break;
            }
            let son_update = if meet_leaf {
                self.update_leaf_page(pid, vnode_id, epoch, safe_epoch, iter, checkpoint)
                    .await?
            } else {
                self.update_index_page(pid, vnode_id, epoch, safe_epoch, iter, checkpoint)
                    .await?
            };
            if son_update.redo_logs.is_empty() {
                continue;
            }
            record.redo_logs.extend(son_update.redo_logs);
            record.deltas.extend(son_update.deltas);
        }
        {
            let mut write_guard = index_delta_chain.write();
            for delta in &record.deltas {
                write_guard.apply_delta(delta.clone());
            }
            checkpoint.index_redo_log.push(record);
            shall_split = write_guard.shall_split(self.options.index_split_count);
            shall_reconcile = write_guard.shall_reconcile(self.options.index_reconcile_count);
        }
        let mut new_record = IndexPageRedoLogRecord {
            redo_logs: vec![],
            deltas: vec![],
            smo_pages_height: 0,
            update_page_id: INVALID_PAGE_ID,
        };
        if shall_split {
            let merged_pages = self
                .may_merge(epoch, safe_epoch, index_delta_chain.clone(), checkpoint)
                .await?;
            for (merge, index_deltas) in merged_pages {
                new_record.redo_logs.push(SMORedoLogRecord {
                    new_pages: vec![*merge.last().unwrap()],
                    origin_pages: merge,
                    smo_kind: SMO_MERGE,
                });
                new_record.deltas.extend(index_deltas);
            }
            let (mut first_page, other_pages, mut origin_right_link) = {
                let guard = index_delta_chain.read();
                let mut pages = guard.split_to_pages(self.options.index_split_count, epoch);
                pages.reverse();
                let mut first_page = pages.pop().unwrap();
                first_page.set_page_id(guard.get_base_page_id());
                (first_page, pages, guard.get_right_link())
            };
            let mut smo_record = SMORedoLogRecord {
                origin_pages: vec![first_page.get_page_id()],
                new_pages: vec![],
                smo_kind: SMO_SPLIT,
            };
            let mut index_data = BytesMut::new();
            for mut page in other_pages {
                let new_page_id = self.get_new_page_id().await?;
                new_record.deltas.push(IndexPageDelta {
                    son: SubtreePageInfo {
                        page_id: new_page_id,
                        smallest_key: page.get_smallest_key(),
                    },
                    epoch,
                    smo: SMOType::Add,
                });
                smo_record.new_pages.push(new_page_id);
                page.set_page_id(new_page_id);
                page.set_right_link(origin_right_link);
                page.encode_to(&mut index_data);
                origin_right_link = new_page_id;
                let chain = IndexPageDeltaChain::create(vec![], page);
                self.page_mapping.insert_index_delta(new_page_id, chain);
                checkpoint
                    .index
                    .push((new_page_id, Bytes::copy_from_slice(&index_data)));
                index_data.clear();
            }
            new_record.deltas.push(IndexPageDelta {
                son: SubtreePageInfo {
                    page_id: first_page.get_page_id(),
                    smallest_key: first_page.get_smallest_key(),
                },
                epoch,
                smo: SMOType::Add,
            });
            smo_record.new_pages.push(first_page.get_page_id());
            smo_record.new_pages.reverse();
            new_record.redo_logs.push(smo_record);
            new_record.deltas.reverse();
            first_page.set_right_link(origin_right_link);
            index_delta_chain.write().set_page(epoch, first_page);
        } else if shall_reconcile {
            new_record.redo_logs.push(SMORedoLogRecord {
                origin_pages: vec![current_page],
                new_pages: vec![current_page],
                smo_kind: SMO_RECONCILE,
            });
            let data = index_delta_chain.write().reconsile(epoch);
            checkpoint.index.push((current_page, data));
        }
        Ok(new_record)
    }

    pub(crate) async fn flush_shared_buffer(
        &self,
        epoch: u64,
        safe_epoch: u64,
    ) -> HummockResult<CheckpointData> {
        let mut checkpoint = CheckpointData {
            leaf_deltas: vec![],
            leaf: vec![],
            index: vec![],
            index_redo_log: vec![],
            vnodes: Default::default(),
            commited_epoch: 0,
        };

        let mut iter = {
            let shared_buffer = self.shared_buffer.read();
            if shared_buffer.check_data_empty(epoch) {
                return Ok(checkpoint);
            }
            shared_buffer.iter(epoch)
        };
        iter.seek_to_first();
        let mut vnodes_map = self.vnodes_map.load_full().as_ref().clone();
        while iter.is_valid() {
            let last_vnode_id = get_vnode_id(&iter.key().user_key);
            match vnodes_map.get(&last_vnode_id) {
                Some((pid, ptyp)) => {
                    let mut record = match ptyp {
                        PageType::Index => {
                            self.update_index_page(
                                *pid,
                                last_vnode_id,
                                epoch,
                                safe_epoch,
                                &mut iter,
                                &mut checkpoint,
                            )
                            .await?
                        }
                        PageType::Leaf => {
                            self.update_leaf_page(
                                *pid,
                                last_vnode_id,
                                epoch,
                                safe_epoch,
                                &mut iter,
                                &mut checkpoint,
                            )
                            .await?
                        }
                    };
                    if record.deltas.is_empty() && record.redo_logs.is_empty() {
                        continue;
                    }
                    if record.deltas.len() > 1 {
                        let new_pid = self.page_id_manager.get_new_page_id().await?;
                        let index_page = IndexPage::new(
                            new_pid,
                            Bytes::new(),
                            epoch,
                            record.smo_pages_height + 1,
                        );
                        let chain = IndexPageDeltaChain::create(record.deltas.clone(), index_page);
                        self.page_mapping.insert_index_delta(new_pid, chain.clone());
                        checkpoint
                            .index
                            .push((new_pid, chain.write().reconsile(epoch)));
                        assert_eq!(record.redo_logs.len(), 1);
                        record.redo_logs.last_mut().unwrap().smo_kind = SMO_CREATE_ROOT_PAGE;
                        record.update_page_id = new_pid;
                        checkpoint
                            .vnodes
                            .insert(last_vnode_id, (new_pid, PageType::Index));
                    } else {
                        record.redo_logs.last_mut().unwrap().smo_kind = SMO_RECONCILE;
                        checkpoint
                            .vnodes
                            .insert(last_vnode_id, (*pid, PageType::Index));
                    }
                    checkpoint.index_redo_log.push(record);
                }
                None => {
                    let pid = self.get_new_page_id().await?;
                    let mut builder = BlockBuilder::default();
                    let mut raw_key = BytesMut::new();
                    let mut raw_value = BytesMut::new();
                    while iter.is_valid() {
                        if get_vnode_id(&iter.key().user_key) != last_vnode_id {
                            break;
                        }
                        iter.key().encode_into(&mut raw_key);
                        iter.value().encode(&mut raw_value);
                        builder.add(&raw_key, &raw_value);
                        raw_key.clear();
                        raw_value.clear();
                        iter.next();
                    }
                    let raw = SortedRecordBlock::decode(builder.build()).unwrap();
                    let page = Arc::new(BasePage::new(pid, Bytes::new(), Bytes::new(), raw, epoch));
                    checkpoint.leaf.push(page.clone());
                    self.page_mapping
                        .insert_syncing_page(pid, Arc::new(LeafPage::new(page)));
                    checkpoint
                        .vnodes
                        .insert(last_vnode_id, (pid, PageType::Leaf));
                }
            }
        }

        if !checkpoint.vnodes.is_empty() {
            for (k, v) in checkpoint.vnodes.iter() {
                vnodes_map.insert(*k, *v);
            }
            self.vnodes_map.store(Arc::new(vnodes_map));
        }

        // these data has been written into btree.
        self.shared_buffer.write().commit(epoch);
        self.gc_collector.refresh_for_gc();
        Ok(checkpoint)
    }

    fn merge_leaf_pages(
        &self,
        epoch: u64,
        safe_epoch: u64,
        current_size: usize,
        pages: &mut Vec<Arc<LeafPage>>,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<Vec<IndexPageDelta>> {
        // take the left page.
        let first_delta = pages.pop().unwrap();
        // reverse to keep page order same with key order.
        pages.reverse();
        let new_page = first_delta.merge_pages(epoch, safe_epoch, current_size, pages.as_ref());
        let first_pid = new_page.get_page_id();
        let new_page = {
            let mut guard = first_delta.as_ref().clone();
            let new_page = Arc::new(new_page);
            checkpoint.leaf.push(new_page.clone());
            guard.set_new_page(new_page);
            Arc::new(guard)
        };
        self.page_mapping.insert_syncing_page(first_pid, new_page);
        // TODO: remove page after all read-request ended. (we need a epoch-based algorithm to make
        // sure that no-threads would access these pages)
        let mut removed_delta = Vec::with_capacity(pages.len());
        for page in pages.iter() {
            removed_delta.push(IndexPageDelta {
                son: SubtreePageInfo {
                    page_id: page.get_base_page().get_page_id(),
                    smallest_key: page.get_base_page().smallest_user_key.clone(),
                },
                smo: SMOType::Remove,
                epoch,
            });
        }
        Ok(removed_delta)
    }

    fn merge_index_pages(
        &self,
        epoch: u64,
        mut pages: &mut Vec<IndexPageHolder>,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<Vec<IndexPageDelta>> {
        // take the left page.
        let first_delta = pages.pop().unwrap();
        // reverse to keep page order same with key order.
        pages.reverse();
        let new_page = first_delta.read().merge_pages(epoch, pages.as_ref());
        let mut buf = BytesMut::new();
        new_page.encode_to(&mut buf);
        let merged_id = {
            let mut guard = first_delta.write();
            guard.set_new_page(new_page);
            guard.get_base_page().get_page_id()
        };

        checkpoint.index.push((merged_id, buf.freeze()));
        let mut removed_delta = Vec::with_capacity(pages.len());
        for delta_chains in pages.iter() {
            let page = delta_chains.read();
            removed_delta.push(IndexPageDelta {
                son: SubtreePageInfo {
                    page_id: page.get_base_page().get_page_id(),
                    smallest_key: page.get_base_page().get_smallest_key(),
                },
                smo: SMOType::Remove,
                epoch,
            });
        }
        Ok(removed_delta)
    }

    async fn may_merge(
        &self,
        epoch: u64,
        safe_epoch: u64,
        index_page: Arc<RwLock<IndexPageDeltaChain>>,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<Vec<(Vec<PageId>, Vec<IndexPageDelta>)>> {
        let (mut sub_pages, is_leaf) = {
            let guard = index_page.read();
            (
                guard.get_base_page().get_son_pages(),
                guard.get_base_page().get_height() == 1,
            )
        };
        if sub_pages.len() < self.options.index_split_count / 2 {
            return Ok(vec![]);
        }
        let mut merged_pages = vec![];
        let mut last_page_ids = vec![];
        let read_version = self.gc_collector.get_snapshot();

        if is_leaf {
            let mut last_sons = vec![];
            let mut current_size = 0;
            // from right to left
            sub_pages.reverse();
            for pid in &sub_pages {
                let page = self.get_leaf_page_delta(*pid).await?;
                let sz = page.page_size();
                if sz + current_size < self.options.index_min_merge_count {
                    current_size += sz;
                    last_sons.push(page);
                    last_page_ids.push(*pid);
                } else {
                    if last_sons.len() > 1 {
                        let removed_info = self.merge_leaf_pages(
                            epoch,
                            safe_epoch,
                            current_size,
                            &mut last_sons,
                            checkpoint,
                        )?;
                        read_version.add_leaf_page(&last_page_ids);
                        merged_pages.push((std::mem::take(&mut last_page_ids), removed_info));
                    }
                    last_sons.clear();
                    last_page_ids.clear();
                    if sz < self.options.index_min_merge_count {
                        last_sons.push(page);
                        last_page_ids.push(*pid);
                    }
                }
            }
            if last_sons.len() > 1 {
                let removed_info = self.merge_leaf_pages(
                    epoch,
                    safe_epoch,
                    current_size,
                    &mut last_sons,
                    checkpoint,
                )?;
                read_version.add_leaf_page(&last_page_ids);
                merged_pages.push((last_page_ids, removed_info));
            }
        } else {
            let mut last_sons = vec![];
            let mut current_count = 0;
            sub_pages.reverse();
            for pid in &sub_pages {
                let delta = self.page_mapping.get_index_page(pid);
                let sz = {
                    let guard = delta.read();
                    guard.get_base_page().get_son_count()
                };
                if sz + current_count < self.options.index_split_count {
                    current_count += sz;
                    last_page_ids.push(*pid);
                    last_sons.push(delta);
                } else {
                    if last_sons.len() > 1 {
                        let removed_info =
                            self.merge_index_pages(epoch, &mut last_sons, checkpoint)?;
                        read_version.add_index_page(&last_page_ids);
                        merged_pages.push((std::mem::take(&mut last_page_ids), removed_info));
                    }
                    last_sons.clear();
                    last_page_ids.clear();
                    if sz < self.options.index_split_count {
                        last_sons.push(delta);
                        last_page_ids.push(*pid);
                    }
                }
            }
            if last_sons.len() > 1 {
                let removed_info = self.merge_index_pages(epoch, &mut last_sons, checkpoint)?;
                read_version.add_index_page(&last_page_ids);
                merged_pages.push((last_page_ids, removed_info));
            }
        }
        Ok(merged_pages)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_root_smo() {}
}
