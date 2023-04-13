use std::collections::HashMap;
use std::sync::Arc;

use async_recursion::async_recursion;
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use parking_lot::RwLock;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::bwtree::delta_chain::{Delta, DeltaChain};
use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, IndexPageHolder, PageType, SMOType,
    SubtreePageInfo,
};
use crate::bwtree::leaf_page::LeafPage;
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

#[derive(Clone)]
pub struct IndexPageRedoLogRecord {
    pub redo_logs: Vec<SMORedoLogRecord>,
    pub deltas: Vec<IndexPageDelta>,
    pub update_page_id: PageId,
    pub smo_pages_height: usize,
}

pub struct CheckpointData {
    pub leaf_deltas: Vec<(PageId, Arc<Delta>)>,
    pub leaf: Vec<Arc<LeafPage>>,
    pub index: Vec<(PageId, Bytes)>,
    pub index_redo_log: Vec<IndexPageRedoLogRecord>,
    pub vnodes: HashMap<usize, (PageId, PageType)>,
    pub commited_epoch: u64,
}

impl BwTreeEngine {
    #[async_recursion]
    async fn update_index_page(
        &self,
        current_page: PageId,
        mut dirty_index_page_changes: Vec<(Bytes, Vec<IndexPageDelta>)>,
        epoch: u64,
        safe_epoch: u64,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<IndexPageRedoLogRecord> {
        let index_delta_chain = self.page_mapping.get_index_page(&current_page);
        let mut next_changes = vec![];
        let mut meet_leaf = false;
        let mut shall_split = false;
        let mut shall_reconcile = false;

        let mut record = IndexPageRedoLogRecord {
            redo_logs: vec![],
            deltas: vec![],
            smo_pages_height: 0,
            update_page_id: current_page,
        };
        {
            let read_guard = index_delta_chain.read();
            meet_leaf = read_guard.get_base_page().get_height() == 1;
            record.smo_pages_height = read_guard.get_base_page().get_height() - 1;
            if !meet_leaf {
                let groups = dirty_index_page_changes.into_iter().group_by(|(k, _)| {
                    let (pid, _) = read_guard.get_page_in_range(&k);
                    pid
                });
                for (next_page_id, sons) in groups.into_iter() {
                    next_changes.push((next_page_id, sons.collect_vec()));
                }
            } else {
                next_changes.push((INVALID_PAGE_ID, dirty_index_page_changes));
            }
        }

        if !meet_leaf {
            for (next_page_id, sons) in next_changes {
                let r = self
                    .update_index_page(next_page_id, sons, epoch, safe_epoch, checkpoint)
                    .await?;
                if !r.redo_logs.is_empty() {
                    record.deltas.extend(r.deltas);
                    record.redo_logs.extend(r.redo_logs);
                }
            }
            let mut write_guard = index_delta_chain.write();
            for delta in &record.deltas {
                write_guard.apply_delta(delta.clone());
            }
            checkpoint.index_redo_log.push(record);
            shall_split = write_guard.shall_split(self.options.index_split_count);
            shall_reconcile = write_guard.shall_reconcile(self.options.index_reconcile_count);
        } else {
            let mut write_guard = index_delta_chain.write();
            for (_, changes) in next_changes {
                for (_, deltas) in changes {
                    record.redo_logs.push(SMORedoLogRecord {
                        origin_pages: vec![deltas[0].son.page_id],
                        new_pages: deltas.iter().map(|change| change.son.page_id).collect_vec(),
                        smo_kind: SMO_SPLIT,
                    });
                    for delta in &deltas {
                        write_guard.apply_delta(delta.clone());
                    }
                    record.deltas.extend(deltas);
                }
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

    pub(crate) async fn execute_smo(
        &self,
        mut dirty_index_page_changes: Vec<(usize, Bytes, Vec<IndexPageDelta>)>,
        epoch: u64,
        safe_epoch: u64,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<()> {
        let vnodes_map = self.vnodes_map.read().clone();
        dirty_index_page_changes.sort_by_key(|a| a.0);
        let dirty_index_page_changes = dirty_index_page_changes
            .into_iter()
            .group_by(|a| a.0)
            .into_iter()
            .map(|(a, b)| (a, b.collect_vec()))
            .collect_vec();
        for (vnode_id, page_changes) in dirty_index_page_changes {
            let (index_page_id, page_type) = vnodes_map.get(&vnode_id).unwrap().clone();
            let mut record = IndexPageRedoLogRecord {
                redo_logs: vec![],
                deltas: vec![],
                smo_pages_height: 0,
                update_page_id: INVALID_PAGE_ID,
            };
            if page_type == PageType::Leaf {
                let deltas = page_changes
                    .into_iter()
                    .flat_map(|(_, son_key, deltas)| deltas)
                    .collect_vec();
                record.redo_logs.push(SMORedoLogRecord {
                    origin_pages: vec![index_page_id],
                    new_pages: deltas.iter().map(|d| d.son.page_id).collect_vec(),
                    smo_kind: SMO_CREATE_ROOT_PAGE,
                });
                record.deltas = deltas;
            } else {
                let mut sons = page_changes
                    .into_iter()
                    .map(|(_, son_key, deltas)| (son_key, deltas))
                    .collect_vec();
                sons.sort_by(|a, b| a.0.cmp(&b.0));
                record = self
                    .update_index_page(index_page_id, sons, epoch, safe_epoch, checkpoint)
                    .await?;
            }
            if record.deltas.is_empty() && record.redo_logs.is_empty() {
                continue;
            }
            let new_pid = self.page_id_manager.get_new_page_id().await?;
            let index_page =
                IndexPage::new(new_pid, Bytes::new(), epoch, record.smo_pages_height + 1);
            let chain = IndexPageDeltaChain::create(record.deltas.clone(), index_page);
            self.page_mapping.insert_index_delta(new_pid, chain.clone());
            checkpoint
                .index
                .push((new_pid, chain.write().reconsile(epoch)));
            assert_eq!(record.redo_logs.len(), 1);
            record.redo_logs.last_mut().unwrap().smo_kind = SMO_CREATE_ROOT_PAGE;
            record.update_page_id = new_pid;
            checkpoint.index_redo_log.push(record);
            checkpoint
                .vnodes
                .insert(vnode_id, (new_pid, PageType::Index));
        }
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
        let mut dirty_pages = updates
            .iter()
            .flat_map(|(_, update)| update.pages.clone())
            .collect_vec();
        // sort and dedup because we may change the same page in several epoch.
        dirty_pages.sort();
        dirty_pages.dedup();
        let mut dirty_index = vec![];
        let mut checkpoint = CheckpointData {
            leaf: vec![],
            index: vec![],
            index_redo_log: vec![],
            vnodes: HashMap::default(),
            leaf_deltas: vec![],
            commited_epoch: epoch,
        };
        for (_, update) in &updates {
            for (vnode_id, pid) in &update.vnodes {
                checkpoint.vnodes.insert(*vnode_id, (*pid, PageType::Leaf));
            }
        }
        for (vnode_id, pid) in dirty_pages {
            let dirty_page = self.page_mapping.get_data_chains(&pid).unwrap();
            let delta = match dirty_page.read().flush(epoch) {
                None => continue,
                Some(delta) => delta,
            };
            dirty_page.write().commit(delta.clone(), epoch);
            let (origin_page_right_link, page_left_key, update_size, base_leaf_size) = {
                let guard = dirty_page.read();
                (
                    guard.get_page_ref().get_right_link(),
                    guard.get_page_ref().smallest_user_key.clone(),
                    guard.update_size(),
                    guard.get_page_ref().page_size(),
                )
            };
            // TODO:  we must record some extra info for other replicas to replay data generated by
            // this node when some split-modification-operation happened.
            if update_size > std::cmp::min(self.options.leaf_reconcile_size, base_leaf_size) {
                let new_pages = dirty_page.read().apply_to_page(
                    self.options.leaf_split_size,
                    self.options.index_split_count / 2,
                    safe_epoch,
                );
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
                    let p = Arc::new(page);
                    if idx + 1 == page_count {
                        if page_count == 1 {
                            self.page_mapping.insert_page(p.get_page_id(), p.clone());
                            dirty_page.write().set_new_page(p.clone());
                        }
                    } else {
                        self.page_mapping.insert_page(p.get_page_id(), p.clone());
                    }
                    leaf_pages.push(p);
                }
                if page_count > 1 {
                    // TODO: optimize read-write lock to avoid split-operation holding write-lock
                    // too long. But I think the complexity would be only O(n),
                    // where n represent the count of keys written  after the
                    // sync epoch.
                    let mut guard = dirty_page.write();
                    let right_buffer = guard.get_shared_memory_buffer();
                    for (idx, page) in leaf_pages.iter().enumerate() {
                        if idx + 1 == page_count {
                            self.page_mapping
                                .insert_page(page.get_page_id(), page.clone());
                            assert_eq!(page.get_page_id(), guard.get_page_ref().get_page_id());
                            guard.set_new_page(page.clone());
                        } else if !right_buffer.is_empty() {
                            let split_buffer = page.fetch_overlap_mem_delta(&right_buffer);
                            if !split_buffer.is_empty() {
                                let mut chain_chain = DeltaChain::new(page.clone());
                                chain_chain.append_delta_from_parent_page(split_buffer);
                                self.page_mapping
                                    .insert_delta(page.get_page_id(), chain_chain);
                            }
                        }
                    }
                }
                if leaf_pages.len() > 1 {
                    // reverse to make origin page id at first.
                    leaf_pages.reverse();
                    dirty_index.push((
                        vnode_id,
                        page_left_key,
                        leaf_pages
                            .iter()
                            .map(|p| {
                                IndexPageDelta::new(
                                    SMOType::Add,
                                    p.get_page_id(),
                                    epoch,
                                    p.smallest_user_key.clone(),
                                )
                            })
                            .collect_vec(),
                    ));
                }
                checkpoint.leaf.extend(leaf_pages);
            } else {
                checkpoint.leaf_deltas.push((pid, delta));
            }
        }
        self.execute_smo(dirty_index, epoch, safe_epoch, &mut checkpoint)
            .await?;
        self.gc_collector.refresh_for_gc();
        Ok(checkpoint)
    }

    fn merge_leaf_pages(
        &self,
        epoch: u64,
        safe_epoch: u64,
        current_size: usize,
        pages: &mut Vec<Arc<RwLock<DeltaChain>>>,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<Vec<IndexPageDelta>> {
        // take the left page.
        let first_delta = pages.pop().unwrap();
        // reverse to keep page order same with key order.
        pages.reverse();
        let new_page =
            first_delta
                .read()
                .merge_pages(epoch, safe_epoch, current_size, pages.as_ref());
        let first_pid = new_page.get_page_id();
        let new_page = {
            // TODO: use optimistic lock mode to avoid hold mutex too long.
            let mut guard = first_delta.write();
            for delta_chains in pages.iter() {
                let mut page = delta_chains.write();
                page.set_pending_merge(first_pid);
                guard.append_delta_from_parent_page(page.get_shared_memory_buffer());
            }
            let new_page = Arc::new(new_page);
            guard.set_new_page(new_page.clone());
            new_page
        };
        checkpoint.leaf.push(new_page.clone());
        self.page_mapping.insert_page(first_pid, new_page);
        // TODO: remove page after all read-request ended. (we need a epoch-based algorithm to make
        // sure that no-threads would access these pages)
        let mut removed_delta = Vec::with_capacity(pages.len());
        for delta_chains in pages.iter() {
            let mut page = delta_chains.read();
            removed_delta.push(IndexPageDelta {
                son: SubtreePageInfo {
                    page_id: page.get_page_ref().get_page_id(),
                    smallest_key: page.get_page_ref().smallest_user_key.clone(),
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
        let (mut sub_pages, parent_id, is_leaf) = {
            let guard = index_page.read();
            (
                guard.get_base_page().get_son_pages(),
                guard.get_base_page().get_page_id(),
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
                let delta = self.get_leaf_page_delta(*pid).await?;
                let sz = {
                    let guard = delta.read();
                    guard.get_page_ref().page_size() + guard.update_size()
                };
                if sz + current_size < self.options.index_min_merge_count {
                    current_size += sz;
                    last_sons.push(delta);
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
                        last_sons.push(delta);
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
