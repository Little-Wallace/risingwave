use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use itertools::Itertools;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::bwtree::delta_chain::{Delta, DeltaChain};
use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, PageType, SMOType, SonPageInfo,
};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::HummockResult;

const SMO_MERGE: u8 = 0;
const SMO_SPLIT: u8 = 1;
const SMO_RECONCILE: u8 = 2;

pub struct SMORedoLogRecord {
    // When smo_kind equals SMO_MERGE, there would be several origin_pages and one new_pages.
    // When smo_kind equals SMO_SPLIT, there would be one origin_pages and several new_pages.
    // When smo_kind equals SMO_RECONCILE, there is only one item in origin_pages, and one item in
    // new_pages.
    origin_pages: Vec<PageId>,
    new_pages: Vec<PageId>,
    smo_kind: u8,
}

pub struct IndexPageRedoLogRecord {
    redo_logs: Vec<SMORedoLogRecord>,
    deltas: Vec<IndexPageDelta>,
    update_page_id: PageId,
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
    pub(crate) async fn exec_structure_modification_operation(
        &self,
        mut dirty_index_page_changes: Vec<(PageId, Vec<IndexPageDelta>)>,
        epoch: u64,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<()> {
        let mut vnode_changes = vec![];
        let mut changed_deltas = vec![];
        let mut vnodes = self.vnodes.load_full();
        let mut new_vnodes = vnodes.as_ref().clone();
        {
            let guard = self.buffer_page.read();
            if !guard.is_empty() {
                for (vnode_id, delta_chain) in guard.iter() {
                    if delta_chain.get_page_ref().epoch() > epoch {
                        continue;
                    }
                    if let Some(delta) = delta_chain.flush(epoch) {
                        changed_deltas.push((*vnode_id, delta));
                    }
                }
            }
        }
        let need_create_index = changed_deltas.len() > 0;
        if !changed_deltas.is_empty() {
            let mut guard = self.buffer_page.write();
            for (vnode_id, delta) in changed_deltas {
                guard.get_mut(&vnode_id).unwrap().commit(delta, epoch);
            }
        }
        let mut new_leafs = vec![];
        if need_create_index {
            {
                let guard = self.buffer_page.read();
                for (vnode_id, delta_chain) in guard.iter() {
                    if delta_chain.get_page_ref().epoch() > epoch {
                        continue;
                    }
                    let mut ret = delta_chain.apply_to_page(self.options.leaf_split_size, 1, epoch);
                    assert_eq!(ret.len(), 1);
                    new_leafs.push((*vnode_id, ret.pop().unwrap()));
                }
            }
            let mut created_leafs = Vec::with_capacity(new_leafs.len());
            for (vnode_id, mut leaf) in new_leafs {
                let pid = self.get_new_page_id().await?;
                leaf.set_page_id(pid);
                created_leafs.push((vnode_id, Arc::new(leaf)));
            }
            let mut guard = self.buffer_page.write();
            for (vnode_id, mut leaf) in created_leafs {
                let pid = leaf.get_page_id();
                checkpoint.leaf.push(leaf.clone());
                let mut delta = guard.remove(&vnode_id).unwrap();
                delta.set_new_page(leaf);
                self.page_mapping.insert_delta(pid, delta);
                new_vnodes.insert(vnode_id, (pid, PageType::Leaf));
                vnode_changes.push((INVALID_PAGE_ID, pid, PageType::Leaf));
            }
        }
        dirty_index_page_changes.sort_by_key(|a| a.0);
        let mut index_change_records = vec![];
        for (dirty_index_page_id, page_changes) in dirty_index_page_changes
            .into_iter()
            .group_by(|a| a.0)
            .into_iter()
        {
            let mut record = IndexPageRedoLogRecord {
                redo_logs: vec![],
                deltas: vec![],
                update_page_id: dirty_index_page_id,
            };
            for (_, changes) in page_changes {
                record.redo_logs.push(SMORedoLogRecord {
                    origin_pages: vec![changes[0].son.page_id],
                    new_pages: changes
                        .iter()
                        .map(|change| change.son.page_id)
                        .collect_vec(),
                    smo_kind: SMO_SPLIT,
                });
                record.deltas.extend(changes);
            }
            index_change_records.push(record);
        }
        while !index_change_records.is_empty() {
            let mut parent_change_records = vec![];
            for mut record in index_change_records.drain(..) {
                if record.update_page_id == INVALID_PAGE_ID {
                    let new_pid = self.page_id_manager.get_new_page_id().await?;
                    let origin_pid = record.deltas[0].son.page_id;
                    let index_page =
                        IndexPage::new(new_pid, INVALID_PAGE_ID, Bytes::new(), epoch, 1);
                    let chain = IndexPageDeltaChain::create(record.deltas, index_page);
                    checkpoint
                        .index
                        .push((new_pid, chain.write().reconsile(epoch)));
                    self.page_mapping.insert_index_delta(new_pid, chain.clone());
                    vnode_changes.push((origin_pid, new_pid, PageType::Index));
                    continue;
                }
                if record.deltas.is_empty() {
                    // this is reconcile redo log.
                    continue;
                }
                let deltas_chain = self.page_mapping.get_index_page(&record.update_page_id);
                let (parent_link, shall_split, shall_reconcile) = {
                    // Here we do not hold the write lock because we assume that there would be only
                    // thread to do SMO.
                    let mut guard = deltas_chain.write();
                    for delta in &record.deltas {
                        guard.apply_delta(delta.clone());
                    }
                    (
                        guard.get_base_page().get_parent_link(),
                        guard.shall_split(self.options.index_split_count),
                        guard.shall_reconcile(self.options.index_reconcile_count),
                    )
                };

                let mut new_record = IndexPageRedoLogRecord {
                    redo_logs: vec![],
                    deltas: vec![],
                    update_page_id: parent_link,
                };
                if shall_split {
                    let mut new_page_id = self.get_new_page_id().await?;
                    let (first_page, other_pages) = {
                        let guard = deltas_chain.read();
                        let mut pages = guard.split_to_pages(epoch);
                        let mut smo_record = SMORedoLogRecord {
                            origin_pages: vec![pages[0].get_page_id()],
                            new_pages: vec![],
                            smo_kind: SMO_SPLIT,
                        };
                        for p in &pages {
                            smo_record.new_pages.push(p.get_page_id());
                        }
                        let other_pages = pages.split_off(1);
                        let mut first_page = pages.pop().unwrap();
                        first_page.set_page_id(guard.get_base_page_id());
                        first_page.set_right_link(new_page_id);
                        (first_page, other_pages)
                    };
                    new_record.deltas.push(IndexPageDelta {
                        son: SonPageInfo {
                            page_id: first_page.get_page_id(),
                            smallest_key: first_page.get_smallest_key(),
                        },
                        epoch,
                        smo: SMOType::Add,
                    });
                    let mut index_data = BytesMut::new();
                    for mut page in other_pages {
                        new_record.deltas.push(IndexPageDelta {
                            son: SonPageInfo {
                                page_id: new_page_id,
                                smallest_key: page.get_smallest_key(),
                            },
                            epoch,
                            smo: SMOType::Add,
                        });
                        page.set_page_id(new_page_id);
                        new_page_id = self.get_new_page_id().await?;
                        page.set_right_link(new_page_id);
                        page.encode_to(&mut index_data);
                        let chain = IndexPageDeltaChain::create(vec![], page);
                        self.page_mapping.insert_index_delta(new_page_id, chain);
                        checkpoint
                            .index
                            .push((new_page_id, Bytes::copy_from_slice(&index_data)));
                        index_data.clear();
                    }
                    // clear because we do not need persist these delta, we will persist the new
                    // pages in storage.
                    record.deltas.clear();
                    deltas_chain.write().set_page(epoch, first_page);
                } else if shall_reconcile {
                    new_record.redo_logs.push(SMORedoLogRecord {
                        origin_pages: vec![record.update_page_id],
                        new_pages: vec![record.update_page_id],
                        smo_kind: SMO_RECONCILE,
                    });
                    let data = deltas_chain.write().reconsile(epoch);
                    checkpoint.index.push((record.update_page_id, data));
                    record.deltas.clear();
                } else {
                    // TODO: merge small page.
                }
                parent_change_records.push(new_record);
                checkpoint.index_redo_log.push(record);
            }
            parent_change_records.sort_by_key(|record| record.update_page_id);
            for record in parent_change_records {
                if let Some(last) = index_change_records.last_mut() {
                    if last.update_page_id == record.update_page_id {
                        last.redo_logs.extend(record.redo_logs);
                        last.deltas.extend(record.deltas);
                        continue;
                    }
                }
                index_change_records.push(record);
            }
        }

        if vnode_changes.is_empty() {
            return Ok(());
        }

        let mut reverse_index = HashMap::with_capacity(new_vnodes.len());
        for (k, (pid, _)) in &new_vnodes {
            reverse_index.insert(*pid, *k);
        }
        for (origin_pid, new_pid, tp) in vnode_changes {
            if origin_pid != INVALID_PAGE_ID {
                let vnode_id = reverse_index.get(&origin_pid).unwrap();
                new_vnodes.insert(*vnode_id, (new_pid, tp));
            }
        }
        checkpoint.vnodes = new_vnodes.clone();
        self.vnodes.store(Arc::new(new_vnodes));
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
        for pid in dirty_pages {
            let dirty_page = self.page_mapping.get_data_chains(&pid).unwrap();
            let delta = match dirty_page.read().flush(epoch) {
                None => continue,
                Some(delta) => delta,
            };
            dirty_page.write().commit(delta.clone(), epoch);
            let (origin_page_right_link, parent_link, update_size, base_leaf_size) = {
                let guard = dirty_page.read();
                (
                    guard.get_page_ref().get_right_link(),
                    guard.get_page_ref().parent_link,
                    guard.update_size(),
                    guard.get_page_ref().page_size(),
                )
            };
            // TODO:  we must record some extra info for other replicas to replay data generated by
            // this node when some split-modification-operation happened.
            if update_size > std::cmp::min(self.options.leaf_reconcile_size, base_leaf_size) {
                let new_pages = dirty_page.read().apply_to_page(
                    self.options.leaf_split_size,
                    self.options.index_split_count,
                    safe_epoch,
                );
                let mut new_pid = INVALID_PAGE_ID;
                let page_count = new_pages.len();
                let mut leaf_pages = vec![];
                for (idx, mut page) in new_pages.into_iter().rev().enumerate() {
                    let last_pid = new_pid;
                    page.set_parent_link(parent_link);
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
                } else if leaf_pages[0].page_size() < self.options.leaf_split_size {
                    // TODO: support merge operation.
                }
                if leaf_pages.len() > 1 {
                    // reverse to make origin page id at first.
                    leaf_pages.reverse();
                    dirty_index.push((
                        leaf_pages[0].parent_link,
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
        self.exec_structure_modification_operation(dirty_index, epoch, &mut checkpoint)
            .await?;
        Ok(checkpoint)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_root_smo() {}
}
