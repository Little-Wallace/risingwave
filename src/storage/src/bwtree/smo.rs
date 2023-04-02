use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use itertools::Itertools;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::bwtree::delta_chain::{Delta, DeltaChain};
use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, SMOType, SonPageInfo,
};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::{PageId, TypedPage, INVALID_PAGE_ID};
use crate::hummock::HummockResult;

pub struct CheckpointData {
    pub leaf_deltas: Vec<(PageId, Arc<Delta>)>,
    pub leaf: Vec<Arc<LeafPage>>,
    pub index: Vec<(PageId, Bytes)>,
    pub index_delta: Vec<(PageId, Bytes)>,
    pub vnodes: HashMap<usize, PageId>,
    pub commited_epoch: u64,
}

impl BwTreeEngine {
    pub(crate) async fn exec_structure_modification_operation(
        &self,
        new_pages: Vec<Arc<LeafPage>>,
        epoch: u64,
        checkpoint: &mut CheckpointData,
    ) -> HummockResult<()> {
        let mut parent_link = new_pages[0].parent_link;
        let mut changed_index = vec![];
        for p in new_pages {
            changed_index.push(IndexPageDelta::new(
                SMOType::Add,
                p.get_page_id(),
                epoch,
                p.smallest_user_key.clone(),
            ));
        }
        while parent_link != INVALID_PAGE_ID && !changed_index.is_empty() {
            let deltas_chain = self.page_mapping.get_index_page(&parent_link);
            {
                // Here we do not hold the write lock because we assume that there would be only
                // thread to do SMO.
                let mut guard = deltas_chain.write();
                for delta in changed_index.drain(..) {
                    guard.append_delta(delta);
                }
            }
            let (shall_split, shall_reconcile, pid) = {
                let guard = deltas_chain.read();
                parent_link = guard.get_parent_link();
                (
                    guard.shall_split(self.options.index_split_count),
                    guard.shall_reconcile(self.options.index_reconcile_count),
                    guard.get_base_page_id(),
                )
            };
            if shall_split {
                let mut new_page_id = self.get_new_page_id().await?;
                let (first_page, other_pages) = {
                    let guard = deltas_chain.read();
                    let mut pages = guard.split_to_pages(epoch);
                    let other_pages = pages.split_off(1);
                    let mut first_page = pages.pop().unwrap();
                    first_page.set_page_id(guard.get_base_page_id());
                    first_page.set_right_link(new_page_id);
                    (first_page, other_pages)
                };

                let mut index_data = BytesMut::new();
                for mut page in other_pages {
                    changed_index.push(IndexPageDelta {
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
                    let chain = IndexPageDeltaChain::create(vec![], page, epoch);
                    self.page_mapping.insert_index_delta(new_page_id, chain);
                    checkpoint
                        .index
                        .push((new_page_id, Bytes::copy_from_slice(&index_data)));
                    index_data.clear();
                }
                deltas_chain.write().set_page(epoch, first_page);
            } else if shall_reconcile {
                let data = deltas_chain.write().reconsile(epoch);
                checkpoint.index.push((pid, data));
            } else {
                let mut buf = BytesMut::new();
                deltas_chain.write().commit_delta(epoch, &mut buf);
                checkpoint.index_delta.push((pid, buf.freeze()));
            }
            // TODO: merge small page.
        }

        if changed_index.is_empty() {
            return Ok(());
        }
        let mut vnodes = self.vnodes.load().as_ref().clone();
        let mut vnode_id = 0;
        for (vid, typedpage) in vnodes.iter() {
            if let TypedPage::DataPage(pid) = typedpage {
                if *pid == changed_index[0].son.page_id {
                    vnode_id = *vid;
                    break;
                }
            }
        }
        let new_pid = self.page_id_manager.get_new_page_id().await?;
        let index_page = IndexPage::new(new_pid, INVALID_PAGE_ID, Bytes::new(), 1);
        let chain = IndexPageDeltaChain::create(changed_index.clone(), index_page, epoch);
        checkpoint
            .index
            .push((new_pid, chain.write().reconsile(epoch)));
        self.page_mapping.insert_index_delta(new_pid, chain.clone());
        vnodes.insert(vnode_id, TypedPage::Index(chain));
        checkpoint.vnodes.insert(vnode_id, new_pid);
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
            let (update_size, base_leaf_size) = {
                let guard = dirty_page.read();
                (guard.update_size(), guard.get_page_ref().page_size())
            };
            if update_size > std::cmp::min(self.options.leaf_reconcile_size, base_leaf_size) {
                let new_pages = dirty_page
                    .read()
                    .apply_to_page(self.options.leaf_split_size, safe_epoch);
                let mut new_pid = INVALID_PAGE_ID;
                let page_count = new_pages.len();
                let mut leaf_pages = vec![];
                for (idx, mut page) in new_pages.into_iter().rev().enumerate() {
                    let last_pid = new_pid;
                    if idx + 1 != page_count {
                        new_pid = self.get_new_page_id().await?;
                        page.set_page_id(new_pid);
                    }
                    if idx != 0 {
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_root_smo() {}
}
