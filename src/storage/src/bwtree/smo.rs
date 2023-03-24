use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::bwtree::index_page::{
    IndexPage, IndexPageDelta, IndexPageDeltaChain, SMOType, SonPageInfo,
};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::root_page::{CheckpointData, RootPage};
use crate::bwtree::{TypedPage, INVALID_PAGE_ID};
use crate::hummock::HummockResult;

impl RootPage {
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
                    guard.shall_split(),
                    guard.shall_reconcile(),
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
        let new_pid = self.sstable_id_manager.get_new_sst_object_id().await?;
        let mut index_page = IndexPage::new(new_pid, INVALID_PAGE_ID, Bytes::new(), 1);
        let chain = IndexPageDeltaChain::create(changed_index.clone(), index_page, epoch);
        checkpoint
            .index
            .push((new_pid, chain.write().reconsile(epoch)));
        self.page_mapping.insert_index_delta(new_pid, chain.clone());
        vnodes.insert(vnode_id, TypedPage::Index(chain));
        checkpoint.vnodes.insert(vnode_id, new_pid);
        Ok(())
    }
}
