use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use risingwave_hummock_sdk::key::{split_key_epoch, StateTableKey};

use crate::bwtree::data_iterator::{MergedDataIterator, MergedSharedBufferIterator};
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::sorted_data_builder::{
    BlockBuilder, BlockBuilderOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::bwtree::sorted_record_block::SortedRecordBlock;
use crate::bwtree::INVALID_PAGE_ID;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::CompressionAlgorithm;

const SPLIT_LEAF_CAPACITY: usize = 50 * 1024;

pub struct Delta {
    raw: SortedRecordBlock,
    min_epoch: u64,
    max_epoch: u64,
}

pub struct DeltaChain {
    current_epoch: u64,
    current_data_size: usize,
    mem_deltas: Vec<SharedBufferBatch>,
    history_delta: Vec<Arc<Delta>>,
    // TODO: replace it with PageId because we do not hope every write operation fetch the whole
    // page from remote-storage.
    base_page: Arc<LeafPage>,
}

impl DeltaChain {
    pub fn new(base_page: Arc<LeafPage>) -> Self {
        Self {
            current_data_size: 0,
            current_epoch: 0,
            mem_deltas: vec![],
            history_delta: vec![],
            base_page,
        }
    }

    pub fn ingest(&mut self, batch: SharedBufferBatch) {
        self.current_data_size += batch.size();
        self.mem_deltas.push(batch);
    }

    pub fn seal_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    /// call commit after flush.
    pub fn commit(&mut self, delta: Arc<Delta>, epoch: u64) {
        self.mem_deltas.retain(|batch| batch.epoch() > epoch);
        self.history_delta.push(delta.clone());
    }

    pub fn flush(&self, epoch: u64) -> Option<Arc<Delta>> {
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: self.update_size(),
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
        let mut raw_value = BytesMut::new();
        let mut raw_key = BytesMut::new();
        let mut min_epoch = u64::MAX;
        let iters = self
            .mem_deltas
            .iter()
            .filter(|batch| batch.epoch() <= epoch)
            .map(|d| {
                min_epoch = std::cmp::min(d.epoch(), min_epoch);
                d.clone().into_forward_iter()
            })
            .collect_vec();
        if iters.is_empty() {
            return None;
        }
        let mut merge_iter = MergedSharedBufferIterator::new(iters);
        merge_iter.seek_to_first();
        while merge_iter.is_valid() {
            merge_iter.key().encode_into(&mut raw_key);
            merge_iter.value().encode(&mut raw_value);
            builder.add(&raw_key, &raw_value);
            merge_iter.next();
            raw_key.clear();
            raw_value.clear();
        }
        let data = builder.build();
        let delta = Arc::new(Delta {
            raw: SortedRecordBlock::decode(data, 0).unwrap(),
            min_epoch,
            max_epoch: epoch,
        });
        Some(delta)
    }

    pub fn update_size(&self) -> usize {
        self.history_delta
            .iter()
            .map(|delta| delta.raw.size())
            .sum::<usize>()
    }

    pub fn buffer_size(&self) -> usize {
        self.mem_deltas
            .iter()
            .map(|delta| delta.size())
            .sum::<usize>()
    }

    pub fn update_count(&self) -> usize {
        self.history_delta.len()
    }

    pub fn get_page(&self) -> Arc<LeafPage> {
        self.base_page.clone()
    }

    pub fn get_page_ref(&self) -> &LeafPage {
        self.base_page.as_ref()
    }

    pub fn get(&self, vk: StateTableKey<Bytes>) -> Option<Bytes> {
        for d in self.mem_deltas.iter().rev() {
            if d.epoch() <= vk.epoch {
                if let Some(v) = d.get(vk.user_key.to_ref()) {
                    return v.into_user_value();
                }
            }
        }
        self.base_page.get(vk)
    }

    pub fn apply_to_page(&self, max_split_size: usize, safe_epoch: u64) -> Vec<LeafPage> {
        let mut iters = Vec::with_capacity(self.history_delta.len());
        for delta in &self.history_delta {
            iters.push(delta.raw.iter());
        }
        iters.push(self.base_page.iter());
        let mut pages = vec![];
        let mut merge_iter = MergedDataIterator::new(iters);
        merge_iter.seek_to_first();
        let mut last_user_key = vec![];
        let mut smallest_key = self.base_page.smallest_user_key.clone();
        let mut data_size = self.base_page.page_size() + self.update_size();
        let split_count = (data_size + max_split_size - 1) / max_split_size;
        let split_size = data_size / split_count;
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: split_size + 1024,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
        while merge_iter.is_valid() {
            let (ukey, mut epoch) = split_key_epoch(merge_iter.key());
            let epoch = epoch.get_u64();
            if epoch > safe_epoch
                || (!ukey.eq(last_user_key.as_slice()) && !merge_iter.value().is_delete())
            {
                builder.add(merge_iter.key(), merge_iter.raw_value());
            }
            if !ukey.eq(last_user_key.as_slice()) && builder.approximate_len() > split_size {
                let largest_key = Bytes::copy_from_slice(ukey);
                pages.push(LeafPage::new(
                    INVALID_PAGE_ID,
                    smallest_key,
                    largest_key.clone(),
                    SortedRecordBlock::decode(builder.build(), 0).unwrap(),
                ));
                builder = BlockBuilder::new(BlockBuilderOptions {
                    capacity: SPLIT_LEAF_CAPACITY,
                    compression_algorithm: CompressionAlgorithm::None,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                });
                smallest_key = largest_key;
            }
            last_user_key.clear();
            last_user_key.extend_from_slice(ukey);
            merge_iter.next();
        }
        if !builder.is_empty() {
            pages.push(LeafPage::new(
                INVALID_PAGE_ID,
                smallest_key,
                self.base_page.largest_user_key.clone(),
                SortedRecordBlock::decode(builder.build(), 0).unwrap(),
            ));
        }
        pages
            .last_mut()
            .unwrap()
            .set_right_link(self.base_page.get_right_link());
        pages
    }

    pub fn get_shared_memory_buffer(&self) -> Vec<SharedBufferBatch> {
        self.mem_deltas.clone()
    }

    pub fn set_new_page(&mut self, page: Arc<LeafPage>) {
        self.history_delta.clear();
        // the structure of tree does not change
        if self.base_page.get_right_link() == page.get_right_link() {
            self.base_page = page;
            return;
        }
        let batches = self
            .mem_deltas
            .drain_filter(|batch| {
                page.smallest_user_key
                    .as_ref()
                    .gt(batch.start_table_key().0)
                    || page.largest_user_key.as_ref().le(batch.end_table_key().0)
            })
            .collect_vec();
        self.mem_deltas
            .extend(page.fetch_overlap_mem_delta(&batches));
        self.mem_deltas.sort_by_key(|batch| batch.epoch());
        assert_eq!(self.base_page.get_page_id(), page.get_page_id());
        self.base_page = page;
    }

    pub fn append_delta_from_parent_page(&mut self, batches: Vec<SharedBufferBatch>) {
        self.mem_deltas.extend(batches);
        self.mem_deltas.sort_by_key(|batch| batch.epoch());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_hummock_sdk::key::{user_key, TableKey};

    use crate::bwtree::delta_chain::DeltaChain;
    use crate::bwtree::leaf_page::LeafPage;
    use crate::bwtree::test_utils::{build_shared_buffer_batch, from_slice_key, generate_data};

    #[test]
    fn test_leaf_apply() {
        let data = vec![
            generate_data(b"aaaa", b"v0"),
            generate_data(b"bbbb", b"v0"),
            generate_data(b"cccc", b"v0"),
            generate_data(b"dddd", b"v0"),
        ];
        let base_leaf = LeafPage::build(
            1,
            data,
            Bytes::copy_from_slice(b""),
            Bytes::copy_from_slice(b"eeee"),
            1,
        );
        let mut delta_chains = DeltaChain::new(Arc::new(base_leaf));
        assert_eq!(
            delta_chains.get(from_slice_key(b"aaaa", 1)).unwrap(),
            Bytes::copy_from_slice(b"v0")
        );
        let data1 = vec![generate_data(b"aaaa", b"v1"), generate_data(b"cccc", b"")];
        delta_chains.ingest(build_shared_buffer_batch(data1, 2));

        assert_eq!(
            delta_chains.get(from_slice_key(b"aaaa", 1)).unwrap(),
            Bytes::copy_from_slice(b"v0")
        );
        assert_eq!(
            delta_chains.get(from_slice_key(b"aaaa", 2)).unwrap(),
            Bytes::copy_from_slice(b"v1")
        );
        assert!(delta_chains.get(from_slice_key(b"cccc", 2)).is_none());
        let data2 = vec![generate_data(b"ccccdddd", b"v2")];
        delta_chains.ingest(build_shared_buffer_batch(data2, 3));
        // safe epoch, would delete all MVCC
        let delta = delta_chains.flush(3).unwrap();
        delta_chains.commit(delta, 3);
        let mut new_pages = delta_chains.apply_to_page(160, 3);
        assert_eq!(new_pages.len(), 1);
        let page = new_pages.pop().unwrap();
        let mut iter = page.iter();
        iter.seek_to_first();
        let data = vec![
            generate_data(b"aaaa", b"v1"),
            generate_data(b"bbbb", b"v0"),
            generate_data(b"ccccdddd", b"v2"),
            generate_data(b"dddd", b"v0"),
        ];
        let mut idx = 0;
        while iter.is_valid() {
            assert_eq!(data[idx].0.as_ref(), user_key(iter.key()));
            assert_eq!(
                data[idx].1.user_value.as_ref().unwrap().as_ref(),
                iter.value().into_user_value().unwrap()
            );
            idx += 1;
            iter.next();
        }
        let mut delta_chains = DeltaChain::new(Arc::new(page));
        let data = vec![
            generate_data(b"aaaa", b"v3"),
            generate_data(b"dddd", b"v3"),
            generate_data(b"eeee", b"v3"),
            generate_data(b"ffff", b"v3"),
            generate_data(b"gggg", b"v3"),
            generate_data(b"hhhh", b"v3"),
        ];
        delta_chains.ingest(build_shared_buffer_batch(data, 4));
        let delta = delta_chains.flush(4).unwrap();
        delta_chains.commit(delta, 3);
        let mut new_pages = delta_chains.apply_to_page(160, 4);
        assert_eq!(new_pages.len(), 2);
        let mut idx = 0;
        let mut iter = new_pages[0].iter();
        iter.seek_to_first();
        let data = vec![
            generate_data(b"aaaa", b"v3"),
            generate_data(b"bbbb", b"v0"),
            generate_data(b"ccccdddd", b"v2"),
            generate_data(b"dddd", b"v3"),
            generate_data(b"eeee", b"v3"),
            generate_data(b"ffff", b"v3"),
            generate_data(b"gggg", b"v3"),
            generate_data(b"hhhh", b"v3"),
        ];
        while iter.is_valid() {
            assert_eq!(data[idx].0.as_ref(), user_key(iter.key()));
            assert_eq!(
                data[idx].1.user_value.as_ref().unwrap().as_ref(),
                iter.value().into_user_value().unwrap()
            );
            idx += 1;
            iter.next();
        }
        let mut iter = new_pages[1].iter();
        iter.seek_to_first();
        while iter.is_valid() {
            assert_eq!(data[idx].0.as_ref(), user_key(iter.key()));
            assert_eq!(
                data[idx].1.user_value.as_ref().unwrap().as_ref(),
                iter.value().into_user_value().unwrap()
            );
            idx += 1;
            iter.next();
        }
    }
}
