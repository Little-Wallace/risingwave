use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::key::{split_key_epoch, user_key, TableKey};
use risingwave_hummock_sdk::KeyComparator;

use crate::bwtree::data_iterator::{MergedDataIterator, MergedSharedBufferIterator};
use crate::bwtree::index_page::IndexPage;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::sorted_data_builder::{
    BlockBuilder, BlockBuilderOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::bwtree::sorted_record_block::SortedRecordBlock;
use crate::bwtree::{VKey, INVALID_PAGE_ID};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::value::HummockValue;
use crate::hummock::CompressionAlgorithm;

const MAX_ALLOW_UPDATE_PERCENT: usize = 40;
const SPLIT_COUNT_LIMIT: usize = 256;
const MAX_LEAF_SIZE_LIMIT: usize = 64 * 1024;
const SPLIT_LEAF_SIZE: usize = 48 * 1024;
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

    pub fn get(&self, key: &Bytes, epoch: u64) -> Option<Bytes> {
        let vk = VKey {
            user_key: key.clone(),
            epoch,
        };
        let mut raw_key = BytesMut::default();
        vk.encode_to(&mut raw_key);
        for d in self.mem_deltas.iter().rev() {
            if d.epoch() <= epoch {
                if let Some(v) = d.get(TableKey(key.as_ref())) {
                    return v.into_user_value();
                }
            }
        }
        let mut iter = self.base_page.iter();
        iter.seek(&raw_key);
        if iter.is_valid() && user_key(iter.key()).eq(key.as_ref()) {
            return iter
                .value()
                .into_user_value()
                .map(|v| Bytes::copy_from_slice(v));
        }
        None
    }

    pub fn apply_to_page(&self, safe_epoch: u64) -> Vec<LeafPage> {
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: SPLIT_LEAF_CAPACITY,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
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
        let mut data_size = (self.base_page.page_size() + self.update_size());
        let split_count = data_size / SPLIT_LEAF_SIZE;
        let split_size = data_size / split_count;
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

    pub fn set_new_page(&mut self, page: Arc<LeafPage>) -> Vec<SharedBufferBatch> {
        self.history_delta.clear();
        // the structure of tree does not change
        if self.base_page.get_right_link() == page.get_right_link() {
            return vec![];
        }
        let batches = self
            .mem_deltas
            .drain_filter(|batch| {
                page.smallest_user_key
                    .as_ref()
                    .le(batch.start_table_key().0)
                    && page.largest_user_key.as_ref().gt(batch.end_table_key().0)
            })
            .collect_vec();
        for batch in &batches {
            if page.smallest_user_key.as_ref().le(batch.end_table_key().0)
                && page.largest_user_key.as_ref().gt(batch.start_table_key().0)
            {
                self.mem_deltas.push(batch.copy_batch_between_range(
                    page.smallest_user_key.as_ref(),
                    page.largest_user_key.as_ref(),
                ));
            }
        }
        assert_eq!(self.base_page.get_page_id(), page.get_page_id());
        self.base_page = page;
        batches
    }

    pub fn append_delta_from_parent_page(&mut self, batches: &[SharedBufferBatch]) {
        for batch in batches {
            self.mem_deltas.push(batch.copy_batch_between_range(
                self.base_page.smallest_user_key.as_ref(),
                self.base_page.largest_user_key.as_ref(),
            ));
        }
    }
}
