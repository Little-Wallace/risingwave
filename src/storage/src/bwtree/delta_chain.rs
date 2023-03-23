use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::KeyComparator;
use crate::bwtree::data_iterator::DataIterator;
use crate::bwtree::{INVALID_PAGE_ID, VKey};

use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::sorted_data_builder::{
    BlockBuilder, BlockBuilderOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::bwtree::sorted_record_block::SortedRecordBlock;
use crate::bwtree::index_page::IndexPage;
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
    start_commit_epoch: u64,
    current_skiplist: BTreeMap<VKey, HummockValue<Bytes>>,
    current_data_size: usize,
    deltas: Vec<Arc<Delta>>,
}

impl DeltaChain {
    pub fn is_memtable_empty(&self) -> bool {
        self.current_skiplist.is_empty()
    }

    pub fn new(history_deltas: Vec<Arc<Delta>>) -> Self {
        Self {
            start_commit_epoch: 0,
            current_skiplist: BTreeMap::new(),
            current_data_size: 0,
            current_epoch: 0,
            deltas: history_deltas,
        }
    }

    pub fn insert(&mut self, key: VKey, value: HummockValue<Bytes>) {
        self.current_data_size += key.len() + value.encoded_len() + std::mem::size_of::<u32>() * 2;
        self.current_skiplist.insert(key, value);
    }

    pub fn seal_epoch(&mut self, epoch: u64) {
        if self.start_commit_epoch == 0 {
            self.start_commit_epoch = epoch;
        }
        self.current_epoch = epoch;
    }

    pub fn commit(&mut self, epoch: u64) -> Arc<Delta> {
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: self.current_data_size,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
        let mut raw_value = BytesMut::new();
        let mut raw_key = BytesMut::new();
        for (key, value) in self.current_skiplist.iter() {
            value.encode(&mut raw_value);
            raw_key.put_slice(&key.user_key);
            raw_key.put_u64(key.epoch);
            builder.add(&raw_key, &raw_value);
        }
        let data = builder.build();
        Arc::new(Delta {
            raw: SortedRecordBlock::decode_from_raw(data),
            min_epoch: self.start_commit_epoch,
            max_epoch: epoch,
        })
    }

    pub fn update_count(&self) -> usize {
        self.current_skiplist.len() + self.deltas.iter().map(|delta|delta.raw.record_count()).sum::<usize>()
    }

    pub fn apply_to_page(&mut self, page: &LeafPage) -> Vec<LeafPage> {
        let iter = page.iter();
        let mut iters = vec![iter];
        let mut delta_size = self.current_data_size;
        for delta in &self.deltas {
            delta_size += delta.raw.size();
            iters.push(delta.raw.iter());
        }
        let mut need_split = delta_size + page.page_size() > MAX_LEAF_SIZE_LIMIT;
        let mut iter = DataIterator::new(iters);
        let mut mem_iter = self.current_skiplist.iter();
        let mut pages = Vec::with_capacity(2);
        let mut builder = BlockBuilder::default();
        let mut mem_item = mem_iter.next();
        let mut raw_value = BytesMut::new();
        let mut raw_key = BytesMut::new();
        while iter.is_valid() || mem_item.is_some() {
            if let Some((key, value)) = mem_item {
                while
                    iter.is_valid() &&
                KeyComparator::compare_encoded_full_key(key.as_ref(), iter.key()) == std::cmp::Ordering::Less {
                    builder.add(iter.key(), iter.value());
                    iter.next();
                    if need_split && builder.approximate_len() > SPLIT_LEAF_SIZE {
                        let data = builder.build();
                        let page = LeafPage::new(SortedRecordBlock::decode_from_raw(data), INVALID_PAGE_ID);
                        pages.push(page);
                        builder = BlockBuilder::default();
                    }
                }
                value.encode(&mut raw_value);
                key.encode_to(&mut raw_key);
                builder.add(raw_key.as_ref(), raw_value.as_ref());
                raw_value.clear();
                raw_key.clear();
                mem_item = mem_iter.next();
            } else {
                builder.add(iter.key(), iter.value());
            }
            if need_split && builder.approximate_len() > SPLIT_LEAF_SIZE {
                let data = builder.build();
                let page = LeafPage::new(SortedRecordBlock::decode_from_raw(data), INVALID_PAGE_ID);
                pages.push(page);
                builder = BlockBuilder::default();
            }
        }
        pages
    }
}
