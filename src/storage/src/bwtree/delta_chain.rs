use std::sync::Arc;

use bytes::{Buf, Bytes};
use parking_lot::RwLock;
use risingwave_hummock_sdk::key::{split_key_epoch, user_key};

use crate::bwtree::data_iterator::MergedDataIterator;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::sorted_data_builder::{
    BlockBuilder, BlockBuilderOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::bwtree::sorted_record_block::{BlockIterator, SortedRecordBlock};
use crate::bwtree::INVALID_PAGE_ID;

const SPLIT_LEAF_CAPACITY: usize = 50 * 1024;

pub struct Delta {
    raw: SortedRecordBlock,
    min_epoch: u64,
    max_epoch: u64,
}

impl Delta {
    pub fn new(data: Bytes, min_epoch: u64, max_epoch: u64) -> Self {
        Self {
            raw: SortedRecordBlock::decode(data).unwrap(),
            min_epoch,
            max_epoch,
        }
    }
}

pub struct DeltaChain {
    current_data_size: usize,
    syncing_deltas: Vec<Arc<Delta>>,
    // TODO: replace it with PageId because we do not hope every write operation fetch the whole
    // page from remote-storage.
    base_page: Arc<LeafPage>,
}

impl DeltaChain {
    pub fn new(base_page: Arc<LeafPage>) -> Self {
        Self {
            current_data_size: 0,
            syncing_deltas: vec![],
            base_page,
        }
    }

    /// call commit after flush.
    pub fn ingest(&mut self, delta: Arc<Delta>) {
        self.syncing_deltas.push(delta.clone());
    }

    pub fn need_split(&self, split_size: usize) -> bool {
        self.update_size() + self.base_page.page_size() > split_size
    }

    pub fn need_reconcile(&self, reconcile_size: usize) -> bool {
        self.update_size() > reconcile_size
    }

    pub fn update_size(&self) -> usize {
        self.syncing_deltas
            .iter()
            .map(|delta| delta.raw.raw_data().len())
            .sum::<usize>()
    }

    pub fn get_page_ref(&self) -> &LeafPage {
        self.base_page.as_ref()
    }

    pub fn get(&self, vk: &[u8], ukey: &[u8], epoch: u64) -> Option<Bytes> {
        for d in self.syncing_deltas.iter().rev() {
            if d.max_epoch <= epoch {
                let mut iter = BlockIterator::new(&d.raw);
                iter.seek(vk);
                if iter.is_valid() && user_key(iter.key()).eq(ukey) {
                    let v = iter.value().to_bytes();
                    return v.into_user_value();
                }
            }
        }
        self.base_page.get(vk, ukey)
    }

    fn add_data_to_builder(&self, max_epoch: u64, safe_epoch: u64, builder: &mut BlockBuilder) {
        let mut iters = Vec::with_capacity(self.syncing_deltas.len() + 1);
        for delta in &self.syncing_deltas {
            assert!(delta.max_epoch <= max_epoch);
            iters.push(delta.raw.iter());
        }
        if !self.base_page.is_empty() {
            iters.push(self.base_page.iter());
        }
        let mut last_user_key = vec![];
        let mut merge_iter = MergedDataIterator::new(iters);
        merge_iter.seek_to_first();
        while merge_iter.is_valid() {
            let (ukey, mut epoch) = split_key_epoch(merge_iter.key());
            let epoch = epoch.get_u64();
            if epoch > safe_epoch
                || (!ukey.eq(last_user_key.as_slice()) && !merge_iter.value().is_delete())
            {
                builder.add(merge_iter.key(), merge_iter.raw_value());
            }
            last_user_key.clear();
            last_user_key.extend_from_slice(ukey);
            merge_iter.next();
        }
    }

    pub fn merge_pages(
        &self,
        max_epoch: u64,
        safe_epoch: u64,
        merge_capacity: usize,
        pages: &[Arc<RwLock<DeltaChain>>],
    ) -> LeafPage {
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: merge_capacity + 1024,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
        let mut largest_key = Bytes::new();
        // Use page-id of the left page and right-link of the right page.
        let mut right_link = INVALID_PAGE_ID;
        self.add_data_to_builder(max_epoch, safe_epoch, &mut builder);
        for page in pages {
            let guard = page.read();
            right_link = guard.get_page_ref().get_right_link();
            largest_key = guard.get_page_ref().largest_user_key.clone();
            guard.add_data_to_builder(max_epoch, safe_epoch, &mut builder);
        }
        let mut page = LeafPage::new(
            self.base_page.get_page_id(),
            self.base_page.smallest_user_key.clone(),
            largest_key,
            SortedRecordBlock::decode(builder.build()).unwrap(),
            max_epoch,
        );
        page.set_right_link(right_link);
        page
    }

    pub fn apply_to_page(
        &self,
        max_split_size: usize,
        max_split_count: usize,
        safe_epoch: u64,
    ) -> Vec<LeafPage> {
        let mut iters = Vec::with_capacity(self.syncing_deltas.len());
        let mut max_epoch = self.base_page.epoch();
        for delta in &self.syncing_deltas {
            iters.push(delta.raw.iter());
            max_epoch = std::cmp::max(max_epoch, delta.max_epoch);
        }
        if !self.base_page.is_empty() {
            iters.push(self.base_page.iter());
        }
        let mut pages = vec![];
        let mut merge_iter = MergedDataIterator::new(iters);
        merge_iter.seek_to_first();
        let mut last_user_key = vec![];
        let mut smallest_key = self.base_page.smallest_user_key.clone();
        let data_size = self.base_page.page_size() + self.update_size();
        let mut split_count = 1;
        if max_split_size < usize::MAX {
            split_count = std::cmp::min(
                max_split_count,
                (data_size + max_split_size - 1) / max_split_size,
            );
        }
        let mut split_size = data_size / split_count;
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: split_size + 1024,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        });
        while merge_iter.is_valid() {
            let (ukey, mut epoch) = split_key_epoch(merge_iter.key());
            let epoch = epoch.get_u64();
            if split_count > 1
                && !ukey.eq(last_user_key.as_slice())
                && builder.approximate_len() > split_size
            {
                let largest_key = Bytes::copy_from_slice(ukey);
                pages.push(LeafPage::new(
                    self.base_page.get_page_id(),
                    smallest_key,
                    largest_key.clone(),
                    SortedRecordBlock::decode(builder.build()).unwrap(),
                    max_epoch,
                ));
                builder = BlockBuilder::new(BlockBuilderOptions {
                    capacity: SPLIT_LEAF_CAPACITY,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                });
                smallest_key = largest_key;
            }
            if epoch > safe_epoch
                || (!ukey.eq(last_user_key.as_slice()) && !merge_iter.value().is_delete())
            {
                builder.add(merge_iter.key(), merge_iter.raw_value());
            }
            last_user_key.clear();
            last_user_key.extend_from_slice(ukey);
            merge_iter.next();
        }
        let data = if !builder.is_empty() {
            SortedRecordBlock::decode(builder.build()).unwrap()
        } else {
            SortedRecordBlock::empty()
        };
        let mut page = LeafPage::new(
            self.base_page.get_page_id(),
            smallest_key,
            self.base_page.largest_user_key.clone(),
            data,
            max_epoch,
        );
        page.set_right_link(self.base_page.get_right_link());
        pages.push(page);
        pages
    }

    pub fn set_new_page(&mut self, page: Arc<LeafPage>) {
        self.syncing_deltas.clear();
        self.base_page = page;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Bytes, BytesMut};
    use risingwave_hummock_sdk::key::{user_key, StateTableKey, TableKey};

    use crate::bwtree::delta_chain::{Delta, DeltaChain};
    use crate::bwtree::leaf_page::LeafPage;
    use crate::bwtree::sorted_data_builder::BlockBuilder;
    use crate::bwtree::sorted_record_block::SortedRecordBlock;
    use crate::bwtree::test_utils::{from_slice_key, generate_data};
    use crate::bwtree::INVALID_PAGE_ID;
    use crate::hummock::value::HummockValue;
    use crate::storage_value::StorageValue;

    fn build_sorted_block(data: Vec<(Bytes, StorageValue)>, epoch: u64) -> Bytes {
        let mut builder = BlockBuilder::default();
        let mut raw_key = BytesMut::new();
        let mut raw_value = BytesMut::new();
        for (k, v) in data {
            let vk = StateTableKey::new(TableKey(k), epoch);
            let v: HummockValue<Bytes> = v.into();
            vk.encode_into(&mut raw_key);
            v.encode(&mut raw_value);
            builder.add(raw_key.as_ref(), raw_value.as_ref());
            raw_key.clear();
            raw_value.clear();
        }
        builder.build()
    }

    #[test]
    fn test_leaf_apply() {
        let data = vec![
            generate_data(b"aaaa", b"v0"),
            generate_data(b"bbbb", b"v0"),
            generate_data(b"cccc", b"v0"),
            generate_data(b"dddd", b"v0"),
        ];
        let base_leaf = LeafPage::new(
            1,
            Bytes::copy_from_slice(b""),
            Bytes::copy_from_slice(b"eeee"),
            SortedRecordBlock::decode(build_sorted_block(data, 1)).unwrap(),
            1,
        );
        let mut delta_chains = DeltaChain::new(Arc::new(base_leaf));
        assert_eq!(
            delta_chains
                .get(&from_slice_key(b"aaaa", 1), b"aaaa", 1)
                .unwrap(),
            Bytes::copy_from_slice(b"v0")
        );
        let data1 = vec![generate_data(b"aaaa", b"v1"), generate_data(b"cccc", b"")];
        delta_chains.ingest(Arc::new(Delta::new(build_sorted_block(data1, 2), 2, 2)));

        assert_eq!(
            delta_chains
                .get(&from_slice_key(b"aaaa", 1), b"aaaa", 1)
                .unwrap(),
            Bytes::copy_from_slice(b"v0")
        );
        assert_eq!(
            delta_chains
                .get(&from_slice_key(b"aaaa", 2), b"aaaa", 2)
                .unwrap(),
            Bytes::copy_from_slice(b"v1")
        );
        assert!(delta_chains
            .get(&from_slice_key(b"cccc", 2), b"cccc", 2)
            .is_none());
        let data2 = vec![generate_data(b"ccccdddd", b"v2")];
        delta_chains.ingest(Arc::new(Delta::new(build_sorted_block(data2, 3), 3, 3)));
        let mut new_pages = delta_chains.apply_to_page(160, 10, 3);
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
        delta_chains.ingest(Arc::new(Delta::new(build_sorted_block(data, 4), 4, 4)));
        let new_pages = delta_chains.apply_to_page(160, 10, 4);
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
