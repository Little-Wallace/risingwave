use std::ops::Bound;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use risingwave_hummock_sdk::key::{split_key_epoch, user_key, StateTableKey, TableKey};

use crate::bwtree::data_iterator::MergedSharedBufferIterator;
use crate::bwtree::index_page::PageType;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::mapping_table::MappingTable;
use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::HummockResult;
use crate::store::IterKeyRange;

pub struct BwTreeIterator {
    mem_iter: MergedSharedBufferIterator,
    page_mapping: Arc<MappingTable>,
    current_tree_idx: usize,
    bwtree_root_page_ids: Vec<(PageId, PageType)>,
    current_page: Option<Arc<LeafPage>>,
    key_range: IterKeyRange,
    valid: bool,
    read_epoch: u64,
    flushed_epoch: u64,
}

impl BwTreeIterator {
    pub fn new(
        key_range: IterKeyRange,
        mem_iter: MergedSharedBufferIterator,
        page_mapping: Arc<MappingTable>,
        bwtree_root_page_ids: Vec<(PageId, PageType)>,
        read_epoch: u64,
        flushed_epoch: u64,
    ) -> Self {
        Self {
            mem_iter,
            page_mapping,
            key_range,
            bwtree_root_page_ids,
            current_page: None,
            current_tree_idx: 0,
            valid: true,
            read_epoch,
            flushed_epoch,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn check_meet_end(&self, user_key: &[u8]) -> bool {
        match &self.key_range.1 {
            Bound::Included(right) => {
                if user_key.gt(right.as_ref()) {
                    return true;
                }
            }
            Bound::Excluded(left) => {
                if user_key.ge(left.as_ref()) {
                    return true;
                }
            }
            Bound::Unbounded => (),
        }
        false
    }

    pub async fn next_chunk(&mut self) -> HummockResult<Vec<(Bytes, Bytes)>> {
        assert!(self.is_valid());
        if self.current_page.is_none() {
            let mut last_page_key = vec![];
            let mut ret = vec![];
            while self.mem_iter.is_valid() {
                if last_page_key.eq(self.mem_iter.key().user_key.0) {
                    self.mem_iter.next();
                    continue;
                }
                if self.check_meet_end(self.mem_iter.key().user_key.0) {
                    self.valid = false;
                    break;
                }
                let (key, value) = self.mem_iter.current_item();
                if let Some(value) = value.into_user_value() {
                    ret.push((key, value))
                }
                last_page_key.clear();
                last_page_key.extend_from_slice(self.mem_iter.key().user_key.as_ref());
                self.mem_iter.next();
            }
            return Ok(ret);
        }

        let current_page = self.current_page.as_ref().unwrap();
        let mut iter = current_page.iter(self.read_epoch);
        let mut ret = vec![];

        let mut raw_key = BytesMut::new();
        match &self.key_range.0 {
            Bound::Included(left) => {
                if left.gt(&current_page.get_base_page().smallest_user_key) {
                    let mut key = StateTableKey {
                        user_key: TableKey(left.clone()),
                        epoch: self.read_epoch,
                    };
                    key.encode_into(&mut raw_key);
                    iter.seek(&raw_key);
                } else {
                    iter.seek_to_first();
                }
            }
            Bound::Excluded(left) => {
                if left.gt(&current_page.get_base_page().smallest_user_key) {
                    let mut key = StateTableKey {
                        user_key: TableKey(left.clone()),
                        epoch: self.read_epoch,
                    };
                    key.encode_into(&mut raw_key);
                    iter.seek(&raw_key);
                    while iter.is_valid() && user_key(iter.key()).eq(left.as_ref()) {
                        iter.next();
                    }
                } else {
                    iter.seek_to_first();
                }
            }
            Bound::Unbounded => {
                iter.seek_to_first();
            }
        }

        let mut last_page_key = vec![];
        while iter.is_valid() {
            let (user_key, mut epoch) = split_key_epoch(iter.key());
            let epoch = epoch.get_u64();
            if epoch > self.flushed_epoch {
                continue;
            }
            let ord = if self.mem_iter.is_valid() {
                std::cmp::Ordering::Greater
            } else if self.mem_iter.key().epoch > self.read_epoch {
                self.mem_iter.next();
                continue;
            } else {
                self.mem_iter.key().user_key.as_ref().cmp(user_key)
            };
            match ord {
                std::cmp::Ordering::Equal => {
                    // The epoch of page could not be larger than shared-buffer, so we could move
                    // page iter directly.
                    if self.check_meet_end(user_key) {
                        self.valid = false;
                        break;
                    }
                    iter.next();
                }
                std::cmp::Ordering::Greater => {
                    if last_page_key.eq(user_key) {
                        iter.next();
                        continue;
                    }
                    if self.check_meet_end(user_key) {
                        self.valid = false;
                        break;
                    }
                    let v = iter.value();
                    if let Some(value) = v.into_user_value() {
                        ret.push((
                            Bytes::copy_from_slice(user_key),
                            Bytes::copy_from_slice(value),
                        ));
                    }
                    last_page_key.clear();
                    last_page_key.extend_from_slice(user_key);
                    iter.next();
                }
                std::cmp::Ordering::Less => {
                    if last_page_key.eq(self.mem_iter.key().user_key.as_ref()) {
                        iter.next();
                        continue;
                    }
                    if self.check_meet_end(self.mem_iter.key().user_key.as_ref()) {
                        self.valid = false;
                        break;
                    }
                    let (key, value) = self.mem_iter.current_item();
                    if let Some(value) = value.into_user_value() {
                        ret.push((key, value))
                    }
                    last_page_key.clear();
                    last_page_key.extend_from_slice(self.mem_iter.key().user_key.as_ref());
                    self.mem_iter.next();
                }
            }
        }
        if self.valid {
            let base_page = current_page.get_base_page();
            if base_page.get_right_link() != INVALID_PAGE_ID
                && self.check_meet_end(base_page.largest_user_key.as_ref())
            {
                let next_link = current_page.get_base_page().get_right_link();
                let next_page = self.page_mapping.get_or_fetch_page(next_link).await?;
                self.current_page = Some(next_page);
            } else {
                self.current_page = None;
            }
        }
        Ok(ret)
    }
}
