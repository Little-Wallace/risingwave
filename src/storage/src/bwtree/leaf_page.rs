use bytes::Bytes;
use risingwave_hummock_sdk::key::user_key;
use risingwave_hummock_sdk::KeyComparator;

use crate::bwtree::sorted_data_builder::{BlockBuilder, BlockBuilderOptions};
use crate::bwtree::sorted_record_block::{BlockIterator, SortedRecordBlock};
use crate::bwtree::{PageID, INVALID_PAGE_ID};
use crate::hummock::CompressionAlgorithm;

const RIGHT_SPLIT_SIZE: usize = 32 * 1024;

pub struct LeafPage {
    raw: SortedRecordBlock,
    id: PageID,
    right_link: PageID,
    pub parent_link: PageID,
    pub smallest_user_key: Bytes,
    // The largest user key always equals the smallest user key of right-link page.
    pub largest_user_key: Bytes,
}

impl LeafPage {
    pub fn new(
        id: PageID,
        smallest_key: Bytes,
        largest_key: Bytes,
        raw: SortedRecordBlock,
    ) -> LeafPage {
        LeafPage {
            raw,
            id,
            right_link: INVALID_PAGE_ID,
            parent_link: INVALID_PAGE_ID,
            smallest_user_key: smallest_key,
            largest_user_key: largest_key,
        }
    }

    pub fn set_page_id(&mut self, pid: PageID) {
        self.id = pid;
    }

    pub fn get_right_link(&self) -> PageID {
        self.right_link
    }

    pub fn set_right_link(&mut self, right_link: PageID) {
        self.right_link = right_link;
    }

    pub fn set_parent_link(&mut self, parent_link: PageID) {
        self.parent_link = parent_link;
    }

    pub fn get_smallest_key_in_data(&self) -> Bytes {
        let iter = self.raw.iter();
        Bytes::copy_from_slice(user_key(iter.key()))
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mut iter = BlockIterator::new(&self.raw);
        iter.seek(key);
        if iter.is_valid() && user_key(iter.key()).eq(user_key(key)) {
            let v = iter.value().to_bytes();
            return v.into_user_value();
        }
        None
    }

    pub fn iter(&self) -> BlockIterator<'_> {
        self.raw.iter()
    }

    pub fn page_size(&self) -> usize {
        self.raw.size()
    }

    pub fn get_page_id(&self) -> PageID {
        self.id
    }

    pub fn get_middle_key(&self) -> Bytes {
        self.raw.get_middle_key()
    }
}
