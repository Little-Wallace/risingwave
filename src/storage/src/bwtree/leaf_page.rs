use bytes::Bytes;
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
}

impl LeafPage {

    pub fn new(raw: SortedRecordBlock, id: PageID) -> LeafPage {
        LeafPage {
            raw,
            id,
            right_link: INVALID_PAGE_ID,
        }
    }

    pub fn split_to_right(&self, middle_count: usize, new_id: PageID) -> LeafPage {
        let mut iter = BlockIterator::new(&self.raw);
        iter.seek_to_first();
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: RIGHT_SPLIT_SIZE,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: 10,
        });
        let mut count = 0;
        while iter.is_valid() {
            if count < middle_count {
                count += 1;
            } else {
                builder.add(iter.key(), iter.value());
            }
            iter.next();
        }
        let data = builder.build();
        LeafPage {
            raw: SortedRecordBlock::decode_from_raw(data),
            id: new_id,
            right_link: INVALID_PAGE_ID,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mut iter = BlockIterator::new(&self.raw);
        iter.seek(key);
        if iter.is_valid()
            && KeyComparator::compare_encoded_full_key(iter.key(), iter.value())
                == std::cmp::Ordering::Equal
        {
            return Some(Bytes::from(iter.value().to_vec()));
        }
        None
    }

    pub fn iter(&self) -> BlockIterator<'_> {
        self.raw.iter()
    }

    pub fn page_size(&self) -> usize {
        self.raw.size()
    }
}
