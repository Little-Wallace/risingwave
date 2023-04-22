use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::key::{user_key, StateTableKey, TableKey};

use crate::bwtree::sorted_data_builder::BlockBuilder;
use crate::bwtree::sorted_record_block::{BlockIterator, SortedRecordBlock};
use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::sstable::utils::get_length_prefixed_slice;
use crate::hummock::value::HummockValue;
use crate::hummock::{CompressionAlgorithm, HummockError, HummockResult};
use crate::storage_value::StorageValue;

const RIGHT_SPLIT_SIZE: usize = 32 * 1024;

pub struct BasePage {
    raw: SortedRecordBlock,
    id: PageId,
    right_link: PageId,
    epoch: u64,
    pub smallest_user_key: Bytes,
    // The largest user key always equals the smallest user key of right-link page.
    pub largest_user_key: Bytes,
}

impl BasePage {
    pub fn empty(pid: PageId, epoch: u64) -> Self {
        BasePage::new(
            pid,
            Bytes::new(),
            Bytes::new(),
            SortedRecordBlock::empty(),
            epoch,
        )
    }

    pub fn encode_meta(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.get_page_id());
        buf.put_u64_le(self.get_right_link());
        buf.put_u64_le(self.epoch());
        buf.put_u32_le(self.smallest_user_key.len() as u32);
        buf.put_slice(self.smallest_user_key.as_ref());
        buf.put_u32_le(self.largest_user_key.len() as u32);
        buf.put_slice(self.largest_user_key.as_ref());
    }

    pub fn decode(data: Bytes) -> HummockResult<Self> {
        let origin_len = data.len();
        let buf = &mut data.as_ref();
        let page_id = buf.get_u64_le();
        let right_link = buf.get_u64_le();
        let epoch = buf.get_u64_le();
        let smallest_user_key = Bytes::from(get_length_prefixed_slice(buf));
        let largest_user_key = Bytes::from(get_length_prefixed_slice(buf));
        let data_len = buf.get_u32_le() as usize;
        if buf.remaining() != data_len {
            return Err(HummockError::decode_error("could not decode base page"));
        }
        let raw = SortedRecordBlock::decode(data.slice(origin_len - data_len..))?;
        Ok(BasePage {
            raw,
            id: page_id,
            right_link,
            epoch,
            smallest_user_key,
            largest_user_key,
        })
    }

    pub fn encode_size(&self) -> usize {
        // page id, epoch, right_link  and size of and smallest_user_key and largest_user_key.
        self.smallest_user_key.len()
            + self.largest_user_key.len()
            + std::mem::size_of::<u64>() * 3
            + std::mem::size_of::<u32>() * 2
    }

    pub fn build(
        pid: PageId,
        kvs: Vec<(Bytes, StorageValue)>,
        smallest_user_key: Bytes,
        largest_user_key: Bytes,
        epoch: u64,
    ) -> Self {
        let mut builder = BlockBuilder::default();
        let mut raw_key = BytesMut::new();
        let mut raw_value = BytesMut::new();
        for (k, v) in kvs {
            let vk = StateTableKey::new(TableKey(k), epoch);
            let v: HummockValue<Bytes> = v.into();
            vk.encode_into(&mut raw_key);
            v.encode(&mut raw_value);
            builder.add(raw_key.as_ref(), raw_value.as_ref());
            raw_key.clear();
            raw_value.clear();
        }
        let raw = SortedRecordBlock::decode(builder.build()).unwrap();
        BasePage::new(pid, smallest_user_key, largest_user_key, raw, epoch)
    }

    pub fn new(
        id: PageId,
        smallest_key: Bytes,
        largest_key: Bytes,
        raw: SortedRecordBlock,
        epoch: u64,
    ) -> BasePage {
        BasePage {
            raw,
            id,
            right_link: INVALID_PAGE_ID,
            smallest_user_key: smallest_key,
            largest_user_key: largest_key,
            epoch,
        }
    }

    pub fn set_page_id(&mut self, pid: PageId) {
        self.id = pid;
    }

    pub fn get_right_link(&self) -> PageId {
        self.right_link
    }

    pub fn set_right_link(&mut self, right_link: PageId) {
        self.right_link = right_link;
    }

    pub fn get(&self, key: &[u8], ukey: &[u8]) -> Option<Bytes> {
        if self.raw.is_empty() {
            return None;
        }
        debug_assert!(ukey.ge(self.smallest_user_key.as_ref()));
        let mut iter = BlockIterator::new(&self.raw);
        iter.seek(key);
        if iter.is_valid() && user_key(iter.key()).eq(ukey) {
            let v = iter.value().to_bytes();
            return v.into_user_value();
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    pub fn iter(&self) -> BlockIterator<'_> {
        self.raw.iter()
    }

    pub fn page_size(&self) -> usize {
        self.raw.raw_data().len()
    }

    pub fn get_page_id(&self) -> PageId {
        self.id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn check_valid_read(&self, user_key: &Bytes) -> bool {
        if self.right_link != INVALID_PAGE_ID && self.largest_user_key.le(user_key) {
            return false;
        }
        true
    }

    pub fn compress(&self, algorithm: CompressionAlgorithm) -> Bytes {
        self.raw.compress(algorithm)
    }
}
