use std::cmp::Ordering;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::bwtree::index_page::IndexPage;

mod data_iterator;
mod delta_chain;
mod leaf_page;
mod mapping_page;
mod mapping_table;
mod delta_hash_table;
mod sorted_data_builder;
mod sorted_record_block;
mod index_page;
mod page_store;
mod bwtree_engine;
mod root_page;


#[derive(Eq, PartialEq, Clone)]
pub struct VKey {
    pub user_key: Bytes,
    pub epoch: u64,
}

impl PartialOrd for VKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key.cmp(&other.user_key).then_with(||other.epoch.cmp(&self.epoch))
    }
}

impl VKey {
    pub fn len(&self) -> usize {
        self.user_key.len() + std::mem::size_of::<u64>()
    }

    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.user_key);
        buf.put_u64(self.epoch);
    }
}

impl From<Bytes> for VKey {
    fn from(data: Bytes) -> Self {
        let l = data.len();
        let epoch = {
            let mut buf = &data;
            buf.advance(l - 8);
            buf.get_u64()
        };
        Self {
            user_key: data.slice(..(l-8)),
            epoch,
        }
    }
}

pub type PageID = u64;
pub const INVALID_PAGE_ID: u64 = 0;

pub enum TypedPage {
    Index(IndexPage),
    DataPage(PageID),
}
