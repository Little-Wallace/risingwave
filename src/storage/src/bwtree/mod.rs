use std::cmp::Ordering;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;

use crate::bwtree::index_page::IndexPageDeltaChain;

mod bw_tree_engine;
mod data_iterator;
mod delta_chain;
mod delta_hash_table;
mod index_page;
mod leaf_page;
mod mapping_table;
mod page_store;
mod smo;
mod sorted_data_builder;
mod sorted_record_block;
mod store;

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
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.epoch.cmp(&self.epoch))
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
            let mut buf = data.as_ref();
            buf.advance(l - 8);
            buf.get_u64()
        };
        Self {
            user_key: data.slice(..(l - 8)),
            epoch,
        }
    }
}

pub type PageID = u64;
pub const INVALID_PAGE_ID: u64 = 0;

#[derive(Clone)]
pub enum TypedPage {
    Index(Arc<RwLock<IndexPageDeltaChain>>),
    DataPage(PageID),
}
