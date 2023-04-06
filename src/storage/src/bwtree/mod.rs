use std::sync::Arc;

use parking_lot::RwLock;

use crate::bwtree::index_page::IndexPageDeltaChain;

mod bw_tree_engine;
mod data_iterator;
mod delta_chain;
mod delta_hash_table;
mod index_page;
mod leaf_page;
mod mapping_table;
mod page_id_generator;
mod page_store;
mod smo;
mod sorted_data_builder;
mod sorted_record_block;
mod store;
#[cfg(test)]
mod test_utils;

pub type PageId = u64;
pub const INVALID_PAGE_ID: u64 = 0;

#[derive(Clone)]
pub enum TypedPage {
    Index(Arc<RwLock<IndexPageDeltaChain>>),
    DataPage(PageId),
}
