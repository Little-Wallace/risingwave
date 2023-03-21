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

pub type PageID = u64;
pub const INVALID_PAGE_ID: u64 = 0;

pub enum TypedPage {
    Index(IndexPage),
    DataPage(PageID),
}
