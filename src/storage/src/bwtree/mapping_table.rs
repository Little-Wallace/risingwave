use risingwave_common::cache::{CacheableEntry, LookupResponse, LruCache, LruCacheEventListener};

use crate::bwtree::index_page::Page;
use crate::bwtree::PageID;

pub type PageHolder = CacheableEntry<PageID, Box<Page>>;

pub struct MappingTable {}
