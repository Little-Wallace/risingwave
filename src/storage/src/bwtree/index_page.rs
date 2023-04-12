use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use parking_lot::RwLock;

use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::sstable::utils::get_length_prefixed_slice;

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum PageType {
    Index,
    Leaf,
}

#[derive(Eq, PartialEq, Clone)]
pub struct SubtreePageInfo {
    pub page_id: PageId,
    // table_key, do not include epoch.
    pub smallest_key: Bytes,
}

impl SubtreePageInfo {
    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.page_id);
        buf.put_u32_le(self.smallest_key.len() as u32);
        buf.put_slice(self.smallest_key.as_ref());
    }

    pub fn decode_from(buf: &mut &[u8]) -> Self {
        let page_id = buf.get_u64_le();
        let smallest_key = Bytes::from(get_length_prefixed_slice(buf));
        Self {
            smallest_key,
            page_id,
        }
    }
}

///  Index Page records the index of leaf-page or other index-page. height represents height of this
/// sub-tree. If the height is 1, it means that all son of this sub-tree is leaf-page.
/// since index-page cache-miss would not be frequent, we can decode it as skip-list.
///  TODO: we can store common prefix for the whole sub-tree and only store suffix in sub-tree. And
/// this BTreeMap could be replace to concurrent-skiplist for batch-query.
pub struct IndexPage {
    pid: PageId,
    sub_tree: BTreeMap<Bytes, PageId>,
    epoch: u64,

    // Do not encode these fields because we can recover it from parent info.
    height: usize,
    right_link: PageId,
    smallest_user_key: Bytes,
}

impl IndexPage {
    pub fn new(pid: PageId, smallest_user_key: Bytes, epoch: u64, height: usize) -> IndexPage {
        Self {
            pid,
            sub_tree: Default::default(),
            epoch,
            height,
            right_link: INVALID_PAGE_ID,
            smallest_user_key,
        }
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_u16_le(self.sub_tree.len() as u16);
        for (key, page_id) in &self.sub_tree {
            buf.put_u64_le(*page_id);
            buf.put_u32_le(key.len() as u32);
            buf.put_slice(&key);
        }
        buf.put_u64_le(self.pid);
        buf.put_u64_le(self.epoch);
    }

    pub fn decode(data: &Bytes) -> Self {
        let mut buf = data.as_ref();
        let node_count = buf.get_u16_le() as usize;
        let mut sub_tree = BTreeMap::new();
        for _ in 0..node_count {
            let page_id = buf.get_u64_le();
            let smallest_key = Bytes::from(get_length_prefixed_slice(&mut buf));
            sub_tree.insert(smallest_key, page_id);
        }
        let pid = buf.get_u64_le();
        let epoch = buf.get_u64_le();
        Self {
            pid,
            sub_tree,
            height: 0,
            epoch,
            right_link: INVALID_PAGE_ID,
            smallest_user_key: Bytes::new(),
        }
    }

    pub fn get_index_key_in_range(&self, key: &Bytes) -> (Bytes, Bytes) {
        let mut index = self.sub_tree.range((
            std::ops::Bound::Included(key.clone()),
            std::ops::Bound::Unbounded,
        ));
        let left = index.next().unwrap().0.clone();
        if let Some(next) = index.next() {
            return (left, next.0.clone());
        }
        (left, Bytes::new())
    }

    pub fn get_page_in_range(&self, key: &Bytes) -> (PageId, PageType) {
        let mut index = self
            .sub_tree
            .range((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Included(key.clone()),
            ))
            .rev();
        let (_, page_idx) = index.next().unwrap();
        let ptype = if self.height == 1 {
            PageType::Leaf
        } else {
            PageType::Index
        };
        (*page_idx, ptype)
    }

    pub fn get_right_link_in_range(&self, key: &Bytes) -> PageId {
        let mut index = self.sub_tree.range((
            std::ops::Bound::Excluded(key.clone()),
            std::ops::Bound::Unbounded,
        ));
        if let Some((_, pid)) = index.next() {
            return *pid;
        }
        INVALID_PAGE_ID
    }

    pub fn get_left_page_info(&self) -> (Bytes, PageId) {
        let (k, p) = self.sub_tree.first_key_value().unwrap();
        (k.clone(), *p)
    }

    pub fn get_right_link(&self) -> PageId {
        self.right_link
    }

    pub fn set_right_link(&mut self, pid: PageId) {
        self.right_link = pid;
    }

    pub fn set_smallest_key(&mut self, key: Bytes) {
        self.smallest_user_key = key;
    }

    pub fn get_smallest_key(&self) -> Bytes {
        self.smallest_user_key.clone()
    }

    pub fn get_height(&self) -> usize {
        self.height
    }

    pub fn insert_page(&mut self, key: Bytes, pid: PageId) {
        self.sub_tree.insert(key, pid);
    }

    pub fn get_page_id(&self) -> PageId {
        self.pid
    }

    pub fn get_son_count(&self) -> usize {
        self.sub_tree.len()
    }

    pub fn get_son_pages(&self) -> Vec<PageId> {
        self.sub_tree.iter().map(|(_, p)| *p).collect_vec()
    }

    pub fn delete_page(&mut self, key: &Bytes) {
        self.sub_tree.remove(key);
    }

    pub fn set_page_id(&mut self, pid: PageId) {
        self.pid = pid;
    }
}

#[derive(Eq, PartialEq, Clone)]
pub enum SMOType {
    Add,
    Remove,
}

#[derive(Clone)]
pub struct IndexPageDelta {
    pub son: SubtreePageInfo,
    pub smo: SMOType,
    pub epoch: u64,
}

impl IndexPageDelta {
    pub fn new(smo: SMOType, page_id: PageId, epoch: u64, smallest_key: Bytes) -> Self {
        Self {
            son: SubtreePageInfo {
                page_id,
                smallest_key,
            },
            smo,
            epoch,
        }
    }
}

pub struct IndexPageDeltaChain {
    uncommited_delta: Vec<IndexPageDelta>,
    immutable_delta_count: usize,
    base_page: IndexPage,
}

pub type IndexPageHolder = Arc<RwLock<IndexPageDeltaChain>>;

impl IndexPageDeltaChain {
    pub fn create(deltas: Vec<IndexPageDelta>, base_page: IndexPage) -> IndexPageHolder {
        Arc::new(RwLock::new(Self::new(deltas, base_page)))
    }

    pub fn new(deltas: Vec<IndexPageDelta>, mut base_page: IndexPage) -> Self {
        let immutable_delta_count = deltas.len();
        for delta in deltas {
            if delta.smo == SMOType::Add {
                base_page.insert_page(delta.son.smallest_key.clone(), delta.son.page_id);
            } else {
                base_page.delete_page(&delta.son.smallest_key);
            }
        }
        Self {
            immutable_delta_count,
            uncommited_delta: vec![],
            base_page,
        }
    }

    /// Query sub-page by user-key. Because we would store all MVCC version of the same key in one
    ///  leaf page.
    pub fn get_page_in_range(&self, key: &Bytes) -> (PageId, PageType) {
        self.base_page.get_page_in_range(key)
    }

    /// Query sub-page by user-key. Because we would store all MVCC version of the same key in one
    ///  leaf page.
    pub fn get_right_link_in_range(&self, key: &Bytes) -> PageId {
        self.base_page.get_right_link_in_range(key)
    }

    pub fn apply_delta(&mut self, delta: IndexPageDelta) {
        if delta.smo == SMOType::Add {
            self.base_page
                .insert_page(delta.son.smallest_key, delta.son.page_id);
        } else {
            self.base_page.delete_page(&delta.son.smallest_key);
        }
        self.immutable_delta_count += 1;
    }

    pub fn shall_reconcile(&self, max_reconcile_count: usize) -> bool {
        self.immutable_delta_count > max_reconcile_count
    }

    pub fn shall_split(&self, max_split_count: usize) -> bool {
        self.base_page.sub_tree.len() > max_split_count
    }

    pub fn set_page(&mut self, commit_epoch: u64, page: IndexPage) {
        self.uncommited_delta
            .retain(|delta| delta.epoch > commit_epoch);
        self.base_page = page;
    }

    pub fn reconsile(&mut self, commit_epoch: u64) -> Bytes {
        let mut buf = BytesMut::new();
        self.base_page.epoch = commit_epoch;
        self.base_page.encode_to(&mut buf);
        let data = buf.freeze();
        self.immutable_delta_count = 0;
        data
    }

    pub fn get_base_page_id(&self) -> PageId {
        self.base_page.pid
    }

    pub fn get_base_page(&self) -> &IndexPage {
        &self.base_page
    }

    pub fn get_right_link(&self) -> PageId {
        self.base_page.right_link
    }

    pub fn split_to_pages(&self, max_split_index_size: usize, commit_epoch: u64) -> Vec<IndexPage> {
        let mut pages = vec![];
        let split_count =
            (self.base_page.sub_tree.len() + max_split_index_size - 1) / max_split_index_size;
        let split_size = std::cmp::min(
            max_split_index_size,
            self.base_page.sub_tree.len() / split_count,
        );
        let mut sub_tree = BTreeMap::default();
        let mut smallest_user_key = self.base_page.smallest_user_key.clone();
        for (smallest_key, pid) in &self.base_page.sub_tree {
            if sub_tree.len() >= split_size {
                pages.push(IndexPage {
                    pid: 0,
                    sub_tree,
                    height: self.base_page.height,
                    epoch: commit_epoch,
                    right_link: 0,
                    smallest_user_key,
                });
                sub_tree = BTreeMap::default();
                smallest_user_key = smallest_key.clone();
            }
            sub_tree.insert(smallest_key.clone(), *pid);
        }
        if !sub_tree.is_empty() {
            pages.push(IndexPage {
                pid: 0,
                sub_tree,
                height: self.base_page.height,
                epoch: commit_epoch,
                right_link: 0,
                smallest_user_key,
            });
        }
        pages
    }

    pub fn encode_page(&mut self, buf: &mut BytesMut) {
        self.base_page.encode_to(buf);
    }

    pub fn set_new_page(&mut self, page: IndexPage) {
        self.uncommited_delta.clear();
        self.base_page = page;
    }

    pub fn merge_pages(&self, max_epoch: u64, pages: &[Arc<RwLock<Self>>]) -> IndexPage {
        let mut sub_tree = self.base_page.sub_tree.clone();
        let mut right_link = self.base_page.right_link;
        for delta_chains in pages {
            let guard = delta_chains.read();
            for (k, v) in &guard.base_page.sub_tree {
                sub_tree.insert(k.clone(), *v);
            }
            right_link = self.base_page.get_right_link();
        }
        IndexPage {
            pid: self.base_page.get_page_id(),
            sub_tree,
            epoch: max_epoch,
            height: self.base_page.height,
            right_link,
            smallest_user_key: Default::default(),
        }
    }
}
