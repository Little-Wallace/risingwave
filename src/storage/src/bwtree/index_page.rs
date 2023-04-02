use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;

use crate::bwtree::{PageId, INVALID_PAGE_ID};
use crate::hummock::sstable::utils::get_length_prefixed_slice;

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum PageType {
    Index,
    Leaf,
}

#[derive(Eq, PartialEq, Clone)]
pub struct SonPageInfo {
    pub page_id: PageId,
    // table_key, do not include epoch.
    pub smallest_key: Bytes,
}

impl SonPageInfo {
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

pub struct TreeInfoData {
    nodes: Vec<SonPageInfo>,
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
    parent_link: PageId,
    smallest_user_key: Bytes,
}

impl TreeInfoData {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16_le(self.nodes.len() as u16);
        for node in &self.nodes {
            node.encode_to(buf);
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let node_count = buf.get_u16_le() as usize;
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(SonPageInfo::decode_from(buf));
        }
        Self { nodes }
    }
}

impl IndexPage {
    pub fn new(
        pid: PageId,
        parent_link: PageId,
        smallest_user_key: Bytes,
        height: usize,
    ) -> IndexPage {
        Self {
            pid,
            sub_tree: Default::default(),
            epoch: 0,
            height,
            parent_link,
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
        let data = TreeInfoData::decode(&mut buf);
        let mut sub_tree = BTreeMap::new();
        for sub_info in data.nodes {
            sub_tree.insert(sub_info.smallest_key, sub_info.page_id);
        }
        let pid = buf.get_u64_le();
        let epoch = buf.get_u64_le();
        Self {
            pid,
            sub_tree,
            height: 0,
            epoch,
            right_link: INVALID_PAGE_ID,
            parent_link: INVALID_PAGE_ID,
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
        let mut index = self.sub_tree.range((
            std::ops::Bound::Included(key.clone()),
            std::ops::Bound::Unbounded,
        ));
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
            std::ops::Bound::Included(key.clone()),
            std::ops::Bound::Unbounded,
        ));
        index.next().unwrap();
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

    pub fn get_parent_link(&self) -> PageId {
        self.parent_link
    }

    pub fn set_right_link(&mut self, pid: PageId) {
        self.right_link = pid;
    }

    pub fn set_parent_link(&mut self, pid: PageId) {
        self.parent_link = pid;
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
    pub son: SonPageInfo,
    pub smo: SMOType,
    pub epoch: u64,
}

impl IndexPageDelta {
    pub fn new(smo: SMOType, page_id: PageId, epoch: u64, smallest_key: Bytes) -> Self {
        Self {
            son: SonPageInfo {
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
    current_epoch: u64,
    base_page: IndexPage,
}

pub type IndexPageHolder = Arc<RwLock<IndexPageDeltaChain>>;

impl IndexPageDeltaChain {
    pub fn create(
        deltas: Vec<IndexPageDelta>,
        base_page: IndexPage,
        max_commit_epoch: u64,
    ) -> IndexPageHolder {
        Arc::new(RwLock::new(Self::new(deltas, base_page, max_commit_epoch)))
    }

    pub fn new(
        deltas: Vec<IndexPageDelta>,
        mut base_page: IndexPage,
        max_commit_epoch: u64,
    ) -> Self {
        let immutable_delta_count = deltas.len();
        for delta in deltas {
            if delta.smo == SMOType::Add {
                base_page.insert_page(delta.son.smallest_key.clone(), delta.son.page_id);
            } else {
                base_page.delete_page(&delta.son.smallest_key);
            }
        }
        Self {
            current_epoch: max_commit_epoch,
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

    pub fn append_delta(&mut self, delta: IndexPageDelta) {
        if delta.smo == SMOType::Add {
            self.base_page
                .insert_page(delta.son.smallest_key.clone(), delta.son.page_id);
        } else {
            self.base_page.delete_page(&delta.son.smallest_key);
        }
        self.immutable_delta_count += 1;
        self.uncommited_delta.push(delta);
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
        self.immutable_delta_count = self.uncommited_delta.len();
        self.base_page = page;
    }

    pub fn reconsile(&mut self, commit_epoch: u64) -> Bytes {
        let mut buf = BytesMut::new();
        self.base_page.encode_to(&mut buf);
        let data = buf.freeze();
        self.uncommited_delta
            .retain(|delta| delta.epoch > commit_epoch);
        self.immutable_delta_count = self.uncommited_delta.len();
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

    pub fn get_parent_link(&self) -> PageId {
        self.base_page.parent_link
    }

    pub fn split_to_pages(&self, commit_epoch: u64) -> Vec<IndexPage> {
        let mut pages = vec![];
        let split_size = self.base_page.sub_tree.len() / 2;
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
                    parent_link: self.base_page.parent_link,
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
                parent_link: self.base_page.parent_link,
                smallest_user_key,
            });
        }
        pages
    }

    pub fn encode_page(&mut self, buf: &mut BytesMut) {
        self.base_page.encode_to(buf);
    }

    pub fn commit_delta(&mut self, commit_epoch: u64, buf: &mut BytesMut) {
        buf.put_u64_le(commit_epoch);
        buf.put_u32_le(self.uncommited_delta.len() as u32);
        for delta in self
            .uncommited_delta
            .drain_filter(|delta| delta.epoch <= commit_epoch)
        {
            delta.son.encode_to(buf);
            if delta.smo == SMOType::Add {
                buf.put_u8(0);
            } else {
                buf.put_u8(1);
            }
        }
    }
}
