use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::KeyComparator;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::PageID;
use crate::hummock::sstable::utils::{get_length_prefixed_slice, put_length_prefixed_slice};


#[derive(Eq, PartialEq)]
pub enum PageType {
    Index,
    Leaf,
}

pub struct SonPageInfo {
    page_id: PageID,
    page_type: PageType,
    smallest_key: Bytes,
    // TODO: add more statistic
}

impl SonPageInfo {
    pub fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.page_id);
        buf.put_u8(if self.page_type == PageType::Leaf {
            0
        } else {
            1
        });
        buf.put_u32_le(self.smallest_key.len() as u32);
        buf.put_slice(self.smallest_key.as_ref());
    }

    pub fn decode_from(buf: &mut &[u8]) -> Self {
        let page_id = buf.get_u64_le();
        let page_type = if buf.get_u8() == 0 {
            PageType::Leaf
        } else {
            PageType::Index
        };
        let smallest_key = Bytes::from(get_length_prefixed_slice(buf));
        Self {
            smallest_key,
            page_id,
            page_type
        }
    }
}

pub struct TreeInfoData {
    nodes: Vec<SonPageInfo>,
}

pub struct IndexPage {
    data: TreeInfoData,
    epoch: u64,
    right_link: PageID,
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
        Self {
            nodes,
        }
    }
}

impl IndexPage {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.data.encode(&mut buf);
        buf.put_u64_le(self.epoch);
        buf.put_u64_le(self.right_link);
        buf.freeze()
    }

    pub fn decode(data: &Bytes) -> Self {
        let mut buf = data.as_ref();
        let data = TreeInfoData::decode(&mut buf);
        let epoch = buf.get_u64_le();
        let right_link = buf.get_u64_le();
        Self {
            data,
            epoch,
            right_link,
        }
    }
}

pub struct IndexDeltaChain {
    delta: Vec<SonPageInfo>,
    immutable_delta: TreeInfoData,
    current_epoch: u64,
    base_page: IndexPage,
}

impl IndexDeltaChain {
    pub fn get_page_in_range(&self, key: &Bytes) -> (PageID,PageType)  {
        let index = self.delta.partition_point(|info| {
            KeyComparator::compare_encoded_full_key(&info.smallest_key, &key) == std::cmp::Ordering::Less
        }).saturating_sub(1);
        (self.delta[index].page_id, self.delta[index].smallest_key)
    }
}